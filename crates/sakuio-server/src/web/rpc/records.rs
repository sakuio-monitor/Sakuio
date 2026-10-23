use crate::AppState;
use chrono::{TimeZone, Utc};
use sakuio_board::{BoardStateStore, RECENT_TELEMETRY_MS};
use sakuio_protocol::{BasicInfoParams, TelemetrySample};
use serde_json::{json, Value};

use super::data::{load_public_clients, parse_basic, v2_report_from_snapshot};
use super::ping::{
    parse_ping_hours, parse_ping_max_count, parse_task_id, query_ping_records, PingQuery,
};

pub async fn common_get_node_recent_status(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let uuid = params
        .and_then(|p| p.get("uuid"))
        .and_then(|u| u.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or((-32602, "UUID is required"))?;

    let since = Utc::now().timestamp_millis() - RECENT_TELEMETRY_MS;
    let samples = state
        .board
        .list_recent_telemetry(uuid, since)
        .await
        .map_err(|_| (-32603, "Internal error"))?;

    let mut records: Vec<Value> = samples
        .iter()
        .map(|s| sample_to_status_record(uuid, s))
        .collect();

    if let Some(limit) = params.and_then(|p| p.get("limit")).and_then(|l| l.as_i64()) {
        if limit > 0 && records.len() > limit as usize {
            records = records.split_off(records.len() - limit as usize);
        }
    }

    Ok(json!({
        "count": records.len(),
        "records": records,
    }))
}

pub async fn public_get_client_recent_records(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let uuid = params
        .and_then(|p| p.get("uuid"))
        .and_then(|u| u.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or((-32602, "UUID is required"))?;

    let rows = load_public_clients(state)
        .await
        .map_err(|_| (-32603, "Internal error"))?;
    let row = rows
        .iter()
        .find(|r| r.uuid == uuid)
        .ok_or((-32602, "UUID is required"))?;

    let since = Utc::now().timestamp_millis() - RECENT_TELEMETRY_MS;
    let samples = state
        .board
        .list_recent_telemetry(uuid, since)
        .await
        .map_err(|_| (-32603, "Internal error"))?;

    let basic = parse_basic(row);
    let reports: Vec<Value> = samples
        .iter()
        .map(|s| v2_report_from_sample(s, &basic))
        .collect();
    Ok(Value::Array(reports))
}

pub async fn public_get_records_by_uuid(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let p = params.ok_or((-32602, "Invalid params"))?;
    let uuid = p
        .get("uuid")
        .and_then(|u| u.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or((-32602, "UUID is required"))?;

    let hours: i64 = p
        .get("hours")
        .and_then(|h| {
            h.as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| h.as_i64())
        })
        .unwrap_or(4);
    let hours = hours.clamp(1, 168);

    let load_type = p.get("load_type").and_then(|t| t.as_str()).unwrap_or("");

    let until_ms = Utc::now().timestamp_millis();
    let since_ms = until_ms - hours * 3_600_000;

    // 原始采样只在内存保留约 10 分钟；更久的数据读降采样桶。
    // 窗口选择：≤1h 用 1 分钟桶，≤24h 用 5 分钟桶，其余用 1 小时桶。
    let bucket_seconds = if hours <= 1 {
        60
    } else if hours <= 24 {
        300
    } else {
        3600
    };

    let store = state.metric.current_store().await;
    let rows = store
        .list_buckets(uuid, bucket_seconds, since_ms, until_ms, 50_000)
        .await
        .map_err(|_| (-32603, "Internal error"))?;

    let records: Vec<Value> = buckets_to_records(uuid, bucket_seconds, &rows, since_ms);

    if !load_type.is_empty() && load_type != "all" {
        let filtered = filter_by_load_type(&records, load_type);
        return Ok(json!({
            "records": filtered,
            "count": filtered.len(),
            "load_type": load_type,
        }));
    }

    Ok(json!({
        "records": records,
        "count": records.len(),
    }))
}

pub async fn common_get_records(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let record_type = params
        .and_then(|p| p.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("load");
    if record_type == "ping" {
        let uuid = params
            .and_then(|p| p.get("uuid"))
            .and_then(|u| u.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from);
        return query_ping_records(
            state,
            PingQuery {
                uuid,
                task_id: parse_task_id(params),
                hours: parse_ping_hours(params),
                max_count: parse_ping_max_count(params),
                require_filter: false,
            },
        )
        .await;
    }

    let uuid = params
        .and_then(|p| p.get("uuid"))
        .and_then(|u| u.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let hours = params
        .and_then(|p| p.get("hours"))
        .and_then(|h| h.as_i64())
        .unwrap_or(1)
        .clamp(1, 168);
    let max_count = params
        .and_then(|p| p.get("maxCount"))
        .and_then(|c| c.as_i64())
        .unwrap_or(4000);
    let max_count = if max_count == -1 {
        50_000
    } else {
        max_count.clamp(1, 50_000)
    };

    let until_ms = Utc::now().timestamp_millis();
    let since_ms = until_ms - hours * 3_600_000;

    // 原始采样只在内存（board）保留约 10 分钟；更久读降采样桶。
    let bucket_seconds = if hours <= 1 {
        60
    } else if hours <= 24 {
        300
    } else {
        3600
    };

    let store = state.metric.current_store().await;
    let node_list: Vec<String> = match uuid {
        Some(u) => vec![u.to_string()],
        None => load_public_clients(state)
            .await
            .map_err(|_| (-32603, "Internal error"))?
            .iter()
            .map(|c| c.uuid.clone())
            .collect(),
    };

    let mut grouped: std::collections::HashMap<String, Vec<Value>> =
        std::collections::HashMap::new();
    let per_node_limit = if uuid.is_some() {
        max_count as i64
    } else {
        5_000
    };
    for client in &node_list {
        let Ok(node_rows) = store
            .list_buckets(client, bucket_seconds, since_ms, until_ms, per_node_limit)
            .await
        else {
            continue;
        };
        for rec in buckets_to_records(client, bucket_seconds, &node_rows, since_ms) {
            grouped.entry(client.clone()).or_default().push(rec);
        }
    }

    let load_type = params
        .and_then(|p| p.get("load_type"))
        .and_then(|t| t.as_str())
        .unwrap_or("");

    if !load_type.is_empty() && load_type != "all" {
        let mut flat: std::collections::HashMap<String, Vec<Value>> =
            std::collections::HashMap::new();
        for (client, records) in &grouped {
            for rec in records {
                let Some(rec) = rec.as_object() else { continue };
                let key = load_type_field(load_type);
                flat.entry(client.clone()).or_default().push(json!({
                    "client": client,
                    "time": rec.get("time").cloned().unwrap_or(Value::Null),
                    "value": rec.get(key).cloned().unwrap_or(Value::Null),
                }));
            }
        }
        let total: usize = flat.values().map(|v| v.len()).sum();
        return Ok(json!({
            "count": total,
            "records": flat,
            "load_type": load_type,
            "from": ms_to_rfc3339(since_ms),
            "to": ms_to_rfc3339(until_ms),
        }));
    }

    let total: usize = grouped.values().map(|v| v.len()).sum();
    Ok(json!({
        "count": total,
        "records": grouped,
        "from": ms_to_rfc3339(since_ms),
        "to": ms_to_rfc3339(until_ms),
    }))
}

pub async fn public_get_ping_records(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let uuid = params
        .and_then(|p| p.get("uuid"))
        .and_then(|u| u.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from);
    let task_id = parse_task_id(params);
    query_ping_records(
        state,
        PingQuery {
            uuid,
            task_id,
            hours: parse_ping_hours(params),
            max_count: 50_000,
            require_filter: true,
        },
    )
    .await
}

fn ms_to_rfc3339(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339())
}

/// Re-assembles bucket rows into chart records (one point per bucket slot).
/// 桶行重组为图表记录：每个桶时刻一个点，指标 key 映射回字段名。
fn buckets_to_records(
    client: &str,
    bucket_seconds: i64,
    rows: &[sakuio_metric::BucketRow],
    _since_ms: i64,
) -> Vec<Value> {
    use std::collections::BTreeMap;
    let mut by_slot: BTreeMap<i64, BTreeMap<&str, f64>> = BTreeMap::new();
    for row in rows {
        let slot = row.bucket_start_ms + bucket_seconds * 1000;
        by_slot
            .entry(slot)
            .or_default()
            .insert(metric_field(row.metric_key.as_str()), row.avg_val);
    }
    by_slot
        .into_iter()
        .map(|(slot, values)| {
            let mut rec = json!({
                "client": client,
                "time": ms_to_rfc3339(slot),
            });
            let cpu = values.get("cpu").copied().unwrap_or(0.0);
            let obj = rec.as_object_mut().unwrap();
            for (field, value) in &values {
                let v = match *field {
                    "ram" | "swap" | "disk" | "net_in" | "net_out" | "process" => {
                        json!(*value as i64)
                    }
                    _ => json!(*value as f32),
                };
                obj.insert((*field).into(), v);
            }
            obj.entry("cpu").or_insert_with(|| json!(cpu as f32));
            rec
        })
        .collect()
}

/// Maps a `load_type` query value to its chart record field.
/// `load_type` 查询值到图表字段的映射。
fn load_type_field(load_type: &str) -> &'static str {
    match load_type {
        "gpu" => "gpu",
        "ram" => "ram",
        "swap" => "swap",
        "load" => "load",
        "temp" => "temp",
        "disk" => "disk",
        "network" => "net_in",
        "process" => "process",
        "connections" => "connections",
        _ => "cpu",
    }
}

/// Projects full records down to `{client, time, value}` for one load type.
/// 将完整记录投影为单一负载类型的 `{client, time, value}`。
fn filter_by_load_type(records: &[Value], load_type: &str) -> Vec<Value> {
    let key = load_type_field(load_type);
    records
        .iter()
        .filter_map(|rec| rec.as_object())
        .map(|rec| {
            json!({
                "client": rec.get("client").cloned().unwrap_or(Value::Null),
                "time": rec.get("time").cloned().unwrap_or(Value::Null),
                "value": rec.get(key).cloned().unwrap_or(Value::Null),
            })
        })
        .collect()
}

/// Maps a metric key to its chart record field.
/// 指标 key 到图表字段的映射。
fn metric_field(key: &str) -> &'static str {
    match key {
        "cpu.usage" => "cpu",
        "memory.used" => "ram",
        "swap.used" => "swap",
        "load.average" => "load",
        "disk.used" => "disk",
        "net.in.rate" => "net_in",
        "net.out.rate" => "net_out",
        "process.count" => "process",
        "gpu.usage" => "gpu",
        "gpu.temperature" => "temp",
        "connections.tcp" => "connections_tcp",
        "connections.udp" => "connections_udp",
        _ => "",
    }
}

fn sample_to_status_record(client: &str, s: &TelemetrySample) -> Value {
    let gpu = gpu_usage(s);
    json!({
        "client": client,
        "time": ms_to_rfc3339(s.observed_at_ms),
        "cpu": s.cpu.usage as f32,
        "gpu": gpu,
        "ram": s.ram.used as i64,
        "ram_total": s.ram.total as i64,
        "swap": s.swap.used as i64,
        "swap_total": s.swap.total as i64,
        "load": s.load.load1 as f32,
        "load5": s.load.load5 as f32,
        "load15": s.load.load15 as f32,
        "temp": 0.0_f32,
        "disk": s.disk.used as i64,
        "disk_total": s.disk.total as i64,
        "net_in": s.network.down as i64,
        "net_out": s.network.up as i64,
        "net_total_up": s.network.total_up as i64,
        "net_total_down": s.network.total_down as i64,
        "process": s.process,
        "connections": s.connections.tcp + s.connections.udp,
        "connections_udp": s.connections.udp,
    })
}

fn gpu_usage(s: &TelemetrySample) -> f32 {
    s.gpu
        .as_ref()
        .and_then(|v| v.get("average_usage").and_then(|x| x.as_f64()))
        .map(|f| f as f32)
        .unwrap_or(0.0)
}

pub fn v2_report_from_sample(sample: &TelemetrySample, basic: &BasicInfoParams) -> Value {
    let snap = sakuio_board::RuntimeSnapshot {
        node_uuid: String::new(),
        observed_at_ms: sample.observed_at_ms,
        cpu_usage: Some(sample.cpu.usage),
        mem_used: Some(sample.ram.used as i64),
        mem_total: Some(sample.ram.total as i64),
        swap_used: Some(sample.swap.used as i64),
        swap_total: Some(sample.swap.total as i64),
        load1: Some(sample.load.load1),
        load5: Some(sample.load.load5),
        load15: Some(sample.load.load15),
        disk_used: Some(sample.disk.used as i64),
        disk_total: Some(sample.disk.total as i64),
        net_up: Some(sample.network.up),
        net_down: Some(sample.network.down),
        net_total_up: Some(sample.network.total_up),
        net_total_down: Some(sample.network.total_down),
        process: Some(sample.process),
        connections_tcp: Some(sample.connections.tcp),
        connections_udp: Some(sample.connections.udp),
        uptime_secs: Some(sample.uptime),
        gpu_usage: Some(gpu_usage(sample)),
        online: true,
    };
    let mut report = v2_report_from_snapshot(&snap, basic);
    if let Some(gpu) = &sample.gpu {
        if let Some(obj) = report.as_object_mut() {
            obj.insert("gpu".into(), gpu.clone());
        }
    }
    report
}
