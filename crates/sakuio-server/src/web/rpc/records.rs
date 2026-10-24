use crate::AppState;
use chrono::{TimeZone, Utc};
use sakuio_board::{BoardStateStore, RECENT_TELEMETRY_MS};
use sakuio_db::ClientRepository;
use sakuio_protocol::{BasicInfoParams, TelemetrySample};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

use super::data::{load_public_clients, parse_basic, v2_report_from_snapshot};
use super::ping::{
    parse_ping_hours, parse_ping_max_count, parse_task_id, query_ping_records, PingQuery,
};

static RECENT_STATUS_CACHE: OnceLock<Mutex<HashMap<(String, i64, i64, i64), Arc<Value>>>> =
    OnceLock::new();
static RECENT_REPORTS_CACHE: OnceLock<Mutex<HashMap<(String, i64, i64, String), Arc<Value>>>> =
    OnceLock::new();

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
    let limit = params
        .and_then(|p| p.get("limit"))
        .and_then(|l| l.as_i64())
        .unwrap_or(0);
    let signature = (
        uuid.to_string(),
        samples.last().map(|s| s.observed_at_ms).unwrap_or(0),
        samples.len() as i64,
        limit,
    );
    let cache = RECENT_STATUS_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(value) = cache.lock().await.get(&signature) {
        return Ok((**value).clone());
    }

    let mut records: Vec<Value> = samples
        .iter()
        .map(|s| sample_to_status_record(uuid, s))
        .collect();

    if limit > 0 && records.len() > limit as usize {
        records = records.split_off(records.len() - limit as usize);
    }

    let value = json!({
        "count": records.len(),
        "records": records,
    });
    let mut cache = cache.lock().await;
    if cache.len() > 512 {
        cache.clear();
    }
    cache.insert(signature, Arc::new(value.clone()));
    Ok(value)
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

    let row = ClientRepository::new(&state.db)
        .get_for_public_api(uuid)
        .await
        .map_err(|_| (-32603, "Internal error"))?
        .ok_or((-32602, "UUID is required"))?;

    let since = Utc::now().timestamp_millis() - RECENT_TELEMETRY_MS;
    let samples = state
        .board
        .list_recent_telemetry(uuid, since)
        .await
        .map_err(|_| (-32603, "Internal error"))?;
    let signature = (
        uuid.to_string(),
        samples.last().map(|s| s.observed_at_ms).unwrap_or(0),
        samples.len() as i64,
        row.updated_at.clone(),
    );
    let cache = RECENT_REPORTS_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(value) = cache.lock().await.get(&signature) {
        return Ok((**value).clone());
    }

    let basic = parse_basic(&row);
    let reports: Vec<Value> = samples
        .iter()
        .map(|s| v2_report_from_sample(s, &basic))
        .collect();
    let value = Value::Array(reports);
    let mut cache = cache.lock().await;
    if cache.len() > 512 {
        cache.clear();
    }
    cache.insert(signature, Arc::new(value.clone()));
    Ok(value)
}

const DEFAULT_POINT_BUDGET: i64 = 2_000;

fn point_budget(params: Option<&Value>, fallback: i64) -> i64 {
    let requested = params
        .and_then(|p| p.get("max_points").or_else(|| p.get("maxPoints")))
        .and_then(|v| v.as_i64())
        .unwrap_or(DEFAULT_POINT_BUDGET);
    requested.clamp(100, fallback.clamp(100, 50_000))
}

fn budget_bucket(span_ms: i64, preferred: i64, budget: i64) -> i64 {
    // `span_ms / budget` is milliseconds per point; window buckets are in
    // seconds. Comparing them directly forced every range to a 1-day bucket.
    let needed_seconds = (span_ms / 1000 / budget.max(1)).max(1);
    for window in [60i64, 300, 3_600, 86_400] {
        if window >= preferred && window >= needed_seconds {
            return window;
        }
    }
    86_400
}

fn downsample_rows(rows: Vec<serde_json::Value>, budget: i64) -> Vec<serde_json::Value> {
    if rows.len() as i64 <= budget || budget <= 0 {
        return rows;
    }
    let step = ((rows.len() as f64) / budget as f64).ceil() as usize;
    rows.into_iter().step_by(step.max(1)).collect()
}

async fn query_buckets(
    store: &sakuio_metric::MetricStore,
    uuid: &str,
    metric_key: Option<&str>,
    bucket_seconds: i64,
    since_ms: i64,
    until_ms: i64,
    limit: i64,
) -> Result<Vec<sakuio_metric::BucketRow>, (i32, &'static str)> {
    match metric_key {
        Some(key) => {
            store
                .list_buckets_for_metric(uuid, key, bucket_seconds, since_ms, until_ms, limit)
                .await
        }
        None => {
            store
                .list_buckets(uuid, bucket_seconds, since_ms, until_ms, limit)
                .await
        }
    }
    .map_err(|_| (-32603, "Internal error"))
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
    let metric_key = p
        .get("metric_key")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty());

    let until_ms = Utc::now().timestamp_millis();
    let since_ms = until_ms - hours * 3_600_000;
    let budget = point_budget(params, 50_000);

    // 原始采样只在内存保留约 10 分钟；更久的数据读降采样桶。
    // 窗口选择同时受时间范围和返回点数预算约束。
    let preferred = if hours <= 1 {
        60
    } else if hours <= 24 {
        300
    } else {
        3600
    };
    let mut bucket_seconds = budget_bucket(hours * 3_600_000, preferred, budget);
    let fetch_limit = (budget * 4).clamp(100, 50_000);

    let store = state.metric.current_store().await;
    let mut rows = query_buckets(
        &store,
        uuid,
        metric_key,
        bucket_seconds,
        since_ms,
        until_ms,
        fetch_limit,
    )
    .await?;
    // A completed 1h bucket only exists after a full hour of samples. For a
    // long range, fall back to the finest available window instead of
    // returning an empty chart while 4h data exists.
    if rows.is_empty() {
        for fallback in [3600i64, 300, 60] {
            if fallback >= bucket_seconds {
                continue;
            }
            let candidate = query_buckets(
                &store,
                uuid,
                metric_key,
                fallback,
                since_ms,
                until_ms,
                fetch_limit,
            )
            .await?;
            if !candidate.is_empty() {
                bucket_seconds = fallback;
                rows = candidate;
                break;
            }
        }
    }

    let mut records: Vec<Value> = buckets_to_records(uuid, bucket_seconds, &rows, since_ms);
    records = downsample_rows(records, budget);

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
    let budget = point_budget(params, max_count);

    // 原始采样只在内存（board）保留约 10 分钟；更久读降采样桶。
    let preferred = if hours <= 1 {
        60
    } else if hours <= 24 {
        300
    } else {
        3600
    };
    let bucket_seconds = budget_bucket(hours * 3_600_000, preferred, budget);
    let fetch_limit = (budget * 4).clamp(100, 50_000);

    let metric_key = params
        .and_then(|p| p.get("metric_key"))
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty());
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
        fetch_limit
    } else {
        (fetch_limit as usize / node_list.len().max(1)).max(100) as i64
    };
    if metric_key.is_none() && node_list.len() > 1 {
        let rows_by_node = store
            .list_buckets_multi(
                &node_list,
                None,
                bucket_seconds,
                since_ms,
                until_ms,
                fetch_limit,
            )
            .await
            .map_err(|_| (-32603, "Internal error"))?;
        for (client, rows) in rows_by_node {
            let mut records = buckets_to_records(&client, bucket_seconds, &rows, since_ms);
            records = downsample_rows(records, budget);
            grouped.insert(client, records);
        }
    } else {
        for client in &node_list {
            let node_rows = match metric_key {
                Some(key) => {
                    store
                        .list_buckets_for_metric(
                            client,
                            key,
                            bucket_seconds,
                            since_ms,
                            until_ms,
                            per_node_limit,
                        )
                        .await
                }
                None => {
                    store
                        .list_buckets(client, bucket_seconds, since_ms, until_ms, per_node_limit)
                        .await
                }
            };
            let Ok(node_rows) = node_rows else {
                continue;
            };
            let mut records = buckets_to_records(client, bucket_seconds, &node_rows, since_ms);
            records = downsample_rows(records, budget);
            grouped.insert(client.clone(), records);
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
    let mut stats_by_slot: BTreeMap<i64, Vec<&sakuio_metric::BucketRow>> = BTreeMap::new();
    for row in rows {
        let slot = row.bucket_start_ms + bucket_seconds * 1000;
        by_slot
            .entry(slot)
            .or_default()
            .insert(metric_field(row.metric_key.as_str()), row.avg_val);
        stats_by_slot.entry(slot).or_default().push(row);
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
            for row in stats_by_slot.get(&slot).into_iter().flatten() {
                let field = metric_field(row.metric_key.as_str());
                obj.insert(format!("{field}_p50"), json!(row.p50_val as f32));
                obj.insert(format!("{field}_p95"), json!(row.p95_val as f32));
                obj.insert(format!("{field}_p99"), json!(row.p99_val as f32));
                obj.insert(format!("{field}_variance"), json!(row.variance as f32));
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

#[cfg(test)]
mod budget_tests {
    use super::*;

    #[test]
    fn budget_bucket_respects_requested_and_budget() {
        assert_eq!(budget_bucket(3_600_000, 60, 2_000), 60);
        assert_eq!(budget_bucket(86_400_000, 300, 2_000), 300);
        assert_eq!(budget_bucket(14_400_000, 300, 2_000), 300);
        assert_eq!(budget_bucket(604_800_000, 300, 2_000), 3_600);
    }

    #[test]
    fn downsample_keeps_points_bounded() {
        let rows = (0..10_000).map(|i| json!({ "value": i })).collect();
        assert_eq!(downsample_rows(rows, 2_000).len(), 2_000);
    }
}
