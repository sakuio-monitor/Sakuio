use crate::AppState;
use chrono::{TimeZone, Utc};
use sakuio_board::{BoardStateStore, RECENT_TELEMETRY_MS};
use sakuio_metric::MetricStore;
use sakuio_protocol::{BasicInfoParams, TelemetrySample};
use serde_json::{json, Value};

use super::data::{load_public_clients, parse_basic, v2_report_from_snapshot};
use super::ping::{parse_ping_hours, parse_ping_max_count, parse_task_id, query_ping_records, PingQuery};

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

    let rows = load_public_clients(state).await.map_err(|_| (-32603, "Internal error"))?;
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
        .and_then(|h| h.as_str().and_then(|s| s.parse().ok()).or_else(|| h.as_i64()))
        .unwrap_or(4);
    let hours = hours.clamp(1, 168);

    let load_type = p
        .get("load_type")
        .and_then(|t| t.as_str())
        .unwrap_or("");

    let until_ms = Utc::now().timestamp_millis();
    let since_ms = until_ms - hours * 3_600_000;

    let store = MetricStore::new(state.db.clone());
    let rows = store
        .list_samples(Some(uuid), since_ms, until_ms, 10_000)
        .await
        .map_err(|_| (-32603, "Internal error"))?;

    let records: Vec<Value> = rows
        .iter()
        .map(|(_, s)| sample_to_load_record(uuid, s))
        .collect();

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
    let max_count = if max_count == -1 { 50_000 } else { max_count.clamp(1, 50_000) };

    let until_ms = Utc::now().timestamp_millis();
    let since_ms = until_ms - hours * 3_600_000;

    let store = MetricStore::new(state.db.clone());
    let rows = store
        .list_samples(uuid, since_ms, until_ms, max_count as i32)
        .await
        .map_err(|_| (-32603, "Internal error"))?;

    let load_type = params
        .and_then(|p| p.get("load_type"))
        .and_then(|t| t.as_str())
        .unwrap_or("");

    if !load_type.is_empty() && load_type != "all" {
        let mut grouped: std::collections::HashMap<String, Vec<Value>> =
            std::collections::HashMap::new();
        for (client, sample) in &rows {
            let rec = sample_to_flat_record(client, sample, load_type);
            grouped.entry(client.clone()).or_default().push(rec);
        }
        let total: usize = grouped.values().map(|v| v.len()).sum();
        return Ok(json!({
            "count": total,
            "records": grouped,
            "load_type": load_type,
            "from": ms_to_rfc3339(since_ms),
            "to": ms_to_rfc3339(until_ms),
        }));
    }

    let mut grouped: std::collections::HashMap<String, Vec<Value>> =
        std::collections::HashMap::new();
    for (client, sample) in &rows {
        grouped
            .entry(client.clone())
            .or_default()
            .push(sample_to_load_record(client, sample));
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

fn sample_to_load_record(client: &str, s: &TelemetrySample) -> Value {
    sample_to_status_record(client, s)
}

fn sample_to_flat_record(client: &str, s: &TelemetrySample, load_type: &str) -> Value {
    let full = sample_to_status_record(client, s);
    let mut out = json!({
        "client": client,
        "time": full.get("time").cloned().unwrap_or(Value::Null),
    });
    let key = match load_type {
        "cpu" => "cpu",
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
    };
    if let Some(v) = full.get(key) {
        out.as_object_mut().unwrap().insert("value".into(), v.clone());
    }
    out
}

fn filter_by_load_type(records: &[Value], load_type: &str) -> Vec<Value> {
    records
        .iter()
        .filter_map(|r| {
            let key = match load_type {
                "cpu" => "cpu",
                "gpu" => "gpu",
                "ram" => "ram",
                "swap" => "swap",
                "load" => "load",
                "temp" => "temp",
                "disk" => "disk",
                "network" => "net_in",
                "process" => "process",
                "connections" => "connections",
                _ => return None,
            };
            Some(json!({
                "client": r.get("client"),
                "time": r.get("time"),
                "value": r.get(key).cloned().unwrap_or(Value::Null),
            }))
        })
        .collect()
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
