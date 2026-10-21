use crate::AppState;
use sakuio_db::{task_to_public_list_json, PingRecordRow, PingRepository, PingTaskRow};
use serde_json::{json, Map, Value};
use sqlx::SqlitePool;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PingQuery {
    pub uuid: Option<String>,
    pub task_id: Option<i64>,
    pub hours: i64,
    pub max_count: i32,
    pub require_filter: bool,
}

pub async fn query_ping_records(
    state: &AppState,
    q: PingQuery,
) -> Result<Value, (i32, &'static str)> {
    if q.require_filter && q.uuid.is_none() && q.task_id.is_none() {
        return Err((-32602, "UUID or task_id is required"));
    }

    let repo = PingRepository::new(&state.db);
    let tasks = repo
        .list_all()
        .await
        .map_err(|_| (-32603, "Internal error"))?;
    if tasks.is_empty() {
        let until_ms = chrono::Utc::now().timestamp_millis();
        let since_ms = until_ms - q.hours.clamp(1, 168) * 3_600_000;
        return Ok(build_ping_response(&[], &[], &q, since_ms, until_ms));
    }

    let until_ms = chrono::Utc::now().timestamp_millis();
    let since_ms = until_ms - q.hours.clamp(1, 168) * 3_600_000;

    let records = repo
        .list_records(
            q.uuid.as_deref(),
            q.task_id,
            since_ms,
            until_ms,
            q.max_count,
        )
        .await
        .map_err(|_| (-32603, "Internal error"))?;

    Ok(build_ping_response(&records, &tasks, &q, since_ms, until_ms))
}

pub async fn public_ping_tasks(state: &AppState) -> Result<Value, (i32, &'static str)> {
    let tasks = PingRepository::new(&state.db)
        .list_all()
        .await
        .map_err(|_| (-32603, "Internal error"))?;
    let out: Vec<Value> = tasks.iter().map(task_to_public_list_json).collect();
    Ok(Value::Array(out))
}

fn build_ping_response(
    records: &[PingRecordRow],
    tasks: &[PingTaskRow],
    q: &PingQuery,
    since_ms: i64,
    until_ms: i64,
) -> Value {
    let mut client_stats: HashMap<String, (i32, i32, i32, i32)> = HashMap::new();
    let mut out_records: Vec<Value> = Vec::with_capacity(records.len());

    for r in records {
        if let Some(ref want) = q.uuid {
            if !uuid_matches(&r.client, want) {
                continue;
            }
        }
        if let Some(tid) = q.task_id {
            if r.task_id != tid {
                continue;
            }
        }

        out_records.push(json!({
            "client": r.client,
            "task_id": r.task_id,
            "time": r.time,
            "value": r.value,
        }));

        let st = client_stats.entry(r.client.clone()).or_insert((0, 0, 0, 0));
        st.0 += 1;
        if r.value < 0 {
            st.1 += 1;
        } else {
            if st.2 == 0 || r.value < st.2 {
                st.2 = r.value;
            }
            if r.value > st.3 {
                st.3 = r.value;
            }
        }
    }

    let basic_info: Vec<Value> = client_stats
        .into_iter()
        .map(|(client, (total, loss, min, max))| {
            let loss_pct = if total > 0 {
                (loss as f64 / total as f64) * 100.0
            } else {
                0.0
            };
            json!({
                "client": client,
                "loss": loss_pct,
                "min": min,
                "max": max,
            })
        })
        .collect();

    let filtered_tasks: Vec<Value> = tasks
        .iter()
        .filter(|t| task_matches_query(t, q))
        .map(|t| task_summary_json(t, records, q))
        .collect();

    let from = ms_to_rfc3339(since_ms);
    let to = ms_to_rfc3339(until_ms);

    json!({
        "count": out_records.len(),
        "records": out_records,
        "basic_info": basic_info,
        "tasks": filtered_tasks,
        "from": from,
        "to": to,
    })
}

fn ms_to_rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339())
}

pub fn parse_ping_hours(params: Option<&Value>) -> i64 {
    params
        .and_then(|p| p.get("hours"))
        .and_then(|h| h.as_str().and_then(|s| s.parse().ok()).or_else(|| h.as_i64()))
        .unwrap_or(1)
}

pub fn parse_ping_max_count(params: Option<&Value>) -> i32 {
    let max = params
        .and_then(|p| p.get("maxCount"))
        .and_then(|c| c.as_i64())
        .unwrap_or(4000);
    if max == -1 {
        50_000
    } else {
        max.clamp(1, 50_000) as i32
    }
}

/// Positive id filters one task; `0`, `-1`, or omitted means all tasks (theme RPC convention).
pub fn parse_task_id(params: Option<&Value>) -> Option<i64> {
    let raw = params
        .and_then(|p| p.get("task_id"))
        .and_then(|t| {
            t.as_i64()
                .or_else(|| t.as_str().and_then(|s| s.parse().ok()))
        });
    match raw {
        None | Some(0) | Some(-1) => None,
        Some(id) if id > 0 => Some(id),
        _ => None,
    }
}

/// Per-node ping stats for `common:getNodesLatestStatus` (`ping` map keyed by task id).
pub async fn ping_stats_for_node(pool: &SqlitePool, node_uuid: &str) -> Result<Value, String> {
    let repo = PingRepository::new(pool);
    let tasks = repo.list_all().await.map_err(|e| e.to_string())?;
    let assigned: Vec<_> = tasks
        .iter()
        .filter(|t| task_applies_to_client(t, node_uuid))
        .collect();
    if assigned.is_empty() {
        return Ok(json!({}));
    }
    let until_ms = chrono::Utc::now().timestamp_millis();
    let since_ms = until_ms - 3_600_000;
    let records = repo
        .list_records(Some(node_uuid), None, since_ms, until_ms, 10_000)
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Map::new();
    for task in assigned {
        let subset: Vec<&PingRecordRow> = records
            .iter()
            .filter(|r| r.task_id == task.id)
            .collect();
        if subset.is_empty() {
            continue;
        }
        let stat = latency_stats_from_records(&subset);
        out.insert(
            task.id.to_string(),
            json!({
                "name": task.name,
                "latest": stat.latest,
                "avg": stat.avg,
                "tail": stat.tail,
                "loss": stat.loss_rate,
                "min": stat.min,
                "max": stat.max,
            }),
        );
    }
    Ok(Value::Object(out))
}

fn uuid_matches(stored: &str, want: &str) -> bool {
    stored.eq_ignore_ascii_case(want)
}

fn task_applies_to_client(task: &PingTaskRow, uuid: &str) -> bool {
    task.clients.iter().any(|c| uuid_matches(c, uuid))
}

fn task_matches_query(task: &PingTaskRow, q: &PingQuery) -> bool {
    if let Some(tid) = q.task_id {
        if task.id != tid {
            return false;
        }
    }
    if let Some(ref uuid) = q.uuid {
        return task_applies_to_client(task, uuid);
    }
    true
}

struct LatencyStats {
    latest: i32,
    avg: i32,
    min: i32,
    max: i32,
    loss_rate: f64,
    tail: f64,
    p50: i32,
    p99: i32,
}

fn latency_stats(values: impl Iterator<Item = i32>) -> LatencyStats {
    let mut total = 0i32;
    let mut loss_count = 0i32;
    let mut sum = 0i64;
    let mut valid = 0i32;
    let mut min = 0i32;
    let mut max = 0i32;
    let mut latencies: Vec<i32> = Vec::new();

    for value in values {
        total += 1;
        if value < 0 {
            loss_count += 1;
            continue;
        }
        valid += 1;
        sum += value as i64;
        latencies.push(value);
        if min == 0 || value < min {
            min = value;
        }
        if value > max {
            max = value;
        }
    }

    let avg = if valid > 0 {
        (sum / valid as i64) as i32
    } else {
        0
    };
    let loss_rate = if total > 0 {
        (loss_count as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    latencies.sort_unstable();
    let p50 = percentile_int(&latencies, 0.5);
    let p99 = percentile_int(&latencies, 0.99);
    let tail = if p50 > 0 && p99 >= p50 {
        (p99 - p50) as f64 / p50 as f64
    } else {
        0.0
    };

    LatencyStats {
        latest: -1,
        avg,
        min,
        max,
        loss_rate,
        tail,
        p50,
        p99,
    }
}

fn latency_stats_from_records(records: &[&PingRecordRow]) -> LatencyStats {
    let mut stat = latency_stats(records.iter().map(|r| r.value));
    for r in records.iter().rev() {
        if r.value >= 0 {
            stat.latest = r.value;
            break;
        }
    }
    stat
}

fn percentile_int(values: &[i32], pct: f64) -> i32 {
    if values.is_empty() {
        return 0;
    }
    if pct <= 0.0 {
        return values[0];
    }
    if pct >= 1.0 {
        return *values.last().unwrap();
    }
    let pos = (values.len() - 1) as f64 * pct;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        return values[lo];
    }
    let frac = pos - lo as f64;
    let v = values[lo] as f64 + (values[hi] - values[lo]) as f64 * frac;
    v.round() as i32
}

fn task_summary_json(task: &PingTaskRow, records: &[PingRecordRow], q: &PingQuery) -> Value {
    let subset: Vec<&PingRecordRow> = records
        .iter()
        .filter(|r| r.task_id == task.id)
        .filter(|r| {
            if let Some(ref uuid) = q.uuid {
                return uuid_matches(&r.client, uuid);
            }
            true
        })
        .collect();

    let total = subset.len() as i32;
    let stat = latency_stats_from_records(&subset);
    let mut info = json!({
        "id": task.id,
        "name": task.name,
        "type": task.task_type,
        "interval": task.interval,
        "default_on": task.default_on,
        "loss": stat.loss_rate,
        "min": stat.min,
        "max": stat.max,
        "avg": stat.avg,
        "latest": stat.latest,
        "total": total,
        "p50": stat.p50,
        "p99": stat.p99,
        "p99_p50_ratio": stat.tail,
    });
    if q.uuid.is_none() && q.task_id.is_some() {
        if let Some(obj) = info.as_object_mut() {
            obj.insert("clients".into(), json!(task.clients));
        }
    }
    info
}
