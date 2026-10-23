//! Admin dashboard snapshots: pre-aggregated reads for large fleets.
//! 管理仪表盘快照：大集群下只读聚合结果，避免扫描全部历史。

use crate::database_api::MetricRuntime;
use axum::{extract::Query, http::StatusCode, Json};
use sakuio_db::DbPool;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RANGES: &[(&str, i64, i64)] = &[
    ("24h", 24 * 3600, 300),
    ("7d", 7 * 86400, 3600),
    ("30d", 30 * 86400, 3600),
    ("90d", 90 * 86400, 86400),
    ("365d", 365 * 86400, 86400),
];

const METRICS: &str = "'cpu.usage','memory.used','disk.used','net.in.rate','net.out.rate','traffic.up','traffic.down'";

#[derive(Clone)]
pub struct DashboardService {
    pool: DbPool,
    metric: Arc<MetricRuntime>,
    cache: Arc<tokio::sync::RwLock<HashMap<String, (std::time::Instant, Value)>>>,
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Deserialize)]
pub struct BootstrapQuery {
    pub range: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Deserialize)]
pub struct LayoutBody {
    pub layout: Value,
}

#[derive(Debug, Clone, Default)]
struct NodeAgg {
    cpu_sum: f64,
    cpu_count: i64,
    cpu_peak: f64,
    cpu_peak_at: i64,
    mem_sum: f64,
    mem_count: i64,
    mem_peak: f64,
    mem_peak_at: i64,
    disk_sum: f64,
    disk_count: i64,
    traffic_up: f64,
    traffic_down: f64,
    peak_rate: f64,
    peak_rate_at: i64,
    sample_count: i64,
}

#[derive(Debug, Clone)]
struct NodeMeta {
    name: String,
    online: bool,
    expired_at: Option<String>,
    cpu_cores: i64,
    mem_total: i64,
    disk_total: i64,
}

impl DashboardService {
    pub fn new(pool: DbPool, metric: Arc<MetricRuntime>) -> Self {
        Self {
            pool,
            metric,
            cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            refresh_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Spawns the snapshot refresher. API requests never wait for this task.
    pub fn spawn(self: &Arc<Self>) {
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            loop {
                for code in ["24h", "7d", "30d"] {
                    if let Err(error) = service.refresh(code).await {
                        tracing::warn!(range = code, error = %error, "dashboard snapshot refresh failed");
                    }
                }
                tokio::time::sleep(Duration::from_secs(300)).await;
            }
        });
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(8)).await;
            loop {
                for code in ["90d", "365d"] {
                    if let Err(error) = service.refresh(code).await {
                        tracing::warn!(range = code, error = %error, "dashboard long-range snapshot failed");
                    }
                }
                let now = now_ms();
                let _ = sqlx::query("DELETE FROM ping_records WHERE observed_at_ms < ?")
                    .bind(now - 48 * 3_600_000)
                    .execute(&service.pool)
                    .await;
                let _ = sqlx::query("DELETE FROM ping_rollup_hourly WHERE hour_start_ms < ?")
                    .bind(now - 90 * 86_400_000)
                    .execute(&service.pool)
                    .await;
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
    }

    pub async fn bootstrap(&self, range: &str) -> Result<Value, String> {
        let range = normalize_range(range);
        self.bootstrap_code(range, range).await
    }

    pub async fn bootstrap_custom(&self, since: i64, until: i64) -> Result<Value, String> {
        let code = format!("custom-{since}-{until}");
        let meta: Option<(i64,)> =
            sqlx::query_as("SELECT generated_at FROM dashboard_meta WHERE range_code = ?")
                .bind(&code)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| e.to_string())?;
        let stale = meta
            .map(|(generated_at,)| now_ms().saturating_sub(generated_at) > 300_000)
            .unwrap_or(true);
        if stale {
            self.refresh_custom(&code, since, until).await?;
            let _ = sqlx::query(
                "DELETE FROM dashboard_fleet_points WHERE range_code LIKE 'custom-%' AND range_code <> ?",
            )
            .bind(&code)
            .execute(&self.pool)
            .await;
            let _ = sqlx::query(
                "DELETE FROM dashboard_node_range WHERE range_code LIKE 'custom-%' AND range_code <> ?",
            )
            .bind(&code)
            .execute(&self.pool)
            .await;
            let _ = sqlx::query(
                "DELETE FROM dashboard_ping_range WHERE range_code LIKE 'custom-%' AND range_code <> ?",
            )
            .bind(&code)
            .execute(&self.pool)
            .await;
            let _ = sqlx::query(
                "DELETE FROM dashboard_meta WHERE range_code LIKE 'custom-%' AND range_code <> ?",
            )
            .bind(&code)
            .execute(&self.pool)
            .await;
        }
        self.bootstrap_code(&code, "custom").await
    }

    async fn bootstrap_code(&self, range: &str, display_range: &str) -> Result<Value, String> {
        if let Some((at, value)) = self.cache.read().await.get(range) {
            if at.elapsed() < Duration::from_secs(5) {
                return Ok(value.clone());
            }
        }
        let meta: Option<(i64, i64, i64, String)> = sqlx::query_as(
            "SELECT revision, generated_at, coverage_start, status FROM dashboard_meta WHERE range_code = ?",
        )
        .bind(range)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        if meta.is_none() && display_range != "custom" {
            let service = self.clone();
            let code = range.to_string();
            tokio::spawn(async move {
                let _ = service.refresh(&code).await;
            });
        }
        let (revision, generated_at, _coverage_start, status) =
            meta.unwrap_or((0, 0, 0, "warming".to_string()));
        let fleet: Vec<(i64, f64, f64, f64, f64, f64, f64, f64, i64, i64)> = sqlx::query_as(
            "SELECT bucket_start_ms, up_rate, down_rate, up_cum, down_cum, cpu_avg, mem_used, mem_total, online_nodes, sample_count FROM dashboard_fleet_points WHERE range_code = ? ORDER BY bucket_start_ms",
        )
        .bind(range)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        let nodes: Vec<(String, f64, f64, i64, f64, f64, i64, f64, f64, f64, f64, f64, i64, i64)> = sqlx::query_as(
            "SELECT node_uuid, cpu_avg, cpu_peak, cpu_peak_at, mem_avg, mem_peak, mem_peak_at, mem_total, disk_avg, traffic_up, traffic_down, peak_rate, peak_rate_at, sample_count FROM dashboard_node_range WHERE range_code = ?",
        )
        .bind(range)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        let status = if fleet.is_empty() && status == "ready" {
            "warming".to_string()
        } else {
            status
        };
        let meta_rows = load_clients(&self.pool).await.map_err(|e| e.to_string())?;
        let meta_map: HashMap<String, NodeMeta> = meta_rows;
        let online = meta_map.values().filter(|node| node.online).count();
        let total = meta_map.len();
        let offline = total.saturating_sub(online);
        let now = now_ms();
        let expiry: Vec<Value> = meta_map
            .iter()
            .filter_map(|(uuid, node)| {
                let raw = node.expired_at.as_deref()?;
                let ts = chrono::DateTime::parse_from_rfc3339(raw)
                    .ok()?
                    .timestamp_millis();
                if ts < now || ts > now + 7 * 86400_000 {
                    return None;
                }
                Some(json!({
                    "uuid": uuid,
                    "name": node.name,
                    "expired_at": raw,
                    "days_left": ((ts - now) as f64 / 86_400_000.0).ceil(),
                }))
            })
            .collect();

        let mut traffic_points = Vec::with_capacity(fleet.len());
        for (time, up_rate, down_rate, up_cum, down_cum, cpu, mem, _mem_total, _online, _samples) in
            fleet
        {
            traffic_points.push(json!({
                "time": time,
                "up_rate": up_rate,
                "down_rate": down_rate,
                "up_cum": up_cum,
                "down_cum": down_cum,
                "cpu": cpu,
                "memory": mem,
            }));
        }
        let mut top_traffic = Vec::new();
        let mut top_cpu = Vec::new();
        let mut top_mem = Vec::new();
        let mut disk_pressure = Vec::new();
        for (
            uuid,
            cpu_avg,
            cpu_peak,
            cpu_at,
            mem_avg,
            mem_peak,
            mem_at,
            mem_total,
            disk_avg,
            up,
            down,
            peak_rate,
            peak_at,
            _samples,
        ) in nodes
        {
            let Some(meta) = meta_map.get(&uuid) else {
                continue;
            };
            let mem_pct = if mem_total > 0.0 {
                mem_avg / mem_total * 100.0
            } else {
                0.0
            };
            let mem_peak_pct = if mem_total > 0.0 {
                mem_peak / mem_total * 100.0
            } else {
                0.0
            };
            top_traffic.push(json!({
                "uuid": uuid, "name": meta.name, "up": up, "down": down,
                "total": up + down, "peak_rate": peak_rate, "peak_time": peak_at
            }));
            top_cpu.push(json!({
                "uuid": uuid, "name": meta.name, "value": cpu_avg, "peak": cpu_peak,
                "peak_time": cpu_at
            }));
            top_mem.push(json!({
                "uuid": uuid, "name": meta.name, "value": mem_pct, "peak": mem_peak_pct,
                "peak_time": mem_at
            }));
            if meta.disk_total > 0 {
                let pct = disk_avg / meta.disk_total as f64 * 100.0;
                disk_pressure.push(json!({
                    "uuid": uuid, "name": meta.name, "percent": pct,
                    "used": disk_avg, "total": meta.disk_total
                }));
            }
        }
        sort_by_number(&mut top_traffic, "total");
        sort_by_number(&mut top_cpu, "value");
        sort_by_number(&mut top_mem, "value");
        sort_by_number(&mut disk_pressure, "percent");

        let capacity = meta_map.values().fold((0i64, 0i64, 0i64), |acc, node| {
            (
                acc.0 + node.cpu_cores,
                acc.1 + node.mem_total,
                acc.2 + node.disk_total,
            )
        });
        let main_bytes = main_db_size(&self.pool).await;
        let metric_bytes = self
            .metric
            .current_store()
            .await
            .size_bytes()
            .await
            .unwrap_or(0);
        let ping = self
            .ping_quality(range, &meta_map)
            .await
            .unwrap_or_else(|_| {
                json!({
                    "stable": [], "unstable": [], "loss": []
                })
            });

        let payload = json!({
            "success": true,
            "data": {
                "range": display_range,
                "revision": revision,
                "generated_at": generated_at,
                "status": status,
                "overview": {
                    "online": online,
                    "offline": offline,
                    "total": total,
                    "online_percent": if total > 0 { online as f64 / total as f64 * 100.0 } else { 0.0 }
                },
                "database": {
                    "main_bytes": main_bytes,
                    "metrics_bytes": metric_bytes,
                    "total_bytes": main_bytes.saturating_add(metric_bytes)
                },
                "expiry": expiry,
                "traffic": {
                    "points": traffic_points,
                    "total_up": traffic_points.last().and_then(|p| p.get("up_cum")).and_then(Value::as_f64).unwrap_or(0.0),
                    "total_down": traffic_points.last().and_then(|p| p.get("down_cum")).and_then(Value::as_f64).unwrap_or(0.0),
                    "top": top_traffic
                },
                "rankings": { "cpu": top_cpu, "memory": top_mem },
                "capacity": {
                    "nodes": total,
                    "cpu_cores": capacity.0,
                    "memory_total": capacity.1,
                    "disk_total": capacity.2
                },
                "disk_pressure": disk_pressure,
                "ping_quality": ping
            }
        });
        self.cache.write().await.insert(
            range.to_string(),
            (std::time::Instant::now(), payload.clone()),
        );
        Ok(payload)
    }

    async fn ping_quality(
        &self,
        range: &str,
        nodes: &HashMap<String, NodeMeta>,
    ) -> Result<Value, String> {
        let rows: Vec<(i64, String, f64, f64, f64, f64, f64, i64, i64)> = sqlx::query_as(
            "SELECT task_id, client_uuid, loss, p95_ms, volatility, p50_ms, p99_ms, samples, valid FROM dashboard_ping_range WHERE range_code = ? LIMIT 50000",
        )
        .bind(range)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        let tasks: HashMap<i64, String> =
            sqlx::query_as::<_, (i64, String)>("SELECT id, name FROM ping_tasks")
                .fetch_all(&self.pool)
                .await
                .unwrap_or_default()
                .into_iter()
                .collect();
        let mut stable = Vec::new();
        let mut unstable = Vec::new();
        let mut loss = Vec::new();
        for (task_id, client, loss_pct, p95, volatility, p50, p99, samples, valid) in rows {
            let item = json!({
                "task_id": task_id,
                "task_name": tasks.get(&task_id).cloned().unwrap_or_default(),
                "uuid": client,
                "name": nodes.get(&client).map(|node| node.name.clone()).unwrap_or_default(),
                "p50": p50,
                "p95": p95,
                "p99": p99,
                "loss": loss_pct,
                "volatility": volatility,
                "samples": samples,
                "valid": valid
            });
            if p95 > 0.0 {
                stable.push(item.clone());
                unstable.push(item.clone());
            }
            loss.push(item);
        }
        sort_by_number(&mut stable, "volatility");
        sort_by_number_desc(&mut unstable, "volatility");
        sort_by_number_desc(&mut loss, "loss");
        stable.truncate(5);
        unstable.truncate(5);
        loss.truncate(5);
        Ok(json!({ "stable": stable, "unstable": unstable, "loss": loss }))
    }

    pub async fn refresh(&self, range: &str) -> Result<(), String> {
        let range = normalize_range(range);
        let Some((_, seconds, bucket_seconds)) = RANGES.iter().find(|r| r.0 == range) else {
            return Ok(());
        };
        let until = now_ms();
        let since = until - seconds * 1000;
        self.refresh_between(range, since, until, *bucket_seconds)
            .await
    }

    pub async fn refresh_custom(&self, code: &str, since: i64, until: i64) -> Result<(), String> {
        let duration = until.saturating_sub(since);
        let bucket_seconds = if duration <= 3 * 86_400_000 {
            300
        } else if duration <= 31 * 86_400_000 {
            3600
        } else {
            86_400
        };
        self.refresh_between(code, since, until, bucket_seconds)
            .await
    }

    async fn refresh_between(
        &self,
        range: &str,
        since: i64,
        until: i64,
        bucket_seconds: i64,
    ) -> Result<(), String> {
        let _refresh_guard = self.refresh_lock.lock().await;
        self.cache.write().await.remove(range);
        let store = self.metric.current_store().await;
        let table = format!("{}buckets", store.prefix());
        let samples_expr = cast_int_sum("SUM(sample_count)", store.dialect());
        let fleet_sql = format!(
            "SELECT bucket_start_ms, metric_key, SUM(avg_val * sample_count) AS wsum, \
             {samples_expr} AS samples, SUM(avg_val) AS sum_val, MAX(max_val) AS max_val \
             FROM {table} WHERE bucket_seconds = ? AND bucket_start_ms >= ? AND bucket_start_ms <= ? \
             AND metric_key IN ({METRICS}) GROUP BY bucket_start_ms, metric_key"
        );
        let fleet_rows = sqlx::query(&fleet_sql)
            .bind(bucket_seconds)
            .bind(since)
            .bind(until)
            .fetch_all(store.pool())
            .await
            .map_err(|e| e.to_string())?;
        let mut by_time: HashMap<i64, HashMap<String, (f64, f64, f64, i64)>> = HashMap::new();
        for row in fleet_rows {
            let time: i64 = row.try_get("bucket_start_ms").unwrap_or(0);
            let key: String = row.try_get("metric_key").unwrap_or_default();
            let wsum: f64 = row.try_get("wsum").unwrap_or(0.0);
            let samples: i64 = row.try_get("samples").unwrap_or(0);
            let sum: f64 = row.try_get("sum_val").unwrap_or(0.0);
            let max: f64 = row.try_get("max_val").unwrap_or(0.0);
            by_time
                .entry(time)
                .or_default()
                .insert(key, (wsum, sum, max, samples));
        }
        let node_sql = format!(
            "SELECT node_uuid, metric_key, SUM(avg_val * sample_count) AS wsum, \
             SUM(avg_val) AS sum_val, {samples_expr} AS samples, MAX(max_val) AS max_val \
             FROM {table} WHERE bucket_seconds = ? AND bucket_start_ms >= ? AND bucket_start_ms <= ? \
             AND metric_key IN ({METRICS}) GROUP BY node_uuid, metric_key"
        );
        let node_rows = sqlx::query(&node_sql)
            .bind(bucket_seconds)
            .bind(since)
            .bind(until)
            .fetch_all(store.pool())
            .await
            .map_err(|e| e.to_string())?;
        let peak_sql = format!(
            "SELECT node_uuid, metric_key, bucket_start_ms, max_val FROM (\
             SELECT node_uuid, metric_key, bucket_start_ms, max_val, \
             ROW_NUMBER() OVER (PARTITION BY node_uuid, metric_key ORDER BY max_val DESC, bucket_start_ms ASC) AS rn \
             FROM {table} WHERE bucket_seconds = ? AND bucket_start_ms >= ? AND bucket_start_ms <= ? \
             AND metric_key IN ('cpu.usage','memory.used','net.in.rate','net.out.rate')\
             ) ranked WHERE rn = 1"
        );
        let peak_rows = sqlx::query(&peak_sql)
            .bind(bucket_seconds)
            .bind(since)
            .bind(until)
            .fetch_all(store.pool())
            .await
            .unwrap_or_default();
        let mut peak_times: HashMap<(String, String), i64> = HashMap::new();
        for row in peak_rows {
            let uuid: String = row.try_get("node_uuid").unwrap_or_default();
            let key: String = row.try_get("metric_key").unwrap_or_default();
            let time: i64 = row.try_get("bucket_start_ms").unwrap_or(0);
            peak_times.insert((uuid, key), time);
        }
        let clients = load_clients(&self.pool).await.map_err(|e| e.to_string())?;
        let online = clients.values().filter(|node| node.online).count() as i64;
        let meta_map = clients;
        let mut nodes: HashMap<String, NodeAgg> = HashMap::new();
        for row in node_rows {
            let uuid: String = row.try_get("node_uuid").unwrap_or_default();
            let key: String = row.try_get("metric_key").unwrap_or_default();
            let wsum: f64 = row.try_get("wsum").unwrap_or(0.0);
            let sum_val: f64 = row.try_get("sum_val").unwrap_or(0.0);
            let samples: i64 = row.try_get("samples").unwrap_or(0);
            let max: f64 = row.try_get("max_val").unwrap_or(0.0);
            let peak_at = peak_times
                .get(&(uuid.clone(), key.clone()))
                .copied()
                .unwrap_or(0);
            let entry = nodes.entry(uuid).or_default();
            match key.as_str() {
                "cpu.usage" => {
                    entry.cpu_sum += wsum;
                    entry.cpu_count += samples;
                    if max >= entry.cpu_peak {
                        entry.cpu_peak = max;
                        entry.cpu_peak_at = peak_at;
                    }
                }
                "memory.used" => {
                    entry.mem_sum += wsum;
                    entry.mem_count += samples;
                    if max >= entry.mem_peak {
                        entry.mem_peak = max;
                        entry.mem_peak_at = peak_at;
                    }
                }
                "disk.used" => {
                    entry.disk_sum += wsum;
                    entry.disk_count += samples;
                }
                "traffic.up" => entry.traffic_up += sum_val,
                "traffic.down" => entry.traffic_down += sum_val,
                "net.in.rate" | "net.out.rate" => {
                    if max >= entry.peak_rate {
                        entry.peak_rate = max;
                        entry.peak_rate_at = peak_at;
                    }
                }
                _ => {}
            }
            entry.sample_count += samples;
        }

        // Snapshot rows are cheap to replace; only one writer is scheduled.
        sqlx::query("DELETE FROM dashboard_fleet_points WHERE range_code = ?")
            .bind(range)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM dashboard_node_range WHERE range_code = ?")
            .bind(range)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        let mut times: Vec<i64> = by_time.keys().copied().collect();
        times.sort_unstable();
        let mut cumulative_up = 0.0;
        let mut cumulative_down = 0.0;
        for time in times {
            let values = &by_time[&time];
            let cpu = values
                .get("cpu.usage")
                .map(|(w, _, _, n)| if *n > 0 { w / *n as f64 } else { 0.0 })
                .unwrap_or(0.0);
            let mem = values
                .get("memory.used")
                .map(|(_, s, _, _)| *s)
                .unwrap_or(0.0);
            let mem_total = meta_map.values().map(|m| m.mem_total as f64).sum::<f64>();
            let up_rate = values
                .get("net.out.rate")
                .map(|(_, s, _, _)| *s)
                .unwrap_or(0.0);
            let down_rate = values
                .get("net.in.rate")
                .map(|(_, s, _, _)| *s)
                .unwrap_or(0.0);
            cumulative_up += values
                .get("traffic.up")
                .map(|(_, s, _, _)| *s)
                .unwrap_or(0.0);
            cumulative_down += values
                .get("traffic.down")
                .map(|(_, s, _, _)| *s)
                .unwrap_or(0.0);
            let up_cum = cumulative_up;
            let down_cum = cumulative_down;
            sqlx::query(
                "INSERT INTO dashboard_fleet_points (range_code, bucket_start_ms, up_rate, down_rate, up_cum, down_cum, cpu_avg, mem_used, mem_total, online_nodes, sample_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(range)
            .bind(time)
            .bind(up_rate)
            .bind(down_rate)
            .bind(up_cum)
            .bind(down_cum)
            .bind(cpu)
            .bind(mem)
            .bind(mem_total)
            .bind(online)
            .bind(values.values().map(|(_, _, _, n)| *n).sum::<i64>())
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        for (uuid, agg) in &nodes {
            let meta = meta_map.get(uuid);
            let cpu_avg = if agg.cpu_count > 0 {
                agg.cpu_sum / agg.cpu_count as f64
            } else {
                0.0
            };
            let mem_avg = if agg.mem_count > 0 {
                agg.mem_sum / agg.mem_count as f64
            } else {
                0.0
            };
            let disk_avg = if agg.disk_count > 0 {
                agg.disk_sum / agg.disk_count as f64
            } else {
                0.0
            };
            let mem_total = meta.map(|m| m.mem_total as f64).unwrap_or(0.0);
            sqlx::query(
                "INSERT INTO dashboard_node_range (range_code, node_uuid, cpu_avg, cpu_peak, cpu_peak_at, mem_avg, mem_peak, mem_peak_at, mem_total, disk_avg, traffic_up, traffic_down, peak_rate, peak_rate_at, sample_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(range)
            .bind(uuid)
            .bind(cpu_avg)
            .bind(agg.cpu_peak)
            .bind(agg.cpu_peak_at)
            .bind(mem_avg)
            .bind(agg.mem_peak)
            .bind(agg.mem_peak_at)
            .bind(mem_total)
            .bind(disk_avg)
            .bind(agg.traffic_up)
            .bind(agg.traffic_down)
            .bind(agg.peak_rate)
            .bind(agg.peak_rate_at)
            .bind(agg.sample_count)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        if let Err(error) = self.refresh_ping(range, since, until).await {
            tracing::warn!(range, error = %error, "dashboard ping snapshot failed");
        }
        let revision: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(revision), 0) FROM dashboard_meta WHERE range_code = ?",
        )
        .bind(range)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);
        let sql = if self.pool.kind() == sakuio_db::DbKind::MySql {
            "INSERT INTO dashboard_meta (range_code, revision, generated_at, coverage_start, status) VALUES (?, ?, ?, ?, 'ready') ON DUPLICATE KEY UPDATE revision = revision + 1, generated_at = VALUES(generated_at), coverage_start = VALUES(coverage_start), status = 'ready'"
        } else {
            "INSERT INTO dashboard_meta (range_code, revision, generated_at, coverage_start, status) VALUES (?, ?, ?, ?, 'ready') ON CONFLICT (range_code) DO UPDATE SET revision = dashboard_meta.revision + 1, generated_at = excluded.generated_at, coverage_start = excluded.coverage_start, status = 'ready'"
        };
        sqlx::query(sql)
            .bind(range)
            .bind(revision + 1)
            .bind(now_ms())
            .bind(since)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

async fn resolve_dashboard(
    state: &crate::AppState,
    query: &BootstrapQuery,
) -> Result<Value, (StatusCode, String)> {
    if let (Some(start), Some(end)) = (query.start.as_deref(), query.end.as_deref()) {
        let since = parse_bound(start, false)
            .ok_or((StatusCode::BAD_REQUEST, "invalid start time".to_string()))?;
        let until = parse_bound(end, true)
            .ok_or((StatusCode::BAD_REQUEST, "invalid end time".to_string()))?;
        if until <= since {
            return Err((StatusCode::BAD_REQUEST, "end must be after start".into()));
        }
        return state
            .dashboard
            .bootstrap_custom(since, until)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e));
    }
    let range = normalize_range(query.range.as_deref().unwrap_or("24h"));
    state
        .dashboard
        .bootstrap(range)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))
}

pub async fn bootstrap(
    state: axum::extract::State<crate::AppState>,
    _user: crate::auth::AdminUser,
    Query(query): Query<BootstrapQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    resolve_dashboard(&state, &query).await.map(Json)
}

fn parse_bound(value: &str, end_of_day: bool) -> Option<i64> {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(value) {
        return Some(parsed.timestamp_millis());
    }
    let date = chrono::NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d").ok()?;
    let time = if end_of_day {
        chrono::NaiveTime::from_hms_milli_opt(23, 59, 59, 999)?
    } else {
        chrono::NaiveTime::from_hms_opt(0, 0, 0)?
    };
    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        chrono::NaiveDateTime::new(date, time),
        chrono::Utc,
    )
    .timestamp_millis()
    .into()
}

pub async fn stream(
    state: axum::extract::State<crate::AppState>,
    _user: crate::auth::AdminUser,
) -> axum::response::Response {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use axum::response::IntoResponse;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, std::convert::Infallible>>(8);
    let pool = state.db.clone();
    tokio::spawn(async move {
        loop {
            let online: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM clients WHERE disabled = 0 AND online = 1",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or(0);
            let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM clients WHERE disabled = 0")
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
            let revision: i64 =
                sqlx::query_scalar("SELECT COALESCE(MAX(revision), 0) FROM dashboard_meta")
                    .fetch_one(&pool)
                    .await
                    .unwrap_or(0);
            let event = Event::default()
                .event("live")
                .json_data(json!({
                    "online": online, "total": total, "revision": revision
                }))
                .unwrap_or_else(|_| Event::default().event("error").data("{}"));
            if tx.send(Ok(event)).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

pub async fn component(
    state: axum::extract::State<crate::AppState>,
    _user: crate::auth::AdminUser,
    axum::extract::Path(component): axum::extract::Path<String>,
    Query(query): Query<BootstrapQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let value = resolve_dashboard(&state, &query).await?;
    let data = value.get("data").cloned().unwrap_or_else(|| json!({}));
    let part = match component.as_str() {
        "traffic" => data.get("traffic").cloned(),
        "rankings" => data.get("rankings").cloned(),
        "ping-quality" => data.get("ping_quality").cloned(),
        "capacity" => data.get("capacity").cloned(),
        "database" => data.get("database").cloned(),
        "expiry" => data.get("expiry").cloned(),
        "overview" => data.get("overview").cloned(),
        _ => None,
    };
    let Some(part) = part else {
        return Err((StatusCode::NOT_FOUND, "unknown dashboard component".into()));
    };
    Ok(Json(json!({ "success": true, "data": part })))
}

pub async fn get_layout(
    state: axum::extract::State<crate::AppState>,
    user: crate::auth::AdminUser,
) -> Result<Json<Value>, StatusCode> {
    let (layout,): (String,) =
        sqlx::query_as("SELECT layout_json FROM admin_dashboard_layouts WHERE admin_id = ?")
            .bind(&user.id)
            .fetch_optional(&state.db)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .unwrap_or_else(|| ("[]".to_string(),));
    Ok(Json(
        json!({ "success": true, "layout": serde_json::from_str::<Value>(&layout).unwrap_or(json!([])) }),
    ))
}

pub async fn put_layout(
    state: axum::extract::State<crate::AppState>,
    user: crate::auth::AdminUser,
    Json(body): Json<LayoutBody>,
) -> Result<Json<Value>, (StatusCode, &'static str)> {
    let Some(items) = body.layout.as_array() else {
        return Err((StatusCode::BAD_REQUEST, "layout must be an array"));
    };
    let allowed = [
        "overview",
        "database",
        "expiry",
        "traffic",
        "traffic_top",
        "cpu",
        "memory",
        "ping",
        "resources",
        "disk",
        "shortcuts",
    ];
    let mut seen = std::collections::HashSet::new();
    let mut normalized = Vec::new();
    for item in items.iter().take(12) {
        let id = item.get("id").and_then(Value::as_str).unwrap_or("");
        if !allowed.contains(&id) || !seen.insert(id.to_string()) {
            continue;
        }
        let width = item
            .get("width")
            .and_then(Value::as_str)
            .unwrap_or("compact");
        let height = item
            .get("height")
            .and_then(Value::as_str)
            .unwrap_or("short");
        let limit = item.get("limit").and_then(Value::as_i64).unwrap_or(5);
        normalized.push(json!({
            "id": id,
            "width": if ["compact","wide","full"].contains(&width) { width } else { "compact" },
            "height": if ["short","normal","tall"].contains(&height) { height } else { "short" },
            "limit": if [2,3,4,5,8,10].contains(&limit) { limit } else { 5 },
        }));
    }
    let raw = serde_json::to_string(&normalized)
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid layout"))?;
    let sql = if state.db.kind() == sakuio_db::DbKind::MySql {
        "INSERT INTO admin_dashboard_layouts (admin_id, layout_json, updated_at) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE layout_json = VALUES(layout_json), updated_at = VALUES(updated_at)"
    } else {
        "INSERT INTO admin_dashboard_layouts (admin_id, layout_json, updated_at) VALUES (?, ?, ?) ON CONFLICT (admin_id) DO UPDATE SET layout_json = excluded.layout_json, updated_at = excluded.updated_at"
    };
    sqlx::query(sql)
        .bind(&user.id)
        .bind(raw)
        .bind(now_ms())
        .execute(&state.db)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "save layout failed"))?;
    Ok(Json(json!({ "success": true, "layout": normalized })))
}

async fn load_clients(pool: &DbPool) -> Result<HashMap<String, NodeMeta>, sqlx::Error> {
    let rows: Vec<(String, String, i64, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT uuid, name, online, expired_at, basic_info_json FROM clients WHERE disabled = 0",
    )
    .fetch_all(pool)
    .await?;
    let mut out = HashMap::new();
    for (uuid, name, online, expired_at, basic) in rows {
        let parsed: Value = basic
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or(Value::Null);
        out.insert(
            uuid,
            NodeMeta {
                name,
                online: online != 0,
                expired_at,
                cpu_cores: parsed.get("cpu_cores").and_then(Value::as_i64).unwrap_or(0),
                mem_total: parsed.get("mem_total").and_then(Value::as_i64).unwrap_or(0),
                disk_total: parsed
                    .get("disk_total")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            },
        );
    }
    Ok(out)
}

impl DashboardService {
    async fn refresh_ping(&self, range: &str, since: i64, until: i64) -> Result<(), String> {
        let samples_expr = cast_int_sum("SUM(samples)", self.pool.kind());
        let valid_expr = cast_int_sum("SUM(valid)", self.pool.kind());
        let loss_expr = cast_int_sum("SUM(loss_count)", self.pool.kind());
        let ping_sql = format!(
            "SELECT task_id, client_uuid, {samples_expr} AS samples, {valid_expr} AS valid, \
             {loss_expr} AS loss_count, SUM(sum_ms) AS sum_ms, SUM(sum_sq_ms) AS sum_sq_ms, \
             MAX(max_ms) AS max_ms FROM ping_rollup_hourly \
             WHERE hour_start_ms >= ? AND hour_start_ms <= ? GROUP BY task_id, client_uuid"
        );
        let rows: Vec<(i64, String, i64, i64, i64, f64, f64, f64)> = sqlx::query_as(&ping_sql)
            .bind(since)
            .bind(until)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query("DELETE FROM dashboard_ping_range WHERE range_code = ?")
            .bind(range)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        for (task_id, client, samples, valid, loss_count, sum_ms, sum_sq, max_ms) in rows {
            let loss = if samples > 0 {
                loss_count as f64 / samples as f64 * 100.0
            } else {
                0.0
            };
            let avg = if valid > 0 {
                sum_ms / valid as f64
            } else {
                0.0
            };
            let variance = if valid > 0 {
                (sum_sq / valid as f64 - avg * avg).max(0.0)
            } else {
                0.0
            };
            let stddev = variance.sqrt();
            let volatility = if avg > 0.0 { stddev / avg } else { 0.0 };
            let p95 = (avg + stddev * 2.0).min(max_ms);
            let p99 = (avg + stddev * 3.0).min(max_ms);
            sqlx::query(
                "INSERT INTO dashboard_ping_range (range_code, task_id, client_uuid, samples, valid, loss, p50_ms, p95_ms, p99_ms, volatility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(range)
            .bind(task_id)
            .bind(client)
            .bind(samples)
            .bind(valid)
            .bind(loss)
            .bind(avg)
            .bind(p95)
            .bind(p99)
            .bind(volatility)
            .execute(&self.pool)
            .await
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

fn normalize_range(range: &str) -> &'static str {
    match range {
        "7d" => "7d",
        "30d" => "30d",
        "90d" => "90d",
        "365d" => "365d",
        _ => "24h",
    }
}

fn cast_int_sum(expr: &str, kind: sakuio_db::DbKind) -> String {
    match kind {
        sakuio_db::DbKind::Postgres => format!("CAST({expr} AS BIGINT)"),
        sakuio_db::DbKind::MySql => format!("CAST({expr} AS SIGNED)"),
        sakuio_db::DbKind::Sqlite => expr.to_string(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn sort_by_number(items: &mut [Value], key: &str) {
    items.sort_by(|a, b| {
        b.get(key)
            .and_then(Value::as_f64)
            .partial_cmp(&a.get(key).and_then(Value::as_f64))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn sort_by_number_desc(items: &mut [Value], key: &str) {
    items.sort_by(|a, b| {
        a.get(key)
            .and_then(Value::as_f64)
            .partial_cmp(&b.get(key).and_then(Value::as_f64))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

async fn main_db_size(pool: &DbPool) -> u64 {
    if pool.kind() == sakuio_db::DbKind::Sqlite {
        let path = pool
            .dsn()
            .strip_prefix("sqlite://")
            .unwrap_or(pool.dsn())
            .split('?')
            .next()
            .unwrap_or("");
        return std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    }
    let sql = if pool.kind() == sakuio_db::DbKind::MySql {
        "SELECT CAST(COALESCE(SUM(data_length + index_length), 0) AS SIGNED) FROM information_schema.tables WHERE table_schema = DATABASE()"
    } else {
        "SELECT pg_database_size(current_database())"
    };
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .ok()
        .filter(|v| *v > 0)
        .map(|v| v as u64)
        .unwrap_or(0)
}
