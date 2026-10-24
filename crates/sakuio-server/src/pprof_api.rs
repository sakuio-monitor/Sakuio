//! Lightweight runtime profiling data for Rust/Tokio.
//! 面向 Rust/Tokio 的运行时性能分析数据（SSE 实时刷新）。

use crate::AppState;
use axum::{
    extract::State,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use futures_util::stream;
use serde::Serialize;
use serde_json::json;
use sqlx::Row;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
use std::time::Duration;
use sysinfo::{Disks, Pid, ProcessesToUpdate, System};

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeSnapshot {
    pub pid: u32,
    pub tasks: u64,
    pub workers: u64,
    pub queue_depth: u64,
    pub threads: u64,
    pub cpu_percent: f64,
    pub system_cpu_percent: f64,
    pub system_other_cpu_percent: f64,
    pub cpu_idle_percent: f64,
    pub heap_alloc: u64,
    pub heap_inuse: u64,
    pub heap_objects: u64,
    pub runtime_sys: u64,
    pub alloc_bytes_per_sec: f64,
    pub uptime_secs: u64,
    pub status: String,
    pub pressure_percent: f64,
    pub database: DatabaseMetrics,
    pub memory: MemoryBreakdown,
    pub heap_preview: String,
}

static DB_ACTIVE_PEAK: AtomicU64 = AtomicU64::new(0);
static DB_MONITOR: Once = Once::new();

fn ensure_pool_monitor(pool: &sakuio_db::DbPool) {
    let pool = pool.clone();
    DB_MONITOR.call_once(|| {
        tokio::spawn(async move {
            loop {
                let inner = pool.inner();
                let active = inner.size().saturating_sub(inner.num_idle() as u32) as u64;
                DB_ACTIVE_PEAK.fetch_max(active, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
    });
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DatabaseMetrics {
    pub kind: String,
    pub size_bytes: u64,
    pub disk_total: u64,
    pub disk_available: u64,
    pub connections: u64,
    pub active_connections: u64,
    pub peak_connections: u64,
    pub idle_connections: u64,
    pub max_connections: u64,
    pub queries_per_sec: f64,
    pub writes_per_sec: f64,
    pub write_pressure_percent: f64,
    pub audit_logs_bytes: u64,
    pub clients_bytes: u64,
    pub metrics_bytes: u64,
    pub other_tables_bytes: u64,
    pub tables: Vec<TableSize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableSize {
    pub name: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryItem {
    pub key: String,
    pub bytes: u64,
    pub detail: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MemoryBreakdown {
    pub agent_cache: u64,
    pub agent_nodes: u64,
    pub websocket: u64,
    pub websocket_sessions: u64,
    pub database: u64,
    pub database_connections: u64,
    pub other: u64,
    pub thread_stacks: u64,
    pub items: Vec<MemoryItem>,
}

#[derive(Debug, Clone, Serialize)]
struct Summary {
    runtime: RuntimeSnapshot,
}

struct Sampler {
    pid: Pid,
    system: System,
    disks: Disks,
    last_writes: u64,
    last_queries: u64,
    last_alloc_bytes: u64,
    last_alloc_at: std::time::Instant,
    last_sample: std::time::Instant,
    db_size_cache: Option<(std::time::Instant, u64)>,
    map_cache: Option<(std::time::Instant, crate::memory_map::MemoryMapStats)>,
    table_sizes_cache: Option<(std::time::Instant, (u64, u64, u64, u64), Vec<TableSize>)>,
    disk_cache: Option<(std::time::Instant, (u64, u64))>,
}

impl Sampler {
    fn new() -> Self {
        let pid = Pid::from_u32(std::process::id());
        let mut system = System::new();
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        Self {
            pid,
            system,
            disks: Disks::new_with_refreshed_list(),
            last_writes: sakuio_db::DB_WRITES.load(Ordering::Relaxed),
            last_queries: sakuio_db::DB_QUERIES.load(Ordering::Relaxed),
            last_alloc_bytes: crate::alloc_metrics::TOTAL_BYTES.load(Ordering::Relaxed),
            last_alloc_at: std::time::Instant::now(),
            last_sample: std::time::Instant::now(),
            db_size_cache: None,
            map_cache: None,
            table_sizes_cache: None,
            disk_cache: None,
        }
    }

    fn snapshot(&mut self) -> RuntimeSnapshot {
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]), true);
        self.system.refresh_cpu_usage();
        let process = self.system.process(self.pid);
        let threads = process_thread_count()
            .or_else(|| process.and_then(|process| process.tasks().map(|tasks| tasks.len() as u64)))
            .unwrap_or(1);
        let heap_alloc = crate::alloc_metrics::LIVE_BYTES.load(Ordering::Relaxed);
        let heap_inuse = process.map(|process| process.memory()).unwrap_or(0);
        let heap_objects = crate::alloc_metrics::LIVE_OBJECTS.load(Ordering::Relaxed);
        let runtime_sys = crate::alloc_metrics::PEAK_BYTES.load(Ordering::Relaxed);
        let cpu_count = self.system.cpus().len().max(1) as f64;
        let process_cpu_percent = process
            .map(|process| (process.cpu_usage() as f64 / cpu_count).clamp(0.0, 100.0))
            .unwrap_or(0.0);
        let system_cpu_percent = self.system.global_cpu_usage().clamp(0.0, 100.0) as f64;
        let process_cpu_percent = process_cpu_percent.min(system_cpu_percent);
        let system_other_cpu_percent = (system_cpu_percent - process_cpu_percent).max(0.0);
        let cpu_idle_percent = (100.0 - system_cpu_percent).max(0.0);
        let uptime_secs = process.map(|process| process.run_time()).unwrap_or(0);
        let metrics = tokio::runtime::Handle::current().metrics();
        let tasks = metrics.num_alive_tasks() as u64;
        let workers = metrics.num_workers() as u64;
        let queue_depth = metrics.global_queue_depth() as u64;
        let heap_preview = format!(
            "Sakuio runtime snapshot\n\
             pid: {}\n\
             tokio tasks: {}\n\
             tokio workers: {}\n\
             global queue depth: {}\n\
             process threads: {}\n\
             heap alloc: {}\n\
             heap in use (rss): {}\n\
             heap objects: {}\n\
             runtime heap peak: {}\n\
             cpu process: {:.2}%\n\
             cpu system used: {:.2}%\n\
             cpu system other: {:.2}%\n\
             cpu idle: {:.2}%\n\
             process uptime: {}s\n",
            std::process::id(),
            tasks,
            workers,
            queue_depth,
            threads,
            format_bytes(heap_alloc),
            format_bytes(heap_inuse),
            heap_objects,
            format_bytes(runtime_sys),
            process_cpu_percent,
            system_cpu_percent,
            system_other_cpu_percent,
            cpu_idle_percent,
            uptime_secs
        );
        RuntimeSnapshot {
            pid: std::process::id(),
            tasks,
            workers,
            queue_depth,
            threads,
            cpu_percent: process_cpu_percent,
            system_cpu_percent,
            system_other_cpu_percent,
            cpu_idle_percent,
            heap_alloc,
            heap_inuse,
            heap_objects,
            runtime_sys,
            alloc_bytes_per_sec: 0.0,
            uptime_secs,
            status: String::new(),
            pressure_percent: 0.0,
            database: DatabaseMetrics::default(),
            memory: MemoryBreakdown::default(),
            heap_preview,
        }
    }
}

fn process_thread_count() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("Threads:")?.trim();
        value.parse().ok()
    })
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut index = 0;
    while value >= 1024.0 && index < UNITS.len() - 1 {
        value /= 1024.0;
        index += 1;
    }
    if index == 0 {
        format!("{} {}", bytes, UNITS[index])
    } else {
        format!("{value:.2} {}", UNITS[index])
    }
}

async fn summary(state: &AppState, sampler: &mut Sampler) -> Summary {
    let mut runtime = sampler.snapshot();
    let alloc_total = crate::alloc_metrics::TOTAL_BYTES.load(Ordering::Relaxed);
    let alloc_elapsed = sampler.last_alloc_at.elapsed().as_secs_f64().max(0.001);
    runtime.alloc_bytes_per_sec =
        alloc_total.saturating_sub(sampler.last_alloc_bytes) as f64 / alloc_elapsed;
    sampler.last_alloc_bytes = alloc_total;
    sampler.last_alloc_at = std::time::Instant::now();
    let db_metrics = database_metrics(state, sampler).await;
    let board = state.board.memory_stats().await;
    let relay = state.relay.stats().await;
    let db_connections = state.db.inner().size() as u64;
    let agent_cache = board.estimated_bytes;
    let websocket = (board.online + relay.sessions)
        .saturating_mul(64 * 1024)
        .saturating_add(relay.buffered_bytes);
    let database_bytes = db_connections.saturating_mul(256 * 1024);
    let other = runtime.heap_inuse.saturating_sub(
        agent_cache
            .saturating_add(websocket)
            .saturating_add(database_bytes),
    );
    let cpu_pressure = (runtime.system_cpu_percent / 100.0).clamp(0.0, 1.0);
    let queue_pressure = if runtime.workers > 0 {
        (runtime.queue_depth as f64 / (runtime.workers as f64 * 4.0)).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let db_pressure = (db_metrics.write_pressure_percent / 100.0).clamp(0.0, 1.0);
    runtime.pressure_percent =
        (cpu_pressure * 0.55 + queue_pressure * 0.25 + db_pressure * 0.2) * 100.0;
    runtime.status = if runtime.pressure_percent < 60.0 {
        "healthy".into()
    } else if runtime.pressure_percent < 80.0 {
        "busy".into()
    } else {
        "critical".into()
    };
    runtime.database = db_metrics;
    let map = memory_map_for(sampler);
    let (thread_stacks, items) = if map.total() > 0 {
        let scale = if map.total() > runtime.heap_inuse {
            runtime.heap_inuse as f64 / map.total() as f64
        } else {
            1.0
        };
        let scaled = |value: u64| (value as f64 * scale) as u64;
        let app_sum = agent_cache
            .saturating_add(websocket)
            .saturating_add(database_bytes);
        let heap_app = app_sum.min(scaled(map.heap));
        let heap_other = scaled(map.heap).saturating_sub(heap_app);
        let mapped_total = scaled(map.total());
        let residual = runtime.heap_inuse.saturating_sub(mapped_total);
        let anonymous = scaled(map.anonymous).saturating_add(residual);
        let stacks = scaled(map.stacks);
        let code = scaled(map.code);
        let mut items = vec![
            MemoryItem {
                key: "agent_cache".into(),
                bytes: agent_cache,
                detail: format!("{} nodes", board.nodes),
            },
            MemoryItem {
                key: "websocket".into(),
                bytes: websocket,
                detail: format!("{} sessions", board.online + relay.sessions),
            },
            MemoryItem {
                key: "database".into(),
                bytes: database_bytes,
                detail: format!("{} connections", db_connections),
            },
            MemoryItem {
                key: "heap_other".into(),
                bytes: heap_other,
                detail: "app cache excluded".into(),
            },
            MemoryItem {
                key: "stacks".into(),
                bytes: stacks,
                detail: format!("{} threads", runtime.threads),
            },
            MemoryItem {
                key: "code".into(),
                bytes: code,
                detail: "binary and libraries".into(),
            },
            MemoryItem {
                key: "anonymous".into(),
                bytes: anonymous,
                detail: "anonymous mappings".into(),
            },
            MemoryItem {
                key: "file_mappings".into(),
                bytes: scaled(
                    map.files
                        .saturating_add(map.database)
                        .saturating_add(map.kernel),
                ),
                detail: "files, database and kernel".into(),
            },
        ];
        items.retain(|item| item.bytes > 0);
        (stacks, items)
    } else {
        let thread_stacks = (runtime.threads.saturating_mul(2 * 1024 * 1024)).min(other);
        let other = other.saturating_sub(thread_stacks);
        let items = vec![
            MemoryItem {
                key: "agent_cache".into(),
                bytes: agent_cache,
                detail: format!("{} nodes", board.nodes),
            },
            MemoryItem {
                key: "websocket".into(),
                bytes: websocket,
                detail: format!("{} sessions", board.online + relay.sessions),
            },
            MemoryItem {
                key: "database".into(),
                bytes: database_bytes,
                detail: format!("{} connections", db_connections),
            },
            MemoryItem {
                key: "thread_stacks".into(),
                bytes: thread_stacks,
                detail: format!("{} threads x 2 MB", runtime.threads),
            },
            MemoryItem {
                key: "other".into(),
                bytes: other,
                detail: "runtime".into(),
            },
        ];
        (thread_stacks, items)
    };
    runtime.memory = MemoryBreakdown {
        agent_cache,
        agent_nodes: board.nodes,
        websocket,
        websocket_sessions: board.online + relay.sessions,
        database: database_bytes,
        database_connections: db_connections,
        other: 0,
        thread_stacks,
        items,
    };
    runtime.heap_preview.push_str(&format!(
        "agent cache: {} ({} nodes)\n\
         websocket: {} ({} sessions)\n\
         database: {} ({} connections)\n\
         other runtime: {}\n",
        format_bytes(agent_cache),
        board.nodes,
        format_bytes(websocket),
        board.online + relay.sessions,
        format_bytes(database_bytes),
        db_connections,
        format_bytes(other)
    ));
    runtime.heap_preview.push_str(&format!(
        "database: {} ({})\n\
         database writes: {:.2} per sec / {:.2} queries per sec\n\
         status: {} pressure={:.1}%\n",
        format_bytes(runtime.database.size_bytes),
        runtime.database.kind,
        runtime.database.writes_per_sec,
        runtime.database.queries_per_sec,
        runtime.status,
        runtime.pressure_percent
    ));
    Summary { runtime }
}

fn memory_map_for(sampler: &mut Sampler) -> crate::memory_map::MemoryMapStats {
    if let Some((at, stats)) = &sampler.map_cache {
        if at.elapsed() < std::time::Duration::from_secs(15) {
            return stats.clone();
        }
    }
    let stats = crate::memory_map::read().unwrap_or_default();
    sampler.map_cache = Some((std::time::Instant::now(), stats.clone()));
    stats
}

async fn database_metrics(state: &AppState, sampler: &mut Sampler) -> DatabaseMetrics {
    ensure_pool_monitor(&state.db);
    let kind = state.db.kind().as_str().to_string();
    let connections = state.db.inner().size() as u64;
    let idle_connections = state.db.inner().num_idle() as u64;
    let active_connections = connections.saturating_sub(idle_connections);
    let peak_connections = DB_ACTIVE_PEAK
        .swap(0, Ordering::Relaxed)
        .max(active_connections);
    let max_connections = state.db.max_connections() as u64;
    let writes_total = sakuio_db::DB_WRITES.load(Ordering::Relaxed);
    let queries_total = sakuio_db::DB_QUERIES.load(Ordering::Relaxed);
    let elapsed = sampler.last_sample.elapsed().as_secs_f64().max(0.001);
    let writes_per_sec = writes_total.saturating_sub(sampler.last_writes) as f64 / elapsed;
    let queries_per_sec = queries_total.saturating_sub(sampler.last_queries) as f64 / elapsed;
    sampler.last_writes = writes_total;
    sampler.last_queries = queries_total;
    sampler.last_sample = std::time::Instant::now();
    let size_bytes = if let Some((at, size)) = sampler.db_size_cache {
        if at.elapsed() < std::time::Duration::from_secs(15) {
            size
        } else {
            let size = database_size(state).await;
            sampler.db_size_cache = Some((std::time::Instant::now(), size));
            size
        }
    } else {
        let size = database_size(state).await;
        sampler.db_size_cache = Some((std::time::Instant::now(), size));
        size
    };
    let (table_sizes, tables) = if let Some((at, sizes, tables)) = sampler.table_sizes_cache.clone()
    {
        if at.elapsed() < std::time::Duration::from_secs(15) {
            (sizes, tables)
        } else {
            let (sizes, tables) = database_table_sizes(state, size_bytes).await;
            sampler.table_sizes_cache = Some((std::time::Instant::now(), sizes, tables.clone()));
            (sizes, tables)
        }
    } else {
        let (sizes, tables) = database_table_sizes(state, size_bytes).await;
        sampler.table_sizes_cache = Some((std::time::Instant::now(), sizes, tables.clone()));
        (sizes, tables)
    };
    let (disk_total, disk_available) = if let Some((at, usage)) = sampler.disk_cache {
        if at.elapsed() < std::time::Duration::from_secs(15) {
            usage
        } else {
            let usage = disk_usage(sampler);
            sampler.disk_cache = Some((std::time::Instant::now(), usage));
            usage
        }
    } else {
        let usage = disk_usage(sampler);
        sampler.disk_cache = Some((std::time::Instant::now(), usage));
        usage
    };
    let write_pressure_percent = if max_connections > 0 {
        (peak_connections as f64 / max_connections as f64 * 100.0).clamp(0.0, 100.0)
    } else {
        0.0
    };
    DatabaseMetrics {
        kind,
        size_bytes,
        disk_total,
        disk_available,
        connections,
        active_connections,
        peak_connections,
        idle_connections,
        max_connections,
        queries_per_sec,
        writes_per_sec,
        write_pressure_percent,
        audit_logs_bytes: table_sizes.0,
        clients_bytes: table_sizes.1,
        metrics_bytes: table_sizes.2,
        other_tables_bytes: table_sizes.3,
        tables,
    }
}

async fn database_table_sizes(
    state: &AppState,
    total: u64,
) -> ((u64, u64, u64, u64), Vec<TableSize>) {
    let sql = match state.db.kind() {
        sakuio_db::DbKind::Sqlite => {
            "SELECT name, SUM(pgsize) AS size FROM dbstat GROUP BY name"
        }
        sakuio_db::DbKind::MySql => {
            "SELECT table_name AS name, (data_length + index_length) AS size FROM information_schema.tables WHERE table_schema = DATABASE()"
        }
        sakuio_db::DbKind::Postgres => {
            "SELECT relname AS name, pg_total_relation_size(relid) AS size FROM pg_catalog.pg_statio_user_tables"
        }
    };
    let Ok(rows) = sqlx::query(sql).fetch_all(&state.db).await else {
        if state.db.kind() == sakuio_db::DbKind::Sqlite {
            return sqlite_table_estimates(state, total).await;
        }
        return ((0, 0, 0, total), Vec::new());
    };
    let mut audit = 0u64;
    let mut clients = 0u64;
    let mut metrics = 0u64;
    let mut other = 0u64;
    let mut logical: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for row in rows {
        let Ok(name) = row.try_get::<String, _>("name") else {
            continue;
        };
        let size = row.try_get::<i64, _>("size").unwrap_or(0).max(0) as u64;
        let lower = name.to_ascii_lowercase();
        // TimescaleDB physically stores rows in internal chunks. Present all
        // backends with the same logical table name as SQLite/MySQL.
        let logical_name = if lower.contains("_hyper_") || lower.contains("_chunk") {
            "metric_buckets".to_string()
        } else {
            name.clone()
        };
        *logical.entry(logical_name.clone()).or_default() += size;
        if logical_name == "audit_logs" || lower.starts_with("idx_audit") {
            audit = audit.saturating_add(size);
        } else if logical_name == "clients"
            || logical_name.starts_with("client")
            || lower.starts_with("idx_client")
        {
            clients = clients.saturating_add(size);
        } else if logical_name.starts_with("metric") || lower.starts_with("idx_metric") {
            metrics = metrics.saturating_add(size);
        } else {
            other = other.saturating_add(size);
        }
    }
    let mut tables: Vec<TableSize> = logical
        .into_iter()
        .filter(|(_, bytes)| *bytes > 0)
        .map(|(name, bytes)| TableSize { name, bytes })
        .collect();
    let known = audit
        .saturating_add(clients)
        .saturating_add(metrics)
        .saturating_add(other);
    let other = other.saturating_add(total.saturating_sub(known));
    tables.sort_by(|left, right| right.bytes.cmp(&left.bytes));
    ((audit, clients, metrics, other), tables)
}

async fn sqlite_table_estimates(
    state: &AppState,
    total: u64,
) -> ((u64, u64, u64, u64), Vec<TableSize>) {
    let Ok(names) = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(&state.db)
    .await
    else {
        return ((0, 0, 0, total), Vec::new());
    };
    let mut tables = Vec::new();
    let mut audit = 0u64;
    let mut clients = 0u64;
    let mut metrics = 0u64;
    let mut other = 0u64;
    for name in names {
        let quoted = name.replace('"', "\"\"");
        let count = sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM \"{quoted}\""))
            .fetch_one(&state.db)
            .await
            .unwrap_or(0)
            .max(0) as u64;
        let per_row = if name == "audit_logs" {
            512
        } else if name.starts_with("client") {
            1024
        } else if name.starts_with("metric") {
            256
        } else {
            256
        };
        let bytes = count.saturating_mul(per_row);
        if bytes == 0 {
            continue;
        }
        tables.push(TableSize {
            name: name.clone(),
            bytes,
        });
        let lower = name.to_ascii_lowercase();
        if lower == "audit_logs" {
            audit = audit.saturating_add(bytes);
        } else if lower.starts_with("client") {
            clients = clients.saturating_add(bytes);
        } else if lower.starts_with("metric") {
            metrics = metrics.saturating_add(bytes);
        } else {
            other = other.saturating_add(bytes);
        }
    }
    let known = audit
        .saturating_add(clients)
        .saturating_add(metrics)
        .saturating_add(other);
    let other = other.saturating_add(total.saturating_sub(known));
    tables.sort_by(|left, right| right.bytes.cmp(&left.bytes));
    ((audit, clients, metrics, other), tables)
}

async fn database_size(state: &AppState) -> u64 {
    if state.db.kind() == sakuio_db::DbKind::Sqlite {
        let dsn = state.db.dsn();
        let path = dsn
            .strip_prefix("sqlite://")
            .or_else(|| dsn.strip_prefix("sqlite:"))
            .unwrap_or(dsn)
            .split('?')
            .next()
            .unwrap_or("");
        if path.is_empty() || path == ":memory:" {
            return 0;
        }
        return ["", "-wal", "-shm"]
            .iter()
            .filter_map(|suffix| std::fs::metadata(format!("{path}{suffix}")).ok())
            .map(|metadata| metadata.len())
            .sum();
    }
    let sql = if state.db.kind() == sakuio_db::DbKind::MySql {
        "SELECT CAST(COALESCE(SUM(data_length + index_length), 0) AS SIGNED) FROM information_schema.tables WHERE table_schema = DATABASE()"
    } else {
        "SELECT pg_database_size(current_database())"
    };
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(&state.db)
        .await
        .ok()
        .filter(|size| *size > 0)
        .map(|size| size as u64)
        .unwrap_or(0)
}

fn disk_usage(sampler: &mut Sampler) -> (u64, u64) {
    sampler.disks.refresh(true);
    let cwd = std::env::current_dir().unwrap_or_default();
    let mut best: Option<(usize, u64, u64)> = None;
    for disk in sampler.disks.list() {
        let mount = disk.mount_point();
        if cwd.starts_with(mount) {
            let depth = mount.components().count();
            if best.is_none_or(|(best_depth, _, _)| depth > best_depth) {
                best = Some((depth, disk.total_space(), disk.available_space()));
            }
        }
    }
    best.map(|(_, total, available)| (total, available))
        .unwrap_or((0, 0))
}

/// GET `/api/admin/pprof/summary` — one-off fallback summary.
pub async fn summary_endpoint(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
) -> Response {
    let mut sampler = Sampler::new();
    Json(json!({ "success": true, "data": summary(&state, &mut sampler).await })).into_response()
}

/// GET `/api/admin/pprof/stream` — SSE summary refreshed every second while
/// the admin page is open. Sampling stops when the connection closes.
pub async fn stream_endpoint(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(8);
    tokio::spawn(async move {
        let mut sampler = Sampler::new();
        loop {
            let data = summary(&state, &mut sampler).await;
            let event = Event::default()
                .event("summary")
                .json_data(&data)
                .unwrap_or_else(|_| Event::default().event("error").data("{}"));
            if tx.send(Ok(event)).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    let stream = stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
