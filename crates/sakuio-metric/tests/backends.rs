//! Cross-backend integration tests for the metric store.
//! 指标库的跨后端集成测试。
//!
//! SQLite always runs. PostgreSQL / MySQL / MariaDB run when the matching
//! environment variable points at a reachable server (see
//! `scripts/test-backends.sh`).

use sakuio_db::{AppDatabase, DbKind};
use sakuio_metric::{BucketRow, MetricDbConfig, MetricStore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

struct Backend {
    name: &'static str,
    dsn: String,
    kind: DbKind,
    sqlite_file: Option<std::path::PathBuf>,
}

impl Backend {
    fn all() -> Vec<Self> {
        let path = std::env::temp_dir().join(format!("{}.db", unique("sakuio-metric-it-")));
        let mut backends = vec![Self {
            name: "sqlite",
            dsn: format!("sqlite://{}?mode=rwc", path.display()),
            kind: DbKind::Sqlite,
            sqlite_file: Some(path),
        }];
        for (name, var, kind) in [
            ("postgres", "SAKUIO_TEST_POSTGRES_DSN", DbKind::Postgres),
            ("mysql", "SAKUIO_TEST_MYSQL_DSN", DbKind::MySql),
            ("mariadb", "SAKUIO_TEST_MARIADB_DSN", DbKind::MySql),
        ] {
            if let Ok(dsn) = std::env::var(var) {
                let dsn = dsn.trim().to_string();
                if !dsn.is_empty() {
                    backends.push(Self {
                        name,
                        dsn,
                        kind,
                        sqlite_file: None,
                    });
                }
            }
        }
        backends
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        if let Some(path) = &self.sqlite_file {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(path.with_extension("db-wal"));
            let _ = std::fs::remove_file(path.with_extension("db-shm"));
        }
    }
}

fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{}{nanos}-{sequence}", std::process::id())
}

/// Separate metric database, merged (colocated) storage, custom table prefix,
/// bucket upsert/list/retention/cleanup and size reporting.
/// 独立指标库、合并存储、自定义表前缀、桶 upsert/查询/保留/清理与容量统计。
#[tokio::test]
async fn metric_store_works_on_every_backend() {
    for backend in Backend::all() {
        eprintln!("metric store matrix: {}", backend.name);
        let app = AppDatabase::connect(&backend.dsn, true)
            .await
            .unwrap_or_else(|e| panic!("{} app connect failed: {e}", backend.name));
        let prefix = format!("t{}_", std::process::id());

        let cfg = MetricDbConfig {
            dsn: backend.dsn.clone(),
            table_prefix: prefix,
            max_connections: 5,
            min_idle_connections: 0,
            ..MetricDbConfig::default()
        };
        let store = MetricStore::connect(app.pool(), &cfg)
            .await
            .unwrap_or_else(|e| panic!("{} metric connect failed: {e}", backend.name));
        assert_eq!(
            store.kind().as_str(),
            metric_kind(backend.kind),
            "{} metric kind",
            backend.name
        );

        let node = unique("metric-node-");
        let start = chrono::Utc::now().timestamp_millis() / 60_000 * 60_000;
        let rows = vec![
            bucket(&node, "cpu.usage", 60, start, 10.0),
            bucket(&node, "cpu.usage", 60, start, 20.0),
            bucket(&node, "memory.used", 60, start, 1024.0),
        ];
        store
            .insert_buckets(&rows)
            .await
            .unwrap_or_else(|e| panic!("{} bucket insert failed: {e}", backend.name));
        let listed = store
            .list_buckets(&node, 60, start, start + 60_000, 100)
            .await
            .unwrap_or_else(|e| panic!("{} bucket list failed: {e}", backend.name));
        // Duplicate bucket is skipped, not updated.
        // 重复桶被跳过而不是更新。
        assert_eq!(listed.len(), 2, "{} bucket rows", backend.name);

        let cpu_only = store
            .list_buckets_for_metric(&node, "cpu.usage", 60, start, start + 60_000, 100)
            .await
            .unwrap_or_else(|e| panic!("{} filtered bucket list failed: {e}", backend.name));
        let second = unique("metric-node-2-");
        store
            .insert_buckets(&[
                bucket(&second, "cpu.usage", 60, start, 30.0),
                bucket(&second, "memory.used", 60, start, 2048.0),
            ])
            .await
            .unwrap();
        let many = store
            .list_buckets_multi(
                &[node.clone(), second.clone()],
                None,
                60,
                start,
                start + 60_000,
                100,
            )
            .await
            .unwrap();
        assert_eq!(many.len(), 2, "{} multi-node groups", backend.name);
        assert_eq!(many[&second].len(), 2, "{} multi-node rows", backend.name);
        assert_eq!(cpu_only.len(), 1, "{} filtered bucket rows", backend.name);
        assert_eq!(cpu_only[0].metric_key, "cpu.usage");

        store
            .set_retention_days("cpu.usage", 30)
            .await
            .unwrap_or_else(|e| panic!("{} retention set failed: {e}", backend.name));
        let days = store
            .retention_days()
            .await
            .unwrap_or_else(|e| panic!("{} retention list failed: {e}", backend.name));
        assert!(days
            .iter()
            .any(|(key, value)| key == "cpu.usage" && *value == 30));

        let policy: std::collections::HashMap<String, i64> = days.into_iter().collect();
        let deleted = store
            .cleanup(&cfg, &policy, start + 90 * 86_400_000)
            .await
            .unwrap_or_else(|e| panic!("{} cleanup failed: {e}", backend.name));
        assert!(deleted > 0, "{} cleanup deleted nothing", backend.name);
        assert!(store.size_bytes().await.unwrap_or(0) > 0);
        store.shutdown().await;

        // Merged storage on the app pool with a custom prefix.
        // 合并存储：在应用库上使用自定义前缀。
        let merged_cfg = MetricDbConfig {
            dsn: String::new(),
            table_prefix: format!("m{}_", std::process::id()),
            ..MetricDbConfig::default()
        };
        let merged = MetricStore::connect(app.pool(), &merged_cfg)
            .await
            .unwrap_or_else(|e| panic!("{} merged connect failed: {e}", backend.name));
        assert_eq!(merged.kind().as_str(), "merged");
        let merged_node = unique("merged-node-");
        merged
            .insert_buckets(&[bucket(&merged_node, "cpu.usage", 60, start, 5.0)])
            .await
            .unwrap_or_else(|e| panic!("{} merged insert failed: {e}", backend.name));
        let merged_rows = merged
            .list_buckets(&merged_node, 60, start, start + 60_000, 10)
            .await
            .unwrap_or_else(|e| panic!("{} merged list failed: {e}", backend.name));
        assert_eq!(merged_rows.len(), 1);
        merged.shutdown().await;

        app.shutdown().await;
    }
}

fn metric_kind(kind: DbKind) -> &'static str {
    match kind {
        DbKind::Sqlite => "sqlite",
        DbKind::MySql => "mysql",
        DbKind::Postgres => "postgres",
    }
}

fn bucket(node: &str, key: &str, window: i64, start: i64, avg: f64) -> BucketRow {
    BucketRow {
        node_uuid: node.to_string(),
        metric_key: key.to_string(),
        bucket_seconds: window,
        bucket_start_ms: start,
        avg_val: avg,
        min_val: avg,
        max_val: avg,
        sample_count: 1,
        sum_sq: avg * avg,
        variance: 0.0,
        p50_val: avg,
        p95_val: avg,
        p99_val: avg,
    }
}
