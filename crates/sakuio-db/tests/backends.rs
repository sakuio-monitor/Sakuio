//! Cross-backend integration tests for the app and metric stores.
//! 应用库与指标库的跨后端集成测试。
//!
//! SQLite always runs. PostgreSQL / MySQL / MariaDB run when the matching
//! environment variable points at a reachable server:
//!
//! ```text
//! SAKUIO_TEST_POSTGRES_DSN=postgres://sakuio:sakuio@127.0.0.1:55432/sakuio_test
//! SAKUIO_TEST_MYSQL_DSN=mysql://sakuio:sakuio@127.0.0.1:53306/sakuio_test
//! SAKUIO_TEST_MARIADB_DSN=mysql://sakuio:sakuio@127.0.0.1:53307/sakuio_test
//! ```
//!
//! `scripts/test-backends.sh` starts the containers and sets these variables.

use sakuio_db::{
    AdminClientPatch, AppDatabase, ClientRepository, DbKind, NewClient, PingRepository,
    SettingsRepository,
};
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
        let mut backends = vec![sqlite_backend()];
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

fn sqlite_backend() -> Backend {
    let path = std::env::temp_dir().join(format!("{}.db", unique("sakuio-it-")));
    Backend {
        name: "sqlite",
        dsn: format!("sqlite://{}?mode=rwc", path.display()),
        kind: DbKind::Sqlite,
        sqlite_file: Some(path),
    }
}

/// Exercises the SQL paths that differ per backend: reserved `key`, upserts,
/// `last_insert_id` / `RETURNING`, transactions and typed row decoding.
/// 覆盖各后端差异最大的路径：保留字 `key`、upsert、`last_insert_id` /
/// `RETURNING`、事务与类型化解码。
#[tokio::test]
async fn app_repositories_work_on_every_backend() {
    for backend in Backend::all() {
        eprintln!("app repository matrix: {}", backend.name);
        let db = AppDatabase::connect(&backend.dsn, true)
            .await
            .unwrap_or_else(|e| panic!("{} connect failed: {e}", backend.name));
        assert_eq!(db.kind(), backend.kind, "{} kind", backend.name);

        // Opening twice must be idempotent (additive schema sync).
        // 二次打开必须幂等（只增不改的声明式建表）。
        let second = AppDatabase::connect(&backend.dsn, true)
            .await
            .unwrap_or_else(|e| panic!("{} reconnect failed: {e}", backend.name));
        second.shutdown().await;

        let pool = db.pool();
        let settings = SettingsRepository::new(pool);
        let key = unique("setting-");
        settings
            .set_json(&key, &serde_json::json!({ "ok": true }))
            .await
            .unwrap_or_else(|e| panic!("{} settings set failed: {e}", backend.name));
        let value: Option<serde_json::Value> = settings
            .get_json(&key)
            .await
            .unwrap_or_else(|e| panic!("{} settings get failed: {e}", backend.name));
        assert_eq!(value, Some(serde_json::json!({ "ok": true })));

        let clients = ClientRepository::new(pool);
        let (uuid, token) = clients
            .insert(&NewClient {
                name: unique("node-"),
                token: None,
            })
            .await
            .unwrap_or_else(|e| panic!("{} client insert failed: {e}", backend.name));
        assert_eq!(
            clients
                .uuid_by_token(&token)
                .await
                .unwrap_or_else(|e| panic!("{} token lookup failed: {e}", backend.name)),
            Some(uuid.clone())
        );
        clients
            .patch_admin(
                &uuid,
                &AdminClientPatch {
                    report_interval_ms: Some(1500),
                    ..AdminClientPatch::default()
                },
            )
            .await
            .unwrap_or_else(|e| panic!("{} interval patch failed: {e}", backend.name));
        assert_eq!(
            clients
                .report_interval_ms(&uuid)
                .await
                .unwrap_or_else(|e| panic!("{} interval read failed: {e}", backend.name)),
            Some(1500)
        );
        clients
            .set_online(&uuid, true)
            .await
            .unwrap_or_else(|e| panic!("{} set online failed: {e}", backend.name));
        let offline = clients
            .set_all_offline()
            .await
            .unwrap_or_else(|e| panic!("{} set_all_offline failed: {e}", backend.name));
        assert!(
            offline.contains(&uuid),
            "{}: updated row must be returned",
            backend.name
        );

        // Ping task insert covers `RETURNING id` on PostgreSQL and
        // `last_insert_id` on SQLite/MySQL.
        // Ping 任务覆盖 PostgreSQL 的 `RETURNING id` 与 SQLite/MySQL 的
        // `last_insert_id`。
        let ping = PingRepository::new(pool);
        let task_id = ping
            .add(&[], false, &unique("ping-"), "127.0.0.1", "icmp", 60)
            .await
            .unwrap_or_else(|e| panic!("{} ping add failed: {e}", backend.name));
        let now = chrono::Utc::now().timestamp_millis();
        ping.insert_record(&uuid, task_id, 7, now)
            .await
            .unwrap_or_else(|e| panic!("{} ping record failed: {e}", backend.name));
        let records = ping
            .list_records(Some(&uuid), Some(task_id), now - 1_000, now + 1_000, 10)
            .await
            .unwrap_or_else(|e| panic!("{} ping list failed: {e}", backend.name));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, 7);

        ping.delete(&[task_id])
            .await
            .unwrap_or_else(|e| panic!("{} ping delete failed: {e}", backend.name));
        clients
            .delete_by_uuid(&uuid)
            .await
            .unwrap_or_else(|e| panic!("{} client delete failed: {e}", backend.name));

        db.shutdown().await;
    }
}

/// Two pools pointing at the same physical database must compare equal; this
/// guards the "don't switch onto the live database" check.
/// 指向同一物理库的两个连接池必须判定相等，用于保护「禁止原地切换」。
#[tokio::test]
async fn detects_same_database_on_every_backend() {
    for backend in Backend::all() {
        let first = AppDatabase::connect(&backend.dsn, true)
            .await
            .unwrap_or_else(|e| panic!("{} connect: {e}", backend.name));
        let second = AppDatabase::connect(&backend.dsn, true)
            .await
            .unwrap_or_else(|e| panic!("{} connect: {e}", backend.name));
        let same = sakuio_db::is_same_database(first.pool(), second.pool()).await;
        assert!(
            matches!(same, Ok(true)),
            "{}: expected same database, got {same:?}",
            backend.name
        );
        first.shutdown().await;
        second.shutdown().await;
    }
}
