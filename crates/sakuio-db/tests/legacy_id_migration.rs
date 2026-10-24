use sakuio_db::AppDatabase;
use sqlx::Row;

#[tokio::test]
async fn legacy_surrogate_ids_are_rebuilt() {
    let path = std::env::temp_dir().join(format!("sakuio-id-migration-{}.db", std::process::id()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let pool = sqlx::sqlite::SqlitePool::connect(&dsn).await.unwrap();
    for sql in [
        "CREATE TABLE ping_tasks (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE ping_records (id INTEGER PRIMARY KEY, client_uuid TEXT NOT NULL, task_id INTEGER NOT NULL, observed_at_ms INTEGER NOT NULL, value_ms INTEGER NOT NULL)",
        "CREATE TABLE metric_buckets (id INTEGER PRIMARY KEY, node_uuid TEXT NOT NULL, metric_key TEXT NOT NULL, bucket_seconds INTEGER NOT NULL, bucket_start_ms INTEGER NOT NULL, avg_val REAL NOT NULL, min_val REAL NOT NULL, max_val REAL NOT NULL, sample_count INTEGER NOT NULL, sum_sq REAL, variance REAL, p50_val REAL, p95_val REAL, p99_val REAL)",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    sqlx::query(
        "INSERT INTO metric_buckets (node_uuid, metric_key, bucket_seconds, bucket_start_ms, avg_val, min_val, max_val, sample_count, sum_sq, variance, p50_val, p95_val, p99_val) VALUES ('node-1', 'cpu.usage', 60, 1000, 5.0, 4.0, 6.0, 2, 50.0, 1.0, 5.0, 6.0, 6.0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ping_records (client_uuid, task_id, observed_at_ms, value_ms) VALUES ('node-1', 1, 1000, 7)",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;

    let app = AppDatabase::connect(&dsn, true).await.unwrap();
    let view_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM metric_buckets_view")
        .fetch_one(app.pool())
        .await
        .unwrap();
    assert_eq!(view_rows, 1);
    let nodes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM metric_nodes")
        .fetch_one(app.pool())
        .await
        .unwrap();
    assert_eq!(nodes, 1);
    for table in ["ping_records", "metric_buckets"] {
        let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(app.pool())
            .await
            .unwrap();
        let names: Vec<String> = rows
            .iter()
            .map(|row| row.get::<String, _>("name"))
            .collect();
        assert!(
            !names.iter().any(|name| name == "id"),
            "{table} still has id"
        );
    }
    drop(app);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
