use sakuio_db::{ensure_metric_schema, AppDatabase};

#[tokio::test]
async fn metric_schema_does_not_keep_duplicate_indexes() {
    let path = std::env::temp_dir().join(format!("sakuio-index-{}.db", std::process::id()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let app = AppDatabase::connect(&dsn, true).await.unwrap();
    ensure_metric_schema(app.pool(), "metric_").await.unwrap();

    sqlx::query(
        "CREATE INDEX idx_metric_buckets_node_metric \
         ON metric_buckets (series_id, bucket_seconds)",
    )
    .execute(app.pool().inner())
    .await
    .unwrap();

    ensure_metric_schema(app.pool(), "metric_").await.unwrap();
    let legacy: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' \
         AND name='idx_metric_buckets_node_metric'",
    )
    .fetch_one(app.pool().inner())
    .await
    .unwrap();
    assert_eq!(legacy, 0);

    drop(app);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
