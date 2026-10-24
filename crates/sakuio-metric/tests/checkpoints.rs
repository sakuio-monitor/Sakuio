use sakuio_db::AppDatabase;
use sakuio_metric::{BucketAggregator, BucketRow, MetricDbConfig, MetricStore};

fn row(node: &str, key: &str, start: i64) -> BucketRow {
    BucketRow {
        node_uuid: node.into(),
        metric_key: key.into(),
        bucket_seconds: 60,
        bucket_start_ms: start,
        avg_val: 12.0,
        min_val: 10.0,
        max_val: 14.0,
        sample_count: 2,
        sum_sq: 296.0,
        variance: 4.0,
        p50_val: 12.0,
        p95_val: 14.0,
        p99_val: 14.0,
    }
}

#[tokio::test]
async fn checkpoints_round_trip_and_restore_aggregator() {
    let path = std::env::temp_dir().join(format!("sakuio-checkpoint-{}.db", std::process::id()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let app = AppDatabase::connect(&dsn, true).await.unwrap();
    let store = MetricStore::connect(app.pool(), &MetricDbConfig::default())
        .await
        .unwrap();
    let start = 1_700_000_000_000;
    let rows = vec![
        row("node-a", "cpu.usage", start),
        row("node-a", "memory.used", start),
    ];
    store.save_checkpoints(&rows).await.unwrap();

    let loaded = store.load_checkpoints().await.unwrap();
    assert_eq!(loaded.get("node-a").map(Vec::len), Some(2));

    let mut aggregator = BucketAggregator::default();
    aggregator.restore("node-a", &loaded["node-a"]);
    let snap = aggregator.snapshot();
    assert_eq!(snap.len(), 2);
    assert!(snap.iter().all(|r| r.sample_count == 2));

    store.clear_checkpoints().await.unwrap();
    assert!(store.load_checkpoints().await.unwrap().is_empty());
    drop(store);
    drop(app);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
