//! Verifies that buckets held in memory during maintenance can be flushed into
//! a specific store before a main-database switch restarts the process.
//! 验证维护期间暂存在内存的桶能在主库切换重启前写入指定存储。

use sakuio_db::AppDatabase;
use sakuio_metric::{MetricDbConfig, MetricStore, MetricWriter, MetricWriterConfig};
use sakuio_protocol::{ConnectionsReport, CpuReport, NetworkReport, TelemetrySample, UsageReport};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn flush_writes_maintenance_backlog_to_target_store() {
    let path = std::env::temp_dir().join(format!("{}.db", unique("sakuio-writer-flush-")));
    let app = AppDatabase::connect(&format!("sqlite://{}?mode=rwc", path.display()), true)
        .await
        .expect("app connect");

    let store = std::sync::Arc::new(
        MetricStore::connect(app.pool(), &MetricDbConfig::default())
            .await
            .expect("metric store"),
    );
    let (writer, handle) = MetricWriter::start(
        app.pool().clone(),
        store.clone(),
        MetricWriterConfig { queue_capacity: 64 },
    );

    let node = unique("node-");
    let minute = 60_000i64;
    let start = chrono::Utc::now().timestamp_millis() / minute * minute;
    sakuio_db::set_maintenance(true);
    writer.enqueue(
        node.clone(),
        vec![sample(start + 1_000, 12.5), sample(start + 2_000, 25.0)],
    );

    // Biased select in the writer drains queued samples before the flush
    // command, so this also proves the backlog survives maintenance.
    // 写入器的 biased select 会先排空采样再处理 flush，因此这里也验证了
    // 维护期间的积压不会丢。
    let flushed = handle
        .flush_to_store(store.clone())
        .await
        .expect("flush to target store");
    assert!(flushed > 0, "flush wrote no buckets");

    let rows = store
        .list_buckets(&node, 60, start, start + minute, 100)
        .await
        .expect("list buckets");
    assert!(!rows.is_empty(), "no buckets in target store after flush");
    assert!(rows.iter().any(|row| row.metric_key == "cpu.usage"));
    assert!(rows
        .iter()
        .any(|row| row.metric_key == "cpu.usage" && row.sample_count == 2));

    sakuio_db::set_maintenance(false);
    drop(writer);
    store.shutdown().await;
    app.shutdown().await;
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

fn sample(observed_at_ms: i64, cpu: f64) -> TelemetrySample {
    TelemetrySample {
        observed_at_ms,
        cpu: CpuReport { usage: cpu },
        ram: UsageReport {
            total: 8 * 1024,
            used: 4 * 1024,
        },
        swap: UsageReport::default(),
        load: Default::default(),
        disk: UsageReport {
            total: 64 * 1024,
            used: 32 * 1024,
        },
        network: NetworkReport {
            up: 100,
            down: 200,
            total_up: 1_000,
            total_down: 2_000,
        },
        connections: ConnectionsReport { tcp: 10, udp: 2 },
        gpu: None,
        uptime: 3_600,
        process: 120,
        message: String::new(),
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
