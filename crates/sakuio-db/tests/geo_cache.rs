//! Persistent IP geo cache lifecycle tests.
//! 持久化 IP 归属地缓存生命周期测试。

use sakuio_db::{AppDatabase, GeoCacheRepository};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    format!(
        "{}{}-{}",
        std::process::id(),
        nanos,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn persists_and_prunes_unreferenced_expired_rows() {
    let path = std::env::temp_dir().join(format!("sakuio-geo-{}.db", unique()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let app = AppDatabase::connect(&dsn, true).await.expect("app db");
    let repo = GeoCacheRepository::new(app.pool());

    repo.upsert("1.1.1.1", "AU", "Australia Brisbane", 10_000, "now")
        .await
        .expect("upsert");
    repo.upsert("8.8.8.8", "US", "United States", 1, "now")
        .await
        .expect("upsert");

    let live = repo.get("1.1.1.1").await.expect("get").expect("row");
    assert_eq!(live.country_code, "AU");
    assert_eq!(live.location, "Australia Brisbane");

    let mut referenced = HashSet::new();
    referenced.insert("8.8.8.8".to_string());
    let deleted = repo
        .delete_expired_unreferenced(5_000, &referenced, 100)
        .await
        .expect("prune");
    assert_eq!(deleted, 0, "referenced expired rows are kept for refresh");
    assert!(repo.get("8.8.8.8").await.expect("get").is_some());

    referenced.clear();
    let deleted = repo
        .delete_expired_unreferenced(5_000, &referenced, 100)
        .await
        .expect("prune");
    assert_eq!(deleted, 1);
    assert!(repo.get("8.8.8.8").await.expect("get").is_none());

    app.shutdown().await;
    let _ = std::fs::remove_file(&path);
}
