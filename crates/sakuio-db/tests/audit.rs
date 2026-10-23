//! Audit repository tests: keyset pagination and bounded retention deletes.
//! 审计仓储测试：游标分页与有界保留清理。

use sakuio_db::{AppDatabase, AuditLogFilter, AuditLogRepository, NewAuditLog};
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

fn entry(index: usize, actor: &str) -> NewAuditLog {
    NewAuditLog {
        created_at: format!("2026-01-01T00:00:{:02}.000Z", index % 60),
        actor: actor.into(),
        actor_id: format!("id-{actor}"),
        action: "POST".into(),
        target: format!("themes/{index}"),
        detail: "{}".into(),
        ip: "127.0.0.1".into(),
        user_agent: "test".into(),
        status: 200,
        method: "POST".into(),
        path: "/api/admin/themes".into(),
        geo_country_code: String::new(),
        geo_location: String::new(),
    }
}

#[tokio::test]
async fn keyset_pagination_and_retention() {
    let path = std::env::temp_dir().join(format!("sakuio-audit-{}.db", unique()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let app = AppDatabase::connect(&dsn, true).await.expect("app db");
    let repo = AuditLogRepository::new(app.pool());
    let rows: Vec<_> = (0..25)
        .map(|index| entry(index, if index % 2 == 0 { "alice" } else { "bob" }))
        .collect();
    repo.insert_batch(&rows).await.expect("insert");

    let first = repo
        .list(&AuditLogFilter {
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("first page");
    assert_eq!(first.len(), 11, "repository returns limit + 1");
    assert_eq!(first[0].actor, "alice");

    let next = repo
        .list(&AuditLogFilter {
            before_id: Some(first[9].id),
            limit: 10,
            ..Default::default()
        })
        .await
        .expect("next page");
    assert!(next.iter().all(|row| row.id < first[9].id));

    let alice = repo
        .list(&AuditLogFilter {
            actor: "alice".into(),
            limit: 50,
            ..Default::default()
        })
        .await
        .expect("filtered");
    assert_eq!(alice.len(), 13);
    assert!(alice.iter().all(|row| row.actor == "alice"));

    let deleted = repo
        .delete_before("2026-01-01T00:00:20.000Z", 10)
        .await
        .expect("delete chunk");
    assert_eq!(deleted, 10);
    app.shutdown().await;
    let _ = std::fs::remove_file(&path);
}
