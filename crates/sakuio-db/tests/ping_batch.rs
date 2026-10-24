use sakuio_db::{AppDatabase, ClientRepository, NewClient, PingRepository};

#[tokio::test]
async fn ping_batch_deduplicates_same_observation() {
    let path = std::env::temp_dir().join(format!("sakuio-ping-batch-{}.db", std::process::id()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let app = AppDatabase::connect(&dsn, true).await.unwrap();
    let clients = ClientRepository::new(app.pool());
    let (client_uuid, _token) = clients
        .insert(&NewClient {
            name: "batch".into(),
            token: Some("c".repeat(64)),
        })
        .await
        .unwrap();
    let ping = PingRepository::new(app.pool());
    let task_id = ping
        .add(&[], false, "batch", "127.0.0.1", "icmp", 60)
        .await
        .unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    ping.insert_records(&[
        (client_uuid.clone(), task_id, 7, now),
        (client_uuid.clone(), task_id, 9, now),
        (client_uuid.clone(), task_id, 11, now + 1),
    ])
    .await
    .unwrap();

    let records = ping
        .list_records(
            Some(&client_uuid),
            Some(task_id),
            now - 1_000,
            now + 1_000,
            10,
        )
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert!(records.iter().any(|r| r.value == 7));
    assert!(records.iter().any(|r| r.value == 11));

    let minute = ping
        .list_minute_rollups(
            Some(&client_uuid),
            Some(task_id),
            now - 120_000,
            now + 120_000,
            10,
        )
        .await
        .unwrap();
    assert_eq!(minute.len(), 1);
    // sum_ms is converted to an average latency by list_rollups.
    assert!(minute[0].value == 9 || minute[0].value == 11);
    let samples: i64 = sqlx::query_scalar(
        "SELECT samples FROM ping_minute_rollup WHERE task_id = ? AND client_uuid = ?",
    )
    .bind(task_id)
    .bind(&client_uuid)
    .fetch_one(app.pool())
    .await
    .unwrap();
    assert_eq!(samples, 2);

    // Replaying the same batch must not increment rollups again.
    ping.insert_records(&[
        (client_uuid.clone(), task_id, 7, now),
        (client_uuid.clone(), task_id, 9, now),
        (client_uuid.clone(), task_id, 11, now + 1),
    ])
    .await
    .unwrap();
    let samples: i64 = sqlx::query_scalar(
        "SELECT samples FROM ping_minute_rollup WHERE task_id = ? AND client_uuid = ?",
    )
    .bind(task_id)
    .bind(&client_uuid)
    .fetch_one(app.pool())
    .await
    .unwrap();
    assert_eq!(samples, 2);

    drop(ping);
    drop(clients);
    drop(app);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
