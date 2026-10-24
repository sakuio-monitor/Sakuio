use sakuio_db::{AppDatabase, ClientRepository, NewClient};

#[tokio::test]
async fn admin_client_search_filters_in_sql() {
    let path = std::env::temp_dir().join(format!("sakuio-search-{}.db", std::process::id()));
    let dsn = format!("sqlite://{}?mode=rwc", path.display());
    let app = AppDatabase::connect(&dsn, true).await.unwrap();
    let repo = ClientRepository::new(app.pool());
    repo.insert(&NewClient { name: "上海联通".into(), token: Some("a".repeat(64)) })
        .await
        .unwrap();
    repo.insert(&NewClient { name: "成都阿里云".into(), token: Some("b".repeat(64)) })
        .await
        .unwrap();

    let page = repo.list_admin_page(None, 20, Some("上海")).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].name, "上海联通");
    assert_eq!(page.total, 1);

    drop(repo);
    drop(app);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
