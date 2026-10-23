use crate::{canonical_driver, AppDatabase, DbError, DbKind, DbResult};
use serde::Deserialize;

/// POST `/api/install/test-db` body.
/// POST `/api/install/test-db` 请求体。
#[derive(Debug, Deserialize)]
pub struct TestDbRequest {
    pub app_db_driver: String,
    pub app_db_dsn: String,
}

/// Probes config database connectivity (SQLite creates file/dirs as needed).
/// 探测配置库连通性（SQLite 会在需要时创建目录与文件）。
pub async fn test_config_database(req: &TestDbRequest) -> DbResult<()> {
    let driver = req.app_db_driver.trim();
    let dsn = req.app_db_dsn.trim();
    if dsn.is_empty() {
        return Err(DbError::Message("database dsn required".into()));
    }
    let supplied = match driver.to_ascii_lowercase().as_str() {
        "sqlite" | "sqlite3" => "sqlite",
        "postgresql" | "postgres" => "postgresql",
        "mysql" | "mariadb" => "mysql",
        _ => {
            return Err(DbError::Message(format!(
                "unsupported database driver: {driver}"
            )))
        }
    };
    let kind = DbKind::detect(dsn);
    if supplied != canonical_driver(kind) {
        return Err(DbError::Message(format!(
            "driver {driver} does not match DSN backend {}",
            canonical_driver(kind)
        )));
    }
    let db = AppDatabase::connect(dsn, true).await?;
    db.pool().close().await;
    tracing::info!(driver, "install db test succeeded");
    Ok(())
}
