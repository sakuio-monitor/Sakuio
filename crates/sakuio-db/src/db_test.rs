use crate::{AppDatabase, DbError, DbResult};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::mysql::MySqlPoolOptions;

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
    match driver {
        "sqlite" => {
            AppDatabase::connect_sqlite(dsn, true).await?;
            tracing::info!(driver, dsn, "install db test succeeded");
            Ok(())
        }
        "postgresql" => {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(dsn)
                .await
                .map_err(DbError::Sqlx)?;
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .map_err(DbError::Sqlx)?;
            pool.close().await;
            tracing::info!(driver, "install db test succeeded");
            Ok(())
        }
        "mysql" | "mariadb" => {
            let pool = MySqlPoolOptions::new()
                .max_connections(1)
                .connect(dsn)
                .await
                .map_err(DbError::Sqlx)?;
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .map_err(DbError::Sqlx)?;
            pool.close().await;
            tracing::info!(driver, "install db test succeeded");
            Ok(())
        }
        other => Err(DbError::Message(format!("unsupported database driver: {other}"))),
    }
}
