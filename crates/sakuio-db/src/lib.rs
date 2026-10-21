//! Application database: install flow, settings, and agent client records.
//! 应用配置库：安装流程、系统设置与 Agent 客户端记录。

mod admin_auth;
mod clients;
mod db_test;
mod exec;
mod install;
mod notification;
mod ping;
mod settings;

pub use admin_auth::{
    generate_totp_secret, guest_me_json, login_requires_2fa, totp_provisioning_uri,
    AdminAuthRepository,
    AdminSessionUser, AdminUserRow, LoginRequest, Sensitive2faError,
};
pub use clients::{
    normalize_bandwidth_unit, normalize_reset_mode, normalize_stat_mode, traffic_used_by_mode,
    AdminClientPatch, AdminClientListPage, AdminClientRecord, ClientRecord, ClientRepository,
    NewClient, PublicClientRow, TrafficLedgerRow, BANDWIDTH_UNITS, TRAFFIC_RESET_MODES,
    TRAFFIC_STAT_MODES,
};
pub use exec::ExecRepository;
pub use db_test::{test_config_database, TestDbRequest};
pub use install::{open_bootstrap_db, CompleteInstallRequest, InstallService, InstallStatus};
pub use notification::{NotificationRepository, NotificationRuleRow};
pub use ping::{
    task_to_public_json, task_to_public_list_json, PingDbError, PingRecordRow, PingRepository,
    PingTaskRow,
};
pub use settings::SettingsRepository;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::{path::Path, str::FromStr};
use thiserror::Error;

/// Errors surfaced from the database layer.
/// 数据库层对外暴露的错误。
#[derive(Debug, Error)]
pub enum DbError {
    #[error("database: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("{0}")]
    Message(String),
}

pub type DbResult<T> = Result<T, DbError>;

/// Shared SQLite pool for the config / app store (v1 default).
/// 配置库 / 应用库的 SQLite 连接池（v1 默认）。
#[derive(Clone)]
pub struct AppDatabase {
    pool: SqlitePool,
}

impl AppDatabase {
    /// Opens SQLite at `dsn` (file path or `:memory:`) and runs embedded migrations.
    /// 打开 `dsn` 处的 SQLite（文件路径或 `:memory:`）并执行内嵌迁移。
    pub async fn connect_sqlite(dsn: &str, create_dirs: bool) -> DbResult<Self> {
        if create_dirs {
            if let Some(parent) = Path::new(dsn).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        DbError::Message(format!("create data directory: {e}"))
                    })?;
                }
            }
        }
        let opts = SqliteConnectOptions::from_str(dsn)?
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(16)
            .connect_with(opts)
            .await?;
        run_config_migrations(&pool).await?;
        Ok(Self { pool })
    }

    /// Returns the underlying pool for repositories.
    /// 返回供仓储使用的底层连接池。
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

/// Runs config-store migrations embedded at compile time (`sqlx::migrate!`).
/// 执行编译期内嵌的配置库迁移（`sqlx::migrate!`）。
pub async fn run_config_migrations(pool: &SqlitePool) -> DbResult<()> {
    sqlx::migrate!().run(pool).await?;
    tracing::info!("config database migrations applied");
    Ok(())
}
