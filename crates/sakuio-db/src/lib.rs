//! Application database: install flow, settings, and agent client records.
//! 应用配置库：安装流程、系统设置与 Agent 客户端记录（SQLite / MySQL / PostgreSQL）。

mod admin_auth;
mod audit;
mod clients;
mod db_test;
mod dialect;
mod exec;
mod geo_cache;
mod install;
mod maintenance;
mod metric_storage;
mod notification;
mod ping;
mod schema;
mod settings;

pub use admin_auth::{
    generate_totp_secret, guest_me_json, login_requires_2fa, totp_provisioning_uri,
    AdminAuthRepository, AdminSessionUser, AdminUserRow, LoginRequest, Sensitive2faError,
};
pub use audit::{
    record_system_event, AuditLogFilter, AuditLogRepository, AuditLogRow, NewAuditLog,
};
pub use clients::{
    normalize_bandwidth_unit, normalize_reset_mode, normalize_stat_mode, traffic_used_by_mode,
    AdminClientListPage, AdminClientPatch, AdminClientRecord, ClientRecord, ClientRepository,
    NewClient, PublicClientRow, TrafficLedgerRow, BANDWIDTH_UNITS, TRAFFIC_RESET_MODES,
    TRAFFIC_STAT_MODES,
};
pub use db_test::{test_config_database, TestDbRequest};
pub use dialect::{
    is_same_database, normalize_dsn, quote_ident, render_sql, DbPool, DbTx, DB_QUERIES, DB_WRITES,
};
pub use exec::ExecRepository;
pub use geo_cache::{GeoCacheRepository, GeoCacheRow};
pub use install::{
    canonical_driver, complete_install, open_bootstrap_db, CompleteInstallRequest, InstallOutcome,
    InstallService, InstallStatus,
};
pub use maintenance::{maintenance_active, set_maintenance};
pub use notification::{NotificationRepository, NotificationRuleRow};
pub use ping::{
    task_to_public_json, task_to_public_list_json, PingDbError, PingRecordRow, PingRepository,
    PingTaskRow,
};
pub use schema::{Col, DbKind, TableDef, Ty, TABLES};
pub use settings::SettingsRepository;

/// Ensures only the monitoring tables on a standalone metric database, using
/// the configured table prefix.
/// 独立监控库只需桶/保留表，并按配置的表前缀建表。
pub async fn ensure_metric_schema(pool: &DbPool, prefix: &str) -> Result<(), sqlx::Error> {
    schema::ensure_metric_schema(pool, prefix).await
}

use sqlx::any::AnyPoolOptions;
use std::time::Duration;
use thiserror::Error;

/// Errors surfaced from the database layer.
/// 数据库层对外暴露的错误。
#[derive(Debug, Error)]
pub enum DbError {
    #[error("database: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("{0}")]
    Message(String),
}

pub type DbResult<T> = Result<T, DbError>;

/// Shared pool for the config / app store; supports SQLite / MySQL / PostgreSQL.
/// 配置库 / 应用库的共享连接池，支持 SQLite / MySQL / PostgreSQL。
#[derive(Clone)]
pub struct AppDatabase {
    pool: DbPool,
    kind: DbKind,
    dsn: String,
}

impl AppDatabase {
    /// Opens the app database at `dsn` (empty = local SQLite `./data/sakuio.db`)
    /// and ensures the declarative schema.
    /// 打开 `dsn` 处的应用库（留空 = 本机 SQLite），并执行声明式表结构同步。
    pub async fn connect(dsn: &str, create_dirs: bool) -> DbResult<Self> {
        Self::connect_with_max(dsn, create_dirs, 16).await
    }

    pub async fn connect_with_max(
        dsn: &str,
        create_dirs: bool,
        max_connections: u32,
    ) -> DbResult<Self> {
        sqlx::any::install_default_drivers();
        let kind = DbKind::detect(dsn);
        let in_memory = dsn.trim() == "sqlite::memory:";
        let effective = if in_memory {
            // SQLx accepts `sqlite::memory:` directly; the generic SQLite URL
            // normalizer below would otherwise rewrite it into an invalid URL.
            "sqlite::memory:".to_string()
        } else {
            match kind {
                DbKind::Sqlite if dsn.trim().is_empty() => {
                    "sqlite://data/sakuio.db?mode=rwc".into()
                }
                DbKind::Sqlite if !dsn.contains("://") => {
                    format!("sqlite://{}?mode=rwc", dsn.trim_start_matches("./"))
                }
                _ => dsn.to_string(),
            }
        };
        if kind == DbKind::Sqlite && create_dirs && !in_memory {
            let path = effective
                .trim_start_matches("sqlite://")
                .split('?')
                .next()
                .unwrap_or("data/sakuio.db");
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| DbError::Message(format!("create data directory: {e}")))?;
                }
            }
        }
        let pool = DbPool::connect_with(
            AnyPoolOptions::new()
                .max_connections(max_connections.clamp(1, 64))
                // Keep a few warm connections for network backends; SQLite
                // opens are cheap and should not pin extra file handles.
                .min_connections(if kind == DbKind::Sqlite {
                    0
                } else {
                    max_connections.min(4)
                })
                .idle_timeout(Duration::from_secs(60))
                .max_lifetime(Duration::from_secs(30 * 60))
                .acquire_timeout(Duration::from_secs(15)),
            &effective,
        )
        .await
        .map_err(|e| DbError::Message(format!("connect app database: {e}")))?;
        schema::ensure_schema(&pool, kind, &[])
            .await
            .map_err(|e| DbError::Message(format!("ensure schema: {e}")))?;
        tracing::info!(kind = kind.as_str(), "app database ready");
        Ok(Self {
            pool,
            kind,
            dsn: effective,
        })
    }

    /// Returns the underlying pool for repositories.
    /// 返回供仓储使用的底层连接池。
    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    pub fn kind(&self) -> DbKind {
        self.kind
    }

    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// Closes the pool (used when re-opening the app database with another DSN).
    /// 关闭连接池（切换 DSN 重开时调用）。
    pub async fn shutdown(&self) {
        self.pool.close().await;
    }
}
