//! Monitoring database configuration persisted in system settings.
//! 持久化在系统设置中的监控数据库配置。

use crate::store::MetricKind;
use sakuio_db::{quote_ident, DbPool};
use serde::{Deserialize, Serialize};

const SETTINGS_KEY: &str = "metric_db";

/// `key` is reserved in MySQL and must be quoted there.
/// `key` 在 MySQL 中是保留字，需要反引号。
fn key_column(pool: &DbPool) -> String {
    quote_ident(pool.kind(), "key")
}

/// Monitoring database configuration; empty DSN = colocated with the main database.
/// 监控数据库配置；DSN 留空 = 与主数据库合并存储。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricDbConfig {
    /// Empty = colocated with the main (SQLite) database.
    /// 留空 = 与主库（SQLite）合并存储。
    pub dsn: String,
    pub table_prefix: String,
    pub max_connections: u32,
    pub min_idle_connections: u32,
    /// 1-minute bucket retention in minutes.
    /// 1 分钟桶保留时间（分钟）。
    pub bucket_1m_minutes: i64,
    /// 5-minute bucket retention in minutes.
    /// 5 分钟桶保留时间（分钟）。
    pub bucket_5m_minutes: i64,
    /// 1-hour bucket retention in hours.
    /// 1 小时桶保留时间（小时）。
    pub bucket_1h_hours: i64,
    // Daily-bucket retention follows each metric's retention days.
    // 日桶保留遵循各指标的保留天数，无额外全局上限。
}

impl Default for MetricDbConfig {
    fn default() -> Self {
        Self {
            dsn: String::new(),
            table_prefix: "metric_".into(),
            max_connections: 25,
            min_idle_connections: 5,
            bucket_1m_minutes: 600,
            bucket_5m_minutes: 3000,
            bucket_1h_hours: 600,
        }
    }
}

impl MetricDbConfig {
    /// Loads the config from `system_settings`, falling back to defaults.
    /// 从 `system_settings` 读取配置，缺省时返回默认值。
    pub async fn load(pool: &DbPool) -> Result<Self, sqlx::Error> {
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT value FROM system_settings WHERE {} = ?",
            key_column(pool)
        ))
        .bind(SETTINGS_KEY)
        .fetch_optional(pool)
        .await?;
        match row {
            Some((raw,)) => Ok(serde_json::from_str(&raw).unwrap_or_default()),
            None => Ok(Self::default()),
        }
    }

    /// Persists the config into `system_settings`.
    /// 将配置写回 `system_settings`。
    pub async fn save(&self, pool: &DbPool) -> Result<(), sqlx::Error> {
        let raw = serde_json::to_string(self).map_err(|e| {
            tracing::error!(error = %e, "serialize metric db config");
            sqlx::Error::ColumnDecode {
                index: "metric_db".into(),
                source: Box::new(e),
            }
        })?;
        let key = key_column(pool);
        let mut tx = pool.begin().await?;
        let updated = sqlx::query(&format!(
            "UPDATE system_settings SET value = ? WHERE {key} = ?"
        ))
        .bind(raw.clone())
        .bind(SETTINGS_KEY)
        .execute(&mut tx)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query(&format!(
                "INSERT INTO system_settings ({key}, value) VALUES (?, ?)"
            ))
            .bind(SETTINGS_KEY)
            .bind(raw)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Resolves the effective monitoring database kind.
    /// 解析当前监控库类型。
    pub fn kind(&self) -> MetricKind {
        match sakuio_db::DbKind::detect(&self.dsn) {
            sakuio_db::DbKind::MySql => MetricKind::MySql,
            sakuio_db::DbKind::Postgres => MetricKind::Postgres,
            sakuio_db::DbKind::Sqlite if self.dsn.trim().is_empty() => MetricKind::Merged,
            sakuio_db::DbKind::Sqlite => MetricKind::Sqlite,
        }
    }
}
