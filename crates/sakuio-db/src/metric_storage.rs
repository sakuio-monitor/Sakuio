//! Optional TimescaleDB storage setup for metric buckets.
//! 指标桶的可选 TimescaleDB 存储初始化。

use crate::{DbKind, DbPool};

/// Converts `metric_buckets` into a Timescale hypertable when the extension is
/// installed. On PostgreSQL without Timescale (and on SQLite/MySQL) this is a
/// no-op, so regular SQL storage continues to work.
pub async fn ensure_timescale(pool: &DbPool, prefix: &str) -> Result<(), sqlx::Error> {
    if pool.kind() != DbKind::Postgres {
        return Ok(());
    }
    let installed: Option<String> =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            .fetch_optional(pool)
            .await
            .unwrap_or(None);
    if installed.is_none() {
        tracing::info!("TimescaleDB not installed; metric buckets use regular PostgreSQL tables");
        return Ok(());
    }
    let table = format!("{prefix}buckets");
    let sql = format!(
        "SELECT create_hypertable('{table}', 'bucket_start_ms', \
         if_not_exists => TRUE, migrate_data => TRUE)"
    );
    if let Err(error) = sqlx::query(&sql).execute(pool).await {
        tracing::warn!(table = %table, error = %error, "create Timescale hypertable failed");
    } else {
        tracing::info!(table = %table, "metric buckets are a Timescale hypertable");
    }
    Ok(())
}
