//! Metric persistence: windowed aggregation, multi-backend store, and migration.
//! 指标持久化：窗口降采样、多后端存储与跨库迁移。

mod aggregate;
mod config;
mod keys;
mod migrate;
mod store;
mod worker;

pub use aggregate::{BucketAggregator, RetentionPolicy, WINDOW_SECONDS};
pub use config::MetricDbConfig;
pub use keys::{sample_value, MetricDef, TrafficDelta, METRIC_KEYS};
pub use migrate::{connect_source, run_bucket_sync, run_metrics_migration};
pub use store::{BucketRow, MetricKind, MetricStore};
pub use worker::{MetricIngestJob, MetricWriter, MetricWriterConfig, MetricWriterHandle};

/// Errors from the metric subsystem.
/// 指标子系统错误。
#[derive(Debug, thiserror::Error)]
pub enum MetricError {
    #[error("database: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("{0}")]
    Message(String),
}

/// Errors surfaced while connecting the monitoring database.
/// 监控库连接错误。
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("connect monitoring database: {0}")]
    Connect(String),
}

pub type MetricResult<T> = Result<T, MetricError>;
