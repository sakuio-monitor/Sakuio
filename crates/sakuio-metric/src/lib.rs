//! Metric persistence and async ingest worker.
//! 指标持久化与异步 ingest 工作线程。

mod store;
mod worker;

pub use store::MetricStore;
pub use worker::{MetricIngestJob, MetricWriter, MetricWriterConfig};
