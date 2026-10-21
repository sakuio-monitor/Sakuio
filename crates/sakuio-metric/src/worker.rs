use crate::store::MetricStore;
use sakuio_protocol::TelemetrySample;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;

/// One ingest job for the background metric writer.
/// 后台指标写入器的一条 ingest 任务。
#[derive(Debug, Clone)]
pub struct MetricIngestJob {
    pub node_uuid: String,
    pub samples: Vec<TelemetrySample>,
}

/// Configuration for the bounded metric queue.
/// 有界指标队列配置。
#[derive(Debug, Clone)]
pub struct MetricWriterConfig {
    pub queue_capacity: usize,
}

impl Default for MetricWriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4096,
        }
    }
}

/// Bounded async writer: ingest path only enqueues and returns immediately.
/// 有界异步写入器：ingest 路径仅入队并立即返回。
pub struct MetricWriter {
    tx: mpsc::Sender<MetricIngestJob>,
    dropped: Arc<AtomicU64>,
}

impl MetricWriter {
    /// Spawns the background task and returns the enqueue handle.
    /// 启动后台任务并返回入队句柄。
    pub fn start(pool: sqlx::SqlitePool, config: MetricWriterConfig) -> Self {
        let (tx, mut rx) = mpsc::channel::<MetricIngestJob>(config.queue_capacity);
        let dropped = Arc::new(AtomicU64::new(0));
        let dropped_worker = dropped.clone();
        tokio::spawn(async move {
            let store = MetricStore::new(pool);
            while let Some(job) = rx.recv().await {
                if let Err(e) = store.insert_samples(&job.node_uuid, &job.samples).await {
                    tracing::error!(
                        node_uuid = %job.node_uuid,
                        error = %e,
                        sample_count = job.samples.len(),
                        "metric batch write failed"
                    );
                }
            }
            tracing::info!("metric writer stopped");
            let _ = dropped_worker;
        });
        Self { tx, dropped }
    }

    /// Enqueues samples; drops when the queue is full and increments counter.
    /// 将采样入队；队列满时丢弃并递增计数。
    pub fn enqueue(&self, node_uuid: String, samples: Vec<TelemetrySample>) {
        if samples.is_empty() {
            return;
        }
        match self.tx.try_send(MetricIngestJob { node_uuid, samples }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if n % 100 == 1 {
                    tracing::warn!(
                        dropped_batches = n,
                        "metric ingest queue full, dropping batch"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("metric ingest queue closed");
            }
        }
    }

    /// Returns how many batches were dropped due to backpressure.
    /// 返回因背压丢弃的批次数。
    pub fn dropped_batches(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
