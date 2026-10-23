//! Bounded ingest queue with windowed aggregation, batch flushing, and cleanup.
//! 有界 ingest 队列：窗口聚合、批量落库与定时清理。

use crate::aggregate::{BucketAggregator, RetentionPolicy};
use crate::config::MetricDbConfig;
use crate::store::MetricStore;
use sakuio_db::DbPool;
use sakuio_protocol::TelemetrySample;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, RwLock};

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

/// Command handled by the writer task.
/// 写入任务处理的命令。
enum WriterCommand {
    /// Flush in-memory buckets (including unfinished windows) into a specific
    /// store; used before a main-database switch restarts the process.
    /// 把内存中的桶（含未完成窗口）写入指定存储；用于主库切换重启前落盘。
    Flush {
        store: Arc<MetricStore>,
        reply: oneshot::Sender<Result<u64, String>>,
    },
}

/// Runtime handle for hot-swapping the metric store after a config change.
/// 配置变更后热切换监控存储的运行时句柄。
#[derive(Clone)]
pub struct MetricWriterHandle {
    store_tx: watch::Sender<Arc<MetricStore>>,
    policy: Arc<RwLock<Arc<RetentionPolicy>>>,
    control_tx: mpsc::Sender<WriterCommand>,
}

impl MetricWriterHandle {
    /// Replaces the active store and returns the old one; the caller owns
    /// closing it (a background sync may still need to read from it).
    /// 替换当前存储并返回旧存储；由调用方负责关闭（后台同步可能还要读旧库）。
    pub async fn swap_store(&self, store: MetricStore) -> Arc<MetricStore> {
        self.store_tx.send_replace(Arc::new(store))
    }

    /// Subscribe handle for readers of the active store.
    /// 当前存储的只读订阅端。
    pub fn receiver(&self) -> watch::Receiver<Arc<MetricStore>> {
        self.store_tx.subscribe()
    }

    /// Refreshes the cached retention policy from the store.
    /// 从存储刷新缓存的保留策略。
    pub async fn reload_policy(&self, store: &MetricStore) {
        let map: RetentionPolicy = store
            .retention_days()
            .await
            .unwrap_or_default()
            .into_iter()
            .collect();
        *self.policy.write().await = Arc::new(map);
    }

    pub async fn policy(&self) -> Arc<RetentionPolicy> {
        self.policy.read().await.clone()
    }

    /// Asks the writer to flush everything it holds into `store`, including
    /// partially filled windows, and waits for the result.
    /// 让写入器把内存中的全部桶（含未填满窗口）写入 `store` 并等待结果。
    pub async fn flush_to_store(&self, store: Arc<MetricStore>) -> Result<u64, String> {
        let (reply, result) = oneshot::channel();
        self.control_tx
            .send(WriterCommand::Flush { store, reply })
            .await
            .map_err(|_| "metric writer stopped".to_string())?;
        match tokio::time::timeout(std::time::Duration::from_secs(20), result).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("metric writer dropped flush reply".into()),
            Err(_) => Err("metric writer flush timed out".into()),
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
    /// Spawns the writer, aggregator, and cleanup loops.
    /// 启动写入、聚合与清理循环。
    pub fn start(
        main_pool: DbPool,
        store: Arc<MetricStore>,
        config: MetricWriterConfig,
    ) -> (Self, MetricWriterHandle) {
        let (tx, mut rx) = mpsc::channel::<MetricIngestJob>(config.queue_capacity);
        let (control_tx, mut control_rx) = mpsc::channel::<WriterCommand>(4);
        let dropped = Arc::new(AtomicU64::new(0));
        let (store_tx, store_rx) = watch::channel(store);
        let policy: Arc<RwLock<Arc<RetentionPolicy>>> = Arc::new(RwLock::new(Arc::default()));
        let handle = MetricWriterHandle {
            store_tx,
            policy: policy.clone(),
            control_tx,
        };
        let cleanup_rx = store_rx.clone();
        let cleanup_pool = main_pool.clone();

        let dropped_worker = dropped.clone();
        tokio::spawn(async move {
            let mut aggregator = BucketAggregator::default();
            let mut pending: Vec<crate::store::BucketRow> = Vec::new();
            let mut current: Arc<MetricStore> = store_rx.borrow().clone();
            let mut last_flush = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    // Prefer draining queued samples before handling a flush
                    // command, otherwise a switch could skip queued telemetry.
                    // 先排空已入队采样再处理 flush，避免切换漏掉落库。
                    biased;
                    job = rx.recv() => {
                        let Some(job) = job else {
                            flush_pending(&current, &mut pending).await;
                            tracing::info!("metric writer stopped");
                            let _ = dropped_worker;
                            return;
                        };
                        current = store_rx.borrow().clone();
                        let policy_snapshot = policy.read().await.clone();
                        for sample in &job.samples {
                            pending.extend(aggregator.push(&job.node_uuid, sample, &policy_snapshot));
                        }
                        if pending.len() >= 500 || last_flush.elapsed() >= std::time::Duration::from_secs(5) {
                            flush_pending(&current, &mut pending).await;
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                    command = control_rx.recv() => {
                        let Some(command) = command else {
                            continue;
                        };
                        match command {
                            WriterCommand::Flush { store, reply } => {
                                pending.extend(aggregator.flush_all());
                                let result = write_pending(&store, &mut pending).await;
                                if let Err(e) = &result {
                                    tracing::warn!(error = %e, "flush in-memory buckets to target store failed");
                                }
                                let _ = reply.send(result);
                            }
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                        if !pending.is_empty() {
                            current = store_rx.borrow().clone();
                            flush_pending(&current, &mut pending).await;
                        }
                        last_flush = tokio::time::Instant::now();
                    }
                }
            }
        });

        // Hourly retention cleanup against the active store.
        // 每小时按保留配置清理过期桶。
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3_600)).await;
                if sakuio_db::maintenance_active() {
                    continue;
                }
                let store = cleanup_rx.borrow().clone();
                let Ok(cfg) = MetricDbConfig::load(&cleanup_pool).await else {
                    continue;
                };
                let days: std::collections::HashMap<String, i64> = store
                    .retention_days()
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                let now_ms = chrono::Utc::now().timestamp_millis();
                match store.cleanup(&cfg, &days, now_ms).await {
                    Ok(n) if n > 0 => {
                        tracing::info!(deleted = n, "metric bucket cleanup completed");
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "metric bucket cleanup failed"),
                }
            }
        });

        (Self { tx, dropped }, handle)
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

async fn flush_pending(store: &MetricStore, pending: &mut Vec<crate::store::BucketRow>) {
    if pending.is_empty() {
        return;
    }
    // Keep completed buckets in memory while the main database (or a colocated
    // metric table) is being copied; they flush after the switch.
    // 主库/合并指标表复制期间把已完成的桶留在内存，切换后继续落盘。
    if sakuio_db::maintenance_active() {
        cap_pending(pending);
        return;
    }
    if let Err(e) = write_pending(store, pending).await {
        tracing::error!(error = %e, "metric bucket flush failed");
        cap_pending(pending);
    }
}

/// Writes pending rows, restoring them on failure so a later flush retries.
/// 写入待落盘桶；失败时放回队列，等待下次重试。
async fn write_pending(
    store: &MetricStore,
    pending: &mut Vec<crate::store::BucketRow>,
) -> Result<u64, String> {
    if pending.is_empty() {
        return Ok(0);
    }
    let rows = std::mem::take(pending);
    let count = rows.len() as u64;
    match store.insert_buckets(&rows).await {
        Ok(()) => Ok(count),
        Err(e) => {
            pending.splice(0..0, rows);
            Err(e.to_string())
        }
    }
}

/// Bounds the in-memory backlog if maintenance or a persistent write error
/// keeps rows from reaching the database.
/// 维护或持续写入失败时限制内存积压。
fn cap_pending(pending: &mut Vec<crate::store::BucketRow>) {
    const MAX_PENDING: usize = 50_000;
    if pending.len() > MAX_PENDING {
        let excess = pending.len() - MAX_PENDING;
        pending.drain(..excess);
        tracing::warn!(
            dropped = excess,
            "metric backlog exceeded, dropping oldest buckets"
        );
    }
}
