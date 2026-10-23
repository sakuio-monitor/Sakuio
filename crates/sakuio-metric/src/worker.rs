//! Bounded ingest queue with windowed aggregation, batch flushing, and cleanup.
//! 有界 ingest 队列：窗口聚合、批量落库与定时清理。

use crate::aggregate::{BucketAggregator, RetentionPolicy};
use crate::config::MetricDbConfig;
use crate::store::MetricStore;
use sakuio_db::DbPool;
use sakuio_protocol::TelemetrySample;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
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
    workers: Arc<StdRwLock<WorkerSet>>,
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
        let senders = self
            .workers
            .read()
            .map_err(|_| "metric worker lock poisoned".to_string())?
            .control
            .clone();
        if senders.is_empty() {
            return Err("metric writer stopped".into());
        }
        let mut replies = Vec::with_capacity(senders.len());
        for tx in senders {
            let (reply, result) = oneshot::channel();
            tx.send(WriterCommand::Flush { store: store.clone(), reply })
                .await
                .map_err(|_| "metric writer stopped".to_string())?;
            replies.push(result);
        }
        let mut total = 0u64;
        for result in replies {
            match tokio::time::timeout(std::time::Duration::from_secs(20), result).await {
                Ok(Ok(Ok(n))) => total += n,
                Ok(Ok(Err(e))) => return Err(e),
                Ok(Err(_)) => return Err("metric writer dropped flush reply".into()),
                Err(_) => return Err("metric writer flush timed out".into()),
            }
        }
        Ok(total)
    }
}

/// Bounded async writer: ingest path only enqueues and returns immediately.
/// 有界异步写入器：ingest 路径仅入队并立即返回。
pub struct MetricWriter {
    workers: Arc<StdRwLock<WorkerSet>>,
    overflow: Overflow,
    dropped: Arc<AtomicU64>,
}

type Overflow = Arc<std::sync::Mutex<std::collections::HashMap<String, Vec<TelemetrySample>>>>;

struct WorkerSet {
    ingest: Vec<mpsc::Sender<MetricIngestJob>>,
    control: Vec<mpsc::Sender<WriterCommand>>,
}

impl MetricWriter {
    /// Spawns a sharded, autoscaling writer pool.
    pub fn start(
        main_pool: DbPool,
        store: Arc<MetricStore>,
        config: MetricWriterConfig,
    ) -> (Self, MetricWriterHandle) {
        let max_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .max(1);
        let capacity = config.queue_capacity.max(64);
        let (ingest_tx, ingest_rx) = mpsc::channel::<MetricIngestJob>(capacity);
        let (control_tx, control_rx) = mpsc::channel::<WriterCommand>(4);
        let workers = Arc::new(StdRwLock::new(WorkerSet {
            ingest: vec![ingest_tx],
            control: vec![control_tx],
        }));
        let dropped = Arc::new(AtomicU64::new(0));
        let overflow: Overflow = Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let (store_tx, store_rx) = watch::channel(store);
        let policy: Arc<RwLock<Arc<RetentionPolicy>>> = Arc::new(RwLock::new(Arc::default()));
        let handle = MetricWriterHandle {
            store_tx: store_tx.clone(),
            policy: policy.clone(),
            workers: workers.clone(),
        };
        spawn_worker(
            ingest_rx,
            control_rx,
            store_rx.clone(),
            policy.clone(),
            dropped.clone(),
        );

        // Autoscaler: one worker starts immediately, additional workers are
        // added only while queues are backing up, capped at logical CPU count.
        let workers_scale = workers.clone();
        let store_scale = store_rx.clone();
        let policy_scale = policy.clone();
        let dropped_scale = dropped.clone();
        let overflow_scale = overflow.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                let mut guard = match workers_scale.write() {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
                let load: usize = guard
                    .ingest
                    .iter()
                    .map(|tx| tx.max_capacity().saturating_sub(tx.capacity()))
                    .sum();
                if guard.ingest.len() < max_workers && load > capacity / 4 {
                    let (tx, rx) = mpsc::channel::<MetricIngestJob>(capacity);
                    let (ctl_tx, ctl_rx) = mpsc::channel::<WriterCommand>(4);
                    guard.ingest.push(tx);
                    guard.control.push(ctl_tx);
                    spawn_worker(rx, ctl_rx, store_scale.clone(), policy_scale.clone(), dropped_scale.clone());
                }
                drop(guard);

                // Coalesced overflow is re-injected when capacity opens up.
                let pending: Vec<(String, Vec<TelemetrySample>)> = {
                    match overflow_scale.lock() {
                        Ok(mut map) => map.drain().collect(),
                        Err(_) => Vec::new(),
                    }
                };
                for (node, samples) in pending {
                    // On failure try_enqueue puts the job back into the
                    // overflow map; keep draining the rest instead of losing
                    // the remaining entries.
                    let _ = try_enqueue(&workers_scale, &overflow_scale, node, samples);
                }
            }
        });

        // Hourly retention cleanup against the active store.
        let cleanup_rx = store_rx;
        let cleanup_pool = main_pool;
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

        (Self { workers, overflow, dropped }, handle)
    }

/// Enqueues samples; drops when the queue is full and increments counter.
    /// 将采样入队；队列满时丢弃并递增计数。
    pub fn enqueue(&self, node_uuid: String, samples: Vec<TelemetrySample>) {
        if samples.is_empty() {
            return;
        }
        if !try_enqueue(&self.workers, &self.overflow, node_uuid, samples) {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n % 100 == 1 {
                tracing::warn!(dropped_batches = n, "metric ingest overflow, coalescing latest samples");
            }
        }
    }

    /// Returns how many batches were dropped due to backpressure.
    /// 返回因背压丢弃的批次数。
    pub fn dropped_batches(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn shard_index(node_uuid: &str, shards: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node_uuid.hash(&mut hasher);
    (hasher.finish() as usize) % shards.max(1)
}

fn try_enqueue(
    workers: &Arc<StdRwLock<WorkerSet>>,
    overflow: &Overflow,
    node_uuid: String,
    samples: Vec<TelemetrySample>,
) -> bool {
    let guard = match workers.read() {
        Ok(guard) => guard,
        Err(_) => return false,
    };
    if guard.ingest.is_empty() {
        drop(guard);
        return false;
    }
    let start = shard_index(&node_uuid, guard.ingest.len());
    let mut job = Some(MetricIngestJob { node_uuid: node_uuid.clone(), samples });
    for offset in 0..guard.ingest.len() {
        let tx = &guard.ingest[(start + offset) % guard.ingest.len()];
        match tx.try_send(job.take().expect("metric job")) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(returned)) => job = Some(returned),
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
    drop(guard);

    // Every shard is full: keep only the newest samples per node instead of
    // dropping the whole batch. This is bounded by the number of nodes.
    if let Some(job) = job {
        if let Ok(mut map) = overflow.lock() {
            let entry = map.entry(node_uuid).or_default();
            entry.extend(job.samples);
            const MAX_OVERFLOW_SAMPLES: usize = 120;
            if entry.len() > MAX_OVERFLOW_SAMPLES {
                let excess = entry.len() - MAX_OVERFLOW_SAMPLES;
                entry.drain(..excess);
            }
        }
    }
    false
}

fn spawn_worker(
    mut rx: mpsc::Receiver<MetricIngestJob>,
    mut control_rx: mpsc::Receiver<WriterCommand>,
    store_rx: watch::Receiver<Arc<MetricStore>>,
    policy: Arc<RwLock<Arc<RetentionPolicy>>>,
    dropped: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        let mut aggregator = BucketAggregator::default();
        let mut pending: Vec<crate::store::BucketRow> = Vec::new();
        let mut current: Arc<MetricStore> = store_rx.borrow().clone();
        let mut last_flush = tokio::time::Instant::now();
        loop {
            tokio::select! {
                biased;
                job = rx.recv() => {
                    let Some(job) = job else {
                        flush_pending(&current, &mut pending).await;
                        tracing::info!("metric worker stopped");
                        let _ = dropped;
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
                    let Some(command) = command else { continue; };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_index_is_stable_and_in_range() {
        assert_eq!(shard_index("node-a", 1), 0);
        let a = shard_index("node-a", 8);
        assert_eq!(a, shard_index("node-a", 8));
        assert!(a < 8);
        let mut seen = std::collections::HashSet::new();
        for i in 0..100 {
            seen.insert(shard_index(&format!("node-{i}"), 8));
        }
        assert!(seen.len() > 1);
    }
}
