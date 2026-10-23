//! Bounded, batched audit writer plus periodic retention cleanup.
//! 有界队列、批量写入的审计记录器，并定期按保留期清理。

use crate::geoip_resolver::GeoEngine;
use chrono::{Duration, SecondsFormat, Utc};
use sakuio_db::{AuditLogRepository, DbPool, NewAuditLog};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

const QUEUE_CAPACITY: usize = 8192;
const BATCH_SIZE: usize = 64;
const FLUSH_INTERVAL_MS: u64 = 250;
const DELETE_BATCH: i64 = 500;

#[derive(Clone)]
pub struct AuditLogger {
    tx: mpsc::Sender<NewAuditLog>,
    dropped: Arc<AtomicU64>,
}

impl AuditLogger {
    /// Starts the batched writer and retention task.
    /// 启动批量写入与保留期清理任务。
    pub fn start(pool: DbPool, geo: std::sync::Arc<GeoEngine>) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let writer_pool = pool.clone();
        tokio::spawn(async move {
            writer_loop(writer_pool, geo, rx).await;
        });

        let cleanup_pool = pool.clone();
        tokio::spawn(async move {
            // Give the server time to finish startup before touching the table.
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            loop {
                let settings = crate::site_settings::load_from_pool(&cleanup_pool).await;
                if settings.audit_retention_days > 0 {
                    cleanup_once(&cleanup_pool, settings.audit_retention_days).await;
                }
                tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
            }
        });
        tracing::info!("audit log retention scheduler started");

        Self { tx, dropped }
    }

    /// Queues a record without blocking the request path. When the queue is
    /// saturated records are dropped and counted instead of growing memory.
    /// 非阻塞入队；队列满时丢弃并计数，避免请求路径被拖慢或内存膨胀。
    pub fn record(&self, entry: NewAuditLog) {
        if let Err(error) = self.tx.try_send(entry) {
            let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if total == 1 || total % 1000 == 0 {
                tracing::warn!(dropped = total, error = %error, "audit log queue saturated; records dropped");
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

async fn writer_loop(
    pool: DbPool,
    geo: std::sync::Arc<GeoEngine>,
    mut rx: mpsc::Receiver<NewAuditLog>,
) {
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(FLUSH_INTERVAL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(entry) => {
                        batch.push(entry);
                        if batch.len() >= BATCH_SIZE {
                            flush(&pool, &geo, &mut batch).await;
                        }
                    }
                    None => {
                        flush(&pool, &geo, &mut batch).await;
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                flush(&pool, &geo, &mut batch).await;
            }
        }
    }
}

async fn flush(pool: &DbPool, geo: &GeoEngine, batch: &mut Vec<NewAuditLog>) {
    if batch.is_empty() {
        return;
    }
    let settings = crate::site_settings::load_from_pool(pool).await;
    if settings.geoip_enabled {
        for entry in batch.iter_mut() {
            if entry.ip.is_empty()
                || !entry.geo_location.is_empty()
                || sakuio_notify::ipgeo::is_private_ip(&entry.ip)
            {
                continue;
            }
            if let Some(info) = geo.lookup(&entry.ip, &settings.geoip_provider).await {
                entry.geo_country_code = info.country_code;
                entry.geo_location = info.location;
            }
        }
    }
    let rows = std::mem::take(batch);
    if let Err(error) = AuditLogRepository::new(pool).insert_batch(&rows).await {
        tracing::error!(count = rows.len(), error = %error, "audit log batch write failed");
    }
}

pub async fn prune_older_than(pool: &DbPool, retention_days: i64) {
    if retention_days > 0 {
        cleanup_once(pool, retention_days).await;
    }
}

async fn cleanup_once(pool: &DbPool, retention_days: i64) {
    let cutoff =
        (Utc::now() - Duration::days(retention_days)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let repo = AuditLogRepository::new(pool);
    let mut total = 0u64;
    loop {
        match repo.delete_before(&cutoff, DELETE_BATCH).await {
            Ok(0) => break,
            Ok(deleted) => {
                total += deleted;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(error) => {
                tracing::warn!(error = %error, "audit log retention cleanup failed");
                break;
            }
        }
    }
    if total > 0 {
        tracing::info!(deleted = total, cutoff = %cutoff, "audit log retention cleanup finished");
    }
}

/// Consistent UTC timestamp used by all audit records.
pub fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}
