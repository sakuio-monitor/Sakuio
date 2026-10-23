//! Cross-database metric migration: legacy raw-sample re-aggregation and
//! bucket copying, powering merge (split storage -> one database) and split flows.
//! 跨库指标迁移：旧原始采样重聚合与桶直搬，支持合并库与独立库互转。

use crate::aggregate::{BucketAggregator, RetentionPolicy};
use crate::config::MetricDbConfig;
use crate::store::{BucketRow, MetricStore};
use sakuio_db::DbPool;
use sakuio_protocol::TelemetrySample;
use std::sync::Arc;

/// Runs a metrics migration in the foreground; callers own concurrency gating
/// and background spawning.
/// 前台执行指标迁移；并发互斥与后台派发由调用方负责。
/// `legacy_samples` = the source still holds the old `metric_samples` raw table.
pub async fn run_metrics_migration(
    target: &Arc<MetricStore>,
    source_pool: DbPool,
    legacy_samples: bool,
    prefix: String,
    progress: impl Fn(u64, &str) + Send + Sync,
) -> Result<(), String> {
    if legacy_samples {
        migrate_legacy_samples(target, &source_pool, &progress).await
    } else {
        migrate_buckets(target, &source_pool, &prefix, &progress).await
    }
}

/// Copies every bucket from one store to another (switch-time background sync).
/// 把一个监控库的全部桶同步到另一个库（切换监控库时的后台异步同步）。
pub async fn run_bucket_sync(
    source: &MetricStore,
    target: &MetricStore,
    progress: impl Fn(u64, &str) + Send + Sync,
) -> Result<(), String> {
    let mut last_id = 0i64;
    let mut total = 0u64;
    loop {
        let rows = source
            .page_buckets(last_id, 1000)
            .await
            .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            break;
        }
        last_id = rows.last().map(|(id, _)| *id).unwrap_or(last_id);
        let batch: Vec<BucketRow> = rows.into_iter().map(|(_, row)| row).collect();
        target
            .insert_buckets(&batch)
            .await
            .map_err(|e| e.to_string())?;
        total += batch.len() as u64;
        progress(total, "syncing buckets");
    }
    Ok(())
}

/// Phase: copy existing bucket rows in id-ascending batches.
/// 阶段：按 id 升序分批搬运已有桶行。
async fn migrate_buckets(
    target: &Arc<MetricStore>,
    source: &DbPool,
    prefix: &str,
    progress: impl Fn(u64, &str),
) -> Result<(), String> {
    let table = format!("{prefix}buckets");
    let mut last_id = 0i64;
    let mut total = 0u64;
    loop {
        let rows: Vec<(i64, String, String, i64, i64, f64, f64, f64, i64)> =
            sqlx::query_as(&format!(
                "SELECT id, node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
             avg_val, min_val, max_val, sample_count FROM {table} \
             WHERE id > ? ORDER BY id LIMIT 1000"
            ))
            .bind(last_id)
            .fetch_all(source)
            .await
            .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            break;
        }
        let mut batch: Vec<BucketRow> = Vec::with_capacity(rows.len());
        for (
            id,
            node_uuid,
            metric_key,
            bucket_seconds,
            bucket_start_ms,
            avg_val,
            min_val,
            max_val,
            sample_count,
        ) in rows
        {
            last_id = id;
            batch.push(BucketRow {
                node_uuid,
                metric_key,
                bucket_seconds,
                bucket_start_ms,
                avg_val,
                min_val,
                max_val,
                sample_count,
            });
        }
        target
            .insert_buckets(&batch)
            .await
            .map_err(|e| e.to_string())?;
        total += batch.len() as u64;
        progress(total, &format!("copying buckets ({total} rows)"));
    }
    Ok(())
}

/// Phase: re-aggregate legacy `metric_samples` payloads into 1-minute buckets.
/// 阶段：把旧 `metric_samples` 原始采样重聚合成 1 分钟桶。
async fn migrate_legacy_samples(
    target: &Arc<MetricStore>,
    source: &DbPool,
    progress: impl Fn(u64, &str),
) -> Result<(), String> {
    let mut last_id = 0i64;
    let mut total = 0u64;
    let mut aggregator = BucketAggregator::default();
    let policy = RetentionPolicy::default();
    loop {
        let rows: Vec<(i64, String, String)> = sqlx::query_as(&format!(
            "SELECT id, node_uuid, payload_json FROM metric_samples \
             WHERE id > ? ORDER BY id LIMIT 1000"
        ))
        .bind(last_id)
        .fetch_all(source)
        .await
        .map_err(|e| e.to_string())?;
        if rows.is_empty() {
            break;
        }
        let mut batch: Vec<BucketRow> = Vec::new();
        for (id, node_uuid, payload_json) in rows {
            last_id = id;
            let Ok(sample) = serde_json::from_str::<TelemetrySample>(&payload_json) else {
                continue;
            };
            batch.extend(aggregator.push(&node_uuid, &sample, &policy));
        }
        if !batch.is_empty() {
            target
                .insert_buckets(&batch)
                .await
                .map_err(|e| e.to_string())?;
            total += batch.len() as u64;
        }
        progress(
            total,
            &format!("re-aggregating legacy samples ({total} buckets)"),
        );
    }
    let tail = aggregator.flush_all();
    if !tail.is_empty() {
        target
            .insert_buckets(&tail)
            .await
            .map_err(|e| e.to_string())?;
        total += tail.len() as u64;
    }
    progress(total, "re-aggregated legacy samples");
    Ok(())
}

/// Resolves the migration source: empty DSN = previous monitoring database
/// (the main app store, which still holds the legacy `metric_samples` table).
/// 迁移源解析：DSN 留空 = 上一个监控库（主库，含旧 metric_samples 表）。
pub async fn connect_source(
    main_pool: &DbPool,
    source_dsn: Option<&str>,
    cfg: &MetricDbConfig,
) -> Result<(DbPool, bool, bool), String> {
    // (pool, legacy_samples, close_after)
    let dsn = source_dsn.map(str::trim).filter(|s| !s.is_empty());
    let Some(dsn) = dsn else {
        return Ok((main_pool.clone(), true, false));
    };
    if dsn == cfg.dsn {
        return Err("source and target databases are identical".into());
    }
    let pool = DbPool::connect(dsn).await.map_err(|e| e.to_string())?;
    Ok((pool, false, true))
}
