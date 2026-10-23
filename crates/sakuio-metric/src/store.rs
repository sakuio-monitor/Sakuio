//! Metric store: bucket tables and retention metadata over a portable pool.
//! 指标存储：桶表与保留元数据（可移植连接池）。

use crate::config::MetricDbConfig;
use crate::MetricError;
use sakuio_db::{DbKind, DbPool};
use sqlx::any::AnyPoolOptions;

/// Rows per multi-value INSERT; 8 binds each stays far below every backend's bind limit.
/// 单条多值 INSERT 的行数；每行 8 个参数，远低于各后端参数上限。
const INSERT_CHUNK: usize = 200;

/// One aggregated metric bucket row.
/// 聚合指标桶行。
#[derive(Debug, Clone)]
pub struct BucketRow {
    pub node_uuid: String,
    pub metric_key: String,
    pub bucket_seconds: i64,
    pub bucket_start_ms: i64,
    pub avg_val: f64,
    pub min_val: f64,
    pub max_val: f64,
    pub sample_count: i64,
}

/// Monitoring database kind, mirrored from the app layer.
/// 监控数据库类型（与应用层保持一致）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Merged,
    Sqlite,
    MySql,
    Postgres,
}

impl MetricKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Merged => "merged",
            Self::Sqlite => "sqlite",
            Self::MySql => "mysql",
            Self::Postgres => "postgres",
        }
    }
}

/// Handle to the monitoring database.
/// 监控数据库句柄。
#[derive(Clone)]
pub struct MetricStore {
    pool: DbPool,
    /// Read-only pool for history queries; separate from the writer pool so a
    /// flush or a large scan cannot exhaust all connections.
    query_pool: Option<DbPool>,
    prefix: String,
    kind: MetricKind,
    dialect: DbKind,
}

impl MetricStore {
    /// Connects and ensures the declarative bucket/retention schema.
    /// 建立连接并确保桶/保留表的声明式结构。
    pub async fn connect(
        main_pool: &DbPool,
        cfg: &MetricDbConfig,
    ) -> Result<Self, crate::PoolError> {
        let prefix = cfg.table_prefix.clone();
        if cfg.dsn.trim().is_empty() {
            sakuio_db::ensure_metric_schema(main_pool, &prefix)
                .await
                .map_err(|e| crate::PoolError::Connect(e.to_string()))?;
            return Ok(Self {
                pool: main_pool.clone(),
                query_pool: None,
                prefix,
                kind: MetricKind::Merged,
                dialect: main_pool.kind(),
            });
        }
        let dialect = DbKind::detect(&cfg.dsn);
        let kind = match dialect {
            DbKind::MySql => MetricKind::MySql,
            DbKind::Postgres => MetricKind::Postgres,
            DbKind::Sqlite => MetricKind::Sqlite,
        };
        let pool = DbPool::connect_with(
            AnyPoolOptions::new()
                .max_connections(cfg.max_connections.max(1))
                .min_connections(cfg.min_idle_connections.min(cfg.max_connections.max(1)))
                .acquire_timeout(std::time::Duration::from_secs(15)),
            &cfg.dsn,
        )
        .await
        .map_err(|e| crate::PoolError::Connect(e.to_string()))?;
        sakuio_db::ensure_metric_schema(&pool, &prefix)
            .await
            .map_err(|e| crate::PoolError::Connect(e.to_string()))?;
        // A second pool keeps long history scans away from the writer.
        let query_pool = DbPool::connect_with(
            AnyPoolOptions::new()
                .max_connections((cfg.max_connections / 2).max(1))
                .min_connections(0)
                .acquire_timeout(std::time::Duration::from_secs(15)),
            &cfg.dsn,
        )
        .await
        .ok();
        if query_pool.is_none() {
            tracing::warn!("metric query pool unavailable; queries share the writer pool");
        }
        Ok(Self {
            pool,
            query_pool,
            prefix,
            kind,
            dialect,
        })
    }

    pub fn kind(&self) -> MetricKind {
        self.kind
    }

    pub fn dialect(&self) -> DbKind {
        self.dialect
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn pool(&self) -> &DbPool {
        &self.pool
    }

    fn read_pool(&self) -> &DbPool {
        self.query_pool.as_ref().unwrap_or(&self.pool)
    }

    /// Closes the pool, never touching the shared main pool.
    /// 关闭连接池；与主库共享的池不受影响。
    pub async fn shutdown(&self) {
        if self.kind != MetricKind::Merged {
            self.pool.close().await;
        }
        if let Some(pool) = &self.query_pool {
            pool.close().await;
        }
    }

    fn table(&self, suffix: &str) -> String {
        format!("{}{}", self.prefix, suffix)
    }

    /// Bulk-inserts bucket rows with multi-value statements; duplicate buckets are skipped.
    /// 多值语句批量写入桶行；重复桶跳过。
    pub async fn insert_buckets(&self, rows: &[BucketRow]) -> Result<(), MetricError> {
        if rows.is_empty() {
            return Ok(());
        }
        let table = self.table("buckets");
        let mut tx = self.pool.begin().await?;
        for chunk in rows.chunks(INSERT_CHUNK) {
            let values: Vec<&str> = (0..chunk.len())
                .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?)")
                .collect();
            let columns = "(node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
                           avg_val, min_val, max_val, sample_count)";
            let sql = match self.dialect {
                DbKind::MySql => {
                    format!(
                        "INSERT IGNORE INTO {table} {columns} VALUES {}",
                        values.join(", ")
                    )
                }
                _ => format!(
                    "INSERT INTO {table} {columns} VALUES {} ON CONFLICT \
                     (node_uuid, metric_key, bucket_seconds, bucket_start_ms) DO NOTHING",
                    values.join(", ")
                ),
            };
            let mut query = sqlx::query(&sql);
            for row in chunk {
                query = query
                    .bind(row.node_uuid.clone())
                    .bind(row.metric_key.clone())
                    .bind(row.bucket_seconds)
                    .bind(row.bucket_start_ms)
                    .bind(row.avg_val)
                    .bind(row.min_val)
                    .bind(row.max_val)
                    .bind(row.sample_count);
            }
            query.execute(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Batches several nodes into one query instead of issuing one query per node.
    pub async fn list_buckets_multi(
        &self,
        nodes: &[String],
        metric_key: Option<&str>,
        bucket_seconds: i64,
        since_ms: i64,
        until_ms: i64,
        limit: i64,
    ) -> Result<std::collections::HashMap<String, Vec<BucketRow>>, MetricError> {
        let mut grouped: std::collections::HashMap<String, Vec<BucketRow>> =
            std::collections::HashMap::new();
        if nodes.is_empty() {
            return Ok(grouped);
        }
        const CHUNK: usize = 400;
        for chunk in nodes.chunks(CHUNK) {
            let placeholders = std::iter::repeat("?").take(chunk.len()).collect::<Vec<_>>().join(", ");
            let metric_filter = if metric_key.is_some() { " AND metric_key = ?" } else { "" };
            let sql = format!(
                "SELECT node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
                 avg_val, min_val, max_val, sample_count FROM {} \
                 WHERE node_uuid IN ({placeholders}) AND bucket_seconds = ? \
                 AND bucket_start_ms >= ? AND bucket_start_ms <= ?{metric_filter} \
                 ORDER BY node_uuid, bucket_start_ms LIMIT ?",
                self.table("buckets"),
            );
            let mut query = sqlx::query_as::<_, (String, String, i64, i64, f64, f64, f64, i64)>(&sql);
            for node in chunk {
                query = query.bind(node.clone());
            }
            query = query.bind(bucket_seconds).bind(since_ms).bind(until_ms);
            if let Some(key) = metric_key {
                query = query.bind(key.to_string());
            }
            let rows = query.bind(limit).fetch_all(self.read_pool()).await?;
            for (node_uuid, metric_key, bucket_seconds, bucket_start_ms, avg_val, min_val, max_val, sample_count) in rows {
                grouped.entry(node_uuid.clone()).or_default().push(BucketRow {
                    node_uuid, metric_key, bucket_seconds, bucket_start_ms,
                    avg_val, min_val, max_val, sample_count,
                });
            }
        }
        Ok(grouped)
    }

    /// Same as [`Self::list_buckets`], but narrows the SQL scan to one metric key.
    pub async fn list_buckets_for_metric(
        &self,
        node_uuid: &str,
        metric_key: &str,
        bucket_seconds: i64,
        since_ms: i64,
        until_ms: i64,
        limit: i64,
    ) -> Result<Vec<BucketRow>, MetricError> {
        let sql = format!(
            "SELECT node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
             avg_val, min_val, max_val, sample_count FROM {} \
             WHERE node_uuid = ? AND metric_key = ? AND bucket_seconds = ? \
             AND bucket_start_ms >= ? AND bucket_start_ms <= ? \
             ORDER BY bucket_start_ms LIMIT ?",
            self.table("buckets"),
        );
        let rows = sqlx::query_as::<_, (String, String, i64, i64, f64, f64, f64, i64)>(&sql)
            .bind(node_uuid.to_string())
            .bind(metric_key.to_string())
            .bind(bucket_seconds)
            .bind(since_ms)
            .bind(until_ms)
            .bind(limit)
            .fetch_all(self.read_pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(node_uuid, metric_key, bucket_seconds, bucket_start_ms, avg_val, min_val, max_val, sample_count)| {
                    BucketRow {
                        node_uuid,
                        metric_key,
                        bucket_seconds,
                        bucket_start_ms,
                        avg_val,
                        min_val,
                        max_val,
                        sample_count,
                    }
                },
            )
            .collect())
    }

    /// Pages every bucket row by ascending id (switch-time full sync).
    /// 按 id 升序分页读取全部桶行（切换库时的全量同步）。
    pub async fn page_buckets(
        &self,
        after_id: i64,
        limit: i64,
    ) -> Result<Vec<(i64, BucketRow)>, MetricError> {
        let sql = format!(
            "SELECT id, node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
             avg_val, min_val, max_val, sample_count FROM {} \
             WHERE id > {} ORDER BY id LIMIT {}",
            self.table("buckets"),
            "?",
            "?",
        );
        let rows = sqlx::query_as::<_, (i64, String, String, i64, i64, f64, f64, f64, i64)>(&sql)
            .bind(after_id)
            .bind(limit)
            .fetch_all(self.read_pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    node_uuid,
                    metric_key,
                    bucket_seconds,
                    bucket_start_ms,
                    avg_val,
                    min_val,
                    max_val,
                    sample_count,
                )| {
                    (
                        id,
                        BucketRow {
                            node_uuid,
                            metric_key,
                            bucket_seconds,
                            bucket_start_ms,
                            avg_val,
                            min_val,
                            max_val,
                            sample_count,
                        },
                    )
                },
            )
            .collect())
    }

    /// Lists bucket rows for one node / metric window.
    /// 查询某节点的桶数据窗口。
    pub async fn list_buckets(
        &self,
        node_uuid: &str,
        bucket_seconds: i64,
        since_ms: i64,
        until_ms: i64,
        limit: i64,
    ) -> Result<Vec<BucketRow>, MetricError> {
        let sql = format!(
            "SELECT node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
             avg_val, min_val, max_val, sample_count FROM {} \
             WHERE node_uuid = {} AND bucket_seconds = {} \
             AND bucket_start_ms >= {} AND bucket_start_ms <= {} \
             ORDER BY bucket_start_ms LIMIT {}",
            self.table("buckets"),
            "?",
            "?",
            "?",
            "?",
            "?",
        );
        let rows = sqlx::query_as::<_, (String, String, i64, i64, f64, f64, f64, i64)>(&sql)
            .bind(node_uuid.to_string())
            .bind(bucket_seconds)
            .bind(since_ms)
            .bind(until_ms)
            .bind(limit)
            .fetch_all(self.read_pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    node_uuid,
                    metric_key,
                    bucket_seconds,
                    bucket_start_ms,
                    avg_val,
                    min_val,
                    max_val,
                    sample_count,
                )| {
                    BucketRow {
                        node_uuid,
                        metric_key,
                        bucket_seconds,
                        bucket_start_ms,
                        avg_val,
                        min_val,
                        max_val,
                        sample_count,
                    }
                },
            )
            .collect())
    }

    /// Removes expired buckets per global window retention and per-metric days.
    /// 按全局窗口保留与各指标保留天数删除过期桶。
    pub async fn cleanup(
        &self,
        cfg: &MetricDbConfig,
        retention_days: &std::collections::HashMap<String, i64>,
        now_ms: i64,
    ) -> Result<u64, MetricError> {
        let mut deleted = 0u64;
        let table = self.table("buckets");
        let by_key = format!(
            "DELETE FROM {table} WHERE metric_key = {} AND bucket_seconds = {} AND bucket_start_ms < {}",
            "?",
            "?",
            "?"
        );
        for (key, days) in retention_days {
            if *days <= 0 {
                continue;
            }
            let deadline = now_ms - days * 86_400_000;
            let r = sqlx::query(&by_key)
                .bind(key.clone())
                .bind(86_400i64)
                .bind(deadline)
                .execute(&self.pool)
                .await?;
            deleted += r.rows_affected();
        }
        let by_window = format!(
            "DELETE FROM {table} WHERE bucket_seconds = {} AND bucket_start_ms < {}",
            "?", "?"
        );
        for (seconds, deadline) in [
            (60i64, now_ms - cfg.bucket_1m_minutes * 60_000),
            (300, now_ms - cfg.bucket_5m_minutes * 60_000),
            (3600, now_ms - cfg.bucket_1h_hours * 3_600_000),
        ] {
            let r = sqlx::query(&by_window)
                .bind(seconds)
                .bind(deadline)
                .execute(&self.pool)
                .await?;
            deleted += r.rows_affected();
        }
        Ok(deleted)
    }

    /// Reads per-metric retention days.
    /// 读取各指标保留天数。
    pub async fn retention_days(&self) -> Result<Vec<(String, i64)>, MetricError> {
        let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
            "SELECT metric_key, days FROM {}",
            self.table("retention")
        ))
        .fetch_all(self.read_pool())
        .await?;
        Ok(rows)
    }

    /// Updates one metric's retention days (0 disables its persistence).
    /// 更新单个指标的保留天数（0 表示禁用该指标持久化）。
    pub async fn set_retention_days(&self, key: &str, days: i64) -> Result<(), MetricError> {
        let table = self.table("retention");
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(&format!(
            "UPDATE {table} SET days = {} WHERE metric_key = {}",
            "?", "?"
        ))
        .bind(days)
        .bind(key.to_string())
        .execute(&mut tx)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query(&format!(
                "INSERT INTO {table} (metric_key, days) VALUES ({}, {})",
                "?", "?"
            ))
            .bind(key.to_string())
            .bind(days)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Returns the logical size of the monitoring database in bytes.
    /// 返回监控数据库大小（字节）。
    pub async fn size_bytes(&self) -> Option<u64> {
        let sql = match self.dialect {
            DbKind::Sqlite => {
                "SELECT CAST((SELECT page_count FROM pragma_page_count) * (SELECT page_size FROM pragma_page_size) AS TEXT)"
            }
            DbKind::MySql => {
                "SELECT CAST(SUM(data_length + index_length) AS CHAR) FROM information_schema.tables WHERE table_schema = DATABASE()"
            }
            DbKind::Postgres => "SELECT CAST(pg_database_size(current_database()) AS TEXT)",
        };
        let row: Option<(Option<String>,)> = sqlx::query_as(sql)
            .fetch_optional(self.read_pool())
            .await
            .ok()
            .flatten();
        row?.0?.parse::<u64>().ok()
    }
}
