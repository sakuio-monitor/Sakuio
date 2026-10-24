//! Metric store: bucket tables and retention metadata over a portable pool.
//! 指标存储：桶表与保留元数据（可移植连接池）。

use crate::config::MetricDbConfig;
use crate::MetricError;
use sakuio_db::{DbKind, DbPool};
use sqlx::any::AnyPoolOptions;
use std::sync::Arc;

/// Rows per multi-value INSERT; 8 binds each stays far below every backend's bind limit.
/// 单条多值 INSERT 的行数；每行 8 个参数，远低于各后端参数上限。
const INSERT_CHUNK: usize = 200;

/// One aggregated metric bucket row.
/// 聚合指标桶行。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BucketRow {
    pub node_uuid: String,
    pub metric_key: String,
    pub bucket_seconds: i64,
    pub bucket_start_ms: i64,
    pub avg_val: f64,
    pub min_val: f64,
    pub max_val: f64,
    pub sample_count: i64,
    pub sum_sq: f64,
    pub variance: f64,
    pub p50_val: f64,
    pub p95_val: f64,
    pub p99_val: f64,
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
    series_cache: Arc<std::sync::Mutex<std::collections::HashMap<(String, String), i64>>>,
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
            let store = Self {
                pool: main_pool.clone(),
                query_pool: None,
                prefix,
                kind: MetricKind::Merged,
                dialect: main_pool.kind(),
                series_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            };
            store.ensure_retention_defaults().await?;
            return Ok(store);
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
        let store = Self {
            pool,
            query_pool,
            prefix,
            kind,
            dialect,
            series_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        };
        store.ensure_retention_defaults().await?;
        Ok(store)
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

    /// Read-only joined view exposing node_uuid/metric_key.
    pub fn view(&self) -> String {
        format!("{}buckets_view", self.prefix)
    }

    /// Bulk-inserts bucket rows with multi-value statements; duplicate buckets are skipped.
    /// 多值语句批量写入桶行；重复桶跳过。
    pub async fn insert_buckets(&self, rows: &[BucketRow]) -> Result<(), MetricError> {
        if rows.is_empty() {
            return Ok(());
        }
        let table = self.table("buckets");
        let nodes = self.table("nodes");
        let keys = self.table("keys");
        let series = self.table("series");
        let mut tx = self.pool.begin().await?;
        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let key = (row.node_uuid.clone(), row.metric_key.clone());
            let cached = self
                .series_cache
                .lock()
                .ok()
                .and_then(|cache| cache.get(&key).copied());
            if let Some(id) = cached {
                ids.push(id);
                continue;
            }
            let ignore = if self.dialect == DbKind::MySql {
                "INSERT IGNORE"
            } else {
                "INSERT"
            };
            let suffix = if self.dialect == DbKind::MySql {
                ""
            } else {
                " ON CONFLICT DO NOTHING"
            };
            sqlx::query(&format!("{ignore} INTO {nodes} (uuid) VALUES (?){suffix}"))
                .bind(&row.node_uuid)
                .execute(&mut tx)
                .await
                .map_err(MetricError::Sqlx)?;
            sqlx::query(&format!(
                "{ignore} INTO {keys} (metric_key) VALUES (?){suffix}"
            ))
            .bind(&row.metric_key)
            .execute(&mut tx)
            .await
            .map_err(MetricError::Sqlx)?;
            let node_id: i64 =
                sqlx::query_scalar(&format!("SELECT id FROM {nodes} WHERE uuid = ?"))
                    .bind(&row.node_uuid)
                    .fetch_one(&mut tx)
                    .await
                    .map_err(MetricError::Sqlx)?;
            let metric_id: i64 =
                sqlx::query_scalar(&format!("SELECT id FROM {keys} WHERE metric_key = ?"))
                    .bind(&row.metric_key)
                    .fetch_one(&mut tx)
                    .await
                    .map_err(MetricError::Sqlx)?;
            sqlx::query(&format!(
                "{ignore} INTO {series} (node_id, metric_id) VALUES (?, ?){suffix}"
            ))
            .bind(node_id)
            .bind(metric_id)
            .execute(&mut tx)
            .await
            .map_err(MetricError::Sqlx)?;
            let series_id: i64 = sqlx::query_scalar(&format!(
                "SELECT id FROM {series} WHERE node_id = ? AND metric_id = ?"
            ))
            .bind(node_id)
            .bind(metric_id)
            .fetch_one(&mut tx)
            .await
            .map_err(MetricError::Sqlx)?;
            if let Ok(mut cache) = self.series_cache.lock() {
                cache.insert(key, series_id);
            }
            ids.push(series_id);
        }
        for (chunk, id_chunk) in rows.chunks(INSERT_CHUNK).zip(ids.chunks(INSERT_CHUNK)) {
            let values: Vec<&str> = (0..chunk.len())
                .map(|_| "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .collect();
            let columns = "(series_id, bucket_seconds, bucket_start_ms, \
                           avg_val, min_val, max_val, sample_count, sum_sq, variance, \
                           p50_val, p95_val, p99_val)";
            let sql = match self.dialect {
                DbKind::MySql => format!(
                    "INSERT INTO {table} {columns} VALUES {} \
                     ON DUPLICATE KEY UPDATE avg_val = VALUES(avg_val), \
                     min_val = VALUES(min_val), max_val = VALUES(max_val), \
                     sample_count = VALUES(sample_count)",
                    values.join(", ")
                ),
                _ => format!(
                    "INSERT INTO {table} {columns} VALUES {} ON CONFLICT \
                     (series_id, bucket_seconds, bucket_start_ms) DO UPDATE SET \
                     avg_val = excluded.avg_val, min_val = excluded.min_val, \
                     max_val = excluded.max_val, sample_count = excluded.sample_count",
                    values.join(", ")
                ),
            };
            let mut query = sqlx::query(&sql);
            for (row, series_id) in chunk.iter().zip(id_chunk) {
                query = query
                    .bind(*series_id)
                    .bind(row.bucket_seconds)
                    .bind(row.bucket_start_ms)
                    .bind(row.avg_val)
                    .bind(row.min_val)
                    .bind(row.max_val)
                    .bind(row.sample_count)
                    .bind(row.sum_sq)
                    .bind(row.variance)
                    .bind(row.p50_val)
                    .bind(row.p95_val)
                    .bind(row.p99_val);
            }
            query.execute(&mut tx).await.map_err(MetricError::Sqlx)?;
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
            let placeholders = std::iter::repeat("?")
                .take(chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let metric_filter = if metric_key.is_some() {
                " AND metric_key = ?"
            } else {
                ""
            };
            let sql = format!(
                "SELECT node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
                 avg_val, min_val, max_val, sample_count FROM {} \
                 WHERE node_uuid IN ({placeholders}) AND bucket_seconds = ? \
                 AND bucket_start_ms >= ? AND bucket_start_ms <= ?{metric_filter} \
                 ORDER BY node_uuid, bucket_start_ms LIMIT ?",
                self.view(),
            );
            let mut query =
                sqlx::query_as::<_, (String, String, i64, i64, f64, f64, f64, i64)>(&sql);
            for node in chunk {
                query = query.bind(node.clone());
            }
            query = query.bind(bucket_seconds).bind(since_ms).bind(until_ms);
            if let Some(key) = metric_key {
                query = query.bind(key.to_string());
            }
            let rows = query.bind(limit).fetch_all(self.read_pool()).await?;
            for (
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
                grouped
                    .entry(node_uuid.clone())
                    .or_default()
                    .push(BucketRow {
                        node_uuid,
                        metric_key,
                        bucket_seconds,
                        bucket_start_ms,
                        avg_val,
                        min_val,
                        max_val,
                        sample_count,
                        sum_sq: 0.0,
                        variance: 0.0,
                        p50_val: 0.0,
                        p95_val: 0.0,
                        p99_val: 0.0,
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
            self.view(),
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
                        sum_sq: 0.0,
                        variance: 0.0,
                        p50_val: 0.0,
                        p95_val: 0.0,
                        p99_val: 0.0,
                    }
                },
            )
            .collect())
    }

    /// Pages every bucket row by the bucket primary key, without a surrogate id.
    /// 按桶复合主键分页读取全部桶行。
    pub async fn page_buckets(
        &self,
        after_start_ms: i64,
        after_node: &str,
        after_metric: &str,
        after_window: i64,
        limit: i64,
    ) -> Result<Vec<BucketRow>, MetricError> {
        let sql = format!(
            "SELECT node_uuid, metric_key, bucket_seconds, bucket_start_ms, \
             avg_val, min_val, max_val, sample_count FROM {} \
             WHERE (bucket_start_ms, node_uuid, metric_key, bucket_seconds) > (?, ?, ?, ?) \
             ORDER BY bucket_start_ms, node_uuid, metric_key, bucket_seconds LIMIT ?",
            self.view()
        );
        let rows = sqlx::query_as::<_, (String, String, i64, i64, f64, f64, f64, i64)>(&sql)
            .bind(after_start_ms)
            .bind(after_node)
            .bind(after_metric)
            .bind(after_window)
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
                )| BucketRow {
                    node_uuid,
                    metric_key,
                    bucket_seconds,
                    bucket_start_ms,
                    avg_val,
                    min_val,
                    max_val,
                    sample_count,
                    sum_sq: 0.0,
                    variance: 0.0,
                    p50_val: 0.0,
                    p95_val: 0.0,
                    p99_val: 0.0,
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
            self.view(),
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
                        sum_sq: 0.0,
                        variance: 0.0,
                        p50_val: 0.0,
                        p95_val: 0.0,
                        p99_val: 0.0,
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
        let series = self.table("series");
        let keys = self.table("keys");
        let by_key = format!(
            "DELETE FROM {table} WHERE series_id IN (SELECT s.id FROM {series} s JOIN {keys} k ON k.id = s.metric_id WHERE k.metric_key = {}) AND bucket_seconds = {} AND bucket_start_ms < {}",
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

    /// Seeds the retention table so every metric has an explicit persisted
    /// default instead of only being shown as 90 by the API fallback.
    async fn ensure_retention_defaults(&self) -> Result<(), crate::PoolError> {
        let existing = self
            .retention_days()
            .await
            .map_err(|e| crate::PoolError::Connect(e.to_string()))?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<std::collections::HashSet<_>>();
        for def in crate::keys::METRIC_KEYS {
            if !existing.contains(def.key) {
                self.set_retention_days(def.key, 90)
                    .await
                    .map_err(|e| crate::PoolError::Connect(e.to_string()))?;
            }
        }
        Ok(())
    }

    /// Reclaims physical space left by retention deletes. PostgreSQL and
    /// SQLite need VACUUM; MySQL needs OPTIMIZE TABLE. This runs in the metric
    /// cleanup task, never on the request path.
    pub async fn reclaim_space(&self) -> Result<(), MetricError> {
        // Never run VACUUM FULL / OPTIMIZE TABLE from the server loop: those
        // take exclusive locks and stall the metric writer for seconds. The
        // periodic pass only updates planner statistics; a one-off compact is
        // an explicit maintenance action.
        let table = self.table("buckets");
        let sql = match self.dialect {
            DbKind::Postgres => format!("VACUUM (ANALYZE) {table}"),
            DbKind::MySql => format!("ANALYZE TABLE {table}"),
            DbKind::Sqlite => format!("ANALYZE {table}"),
        };
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    /// Persists the open-window aggregator snapshot, one JSON row per node.
    pub async fn save_checkpoints(&self, rows: &[BucketRow]) -> Result<(), MetricError> {
        use std::collections::HashMap;
        if rows.is_empty() {
            return Ok(());
        }
        let mut by_node: HashMap<&str, Vec<&BucketRow>> = HashMap::new();
        for row in rows {
            by_node.entry(row.node_uuid.as_str()).or_default().push(row);
        }
        let table = self.table("checkpoints");
        let now = chrono::Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;
        let entries: Vec<(&str, String)> = by_node
            .into_iter()
            .map(|(node, node_rows)| {
                serde_json::to_string(&node_rows)
                    .map(|payload| (node, payload))
                    .map_err(MetricError::Json)
            })
            .collect::<Result<_, _>>()?;
        // One multi-row upsert instead of one statement per node. 200 rows
        // keeps the bind count below SQLite's 999-variable limit.
        for chunk in entries.chunks(200) {
            let values = vec!["(?, ?, ?)"; chunk.len()].join(", ");
            let sql = match self.dialect {
                DbKind::MySql => format!(
                    "INSERT INTO {table} (node_uuid, checkpoint_ms, payload) VALUES {values} \
                     ON DUPLICATE KEY UPDATE checkpoint_ms = VALUES(checkpoint_ms), payload = VALUES(payload)"
                ),
                _ => format!(
                    "INSERT INTO {table} (node_uuid, checkpoint_ms, payload) VALUES {values} \
                     ON CONFLICT (node_uuid) DO UPDATE SET \
                     checkpoint_ms = excluded.checkpoint_ms, payload = excluded.payload"
                ),
            };
            let mut query = sqlx::query(&sql);
            for (node, payload) in chunk {
                query = query.bind(*node).bind(now).bind(payload);
            }
            query.execute(&mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Reclaims SQLite WAL pages without blocking readers or writers.
    /// 无阻塞回收 SQLite WAL 页，避免运行中 WAL 持续膨胀。
    pub async fn checkpoint_wal(&self) -> Result<(), MetricError> {
        if self.dialect == DbKind::Sqlite {
            sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                .execute(self.read_pool())
                .await?;
        }
        Ok(())
    }

    /// Removes checkpoints after a graceful flush/shutdown.
    pub async fn clear_checkpoints(&self) -> Result<(), MetricError> {
        sqlx::query(&format!("DELETE FROM {}", self.table("checkpoints")))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Loads all open-window checkpoints.
    pub async fn load_checkpoints(
        &self,
    ) -> Result<std::collections::HashMap<String, Vec<BucketRow>>, MetricError> {
        let table = self.table("checkpoints");
        let rows: Vec<(String, String)> =
            sqlx::query_as(&format!("SELECT node_uuid, payload FROM {table}"))
                .fetch_all(self.read_pool())
                .await?;
        let mut result = std::collections::HashMap::new();
        for (node, payload) in rows {
            let parsed: Vec<BucketRow> =
                serde_json::from_str(&payload).map_err(MetricError::Json)?;
            result.insert(node, parsed);
        }
        Ok(result)
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
