use chrono::Utc;
use sakuio_protocol::TelemetrySample;
use sqlx::SqlitePool;
use thiserror::Error;

/// Metric database errors.
/// 指标库错误。
#[derive(Debug, Error)]
pub enum MetricError {
    #[error("database: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("{0}")]
    Message(String),
}

pub type MetricResult<T> = Result<T, MetricError>;

/// Writes telemetry samples to the app database (`metric_samples` table).
/// 将遥测采样写入应用库的 `metric_samples` 表。
pub struct MetricStore {
    pool: SqlitePool,
}

impl MetricStore {
    /// Binds the store to the given pool (v1 colocated metrics).
    /// 绑定到指定连接池（v1 指标与配置同库）。
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Inserts one sample as JSON for flexible schema evolution.
    /// 以 JSON 插入单条采样，便于 schema 演进。
    pub async fn insert_sample(
        &self,
        node_uuid: &str,
        sample: &TelemetrySample,
    ) -> MetricResult<()> {
        let payload = serde_json::to_string(sample)
            .map_err(|e| MetricError::Message(e.to_string()))?;
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO metric_samples (node_uuid, observed_at_ms, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(node_uuid)
        .bind(sample.observed_at_ms)
        .bind(payload)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts many samples in one transaction.
    /// 在一个事务中批量插入采样。
    pub async fn insert_samples(
        &self,
        node_uuid: &str,
        samples: &[TelemetrySample],
    ) -> MetricResult<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        let now = Utc::now().to_rfc3339();
        for sample in samples {
            let payload = serde_json::to_string(sample)
                .map_err(|e| MetricError::Message(e.to_string()))?;
            sqlx::query(
                "INSERT INTO metric_samples (node_uuid, observed_at_ms, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(node_uuid)
            .bind(sample.observed_at_ms)
            .bind(payload)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Lists samples in `[since_ms, until_ms]` ordered by time ascending.
    pub async fn list_samples(
        &self,
        node_uuid: Option<&str>,
        since_ms: i64,
        until_ms: i64,
        limit: i32,
    ) -> MetricResult<Vec<(String, TelemetrySample)>> {
        let limit = limit.clamp(1, 50_000);
        let rows: Vec<(String, String)> = if let Some(uuid) = node_uuid {
            sqlx::query_as(
                "SELECT node_uuid, payload_json FROM metric_samples
                 WHERE node_uuid = ?1 AND observed_at_ms >= ?2 AND observed_at_ms <= ?3
                 ORDER BY observed_at_ms ASC
                 LIMIT ?4",
            )
            .bind(uuid)
            .bind(since_ms)
            .bind(until_ms)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                "SELECT node_uuid, payload_json FROM metric_samples
                 WHERE observed_at_ms >= ?1 AND observed_at_ms <= ?2
                 ORDER BY observed_at_ms ASC
                 LIMIT ?3",
            )
            .bind(since_ms)
            .bind(until_ms)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };

        let mut out = Vec::with_capacity(rows.len());
        for (uuid, payload) in rows {
            let sample: TelemetrySample = serde_json::from_str(&payload)
                .map_err(|e| MetricError::Message(e.to_string()))?;
            out.push((uuid, sample));
        }
        Ok(out)
    }
}
