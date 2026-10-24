use crate::{DbKind, DbPool};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PingDbError {
    #[error("database: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("{0}")]
    Message(String),
}

pub type PingDbResult<T> = Result<T, PingDbError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingTaskRow {
    pub id: i64,
    #[serde(default)]
    pub weight: i32,
    pub name: String,
    pub clients: Vec<String>,
    pub default_on: bool,
    #[serde(rename = "type")]
    pub task_type: String,
    pub target: String,
    pub interval: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct PingRecordRow {
    pub client: String,
    pub task_id: i64,
    pub time: String,
    pub value: i32,
}

pub struct PingRepository<'a> {
    pool: &'a DbPool,
}

impl<'a> PingRepository<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn list_all(&self) -> PingDbResult<Vec<PingTaskRow>> {
        let rows = sqlx::query_as::<_, PingTaskDbRow>(
            "SELECT id, weight, name, clients_json, default_on, type, target, interval_secs
             FROM ping_tasks ORDER BY weight ASC, id ASC",
        )
        .fetch_all(self.pool)
        .await?;
        Ok(rows.into_iter().map(PingTaskDbRow::into_row).collect())
    }

    pub async fn add(
        &self,
        clients: &[String],
        default_on: bool,
        name: &str,
        target: &str,
        task_type: &str,
        interval: i32,
    ) -> PingDbResult<i64> {
        let now = Utc::now().to_rfc3339();
        let clients_json =
            serde_json::to_string(clients).map_err(|e| PingDbError::Message(e.to_string()))?;
        let sql = if self.pool.kind() == DbKind::Postgres {
            // PostgreSQL has no last_insert_id: RETURNING is the portable path.
            // PostgreSQL 没有 last_insert_id：使用 RETURNING。
            "INSERT INTO ping_tasks (weight, name, clients_json, default_on, type, target, interval_secs, created_at, updated_at)
             VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id"
        } else {
            "INSERT INTO ping_tasks (weight, name, clients_json, default_on, type, target, interval_secs, created_at, updated_at)
             VALUES (0, ?, ?, ?, ?, ?, ?, ?, ?)"
        };
        let query = sqlx::query(sql)
            .bind(name)
            .bind(clients_json)
            .bind(default_on as i32)
            .bind(task_type)
            .bind(target)
            .bind(interval)
            .bind(&now)
            .bind(&now);
        let id = if self.pool.kind() == DbKind::Postgres {
            let row = query.fetch_one(self.pool).await?;
            use sqlx::Row;
            row.try_get::<i64, _>(0)
                .map_err(|e| PingDbError::Message(e.to_string()))?
        } else {
            let result = query.execute(self.pool).await?;
            match result.last_insert_id() {
                Some(id) => id,
                None => {
                    sqlx::query_scalar::<_, i64>("SELECT MAX(id) FROM ping_tasks")
                        .fetch_one(self.pool)
                        .await?
                }
            }
        };
        // New tasks go to the end of the order; weight starts at their id.
        // 新任务排在末尾，初始权重取自身 id。
        let weight = id as i32;
        sqlx::query("UPDATE ping_tasks SET weight = ? WHERE id = ?")
            .bind(weight)
            .bind(id)
            .execute(self.pool)
            .await?;
        Ok(id)
    }

    pub async fn delete_records_for_tasks(&self, ids: &[i64]) -> PingDbResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM ping_records WHERE task_id IN ({placeholders})");
        let mut q = sqlx::query(&sql);
        for id in ids {
            q = q.bind(id);
        }
        q.execute(self.pool).await?;
        Ok(())
    }

    pub async fn delete(&self, ids: &[i64]) -> PingDbResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        self.delete_records_for_tasks(ids).await?;
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("DELETE FROM ping_tasks WHERE id IN ({placeholders})");
        let mut q = sqlx::query(&sql);
        for id in ids {
            q = q.bind(id);
        }
        let n = q.execute(self.pool).await?.rows_affected();
        if n == 0 {
            return Err(PingDbError::Message("ping task not found".into()));
        }
        Ok(())
    }

    pub async fn update_task(&self, task: &PingTaskRow) -> PingDbResult<()> {
        let now = Utc::now().to_rfc3339();
        let clients_json = serde_json::to_string(&task.clients)
            .map_err(|e| PingDbError::Message(e.to_string()))?;
        let n = sqlx::query(
            "UPDATE ping_tasks SET name = ?, clients_json = ?, default_on = ?, type = ?, target = ?,
             interval_secs = ?, updated_at = ? WHERE id = ?",
        )
        .bind(&task.name)
        .bind(clients_json)
        .bind(task.default_on as i32)
        .bind(&task.task_type)
        .bind(&task.target)
        .bind(task.interval)
        .bind(&now)
        .bind(task.id)
        .execute(self.pool)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(PingDbError::Message("ping task not found".into()));
        }
        Ok(())
    }

    pub async fn update_order(&self, order: &[(i64, i32)]) -> PingDbResult<()> {
        for (id, weight) in order {
            sqlx::query("UPDATE ping_tasks SET weight = ? WHERE id = ?")
                .bind(weight)
                .bind(id)
                .execute(self.pool)
                .await?;
        }
        Ok(())
    }

    pub async fn update_order_validated(&self, order: &[(i64, i32)]) -> PingDbResult<()> {
        if order.is_empty() {
            return Ok(());
        }
        let ids: Vec<i64> = order.iter().map(|(id, _)| *id).collect();
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!("SELECT COUNT(*) as c FROM ping_tasks WHERE id IN ({placeholders})");
        let mut q = sqlx::query_as::<_, (i64,)>(&sql);
        for id in &ids {
            q = q.bind(id);
        }
        let (count,) = q.fetch_one(self.pool).await?;
        if count as usize != ids.len() {
            return Err(PingDbError::Message("ping task not found".into()));
        }
        self.update_order(order).await
    }

    pub async fn append_default_on_client(&self, uuid: &str) -> PingDbResult<bool> {
        let tasks = sqlx::query_as::<_, PingTaskDbRow>(
            "SELECT id, weight, name, clients_json, default_on, type, target, interval_secs
             FROM ping_tasks WHERE default_on = 1",
        )
        .fetch_all(self.pool)
        .await?;
        let mut changed = false;
        for row in tasks {
            let mut task = row.into_row();
            if task.clients.iter().any(|c| c == uuid) {
                continue;
            }
            task.clients.push(uuid.to_string());
            self.update_task(&task).await?;
            changed = true;
        }
        Ok(changed)
    }

    pub async fn insert_record(
        &self,
        client_uuid: &str,
        task_id: i64,
        value_ms: i32,
        observed_at_ms: i64,
    ) -> PingDbResult<()> {
        if crate::maintenance_active() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO ping_records (client_uuid, task_id, observed_at_ms, value_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(client_uuid)
        .bind(task_id)
        .bind(observed_at_ms)
        .bind(value_ms)
        .execute(self.pool)
        .await?;
        self.rollup_ping(client_uuid, task_id, observed_at_ms, value_ms)
            .await?;
        self.rollup_minute(client_uuid, task_id, observed_at_ms, value_ms)
            .await?;
        Ok(())
    }

    async fn rollup_ping(
        &self,
        client_uuid: &str,
        task_id: i64,
        observed_at_ms: i64,
        value_ms: i32,
    ) -> PingDbResult<()> {
        let hour = observed_at_ms - observed_at_ms.rem_euclid(3_600_000);
        let valid = if value_ms >= 0 { 1 } else { 0 };
        let loss = 1 - valid;
        let value = value_ms.max(0) as f64;
        let sql = match self.pool.kind() {
            DbKind::MySql => "INSERT INTO ping_rollup_hourly (task_id, client_uuid, hour_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE samples = samples + 1, valid = valid + VALUES(valid), loss_count = loss_count + VALUES(loss_count), sum_ms = sum_ms + VALUES(sum_ms), sum_sq_ms = sum_sq_ms + VALUES(sum_sq_ms), min_ms = LEAST(min_ms, VALUES(min_ms)), max_ms = GREATEST(max_ms, VALUES(max_ms))",
            DbKind::Postgres => "INSERT INTO ping_rollup_hourly (task_id, client_uuid, hour_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?) ON CONFLICT (task_id, client_uuid, hour_start_ms) DO UPDATE SET samples = ping_rollup_hourly.samples + 1, valid = ping_rollup_hourly.valid + excluded.valid, loss_count = ping_rollup_hourly.loss_count + excluded.loss_count, sum_ms = ping_rollup_hourly.sum_ms + excluded.sum_ms, sum_sq_ms = ping_rollup_hourly.sum_sq_ms + excluded.sum_sq_ms, min_ms = LEAST(ping_rollup_hourly.min_ms, excluded.min_ms), max_ms = GREATEST(ping_rollup_hourly.max_ms, excluded.max_ms)",
            DbKind::Sqlite => "INSERT INTO ping_rollup_hourly (task_id, client_uuid, hour_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?) ON CONFLICT (task_id, client_uuid, hour_start_ms) DO UPDATE SET samples = ping_rollup_hourly.samples + 1, valid = ping_rollup_hourly.valid + excluded.valid, loss_count = ping_rollup_hourly.loss_count + excluded.loss_count, sum_ms = ping_rollup_hourly.sum_ms + excluded.sum_ms, sum_sq_ms = ping_rollup_hourly.sum_sq_ms + excluded.sum_sq_ms, min_ms = MIN(ping_rollup_hourly.min_ms, excluded.min_ms), max_ms = MAX(ping_rollup_hourly.max_ms, excluded.max_ms)",
        };
        sqlx::query(sql)
            .bind(task_id)
            .bind(client_uuid)
            .bind(hour)
            .bind(valid)
            .bind(loss)
            .bind(value)
            .bind(value * value)
            .bind(value)
            .bind(value)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    async fn rollup_minute(
        &self,
        client_uuid: &str,
        task_id: i64,
        observed_at_ms: i64,
        value_ms: i32,
    ) -> PingDbResult<()> {
        let minute = observed_at_ms - observed_at_ms.rem_euclid(60_000);
        let valid = if value_ms >= 0 { 1 } else { 0 };
        let loss = 1 - valid;
        let value = value_ms.max(0) as f64;
        let sql = match self.pool.kind() {
            DbKind::MySql => "INSERT INTO ping_minute_rollup (task_id, client_uuid, minute_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE samples = samples + 1, valid = valid + VALUES(valid), loss_count = loss_count + VALUES(loss_count), sum_ms = sum_ms + VALUES(sum_ms), sum_sq_ms = sum_sq_ms + VALUES(sum_sq_ms), min_ms = LEAST(min_ms, VALUES(min_ms)), max_ms = GREATEST(max_ms, VALUES(max_ms))",
            DbKind::Postgres => "INSERT INTO ping_minute_rollup (task_id, client_uuid, minute_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?) ON CONFLICT (task_id, client_uuid, minute_start_ms) DO UPDATE SET samples = ping_minute_rollup.samples + 1, valid = ping_minute_rollup.valid + excluded.valid, loss_count = ping_minute_rollup.loss_count + excluded.loss_count, sum_ms = ping_minute_rollup.sum_ms + excluded.sum_ms, sum_sq_ms = ping_minute_rollup.sum_sq_ms + excluded.sum_sq_ms, min_ms = LEAST(ping_minute_rollup.min_ms, excluded.min_ms), max_ms = GREATEST(ping_minute_rollup.max_ms, excluded.max_ms)",
            DbKind::Sqlite => "INSERT INTO ping_minute_rollup (task_id, client_uuid, minute_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?) ON CONFLICT (task_id, client_uuid, minute_start_ms) DO UPDATE SET samples = ping_minute_rollup.samples + 1, valid = ping_minute_rollup.valid + excluded.valid, loss_count = ping_minute_rollup.loss_count + excluded.loss_count, sum_ms = ping_minute_rollup.sum_ms + excluded.sum_ms, sum_sq_ms = ping_minute_rollup.sum_sq_ms + excluded.sum_sq_ms, min_ms = MIN(ping_minute_rollup.min_ms, excluded.min_ms), max_ms = MAX(ping_minute_rollup.max_ms, excluded.max_ms)",
        };
        sqlx::query(sql)
            .bind(task_id)
            .bind(client_uuid)
            .bind(minute)
            .bind(valid)
            .bind(loss)
            .bind(value)
            .bind(value * value)
            .bind(value)
            .bind(value)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    /// One-time migration for raw probes written by older versions.
    pub async fn backfill_minute_rollups(&self) -> PingDbResult<u64> {
        let cutoff = chrono::Utc::now().timestamp_millis() - 10 * 60_000;
        let existing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ping_minute_rollup WHERE minute_start_ms < ?",
        )
        .bind(cutoff)
        .fetch_one(self.pool)
        .await?;
        if existing > 0 {
            return Ok(0);
        }
        let raw: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ping_records")
            .fetch_one(self.pool)
            .await?;
        if raw == 0 {
            return Ok(0);
        }
        let conflict = match self.pool.kind() {
            DbKind::MySql => "ON DUPLICATE KEY UPDATE samples = samples",
            _ => "ON CONFLICT (task_id, client_uuid, minute_start_ms) DO NOTHING",
        };
        let sql = format!(
            "INSERT INTO ping_minute_rollup
             (task_id, client_uuid, minute_start_ms, samples, valid, loss_count, sum_ms, sum_sq_ms, min_ms, max_ms)
             SELECT task_id, client_uuid,
                    observed_at_ms - (observed_at_ms % 60000),
                    COUNT(*),
                    SUM(CASE WHEN value_ms >= 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN value_ms < 0 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN value_ms >= 0 THEN value_ms ELSE 0 END),
                    SUM(CASE WHEN value_ms >= 0 THEN value_ms * value_ms ELSE 0 END),
                    MIN(CASE WHEN value_ms >= 0 THEN value_ms ELSE 0 END),
                    MAX(CASE WHEN value_ms >= 0 THEN value_ms ELSE 0 END)
             FROM ping_records
             GROUP BY task_id, client_uuid, observed_at_ms - (observed_at_ms % 60000)
             {conflict}"
        );
        let result = sqlx::query(&sql).execute(self.pool).await?;
        Ok(result.rows_affected())
    }

    /// Reads minute rollups and converts them to the Ping record shape.
    pub async fn list_minute_rollups(
        &self,
        client_uuid: Option<&str>,
        task_id: Option<i64>,
        since_ms: i64,
        until_ms: i64,
        limit: i32,
    ) -> PingDbResult<Vec<PingRecordRow>> {
        self.list_rollups("ping_minute_rollup", "minute_start_ms", client_uuid, task_id, since_ms, until_ms, limit)
            .await
    }

    /// Reads hourly rollups and converts them to the Ping record shape.
    pub async fn list_hourly_rollups(
        &self,
        client_uuid: Option<&str>,
        task_id: Option<i64>,
        since_ms: i64,
        until_ms: i64,
        limit: i32,
    ) -> PingDbResult<Vec<PingRecordRow>> {
        self.list_rollups("ping_rollup_hourly", "hour_start_ms", client_uuid, task_id, since_ms, until_ms, limit)
            .await
    }

    async fn list_rollups(
        &self,
        table: &'static str,
        time_col: &'static str,
        client_uuid: Option<&str>,
        task_id: Option<i64>,
        since_ms: i64,
        until_ms: i64,
        limit: i32,
    ) -> PingDbResult<Vec<PingRecordRow>> {
        let limit = limit.clamp(1, 50_000);
        let mut sql = format!(
            "SELECT client_uuid, task_id, {time_col}, valid, sum_ms FROM {table} WHERE {time_col} >= ? AND {time_col} <= ?"
        );
        if client_uuid.is_some() {
            sql.push_str(" AND client_uuid = ?");
        }
        if task_id.is_some() {
            sql.push_str(" AND task_id = ?");
        }
        sql.push_str(&format!(" ORDER BY {time_col} DESC LIMIT ?"));
        let mut q = sqlx::query_as::<_, (String, i64, i64, i64, f64)>(&sql).bind(since_ms).bind(until_ms);
        if let Some(uuid) = client_uuid {
            q = q.bind(uuid);
        }
        if let Some(tid) = task_id {
            q = q.bind(tid);
        }
        let mut rows = q.bind(limit).fetch_all(self.pool).await?;
        rows.reverse();
        Ok(rows
            .into_iter()
            .map(|(client, task, ms, valid, sum)| PingRecordRow {
                client,
                task_id: task,
                value: if valid > 0 { (sum / valid as f64).round() as i32 } else { -1 },
                time: ms_to_rfc3339(ms),
            })
            .collect())
    }

    pub async fn list_records(
        &self,
        client_uuid: Option<&str>,
        task_id: Option<i64>,
        since_ms: i64,
        until_ms: i64,
        limit: i32,
    ) -> PingDbResult<Vec<PingRecordRow>> {
        let limit = limit.clamp(1, 50_000);
        let rows: Vec<(String, i64, i64, i32)> = match (client_uuid, task_id) {
            (Some(uuid), Some(tid)) => {
                sqlx::query_as(
                    "SELECT client_uuid, task_id, observed_at_ms, value_ms FROM ping_records
                     WHERE client_uuid = ? AND task_id = ? AND observed_at_ms >= ? AND observed_at_ms <= ?
                     ORDER BY observed_at_ms DESC LIMIT ?",
                )
                .bind(uuid)
                .bind(tid)
                .bind(since_ms)
                .bind(until_ms)
                .bind(limit)
                .fetch_all(self.pool)
                .await?
            }
            (Some(uuid), None) => {
                sqlx::query_as(
                    "SELECT client_uuid, task_id, observed_at_ms, value_ms FROM ping_records
                     WHERE client_uuid = ? AND observed_at_ms >= ? AND observed_at_ms <= ?
                     ORDER BY observed_at_ms DESC LIMIT ?",
                )
                .bind(uuid)
                .bind(since_ms)
                .bind(until_ms)
                .bind(limit)
                .fetch_all(self.pool)
                .await?
            }
            (None, Some(tid)) => {
                sqlx::query_as(
                    "SELECT client_uuid, task_id, observed_at_ms, value_ms FROM ping_records
                     WHERE task_id = ? AND observed_at_ms >= ? AND observed_at_ms <= ?
                     ORDER BY observed_at_ms DESC LIMIT ?",
                )
                .bind(tid)
                .bind(since_ms)
                .bind(until_ms)
                .bind(limit)
                .fetch_all(self.pool)
                .await?
            }
            (None, None) => {
                sqlx::query_as(
                    "SELECT client_uuid, task_id, observed_at_ms, value_ms FROM ping_records
                     WHERE observed_at_ms >= ? AND observed_at_ms <= ?
                     ORDER BY observed_at_ms DESC LIMIT ?",
                )
                .bind(since_ms)
                .bind(until_ms)
                .bind(limit)
                .fetch_all(self.pool)
                .await?
            }
        };
        let mut rows = rows;
        rows.reverse();
        Ok(rows
            .into_iter()
            .map(|(client, task_id, ms, value)| PingRecordRow {
                client,
                task_id,
                value,
                time: ms_to_rfc3339(ms),
            })
            .collect())
    }
}

fn ms_to_rfc3339(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339())
}

#[derive(sqlx::FromRow)]
struct PingTaskDbRow {
    id: i64,
    weight: i32,
    name: String,
    clients_json: String,
    default_on: i32,
    r#type: String,
    target: String,
    interval_secs: i32,
}

impl PingTaskDbRow {
    fn into_row(self) -> PingTaskRow {
        let clients: Vec<String> = serde_json::from_str(&self.clients_json).unwrap_or_default();
        PingTaskRow {
            id: self.id,
            weight: self.weight,
            name: self.name,
            clients,
            default_on: self.default_on != 0,
            task_type: self.r#type,
            target: self.target,
            interval: self.interval_secs,
        }
    }
}

pub fn task_to_public_json(task: &PingTaskRow) -> Value {
    serde_json::json!({
        "id": task.id,
        "weight": task.weight,
        "name": task.name,
        "clients": task.clients,
        "default_on": task.default_on,
        "type": task.task_type,
        "target": task.target,
        "interval": task.interval,
    })
}

pub fn task_to_public_list_json(task: &PingTaskRow) -> Value {
    serde_json::json!({
        "id": task.id,
        "weight": task.weight,
        "name": task.name,
        "clients": task.clients,
        "default_on": task.default_on,
        "type": task.task_type,
        "interval": task.interval,
    })
}
