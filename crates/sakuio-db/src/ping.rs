use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;
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
    pool: &'a SqlitePool,
}

impl<'a> PingRepository<'a> {
    pub fn new(pool: &'a SqlitePool) -> Self {
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
        let clients_json = serde_json::to_string(clients).map_err(|e| PingDbError::Message(e.to_string()))?;
        let result = sqlx::query(
            "INSERT INTO ping_tasks (weight, name, clients_json, default_on, type, target, interval_secs, created_at, updated_at)
             VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        )
        .bind(name)
        .bind(clients_json)
        .bind(default_on as i32)
        .bind(task_type)
        .bind(target)
        .bind(interval)
        .bind(&now)
        .execute(self.pool)
        .await?;
        let id = result.last_insert_rowid();
        sqlx::query("UPDATE ping_tasks SET weight = ?1 WHERE id = ?1")
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
        let clients_json =
            serde_json::to_string(&task.clients).map_err(|e| PingDbError::Message(e.to_string()))?;
        let n = sqlx::query(
            "UPDATE ping_tasks SET name = ?1, clients_json = ?2, default_on = ?3, type = ?4, target = ?5,
             interval_secs = ?6, updated_at = ?7 WHERE id = ?8",
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
            sqlx::query("UPDATE ping_tasks SET weight = ?1 WHERE id = ?2")
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
        sqlx::query(
            "INSERT INTO ping_records (client_uuid, task_id, observed_at_ms, value_ms) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(client_uuid)
        .bind(task_id)
        .bind(observed_at_ms)
        .bind(value_ms)
        .execute(self.pool)
        .await?;
        Ok(())
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
                     WHERE client_uuid = ?1 AND task_id = ?2 AND observed_at_ms >= ?3 AND observed_at_ms <= ?4
                     ORDER BY observed_at_ms ASC LIMIT ?5",
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
                     WHERE client_uuid = ?1 AND observed_at_ms >= ?2 AND observed_at_ms <= ?3
                     ORDER BY observed_at_ms ASC LIMIT ?4",
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
                     WHERE task_id = ?1 AND observed_at_ms >= ?2 AND observed_at_ms <= ?3
                     ORDER BY observed_at_ms ASC LIMIT ?4",
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
                     WHERE observed_at_ms >= ?1 AND observed_at_ms <= ?2
                     ORDER BY observed_at_ms ASC LIMIT ?3",
                )
                .bind(since_ms)
                .bind(until_ms)
                .bind(limit)
                .fetch_all(self.pool)
                .await?
            }
        };
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
