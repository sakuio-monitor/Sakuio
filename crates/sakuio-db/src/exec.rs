use chrono::Utc;
use serde_json::Value;
use sqlx::SqlitePool;

use crate::DbResult;

pub struct ExecRepository<'a> {
    pool: &'a SqlitePool,
}

impl<'a> ExecRepository<'a> {
    pub fn new(pool: &'a SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn create_task(
        &self,
        task_id: &str,
        command: &str,
        clients: &[String],
    ) -> DbResult<()> {
        let clients_json = serde_json::to_string(clients).unwrap_or_else(|_| "[]".into());
        let created_at = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO exec_tasks (task_id, command, clients_json, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(task_id)
        .bind(command)
        .bind(clients_json)
        .bind(created_at)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_pending_result(&self, task_id: &str, client_uuid: &str) -> DbResult<()> {
        let created_at = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO exec_task_results (task_id, client_uuid, result, exit_code, finished_at, created_at)
             VALUES (?, ?, '', NULL, NULL, ?)",
        )
        .bind(task_id)
        .bind(client_uuid)
        .bind(created_at)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn finish_result(
        &self,
        task_id: &str,
        client_uuid: &str,
        result: &str,
        exit_code: i32,
    ) -> DbResult<()> {
        let finished_at = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE exec_task_results
             SET result = ?, exit_code = ?, finished_at = ?
             WHERE task_id = ? AND client_uuid = ?",
        )
        .bind(result)
        .bind(exit_code)
        .bind(finished_at)
        .bind(task_id)
        .bind(client_uuid)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn append_result_chunk(
        &self,
        task_id: &str,
        client_uuid: &str,
        chunk: &str,
    ) -> DbResult<()> {
        sqlx::query(
            "UPDATE exec_task_results SET result = result || ? WHERE task_id = ? AND client_uuid = ?",
        )
        .bind(chunk)
        .bind(task_id)
        .bind(client_uuid)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_results(&self, task_id: &str) -> DbResult<Vec<Value>> {
        let rows = sqlx::query_as::<_, (String, String, String, Option<i64>, Option<String>, String)>(
            "SELECT r.task_id, r.client_uuid, r.result, r.exit_code, r.finished_at, r.created_at
             FROM exec_task_results r
             WHERE r.task_id = ?
             ORDER BY r.client_uuid",
        )
        .bind(task_id)
        .fetch_all(self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for (task_id, client, result, exit_code, finished_at, created_at) in rows {
            let name = sqlx::query_scalar::<_, Option<String>>(
                "SELECT name FROM clients WHERE uuid = ? LIMIT 1",
            )
            .bind(&client)
            .fetch_optional(self.pool)
            .await?
            .flatten()
            .unwrap_or_else(|| client.clone());

            out.push(serde_json::json!({
                "task_id": task_id,
                "client": client,
                "client_info": {
                    "uuid": client,
                    "name": name,
                },
                "result": result,
                "exit_code": exit_code,
                "finished_at": finished_at,
                "created_at": created_at,
            }));
        }
        Ok(out)
    }
}
