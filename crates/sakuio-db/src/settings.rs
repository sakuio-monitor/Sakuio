use crate::{quote_ident, DbError, DbPool, DbResult};

/// Key-value settings persisted in `system_settings`.
/// 持久化在 `system_settings` 表中的键值设置。
pub struct SettingsRepository<'a> {
    pool: &'a DbPool,
}

impl<'a> SettingsRepository<'a> {
    /// Creates a repository bound to the given pool.
    /// 绑定到指定连接池的仓储实例。
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    /// `key` is reserved in MySQL, so every statement quotes it.
    /// `key` 在 MySQL 中是保留字，所有语句都必须引用。
    fn key_column(&self) -> String {
        quote_ident(self.pool.kind(), "key")
    }

    /// Returns whether the install wizard has completed.
    /// 返回安装向导是否已完成。
    pub async fn is_installed(&self) -> DbResult<bool> {
        let sql = format!(
            "SELECT value FROM system_settings WHERE {} = ? LIMIT 1",
            self.key_column()
        );
        let row: Option<(String,)> = sqlx::query_as(&sql)
            .bind("installation_state")
            .fetch_optional(self.pool)
            .await?;
        Ok(row.is_some_and(|(v,)| v.contains("ready") || v.contains("completed")))
    }

    /// Sets a JSON-serializable setting value (portable upsert).
    /// 写入可 JSON 序列化的设置值（跨方言 upsert）。
    pub async fn set_json<T: serde::Serialize>(&self, key: &str, value: &T) -> DbResult<()> {
        let raw = serde_json::to_string(value).map_err(|e| DbError::Message(e.to_string()))?;
        let key_column = self.key_column();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(&format!(
            "UPDATE system_settings SET value = ? WHERE {key_column} = ?"
        ))
        .bind(&raw)
        .bind(key)
        .execute(&mut tx)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query(&format!(
                "INSERT INTO system_settings ({key_column}, value) VALUES (?, ?)"
            ))
            .bind(key)
            .bind(&raw)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Deletes a setting; missing keys are ignored.
    /// 删除设置项；不存在时忽略。
    pub async fn delete(&self, key: &str) -> DbResult<()> {
        let key_column = self.key_column();
        sqlx::query(&format!(
            "DELETE FROM system_settings WHERE {key_column} = ?"
        ))
        .bind(key)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Reads a JSON setting or returns None if missing.
    /// 读取 JSON 设置；不存在时返回 None。
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> DbResult<Option<T>> {
        let sql = format!(
            "SELECT value FROM system_settings WHERE {} = ? LIMIT 1",
            self.key_column()
        );
        let row: Option<(String,)> = sqlx::query_as(&sql)
            .bind(key)
            .fetch_optional(self.pool)
            .await?;
        let Some((raw,)) = row else {
            return Ok(None);
        };
        let parsed = serde_json::from_str(&raw).map_err(|e| DbError::Message(e.to_string()))?;
        Ok(Some(parsed))
    }
}
