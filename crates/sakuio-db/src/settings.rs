use crate::{DbError, DbResult};
use sqlx::SqlitePool;

/// Key-value settings persisted in `system_settings`.
/// 持久化在 `system_settings` 表中的键值设置。
pub struct SettingsRepository<'a> {
    pool: &'a SqlitePool,
}

impl<'a> SettingsRepository<'a> {
    /// Creates a repository bound to the given pool.
    /// 绑定到指定连接池的仓储实例。
    pub fn new(pool: &'a SqlitePool) -> Self {
        Self { pool }
    }

    /// Returns whether the install wizard has completed.
    /// 返回安装向导是否已完成。
    pub async fn is_installed(&self) -> DbResult<bool> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM system_settings WHERE key = 'installation_state' LIMIT 1",
        )
        .fetch_optional(self.pool)
        .await?;
        Ok(row.is_some_and(|(v,)| v.contains("ready")))
    }

    /// Sets a JSON-serializable setting value.
    /// 写入可 JSON 序列化的设置值。
    pub async fn set_json<T: serde::Serialize>(&self, key: &str, value: &T) -> DbResult<()> {
        let raw = serde_json::to_string(value).map_err(|e| DbError::Message(e.to_string()))?;
        sqlx::query(
            "INSERT INTO system_settings (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(raw)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Reads a JSON setting or returns None if missing.
    /// 读取 JSON 设置；不存在时返回 None。
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> DbResult<Option<T>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM system_settings WHERE key = ?1 LIMIT 1")
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
