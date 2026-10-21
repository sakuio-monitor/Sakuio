use chrono::Utc;
use serde_json::Value;
use sqlx::SqlitePool;

use crate::DbResult;

#[derive(Debug, Clone)]
pub struct NotificationInstanceRow {
    pub id: String,
    pub channel_type: String,
    pub name: String,
    pub enabled: bool,
    pub config: Value,
    pub updated_at: String,
}

pub struct NotificationRepository<'a> {
    pool: &'a SqlitePool,
}

impl<'a> NotificationRepository<'a> {
    pub fn new(pool: &'a SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list_all(&self) -> DbResult<Vec<NotificationInstanceRow>> {
        let rows: Vec<(String, String, String, i64, String, String)> = sqlx::query_as(
            "SELECT id, channel_type, name, enabled, config_json, updated_at
             FROM notification_channel_instances
             ORDER BY updated_at DESC",
        )
        .fetch_all(self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(id, channel_type, name, enabled, config_json, updated_at)| {
                NotificationInstanceRow {
                    id,
                    channel_type,
                    name,
                    enabled: enabled != 0,
                    config: parse_config(&config_json),
                    updated_at,
                }
            })
            .collect())
    }

    pub async fn get_by_id(&self, id: &str) -> DbResult<Option<NotificationInstanceRow>> {
        let row: Option<(String, String, String, i64, String, String)> = sqlx::query_as(
            "SELECT id, channel_type, name, enabled, config_json, updated_at
             FROM notification_channel_instances
             WHERE id = ?
             LIMIT 1",
        )
        .bind(id)
        .fetch_optional(self.pool)
        .await?;
        Ok(row.map(
            |(id, channel_type, name, enabled, config_json, updated_at)| NotificationInstanceRow {
                id,
                channel_type,
                name,
                enabled: enabled != 0,
                config: parse_config(&config_json),
                updated_at,
            },
        ))
    }

    pub async fn insert(
        &self,
        id: &str,
        channel_type: &str,
        name: &str,
        enabled: bool,
        config: &Value,
    ) -> DbResult<()> {
        let raw = serde_json::to_string(config).unwrap_or_else(|_| "{}".into());
        let updated_at = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO notification_channel_instances
             (id, channel_type, name, enabled, config_json, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(channel_type)
        .bind(name)
        .bind(if enabled { 1i64 } else { 0 })
        .bind(raw)
        .bind(updated_at)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update(
        &self,
        id: &str,
        name: &str,
        enabled: bool,
        config: &Value,
    ) -> DbResult<bool> {
        let raw = serde_json::to_string(config).unwrap_or_else(|_| "{}".into());
        let updated_at = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE notification_channel_instances
             SET name = ?, enabled = ?, config_json = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(name)
        .bind(if enabled { 1i64 } else { 0 })
        .bind(raw)
        .bind(updated_at)
        .bind(id)
        .execute(self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete(&self, id: &str) -> DbResult<bool> {
        let result = sqlx::query("DELETE FROM notification_channel_instances WHERE id = ?")
            .bind(id)
            .execute(self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn list_rules(&self) -> DbResult<Vec<NotificationRuleRow>> {
        let rows: Vec<(String, String, String, i64, String, String, String, String, String)> =
            sqlx::query_as(
                "SELECT id, name, event_type, threshold, channels_json, clients_json,
                        template_title, template_body, updated_at
                 FROM notification_rules
                 ORDER BY updated_at DESC",
            )
            .fetch_all(self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, name, event_type, threshold, channels_json, clients_json, template_title, template_body, updated_at)| {
                    NotificationRuleRow {
                        id,
                        name,
                        event_type,
                        threshold,
                        channels: parse_string_list(&channels_json),
                        clients: parse_string_list(&clients_json),
                        template_title,
                        template_body,
                        updated_at,
                    }
                },
            )
            .collect())
    }

    pub async fn get_rule(&self, id: &str) -> DbResult<Option<NotificationRuleRow>> {
        let rows: Vec<(String, String, String, i64, String, String, String, String, String)> =
            sqlx::query_as(
                "SELECT id, name, event_type, threshold, channels_json, clients_json,
                        template_title, template_body, updated_at
                 FROM notification_rules
                 WHERE id = ?
                 LIMIT 1",
            )
            .bind(id)
            .fetch_all(self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, name, event_type, threshold, channels_json, clients_json, template_title, template_body, updated_at)| {
                    NotificationRuleRow {
                        id,
                        name,
                        event_type,
                        threshold,
                        channels: parse_string_list(&channels_json),
                        clients: parse_string_list(&clients_json),
                        template_title,
                        template_body,
                        updated_at,
                    }
                },
            )
            .next())
    }

    pub async fn insert_rule(
        &self,
        id: &str,
        name: &str,
        event_type: &str,
        threshold: i64,
        channels: &[String],
        clients: &[String],
        template_title: &str,
        template_body: &str,
    ) -> DbResult<()> {
        let updated_at = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO notification_rules
             (id, name, event_type, threshold, channels_json, clients_json,
              template_title, template_body, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(event_type)
        .bind(threshold)
        .bind(serde_json::to_string(channels).unwrap_or_else(|_| "[]".into()))
        .bind(serde_json::to_string(clients).unwrap_or_else(|_| "[]".into()))
        .bind(template_title)
        .bind(template_body)
        .bind(updated_at)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_rule(
        &self,
        id: &str,
        name: &str,
        event_type: &str,
        threshold: i64,
        channels: &[String],
        clients: &[String],
        template_title: &str,
        template_body: &str,
    ) -> DbResult<bool> {
        let updated_at = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE notification_rules
             SET name = ?, event_type = ?, threshold = ?, channels_json = ?, clients_json = ?,
                 template_title = ?, template_body = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(name)
        .bind(event_type)
        .bind(threshold)
        .bind(serde_json::to_string(channels).unwrap_or_else(|_| "[]".into()))
        .bind(serde_json::to_string(clients).unwrap_or_else(|_| "[]".into()))
        .bind(template_title)
        .bind(template_body)
        .bind(updated_at)
        .bind(id)
        .execute(self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn delete_rule(&self, id: &str) -> DbResult<bool> {
        let result = sqlx::query("DELETE FROM notification_rules WHERE id = ?")
            .bind(id)
            .execute(self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

#[derive(Debug, Clone)]
pub struct NotificationRuleRow {
    pub id: String,
    pub name: String,
    pub event_type: String,
    pub threshold: i64,
    pub channels: Vec<String>,
    pub clients: Vec<String>,
    pub template_title: String,
    pub template_body: String,
    pub updated_at: String,
}

fn parse_string_list(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn parse_config(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or(Value::Object(Default::default()))
}
