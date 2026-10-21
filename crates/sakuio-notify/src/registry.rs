use crate::types::{ChannelListItem, ChannelMeta, NotificationMessage};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("channel not found: {0}")]
    NotFound(String),
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("send failed: {0}")]
    Send(String),
}

pub type NotifyResult<T> = Result<T, NotifyError>;

#[async_trait]
pub trait NotificationChannel: Send + Sync {
    fn meta(&self) -> ChannelMeta;
    async fn send(
        &self,
        config: &serde_json::Value,
        message: &NotificationMessage,
    ) -> NotifyResult<()>;
}

#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<RwLock<HashMap<String, Arc<dyn NotificationChannel>>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, channel: Arc<dyn NotificationChannel>) {
        let id = channel.meta().id.clone();
        self.inner.write().await.insert(id, channel);
    }

    pub async fn list_meta(&self) -> Vec<ChannelMeta> {
        let guard = self.inner.read().await;
        let mut items: Vec<ChannelMeta> = guard.values().map(|c| c.meta()).collect();
        items.sort_by(|a, b| a.id.cmp(&b.id));
        items
    }

    pub async fn list_items(
        &self,
        enabled: impl Fn(&str) -> bool,
    ) -> Vec<ChannelListItem> {
        self.list_meta()
            .await
            .into_iter()
            .map(|m| ChannelListItem {
                id: m.id.clone(),
                name: m.name.clone(),
                fields: m.fields.clone(),
                enabled: enabled(&m.id),
            })
            .collect()
    }

    pub async fn get(&self, id: &str) -> Option<Arc<dyn NotificationChannel>> {
        self.inner.read().await.get(id).cloned()
    }

    pub async fn send(
        &self,
        id: &str,
        config: &serde_json::Value,
        message: &NotificationMessage,
    ) -> NotifyResult<()> {
        let channel = self
            .get(id)
            .await
            .ok_or_else(|| NotifyError::NotFound(id.to_string()))?;
        channel.send(config, message).await
    }
}
