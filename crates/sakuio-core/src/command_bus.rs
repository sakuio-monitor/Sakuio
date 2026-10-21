use sakuio_protocol::ControlMessage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

/// Pushes outbound control messages to live agent WebSocket sessions.
pub struct AgentCommandBus {
    inner: Arc<RwLock<HashMap<String, mpsc::UnboundedSender<ControlMessage>>>>,
}

impl Default for AgentCommandBus {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Clone for AgentCommandBus {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl AgentCommandBus {
    pub fn subscribe(&self, node_uuid: &str) -> mpsc::UnboundedReceiver<ControlMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        if let Ok(mut guard) = self.inner.try_write() {
            guard.insert(node_uuid.to_string(), tx);
        }
        rx
    }

    pub async fn register(&self, node_uuid: &str) -> mpsc::UnboundedReceiver<ControlMessage> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut guard = self.inner.write().await;
        guard.insert(node_uuid.to_string(), tx);
        rx
    }

    pub async fn unregister(&self, node_uuid: &str) {
        let mut guard = self.inner.write().await;
        guard.remove(node_uuid);
    }

    pub async fn push(&self, node_uuid: &str, msg: ControlMessage) -> bool {
        let guard = self.inner.read().await;
        if let Some(tx) = guard.get(node_uuid) {
            return tx.send(msg).is_ok();
        }
        false
    }
}
