use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};

const RELAY_TTL: Duration = Duration::from_secs(120);
const CHANNEL_CAP: usize = 32;
const PENDING_CLIENT_CAP: usize = 64;
const PENDING_AGENT_OUT_CAP: usize = 512;

/// Browser → agent: stdin is binary; resize stays WebSocket text (must not become PTY stdin).
enum AgentInbound {
    Stdin(Vec<u8>),
    Resize(String),
}

fn browser_to_agent(msg: Message) -> Option<AgentInbound> {
    match msg {
        Message::Binary(b) => Some(AgentInbound::Stdin(b)),
        Message::Text(t) => {
            let trimmed = t.trim();
            if !trimmed.starts_with('{') {
                return None;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                return None;
            };
            let Some(obj) = v.as_object() else {
                return None;
            };
            let kind = obj
                .get("op")
                .and_then(|v| v.as_str())
                .or_else(|| obj.get("type").and_then(|v| v.as_str()));
            match kind {
                Some("resize") => {
                    let normalized = serde_json::json!({
                        "op": "resize",
                        "cols": obj.get("cols").cloned().unwrap_or(serde_json::json!(80)),
                        "rows": obj.get("rows").cloned().unwrap_or(serde_json::json!(24)),
                    });
                    Some(AgentInbound::Resize(normalized.to_string()))
                }
                _ => None,
            }
        }
        Message::Ping(_) | Message::Pong(_) => None,
        Message::Close(_) => None,
    }
}

struct Slot {
    owner: Option<String>,
    to_agent: Option<mpsc::Sender<AgentInbound>>,
    to_client: Option<mpsc::Sender<Vec<u8>>>,
    pending_upload_rx: Option<mpsc::Receiver<Vec<u8>>>,
    pending_to_agent: Vec<AgentInbound>,
    pending_to_client: Vec<Vec<u8>>,
    created: Instant,
}

impl Slot {
    fn new(owner: Option<String>) -> Self {
        Self {
            owner,
            to_agent: None,
            to_client: None,
            pending_upload_rx: None,
            pending_to_agent: Vec::new(),
            pending_to_client: Vec::new(),
            created: Instant::now(),
        }
    }

    fn stale(&self) -> bool {
        self.created.elapsed() > RELAY_TTL
    }
}

/// Approximate relay socket occupancy for the profiling page.
/// 供性能分析页面使用的中继连接占用估算。
#[derive(Debug, Clone, Default)]
pub struct RelayStats {
    pub sessions: u64,
    pub buffered_bytes: u64,
}

#[derive(Clone, Default)]
pub struct RelayBroker {
    inner: Arc<RwLock<HashMap<String, Slot>>>,
}

impl RelayBroker {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn stats(&self) -> RelayStats {
        let guard = self.inner.read().await;
        let mut stats = RelayStats::default();
        for slot in guard.values() {
            if slot.to_agent.is_some() || slot.to_client.is_some() {
                stats.sessions += 1;
            }
            stats.buffered_bytes += slot
                .pending_to_client
                .iter()
                .map(|bytes| bytes.len() as u64)
                .sum::<u64>();
            stats.buffered_bytes += slot
                .pending_to_agent
                .iter()
                .map(|inbound| match inbound {
                    AgentInbound::Stdin(bytes) => bytes.len() as u64,
                    AgentInbound::Resize(text) => text.len() as u64,
                })
                .sum::<u64>();
        }
        stats
    }

    pub async fn register(&self, relay_id: String, node_uuid: &str) {
        let mut guard = self.inner.write().await;
        guard.retain(|_, slot| !slot.stale());
        guard.insert(relay_id, Slot::new(Some(node_uuid.to_string())));
    }

    pub async fn open_client_download(&self, relay_id: &str) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAP);
        let mut guard = self.inner.write().await;
        let slot = guard.entry(relay_id.to_string()).or_insert_with(|| Slot::new(None));
        slot.to_client = Some(tx);
        rx
    }

    pub async fn bind_upload_feed(&self, relay_id: &str) -> mpsc::Sender<Vec<u8>> {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAP);
        let mut guard = self.inner.write().await;
        let slot = guard.entry(relay_id.to_string()).or_insert_with(|| Slot::new(None));
        slot.pending_upload_rx = Some(rx);
        tx
    }

    pub async fn attach_agent_for(&self, relay_id: &str, node_uuid: &str, socket: WebSocket) -> bool {
        let owner = {
            let guard = self.inner.read().await;
            guard
                .get(relay_id)
                .and_then(|slot| slot.owner.as_deref())
                .map(str::to_string)
        };
        if owner.as_deref() != Some(node_uuid) {
            return false;
        }
        self.attach_side(relay_id, socket, true).await;
        true
    }

    pub async fn attach_client(&self, relay_id: &str, socket: WebSocket) {
        self.attach_side(relay_id, socket, false).await;
    }

    async fn attach_side(&self, relay_id: &str, socket: WebSocket, is_agent: bool) {
        if is_agent {
            self.attach_agent_side(relay_id, socket).await;
        } else {
            self.attach_browser_side(relay_id, socket).await;
        }
    }

    async fn attach_agent_side(&self, relay_id: &str, socket: WebSocket) {
        let (mut ws_tx, mut ws_rx) = socket.split();
        let (to_agent_tx, mut to_agent_rx) = mpsc::channel::<AgentInbound>(CHANNEL_CAP);

        let mut pending_upload_rx = {
            let mut guard = self.inner.write().await;
            let slot = guard.entry(relay_id.to_string()).or_insert_with(|| Slot::new(None));
            slot.to_agent = Some(to_agent_tx.clone());
            let pending = std::mem::take(&mut slot.pending_to_agent);
            let upload = slot.pending_upload_rx.take();
            for inbound in pending {
                let _ = to_agent_tx.send(inbound).await;
            }
            upload
        };

        let relay_id_owned = relay_id.to_string();
        let inner = self.inner.clone();

        let forward_in = tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_rx.next().await {
                let bytes = match msg {
                    Message::Binary(b) => b,
                    Message::Text(t) => t.into_bytes(),
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(_) => break,
                };
                let peer = {
                    let guard = inner.read().await;
                    guard
                        .get(&relay_id_owned)
                        .and_then(|slot| slot.to_client.clone())
                };
                if let Some(peer) = peer {
                    let _ = peer.send(bytes).await;
                } else {
                    let mut guard = inner.write().await;
                    if let Some(slot) = guard.get_mut(&relay_id_owned) {
                        if slot.pending_to_client.len() < PENDING_AGENT_OUT_CAP {
                            slot.pending_to_client.push(bytes);
                        }
                    }
                }
            }
            inner.write().await.remove(&relay_id_owned);
        });

        loop {
            tokio::select! {
                chunk = to_agent_rx.recv() => {
                    match chunk {
                        Some(AgentInbound::Stdin(bytes)) => {
                            if ws_tx.send(Message::Binary(bytes)).await.is_err() {
                                break;
                            }
                        }
                        Some(AgentInbound::Resize(text)) => {
                            if ws_tx.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                chunk = async {
                    match pending_upload_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                }, if pending_upload_rx.is_some() => {
                    match chunk {
                        Some(bytes) => {
                            if ws_tx.send(Message::Binary(bytes)).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            pending_upload_rx = None;
                        }
                    }
                }
            }
        }

        let _ = forward_in.await;
    }

    async fn attach_browser_side(&self, relay_id: &str, socket: WebSocket) {
        let (mut ws_tx, mut ws_rx) = socket.split();
        let (to_client_tx, mut to_client_rx) = mpsc::channel::<Vec<u8>>(CHANNEL_CAP);

        let pending_out = {
            let mut guard = self.inner.write().await;
            let slot = guard.entry(relay_id.to_string()).or_insert_with(|| Slot::new(None));
            slot.to_client = Some(to_client_tx.clone());
            std::mem::take(&mut slot.pending_to_client)
        };
        for bytes in pending_out {
            let _ = to_client_tx.send(bytes).await;
        }

        let relay_id_owned = relay_id.to_string();
        let inner = self.inner.clone();

        let forward_in = tokio::spawn(async move {
            while let Some(Ok(msg)) = ws_rx.next().await {
                let Some(inbound) = browser_to_agent(msg) else {
                    continue;
                };
                let peer = {
                    let guard = inner.read().await;
                    guard
                        .get(&relay_id_owned)
                        .and_then(|slot| slot.to_agent.clone())
                };
                if let Some(peer) = peer {
                    let _ = peer.send(inbound).await;
                } else {
                    let mut guard = inner.write().await;
                    if let Some(slot) = guard.get_mut(&relay_id_owned) {
                        if slot.pending_to_agent.len() < PENDING_CLIENT_CAP {
                            slot.pending_to_agent.push(inbound);
                        }
                    }
                }
            }
            inner.write().await.remove(&relay_id_owned);
        });

        while let Some(bytes) = to_client_rx.recv().await {
            if ws_tx.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }

        let _ = forward_in.await;
    }
}
