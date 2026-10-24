use crate::state::{
    BoardStateStore, CommandCursor, CommandEnvelope, Presence, RuntimeSnapshot, StateError,
    RECENT_TELEMETRY_MS,
};
use async_trait::async_trait;
use sakuio_protocol::TelemetrySample;
use serde::Serialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;

enum RecentSample {
    Full(TelemetrySample),
    Encoded { at: i64, bytes: Vec<u8> },
}

impl RecentSample {
    fn observed_at_ms(&self) -> i64 {
        match self {
            Self::Full(sample) => sample.observed_at_ms,
            Self::Encoded { at, .. } => *at,
        }
    }
}

#[derive(Default)]
struct NodeState {
    presence: Option<Presence>,
    commands: Vec<CommandEnvelope>,
    next_cursor: u64,
    snapshot: Option<RuntimeSnapshot>,
    recent: Vec<RecentSample>,
}

/// In-process board state used when Redis is disabled.
/// 未启用 Redis 时使用的进程内板级状态。
#[derive(Clone, Default)]
pub struct MemoryBoardStore {
    inner: Arc<RwLock<HashMap<String, NodeState>>>,
}

#[async_trait]
impl BoardStateStore for MemoryBoardStore {
    async fn upsert_presence(
        &self,
        node_uuid: &str,
        instance_id: &str,
        session_id: &str,
    ) -> Result<(), StateError> {
        let mut guard = self.inner.write().await;
        let entry = guard.entry(node_uuid.to_string()).or_default();
        entry.presence = Some(Presence {
            node_uuid: node_uuid.to_string(),
            instance_id: instance_id.to_string(),
            session_id: session_id.to_string(),
        });
        Ok(())
    }

    async fn clear_presence(&self, node_uuid: &str, session_id: &str) -> Result<(), StateError> {
        let mut guard = self.inner.write().await;
        if let Some(entry) = guard.get_mut(node_uuid) {
            if entry
                .presence
                .as_ref()
                .is_some_and(|p| p.session_id == session_id)
            {
                entry.presence = None;
            }
        }
        Ok(())
    }

    async fn resolve_presence(&self, node_uuid: &str) -> Result<Option<Presence>, StateError> {
        let guard = self.inner.read().await;
        Ok(guard.get(node_uuid).and_then(|n| n.presence.clone()))
    }

    async fn enqueue_command(
        &self,
        node_uuid: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<CommandEnvelope, StateError> {
        let mut guard = self.inner.write().await;
        let entry = guard.entry(node_uuid.to_string()).or_default();
        entry.next_cursor += 1;
        let env = CommandEnvelope {
            cursor: CommandCursor(entry.next_cursor),
            method: method.to_string(),
            payload,
        };
        entry.commands.push(env.clone());
        Ok(env)
    }

    async fn read_commands(
        &self,
        node_uuid: &str,
        since: CommandCursor,
        limit: u32,
    ) -> Result<Vec<CommandEnvelope>, StateError> {
        let guard = self.inner.read().await;
        let Some(entry) = guard.get(node_uuid) else {
            return Ok(Vec::new());
        };
        let out: Vec<_> = entry
            .commands
            .iter()
            .filter(|c| c.cursor.0 > since.0)
            .take(limit as usize)
            .cloned()
            .collect();
        Ok(out)
    }

    async fn put_latest_snapshot(
        &self,
        node_uuid: &str,
        snap: RuntimeSnapshot,
        _ttl: Duration,
    ) -> Result<(), StateError> {
        let mut guard = self.inner.write().await;
        let entry = guard.entry(node_uuid.to_string()).or_default();
        entry.snapshot = Some(snap);
        Ok(())
    }

    async fn get_latest_snapshot(
        &self,
        node_uuid: &str,
    ) -> Result<Option<RuntimeSnapshot>, StateError> {
        let guard = self.inner.read().await;
        Ok(guard.get(node_uuid).and_then(|n| n.snapshot.clone()))
    }

    async fn append_recent_telemetry(
        &self,
        node_uuid: &str,
        samples: &[TelemetrySample],
    ) -> Result<(), StateError> {
        if samples.is_empty() {
            return Ok(());
        }
        let cutoff = chrono::Utc::now().timestamp_millis() - RECENT_TELEMETRY_MS;
        let mut guard = self.inner.write().await;
        let entry = guard.entry(node_uuid.to_string()).or_default();
        let encode_before = chrono::Utc::now().timestamp_millis() - 60_000;
        for sample in samples {
            if sample.observed_at_ms < cutoff {
                continue;
            }
            let recent = if sample.observed_at_ms < encode_before {
                let mut bytes = Vec::new();
                match cbor4ii::serde::to_writer(&mut bytes, sample) {
                    Ok(()) => RecentSample::Encoded {
                        at: sample.observed_at_ms,
                        bytes,
                    },
                    Err(_) => RecentSample::Full(sample.clone()),
                }
            } else {
                RecentSample::Full(sample.clone())
            };
            let pos = entry
                .recent
                .partition_point(|s| s.observed_at_ms() < sample.observed_at_ms);
            entry.recent.insert(pos, recent);
        }
        // Compress older in-memory samples instead of keeping full structs for
        // the whole ten-minute window.
        for item in &mut entry.recent {
            if let RecentSample::Full(sample) = item {
                if sample.observed_at_ms < encode_before {
                    let mut bytes = Vec::new();
                    if cbor4ii::serde::to_writer(&mut bytes, sample).is_ok() {
                        *item = RecentSample::Encoded {
                            at: sample.observed_at_ms,
                            bytes,
                        };
                    }
                }
            }
        }
        entry.recent.retain(|s| s.observed_at_ms() >= cutoff);
        Ok(())
    }

    async fn list_recent_telemetry(
        &self,
        node_uuid: &str,
        since_ms: i64,
    ) -> Result<Vec<TelemetrySample>, StateError> {
        let guard = self.inner.read().await;
        let Some(entry) = guard.get(node_uuid) else {
            return Ok(Vec::new());
        };
        Ok(entry
            .recent
            .iter()
            .filter(|s| s.observed_at_ms() >= since_ms)
            .filter_map(|s| match s {
                RecentSample::Full(sample) => Some(sample.clone()),
                RecentSample::Encoded { bytes, .. } => cbor4ii::serde::from_slice(bytes).ok(),
            })
            .collect())
    }
}

/// Approximate in-process board cache footprint, used by the profiling page.
/// 板级内存缓存占用估算，供性能分析页面使用。
#[derive(Debug, Clone, Default, Serialize)]
pub struct BoardMemoryStats {
    pub nodes: u64,
    pub online: u64,
    pub snapshots: u64,
    pub recent_samples: u64,
    pub commands: u64,
    pub estimated_bytes: u64,
}

impl MemoryBoardStore {
    pub async fn memory_stats(&self) -> BoardMemoryStats {
        let guard = self.inner.read().await;
        let mut stats = BoardMemoryStats {
            nodes: guard.len() as u64,
            ..Default::default()
        };
        for state in guard.values() {
            if state.presence.is_some() {
                stats.online += 1;
            }
            if let Some(snapshot) = &state.snapshot {
                stats.snapshots += 1;
                stats.estimated_bytes += serde_json::to_vec(snapshot)
                    .map(|bytes| bytes.len() as u64)
                    .unwrap_or(256);
            }
            stats.recent_samples += state.recent.len() as u64;
            stats.estimated_bytes += state
                .recent
                .iter()
                .map(|s| match s {
                    RecentSample::Full(_) => 512u64,
                    RecentSample::Encoded { bytes, .. } => (bytes.len() as u64).saturating_add(32),
                })
                .sum::<u64>();
            for command in &state.commands {
                stats.commands += 1;
                stats.estimated_bytes += command.payload.to_string().len() as u64;
            }
            stats.estimated_bytes += 128;
        }
        stats
    }
}
