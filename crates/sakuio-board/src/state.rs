use async_trait::async_trait;
use sakuio_protocol::TelemetrySample;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// Recent telemetry window for realtime charts (~1 minute).
pub const RECENT_TELEMETRY_MS: i64 = 60_000;

/// Monotonic command cursor for sync/pull semantics.
/// 用于同步/拉取语义的单调命令游标。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommandCursor(pub u64);

/// One pending or historical command for an agent.
/// 面向 Agent 的待执行或历史命令。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandEnvelope {
    pub cursor: CommandCursor,
    pub method: String,
    pub payload: serde_json::Value,
}

/// Which session currently owns the live board link for a node.
/// 当前持有节点活跃板级链路的会话信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    pub node_uuid: String,
    pub instance_id: String,
    pub session_id: String,
}

/// Latest runtime metrics used for fast dashboard reads.
/// 供仪表盘快速读取的最新运行时指标。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub node_uuid: String,
    pub observed_at_ms: i64,
    pub cpu_usage: Option<f64>,
    pub mem_used: Option<i64>,
    pub mem_total: Option<i64>,
    pub swap_used: Option<i64>,
    pub swap_total: Option<i64>,
    pub load1: Option<f64>,
    pub load5: Option<f64>,
    pub load15: Option<f64>,
    pub disk_used: Option<i64>,
    pub disk_total: Option<i64>,
    pub net_up: Option<u64>,
    pub net_down: Option<u64>,
    pub net_total_up: Option<u64>,
    pub net_total_down: Option<u64>,
    pub process: Option<i32>,
    pub connections_tcp: Option<i32>,
    pub connections_udp: Option<i32>,
    pub uptime_secs: Option<u64>,
    pub gpu_usage: Option<f32>,
    pub online: bool,
}

/// Board state store errors.
/// 板级状态存储错误。
#[derive(Debug, Error)]
pub enum StateError {
    #[error("store unavailable")]
    Unavailable,
    #[error("{0}")]
    Message(String),
}

/// Abstract board state (memory, Redis, etc.).
/// 板级状态抽象（内存、Redis 等）。
#[async_trait]
pub trait BoardStateStore: Send + Sync {
    /// Records which session is currently online for the node.
    /// 记录节点当前在线的会话。
    async fn upsert_presence(
        &self,
        node_uuid: &str,
        instance_id: &str,
        session_id: &str,
    ) -> Result<(), StateError>;

    /// Clears presence when the given session disconnects.
    /// 指定会话断开时清除在线状态。
    async fn clear_presence(&self, node_uuid: &str, session_id: &str) -> Result<(), StateError>;

    /// Resolves live presence for command routing.
    /// 解析在线状态以便路由命令。
    async fn resolve_presence(&self, node_uuid: &str) -> Result<Option<Presence>, StateError>;

    /// Enqueues a command for the agent to pull.
    /// 入队供 Agent 拉取的命令。
    async fn enqueue_command(
        &self,
        node_uuid: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<CommandEnvelope, StateError>;

    /// Reads commands after `since`, up to `limit`.
    /// 读取 `since` 之后的命令，最多 `limit` 条。
    async fn read_commands(
        &self,
        node_uuid: &str,
        since: CommandCursor,
        limit: u32,
    ) -> Result<Vec<CommandEnvelope>, StateError>;

    /// Optional wake-up hook when a command is queued (Redis pub/sub, etc.).
    /// 命令入队时的可选唤醒钩子（Redis pub/sub 等）。
    async fn notify_command_pending(&self, _node_uuid: &str) -> Result<(), StateError> {
        Ok(())
    }

    /// Stores the latest runtime snapshot with a suggested TTL.
    /// 写入最新运行时快照并附带建议 TTL。
    async fn put_latest_snapshot(
        &self,
        node_uuid: &str,
        snap: RuntimeSnapshot,
        _ttl: Duration,
    ) -> Result<(), StateError>;

    /// Reads the latest runtime snapshot if present.
    /// 读取最新运行时快照（若存在）。
    async fn get_latest_snapshot(
        &self,
        node_uuid: &str,
    ) -> Result<Option<RuntimeSnapshot>, StateError>;

    /// Appends samples to the in-memory recent window for live charts.
    async fn append_recent_telemetry(
        &self,
        node_uuid: &str,
        samples: &[TelemetrySample],
    ) -> Result<(), StateError> {
        let _ = (node_uuid, samples);
        Ok(())
    }

    /// Lists recent samples with `observed_at_ms >= since_ms`, sorted ascending.
    async fn list_recent_telemetry(
        &self,
        node_uuid: &str,
        since_ms: i64,
    ) -> Result<Vec<TelemetrySample>, StateError> {
        let _ = (node_uuid, since_ms);
        Ok(Vec::new())
    }
}
