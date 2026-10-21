use crate::BoardFrame;
use async_trait::async_trait;
use sakuio_protocol::ControlMessage;
use std::sync::Arc;

/// Bidirectional board link for one authenticated agent session.
/// 单个已鉴权 Agent 会话的双向板级链路。
#[async_trait]
pub trait BoardSession: Send {
    /// Sends a multiplexed frame to the agent.
    /// 向 Agent 发送多路复用帧。
    async fn send(&mut self, frame: BoardFrame) -> Result<(), String>;
    /// Receives the next frame, or `None` when the transport closed.
    /// 接收下一帧；传输关闭时返回 `None`。
    async fn recv(&mut self) -> Result<Option<BoardFrame>, String>;
    /// Returns the server-assigned session identifier.
    /// 返回服务端分配的会话标识。
    fn session_id(&self) -> &str;
}

/// Application logic invoked by transport plugins for each live session.
/// 传输插件在每个活跃会话上调用的应用逻辑。
#[async_trait]
pub trait BoardSessionHandler: Send + Sync {
    /// Called after token auth succeeds and before the read loop starts.
    /// Token 鉴权成功后、读循环开始前调用。
    async fn on_connect(&self, node_uuid: String, session_id: String) -> Result<(), String>;
    /// Called when the transport closes or errors out.
    /// 传输关闭或出错时调用。
    async fn on_disconnect(&self, node_uuid: String, session_id: String);
    /// Handles a decoded control message; may return zero or more replies.
    /// 处理已解码的控制消息；可返回零条或多条回复。
    async fn on_control(
        &self,
        node_uuid: &str,
        session_id: &str,
        msg: ControlMessage,
    ) -> Result<Vec<ControlMessage>, String>;
    /// Handles a telemetry batch; may return an ack control message.
    /// 处理遥测批次；可返回 ack 控制消息。
    async fn on_telemetry(
        &self,
        node_uuid: &str,
        session_id: &str,
        batch_seq: u32,
        batch: sakuio_protocol::TelemetryBatch,
    ) -> Result<Option<ControlMessage>, String>;
}

/// Shared handler pointer installed once at server startup.
/// 服务启动时安装一次的共享处理器指针。
pub type SharedBoardSessionHandler = Arc<dyn BoardSessionHandler>;
