use crate::BoardSession;
use async_trait::async_trait;
use sakuio_protocol::ControlMessage;
use std::sync::Arc;
use thiserror::Error;

/// Metadata collected when an agent opens a board transport connection.
/// Agent 建立板级传输连接时收集的元数据。
#[derive(Debug, Clone)]
pub struct BoardConnectInfo {
    pub node_token: String,
    pub agent_version: String,
    pub capabilities: Vec<String>,
}

/// Transport-level failures (not application protocol errors).
/// 传输层失败（非应用协议错误）。
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connect failed: {0}")]
    Connect(String),
    #[error("io: {0}")]
    Io(String),
}

/// Outbound board transport used by agents.
/// Agent 使用的出站板级传输。
#[async_trait]
pub trait BoardTransport: Send + Sync {
    /// Returns the transport plugin name (e.g. `websocket`).
    /// 返回传输插件名称（如 `websocket`）。
    fn name(&self) -> &'static str;
    /// Establishes a board session to the panel.
    /// 与中控建立板级会话。
    async fn connect(
        &self,
        info: BoardConnectInfo,
    ) -> Result<Box<dyn BoardSession>, TransportError>;
}

/// Inbound board transport server mounted on HTTP (or other) listeners.
/// 挂载在 HTTP（或其他）监听器上的入站板级传输服务。
#[async_trait]
pub trait BoardTransportServer: Send + Sync {
    /// Returns the transport plugin name (e.g. `websocket`).
    /// 返回传输插件名称（如 `websocket`）。
    fn name(&self) -> &'static str;
    /// Registers routes or listeners and attaches the shared session handler.
    /// 注册路由或监听器并挂载共享会话处理器。
    fn mount(
        self: Arc<Self>,
        handler: Arc<dyn crate::session::BoardSessionHandler>,
    ) -> Result<(), TransportError>;
}

/// Callback type for pushing control messages on some transports.
/// 部分传输上推送控制消息的回调类型。
#[allow(dead_code)]
pub type ControlWriter = Box<dyn Fn(ControlMessage) -> Result<(), TransportError> + Send + Sync>;
