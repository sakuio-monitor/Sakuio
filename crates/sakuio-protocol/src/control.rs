use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Control channel identifier on the board link.
/// 板级链路上的控制通道标识。
pub const CHANNEL_CTL: &str = "ctl";

/// Control-plane operation names (not JSON-RPC).
/// 控制面操作名（非 JSON-RPC）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlOp {
    Hello,
    Welcome,
    Heartbeat,
    BasicInfo,
    Cmd,
    CmdResult,
    TelemetryAck,
    SyncReq,
    SyncResp,
    Error,
}

/// UTF-8 JSON control envelope for all board Text frames.
/// 所有 Board Text 帧使用的 UTF-8 JSON 控制信封。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlMessage {
    pub v: u8,
    pub ch: String,
    pub op: ControlOp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<i64>,
    #[serde(default)]
    pub p: Value,
}

impl ControlMessage {
    /// Builds a control message with the current protocol version and timestamp.
    /// 使用当前协议版本与时间戳构造控制消息。
    pub fn new(op: ControlOp, p: Value) -> Self {
        Self {
            v: super::SAKULINK_VERSION,
            ch: CHANNEL_CTL.to_string(),
            op,
            id: None,
            ts: Some(unix_millis()),
            p,
        }
    }

    /// Sets the correlation id for request/response pairing.
    /// 设置请求/响应配对的关联 id。
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Serializes the message to JSON bytes.
    /// 将消息序列化为 JSON 字节。
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Parses a control message from JSON bytes.
    /// 从 JSON 字节解析控制消息。
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

fn unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Agent hello payload after the transport connects.
/// 传输连接建立后 Agent 发送的 hello 载荷。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloParams {
    pub agent_version: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// Server welcome payload after token authentication.
/// Token 鉴权通过后服务端下发的 welcome 载荷。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WelcomeParams {
    pub session_id: String,
    pub server_time_ms: i64,
    pub node_uuid: String,
    /// Node token assigned by the server (auto-discovery registration).
    /// 服务端分配的节点 token（自动发现注册时下发）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_token: Option<String>,
}

/// Static host information reported by the agent at connect time.
/// 主机静态信息（Agent 连接时上报）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicInfoParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    pub cpu_name: Option<String>,
    pub cpu_cores: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_physical_cores: Option<i32>,
    pub arch: Option<String>,
    pub os: Option<String>,
    pub kernel_version: Option<String>,
    pub ipv4: Option<String>,
    pub ipv6: Option<String>,
    pub mem_total: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swap_total: Option<i64>,
    pub disk_total: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub virtualization: Option<String>,
    pub version: Option<String>,
}

/// Acknowledgement for an ingested telemetry batch.
/// 对已入库遥测批次的确认。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryAckParams {
    pub batch_seq: u32,
    pub status: String,
}

/// Agent pull for pending commands after reconnect.
/// 重连后 Agent 拉取待执行命令。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReqParams {
    #[serde(default)]
    pub since: u64,
}

/// Server response to sync with command envelopes.
/// 服务端 sync 响应与命令信封列表。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRespParams {
    pub commands: Vec<SyncCommandItem>,
}

/// One command item in a sync response.
/// sync 响应中的单条命令。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncCommandItem {
    pub cursor: u64,
    pub method: String,
    pub payload: Value,
}
