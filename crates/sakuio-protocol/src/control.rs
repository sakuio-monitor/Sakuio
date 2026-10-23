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
    /// Board-initiated runtime configuration update.
    /// 面板发起的运行时配置更新。
    Config,
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

/// Board-advertised compatible agent version range (inclusive bounds).
/// 面板声明的兼容 Agent 版本区间（含边界）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentVersionRange {
    pub min: String,
    pub max: String,
}

/// 兼容区间随面板发布维护：协议变更或新增兼容 Agent 版本时同步调整。
/// Agent 自动更新只升到该区间内的最新版本；旧面板未下发区间时 Agent 保守地只升当前版本线。
pub const AGENT_VERSION_RANGE_MIN: &str = "0.0.1";
pub const AGENT_VERSION_RANGE_MAX: &str = "0.2.5";

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
    /// Telemetry collection/report interval in milliseconds (1000 = 1s).
    /// 遥测采集/上报间隔（毫秒，1000 = 1 秒）。
    #[serde(default = "default_telemetry_interval_ms")]
    pub telemetry_interval_ms: u64,
    /// Compatible agent version range advertised by the board.
    /// 面板声明的兼容 Agent 版本区间，自动更新以此为上界。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version_range: Option<AgentVersionRange>,
}

/// Default telemetry interval when the board does not send one.
/// 面板未下发时的默认遥测间隔。
pub const DEFAULT_TELEMETRY_INTERVAL_MS: u64 = 1000;

fn default_telemetry_interval_ms() -> u64 {
    DEFAULT_TELEMETRY_INTERVAL_MS
}

/// Runtime configuration pushed by the board (`op = "config"`).
/// 面板下发的运行时配置（`op = "config"`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigParams {
    pub telemetry_interval_ms: u64,
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
