//! SakuLink protocol types: control JSON envelope and SKTL telemetry frames.
//! SakuLink 协议类型：控制面 JSON 信封与 SKTL 遥测帧。

mod control;
mod ping;
mod remote;
mod sktl;
mod telemetry;

pub use control::{
    BasicInfoParams, ControlMessage, ControlOp, HelloParams, SyncCommandItem, SyncReqParams,
    SyncRespParams, TelemetryAckParams, WelcomeParams,
};
pub use sktl::{decode_sktl, encode_sktl, SkltPayload, SKTL_MAGIC};
pub use ping::{
    AgentPingParams, AgentPingResultParams, METHOD_AGENT_PING, METHOD_AGENT_PING_RESULT,
};
pub use remote::{
    BoardRelaySpec, RemoteExecOpen, RemoteExecResultParams, RemoteFileCommand, RemoteFileResult,
    RemoteShellOpen, RemoteStreamRead, RemoteStreamWrite, METHOD_REMOTE_EXEC,
    METHOD_REMOTE_EXEC_RESULT, METHOD_REMOTE_FILE, METHOD_REMOTE_FILE_RESULT, METHOD_REMOTE_SHELL,
    METHOD_REMOTE_STREAM_READ, METHOD_REMOTE_STREAM_WRITE,
};
pub use telemetry::{
    ConnectionsReport, CpuReport, GpuDetailedReport, GpuDeviceReport, GpuModelsReport, LoadReport,
    NetworkReport, TelemetryBatch, TelemetrySample, UsageReport,
};

/// SakuLink protocol version carried in control messages and SKTL headers.
/// 控制消息与 SKTL 头中携带的 SakuLink 协议版本。
pub const SAKULINK_VERSION: u8 = 1;
