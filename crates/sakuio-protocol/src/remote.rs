use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Control-channel method: filesystem metadata operation (agent-bound).
pub const METHOD_REMOTE_FILE: &str = "remote.file";
pub const METHOD_REMOTE_FILE_RESULT: &str = "remote.file.result";

/// Control-channel method: attach shell to a relay id.
pub const METHOD_REMOTE_SHELL: &str = "remote.shell";

/// Data-plane file read (agent opens relay and streams bytes).
pub const METHOD_REMOTE_STREAM_READ: &str = "remote.stream.read";
/// Data-plane file write (agent opens relay and accepts bytes).
pub const METHOD_REMOTE_STREAM_WRITE: &str = "remote.stream.write";

/// One-shot remote command; stdout/stderr stream on relay, final metadata on CmdResult.
pub const METHOD_REMOTE_EXEC: &str = "remote.exec";
pub const METHOD_REMOTE_EXEC_RESULT: &str = "remote.exec.result";

/// Opaque relay handle issued by the server; agent dials `path` on its transport plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardRelaySpec {
    pub relay_id: String,
    /// Server-relative or absolute URL path the agent should open (plugin-defined).
    pub path: String,
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFileCommand {
    pub req_id: String,
    pub op: String,
    #[serde(default)]
    pub args: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteFileResult {
    pub req_id: String,
    pub ok: bool,
    #[serde(default)]
    pub result: Value,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteShellOpen {
    pub relay: BoardRelaySpec,
    #[serde(default)]
    pub cols: u16,
    #[serde(default)]
    pub rows: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStreamRead {
    pub relay: BoardRelaySpec,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteStreamWrite {
    pub relay: BoardRelaySpec,
    pub path: String,
    #[serde(default)]
    pub truncate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteExecOpen {
    pub relay: BoardRelaySpec,
    pub command: String,
    pub task_id: String,
    pub client_uuid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteExecResultParams {
    pub task_id: String,
    pub client_uuid: String,
    pub result: String,
    pub exit_code: i32,
    pub finished_at: String,
}
