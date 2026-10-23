use serde::{Deserialize, Serialize};

/// `agent.ping` command payload (`ping_task_id`, `ping_type`, `ping_target`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPingParams {
    #[serde(rename = "ping_task_id")]
    pub task_id: u64,
    #[serde(rename = "ping_type")]
    pub ping_type: String,
    #[serde(rename = "ping_target")]
    pub target: String,
}

/// `agent.pingBatch` command payload; executes several ping tasks in one push.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPingBatchParams {
    pub tasks: Vec<AgentPingParams>,
}

/// `agent.pingResult` payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPingResultParams {
    pub task_id: u64,
    pub ping_type: String,
    pub value: i32,
    pub finished_at: String,
}

pub const METHOD_AGENT_PING: &str = "agent.ping";
pub const METHOD_AGENT_PING_BATCH: &str = "agent.pingBatch";
pub const METHOD_AGENT_PING_RESULT: &str = "agent.pingResult";
