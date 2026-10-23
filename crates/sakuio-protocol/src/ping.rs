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
pub const METHOD_AGENT_PING_BATCH_RESULT: &str = "agent.pingBatchResult";

/// `agent.pingBatchResult` payload; one frame carries all results from a batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentPingBatchResultParams {
    pub results: Vec<AgentPingResultParams>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_batch_result_round_trips() {
        let params = AgentPingBatchResultParams {
            results: vec![
                AgentPingResultParams {
                    task_id: 1,
                    ping_type: "icmp".into(),
                    value: 12,
                    finished_at: "2026-10-24T00:00:00Z".into(),
                },
                AgentPingResultParams {
                    task_id: 2,
                    ping_type: "tcp".into(),
                    value: -1,
                    finished_at: "2026-10-24T00:00:01Z".into(),
                },
            ],
        };
        let encoded = serde_json::to_value(&params).unwrap();
        let decoded: AgentPingBatchResultParams = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.results.len(), 2);
        assert_eq!(decoded.results[1].task_id, 2);
    }
}
