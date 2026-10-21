mod exec_api;
mod fs_api;
mod relay_api;
mod shell_api;

pub use exec_api::{exec_stream_ws, exec_task, exec_task_results, ExecStreamRegistry};
pub use fs_api::fs_routes;
pub use relay_api::agent_relay_ws;
pub use shell_api::shell_ws;

use sakuio_protocol::BoardRelaySpec;

pub fn build_relay_spec(relay_id: &str, node_token: &str, host: &str) -> BoardRelaySpec {
    BoardRelaySpec {
        relay_id: relay_id.to_string(),
        path: format!("ws://{host}/api/agent/relay/{relay_id}"),
        token: node_token.to_string(),
    }
}
