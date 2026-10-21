use crate::AgentCommandBus;
use sakuio_board::{BoardStateStore, CommandCursor};
use sakuio_protocol::{ControlMessage, ControlOp, METHOD_AGENT_PING};

const FLUSH_LIMIT: u32 = 64;

/// Replays queued `agent.ping` commands to a node that just connected.
pub async fn flush_pending_ping_commands(
    board: &dyn BoardStateStore,
    bus: &AgentCommandBus,
    node_uuid: &str,
) {
    let Ok(cmds) = board
        .read_commands(node_uuid, CommandCursor(0), FLUSH_LIMIT)
        .await
    else {
        return;
    };
    for cmd in cmds {
        if cmd.method != METHOD_AGENT_PING {
            continue;
        }
        let msg = ControlMessage::new(
            ControlOp::Cmd,
            serde_json::json!({
                "method": cmd.method,
                "payload": cmd.payload,
            }),
        );
        let _ = bus.push(node_uuid, msg).await;
    }
}
