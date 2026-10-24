use crate::AgentCommandBus;
use sakuio_board::BoardStateStore;
use sakuio_protocol::{
    ControlMessage, ControlOp, RemoteFileCommand, RemoteFileResult, RemoteStreamRead,
    RemoteStreamWrite, METHOD_REMOTE_FILE, METHOD_REMOTE_FILE_RESULT, METHOD_REMOTE_STREAM_READ,
    METHOD_REMOTE_STREAM_WRITE,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, RwLock};

const DEFAULT_FILE_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RemoteHub {
    pending: Arc<RwLock<HashMap<String, oneshot::Sender<RemoteFileResult>>>>,
}

impl Default for RemoteHub {
    fn default() -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Clone for RemoteHub {
    fn clone(&self) -> Self {
        Self {
            pending: self.pending.clone(),
        }
    }
}

impl RemoteHub {
    pub async fn resolve_result_async(&self, result: RemoteFileResult) -> bool {
        let tx = self.pending.write().await.remove(&result.req_id);
        if let Some(tx) = tx {
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }

    pub async fn call_file(
        &self,
        board: &dyn BoardStateStore,
        bus: &AgentCommandBus,
        node_uuid: &str,
        op: &str,
        args: serde_json::Value,
    ) -> Result<serde_json::Value, RemoteError> {
        let req_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        {
            let mut guard = self.pending.write().await;
            guard.insert(req_id.clone(), tx);
        }

        let cmd = RemoteFileCommand {
            req_id: req_id.clone(),
            op: op.to_string(),
            args,
        };
        let payload = serde_json::to_value(&cmd).map_err(|_| RemoteError::Internal)?;
        let msg = ControlMessage::new(
            ControlOp::Cmd,
            serde_json::json!({
                "method": METHOD_REMOTE_FILE,
                "payload": payload,
            }),
        );

        let online = bus.push(node_uuid, msg).await;
        if !online {
            let _ = board
                .enqueue_command(node_uuid, METHOD_REMOTE_FILE, payload)
                .await;
            self.pending.write().await.remove(&req_id);
            return Err(RemoteError::Offline);
        }
        let _ = board
            .enqueue_command(
                node_uuid,
                METHOD_REMOTE_FILE,
                serde_json::to_value(&cmd).unwrap(),
            )
            .await;

        match tokio::time::timeout(DEFAULT_FILE_TIMEOUT, rx).await {
            Ok(Ok(result)) => {
                if result.ok {
                    Ok(result.result)
                } else {
                    Err(RemoteError::Agent(
                        result
                            .error
                            .unwrap_or_else(|| "file operation failed".into()),
                    ))
                }
            }
            Ok(Err(_)) => Err(RemoteError::Internal),
            Err(_) => {
                self.pending.write().await.remove(&req_id);
                Err(RemoteError::Timeout)
            }
        }
    }

    async fn push_control_cmd(
        board: &dyn BoardStateStore,
        bus: &AgentCommandBus,
        node_uuid: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<(), RemoteError> {
        let msg = ControlMessage::new(
            ControlOp::Cmd,
            serde_json::json!({
                "method": method,
                "payload": payload,
            }),
        );
        if !bus.push(node_uuid, msg).await {
            let _ = board.enqueue_command(node_uuid, method, payload).await;
            return Err(RemoteError::Offline);
        }
        Ok(())
    }

    pub async fn push_stream_read(
        &self,
        board: &dyn BoardStateStore,
        bus: &AgentCommandBus,
        node_uuid: &str,
        spec: RemoteStreamRead,
    ) -> Result<(), RemoteError> {
        let payload = serde_json::to_value(&spec).map_err(|_| RemoteError::Internal)?;
        Self::push_control_cmd(board, bus, node_uuid, METHOD_REMOTE_STREAM_READ, payload).await
    }

    pub async fn push_stream_write(
        &self,
        board: &dyn BoardStateStore,
        bus: &AgentCommandBus,
        node_uuid: &str,
        spec: RemoteStreamWrite,
    ) -> Result<(), RemoteError> {
        let payload = serde_json::to_value(&spec).map_err(|_| RemoteError::Internal)?;
        Self::push_control_cmd(board, bus, node_uuid, METHOD_REMOTE_STREAM_WRITE, payload).await
    }

    pub async fn push_shell(
        &self,
        board: &dyn BoardStateStore,
        bus: &AgentCommandBus,
        node_uuid: &str,
        payload: serde_json::Value,
    ) -> Result<(), RemoteError> {
        Self::push_control_cmd(
            board,
            bus,
            node_uuid,
            sakuio_protocol::METHOD_REMOTE_SHELL,
            payload,
        )
        .await
    }

    pub async fn push_exec(
        &self,
        board: &dyn BoardStateStore,
        bus: &AgentCommandBus,
        node_uuid: &str,
        open: sakuio_protocol::RemoteExecOpen,
    ) -> Result<(), RemoteError> {
        let payload = serde_json::to_value(&open).map_err(|_| RemoteError::Internal)?;
        Self::push_control_cmd(
            board,
            bus,
            node_uuid,
            sakuio_protocol::METHOD_REMOTE_EXEC,
            payload,
        )
        .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    #[error("agent offline")]
    Offline,
    #[error("timeout")]
    Timeout,
    #[error("{0}")]
    Agent(String),
    #[error("internal")]
    Internal,
}

pub fn parse_file_result(payload: &serde_json::Value) -> Option<RemoteFileResult> {
    let method = payload
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or(METHOD_REMOTE_FILE_RESULT);
    if method != METHOD_REMOTE_FILE_RESULT {
        return None;
    }
    if let Some(inner) = payload.get("payload") {
        if let Ok(r) = serde_json::from_value::<RemoteFileResult>(inner.clone()) {
            return Some(r);
        }
    }
    serde_json::from_value(payload.clone()).ok()
}
