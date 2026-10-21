use async_trait::async_trait;
use sakuio_board::{BoardSessionHandler, BoardStateStore, CommandCursor, StateError};
use sakuio_db::ClientRepository;
use sakuio_metric::MetricWriter;
use sakuio_protocol::{
    AgentPingResultParams, BasicInfoParams, ControlMessage, ControlOp, HelloParams,
    RemoteExecResultParams, SyncReqParams, SyncRespParams, TelemetryBatch, WelcomeParams,
    METHOD_AGENT_PING_RESULT, METHOD_REMOTE_EXEC_RESULT,
};
use sakuio_db::{ExecRepository, PingRepository};
use sqlx::SqlitePool;
use std::{sync::Arc, time::Duration};

use crate::ingest::IngestPipeline;
use crate::remote::parse_file_result;
use crate::RemoteHub;

const SNAPSHOT_TTL: Duration = Duration::from_secs(120);
const SYNC_READ_LIMIT: u32 = 64;

/// Handles SakuLink control and telemetry for one board transport.
/// 处理单一板级传输上的 SakuLink 控制与遥测。
pub struct LinkSessionHandler {
    db: SqlitePool,
    board: Arc<dyn BoardStateStore>,
    ingest: IngestPipeline,
    remote: Option<Arc<RemoteHub>>,
}

impl LinkSessionHandler {
    /// Builds a handler with DB, board store, and metric writer.
    /// 使用数据库、board 存储与指标写入器构造处理器。
    pub fn new(
        db: SqlitePool,
        board: Arc<dyn BoardStateStore>,
        metrics: Arc<MetricWriter>,
    ) -> Self {
        let ingest = IngestPipeline::new(board.clone(), metrics, SNAPSHOT_TTL);
        Self {
            db,
            board,
            ingest,
            remote: None,
        }
    }

    pub fn with_remote_hub(mut self, remote: Arc<RemoteHub>) -> Self {
        self.remote = Some(remote);
        self
    }

    /// Convenience constructor for v1 single-process deployments.
    /// v1 单进程部署的便捷构造方式。
    pub fn with_memory_board(
        db: SqlitePool,
        board: Arc<dyn BoardStateStore>,
        metrics: Arc<MetricWriter>,
    ) -> Self {
        Self::new(db, board, metrics)
    }
}

#[async_trait]
impl BoardSessionHandler for LinkSessionHandler {
    async fn on_connect(&self, node_uuid: String, session_id: String) -> Result<(), String> {
        let clients = ClientRepository::new(&self.db);
        if clients.is_disabled(&node_uuid).await.map_err(|e| e.to_string())? {
            tracing::warn!(node_uuid = %node_uuid, "agent link rejected: client disabled");
            return Err("client disabled".into());
        }
        clients
            .set_online(&node_uuid, true)
            .await
            .map_err(|e| e.to_string())?;
        clients
            .touch_last_seen(&node_uuid)
            .await
            .map_err(|e| e.to_string())?;
        self.board
            .upsert_presence(&node_uuid, "local", &session_id)
            .await
            .map_err(state_err)?;
        tracing::info!(
            node_uuid = %node_uuid,
            session_id = %session_id,
            "agent board session established"
        );
        Ok(())
    }

    async fn on_disconnect(&self, node_uuid: String, session_id: String) {
        let clients = ClientRepository::new(&self.db);
        if let Err(e) = clients.set_online(&node_uuid, false).await {
            tracing::warn!(
                node_uuid = %node_uuid,
                error = %e,
                "failed to mark client offline"
            );
        }
        if let Err(e) = self.board.clear_presence(&node_uuid, &session_id).await {
            tracing::warn!(
                node_uuid = %node_uuid,
                session_id = %session_id,
                error = %e,
                "failed to clear board presence"
            );
        }
        if let Ok(Some(mut snap)) = self.board.get_latest_snapshot(&node_uuid).await {
            snap.online = false;
            let _ = self
                .board
                .put_latest_snapshot(&node_uuid, snap, SNAPSHOT_TTL)
                .await;
        }
        tracing::info!(
            node_uuid = %node_uuid,
            session_id = %session_id,
            "agent board session closed"
        );
    }

    async fn on_control(
        &self,
        node_uuid: &str,
        session_id: &str,
        msg: ControlMessage,
    ) -> Result<Vec<ControlMessage>, String> {
        match msg.op {
            ControlOp::Hello => {
                let params: HelloParams = serde_json::from_value(msg.p.clone())
                    .map_err(|e| format!("hello params: {e}"))?;
                let clients = ClientRepository::new(&self.db);
                clients
                    .set_agent_version(node_uuid, &params.agent_version)
                    .await
                    .map_err(|e| e.to_string())?;
                clients
                    .touch_last_seen(node_uuid)
                    .await
                    .map_err(|e| e.to_string())?;
                tracing::debug!(
                    node_uuid = %node_uuid,
                    agent_version = %params.agent_version,
                    "received hello"
                );
                let welcome = WelcomeParams {
                    session_id: session_id.to_string(),
                    server_time_ms: chrono::Utc::now().timestamp_millis(),
                    node_uuid: node_uuid.to_string(),
                    node_token: clients.token_by_uuid(node_uuid).await.ok().flatten(),
                    agent_version_range: Some(sakuio_protocol::AgentVersionRange {
                        min: sakuio_protocol::AGENT_VERSION_RANGE_MIN.to_string(),
                        max: sakuio_protocol::AGENT_VERSION_RANGE_MAX.to_string(),
                    }),
                };
                Ok(vec![ControlMessage::new(
                    ControlOp::Welcome,
                    serde_json::to_value(welcome).map_err(|e| e.to_string())?,
                )])
            }
            ControlOp::BasicInfo => {
                let params: BasicInfoParams = serde_json::from_value(msg.p.clone())
                    .map_err(|e| format!("basic_info params: {e}"))?;
                let clients = ClientRepository::new(&self.db);
                let basic_json = serde_json::to_value(&params).map_err(|e| e.to_string())?;
                clients
                    .update_basic_info(node_uuid, &basic_json)
                    .await
                    .map_err(|e| e.to_string())?;
                clients
                    .sync_host_from_basic_info(
                        node_uuid,
                        params.ipv4.as_deref(),
                        params.ipv6.as_deref(),
                        params.version.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                clients
                    .apply_auto_name_from_basic_info(
                        node_uuid,
                        params.hostname.as_deref(),
                        params.os.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                clients
                    .touch_last_seen(node_uuid)
                    .await
                    .map_err(|e| e.to_string())?;
                tracing::debug!(node_uuid = %node_uuid, "basic host info stored");
                Ok(Vec::new())
            }
            ControlOp::Heartbeat => {
                let clients = ClientRepository::new(&self.db);
                clients
                    .touch_last_seen(node_uuid)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Vec::new())
            }
            ControlOp::SyncReq => {
                let params: SyncReqParams = serde_json::from_value(msg.p.clone())
                    .map_err(|e| format!("sync_req params: {e}"))?;
                let cmds = self
                    .board
                    .read_commands(node_uuid, CommandCursor(params.since), SYNC_READ_LIMIT)
                    .await
                    .map_err(state_err)?;
                let resp = SyncRespParams {
                    commands: cmds
                        .into_iter()
                        .map(|c| sakuio_protocol::SyncCommandItem {
                            cursor: c.cursor.0,
                            method: c.method,
                            payload: c.payload,
                        })
                        .collect(),
                };
                let mut out = ControlMessage::new(
                    ControlOp::SyncResp,
                    serde_json::to_value(resp).map_err(|e| e.to_string())?,
                );
                if let Some(id) = msg.id {
                    out = out.with_id(id);
                }
                Ok(vec![out])
            }
            ControlOp::CmdResult => {
                if let Some(hub) = &self.remote {
                    if let Some(result) = parse_file_result(&msg.p) {
                        let _ = hub.resolve_result_async(result).await;
                    }
                }
                if let Err(e) = self.ingest_ping_result(node_uuid, &msg.p).await {
                    tracing::warn!(node_uuid = %node_uuid, error = %e, "ping result ingest failed");
                }
                if let Err(e) = self.ingest_exec_result(&msg.p).await {
                    tracing::warn!(node_uuid = %node_uuid, error = %e, "exec result ingest failed");
                }
                Ok(Vec::new())
            }
            _ => {
                tracing::debug!(
                    node_uuid = %node_uuid,
                    op = ?msg.op,
                    "ignored control op in handler"
                );
                Ok(Vec::new())
            }
        }
    }

    async fn on_telemetry(
        &self,
        node_uuid: &str,
        _session_id: &str,
        batch_seq: u32,
        batch: TelemetryBatch,
    ) -> Result<Option<ControlMessage>, String> {
        if batch.node_uuid != node_uuid {
            tracing::warn!(
                expected = %node_uuid,
                got = %batch.node_uuid,
                "telemetry batch uuid mismatch"
            );
        }
        self.ingest
            .ingest_telemetry(node_uuid, &batch)
            .await
            .map_err(state_err)?;
        let clients = ClientRepository::new(&self.db);
        let _ = clients.touch_last_seen(node_uuid).await;
        // Persist cumulative counters so totals survive board restarts.
        // 持久化累计值，保证面板重启后总流量 / 运行时长不清零。
        if let Some(sample) = batch.samples.last() {
            let _ = clients
                .update_last_runtime(
                    node_uuid,
                    sample.network.total_up,
                    sample.network.total_down,
                    sample.uptime,
                )
                .await;
        }
        tracing::debug!(
            node_uuid = %node_uuid,
            batch_seq,
            sample_count = batch.samples.len(),
            "telemetry batch ingested"
        );
        Ok(Some(ControlMessage::new(
            ControlOp::TelemetryAck,
            serde_json::json!({
                "batch_seq": batch_seq,
                "status": "ok",
            }),
        )))
    }
}

fn state_err(e: StateError) -> String {
    e.to_string()
}

impl LinkSessionHandler {
    async fn ingest_exec_result(&self, payload: &serde_json::Value) -> Result<(), String> {
        let method = payload
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if method != METHOD_REMOTE_EXEC_RESULT {
            return Ok(());
        }
        let params: RemoteExecResultParams = serde_json::from_value(payload.clone())
            .map_err(|e| format!("exec result params: {e}"))?;
        ExecRepository::new(&self.db)
            .finish_result(
                &params.task_id,
                &params.client_uuid,
                &params.result,
                params.exit_code,
            )
            .await
            .map_err(|e| e.to_string())
    }

    async fn ingest_ping_result(&self, node_uuid: &str, payload: &serde_json::Value) -> Result<(), String> {
        let method = payload
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or(METHOD_AGENT_PING_RESULT);
        if method != METHOD_AGENT_PING_RESULT {
            return Ok(());
        }
        let params: AgentPingResultParams = if payload.get("task_id").is_some() {
            serde_json::from_value(payload.clone()).map_err(|e| e.to_string())?
        } else {
            serde_json::from_value(
                payload
                    .get("payload")
                    .cloned()
                    .unwrap_or_else(|| payload.clone()),
            )
            .map_err(|e| e.to_string())?
        };
        let observed_at_ms = chrono::DateTime::parse_from_rfc3339(&params.finished_at)
            .map(|t| t.timestamp_millis())
            .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
        PingRepository::new(&self.db)
            .insert_record(node_uuid, params.task_id as i64, params.value, observed_at_ms)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
