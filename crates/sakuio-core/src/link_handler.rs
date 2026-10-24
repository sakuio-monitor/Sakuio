use async_trait::async_trait;
use sakuio_board::{BoardSessionHandler, BoardStateStore, CommandCursor, StateError};
use sakuio_db::ClientRepository;
use sakuio_db::DbPool;
use sakuio_db::{ExecRepository, PingRepository};
use sakuio_metric::MetricWriter;
use sakuio_protocol::{
    AgentPingBatchResultParams, AgentPingResultParams, BasicInfoParams, ControlMessage, ControlOp,
    HelloParams, RemoteExecResultParams, SyncReqParams, SyncRespParams, TelemetryBatch,
    WelcomeParams, METHOD_AGENT_PING_BATCH_RESULT, METHOD_AGENT_PING_RESULT,
    METHOD_REMOTE_EXEC_RESULT,
};
use std::{sync::Arc, time::Duration};

use crate::ingest::IngestPipeline;
use crate::remote::parse_file_result;
use crate::RemoteHub;

const SNAPSHOT_TTL: Duration = Duration::from_secs(120);
const SYNC_READ_LIMIT: u32 = 64;

/// Handles SakuLink control and telemetry for one board transport.
/// 处理单一板级传输上的 SakuLink 控制与遥测。
pub struct LinkSessionHandler {
    db: DbPool,
    board: Arc<dyn BoardStateStore>,
    ingest: IngestPipeline,
    remote: Option<Arc<RemoteHub>>,
    geo_wake: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl LinkSessionHandler {
    /// Builds a handler with DB, board store, and metric writer.
    /// 使用数据库、board 存储与指标写入器构造处理器。
    pub fn new(db: DbPool, board: Arc<dyn BoardStateStore>, metrics: Arc<MetricWriter>) -> Self {
        let ingest = IngestPipeline::new(board.clone(), metrics, SNAPSHOT_TTL);
        Self {
            db,
            board,
            ingest,
            remote: None,
            geo_wake: None,
        }
    }

    /// Registers a callback fired after a node reports new basic information.
    pub fn with_geo_wake(mut self, wake: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.geo_wake = Some(wake);
        self
    }

    pub fn with_remote_hub(mut self, remote: Arc<RemoteHub>) -> Self {
        self.remote = Some(remote);
        self
    }

    /// Convenience constructor for v1 single-process deployments.
    /// v1 单进程部署的便捷构造方式。
    pub fn with_memory_board(
        db: DbPool,
        board: Arc<dyn BoardStateStore>,
        metrics: Arc<MetricWriter>,
    ) -> Self {
        Self::new(db, board, metrics)
    }
}

#[async_trait]
impl BoardSessionHandler for LinkSessionHandler {
    async fn on_connect(&self, node_uuid: String, session_id: String) -> Result<(), String> {
        if sakuio_db::maintenance_active() {
            return Err("server maintenance in progress".into());
        }
        let clients = ClientRepository::new(&self.db);
        if clients
            .is_disabled(&node_uuid)
            .await
            .map_err(|e| e.to_string())?
        {
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
        audit_presence(&self.db, &node_uuid, true, "agent connected");
        Ok(())
    }

    async fn on_disconnect(&self, node_uuid: String, session_id: String) {
        // Do not write the online flag while the main database is being copied.
        // 主库复制期间不写在线标记。
        if !sakuio_db::maintenance_active() {
            let clients = ClientRepository::new(&self.db);
            if let Err(e) = clients.set_online(&node_uuid, false).await {
                tracing::warn!(
                    node_uuid = %node_uuid,
                    error = %e,
                    "failed to mark client offline"
                );
            }
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
        if !sakuio_db::maintenance_active() {
            audit_presence(&self.db, &node_uuid, false, "agent disconnected");
        }
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
                let interval_ms = clients
                    .report_interval_ms(node_uuid)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(sakuio_protocol::DEFAULT_TELEMETRY_INTERVAL_MS as i64)
                    .clamp(200, 3_600_000) as u64;
                let welcome = WelcomeParams {
                    session_id: session_id.to_string(),
                    server_time_ms: chrono::Utc::now().timestamp_millis(),
                    node_uuid: node_uuid.to_string(),
                    node_token: clients.token_by_uuid(node_uuid).await.ok().flatten(),
                    telemetry_interval_ms: interval_ms,
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
                if let Some(wake) = &self.geo_wake {
                    wake();
                }
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
        // One UPDATE per batch covers last_seen and the cumulative snapshot.
        if let Some(sample) = batch.samples.last() {
            let _ = clients
                .touch_last_seen_with_runtime(
                    node_uuid,
                    sample.network.total_up,
                    sample.network.total_down,
                    sample.uptime,
                )
                .await;
        } else {
            let _ = clients.touch_last_seen(node_uuid).await;
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
        let method = payload.get("method").and_then(|v| v.as_str()).unwrap_or("");
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

    async fn ingest_ping_result(
        &self,
        node_uuid: &str,
        payload: &serde_json::Value,
    ) -> Result<(), String> {
        let method = payload
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or(METHOD_AGENT_PING_RESULT);
        if method == METHOD_AGENT_PING_BATCH_RESULT {
            // Agent sends `{"method": ..., "results": [...]}`, while the
            // struct expects `{"results": [...]}`. Do not deserialize the
            // array itself, or every batch result is silently rejected.
            let params = parse_ping_batch_result(payload)?;
            for result in params.results {
                self.insert_ping_result(node_uuid, result).await?;
            }
            return Ok(());
        }
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
        self.insert_ping_result(node_uuid, params).await
    }

    async fn insert_ping_result(
        &self,
        node_uuid: &str,
        params: AgentPingResultParams,
    ) -> Result<(), String> {
        let observed_at_ms = chrono::DateTime::parse_from_rfc3339(&params.finished_at)
            .map(|t| t.timestamp_millis())
            .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
        PingRepository::new(&self.db)
            .insert_record(
                node_uuid,
                params.task_id as i64,
                params.value,
                observed_at_ms,
            )
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

fn parse_ping_batch_result(
    payload: &serde_json::Value,
) -> Result<AgentPingBatchResultParams, String> {
    if let Some(results) = payload.get("results") {
        return serde_json::from_value(serde_json::json!({ "results": results }))
            .map_err(|e| format!("ping batch result params: {e}"));
    }
    serde_json::from_value(
        payload
            .get("payload")
            .cloned()
            .unwrap_or_else(|| payload.clone()),
    )
    .map_err(|e| format!("ping batch result params: {e}"))
}

fn audit_presence(db: &DbPool, node_uuid: &str, online: bool, reason: &'static str) {
    let db = db.clone();
    let node_uuid = node_uuid.to_string();
    let action = if online {
        "SERVER_ONLINE"
    } else {
        "SERVER_OFFLINE"
    };
    tokio::spawn(async move {
        let ip = ClientRepository::new(&db)
            .address_by_uuid(&node_uuid)
            .await
            .ok()
            .flatten()
            .unwrap_or_default();
        let geo = sakuio_db::GeoCacheRepository::new(&db)
            .get(&ip)
            .await
            .ok()
            .flatten();
        let (geo_code, geo_location) = geo
            .map(|row| (row.country_code, row.location))
            .unwrap_or_default();
        if let Err(error) = sakuio_db::record_system_event(
            &db,
            action,
            &node_uuid,
            reason,
            &ip,
            &geo_code,
            &geo_location,
        )
        .await
        {
            tracing::warn!(node_uuid = %node_uuid, error = %error, "presence audit write failed");
        }
    });
}

#[cfg(test)]
mod ping_batch_tests {
    use super::*;

    #[test]
    fn parses_agent_batch_result_envelope() {
        let payload = serde_json::json!({
            "method": METHOD_AGENT_PING_BATCH_RESULT,
            "results": [
                {"task_id": 7, "ping_type": "icmp", "value": 12, "finished_at": "2026-10-24T00:00:00Z"}
            ]
        });
        let parsed = parse_ping_batch_result(&payload).expect("batch result");
        assert_eq!(parsed.results.len(), 1);
        assert_eq!(parsed.results[0].task_id, 7);
    }
}
