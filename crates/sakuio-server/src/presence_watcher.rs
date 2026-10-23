//! Reconciles persisted online flags with actual websocket sessions.
//! 在线状态兜底任务：清理面板重启遗留的脏在线标记，并按心跳超时下线僵尸节点。

use std::time::Duration;

use sakuio_board::BoardStateStore;
use sakuio_db::ClientRepository;

use crate::AppState;

/// Snapshot TTL used when rewriting an offline snapshot.
/// 覆写离线快照时使用的 TTL。
const SNAPSHOT_TTL: Duration = Duration::from_secs(120);

/// Heartbeat staleness threshold. Agents heartbeat well inside this window,
/// so anything older means the session is gone (crash, network drop, SIGKILL)
/// and no disconnect frame was ever received.
/// 心跳超时阈值。正常心跳远快于该值，超时说明会话已消失
/// （进程崩溃、断网、强杀），且不会再来断开事件。
const STALE_SECS: i64 = 30;

/// Spawns the presence watchdog. Runs one startup sweep synchronously, then
/// reconciles every 10 seconds.
/// 启动看门狗：先同步执行一次启动清理，之后每 10 秒对账一次。
pub async fn spawn_presence_watcher(state: AppState) {
    // Startup sweep: no websocket session can exist before the process boots,
    // so any persisted online flag is stale state left by a previous run
    // (the process may have been killed without any disconnect event).
    // 启动清理：进程刚启动不可能有活跃会话，遗留的 online 标记
    // 全部来自上次运行（可能是被直接杀掉，从未触发断开事件）。
    let clients = ClientRepository::new(&state.db);
    match clients.set_all_offline().await {
        Ok(uuids) if !uuids.is_empty() => {
            tracing::info!(
                count = uuids.len(),
                "cleared stale online flags left by previous run"
            );
            for uuid in &uuids {
                clear_snapshot(&state.board, uuid).await;
                audit_offline(&state, uuid, "cleared stale online flag at startup");
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(
            error = %e,
            "failed to clear stale online flags at startup"
        ),
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            if sakuio_db::maintenance_active() {
                continue;
            }
            let clients = ClientRepository::new(&state.db);
            match clients.mark_stale_offline(STALE_SECS).await {
                Ok(uuids) if !uuids.is_empty() => {
                    for uuid in &uuids {
                        tracing::info!(
                            node_uuid = %uuid,
                            "client marked offline by heartbeat watchdog"
                        );
                        clear_snapshot(&state.board, uuid).await;
                        audit_offline(&state, uuid, "heartbeat timeout");
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    error = %e,
                    "heartbeat watchdog failed to mark stale clients offline"
                ),
            }
        }
    });
}

/// Best-effort rewrite of the cached snapshot so charts/lists stop showing
/// the node as live. The in-memory store is empty after a restart, so this
/// only touches snapshots created during the current process lifetime.
/// 尽力覆写缓存的快照，让图表/列表不再显示该节点在线。
/// 重启后内存 store 为空，因此只会影响本次进程生命周期内产生的快照。
fn audit_offline(state: &AppState, node_uuid: &str, reason: &'static str) {
    let db = state.db.clone();
    let node_uuid = node_uuid.to_string();
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
            "SERVER_OFFLINE",
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

async fn clear_snapshot(board: &sakuio_board::MemoryBoardStore, node_uuid: &str) {
    if let Ok(Some(mut snap)) = board.get_latest_snapshot(node_uuid).await {
        if snap.online {
            snap.online = false;
            let _ = board
                .put_latest_snapshot(node_uuid, snap, SNAPSHOT_TTL)
                .await;
        }
    }
}
