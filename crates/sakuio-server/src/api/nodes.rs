use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    response::IntoResponse,
    Json,
};
use sakuio_board::BoardStateStore;
use sakuio_db::{AdminClientPatch, ClientRepository, NewClient, PingRepository};
use serde::Deserialize;
use std::convert::Infallible;
use std::time::Duration;

#[derive(Deserialize)]
pub struct AddClientBody {
    #[serde(default)]
    pub name: String,
}

#[derive(Deserialize)]
pub struct ListAdminClientsQuery {
    pub cursor: Option<String>,
    #[serde(default = "default_admin_clients_limit")]
    pub limit: u32,
}

fn default_admin_clients_limit() -> u32 {
    20
}

/// 按节点 IP（v4 优先）填充 v4 / v6 归属地国家代码（统一 GeoIP 缓存）。
fn fill_geo_regions(region: &mut String, region6: &mut String, ipv4: &str, ipv6: &str) {
    let v4 = ipv4.trim();
    let v6 = ipv6.trim();
    *region = crate::geoip_resolver::region_code_for(if v4.is_empty() { v6 } else { v4 });
    *region6 = crate::geoip_resolver::region_code_for(v6);
}

/// GET `/api/admin/clients` — admin client registry (cursor pagination).
/// GET `/api/admin/clients`：管理端客户端注册表（游标分页）。
pub async fn list_admin_clients(
    State(state): State<AppState>,
    Query(query): Query<ListAdminClientsQuery>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo
        .list_admin_page(query.cursor.as_deref(), query.limit)
        .await
    {
        Ok(mut page) => {
            for client in &mut page.items {
                fill_geo_regions(&mut client.region, &mut client.region6, &client.ipv4, &client.ipv6);
                if let Ok(Some(snap)) = state.board.get_latest_snapshot(&client.uuid).await {
                    if snap.observed_at_ms > 0 {
                        client.online = snap.online;
                    }
                }
            }
            Json(serde_json::json!({
                "items": page.items,
                "next_cursor": page.next_cursor,
            }))
                .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "list admin clients failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// 当前页快照（region + 在线状态已填充），SSE 与 POST 同步端点共用。
async fn admin_clients_snapshot(
    state: &AppState,
    cursor: Option<&str>,
    limit: u32,
) -> Result<serde_json::Value, sakuio_db::DbError> {
    let repo = ClientRepository::new(&state.db);
    let mut page = repo.list_admin_page(cursor, limit).await?;
    for client in &mut page.items {
        fill_geo_regions(&mut client.region, &mut client.region6, &client.ipv4, &client.ipv6);
        if let Ok(Some(snap)) = state.board.get_latest_snapshot(&client.uuid).await {
            if snap.observed_at_ms > 0 {
                client.online = snap.online;
            }
            client.runtime = Some(serde_json::json!({
                "net_total_up": snap.net_total_up,
                "net_total_down": snap.net_total_down,
                "disk_used": snap.disk_used,
                "disk_total": snap.disk_total,
                "uptime_secs": snap.uptime_secs,
            }));
        }
    }
    Ok(serde_json::json!({
        "items": page.items,
        "next_cursor": page.next_cursor,
        "total": page.total,
    }))
}

#[derive(Deserialize)]
pub struct SyncAdminClientsBody {
    pub cursor: Option<String>,
    #[serde(default = "default_admin_clients_limit")]
    pub limit: u32,
}

/// POST `/api/admin/clients/sync` — SSE 不可用时的轮询降级端点（当前页 + 总数）。
pub async fn sync_admin_clients(
    State(state): State<AppState>,
    Json(body): Json<SyncAdminClientsBody>,
) -> impl IntoResponse {
    match admin_clients_snapshot(&state, body.cursor.as_deref(), body.limit).await {
        Ok(payload) => Json(payload).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "sync admin clients failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// GET `/api/admin/clients/stream` — SSE：当前页快照变化即推送（内容不变不发送）。
pub async fn stream_admin_clients(
    State(state): State<AppState>,
    Query(query): Query<ListAdminClientsQuery>,
) -> impl IntoResponse {
    let cursor = query.cursor.clone();
    let limit = query.limit;
    let stream = futures_util::stream::unfold(
        (
            state,
            tokio::time::interval(Duration::from_secs(4)),
            String::new(),
        ),
        move |(state, mut interval, mut last_json)| {
            let cursor = cursor.clone();
            async move {
                loop {
                    interval.tick().await;
                    match admin_clients_snapshot(&state, cursor.as_deref(), limit).await {
                        Ok(payload) => {
                            let json = payload.to_string();
                            if json == last_json {
                                continue;
                            }
                            last_json = json.clone();
                            let event = SseEvent::default().event("clients").data(json);
                            break Some((Ok::<_, Infallible>(event), (state, interval, last_json)));
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "admin clients snapshot failed");
                            continue;
                        }
                    }
                }
            }
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// POST `/api/admin/clients` — creates uuid + token (v1, no auth yet).
/// POST `/api/admin/clients`：创建 uuid 与 token（v1，尚无鉴权）。
pub async fn add_client(
    State(state): State<AppState>,
    Json(body): Json<AddClientBody>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo.insert(&NewClient {
        name: body.name.clone(),
    })
    .await
    {
        Ok((uuid, token)) => {
            let ping_repo = PingRepository::new(&state.db);
            if let Err(e) = ping_repo.append_default_on_client(&uuid).await {
                tracing::warn!(error = %e, uuid = %uuid, "append default ping tasks failed");
            }
            state.ping_scheduler.reload().await;
            Json(serde_json::json!({ "uuid": uuid, "token": token })).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "add client failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// PATCH `/api/admin/clients/:uuid` — update server metadata.
pub async fn patch_admin_client(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<AdminClientPatch>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo.patch_admin(&uuid, &body).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "message": "client not found" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, uuid = %uuid, "patch admin client failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/admin/clients/:uuid/reset-traffic` — 手动重置流量账本。
pub async fn reset_client_traffic(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo.reset_traffic(&uuid).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, uuid = %uuid, "reset client traffic failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// DELETE `/api/admin/clients/:uuid` — remove server registration.
pub async fn delete_admin_client(
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo.delete_by_uuid(&uuid).await {
        Ok(true) => {
            // 节点已删除：注销命令通道，agent 的 link 会话随即断开并下线。
            state.command_bus.unregister(&uuid).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "message": "client not found" })),
        )
            .into_response(),
        Err(e) => {
            tracing::error!(error = %e, uuid = %uuid, "delete admin client failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct ReorderClientBody {
    /// Client to move.
    pub uuid: String,
    /// Insert before this client's uuid; omit / null to move to the end.
    /// 插到该 uuid 之前；省略表示移到末尾。
    #[serde(default)]
    pub before: Option<String>,
}

/// PUT `/api/admin/clients/reorder` — move one client for drag-and-drop sort.
/// PUT `/api/admin/clients/reorder`：拖拽排序（移动单个节点位置）。
pub async fn reorder_client(
    State(state): State<AppState>,
    Json(body): Json<ReorderClientBody>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo.reorder_client(&body.uuid, body.before.as_deref()).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, uuid = %body.uuid, "reorder client failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

#[derive(Deserialize)]
pub struct BatchClientsBody {
    pub uuids: Vec<String>,
    /// Only for the batch-group action: group list joined by commas.
    /// 仅批量分组动作使用：逗号分隔的分组列表。
    #[serde(default)]
    pub group_name: Option<String>,
}

/// POST `/api/admin/clients/batch-delete` — bulk remove server registrations.
/// POST `/api/admin/clients/batch-delete`：批量删除节点。
pub async fn batch_delete_clients(
    State(state): State<AppState>,
    Json(body): Json<BatchClientsBody>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    match repo.delete_by_uuids(&body.uuids).await {
        Ok(removed) => {
            // 已删除的节点：逐个注销命令通道，断开对应 agent 的连接。
            for uuid in &body.uuids {
                state.command_bus.unregister(uuid).await;
            }
            Json(serde_json::json!({ "removed": removed })).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, count = body.uuids.len(), "batch delete clients failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/admin/clients/batch-group` — bulk set group for servers.
/// POST `/api/admin/clients/batch-group`：批量设置节点分组。
pub async fn batch_set_group(
    State(state): State<AppState>,
    Json(body): Json<BatchClientsBody>,
) -> impl IntoResponse {
    let group = body.group_name.unwrap_or_default();
    let repo = ClientRepository::new(&state.db);
    match repo.set_group_for_many(&body.uuids, group.trim()).await {
        Ok(updated) => Json(serde_json::json!({ "updated": updated })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, count = body.uuids.len(), "batch set group failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}
