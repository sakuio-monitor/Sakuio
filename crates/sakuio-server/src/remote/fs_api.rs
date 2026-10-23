use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::routing::{delete, get, post};
use std::convert::Infallible;
use axum::{Json, Router};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::auth::AdminUser;
use crate::remote::build_relay_spec;
use crate::AppState;
use sakuio_core::RemoteError;
use sakuio_db::ClientRepository;
use sakuio_protocol::{RemoteStreamRead, RemoteStreamWrite};

pub fn fs_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/admin/nodes/:uuid/fs/roots",
            get(fs_roots),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/list",
            get(fs_list),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/stat",
            get(fs_stat),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/mkdir",
            post(fs_mkdir),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/delete",
            delete(fs_delete),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/move",
            post(fs_move),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/copy",
            post(fs_copy),
        )
        .route(
            "/api/admin/nodes/:uuid/fs/text",
            get(fs_read_text).put(fs_write_text),
        )
        .route("/api/admin/nodes/:uuid/fs/raw", get(fs_download).put(fs_upload))
        .route("/api/admin/nodes/:uuid/fs/search", get(fs_search))
        .route("/api/admin/nodes/:uuid/fs/rename", post(fs_rename))
        .route("/api/admin/nodes/:uuid/fs/chmod", post(fs_chmod))
        .route("/api/admin/nodes/:uuid/fs/chown", post(fs_chown))
        .route("/api/admin/nodes/:uuid/fs/trash", get(fs_trash_list))
        .route("/api/admin/nodes/:uuid/fs/trash/restore", post(fs_trash_restore))
        .route("/api/admin/nodes/:uuid/fs/trash/empty", post(fs_trash_empty))
        .route(
            "/api/admin/nodes/:uuid/fs/trash/delete",
            post(fs_trash_delete),
        )
}

#[derive(Deserialize)]
struct PathQuery {
    path: String,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    permanent: Option<bool>,
}

#[derive(Deserialize)]
struct SearchQuery {
    path: String,
    query: String,
}

#[derive(Deserialize)]
struct ChmodBody {
    path: String,
    mode: String,
}

#[derive(Deserialize)]
struct ChownBody {
    path: String,
    #[serde(default)]
    owner: String,
    #[serde(default)]
    group: String,
}

#[derive(Deserialize)]
struct TrashIdBody {
    trash_id: String,
}

#[derive(Deserialize)]
struct MoveBody {
    from: String,
    to: String,
}

#[derive(Deserialize)]
struct WriteTextBody {
    content: String,
}

async fn fs_roots(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(&state, &uuid, "list_roots", json!({})).await
}

async fn fs_list(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "list",
        json!({
            "path": q.path,
            "offset": q.offset.unwrap_or(0),
            "limit": q.limit.unwrap_or(200),
        }),
    )
    .await
}

async fn fs_stat(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(&state, &uuid, "stat", json!({ "path": q.path })).await
}

async fn fs_mkdir(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<PathQuery>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(&state, &uuid, "mkdir", json!({ "path": body.path })).await
}

async fn fs_delete(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "delete",
        json!({
            "path": q.path,
            "permanent": q.permanent.unwrap_or(false),
        }),
    )
    .await
}

async fn fs_move(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<MoveBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "move",
        json!({ "from": body.from, "to": body.to }),
    )
    .await
}

async fn fs_copy(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<MoveBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "copy",
        json!({ "from": body.from, "to": body.to }),
    )
    .await
}

async fn fs_read_text(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(&state, &uuid, "read_text", json!({ "path": q.path })).await
}

async fn fs_write_text(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
    Json(body): Json<WriteTextBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "write_text",
        json!({ "path": q.path, "content": body.content }),
    )
    .await
}

async fn fs_search(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "search",
        json!({ "path": q.path, "query": q.query }),
    )
    .await
}

async fn fs_rename(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<MoveBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "rename",
        json!({ "from": body.from, "to": body.to }),
    )
    .await
}

async fn fs_chmod(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<ChmodBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "chmod",
        json!({ "path": body.path, "mode": body.mode }),
    )
    .await
}

async fn fs_chown(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<ChownBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "chown",
        json!({ "path": body.path, "owner": body.owner, "group": body.group }),
    )
    .await
}

async fn fs_trash_list(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(&state, &uuid, "trash_list", json!({})).await
}

async fn fs_trash_restore(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<TrashIdBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "trash_restore",
        json!({ "trash_id": body.trash_id }),
    )
    .await
}

async fn fs_trash_empty(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(&state, &uuid, "trash_empty", json!({})).await
}

async fn fs_trash_delete(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Json(body): Json<TrashIdBody>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    call(
        &state,
        &uuid,
        "trash_delete",
        json!({ "trash_id": body.trash_id }),
    )
    .await
}

async fn fs_download(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
) -> Result<Response, (StatusCode, String)> {
    let token = node_token(&state, &uuid).await?;
    let relay_id = Uuid::new_v4().to_string();
    state.relay.register(relay_id.clone(), &uuid).await;
    let relay = build_relay_spec(&relay_id, &token);
    let rx = state.relay.open_client_download(&relay_id).await;
    state
        .remote
        .push_stream_read(
            state.board.as_ref(),
            &state.command_bus,
            &uuid,
            RemoteStreamRead {
                relay,
                path: q.path.clone(),
            },
        )
        .await
        .map_err(map_remote_err)?;

    let filename = q
        .path
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("download");
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Some(chunk) => Some((Ok::<Bytes, Infallible>(Bytes::from(chunk)), rx)),
            None => None,
        }
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from_stream(stream))
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "response".into()))
}

async fn fs_upload(
    _user: AdminUser,
    State(state): State<AppState>,
    Path(uuid): Path<String>,
    Query(q): Query<PathQuery>,
    body: Body,
) -> Result<Json<Value>, (StatusCode, String)> {
    let token = node_token(&state, &uuid).await?;
    let relay_id = Uuid::new_v4().to_string();
    state.relay.register(relay_id.clone(), &uuid).await;
    let feed_tx = state.relay.bind_upload_feed(&relay_id).await;
    let relay = build_relay_spec(&relay_id, &token);
    state
        .remote
        .push_stream_write(
            state.board.as_ref(),
            &state.command_bus,
            &uuid,
            RemoteStreamWrite {
                relay,
                path: q.path.clone(),
                truncate: true,
            },
        )
        .await
        .map_err(map_remote_err)?;

    let mut body = body.into_data_stream();
    while let Some(frame) = body.next().await {
        let chunk = frame.map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        if feed_tx.send(chunk.to_vec()).await.is_err() {
            break;
        }
    }
    drop(feed_tx);
    Ok(Json(json!({ "ok": true })))
}

async fn node_token(state: &AppState, uuid: &str) -> Result<String, (StatusCode, String)> {
    ClientRepository::new(&state.db)
        .token_by_uuid(uuid)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "db".into()))?
        .ok_or((StatusCode::NOT_FOUND, "node not found".into()))
}

async fn call(
    state: &AppState,
    uuid: &str,
    op: &str,
    args: Value,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    state
        .remote
        .call_file(state.board.as_ref(), &state.command_bus, uuid, op, args)
        .await
        .map(Json)
        .map_err(map_remote_err)
}

fn map_remote_err(err: RemoteError) -> (axum::http::StatusCode, String) {
    match err {
        RemoteError::Offline => (axum::http::StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
        RemoteError::Timeout => (axum::http::StatusCode::GATEWAY_TIMEOUT, err.to_string()),
        RemoteError::Agent(msg) => (axum::http::StatusCode::BAD_REQUEST, msg),
        RemoteError::Internal => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "internal".into(),
        ),
    }
}
