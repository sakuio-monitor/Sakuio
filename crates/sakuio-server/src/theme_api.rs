//! Admin theme management API: list, upload, apply, delete, update, market.
//! 管理端主题管理 API：列表、上传、应用、删除、更新与市场。

use crate::AppState;
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use futures_util::{stream, StreamExt};
use serde_json::json;
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "success": false, "message": message.into() })),
    )
        .into_response()
}

/// GET `/api/admin/themes`.
/// 主题列表（含当前激活项与内置默认主题）。
pub async fn list(State(state): State<AppState>, _user: crate::auth::AdminUser) -> Response {
    let themes = state.theme.list().await;
    Json(json!({
        "success": true,
        "active": state.theme.active().await,
        "market_url": state.theme.market_url(),
        "themes": themes,
    }))
    .into_response()
}

/// POST `/api/admin/themes/upload` (raw ZIP body).
/// 上传主题 ZIP（请求体为原始字节）。
pub async fn upload(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        return error(StatusCode::BAD_REQUEST, "empty theme archive");
    }
    match state.theme.install_upload(body.to_vec()).await {
        Ok(theme) => Json(json!({ "success": true, "theme": theme })).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "theme upload rejected");
            error(StatusCode::BAD_REQUEST, e)
        }
    }
}

/// GET `/api/admin/themes/:short/config` — declared managed configuration.
/// 读取主题声明的托管配置与当前值。
pub async fn get_config(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
) -> Response {
    match state.theme.config_view(&short).await {
        Ok(configuration) => {
            Json(json!({ "success": true, "configuration": configuration })).into_response()
        }
        Err(message) => error(StatusCode::BAD_REQUEST, message),
    }
}

/// PUT `/api/admin/themes/:short/config` — save managed configuration values.
/// 保存主题托管配置值。
pub async fn save_config(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let values = body.get("values").cloned().unwrap_or_else(|| body.clone());
    match state.theme.save_config(&short, values).await {
        Ok(configuration) => {
            Json(json!({ "success": true, "configuration": configuration })).into_response()
        }
        Err(message) => {
            tracing::warn!(theme = %short, error = %message, "theme configuration save failed");
            error(StatusCode::BAD_REQUEST, message)
        }
    }
}

/// GET `/api/admin/themes/:short/preview` — installed theme preview image.
/// 已安装主题的预览图。
pub async fn preview(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
) -> Response {
    match state.theme.read_preview(&short) {
        Some(file) => (
            StatusCode::OK,
            [
                (axum::http::header::CONTENT_TYPE, file.mime),
                (
                    axum::http::header::CACHE_CONTROL,
                    "public, max-age=3600".to_string(),
                ),
            ],
            file.bytes,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// POST `/api/admin/themes/:short/apply`.
/// 应用主题。
pub async fn apply(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
) -> Response {
    match state.theme.apply(&short).await {
        Ok(()) => {
            Json(json!({ "success": true, "active": state.theme.active().await })).into_response()
        }
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

/// DELETE `/api/admin/themes/:short`.
/// 删除主题。
pub async fn delete(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
) -> Response {
    match state.theme.delete(&short).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => error(StatusCode::BAD_REQUEST, e),
    }
}

/// POST `/api/admin/themes/:short/update?refresh=1`.
/// 从市场或原地址更新主题。
pub async fn update(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let force = query.get("refresh").is_some_and(|value| value != "0");
    let proxy = query.get("proxy").cloned();
    let job = state.theme.start_update(short, force, proxy).await;
    (
        StatusCode::ACCEPTED,
        Json(json!({ "success": true, "job_id": job.id() })),
    )
        .into_response()
}

/// GET `/api/admin/themes/jobs/:id` — snapshot fallback for clients without SSE.
/// 任务快照（不支持 SSE 时的回退）。
pub async fn job_status(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(id): Path<String>,
) -> Response {
    match state.theme.jobs().get(&id).await {
        Some(job) => Json(json!({ "success": true, "job": job.snapshot() })).into_response(),
        None => error(StatusCode::NOT_FOUND, "theme job not found"),
    }
}

/// GET `/api/admin/themes/jobs/:id/events` — SSE progress stream.
/// SSE 进度流。
pub async fn job_events(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(id): Path<String>,
) -> Response {
    let Some(job) = state.theme.jobs().get(&id).await else {
        return error(StatusCode::NOT_FOUND, "theme job not found");
    };
    let snapshot = job.snapshot();
    let receiver = job.subscribe();
    let done = snapshot.status != "running";
    let head = stream::once(async move { Ok::<_, Infallible>(to_event(&snapshot)) });
    let tail = stream::unfold((receiver, done), |(mut receiver, mut done)| async move {
        if done {
            return None;
        }
        loop {
            match receiver.recv().await {
                Ok(event) => {
                    done = event.status != "running";
                    return Some((Ok::<_, Infallible>(to_event(&event)), (receiver, done)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(head.chain(tail))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

fn to_event(event: &crate::theme::ThemeJobEvent) -> Event {
    Event::default()
        .event("progress")
        .data(serde_json::to_string(event).unwrap_or_else(|_| "{}".into()))
}

/// GET `/api/admin/themes/market?refresh=1`.
/// 主题市场列表。
pub async fn market(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let force = query.get("refresh").is_some_and(|value| value != "0");
    let installed = state.theme.list().await;
    match state.theme.market_list(force).await {
        Ok(themes) => {
            let views: Vec<_> = themes
                .into_iter()
                .map(|theme| {
                    let installed_theme = installed
                        .iter()
                        .find(|row| row.short == theme.short)
                        .cloned();
                    json!({
                        "short": theme.short,
                        "name": theme.display_name(),
                        "description": theme.display_description(),
                        "version": theme.version,
                        "author": theme.display_author(),
                        "url": theme.url,
                        "preview": theme.preview,
                        "download": theme.download,
                        "sha256": theme.sha256,
                        "installed": installed_theme.is_some(),
                        "installed_version": installed_theme.as_ref().and_then(|row| row.installed_version.clone()),
                        "active": installed_theme.as_ref().is_some_and(|row| row.active),
                    })
                })
                .collect();
            Json(json!({
                "success": true,
                "market_url": state.theme.market_url(),
                "themes": views,
            }))
            .into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, "theme market fetch failed");
            error(StatusCode::BAD_GATEWAY, e)
        }
    }
}

/// POST `/api/admin/themes/market/:short/install?refresh=1`.
/// 从市场安装主题。
pub async fn install_market(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Path(short): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let force = query.get("refresh").is_some_and(|value| value != "0");
    let source = query.get("source").cloned();
    let proxy = query.get("proxy").cloned();
    let job = state.theme.start_market_install(short, source, proxy, force).await;
    (
        StatusCode::ACCEPTED,
        Json(json!({ "success": true, "job_id": job.id() })),
    )
        .into_response()
}
