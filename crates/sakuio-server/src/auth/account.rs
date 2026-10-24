use axum::{extract::{ConnectInfo, State}, http::{header, HeaderMap, StatusCode}, Json};
use serde::Deserialize;
use serde_json::json;

use crate::auth::RequireAdmin;
use crate::web::respond::{api_error, api_success};
use crate::AppState;
use sakuio_db::AdminAuthRepository;
use std::net::SocketAddr;

#[derive(Deserialize)]
pub struct ChangeUsernameBody {
    pub username: String,
}

#[derive(Deserialize)]
pub struct ChangePasswordBody {
    pub new_password: String,
    #[serde(default)]
    pub repeat_password: String,
}

fn valid_username(username: &str) -> bool {
    !username.is_empty() && username.len() <= 64 && !username.chars().any(char::is_control)
}

async fn audit_session_revoke(
    state: &AppState,
    user: &crate::auth::AdminUser,
    headers: &HeaderMap,
    addr: SocketAddr,
    action: &str,
    reason: &str,
) {
    let ip = crate::audit::client_ip(headers, Some(addr));
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();
    crate::audit::record_event(
        state, &user.username, &user.id, action, &user.username, reason, &ip, &ua,
        200, "POST", "/api/admin/account",
    )
    .await;
}

pub async fn change_username(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    jar: axum_extra::extract::CookieJar,
    Json(body): Json<ChangeUsernameBody>,
) -> impl axum::response::IntoResponse {
    let user = match RequireAdmin::require(&state, &jar).await {
        Ok(user) => user,
        Err(status) => return api_error(status, "Unauthorized"),
    };
    let username = body.username.trim().to_string();
    if !valid_username(&username) {
        return api_error(StatusCode::BAD_REQUEST, "Invalid username");
    }
    let repo = AdminAuthRepository::new(&state.db);
    match repo.username_exists(&username, &user.id).await {
        Ok(true) => return api_error(StatusCode::CONFLICT, "Username already exists"),
        Ok(false) => {}
        Err(_) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Database error"),
    }
    if repo.update_username(&user.id, &username).await.is_err() {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Database error");
    }
    let _ = repo.delete_sessions_for_user(&user.id).await;
    audit_session_revoke(&state, &user, &headers, addr, "SESSION_REVOKE_ALL", "username change").await;
    api_success(json!({
        "username": username,
        "2fa_enabled": user.totp_enabled,
    }))
}

pub async fn change_password(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    jar: axum_extra::extract::CookieJar,
    Json(body): Json<ChangePasswordBody>,
) -> impl axum::response::IntoResponse {
    let user = match RequireAdmin::require(&state, &jar).await {
        Ok(user) => user,
        Err(status) => return api_error(status, "Unauthorized"),
    };
    if body.new_password.len() < 8 {
        return api_error(
            StatusCode::BAD_REQUEST,
            "Password must be at least 8 characters",
        );
    }
    if !body.repeat_password.is_empty() && body.new_password != body.repeat_password {
        return api_error(StatusCode::BAD_REQUEST, "Passwords do not match");
    }
    let repo = AdminAuthRepository::new(&state.db);
    let hash = match AdminAuthRepository::hash_password(&body.new_password) {
        Ok(hash) => hash,
        Err(_) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Password hash failed"),
    };
    if repo.update_password_hash(&user.id, &hash).await.is_err() {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "Database error");
    }
    let _ = repo.delete_sessions_for_user(&user.id).await;
    audit_session_revoke(&state, &user, &headers, addr, "SESSION_REVOKE_ALL", "password change").await;
    api_success(json!({ "updated": true, "reauthentication_required": true }))
}
