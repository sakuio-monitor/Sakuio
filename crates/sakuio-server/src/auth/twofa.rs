use axum::{extract::{ConnectInfo, State}, http::{header, HeaderMap, StatusCode}, Json};
use serde::Deserialize;
use serde_json::json;

use crate::auth::RequireAdmin;
use crate::web::respond::{api_error, api_success};
use crate::AppState;
use axum_extra::extract::CookieJar;
use qrcodegen::{QrCode, QrCodeEcc};
use sakuio_db::{generate_totp_secret, totp_provisioning_uri, AdminAuthRepository};
use std::net::SocketAddr;

#[derive(Deserialize)]
pub struct TwoFaCodeBody {
    #[serde(default, rename = "2fa_code")]
    pub two_fa_code: String,
}

#[derive(Deserialize)]
pub struct Disable2faBody {
    #[serde(default)]
    pub password: String,
    #[serde(default, rename = "2fa_code")]
    pub two_fa_code: String,
}

pub async fn setup_2fa(
    State(state): State<AppState>,
    jar: CookieJar,
) -> impl axum::response::IntoResponse {
    let user = match RequireAdmin::require(&state, &jar).await {
        Ok(u) => u,
        Err(s) => return api_error(s, "Unauthorized"),
    };
    if user.totp_enabled {
        return api_error(StatusCode::BAD_REQUEST, "2FA already enabled");
    }
    let secret = generate_totp_secret();
    let repo = AdminAuthRepository::new(&state.db);
    if repo.set_totp_pending(&user.id, &secret).await.is_err() {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "db error");
    }
    let uri = match totp_provisioning_uri(&user.username, &secret) {
        Ok(u) => u,
        Err(_) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, "totp error"),
    };
    api_success(json!({
        "secret": secret,
        "otpauth_url": uri,
        "qr_svg": render_qr_svg(&uri),
    }))
}

fn render_qr_svg(data: &str) -> Option<String> {
    let qr = QrCode::encode_text(data, QrCodeEcc::Medium).ok()?;
    let border = 4;
    let size = qr.size() + border * 2;
    let mut path = String::new();
    for y in 0..qr.size() {
        for x in 0..qr.size() {
            if qr.get_module(x, y) {
                path.push_str(&format!("M{} {}h1v1h-1z", x + border, y + border));
            }
        }
    }
    Some(format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {size} {size}\" shape-rendering=\"crispEdges\"><rect width=\"{size}\" height=\"{size}\" fill=\"#fff\"/><path d=\"{path}\" fill=\"#000\"/></svg>"
    ))
}

pub async fn enable_2fa(
    State(state): State<AppState>,
    jar: CookieJar,
    Json(body): Json<TwoFaCodeBody>,
) -> impl axum::response::IntoResponse {
    let user = match RequireAdmin::require(&state, &jar).await {
        Ok(u) => u,
        Err(s) => return api_error(s, "Unauthorized"),
    };
    let repo = AdminAuthRepository::new(&state.db);
    let secret = match repo.totp_secret(&user.id).await {
        Ok(Some(s)) => s,
        _ => return api_error(StatusCode::BAD_REQUEST, "call setup first"),
    };
    if !AdminAuthRepository::verify_totp(&secret, &body.two_fa_code) {
        return api_error(StatusCode::UNAUTHORIZED, "Invalid 2FA code");
    }
    if repo.enable_totp(&user.id).await.is_err() {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "db error");
    }
    api_success(json!({ "enabled": true }))
}

pub async fn disable_2fa(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    jar: CookieJar,
    Json(body): Json<Disable2faBody>,
) -> impl axum::response::IntoResponse {
    let user = match RequireAdmin::require(&state, &jar).await {
        Ok(u) => u,
        Err(s) => return api_error(s, "Unauthorized"),
    };
    let repo = AdminAuthRepository::new(&state.db);
    let Some((_, hash, _, secret)) = repo.find_by_username(&user.username).await.ok().flatten()
    else {
        return api_error(StatusCode::UNAUTHORIZED, "Unauthorized");
    };
    if !body.password.is_empty() && !AdminAuthRepository::verify_password(&body.password, &hash) {
        return api_error(StatusCode::UNAUTHORIZED, "Invalid password");
    }
    if user.totp_enabled {
        let sec = secret.unwrap_or_default();
        if !AdminAuthRepository::verify_totp(&sec, &body.two_fa_code) {
            return api_error(StatusCode::UNAUTHORIZED, "Invalid 2FA code");
        }
    }
    if repo.disable_totp(&user.id).await.is_err() {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, "db error");
    }
    let _ = repo.delete_sessions_for_user(&user.id).await;
    let ip = crate::audit::client_ip(&headers, Some(addr));
    let ua = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();
    crate::audit::record_event(
        &state, &user.username, &user.id, "SESSION_REVOKE_ALL", &user.username,
        "2FA disabled", &ip, &ua, 200, "POST", "/api/admin/2fa/disable",
    )
    .await;
    api_success(json!({ "enabled": false, "reauthentication_required": true }))
}
