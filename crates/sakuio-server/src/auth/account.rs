use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::json;

use crate::auth::RequireAdmin;
use crate::web::respond::{api_error, api_success};
use crate::AppState;
use sakuio_db::AdminAuthRepository;

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

pub async fn change_username(
    State(state): State<AppState>,
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
    api_success(json!({
        "username": username,
        "2fa_enabled": user.totp_enabled,
    }))
}

pub async fn change_password(
    State(state): State<AppState>,
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
    api_success(json!({ "updated": true }))
}
