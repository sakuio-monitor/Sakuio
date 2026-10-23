mod account;
mod login;
mod middleware;
mod twofa;

pub use account::{change_password, change_username};
pub use login::{login, logout, session_me};
pub use middleware::{admin_auth_layer, AdminUser, RequireAdmin};
pub use twofa::{disable_2fa, enable_2fa, setup_2fa};


pub const SESSION_COOKIE: &str = "sakuio_session";
/// Komari-theme / Komari compatibility cookie.
pub const SESSION_COOKIE_COMPAT: &str = "session_token";

pub fn session_token(jar: &axum_extra::extract::CookieJar) -> Option<&str> {
    jar.get(SESSION_COOKIE)
        .or_else(|| jar.get(SESSION_COOKIE_COMPAT))
        .map(|cookie| cookie.value())
}

/// Komari expects a flat `/api/me` body, while the Sakuio admin client reads
/// the same fields from `data`. Return both shapes in one response.
pub fn me_response(data: serde_json::Value, token: Option<&str>) -> serde_json::Value {
    let mut flat = data.clone();
    if let Some(object) = flat.as_object_mut() {
        object.insert("status".into(), serde_json::json!("success"));
        object.insert("message".into(), serde_json::json!(""));
        object.insert("data".into(), data);
        if let Some(token) = token {
            object.insert(
                "set-cookie".into(),
                serde_json::json!({ SESSION_COOKIE_COMPAT: token }),
            );
        }
    }
    flat
}


