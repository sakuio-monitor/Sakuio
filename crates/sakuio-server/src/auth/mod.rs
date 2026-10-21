mod login;
mod middleware;
mod twofa;

pub use login::{login, logout, session_me};
pub use middleware::{admin_auth_layer, AdminUser, RequireAdmin};
pub use twofa::{disable_2fa, enable_2fa, setup_2fa};

use sakuio_db::AdminSessionUser;

pub const SESSION_COOKIE: &str = "sakuio_session";

pub fn user_me_json(user: &AdminSessionUser) -> serde_json::Value {
    serde_json::json!({
        "username": user.username,
        "logged_in": true,
        "2fa_enabled": user.totp_enabled,
    })
}
