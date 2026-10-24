//! Semantic action names for audit records (never HTTP methods).
//! 审计记录的系统语义动作名（不使用 HTTP 方法作为日志类型）。

use axum::http::Method;

pub const ADMIN_OPERATION: &str = "ADMIN_OPERATION";

pub fn classify(method: &Method, path: &str) -> &'static str {
    let path = path.trim_end_matches('/');
    if path.contains("/nodes/") && path.ends_with("/shell") {
        return "WEB_TERMINAL_OPEN";
    }
    if path.contains("/fs/") {
        return classify_file(method, path);
    }
    if path.starts_with("/api/admin/account") {
        return if path.ends_with("/username") {
            "ACCOUNT_USERNAME_CHANGE"
        } else if path.ends_with("/password") {
            "ACCOUNT_PASSWORD_CHANGE"
        } else {
            "ACCOUNT_UPDATE"
        };
    }
    if path.starts_with("/api/admin/logs") {
        return if path.ends_with("/ip-geo") {
            "AUDIT_LOG_IP_GEO"
        } else if path.ends_with("/export") {
            "AUDIT_LOG_EXPORT"
        } else {
            "AUDIT_LOG_VIEW"
        };
    }
    if path.starts_with("/api/admin/pprof/") {
        if path.ends_with("/profile") {
            return "PPROF_PROFILE_VIEW";
        }
        if path.ends_with("/stream") || path.ends_with("/summary") {
            return "PPROF_STREAM_VIEW";
        }
        return "PPROF_VIEW";
    }
    if path.starts_with("/api/admin/ddns/") {
        let providers = path.contains("/auth-providers") || path.contains("/providers");
        let sync = path.ends_with("/sync");
        if sync {
            return "DDNS_SYNC";
        }
        if providers {
            return match *method {
                Method::POST => "DDNS_PROVIDER_CREATE",
                Method::DELETE => "DDNS_PROVIDER_DELETE",
                Method::PUT | Method::PATCH => "DDNS_PROVIDER_UPDATE",
                _ => "DDNS_PROVIDER_VIEW",
            };
        }
        return match *method {
            Method::POST => "DDNS_RECORD_CREATE",
            Method::DELETE => "DDNS_RECORD_DELETE",
            Method::PUT | Method::PATCH => "DDNS_RECORD_UPDATE",
            _ => "DDNS_RECORD_VIEW",
        };
    }
    if path.ends_with("/update-agent") {
        return "AGENT_UPDATE_SEND";
    }
    if path.starts_with("/api/admin/clients") {
        return classify_clients(method, path);
    }
    if path.starts_with("/api/admin/ping") {
        return classify_ping(method, path);
    }
    if path == "/api/admin/task/exec" {
        return "COMMAND_EXEC";
    }
    if path.starts_with("/api/admin/settings/database") {
        return classify_database(method, path);
    }
    if path.starts_with("/api/admin/settings/site") {
        return classify_site(method, path);
    }
    if path == "/api/admin/settings/backup" {
        return "BACKUP_DOWNLOAD";
    }
    if path == "/api/admin/settings/restore" {
        return "BACKUP_RESTORE";
    }
    if path.starts_with("/api/admin/settings/geoip") {
        return if path.ends_with("/update") {
            "GEOIP_DATABASE_UPDATE"
        } else if path.ends_with("/config") {
            "GEOIP_CONFIG_UPDATE"
        } else {
            "GEOIP_TEST"
        };
    }
    if path.starts_with("/api/admin/notification") {
        return classify_notification(method, path);
    }
    if path.starts_with("/api/admin/themes") {
        return classify_theme(method, path);
    }
    if path.starts_with("/api/admin/2fa") {
        return if path.ends_with("/enable") {
            "TWO_FACTOR_ENABLE"
        } else if path.ends_with("/disable") {
            "TWO_FACTOR_DISABLE"
        } else {
            "TWO_FACTOR_SETUP"
        };
    }
    ADMIN_OPERATION
}

fn classify_file(method: &Method, path: &str) -> &'static str {
    if path.ends_with("/fs/raw") {
        return if method == Method::GET {
            "FILE_DOWNLOAD"
        } else {
            "FILE_UPLOAD"
        };
    }
    if path.ends_with("/fs/text") {
        return if method == Method::GET {
            "FILE_READ"
        } else {
            "FILE_EDIT"
        };
    }
    if path.ends_with("/fs/mkdir") {
        return "FILE_MKDIR";
    }
    if path.ends_with("/fs/delete") {
        return "FILE_DELETE";
    }
    if path.ends_with("/fs/move") {
        return "FILE_MOVE";
    }
    if path.ends_with("/fs/rename") {
        return "FILE_RENAME";
    }
    if path.ends_with("/fs/chmod") {
        return "FILE_CHMOD";
    }
    if path.ends_with("/fs/search") {
        return "FILE_SEARCH";
    }
    if method == Method::GET {
        return "FILE_LIST";
    }
    if path.ends_with("/fs/trash/restore") {
        return "FILE_TRASH_RESTORE";
    }
    if path.ends_with("/fs/trash/empty") {
        return "FILE_TRASH_EMPTY";
    }
    if path.ends_with("/fs/trash/delete") {
        return "FILE_TRASH_DELETE";
    }
    "FILE_OPERATION"
}

fn classify_clients(method: &Method, path: &str) -> &'static str {
    if path.ends_with("/clients/sync") {
        return "SERVER_SYNC";
    }
    if path.ends_with("/clients/reorder") {
        return "SERVER_REORDER";
    }
    if path.ends_with("/clients/batch-delete") {
        return "SERVER_BATCH_DELETE";
    }
    if path.ends_with("/clients/batch-group") {
        return "SERVER_GROUP_UPDATE";
    }
    if path.ends_with("/reset-traffic") {
        return "SERVER_TRAFFIC_RESET";
    }
    if path == "/api/admin/clients" {
        return "SERVER_CREATE";
    }
    if method == Method::DELETE {
        return "SERVER_DELETE";
    }
    "SERVER_UPDATE"
}

fn classify_ping(method: &Method, path: &str) -> &'static str {
    if path.ends_with("/ping/add") {
        return "PING_TASK_CREATE";
    }
    if path.ends_with("/ping/delete") {
        return "PING_TASK_DELETE";
    }
    if path.ends_with("/ping/edit") {
        return "PING_TASK_UPDATE";
    }
    if path.ends_with("/ping/order") {
        return "PING_TASK_REORDER";
    }
    let _ = method;
    "PING_TASK_UPDATE"
}

fn classify_database(method: &Method, path: &str) -> &'static str {
    if method == Method::GET && path.ends_with("/database") {
        return "DATABASE_VIEW";
    }
    if path.ends_with("/database/main") {
        return "DATABASE_SWITCH";
    }
    if path.ends_with("/database/migrate") {
        return "DATABASE_MIGRATE";
    }
    if method == Method::PUT {
        return "DATABASE_UPDATE";
    }
    "DATABASE_OPERATION"
}

fn classify_site(method: &Method, path: &str) -> &'static str {
    if path.ends_with("/share-token") {
        return "SHARE_TOKEN_REGENERATE";
    }
    if method == Method::PUT {
        return "SITE_SETTINGS_UPDATE";
    }
    "SITE_SETTINGS_UPDATE"
}

fn classify_notification(method: &Method, path: &str) -> &'static str {
    let instances = path.contains("/notification/instances");
    let rules = path.contains("/notification/rules");
    if instances {
        if path.ends_with("/test") {
            return "NOTIFICATION_CHANNEL_TEST";
        }
        return match *method {
            Method::POST => "NOTIFICATION_CHANNEL_CREATE",
            Method::DELETE => "NOTIFICATION_CHANNEL_DELETE",
            _ => "NOTIFICATION_CHANNEL_UPDATE",
        };
    }
    if rules {
        return match *method {
            Method::POST => "NOTIFICATION_RULE_CREATE",
            Method::DELETE => "NOTIFICATION_RULE_DELETE",
            _ => "NOTIFICATION_RULE_UPDATE",
        };
    }
    "NOTIFICATION_OPERATION"
}

fn classify_theme(method: &Method, path: &str) -> &'static str {
    if path.ends_with("/preview") && method == Method::GET {
        return "THEME_SOURCE_VIEW";
    }
    if path.ends_with("/themes/upload") {
        return "THEME_UPLOAD";
    }
    if path.contains("/themes/market/") && path.ends_with("/install") {
        return "THEME_MARKET_INSTALL";
    }
    if path.ends_with("/config") && method == Method::PUT {
        return "THEME_CONFIG_UPDATE";
    }
    if path.ends_with("/apply") {
        return "THEME_APPLY";
    }
    if path.ends_with("/update") {
        return "THEME_UPDATE";
    }
    if method == Method::DELETE {
        return "THEME_DELETE";
    }
    "THEME_OPERATION"
}

/// Actions belonging to a user-facing category; `None` means no filter.
/// 用户可见分类对应的动作集合；`None` 表示不过滤。
pub fn category_actions(category: &str) -> Option<&'static [&'static str]> {
    const AUTH: &[&str] = &[
        "LOGIN",
        "LOGIN_FAILED",
        "LOGIN_CHALLENGE",
        "LOGOUT",
        "INSTALL",
        "INSTALL_FAILED",
        "TWO_FACTOR_SETUP",
        "TWO_FACTOR_ENABLE",
        "TWO_FACTOR_DISABLE",
        "ACCOUNT_USERNAME_CHANGE",
        "ACCOUNT_PASSWORD_CHANGE",
        "ACCOUNT_UPDATE",
        "LOGIN_RATE_LIMITED",
        "SESSION_REVOKE",
        "SESSION_REVOKE_ALL",
    ];
    const SERVER: &[&str] = &[
        "SERVER_ONLINE",
        "SERVER_OFFLINE",
        "SERVER_CREATE",
        "SERVER_UPDATE",
        "SERVER_DELETE",
        "SERVER_BATCH_DELETE",
        "SERVER_SYNC",
        "SERVER_REORDER",
        "SERVER_GROUP_UPDATE",
        "SERVER_TRAFFIC_RESET",
        "SERVER_TOKEN_VIEW",
        "SERVER_INSTALL_COMMAND_VIEW",
        "AGENT_UPDATE_SEND",
        "AGENT_UPDATE_SUCCESS",
        "AGENT_UPDATE_FAILED",
        "AGENT_LINK_OPEN",
        "AGENT_AUTH_FAILED",
        "AGENT_AUTODISCOVERY_REGISTER",
        "AGENT_AUTODISCOVERY_REJECTED",
        "AGENT_VERSION_REJECTED",
        "AGENT_RATE_LIMITED",
    ];
    const TERMINAL: &[&str] = &[
        "WEB_TERMINAL_OPEN",
        "WEB_TERMINAL_CLOSE",
        "COMMAND_EXEC",
        "FILE_UPLOAD",
        "FILE_DOWNLOAD",
        "FILE_READ",
        "FILE_EDIT",
        "FILE_DELETE",
        "FILE_MKDIR",
        "FILE_MOVE",
        "FILE_RENAME",
        "FILE_CHMOD",
        "FILE_SEARCH",
        "FILE_LIST",
        "FILE_TRASH_RESTORE",
        "FILE_TRASH_EMPTY",
        "FILE_TRASH_DELETE",
        "FILE_OPERATION",
    ];
    const THEME: &[&str] = &[
        "THEME_UPLOAD",
        "THEME_INSTALL",
        "THEME_UPDATE",
        "THEME_APPLY",
        "THEME_DELETE",
        "THEME_CONFIG_UPDATE",
        "THEME_OPERATION",
        "THEME_SOURCE_VIEW",
        "THEME_MARKET_INSTALL",
    ];
    const SETTINGS: &[&str] = &[
        "SITE_SETTINGS_UPDATE",
        "SHARE_TOKEN_REGENERATE",
        "GEOIP_UPDATE",
        "GEOIP_TEST",
        "GEOIP_CONFIG_UPDATE",
        "GEOIP_DATABASE_UPDATE",
        "BACKUP_DOWNLOAD",
        "BACKUP_RESTORE",
        "ADMIN_OPERATION",
        "AUDIT_LOG_VIEW",
        "AUDIT_LOG_EXPORT",
        "AUDIT_LOG_IP_GEO",
        "PPROF_PROFILE_VIEW",
        "PPROF_STREAM_VIEW",
        "PPROF_VIEW",
        "DDNS_PROVIDER_CREATE",
        "DDNS_PROVIDER_UPDATE",
        "DDNS_PROVIDER_DELETE",
        "DDNS_PROVIDER_VIEW",
        "DDNS_RECORD_CREATE",
        "DDNS_RECORD_UPDATE",
        "DDNS_RECORD_DELETE",
        "DDNS_RECORD_VIEW",
        "DDNS_SYNC",
        "GEOIP_CONFIG_UPDATE",
        "GEOIP_DATABASE_UPDATE",
    ];
    const DATABASE: &[&str] = &[
        "DATABASE_SWITCH",
        "DATABASE_MIGRATE",
        "DATABASE_UPDATE",
        "DATABASE_OPERATION",
        "DATABASE_VIEW",
    ];
    const NOTIFICATION: &[&str] = &[
        "NOTIFICATION_CHANNEL_CREATE",
        "NOTIFICATION_CHANNEL_UPDATE",
        "NOTIFICATION_CHANNEL_DELETE",
        "NOTIFICATION_CHANNEL_TEST",
        "NOTIFICATION_RULE_CREATE",
        "NOTIFICATION_RULE_UPDATE",
        "NOTIFICATION_RULE_DELETE",
        "NOTIFICATION_OPERATION",
    ];
    const PING: &[&str] = &[
        "PING_TASK_CREATE",
        "PING_TASK_UPDATE",
        "PING_TASK_DELETE",
        "PING_TASK_REORDER",
    ];
    match category {
        "auth" => Some(AUTH),
        "server" => Some(SERVER),
        "terminal" => Some(TERMINAL),
        "theme" => Some(THEME),
        "settings" => Some(SETTINGS),
        "database" => Some(DATABASE),
        "notification" => Some(NOTIFICATION),
        "ping" => Some(PING),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actions_are_semantic_not_http_methods() {
        assert_eq!(
            classify(&Method::DELETE, "/api/admin/clients/abc"),
            "SERVER_DELETE"
        );
        assert_eq!(
            classify(&Method::POST, "/api/admin/clients/batch-delete"),
            "SERVER_BATCH_DELETE"
        );
        assert_eq!(
            classify(&Method::GET, "/api/admin/nodes/abc/fs/raw"),
            "FILE_DOWNLOAD"
        );
        assert_eq!(
            classify(&Method::DELETE, "/api/admin/nodes/abc/fs/delete"),
            "FILE_DELETE"
        );
        assert_eq!(
            classify(&Method::GET, "/api/admin/nodes/abc/shell"),
            "WEB_TERMINAL_OPEN"
        );
        assert_eq!(
            classify(&Method::POST, "/api/admin/themes/demo/apply"),
            "THEME_APPLY"
        );
        assert_eq!(
            classify(&Method::PUT, "/api/admin/unknown"),
            ADMIN_OPERATION
        );
        for action in [
            "SERVER_DELETE",
            "WEB_TERMINAL_OPEN",
            "FILE_UPLOAD",
            "THEME_APPLY",
        ] {
            assert!(!matches!(
                action,
                "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
            ));
        }
    }

    #[test]
    fn categories_cover_server_and_terminal_events() {
        assert!(category_actions("server")
            .unwrap()
            .contains(&"SERVER_ONLINE"));
        assert!(category_actions("terminal")
            .unwrap()
            .contains(&"WEB_TERMINAL_CLOSE"));
        assert!(category_actions("unknown").is_none());
    }
}
