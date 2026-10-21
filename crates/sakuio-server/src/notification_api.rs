use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::auth::AdminUser;
use crate::AppState;
use sakuio_board::BoardStateStore;
use sakuio_db::{NotificationRepository, NotificationRuleRow};
use sakuio_notify::{NotificationMessage, Registry};

pub const EVENT_OFFLINE: &str = "offline";
pub const EVENT_LOGIN: &str = "login";
pub const EVENT_EXPIRE: &str = "expire";
pub const EVENT_TRAFFIC: &str = "traffic";
pub const EVENT_TRAFFIC_EXCEEDED: &str = "traffic_exceeded";

#[derive(Serialize)]
pub struct ChannelTypeResponse {
    pub id: String,
    pub name: String,
    pub fields: Vec<sakuio_notify::ConfigField>,
}

#[derive(Serialize)]
pub struct InstanceSummary {
    pub id: String,
    pub channel_type: String,
    pub type_name: String,
    pub name: String,
    pub updated_at: String,
}

#[derive(Serialize)]
pub struct InstanceDetailResponse {
    pub id: String,
    pub channel_type: String,
    pub type_name: String,
    pub name: String,
    pub fields: Vec<sakuio_notify::ConfigField>,
    pub data: Value,
}

#[derive(Deserialize)]
pub struct CreateInstanceBody {
    pub channel_type: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub data: Value,
}

#[derive(Deserialize)]
pub struct UpdateInstanceBody {
    pub name: String,
    pub data: Value,
}

#[derive(Deserialize)]
pub struct TestInstanceBody {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub to_email: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateRuleBody {
    pub name: String,
    pub event_type: String,
    #[serde(default)]
    pub threshold: i64,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub clients: Vec<String>,
    #[serde(default)]
    pub template_title: String,
    #[serde(default)]
    pub template_body: String,
}

#[derive(Deserialize)]
pub struct UpdateRuleBody {
    pub name: String,
    pub event_type: String,
    #[serde(default)]
    pub threshold: i64,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub clients: Vec<String>,
    #[serde(default)]
    pub template_title: String,
    #[serde(default)]
    pub template_body: String,
}

fn valid_event(event: &str) -> bool {
    matches!(
        event,
        EVENT_OFFLINE | EVENT_LOGIN | EVENT_EXPIRE | EVENT_TRAFFIC
    )
}

fn rule_from_row(row: NotificationRuleRow, detailed: bool) -> Value {
    let mut out = json!({
        "id": row.id,
        "name": row.name,
        "event_type": row.event_type,
        "threshold": row.threshold,
        "channels": row.channels,
        "clients": row.clients,
        "template_title": row.template_title,
        "template_body": row.template_body,
    });
    if detailed {
        out["updated_at"] = json!(row.updated_at);
    }
    out
}

// ---------------------------------------------------------------- channels

pub async fn list_channel_types(State(state): State<AppState>, _user: AdminUser) -> Json<Value> {
    let types: Vec<ChannelTypeResponse> = state
        .notify
        .list_meta()
        .await
        .into_iter()
        .map(|m| ChannelTypeResponse {
            id: m.id,
            name: m.name,
            fields: m.fields,
        })
        .collect();
    Json(json!({ "types": types }))
}

pub async fn list_instances(
    State(state): State<AppState>,
    _user: AdminUser,
) -> Result<Json<Value>, StatusCode> {
    let repo = NotificationRepository::new(&state.db);
    let rows = repo
        .list_all()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut out = Vec::new();
    for row in rows {
        let type_name = type_name_for(&state.notify, &row.channel_type).await;
        out.push(InstanceSummary {
            id: row.id,
            channel_type: row.channel_type,
            type_name,
            name: row.name,
            updated_at: row.updated_at,
        });
    }
    Ok(Json(json!({ "instances": out })))
}

pub async fn get_instance(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
) -> Result<Json<InstanceDetailResponse>, StatusCode> {
    let repo = NotificationRepository::new(&state.db);
    let row = repo
        .get_by_id(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let channel = state
        .notify
        .get(&row.channel_type)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let meta = channel.meta();
    Ok(Json(InstanceDetailResponse {
        id: row.id,
        channel_type: row.channel_type,
        type_name: meta.name,
        name: row.name,
        fields: meta.fields,
        data: row.config,
    }))
}

pub async fn create_instance(
    State(state): State<AppState>,
    _user: AdminUser,
    Json(body): Json<CreateInstanceBody>,
) -> Result<Json<Value>, StatusCode> {
    let channel_type = body.channel_type.trim();
    if channel_type.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let channel = state
        .notify
        .get(channel_type)
        .await
        .ok_or(StatusCode::BAD_REQUEST)?;
    let meta = channel.meta();
    let name = body
        .name
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .unwrap_or_else(|| meta.name.clone());
    let id = Uuid::new_v4().to_string();
    let data = if body.data.is_null() {
        json!({})
    } else {
        body.data
    };
    NotificationRepository::new(&state.db)
        .insert(&id, channel_type, &name, true, &data)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "id": id })))
}

pub async fn update_instance(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
    Json(body): Json<UpdateInstanceBody>,
) -> Result<StatusCode, StatusCode> {
    let name = body.name.trim();
    if name.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let repo = NotificationRepository::new(&state.db);
    let existing = repo
        .get_by_id(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if state.notify.get(&existing.channel_type).await.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let ok = repo
        .update(&id, name, true, &body.data)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if ok {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn delete_instance(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let ok = NotificationRepository::new(&state.db)
        .delete(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if ok {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn test_instance(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
    Json(body): Json<TestInstanceBody>,
) -> Result<Json<Value>, StatusCode> {
    let repo = NotificationRepository::new(&state.db);
    let row = repo
        .get_by_id(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let mut config = row.config;
    if let Some(to) = body.to_email.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some(obj) = config.as_object_mut() {
            obj.insert("to_email".into(), json!(to));
        }
    }
    let msg = NotificationMessage {
        title: body.title.unwrap_or_else(|| "Sakuio 测试通知".into()),
        body: body
            .message
            .unwrap_or_else(|| "这是一条来自 Sakuio 管理后台的测试消息。".into()),
    };
    state
        .notify
        .send(&row.channel_type, &config, &msg)
        .await
        .map_err(|e| {
            tracing::warn!(
                instance = %id,
                channel_type = %row.channel_type,
                error = %e,
                "notification test failed"
            );
            StatusCode::BAD_GATEWAY
        })?;
    Ok(Json(json!({ "success": true })))
}

// ---------------------------------------------------------------- rules

pub async fn list_rules(
    State(state): State<AppState>,
    _user: AdminUser,
) -> Result<Json<Value>, StatusCode> {
    let repo = NotificationRepository::new(&state.db);
    let rows = repo
        .list_rules()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let out: Vec<Value> = rows.into_iter().map(|r| rule_from_row(r, false)).collect();
    Ok(Json(json!({ "rules": out })))
}

pub async fn get_rule(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let repo = NotificationRepository::new(&state.db);
    let row = repo
        .get_rule(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(rule_from_row(row, true)))
}

pub async fn create_rule(
    State(state): State<AppState>,
    _user: AdminUser,
    Json(body): Json<CreateRuleBody>,
) -> Result<Json<Value>, StatusCode> {
    let name = body.name.trim();
    if name.is_empty() || !valid_event(body.event_type.trim()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let id = Uuid::new_v4().to_string();
    NotificationRepository::new(&state.db)
        .insert_rule(
            &id,
            name,
            body.event_type.trim(),
            body.threshold.max(0),
            &body.channels,
            &body.clients,
            body.template_title.trim(),
            body.template_body.trim(),
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({ "id": id })))
}

pub async fn update_rule(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
    Json(body): Json<UpdateRuleBody>,
) -> Result<StatusCode, StatusCode> {
    let name = body.name.trim();
    if name.is_empty() || !valid_event(body.event_type.trim()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let ok = NotificationRepository::new(&state.db)
        .update_rule(
            &id,
            name,
            body.event_type.trim(),
            body.threshold.max(0),
            &body.channels,
            &body.clients,
            body.template_title.trim(),
            body.template_body.trim(),
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if ok {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn delete_rule(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let ok = NotificationRepository::new(&state.db)
        .delete_rule(&id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if ok {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

pub async fn init_notify_registry() -> Registry {
    let registry = Registry::new();
    sakuio_notify::register_builtin(&registry).await;
    registry
}

async fn type_name_for(notify: &Registry, channel_type: &str) -> String {
    notify
        .get(channel_type)
        .await
        .map(|c| c.meta().name)
        .unwrap_or_else(|| channel_type.to_string())
}

// ---------------------------------------------------------------- engine

fn render_template(template: &str, vars: &HashMap<String, String>) -> String {
    let mut out = template.to_string();
    for (key, value) in vars {
        let token = format!("{{{{{}}}}}", key);
        out = out.replace(&token, value);
    }
    out
}

fn rule_matches_client(rule: &NotificationRuleRow, node_uuid: &str) -> bool {
    rule.clients.is_empty() || rule.clients.iter().any(|c| c == node_uuid)
}

fn default_title(event: &str) -> String {
    match event {
        EVENT_OFFLINE => "{{node_name}} 离线告警".into(),
        EVENT_LOGIN => "管理员登录".into(),
        EVENT_EXPIRE => "{{node_name}} 即将到期".into(),
        EVENT_TRAFFIC => "{{node_name}} 流量告警".into(),
        EVENT_TRAFFIC_EXCEEDED => "{{node_name}} 流量超限".into(),
        _ => "Sakuio 通知".into(),
    }
}

fn default_body(event: &str) -> String {
    match event {
        EVENT_OFFLINE => "{{node_name}} 已离线超过 {{grace_period}} 秒（{{time}}）。".into(),
        EVENT_LOGIN => "用户 {{username}} 于 {{time}} 登录管理后台。".into(),
        EVENT_EXPIRE => {
            "{{node_name}} 将于 {{expire_date}} 到期，剩余 {{days_left}} 天。".into()
        }
        EVENT_TRAFFIC => {
            "{{node_name}} 累计流量已达 {{traffic_used}}，达到 {{traffic_threshold}}（{{time}}）。"
                .into()
        }
        EVENT_TRAFFIC_EXCEEDED => {
            "{{node_name}} 本周期流量已达 {{traffic_used}}，为阈值 {{traffic_threshold}} 的 {{level}}（{{time}}）。"
                .into()
        }
        _ => "{{time}}".into(),
    }
}

/// Fires every rule bound to `event_type` that covers `node_uuid` (empty scope = all).
pub async fn dispatch_event(
    state: &AppState,
    event_type: &str,
    node_uuid: &str,
    node_name: &str,
    extra_vars: &[(&str, String)],
) {
    let repo = NotificationRepository::new(&state.db);
    let rules = match repo.list_rules().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "notification rules load failed");
            return;
        }
    };
    let now = Utc::now().to_rfc3339();
    for rule in rules {
        if rule.event_type != event_type || !rule_matches_client(&rule, node_uuid) {
            continue;
        }
        let mut vars: HashMap<String, String> = extra_vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        vars.insert("node_name".into(), node_name.to_string());
        vars.insert("node_uuid".into(), node_uuid.to_string());
        vars.insert("time".into(), now.clone());
        let title_raw = if rule.template_title.trim().is_empty() {
            default_title(event_type)
        } else {
            rule.template_title.clone()
        };
        let body_raw = if rule.template_body.trim().is_empty() {
            default_body(event_type)
        } else {
            rule.template_body.clone()
        };
        let msg = NotificationMessage {
            title: render_template(&title_raw, &vars),
            body: render_template(&body_raw, &vars),
        };
        for instance_id in &rule.channels {
            let Some(row) = repo.get_by_id(instance_id).await.ok().flatten() else {
                continue;
            };
            if let Err(e) = state
                .notify
                .send(&row.channel_type, &row.config, &msg)
                .await
            {
                tracing::warn!(
                    rule = %rule.name,
                    instance = %instance_id,
                    error = %e,
                    "notification rule dispatch failed"
                );
            }
        }
    }
}

struct OfflineEpisode {
    offline_since: DateTime<Utc>,
    notified_grace: i64,
}

#[derive(Default)]
struct WatcherState {
    offline: HashMap<String, OfflineEpisode>,
    expire_notified: HashMap<String, String>,
    traffic_notified: HashMap<String, i64>,
}

fn fmt_gb(bytes: u64) -> String {
    format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
}

/// Periodically evaluates offline / expire / traffic rules. Login rules are
/// fired from the login handler. Runs until the process exits.
pub fn spawn_notification_watcher(state: AppState) {
    let state = Arc::new(state);
    let watcher = Arc::new(Mutex::new(WatcherState::default()));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if let Err(e) = tick_watcher(&state, &watcher).await {
                tracing::warn!(error = %e, "notification watcher tick failed");
            }
        }
    });
}

fn parse_expire_date(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc));
    }
    chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc())
}

async fn list_all_admin_clients(
    state: &AppState,
) -> Result<Vec<sakuio_db::AdminClientRecord>, String> {
    let repo = sakuio_db::ClientRepository::new(&state.db);
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = repo
            .list_admin_page(cursor.as_deref(), 200)
            .await
            .map_err(|e| e.to_string())?;
        out.extend(page.items);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(out)
}

async fn tick_watcher(state: &AppState, watcher: &Mutex<WatcherState>) -> Result<(), String> {
    let repo = NotificationRepository::new(&state.db);
    let rules = repo.list_rules().await.map_err(|e| e.to_string())?;
    if rules.is_empty() {
        return Ok(());
    }
    let now = Utc::now();
    let mut guard = watcher.lock().await;

    // Expire rules: fire at most once per day per node.
    let expire_rules: Vec<&NotificationRuleRow> = rules
        .iter()
        .filter(|r| r.event_type == EVENT_EXPIRE)
        .collect();
    if !expire_rules.is_empty() {
        for row in list_all_admin_clients(state).await? {
            let Some(expired_raw) = row.expired_at.clone().filter(|s| !s.trim().is_empty()) else {
                continue;
            };
            let Some(expire_at) = parse_expire_date(&expired_raw) else {
                continue;
            };
            let days_left = (expire_at - now).num_days();
            let day_key = now.format("%Y-%m-%d").to_string();
            for rule in &expire_rules {
                if days_left < 0 || days_left > rule.threshold {
                    continue;
                }
                if !rule_matches_client(rule, &row.uuid) {
                    continue;
                }
                let key = format!("{}:{}", rule.id, row.uuid);
                if guard.expire_notified.get(&key) == Some(&day_key) {
                    continue;
                }
                guard.expire_notified.insert(key, day_key.clone());
                dispatch_event(
                    state,
                    EVENT_EXPIRE,
                    &row.uuid,
                    &row.name,
                    &[
                        ("days_left", days_left.to_string()),
                        ("expire_date", expired_raw.clone()),
                    ],
                )
                .await;
            }
        }
    }

    // Traffic rules: cumulative network counters vs threshold (GB).
    let traffic_rules: Vec<&NotificationRuleRow> = rules
        .iter()
        .filter(|r| r.event_type == EVENT_TRAFFIC && r.threshold > 0)
        .collect();
    if !traffic_rules.is_empty() {
        for row in list_all_admin_clients(state).await? {
            let Some(snap) = state
                .board
                .get_latest_snapshot(&row.uuid)
                .await
                .ok()
                .flatten()
            else {
                continue;
            };
            let total = snap
                .net_total_up
                .unwrap_or(0)
                .saturating_add(snap.net_total_down.unwrap_or(0));
            if total == 0 {
                continue;
            }
            for rule in &traffic_rules {
                if !rule_matches_client(rule, &row.uuid) {
                    continue;
                }
                let rule_bytes = (rule.threshold as u64).saturating_mul(1024 * 1024 * 1024);
                if total < rule_bytes {
                    continue;
                }
                let key = format!("{}:{}", rule.id, row.uuid);
                if let Some(prev) = guard.traffic_notified.get(&key) {
                    // Re-alert in 50% steps so sustained overuse keeps notifying.
                    if total.saturating_sub(*prev as u64) < rule_bytes / 2 {
                        continue;
                    }
                }
                guard.traffic_notified.insert(key, total as i64);
                dispatch_event(
                    state,
                    EVENT_TRAFFIC,
                    &row.uuid,
                    &row.name,
                    &[
                        ("traffic_used", fmt_gb(total)),
                        ("traffic_threshold", format!("{} GB", rule.threshold)),
                    ],
                )
                .await;
            }
        }
    }

    // Offline rules: notify once per offline episode after the grace period.
    let offline_rules: Vec<&NotificationRuleRow> = rules
        .iter()
        .filter(|r| r.event_type == EVENT_OFFLINE)
        .collect();
    if offline_rules.is_empty() {
        guard.offline.clear();
        return Ok(());
    }
    for row in list_all_admin_clients(state).await? {
        if row.online {
            guard.offline.remove(&row.uuid);
            continue;
        }
        let episode = guard.offline.entry(row.uuid.clone()).or_insert_with(|| {
            OfflineEpisode {
                offline_since: now,
                notified_grace: -1,
            }
        });
        let elapsed = (now - episode.offline_since).num_seconds();
        for rule in &offline_rules {
            if !rule_matches_client(rule, &row.uuid) {
                continue;
            }
            let grace = rule.threshold.max(0);
            if elapsed < grace || episode.notified_grace >= grace {
                continue;
            }
            episode.notified_grace = grace;
            dispatch_event(
                state,
                EVENT_OFFLINE,
                &row.uuid,
                &row.name,
                &[
                    ("grace_period", grace.to_string()),
                    (
                        "last_online",
                        row.last_seen_at.clone().unwrap_or_default(),
                    ),
                ],
            )
            .await;
        }
    }
    Ok(())
}
