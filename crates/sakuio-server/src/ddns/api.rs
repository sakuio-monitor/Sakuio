//! Admin APIs and background synchronization for authorization providers and DDNS.
//! 授权 API 与 DDNS 的管理接口及后台同步。

use super::{all_drivers, driver, DriverField};
use crate::{auth::AdminUser, AppState};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use sakuio_db::DbPool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize)]
struct DriverView {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    fields: Vec<DriverField>,
}

#[derive(Serialize)]
struct ProviderView {
    id: i64,
    name: String,
    driver: String,
    driver_name: String,
    description: String,
    auth_type: String,
    configured_fields: Vec<String>,
    last_validated_at: String,
    last_used_at: String,
    created_at: String,
    updated_at: String,
}

#[derive(Serialize)]
struct RecordView {
    id: i64,
    name: String,
    provider_id: i64,
    provider_name: String,
    driver: String,
    client_uuid: String,
    client_name: String,
    zone_name: String,
    record_name: String,
    record_type: String,
    proxied: bool,
    ttl: i32,
    enabled: bool,
    last_ip: String,
    last_error: String,
    last_synced_at: Option<String>,
}

#[derive(Deserialize)]
struct ProviderBody {
    name: String,
    driver: String,
    #[serde(default)]
    config: Value,
}

#[derive(Deserialize)]
struct ProviderUpdate {
    name: Option<String>,
    #[serde(default)]
    config: Option<Value>,
}

#[derive(Deserialize)]
struct RecordBody {
    #[serde(default)]
    name: String,
    provider_id: i64,
    client_uuid: String,
    #[serde(default)]
    fqdn: Option<String>,
    #[serde(default)]
    zone_name: String,
    #[serde(default)]
    record_name: String,
    record_type: String,
    #[serde(default)]
    proxied: bool,
    #[serde(default = "default_ttl")]
    ttl: i32,
}

fn default_ttl() -> i32 {
    1
}

fn resolve_names(body: &RecordBody) -> Result<(String, String), String> {
    if let Some(fqdn) = body
        .fqdn
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        super::zone::split_fqdn(fqdn)
    } else if !body.zone_name.trim().is_empty() {
        Ok((
            body.zone_name.trim().to_ascii_lowercase(),
            body.record_name.trim().to_owned(),
        ))
    } else {
        Err("请输入域名".into())
    }
}

async fn insert_record_row(
    pool: &DbPool,
    body: &RecordBody,
    zone_name: &str,
    record_name: &str,
    record_type: &str,
) -> Result<i64, String> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO ddns_records (name, provider_id, client_uuid, zone_name, record_name, record_type, proxied, ttl, enabled, zone_id, remote_record_id, last_ip, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', '', '', ?, ?)",
    )
    .bind(body.name.trim())
    .bind(body.provider_id)
    .bind(&body.client_uuid)
    .bind(zone_name)
    .bind(record_name)
    .bind(record_type)
    .bind(if body.proxied { 1 } else { 0 })
    .bind(body.ttl.clamp(1, 86400))
    .bind(1)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .map_err(|error| error.to_string())?;
    let id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM ddns_records WHERE provider_id = ? AND client_uuid = ? AND zone_name = ? AND record_name = ? AND record_type = ? AND created_at = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(body.provider_id)
    .bind(&body.client_uuid)
    .bind(zone_name)
    .bind(record_name)
    .bind(record_type)
    .bind(&now)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    id.filter(|id| *id > 0)
        .ok_or_else(|| "创建 DDNS 记录失败：未返回记录 ID".to_string())
}

fn fail(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "message": message.into() }))).into_response()
}

async fn providers_list(
    State(state): State<AppState>,
    _user: AdminUser,
) -> Result<Json<Value>, Response> {
    let rows: Vec<(i64, String, String, String, String, String, String, String)> = sqlx::query_as(
        "SELECT id, name, driver, config_json, last_validated_at, last_used_at, created_at, updated_at FROM auth_providers ORDER BY id DESC",
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let items: Vec<ProviderView> = rows
        .into_iter()
        .map(
            |(id, name, driver_id, config, validated, last_used, created, updated)| {
                let parsed_config = serde_json::from_str::<Value>(&config).unwrap_or(Value::Null);
                let configured_fields = parsed_config
                    .as_object()
                    .map(|map| map.keys().cloned().collect())
                    .unwrap_or_default();
                let auth_type = parsed_config
                    .get("auth_type")
                    .and_then(Value::as_str)
                    .unwrap_or("token")
                    .to_string();
                let driver_info = driver(&driver_id);
                ProviderView {
                    id,
                    name,
                    driver: driver_id,
                    driver_name: driver_info
                        .as_ref()
                        .map(|driver| driver.name().to_string())
                        .unwrap_or_else(|| "未知驱动".into()),
                    description: driver_info
                        .as_ref()
                        .map(|driver| driver.description().to_string())
                        .unwrap_or_default(),
                    auth_type,
                    configured_fields,
                    last_validated_at: validated,
                    last_used_at: last_used,
                    created_at: created,
                    updated_at: updated,
                }
            },
        )
        .collect();
    Ok(Json(json!({ "items": items })))
}

async fn drivers_list(State(_state): State<AppState>, _user: AdminUser) -> Json<Value> {
    let drivers: Vec<DriverView> = all_drivers()
        .into_iter()
        .map(|driver| DriverView {
            id: driver.id(),
            name: driver.name(),
            description: driver.description(),
            fields: driver.fields(),
        })
        .collect();
    Json(json!({ "drivers": drivers }))
}

async fn provider_create(
    State(state): State<AppState>,
    _user: AdminUser,
    Json(body): Json<ProviderBody>,
) -> Response {
    let name = body.name.trim();
    if name.is_empty() {
        return fail(StatusCode::BAD_REQUEST, "授权名称不能为空");
    }
    let Some(driver) = driver(body.driver.trim()) else {
        return fail(StatusCode::BAD_REQUEST, "不支持的授权驱动");
    };
    if body.config.is_null() || !body.config.is_object() {
        return fail(StatusCode::BAD_REQUEST, "授权配置不能为空");
    }
    if let Err(error) = driver.validate(&body.config).await {
        return fail(StatusCode::BAD_REQUEST, error);
    }
    let now = Utc::now().to_rfc3339();
    let config = body.config.to_string();
    let result = sqlx::query(
        "INSERT INTO auth_providers (name, driver, config_json, last_validated_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(name)
    .bind(driver.id())
    .bind(config)
    .bind(&now)
    .bind(&now)
    .bind(&now)
    .execute(&state.db)
    .await;
    match result {
        Ok(_) => Json(json!({ "success": true })).into_response(),
        Err(error) => fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn provider_get_row(pool: &DbPool, id: i64) -> Result<Option<(String, String)>, sqlx::Error> {
    sqlx::query_as("SELECT driver, config_json FROM auth_providers WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

async fn provider_update(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<i64>,
    Json(body): Json<ProviderUpdate>,
) -> Response {
    let Some((driver_id, old_config)) = provider_get_row(&state.db, id).await.ok().flatten() else {
        return fail(StatusCode::NOT_FOUND, "授权不存在");
    };
    let Some(driver) = driver(&driver_id) else {
        return fail(StatusCode::BAD_REQUEST, "授权驱动已不可用");
    };
    let config = match body.config {
        Some(config) => {
            if !config.is_object() {
                return fail(StatusCode::BAD_REQUEST, "授权配置格式错误");
            }
            if let Err(error) = driver.validate(&config).await {
                return fail(StatusCode::BAD_REQUEST, error);
            }
            config.to_string()
        }
        None => old_config,
    };
    let now = Utc::now().to_rfc3339();
    let mut sql = String::from(
        "UPDATE auth_providers SET config_json = ?, last_validated_at = ?, updated_at = ?",
    );
    if body
        .name
        .as_deref()
        .is_some_and(|name| !name.trim().is_empty())
    {
        sql.push_str(", name = ?");
    }
    sql.push_str(" WHERE id = ?");
    let mut query = sqlx::query(&sql).bind(config).bind(&now).bind(&now);
    if let Some(name) = body.name.filter(|name| !name.trim().is_empty()) {
        let name = name.trim().to_string();
        query = query.bind(name);
    }
    match query.bind(id).execute(&state.db).await {
        Ok(_) => Json(json!({ "success": true })).into_response(),
        Err(error) => fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn provider_delete(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<i64>,
) -> Response {
    let used: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ddns_records WHERE provider_id = ?")
        .bind(id)
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    if used > 0 {
        return fail(StatusCode::CONFLICT, "该授权仍被 DDNS 记录使用，不能删除");
    }
    match sqlx::query("DELETE FROM auth_providers WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(_) => Json(json!({ "success": true })).into_response(),
        Err(error) => fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

#[derive(Deserialize)]
struct ClientQuery {
    search: Option<String>,
    page: Option<u32>,
    limit: Option<u32>,
}

async fn clients_list(
    State(state): State<AppState>,
    _user: AdminUser,
    Query(query): Query<ClientQuery>,
) -> Result<Json<Value>, Response> {
    let search = query.search.unwrap_or_default().trim().to_string();
    let limit = query.limit.unwrap_or(20).clamp(1, 50);
    let page = query.page.unwrap_or(1).max(1);
    let offset = (page - 1).saturating_mul(limit);
    let (rows, total): (Vec<(String, String, String, String)>, i64) = if search.is_empty() {
        let rows = sqlx::query_as(
            "SELECT uuid, name, ipv4, ipv6 FROM clients WHERE disabled = 0 ORDER BY name, uuid LIMIT ? OFFSET ?",
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&state.db)
        .await
        .map_err(|error| fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        let total = sqlx::query_scalar("SELECT COUNT(*) FROM clients WHERE disabled = 0")
            .fetch_one(&state.db)
            .await
            .unwrap_or(0);
        (rows, total)
    } else {
        let pattern = format!("%{search}%");
        let rows = sqlx::query_as(
            "SELECT uuid, name, ipv4, ipv6 FROM clients WHERE disabled = 0 AND (name LIKE ? OR uuid LIKE ?) ORDER BY name, uuid LIMIT ? OFFSET ?",
        )
        .bind(&pattern)
        .bind(&pattern)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&state.db)
        .await
        .map_err(|error| fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        let total = sqlx::query_scalar(
            "SELECT COUNT(*) FROM clients WHERE disabled = 0 AND (name LIKE ? OR uuid LIKE ?)",
        )
        .bind(&pattern)
        .bind(&pattern)
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
        (rows, total)
    };
    let items: Vec<Value> = rows
        .into_iter()
        .map(|(uuid, name, ipv4, ipv6)| {
            json!({
                "uuid": uuid, "name": name, "ipv4": ipv4, "ipv6": ipv6
            })
        })
        .collect();
    Ok(Json(json!({
        "items": items, "total": total, "page": page, "limit": limit
    })))
}

async fn records_list(
    State(state): State<AppState>,
    _user: AdminUser,
) -> Result<Json<Value>, Response> {
    let rows = sqlx::query(
        "SELECT d.id, d.name, d.provider_id, p.name AS provider_name, p.driver, d.client_uuid, c.name AS client_name, d.zone_name, d.record_name, d.record_type, d.proxied, d.ttl, d.enabled, d.last_ip, d.last_error, d.last_synced_at FROM ddns_records d LEFT JOIN auth_providers p ON p.id = d.provider_id LEFT JOIN clients c ON c.uuid = d.client_uuid ORDER BY d.id DESC",
    )
    .fetch_all(&state.db)
    .await
    .map_err(|error| fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let items: Vec<RecordView> = rows
        .into_iter()
        .map(|row| RecordView {
            id: row.try_get("id").unwrap_or(0),
            name: row.try_get("name").unwrap_or_default(),
            provider_id: row.try_get("provider_id").unwrap_or(0),
            provider_name: row.try_get("provider_name").unwrap_or_default(),
            driver: row.try_get("driver").unwrap_or_default(),
            client_uuid: row.try_get("client_uuid").unwrap_or_default(),
            client_name: row.try_get("client_name").unwrap_or_default(),
            zone_name: row.try_get("zone_name").unwrap_or_default(),
            record_name: row.try_get("record_name").unwrap_or_default(),
            record_type: row.try_get("record_type").unwrap_or_default(),
            proxied: row.try_get::<i32, _>("proxied").unwrap_or(0) != 0,
            ttl: row.try_get("ttl").unwrap_or(1),
            enabled: row.try_get::<i32, _>("enabled").unwrap_or(0) != 0,
            last_ip: row.try_get("last_ip").unwrap_or_default(),
            last_error: row.try_get("last_error").unwrap_or_default(),
            last_synced_at: row.try_get("last_synced_at").ok().flatten(),
        })
        .collect();
    Ok(Json(json!({ "items": items })))
}

async fn client_address(
    pool: &DbPool,
    uuid: &str,
    record_type: &str,
) -> Result<(String, String), String> {
    let row: Option<(String, String, String)> =
        sqlx::query_as("SELECT name, ipv4, ipv6 FROM clients WHERE uuid = ?")
            .bind(uuid)
            .fetch_optional(pool)
            .await
            .map_err(|error| error.to_string())?;
    let Some((name, ipv4, ipv6)) = row else {
        return Err("服务器不存在".into());
    };
    let ip = if record_type.eq_ignore_ascii_case("AAAA") {
        ipv6.trim().to_string()
    } else {
        ipv4.trim().to_string()
    };
    if ip.is_empty() {
        return Err(format!("服务器 {name} 尚未上报对应地址"));
    }
    Ok((name, ip))
}

async fn sync_record(state: &AppState, id: i64) -> Result<(), String> {
    let row = sqlx::query(
        "SELECT d.*, p.driver, p.config_json FROM ddns_records d LEFT JOIN auth_providers p ON p.id = d.provider_id WHERE d.id = ?",
    )
    .bind(id)
    .fetch_optional(&state.db)
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "DDNS 记录不存在".to_string())?;
    let provider_id: i64 = row.try_get("provider_id").unwrap_or(0);
    let driver_id: String = row.try_get("driver").unwrap_or_default();
    let config: String = row.try_get("config_json").unwrap_or_else(|_| "{}".into());
    let record_type: String = row.try_get("record_type").unwrap_or_else(|_| "A".into());
    let client_uuid: String = row.try_get("client_uuid").unwrap_or_default();
    let (_, ip) = client_address(&state.db, &client_uuid, &record_type).await?;
    let Some(driver) = driver(&driver_id) else {
        return Err("DDNS 驱动不可用".into());
    };
    let config: Value = serde_json::from_str(&config).unwrap_or(Value::Null);
    let _ = sqlx::query("UPDATE auth_providers SET last_used_at = ? WHERE id = ?")
        .bind(Utc::now().to_rfc3339())
        .bind(provider_id)
        .execute(&state.db)
        .await;
    let request = super::DdnsUpdate {
        zone_name: row.try_get("zone_name").unwrap_or_default(),
        record_name: row.try_get("record_name").unwrap_or_default(),
        record_type: record_type.clone(),
        content: ip.clone(),
        proxied: row.try_get::<i32, _>("proxied").unwrap_or(0) != 0,
        ttl: row.try_get("ttl").unwrap_or(1),
        zone_id: row
            .try_get::<String, _>("zone_id")
            .ok()
            .filter(|value| !value.is_empty()),
        record_id: row
            .try_get::<String, _>("remote_record_id")
            .ok()
            .filter(|value| !value.is_empty()),
    };
    match driver.update(&config, &request).await {
        Ok(result) => {
            let now = Utc::now().to_rfc3339();
            sqlx::query(
                "UPDATE ddns_records SET zone_id = ?, remote_record_id = ?, last_ip = ?, last_error = '', last_synced_at = ?, updated_at = ? WHERE id = ?",
            )
            .bind(result.zone_id)
            .bind(result.record_id)
            .bind(&ip)
            .bind(&now)
            .bind(&now)
            .bind(id)
            .execute(&state.db)
            .await
            .map_err(|error| error.to_string())?;
            tracing::info!(record_id = id, provider_id, "DDNS record synchronized");
            Ok(())
        }
        Err(error) => {
            let now = Utc::now().to_rfc3339();
            let _ =
                sqlx::query("UPDATE ddns_records SET last_error = ?, updated_at = ? WHERE id = ?")
                    .bind(&error)
                    .bind(&now)
                    .bind(id)
                    .execute(&state.db)
                    .await;
            Err(error)
        }
    }
}

async fn record_create(
    State(state): State<AppState>,
    _user: AdminUser,
    Json(body): Json<RecordBody>,
) -> Response {
    let provider: Option<(String,)> =
        sqlx::query_as("SELECT driver FROM auth_providers WHERE id = ?")
            .bind(body.provider_id)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();
    let Some((driver_id,)) = provider else {
        return fail(StatusCode::BAD_REQUEST, "授权不存在");
    };
    if driver(&driver_id).is_none() {
        return fail(StatusCode::BAD_REQUEST, "授权驱动不可用");
    }
    let (zone_name, record_name) = match resolve_names(&body) {
        Ok(names) => names,
        Err(error) => return fail(StatusCode::BAD_REQUEST, error),
    };
    let record_type = body.record_type.to_ascii_uppercase();
    let types: Vec<&str> = match record_type.as_str() {
        "A" => vec!["A"],
        "AAAA" => vec!["AAAA"],
        "BOTH" => vec!["A", "AAAA"],
        _ => {
            return fail(
                StatusCode::BAD_REQUEST,
                "记录类型仅支持 A、AAAA 或两者同时解析",
            )
        }
    };
    let mut ids = Vec::new();
    for record_type in types {
        if let Err(error) = client_address(&state.db, &body.client_uuid, record_type).await {
            for id in &ids {
                let _ = sqlx::query("DELETE FROM ddns_records WHERE id = ?")
                    .bind(id)
                    .execute(&state.db)
                    .await;
            }
            return fail(StatusCode::BAD_REQUEST, error);
        }
        let id = match insert_record_row(&state.db, &body, &zone_name, &record_name, record_type)
            .await
        {
            Ok(id) => id,
            Err(error) => {
                for id in &ids {
                    let _ = sqlx::query("DELETE FROM ddns_records WHERE id = ?")
                        .bind(id)
                        .execute(&state.db)
                        .await;
                }
                return fail(StatusCode::INTERNAL_SERVER_ERROR, error);
            }
        };
        ids.push(id);
    }
    let sync_state = state.clone();
    let sync_ids = ids.clone();
    tokio::spawn(async move {
        for id in sync_ids {
            if let Err(error) = sync_record(&sync_state, id).await {
                tracing::warn!(record_id = id, error = %error, "DDNS record background create sync failed");
            }
        }
    });
    Json(json!({ "success": true, "ids": ids })).into_response()
}

async fn record_update(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<i64>,
    Json(body): Json<RecordBody>,
) -> Response {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ddns_records WHERE id = ?")
        .bind(id)
        .fetch_one(&state.db)
        .await
        .unwrap_or(0);
    if exists == 0 {
        return fail(StatusCode::NOT_FOUND, "DDNS 记录不存在");
    }
    let provider: Option<(String,)> =
        sqlx::query_as("SELECT driver FROM auth_providers WHERE id = ?")
            .bind(body.provider_id)
            .fetch_optional(&state.db)
            .await
            .ok()
            .flatten();
    let Some((driver_id,)) = provider else {
        return fail(StatusCode::BAD_REQUEST, "授权不存在");
    };
    if driver(&driver_id).is_none() {
        return fail(StatusCode::BAD_REQUEST, "授权驱动不可用");
    }
    let (zone_name, record_name) = match resolve_names(&body) {
        Ok(names) => names,
        Err(error) => return fail(StatusCode::BAD_REQUEST, error),
    };
    let record_type = body.record_type.to_ascii_uppercase();
    if record_type != "A" && record_type != "AAAA" {
        return fail(
            StatusCode::BAD_REQUEST,
            "编辑单条记录时类型仅支持 A 或 AAAA",
        );
    }
    if let Err(error) = client_address(&state.db, &body.client_uuid, &record_type).await {
        return fail(StatusCode::BAD_REQUEST, error);
    }
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query(
        "UPDATE ddns_records SET name = ?, provider_id = ?, client_uuid = ?, zone_name = ?, record_name = ?, record_type = ?, proxied = ?, ttl = ?, enabled = 1, zone_id = '', remote_record_id = '', last_error = '', last_ip = '', last_synced_at = NULL, updated_at = ? WHERE id = ?",
    )
    .bind(body.name.trim())
    .bind(body.provider_id)
    .bind(&body.client_uuid)
    .bind(&zone_name)
    .bind(&record_name)
    .bind(&record_type)
    .bind(if body.proxied { 1 } else { 0 })
    .bind(body.ttl.clamp(1, 86400))
    .bind(&now)
    .bind(id)
    .execute(&state.db)
    .await;
    if let Err(error) = result {
        return fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
    }
    let sync_state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = sync_record(&sync_state, id).await {
            tracing::warn!(record_id = id, error = %error, "DDNS record background update sync failed");
        }
    });
    Json(json!({ "success": true })).into_response()
}

async fn record_delete(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<i64>,
) -> Response {
    match sqlx::query("DELETE FROM ddns_records WHERE id = ?")
        .bind(id)
        .execute(&state.db)
        .await
    {
        Ok(_) => Json(json!({ "success": true })).into_response(),
        Err(error) => fail(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
    }
}

async fn record_sync(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(id): Path<i64>,
) -> Response {
    match sync_record(&state, id).await {
        Ok(()) => Json(json!({ "success": true })).into_response(),
        Err(error) => fail(StatusCode::BAD_REQUEST, error),
    }
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/admin/auth/drivers", get(drivers_list))
        .route(
            "/api/admin/auth/providers",
            get(providers_list).post(provider_create),
        )
        .route(
            "/api/admin/auth/providers/:id",
            axum::routing::put(provider_update).delete(provider_delete),
        )
        .route("/api/admin/ddns/clients", get(clients_list))
        .route("/api/admin/ddns", get(records_list).post(record_create))
        .route(
            "/api/admin/ddns/:id",
            axum::routing::put(record_update).delete(record_delete),
        )
        .route("/api/admin/ddns/:id/sync", post(record_sync))
}

/// Background watcher: sync only records whose reported address changed.
pub struct DdnsService {
    pool: DbPool,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl DdnsService {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn spawn(self: &Arc<Self>) {
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let _ = sqlx::query("UPDATE ddns_records SET enabled = 1 WHERE enabled = 0")
                .execute(&service.pool)
                .await;
            loop {
                let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
                    "SELECT d.id, d.record_type, d.last_ip, CASE WHEN d.record_type = 'AAAA' THEN c.ipv6 ELSE c.ipv4 END AS current_ip FROM ddns_records d JOIN clients c ON c.uuid = d.client_uuid WHERE d.enabled = 1",
                )
                .fetch_all(&service.pool)
                .await
                .unwrap_or_default();
                for (id, record_type, last_ip, current_ip) in rows {
                    if current_ip.trim().is_empty() || current_ip == last_ip {
                        continue;
                    }
                    let _guard = service.lock.lock().await;
                    let _ = record_type;
                    if let Err(error) = sync_record_pool(&service.pool, id).await {
                        tracing::warn!(record_id = id, error = %error, "DDNS background sync failed");
                    }
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
}

async fn sync_record_pool(pool: &DbPool, id: i64) -> Result<(), String> {
    let row = sqlx::query(
        "SELECT d.*, p.driver, p.config_json FROM ddns_records d LEFT JOIN auth_providers p ON p.id = d.provider_id WHERE d.id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "DDNS 记录不存在".to_string())?;
    let provider_id: i64 = row.try_get("provider_id").unwrap_or(0);
    let driver_id: String = row.try_get("driver").unwrap_or_default();
    let config: String = row.try_get("config_json").unwrap_or_else(|_| "{}".into());
    let record_type: String = row.try_get("record_type").unwrap_or_else(|_| "A".into());
    let client_uuid: String = row.try_get("client_uuid").unwrap_or_default();
    let (_, ip) = client_address(pool, &client_uuid, &record_type).await?;
    let Some(driver) = driver(&driver_id) else {
        return Err("DDNS 驱动不可用".into());
    };
    let config: Value = serde_json::from_str(&config).unwrap_or(Value::Null);
    let _ = sqlx::query("UPDATE auth_providers SET last_used_at = ? WHERE id = ?")
        .bind(Utc::now().to_rfc3339())
        .bind(provider_id)
        .execute(pool)
        .await;
    let request = super::DdnsUpdate {
        zone_name: row.try_get("zone_name").unwrap_or_default(),
        record_name: row.try_get("record_name").unwrap_or_default(),
        record_type: record_type.clone(),
        content: ip.clone(),
        proxied: row.try_get::<i32, _>("proxied").unwrap_or(0) != 0,
        ttl: row.try_get("ttl").unwrap_or(1),
        zone_id: row
            .try_get::<String, _>("zone_id")
            .ok()
            .filter(|value| !value.is_empty()),
        record_id: row
            .try_get::<String, _>("remote_record_id")
            .ok()
            .filter(|value| !value.is_empty()),
    };
    match driver.update(&config, &request).await {
        Ok(result) => {
            let now = Utc::now().to_rfc3339();
            sqlx::query(
                "UPDATE ddns_records SET zone_id = ?, remote_record_id = ?, last_ip = ?, last_error = '', last_synced_at = ?, updated_at = ? WHERE id = ?",
            )
            .bind(result.zone_id)
            .bind(result.record_id)
            .bind(&ip)
            .bind(&now)
            .bind(&now)
            .bind(id)
            .execute(pool)
            .await
            .map_err(|error| error.to_string())?;
            Ok(())
        }
        Err(error) => {
            let now = Utc::now().to_rfc3339();
            let _ =
                sqlx::query("UPDATE ddns_records SET last_error = ?, updated_at = ? WHERE id = ?")
                    .bind(&error)
                    .bind(&now)
                    .bind(id)
                    .execute(pool)
                    .await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dsn() -> (String, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!(
            "sakuio-ddns-{}-{}-{}.db",
            std::process::id(),
            nanos,
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        (format!("sqlite://{}?mode=rwc", path.display()), path)
    }

    async fn database_with_client(
        ipv4: &str,
        ipv6: &str,
    ) -> (sakuio_db::AppDatabase, std::path::PathBuf) {
        let (dsn, path) = temp_dsn();
        let database = sakuio_db::AppDatabase::connect(&dsn, true)
            .await
            .expect("database");
        sqlx::query(
            "INSERT INTO clients (uuid, name, token, online, ipv4, ipv6, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
        )
        .bind("node-test")
        .bind("Test Node")
        .bind("token")
        .bind(ipv4)
        .bind(ipv6)
        .bind("2026-01-01T00:00:00Z")
        .bind("2026-01-01T00:00:00Z")
        .execute(database.pool())
        .await
        .expect("client");
        (database, path)
    }

    #[tokio::test]
    async fn selects_ipv4_for_a_record() {
        let (database, path) = database_with_client("203.0.113.10", "2001:db8::10").await;
        let (_, ip) = client_address(database.pool(), "node-test", "A")
            .await
            .expect("address");
        assert_eq!(ip, "203.0.113.10");
        database.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn selects_ipv6_for_aaaa_record() {
        let (database, path) = database_with_client("203.0.113.10", "2001:db8::10").await;
        let (_, ip) = client_address(database.pool(), "node-test", "AAAA")
            .await
            .expect("address");
        assert_eq!(ip, "2001:db8::10");
        database.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rejects_missing_address_for_selected_family() {
        let (database, path) = database_with_client("203.0.113.10", "").await;
        let error = client_address(database.pool(), "node-test", "AAAA")
            .await
            .expect_err("missing IPv6");
        assert!(error.contains("尚未上报"));
        database.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn rejects_unknown_server() {
        let (database, path) = database_with_client("203.0.113.10", "").await;
        let error = client_address(database.pool(), "missing", "A")
            .await
            .expect_err("missing node");
        assert!(error.contains("服务器不存在"));
        database.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn inserts_record_and_returns_auto_id() {
        let (dsn, path) = temp_dsn();
        let database = sakuio_db::AppDatabase::connect(&dsn, true)
            .await
            .expect("database");
        sqlx::query(
            "INSERT INTO clients (uuid, name, token, online, ipv4, ipv6, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
        )
        .bind("node-insert")
        .bind("Insert Node")
        .bind("token")
        .bind("203.0.113.20")
        .bind("")
        .bind("2026-01-01T00:00:00Z")
        .bind("2026-01-01T00:00:00Z")
        .execute(database.pool())
        .await
        .expect("client");
        sqlx::query(
            "INSERT INTO auth_providers (name, driver, config_json, last_validated_at, created_at, updated_at) VALUES ('p', 'cloudflare', '{}', '', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        )
        .execute(database.pool())
        .await
        .expect("provider");
        let body = RecordBody {
            name: "test".into(),
            provider_id: 1,
            client_uuid: "node-insert".into(),
            fqdn: Some("www.example.com".into()),
            zone_name: String::new(),
            record_name: String::new(),
            record_type: "A".into(),
            proxied: false,
            ttl: 600,
        };
        let id = insert_record_row(database.pool(), &body, "example.com", "www", "A")
            .await
            .expect("insert id");
        assert!(id > 0);
        database.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn normalizes_record_types() {
        assert!("a".to_ascii_uppercase() == "A");
        assert!("aaaa".to_ascii_uppercase() == "AAAA");
    }
}
