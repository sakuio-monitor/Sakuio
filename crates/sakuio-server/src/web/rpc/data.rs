use crate::AppState;

use super::ping::ping_stats_for_node;
use chrono::{TimeZone, Utc};
use sakuio_board::BoardStateStore;
use sakuio_db::{ClientRepository, PublicClientRow};
use sakuio_protocol::BasicInfoParams;
use serde_json::{json, Value};

pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Public site settings (`public:getPublicSettings` / `common:getPublicInfo`).
pub async fn public_settings(state: &AppState) -> Value {
    const RETENTION_HOURS: i64 = 168;
    let site = crate::site_settings::load(state).await;
    let theme = state.theme.active().await;
    let theme_settings = state.theme.theme_settings(&theme).await;
    json!({
        "sitename": site.site_name,
        "description": site.site_description,
        "custom_head": site.custom_head,
        "custom_body": site.custom_body,
        "favicon": site.favicon,
        "cors_origin_check_enabled": site.cors_origin_check_enabled,
        "ws_origin_check_enabled": site.ws_origin_check_enabled,
        "ssrf_protection": site.ssrf_protection,
        "show_partial_ip_to_guest": site.show_partial_ip,
        "agent_connect_address": site.agent_connect_address,
        "private_site": site.private_site,
        "allow_cors": true,
        "disable_password_login": false,
        "oauth_enable": false,
        "oauth_provider": "",
        "record_enabled": true,
        "record_preserve_time": RETENTION_HOURS,
        "ping_record_preserve_time": RETENTION_HOURS,
        "theme": theme,
        "theme_settings": theme_settings,
    })
}

pub fn version_info() -> Value {
    json!({
        "version": SERVER_VERSION,
        "hash": "sakuio",
    })
}

pub fn guest_me() -> Value {
    json!({
        "username": "Guest",
        "logged_in": false,
    })
}

static PUBLIC_CLIENTS_CACHE: tokio::sync::Mutex<
    Option<(std::time::Instant, Vec<PublicClientRow>)>,
> = tokio::sync::Mutex::const_new(None);
const PUBLIC_CLIENTS_TTL: std::time::Duration = std::time::Duration::from_secs(1);

pub async fn load_public_clients(state: &AppState) -> Result<Vec<PublicClientRow>, i32> {
    // Public SSE/WS pushes run per visitor. A one-second snapshot keeps that
    // fan-out from issuing one full clients-table SELECT per visitor per tick.
    let mut cache = PUBLIC_CLIENTS_CACHE.lock().await;
    if let Some((at, rows)) = cache.as_ref() {
        if at.elapsed() < PUBLIC_CLIENTS_TTL {
            return Ok(rows.clone());
        }
    }
    let rows = ClientRepository::new(&state.db)
        .list_for_public_api()
        .await
        .map_err(|_| -32603)?;
    *cache = Some((std::time::Instant::now(), rows.clone()));
    Ok(rows)
}

pub fn parse_basic(row: &PublicClientRow) -> BasicInfoParams {
    row.basic_info_json
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(BasicInfoParams {
            hostname: None,
            cpu_name: None,
            cpu_cores: None,
            cpu_physical_cores: None,
            arch: None,
            os: None,
            kernel_version: None,
            ipv4: None,
            ipv6: None,
            mem_total: None,
            swap_total: None,
            disk_total: None,
            gpu_name: None,
            virtualization: None,
            version: None,
        })
}

/// Masks an address for guests: `9.*.*.*` for IPv4, `fe80:*:*:*:*:*:*:*` for IPv6.
fn mask_ip(ip: &str) -> String {
    if ip.contains(':') {
        let first = ip.split(':').find(|s| !s.is_empty()).unwrap_or("*");
        format!("{first}:*:*:*:*:*:*:*")
    } else if ip.split('.').count() == 4 {
        format!("{}.*.*.*", ip.split('.').next().unwrap_or("*"))
    } else {
        String::new()
    }
}

pub fn client_record(row: &PublicClientRow, guest_view: bool, show_partial_ip: bool) -> Value {
    let basic = parse_basic(row);
    let mut ipv4 = if row.ipv4.is_empty() {
        basic.ipv4.unwrap_or_default()
    } else {
        row.ipv4.clone()
    };
    let mut ipv6 = if row.ipv6.is_empty() {
        basic.ipv6.unwrap_or_default()
    } else {
        row.ipv6.clone()
    };
    let mut version = row
        .agent_version
        .clone()
        .or(basic.version)
        .unwrap_or_default();
    // 归属地按原始 IP 解析（掩码/隐藏前），旗帜对访客仍然可用
    let region_code = crate::geoip_resolver::region_code_for(if ipv4.is_empty() {
        ipv6.as_str()
    } else {
        ipv4.as_str()
    });
    let mut remark = row.private_remark.clone();
    if guest_view {
        if show_partial_ip {
            ipv4 = mask_ip(&ipv4);
            ipv6 = mask_ip(&ipv6);
        } else {
            ipv4.clear();
            ipv6.clear();
        }
        version.clear();
        remark.clear();
    }
    json!({
        "uuid": row.uuid,
        "name": row.name,
        "cpu_name": basic.cpu_name.unwrap_or_default(),
        "virtualization": basic.virtualization.unwrap_or_default(),
        "arch": basic.arch.unwrap_or_default(),
        "cpu_cores": basic.cpu_cores.unwrap_or(0),
        "os": basic.os.unwrap_or_default(),
        "kernel_version": basic.kernel_version.unwrap_or_default(),
        "gpu_name": basic.gpu_name.unwrap_or_default(),
        "ipv4": ipv4,
        "ipv6": ipv6,
        "region": region_code,
        "remark": remark,
        "public_remark": "",
        "mem_total": basic.mem_total.unwrap_or(0),
        "swap_total": basic.swap_total.unwrap_or(0),
        "disk_total": basic.disk_total.unwrap_or(0),
        "version": version,
        "weight": row.sort_order,
        "price": row.price,
        "billing_cycle": row.billing_cycle,
        "auto_renewal": row.auto_renewal,
        "currency": row.currency,
        "expired_at": row.expired_at.clone().unwrap_or_default(),
        "group": row.group_name,
        "tags": row.tags,
        "hidden": false,
        "traffic_limit": row.traffic_threshold,
        "traffic_limit_type": match row.traffic_stat_mode.as_str() {
            "both" => "sum",
            other => other,
        },
        "traffic_used": sakuio_db::traffic_used_by_mode(
            row.traffic_used_up,
            row.traffic_used_down,
            &row.traffic_stat_mode,
        ),
        "traffic_used_up": row.traffic_used_up,
        "traffic_used_down": row.traffic_used_down,
        "traffic_reset_mode": row.traffic_reset_mode,
        "bandwidth": row.bandwidth,
        "bandwidth_unit": row.bandwidth_unit,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    })
}

pub async fn common_get_nodes_map(
    state: &AppState,
    filter_uuid: Option<&str>,
) -> Result<Value, (i32, &'static str)> {
    let show_partial_ip = crate::site_settings::load(state).await.show_partial_ip;
    let rows = load_public_clients(state)
        .await
        .map_err(|c| (c, "Internal error"))?;
    if let Some(uuid) = filter_uuid {
        let Some(row) = rows.into_iter().find(|r| r.uuid == uuid) else {
            return Err((-32602, "Node not found"));
        };
        return Ok(client_record(&row, true, show_partial_ip));
    }
    let mut map = serde_json::Map::new();
    for row in rows {
        if row.disabled {
            continue;
        }
        map.insert(row.uuid.clone(), client_record(&row, true, show_partial_ip));
    }
    Ok(Value::Object(map))
}

pub async fn public_nodes_list(state: &AppState) -> Result<Value, i32> {
    let show_partial_ip = crate::site_settings::load(state).await.show_partial_ip;
    let rows = load_public_clients(state).await?;
    let list: Vec<Value> = rows
        .iter()
        .filter(|r| !r.disabled)
        .map(|r| client_record(r, true, show_partial_ip))
        .collect();
    Ok(Value::Array(list))
}

pub async fn common_get_nodes_latest_status(
    state: &AppState,
    filter_uuid: Option<&str>,
) -> Result<Value, (i32, &'static str)> {
    let rows = load_public_clients(state)
        .await
        .map_err(|c| (c, "Internal error"))?;
    let mut map = serde_json::Map::new();
    for row in rows {
        if row.disabled {
            continue;
        }
        if let Some(u) = filter_uuid {
            if row.uuid != u {
                continue;
            }
        }
        let snap = state
            .board
            .get_latest_snapshot(&row.uuid)
            .await
            .ok()
            .flatten();
        let ping = ping_stats_for_node(&state.db, &row.uuid)
            .await
            .unwrap_or_else(|_| json!({}));
        let mut status = node_status_json(&row, snap.as_ref());
        if let Some(obj) = status.as_object_mut() {
            obj.insert("ping".into(), ping);
        }
        if filter_uuid.is_some() {
            return Ok(status);
        }
        map.insert(row.uuid.clone(), status);
    }
    if filter_uuid.is_some() && map.is_empty() {
        return Err((-32602, "Node not found"));
    }
    Ok(Value::Object(map))
}

pub fn node_status_json(
    row: &sakuio_db::PublicClientRow,
    snap: Option<&sakuio_board::RuntimeSnapshot>,
) -> Value {
    let Some(snap) = snap else {
        return offline_status(row);
    };
    let uuid = row.uuid.as_str();
    let time = Utc
        .timestamp_millis_opt(snap.observed_at_ms)
        .single()
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    let cpu = snap.cpu_usage.unwrap_or(0.0) as f32;
    let online = if snap.observed_at_ms > 0 {
        snap.online
    } else {
        row.online
    };
    let tcp = snap.connections_tcp.unwrap_or(0);
    let udp = snap.connections_udp.unwrap_or(0);
    json!({
        "client": uuid,
        "time": time,
        "cpu": cpu,
        "gpu": snap.gpu_usage.unwrap_or(0.0),
        "ram": snap.mem_used.unwrap_or(0),
        "ram_total": snap.mem_total.unwrap_or(0),
        "swap": snap.swap_used.unwrap_or(0),
        "swap_total": snap.swap_total.unwrap_or(0),
        "load": snap.load1.unwrap_or(0.0) as f32,
        "load5": snap.load5.unwrap_or(0.0) as f32,
        "load15": snap.load15.unwrap_or(0.0) as f32,
        "temp": 0.0,
        "disk": snap.disk_used.unwrap_or(0),
        "disk_total": snap.disk_total.unwrap_or(0),
        "net_in": snap.net_down.unwrap_or(0) as i64,
        "net_out": snap.net_up.unwrap_or(0) as i64,
        "net_total_up": snap.net_total_up.unwrap_or(0) as i64,
        "net_total_down": snap.net_total_down.unwrap_or(0) as i64,
        "process": snap.process.unwrap_or(0),
        "connections": tcp + udp,
        "connections_udp": udp,
        "online": online,
        "uptime": snap.uptime_secs.unwrap_or(0) as i64,
    })
}

fn offline_status(row: &sakuio_db::PublicClientRow) -> Value {
    // Live gauges (cpu / ram / rates) are zeroed, but cumulative counters and
    // uptime fall back to the last values persisted from telemetry, so a
    // board restart no longer wipes the node's totals.
    // 实时指标清零，但累计流量与运行时长回退到遥测持久化的最后值，
    // 面板重启不再把总流量清零。
    json!({
        "client": row.uuid,
        "time": Utc::now().to_rfc3339(),
        "cpu": 0.0,
        "gpu": 0.0,
        "ram": 0,
        "ram_total": 0,
        "swap": 0,
        "swap_total": 0,
        "load": 0.0,
        "load5": 0.0,
        "load15": 0.0,
        "temp": 0.0,
        "disk": 0,
        "disk_total": 0,
        "net_in": 0,
        "net_out": 0,
        "net_total_up": row.last_net_total_up.unwrap_or(0),
        "net_total_down": row.last_net_total_down.unwrap_or(0),
        "process": 0,
        "connections": 0,
        "connections_udp": 0,
        "online": row.online,
        "uptime": row.last_uptime_secs.unwrap_or(0),
        "ping": {},
    })
}

/// Agent v2 report shape for `/api/clients` WebSocket push.
pub fn v2_report_from_snapshot(
    snap: &sakuio_board::RuntimeSnapshot,
    basic: &BasicInfoParams,
) -> Value {
    let mut cpu_usage = snap.cpu_usage.unwrap_or(0.0);
    if cpu_usage == 0.0 {
        cpu_usage = 0.01;
    }
    let updated_at = Utc
        .timestamp_millis_opt(snap.observed_at_ms)
        .single()
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| Utc::now().to_rfc3339());
    json!({
        "cpu": {
            "name": basic.cpu_name.clone().unwrap_or_default(),
            "cores": basic.cpu_cores.unwrap_or(0),
            "arch": basic.arch.clone().unwrap_or_default(),
            "usage": cpu_usage,
        },
        "ram": {
            "total": snap.mem_total.unwrap_or(0),
            "used": snap.mem_used.unwrap_or(0),
        },
        "swap": {
            "total": snap.swap_total.unwrap_or(0) as u64,
            "used": snap.swap_used.unwrap_or(0) as u64,
        },
        "load": {
            "load1": snap.load1.unwrap_or(0.0),
            "load5": snap.load5.unwrap_or(0.0),
            "load15": snap.load15.unwrap_or(0.0),
        },
        "disk": {
            "total": snap.disk_total.unwrap_or(0),
            "used": snap.disk_used.unwrap_or(0),
        },
        "network": {
            "up": snap.net_up.unwrap_or(0),
            "down": snap.net_down.unwrap_or(0),
            "totalUp": snap.net_total_up.unwrap_or(0),
            "totalDown": snap.net_total_down.unwrap_or(0),
        },
        "connections": {
            "tcp": snap.connections_tcp.unwrap_or(0),
            "udp": snap.connections_udp.unwrap_or(0),
        },
        "uptime": snap.uptime_secs.unwrap_or(0),
        "process": snap.process.unwrap_or(0),
        "message": "",
        "updated_at": updated_at,
    })
}

pub fn param_uuid(params: Option<&Value>) -> Option<String> {
    params
        .and_then(|p| p.get("uuid"))
        .and_then(|u| u.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}
