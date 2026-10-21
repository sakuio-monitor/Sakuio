//! 统一 GeoIP 缓存解析器（内存版）。
//! - 按 **IP** 维护国家代码缓存：每个 IP 首次出现解析一次，后续全部走缓存
//! - TTL 7 天 ± 48h 随机抖动（每条目独立），避免大量节点同时到期把上游 API 打爆
//! - 上游查询全局限速 ~15 req/s（排队节流），无论节点数量多少
//! - 切换 GeoIP 提供商（常规设置）时全部缓存标记失效，按新提供商重新解析
//! - 前后端旗帜渲染统一从这里取数，前端不再直连第三方 geo API

use crate::AppState;
use sakuio_db::{ClientRepository, PublicClientRow};
use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

struct CacheEntry {
    code: String,
    expires_at: Instant,
}

fn mem() -> &'static RwLock<HashMap<String, CacheEntry>> {
    static STORE: OnceLock<RwLock<HashMap<String, CacheEntry>>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 基础 TTL：7 天。
const BASE_TTL_SECS: u64 = 7 * 86400;
/// 抖动幅度：±48 小时（实际 TTL ∈ [5d, 9d]）。
const JITTER_SECS: u64 = 2 * 86400;
/// 空 / 失败结果的负缓存时长，避免每轮重复请求上游。
const NEGATIVE_TTL: Duration = Duration::from_secs(10 * 60);

fn jittered_ttl() -> Duration {
    Duration::from_secs(BASE_TTL_SECS - JITTER_SECS + rand_jitter_secs())
}

/// 无 rand 依赖的抖动：纳秒 ^ 秒做 xorshift 混合。
fn rand_jitter_secs() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 ^ d.as_secs())
        .unwrap_or(0);
    let mut x = nanos | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x % (2 * JITTER_SECS)
}

/// 上游查询全局限速：相邻请求间隔 ≥ 70ms（≈14.3 req/s，恒低于 15 req/s）。
async fn acquire_query_slot() {
    static SLOT: OnceLock<Mutex<Instant>> = OnceLock::new();
    let slot = SLOT.get_or_init(|| Mutex::new(Instant::now()));
    const MIN_INTERVAL: Duration = Duration::from_millis(70);
    let mut next_at = slot.lock().await;
    let now = Instant::now();
    let wait = if *next_at > now {
        *next_at - now
    } else {
        Duration::ZERO
    };
    *next_at = now + wait + MIN_INTERVAL;
    drop(next_at);
    if wait > Duration::ZERO {
        tokio::time::sleep(wait).await;
    }
}

/// 国家代码（按 IP 查缓存）；空串表示未知。
pub fn region_code_for(ip: &str) -> String {
    let ip = ip.trim();
    if ip.is_empty() {
        return String::new();
    }
    mem_read(ip).unwrap_or_default()
}

/// 读取有效缓存条目；过期视为未命中（由下一轮 tick 重新解析）。
fn mem_read(ip: &str) -> Option<String> {
    let map = mem().read().ok()?;
    let entry = map.get(ip)?;
    if entry.expires_at <= Instant::now() {
        return None;
    }
    Some(entry.code.clone())
}

fn mem_insert(ip: &str, code: &str, ttl: Duration) {
    if let Ok(mut map) = mem().write() {
        map.insert(
            ip.to_string(),
            CacheEntry {
                code: code.to_string(),
                expires_at: Instant::now() + ttl,
            },
        );
    }
}

/// Starts the periodic resolver loop (60s tick).
pub fn spawn_geoip_resolver(state: AppState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        let mut last_provider = String::new();
        loop {
            ticker.tick().await;
            if let Err(e) = tick(&state, &mut last_provider).await {
                tracing::warn!(error = %e, "geoip resolver tick failed");
            }
        }
    });
}

async fn tick(state: &AppState, last_provider: &mut String) -> Result<(), String> {
    let settings = crate::site_settings::load(state).await;
    if !settings.geoip_enabled {
        if !last_provider.is_empty() {
            // 关闭即整体失效；重新开启后按当前提供商重新解析
            if let Ok(mut map) = mem().write() {
                map.clear();
            }
            last_provider.clear();
        }
        return Ok(());
    }
    if *last_provider != settings.geoip_provider {
        // 更换提供商：全部缓存失效，按新提供商重新解析
        if let Ok(mut map) = mem().write() {
            map.clear();
        }
        *last_provider = settings.geoip_provider.clone();
    }

    let rows = ClientRepository::new(&state.db)
        .list_for_public_api()
        .await
        .map_err(|e| e.to_string())?;

    // 收集全部待解析 IP（连接地址 + basic_info 上报地址，v4/v6 一起），跳过已缓存与私网
    let mut ips: HashSet<String> = HashSet::new();
    for row in &rows {
        ips.extend(client_ips(row));
    }
    ips.retain(|ip| !ip.is_empty() && !sakuio_notify::ipgeo::is_private_ip(ip));

    let mut pending: Vec<String> = ips
        .into_iter()
        .filter(|ip| mem_read(ip).is_none())
        .collect();
    pending.sort();
    for ip in pending {
        // 全局限速排队：节点再多也不会超过 15 req/s
        acquire_query_slot().await;
        let info = sakuio_notify::ipgeo::lookup_info_with(&settings.geoip_provider, &ip).await;
        match info {
            Some(info) if !info.country_code.is_empty() => {
                mem_insert(&ip, &info.country_code, jittered_ttl());
            }
            _ => {
                mem_insert(&ip, "", NEGATIVE_TTL);
            }
        }
    }
    Ok(())
}

/// 节点的全部公网候选 IP（连接地址 + basic_info 上报地址）。
fn client_ips(row: &PublicClientRow) -> Vec<String> {
    let mut ips = Vec::new();
    for raw in [&row.ipv4, &row.ipv6] {
        for part in raw.split(',') {
            let ip = part.split('/').next().unwrap_or("").trim();
            if !ip.is_empty() {
                ips.push(ip.to_string());
            }
        }
    }
    if let Ok(v) =
        serde_json::from_str::<serde_json::Value>(row.basic_info_json.as_deref().unwrap_or(""))
    {
        for key in ["ipv4", "ipv6"] {
            if let Some(s) = v.get(key).and_then(|p| p.as_str()) {
                let ip = s.split('/').next().unwrap_or("").trim();
                if !ip.is_empty() {
                    ips.push(ip.to_string());
                }
            }
        }
    }
    ips
}
