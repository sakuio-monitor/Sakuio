//! Global persistent IP geolocation engine shared by node flags, audit logs
//! and IP detail views. The hot path uses an in-memory map; the durable cache
//! lives in `ip_geo_cache` so restarts do not re-query upstream providers.
//! 全局 IP 归属地引擎：内存热缓存 + 数据库持久缓存，节点/日志/详情共用。

use crate::AppState;
use chrono::{SecondsFormat, Utc};
use sakuio_db::{ClientRepository, DbPool, GeoCacheRepository, PublicClientRow};
use sakuio_notify::ipgeo::GeoInfo;
use std::collections::{HashMap, HashSet};

/// Wakes the geo resolver immediately after an agent reports a new IP.
static GEO_WAKE: tokio::sync::Notify = tokio::sync::Notify::const_new();

pub fn wake_geo_resolver() {
    GEO_WAKE.notify_one();
}
use std::sync::{Arc, Mutex as StdMutex, OnceLock, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Mutex as AsyncMutex};

struct CacheEntry {
    code: String,
    location: String,
    expires_at: Instant,
}

fn mem() -> &'static RwLock<HashMap<String, CacheEntry>> {
    static STORE: OnceLock<RwLock<HashMap<String, CacheEntry>>> = OnceLock::new();
    STORE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 基础 TTL：7 天。
const BASE_TTL_SECS: u64 = 7 * 86400;
/// 抖动幅度：±48 小时。
const JITTER_SECS: u64 = 2 * 86400;
/// 空 / 失败结果的负缓存时长。
const NEGATIVE_TTL: Duration = Duration::from_secs(60);

fn jittered_ttl() -> Duration {
    Duration::from_secs(BASE_TTL_SECS - JITTER_SECS + rand_jitter_secs())
}

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

/// 上游查询全局限速：相邻请求间隔 ≥ 70ms（≈14.3 req/s）。
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

/// 国家代码（同步读内存热缓存）；空串表示未知。
pub fn region_code_for(ip: &str) -> String {
    let ip = ip.trim();
    if ip.is_empty() {
        return String::new();
    }
    mem_read(ip).map(|(code, _)| code).unwrap_or_default()
}

/// 清空内存热缓存（持久缓存由 [`GeoEngine::clear_persistent`] 清理）。
pub fn invalidate_all() {
    if let Ok(mut map) = mem().write() {
        map.clear();
    }
}

fn mem_read(ip: &str) -> Option<(String, String)> {
    let map = mem().read().ok()?;
    let entry = map.get(ip)?;
    if entry.expires_at <= Instant::now() {
        return None;
    }
    Some((entry.code.clone(), entry.location.clone()))
}

fn mem_insert(ip: &str, code: &str, location: &str, ttl: Duration) {
    if let Ok(mut map) = mem().write() {
        if map.len() > 16_384 {
            let now = Instant::now();
            map.retain(|_, entry| entry.expires_at > now);
            if map.len() > 16_384 {
                map.clear();
            }
        }
        map.insert(
            ip.to_string(),
            CacheEntry {
                code: code.to_string(),
                location: location.to_string(),
                expires_at: Instant::now() + ttl,
            },
        );
    }
}

fn inflight() -> &'static StdMutex<HashMap<String, Arc<AsyncMutex<()>>>> {
    static STORE: OnceLock<StdMutex<HashMap<String, Arc<AsyncMutex<()>>>>> = OnceLock::new();
    STORE.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn release_inflight(ip: &str, lock: &Arc<AsyncMutex<()>>) {
    // Remove the per-IP mutex once no waiter holds it. Keeping an entry per
    // unique IP forever was an unbounded growth path for audit/public traffic.
    if Arc::strong_count(lock) <= 2 {
        if let Ok(mut map) = inflight().lock() {
            if map.get(ip).is_some_and(|entry| Arc::ptr_eq(entry, lock)) {
                map.remove(ip);
            }
        }
    }
}

/// Durable, shared IP geolocation engine.
/// 持久化、全局共享的 IP 归属地引擎。
#[derive(Clone)]
pub struct GeoEngine {
    pool: DbPool,
}

impl GeoEngine {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Resolves an IP with the configured provider, using memory then database
    /// cache before calling the upstream provider. Concurrent misses for the
    /// same IP are coalesced into one request.
    /// 解析 IP：先内存、再数据库，最后上游；同 IP 并发未命中只请求一次。
    pub async fn lookup(&self, ip: &str, provider: &str) -> Option<GeoInfo> {
        let ip = ip.trim();
        if ip.is_empty() || sakuio_notify::ipgeo::is_private_ip(ip) {
            return None;
        }
        if let Some((code, location)) = mem_read(ip) {
            return Some(GeoInfo {
                country_code: code,
                location,
            });
        }

        let lock = {
            let mut map = inflight()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if map.len() > 4096 {
                map.retain(|_, entry| Arc::strong_count(entry) > 1);
            }
            map.entry(ip.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        let _guard = lock.lock().await;
        if let Some((code, location)) = mem_read(ip) {
            release_inflight(ip, &lock);
            return Some(GeoInfo {
                country_code: code,
                location,
            });
        }
        let now = Utc::now().timestamp();
        if let Ok(Some(row)) = GeoCacheRepository::new(&self.pool).get(ip).await {
            if row.expires_at > now {
                mem_insert(ip, &row.country_code, &row.location, jittered_ttl());
                release_inflight(ip, &lock);
                if row.country_code.is_empty() && row.location.is_empty() {
                    return None;
                }
                return Some(GeoInfo {
                    country_code: row.country_code,
                    location: row.location,
                });
            }
        }

        acquire_query_slot().await;
        let info = sakuio_notify::ipgeo::lookup_info_with(provider, ip).await;
        let (code, location, ttl) = match &info {
            Some(info) if !info.country_code.is_empty() || !info.location.is_empty() => (
                info.country_code.clone(),
                info.location.clone(),
                jittered_ttl(),
            ),
            _ => (String::new(), String::new(), NEGATIVE_TTL),
        };
        if let Err(error) = GeoCacheRepository::new(&self.pool)
            .upsert(
                ip,
                &code,
                &location,
                now + ttl.as_secs() as i64,
                &Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            )
            .await
        {
            tracing::warn!(ip = %ip, error = %error, "persist ip geo cache failed");
        }
        mem_insert(ip, &code, &location, ttl);
        release_inflight(ip, &lock);
        info
    }

    /// Resolves using the current site settings; used by audit enrichment.
    pub async fn lookup_current(&self, ip: &str) -> Option<GeoInfo> {
        let settings = crate::site_settings::load_from_pool(&self.pool).await;
        if !settings.geoip_enabled {
            return None;
        }
        self.lookup(ip, &settings.geoip_provider).await
    }

    /// Clears durable rows after GeoIP provider / MMDB changes.
    pub async fn clear_persistent(&self) {
        if let Err(error) = sqlx::query("DELETE FROM ip_geo_cache")
            .execute(&self.pool)
            .await
        {
            tracing::warn!(error = %error, "clear ip geo cache failed");
        }
    }

    /// Deletes expired rows that no server currently references.
    pub async fn cleanup_expired(&self) {
        if let Ok(mut map) = mem().write() {
            let now = Instant::now();
            map.retain(|_, entry| entry.expires_at > now);
        }
        if let Ok(mut map) = inflight().lock() {
            map.retain(|_, entry| Arc::strong_count(entry) > 1);
        }
        let Ok(rows) = ClientRepository::new(&self.pool)
            .list_for_public_api()
            .await
        else {
            return;
        };
        let mut referenced: HashSet<String> = HashSet::new();
        for row in &rows {
            for (ip, _) in client_ips(row) {
                referenced.insert(ip);
            }
        }
        let now = Utc::now().timestamp();
        loop {
            match GeoCacheRepository::new(&self.pool)
                .delete_expired_unreferenced(now, &referenced, 500)
                .await
            {
                Ok(0) => break,
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(error) => {
                    tracing::warn!(error = %error, "cleanup ip geo cache failed");
                    break;
                }
            }
        }
    }
}

/// Shared lookup used by the log-detail endpoint.
pub async fn lookup_ip(state: &AppState, ip: &str) -> Option<GeoInfo> {
    let settings = crate::site_settings::load(state).await;
    if !settings.geoip_enabled {
        return None;
    }
    state.geo.lookup(ip, &settings.geoip_provider).await
}

/// Starts the periodic resolver (60s tick) and cache cleanup (6h).
pub fn spawn_geoip_resolver(state: AppState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        let mut cleanup_ticker = tokio::time::interval(Duration::from_secs(6 * 3600));
        let mut last_provider = String::new();
        loop {
            if sakuio_db::maintenance_active() {
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = tick(&state, &mut last_provider).await {
                        tracing::warn!(error = %e, "geoip resolver tick failed");
                    }
                }
                _ = GEO_WAKE.notified() => {
                    // Debounce a burst of agents connecting at once.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if let Err(e) = tick(&state, &mut last_provider).await {
                        tracing::warn!(error = %e, "geoip resolver wake failed");
                    }
                }
                _ = cleanup_ticker.tick() => {
                    state.geo.cleanup_expired().await;
                }
            }
        }
    });
}

async fn tick(state: &AppState, last_provider: &mut String) -> Result<(), String> {
    let settings = crate::site_settings::load(state).await;
    if !settings.geoip_enabled {
        if !last_provider.is_empty() {
            invalidate_all();
            last_provider.clear();
        }
        return Ok(());
    }
    if *last_provider != settings.geoip_provider {
        if !last_provider.is_empty() {
            invalidate_all();
            state.geo.clear_persistent().await;
        }
        *last_provider = settings.geoip_provider.clone();
    }

    let rows = ClientRepository::new(&state.db)
        .list_for_public_api()
        .await
        .map_err(|e| e.to_string())?;
    let mut ips: HashSet<String> = HashSet::new();
    let mut ip_targets: HashMap<String, Vec<(&str, bool)>> = HashMap::new();
    for row in &rows {
        for (ip, is_v6) in client_ips(row) {
            ips.insert(ip.clone());
            ip_targets.entry(ip).or_default().push((&row.uuid, is_v6));
        }
    }
    ips.retain(|ip| !ip.is_empty() && !sakuio_notify::ipgeo::is_private_ip(ip));

    let mut pending: Vec<String> = ips
        .into_iter()
        .filter(|ip| mem_read(ip).is_none())
        .collect();
    pending.sort();
    for ip in &pending {
        let info = state.geo.lookup(ip, &settings.geoip_provider).await;
        if let Some(info) = info.filter(|info| !info.country_code.is_empty()) {
            let repo = ClientRepository::new(&state.db);
            for (uuid, is_v6) in ip_targets.get(ip).into_iter().flatten() {
                let res = repo.update_geo_regions(
                    uuid,
                    (!is_v6).then_some(info.country_code.as_str()),
                    is_v6.then_some(info.country_code.as_str()),
                );
                if let Err(e) = res.await {
                    tracing::warn!(node_uuid = %uuid, error = %e, "failed to persist geo region");
                }
            }
        }
    }
    Ok(())
}

fn client_ips(row: &PublicClientRow) -> Vec<(String, bool)> {
    let mut ips = Vec::new();
    for (raw, is_v6) in [(&row.ipv4, false), (&row.ipv6, true)] {
        for part in raw.split(',') {
            let ip = part.split('/').next().unwrap_or("").trim();
            if !ip.is_empty() {
                ips.push((ip.to_string(), is_v6));
            }
        }
    }
    if let Ok(v) =
        serde_json::from_str::<serde_json::Value>(row.basic_info_json.as_deref().unwrap_or(""))
    {
        for (key, is_v6) in [("ipv4", false), ("ipv6", true)] {
            if let Some(s) = v.get(key).and_then(|p| p.as_str()) {
                let ip = s.split('/').next().unwrap_or("").trim();
                if !ip.is_empty() {
                    ips.push((ip.to_string(), is_v6));
                }
            }
        }
    }
    ips
}
