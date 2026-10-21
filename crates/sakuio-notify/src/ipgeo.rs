use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

pub const PROVIDER_IP9: &str = "ip9";
pub const PROVIDER_GEOJS: &str = "geojs";
pub const PROVIDER_IPAPI: &str = "ip-api";
pub const PROVIDER_MMDB: &str = "mmdb";

/// Resolved geo information for one IP.
/// 一个 IP 解析出的归属地信息。
#[derive(Clone, Debug, serde::Serialize)]
pub struct GeoInfo {
    /// ISO 3166-1 alpha-2 code (e.g. `CN`, `US`) used for flag rendering.
    pub country_code: String,
    /// Human readable location ("国家 城市").
    pub location: String,
}

#[derive(serde::Deserialize)]
struct GeoJsResponse {
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    country_code: Option<String>,
    #[serde(default)]
    city: Option<String>,
}

#[derive(serde::Deserialize)]
struct IpApiResponse {
    #[serde(default)]
    country: Option<String>,
    #[serde(default, rename = "countryCode")]
    country_code: Option<String>,
    #[serde(default)]
    city: Option<String>,
}

#[derive(serde::Deserialize)]
struct Ip9Response {
    #[serde(default)]
    ret: i32,
    #[serde(default)]
    data: Option<Ip9Data>,
}

#[derive(serde::Deserialize)]
struct Ip9Data {
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    country_code: Option<String>,
    #[serde(default)]
    prov: Option<String>,
    #[serde(default)]
    city: Option<String>,
}

fn cache() -> &'static Mutex<HashMap<String, (Option<GeoInfo>, std::time::Instant)>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (Option<GeoInfo>, std::time::Instant)>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns whether the address is loopback / private / link-local.
pub fn is_private_ip(ip: &str) -> bool {
    let ip = ip.trim();
    ip.is_empty()
        || ip.starts_with("127.")
        || ip.starts_with("10.")
        || ip.starts_with("192.168.")
        || ip.starts_with("172.16.")
        || ip.starts_with("172.17.")
        || ip.starts_with("172.18.")
        || ip.starts_with("172.19.")
        || ip.starts_with("172.2")
        || ip.starts_with("172.30.")
        || ip.starts_with("172.31.")
        || ip == "::1"
        || ip.starts_with("fe80:")
}

/// Resolves the approximate location ("国家 城市") via geojs.io (default provider).
pub async fn lookup_location(ip: &str) -> Option<String> {
    lookup_with(PROVIDER_GEOJS, ip).await
}

/// Resolves the location text with an explicit provider.
pub async fn lookup_with(provider: &str, ip: &str) -> Option<String> {
    lookup_info_with(provider, ip)
        .await
        .map(|info| info.location)
}

/// Resolves geo information (country code + location text) with an explicit provider.
/// Providers: `geojs` (geojs.io), `ip-api` (ip-api.com), `mmdb` (local GeoLite database).
/// Results are cached with a TTL; failures are only cached briefly.
pub async fn lookup_info_with(provider: &str, ip: &str) -> Option<GeoInfo> {
    let ip = ip.trim();
    if is_private_ip(ip) {
        return None;
    }
    let cache_key = format!("{provider}:{ip}");
    if let Some(cached) = cached_valid(&cache_key) {
        return cached;
    }
    let result = match provider {
        PROVIDER_IP9 => lookup_ip9(ip).await,
        PROVIDER_IPAPI => lookup_ip_api(ip).await,
        PROVIDER_MMDB => lookup_mmdb(ip),
        _ => lookup_geojs(ip).await,
    };
    if let Ok(mut c) = cache().lock() {
        c.insert(cache_key, (result.clone(), std::time::Instant::now()));
    }
    result
}

const SUCCESS_TTL: Duration = Duration::from_secs(60 * 60);
const FAILURE_TTL: Duration = Duration::from_secs(10);

/// Returns the cached entry when present and not expired.
fn cached_valid(key: &str) -> Option<Option<GeoInfo>> {
    let c = cache().lock().ok()?;
    let (entry, at) = c.get(key)?;
    let ttl = if entry.is_some() { SUCCESS_TTL } else { FAILURE_TTL };
    if at.elapsed() < ttl {
        Some(entry.clone())
    } else {
        None
    }
}

/// Clears the geo lookup cache (used after switching provider / updating the database).
pub fn clear_cache() {
    if let Ok(mut c) = cache().lock() {
        c.clear();
    }
}

fn join_location(country: &str, city: &str) -> String {
    let country = country.trim();
    let city = city.trim();
    let mut parts = Vec::new();
    if !country.is_empty() {
        parts.push(country.to_string());
    }
    if !city.is_empty() && city != country {
        parts.push(city.to_string());
    }
    parts.join(" ")
}

async fn lookup_ip9(ip: &str) -> Option<GeoInfo> {
    let url = format!("https://ip9.com.cn/get?ip={}", urlencode(ip));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = match client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(ip = %ip, error = %e, "ip9 request failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        tracing::warn!(ip = %ip, status = %resp.status(), "ip9 returned non-success");
        return None;
    }
    let parsed: Ip9Response = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(ip = %ip, error = %e, "ip9 response parse failed");
            return None;
        }
    };
    if parsed.ret != 200 {
        tracing::warn!(ip = %ip, ret = parsed.ret, "ip9 query rejected");
        return None;
    }
    let data = parsed.data?;
    let country = data.country.as_deref().unwrap_or("").trim();
    let country_code = data
        .country_code
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_uppercase();
    // 位置文本：国家 + 城市（无城市时用省份）。
    let city = data
        .city
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| data.prov.as_deref().map(str::trim).filter(|s| !s.is_empty()))
        .unwrap_or("");
    Some(GeoInfo {
        country_code,
        location: join_location(country, city),
    })
    .filter(|info| !info.country_code.is_empty() || !info.location.is_empty())
}

async fn lookup_geojs(ip: &str) -> Option<GeoInfo> {
    let url = format!("https://get.geojs.io/v1/ip/geo/{}.json", urlencode(ip));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = match client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(ip = %ip, error = %e, "geojs request failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        tracing::warn!(ip = %ip, status = %resp.status(), "geojs returned non-success");
        return None;
    }
    match resp.json::<GeoJsResponse>().await {
        Ok(geo) => Some(GeoInfo {
            country_code: geo.country_code.unwrap_or_default().trim().to_uppercase(),
            location: join_location(
                geo.country.as_deref().unwrap_or(""),
                geo.city.as_deref().unwrap_or(""),
            ),
        })
        .filter(|info| !info.country_code.is_empty() || !info.location.is_empty()),
        Err(e) => {
            tracing::warn!(ip = %ip, error = %e, "geojs response parse failed");
            None
        }
    }
}

async fn lookup_ip_api(ip: &str) -> Option<GeoInfo> {
    let url = format!("http://ip-api.com/json/{}?lang=zh-CN", urlencode(ip));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = match client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(ip = %ip, error = %e, "ip-api request failed");
            return None;
        }
    };
    if !resp.status().is_success() {
        tracing::warn!(ip = %ip, status = %resp.status(), "ip-api returned non-success");
        return None;
    }
    match resp.json::<IpApiResponse>().await {
        Ok(geo) => Some(GeoInfo {
            country_code: geo.country_code.unwrap_or_default().trim().to_uppercase(),
            location: join_location(
                geo.country.as_deref().unwrap_or(""),
                geo.city.as_deref().unwrap_or(""),
            ),
        })
        .filter(|info| !info.country_code.is_empty() || !info.location.is_empty()),
        Err(e) => {
            tracing::warn!(ip = %ip, error = %e, "ip-api response parse failed");
            None
        }
    }
}

fn mmdb_path() -> &'static str {
    "./data/GeoLite.mmdb"
}

fn lookup_mmdb(ip: &str) -> Option<GeoInfo> {
    let reader = maxminddb::Reader::open_readfile(mmdb_path()).ok()?;
    let record: maxminddb::geoip2::City = reader.lookup(ip.parse().ok()?).ok()?;
    let pick = |names: Option<BTreeMap<&str, &str>>| {
        names
            .and_then(|n| n.get("zh-CN").or_else(|| n.get("en")).map(|s| s.to_string()))
            .unwrap_or_default()
    };
    let country = record.country.as_ref();
    let country_code = country
        .and_then(|c| c.iso_code.as_deref())
        .map(|s| s.trim().to_uppercase())
        .unwrap_or_default();
    let country_name = pick(country.map(|c| c.names.clone()).unwrap_or_default());
    let city = pick(record.city.as_ref().map(|c| c.names.clone()).unwrap_or_default());
    Some(GeoInfo {
        country_code,
        location: join_location(&country_name, &city),
    })
    .filter(|info| !info.country_code.is_empty() || !info.location.is_empty())
}

fn urlencode(ip: &str) -> String {
    // IPs and IPv6 hex/colons are URL-safe; strip anything unusual.
    ip.chars()
        .filter(|c| c.is_ascii_hexdigit() || *c == '.' || *c == ':')
        .collect()
}
