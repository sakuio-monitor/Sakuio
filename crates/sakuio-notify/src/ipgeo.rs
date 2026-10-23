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
    region: Option<String>,
    #[serde(default)]
    city: Option<String>,
}

#[derive(serde::Deserialize)]
struct IpApiResponse {
    #[serde(default)]
    country: Option<String>,
    #[serde(default, rename = "countryCode")]
    country_code: Option<String>,
    #[serde(default, rename = "regionName")]
    region_name: Option<String>,
    #[serde(default)]
    city: Option<String>,
    #[serde(default)]
    district: Option<String>,
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
    #[serde(default)]
    area: Option<String>,
    #[serde(default)]
    district: Option<String>,
}

fn cache() -> &'static Mutex<HashMap<String, (Option<GeoInfo>, std::time::Instant)>> {
    static CACHE: OnceLock<Mutex<HashMap<String, (Option<GeoInfo>, std::time::Instant)>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns whether the address is loopback / private / link-local / reserved.
/// Parses the address instead of string prefixes so addresses such as
/// `172.200.1.1` are not mistaken for RFC1918 space.
pub fn is_private_ip(ip: &str) -> bool {
    let Ok(address) = ip.trim().parse::<std::net::IpAddr>() else {
        return ip.trim().is_empty();
    };
    match address {
        std::net::IpAddr::V4(v4) => is_reserved_v4(v4),
        std::net::IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_reserved_v4(v4);
            }
            let segments = v6.segments();
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        }
    }
}

fn is_reserved_v4(ip: std::net::Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || ip.is_multicast()
        || a == 0
        || (a == 100 && (64..=127).contains(&b))
        || (a == 198 && (b == 18 || b == 19))
        || (a == 192 && b == 0 && (c == 0 || c == 2))
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 240
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
/// 单次请求失败立即重试一次，两次都失败才按失败缓存。
pub async fn lookup_info_with(provider: &str, ip: &str) -> Option<GeoInfo> {
    let ip = ip.trim();
    if is_private_ip(ip) {
        return None;
    }
    let cache_key = format!("{provider}:{ip}");
    if let Some(cached) = cached_valid(&cache_key) {
        return cached;
    }
    let result = match lookup_one(provider, ip).await {
        Some(info) => Some(info),
        // 一次失败立刻重试（网络抖动/瞬时超时），仍失败才接受
        None => lookup_one(provider, ip).await,
    };
    if let Ok(mut c) = cache().lock() {
        c.insert(cache_key, (result.clone(), std::time::Instant::now()));
    }
    result
}

async fn lookup_one(provider: &str, ip: &str) -> Option<GeoInfo> {
    match provider {
        PROVIDER_IP9 => lookup_ip9(ip).await,
        PROVIDER_IPAPI => lookup_ip_api(ip).await,
        PROVIDER_MMDB => lookup_mmdb(ip),
        _ => lookup_geojs(ip).await,
    }
}

const SUCCESS_TTL: Duration = Duration::from_secs(60 * 60);
const FAILURE_TTL: Duration = Duration::from_secs(10);

/// Returns the cached entry when present and not expired.
fn cached_valid(key: &str) -> Option<Option<GeoInfo>> {
    let c = cache().lock().ok()?;
    let (entry, at) = c.get(key)?;
    let ttl = if entry.is_some() {
        SUCCESS_TTL
    } else {
        FAILURE_TTL
    };
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

fn join_location_parts(parts: &[&str]) -> String {
    let mut output: Vec<&str> = Vec::new();
    for part in parts {
        let part = part.trim();
        if !part.is_empty() && !output.contains(&part) {
            output.push(part);
        }
    }
    output.join(" ")
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
    // 位置文本：国家 + 省份 + 城市；没有城市时保留省份。
    let province = data.prov.as_deref().unwrap_or("");
    let city = data.city.as_deref().unwrap_or("");
    let district = data
        .area
        .as_deref()
        .or(data.district.as_deref())
        .unwrap_or("");
    Some(GeoInfo {
        country_code,
        location: join_location_parts(&[country, province, city, district]),
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
            location: join_location_parts(&[
                geo.country.as_deref().unwrap_or(""),
                geo.region.as_deref().unwrap_or(""),
                geo.city.as_deref().unwrap_or(""),
            ]),
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
            location: join_location_parts(&[
                geo.country.as_deref().unwrap_or(""),
                geo.region_name.as_deref().unwrap_or(""),
                geo.city.as_deref().unwrap_or(""),
                geo.district.as_deref().unwrap_or(""),
            ]),
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
            .and_then(|n| {
                n.get("zh-CN")
                    .or_else(|| n.get("en"))
                    .map(|s| s.to_string())
            })
            .unwrap_or_default()
    };
    let country = record.country.as_ref();
    let country_code = country
        .and_then(|c| c.iso_code.as_deref())
        .map(|s| s.trim().to_uppercase())
        .unwrap_or_default();
    let country_name = pick(country.map(|c| c.names.clone()).unwrap_or_default());
    let subdivisions: Vec<String> = record
        .subdivisions
        .as_ref()
        .map(|items| {
            items
                .iter()
                .take(2)
                .map(|subdivision| pick(subdivision.names.clone()))
                .filter(|name| !name.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let city = pick(
        record
            .city
            .as_ref()
            .map(|c| c.names.clone())
            .unwrap_or_default(),
    );
    let mut parts: Vec<&str> = vec![&country_name];
    parts.extend(subdivisions.iter().map(String::as_str));
    parts.push(&city);
    Some(GeoInfo {
        country_code,
        location: join_location_parts(&parts),
    })
    .filter(|info| !info.country_code.is_empty() || !info.location.is_empty())
}

fn urlencode(ip: &str) -> String {
    // IPs and IPv6 hex/colons are URL-safe; strip anything unusual.
    ip.chars()
        .filter(|c| c.is_ascii_hexdigit() || *c == '.' || *c == ':')
        .collect()
}

#[cfg(test)]
mod tests {
    use super::is_private_ip;

    #[test]
    fn reserved_and_public_ranges() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.0.1",
            "0.0.0.0",
            "224.0.0.1",
            "::1",
            "fd00::1",
            "fe80::1",
            "2001:db8::1",
        ] {
            assert!(is_private_ip(ip), "{ip} should be reserved");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "172.200.1.1", "2001:4860:4860::8888"] {
            assert!(!is_private_ip(ip), "{ip} should be public");
        }
    }
}
