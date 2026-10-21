//! GeoLite MMDB database download and validation.
//! GeoLite MMDB 数据库下载与校验。

/// Community-maintained GeoLite2-City mirror.
pub const DEFAULT_MMDB_URL: &str =
    "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-City.mmdb";

pub const MMDB_PATH: &str = "./data/GeoLite.mmdb";

/// Downloads an MMDB database to `./data/GeoLite.mmdb` (atomic via temp file).
/// Returns the number of bytes written.
pub async fn download_mmdb(url: &str) -> Result<u64, String> {
    let url = if url.trim().is_empty() {
        DEFAULT_MMDB_URL
    } else {
        url.trim()
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client.get(url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;
    // MMDB files start with the magic: D2 9B 08 2C ...
    // Actually the metadata marker search is the robust check.
    if bytes.len() < 0x10_000 {
        return Err("downloaded file is too small to be an MMDB database".into());
    }
    if !is_mmdb(&bytes) {
        return Err("downloaded file is not a valid MMDB database".into());
    }
    let tmp = format!("{MMDB_PATH}.tmp");
    tokio::fs::write(&tmp, &bytes)
        .await
        .map_err(|e| e.to_string())?;
    tokio::fs::rename(&tmp, MMDB_PATH)
        .await
        .map_err(|e| e.to_string())?;
    Ok(bytes.len() as u64)
}

/// Fetches the server's public egress IPv4 (used by the GeoIP test with no explicit IP).
pub async fn public_ip() -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let body = client
        .get("https://api.ipify.org")
        .send()
        .await
        .map_err(|e| e.to_string())?
        .text()
        .await
        .map_err(|e| e.to_string())?;
    Ok(body.trim().to_string())
}

/// MMDB files embed the metadata marker near the end of the file.
fn is_mmdb(bytes: &[u8]) -> bool {
    const MARKER: &[u8] = b"\xab\xcd\xefMaxMind.com";
    let start = bytes.len().saturating_sub(256 * 1024);
    bytes[start..]
        .windows(MARKER.len())
        .any(|w| w == MARKER)
}
