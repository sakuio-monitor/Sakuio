//! Baidu Cloud DNS driver.
//! 百度云 DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::Arc;

const HOST: &str = "dns.baidubce.com";
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ').add(b'"').add(b'#').add(b'$').add(b'%').add(b'&')
    .add(b'\'').add(b'(').add(b')').add(b'*').add(b'+').add(b',')
    .add(b'/').add(b':').add(b';').add(b'<').add(b'=').add(b'>')
    .add(b'?').add(b'@').add(b'[').add(b'\\').add(b']').add(b'^')
    .add(b'`').add(b'{').add(b'|').add(b'}');

struct BaiduDriver;

fn encode(value: &str) -> String { utf8_percent_encode(value, ENCODE_SET).to_string() }

fn sign_with_key(secret: &str, access_key: &str, method: &str, uri: &str, query: &[(&str, String)], date: &str) -> String {
    let mut pairs = query.iter().map(|(k, v)| (encode(k), encode(v))).collect::<Vec<_>>();
    pairs.sort();
    let canonical_query = pairs.into_iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let canonical = format!("{method}\n{uri}\n{canonical_query}\nhost:{HOST}\nx-bce-date:{date}");
    let key_info = format!("bce-auth-v1/{access_key}/{date}/1800");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(key_info.as_bytes());
    let sign_key = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&sign_key).expect("hmac");
    mac.update(canonical.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    format!("{key_info}/host;x-bce-date/{signature}")
}

async fn call(
    config: &Value,
    method: &str,
    uri: &str,
    query: &[(&str, String)],
    body: Value,
) -> Result<Value, String> {
    let access_key = common::required(config, "access_key", "AccessKey")?;
    let secret = common::required(config, "secret_key", "SecretKey")?;
    let client = common::client()?;
    let date = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let authorization = sign_with_key(&secret, &access_key, method, uri, query, &date);
    let payload = if body.is_null() { String::new() } else { body.to_string() };
    let mut request = match method {
        "GET" => client.get(format!("https://{HOST}{uri}")),
        "POST" => client.post(format!("https://{HOST}{uri}")),
        "PUT" => client.put(format!("https://{HOST}{uri}")),
        _ => return Err("不支持的百度云请求方法".into()),
    };
    if !query.is_empty() { request = request.query(query); }
    let response = request
        .header("x-bce-date", date)
        .header("Authorization", authorization)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await
        .map_err(|error| format!("连接百度云 DNS 失败：{error}"))?;
    if !response.status().is_success() {
        return Err(common::error_message(response).await);
    }
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    response.json().await.map_err(|error| format!("解析百度云响应失败：{error}"))
}

#[async_trait]
impl DdnsDriver for BaiduDriver {
    fn id(&self) -> &'static str { "baidu" }
    fn name(&self) -> &'static str { "百度云" }
    fn description(&self) -> &'static str { "百度云 AccessKey / SecretKey，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "access_key", label: "AccessKey", kind: "text", required: true, placeholder: "百度云 AccessKey", options: None },
            DriverField { key: "secret_key", label: "SecretKey", kind: "password", required: true, placeholder: "百度云 SecretKey", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "access_key", "AccessKey")?;
        common::required(config, "secret_key", "SecretKey")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let zone = request.zone_name.trim().trim_end_matches('.');
        let rr = if request.record_name.trim().is_empty() { "@" } else { request.record_name.trim() };
        let record_type = request.record_type.to_ascii_uppercase();
        let mut id = request.record_id.clone().filter(|value| !value.is_empty());
        if id.is_none() {
            let value = call(config, "GET", &format!("/v1/dns/zone/{zone}/record"), &[("rr", rr.into()), ("maxKeys", "100".into())], Value::Null).await?;
            id = value.get("records").and_then(Value::as_array).and_then(|items| {
                items.iter().find(|item| {
                    item.get("rr").and_then(Value::as_str) == Some(rr)
                        && item.get("type").and_then(Value::as_str) == Some(record_type.as_str())
                })
            }).and_then(|item| item.get("id")).and_then(Value::as_str).map(str::to_string);
        }
        let body = json!({"rr": rr, "type": record_type, "value": request.content, "ttl": request.ttl.max(60)});
        if let Some(id) = id {
            call(config, "PUT", &format!("/v1/dns/zone/{zone}/record/{id}"), &[], body).await?;
            Ok(DdnsResult { zone_id: zone.into(), record_id: id })
        } else {
            let value = call(config, "POST", &format!("/v1/dns/zone/{zone}/record"), &[], body).await?;
            let id = value.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
            Ok(DdnsResult { zone_id: zone.into(), record_id: id })
        }
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(BaiduDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_metadata() {
        assert_eq!(BaiduDriver.id(), "baidu");
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(BaiduDriver.validate(&json!({})).await.is_err());
    }
    #[test]
    fn signature_is_stable() {
        let value = sign_with_key("secret", "ak", "GET", "/v1/dns/zone", &[("name", "example.com".into())], "2026-01-01T00:00:00Z");
        assert!(value.starts_with("bce-auth-v1/ak/"));
    }
}
