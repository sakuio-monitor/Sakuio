//! Huawei Cloud DNS driver.
//! 华为云 DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const HOST: &str = "dns.myhuaweicloud.com";
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ').add(b'"').add(b'#').add(b'$').add(b'%').add(b'&')
    .add(b'\'').add(b'(').add(b')').add(b'*').add(b'+').add(b',')
    .add(b'/').add(b':').add(b';').add(b'<').add(b'=').add(b'>')
    .add(b'?').add(b'@').add(b'[').add(b'\\').add(b']').add(b'^')
    .add(b'`').add(b'{').add(b'|').add(b'}');

struct HuaweiDriver;

fn encode(value: &str) -> String {
    utf8_percent_encode(value, ENCODE_SET).to_string()
}

fn signature(secret: &str, method: &str, uri: &str, query: &[(&str, String)], body: &str, date: &str) -> String {
    let mut encoded = query
        .iter()
        .map(|(key, value)| (encode(key), encode(value)))
        .collect::<Vec<_>>();
    encoded.sort();
    let canonical_query = encoded
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical_headers = format!("host:{HOST}\nx-sdk-date:{date}\n");
    let signed_headers = "host;x-sdk-date";
    let canonical_request = format!(
        "{method}\n{uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{}",
        hex::encode(Sha256::digest(body.as_bytes()))
    );
    let string_to_sign = format!(
        "SDK-HMAC-SHA256\n{date}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(string_to_sign.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn call(
    config: &Value,
    method: &str,
    uri: &str,
    query: &[(&str, String)],
    body: Value,
) -> Result<Value, String> {
    let ak = common::required(config, "access_key", "AccessKey")?;
    let sk = common::required(config, "secret_key", "SecretKey")?;
    let client = common::client()?;
    let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let payload = if body.is_null() { String::new() } else { body.to_string() };
    let signature = signature(&sk, method, uri, query, &payload, &date);
    let authorization = format!("SDK-HMAC-SHA256 Access={ak}, SignedHeaders=host;x-sdk-date, Signature={signature}");
    let mut request = match method {
        "GET" => client.get(format!("https://{HOST}{uri}")),
        "POST" => client.post(format!("https://{HOST}{uri}")),
        "PUT" => client.put(format!("https://{HOST}{uri}")),
        _ => return Err("不支持的华为云请求方法".into()),
    };
    if !query.is_empty() {
        request = request.query(query);
    }
    let response = request
        .header("X-Sdk-Date", date)
        .header("Authorization", authorization)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await
        .map_err(|error| format!("连接华为云 DNS 失败：{error}"))?;
    if !response.status().is_success() {
        return Err(common::error_message(response).await);
    }
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    response.json().await.map_err(|error| format!("解析华为云响应失败：{error}"))
}

#[async_trait]
impl DdnsDriver for HuaweiDriver {
    fn id(&self) -> &'static str { "huaweicloud" }
    fn name(&self) -> &'static str { "华为云" }
    fn description(&self) -> &'static str { "华为云 AccessKey / SecretKey，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "access_key", label: "AccessKey", kind: "text", required: true, placeholder: "华为云 AccessKey", options: None },
            DriverField { key: "secret_key", label: "SecretKey", kind: "password", required: true, placeholder: "华为云 SecretKey", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "access_key", "AccessKey")?;
        common::required(config, "secret_key", "SecretKey")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let zone = request.zone_name.trim().trim_end_matches('.');
        let record_name = if request.record_name.trim().is_empty() || request.record_name.trim() == "@" {
            zone.to_string()
        } else {
            format!("{}.{}", request.record_name.trim(), zone)
        };
        let fqdn = format!("{record_name}.");
        let record_type = request.record_type.to_ascii_uppercase();
        let mut zone_id = request.zone_id.clone().filter(|value| !value.is_empty());
        if zone_id.is_none() {
            let value = call(config, "GET", "/v2/zones", &[("name", zone.to_string())], Value::Null).await?;
            zone_id = value
                .get("zones")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        let zone_id = zone_id.ok_or_else(|| format!("华为云中找不到域名 {zone}"))?;
        let mut record_id = request.record_id.clone().filter(|value| !value.is_empty());
        if record_id.is_none() {
            let value = call(
                config,
                "GET",
                &format!("/v2/zones/{zone_id}/recordsets"),
                &[("name", fqdn.clone()), ("type", record_type.clone())],
                Value::Null,
            ).await?;
            record_id = value
                .get("recordsets")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(|item| item.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        let body = json!({
            "name": fqdn,
            "type": record_type,
            "ttl": request.ttl.max(60),
            "records": [request.content]
        });
        if let Some(id) = record_id {
            call(config, "PUT", &format!("/v2/zones/{zone_id}/recordsets/{id}"), &[], body).await?;
            Ok(DdnsResult { zone_id, record_id: id })
        } else {
            let value = call(config, "POST", &format!("/v2/zones/{zone_id}/recordsets"), &[], body).await?;
            let id = value.get("id").and_then(Value::as_str).unwrap_or_default().to_string();
            Ok(DdnsResult { zone_id, record_id: id })
        }
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(HuaweiDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_metadata() {
        assert_eq!(HuaweiDriver.id(), "huaweicloud");
        assert!(HuaweiDriver.fields().iter().any(|f| f.key == "secret_key"));
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(HuaweiDriver.validate(&json!({})).await.is_err());
    }
    #[test]
    fn deterministic_signature() {
        let a = signature("secret", "GET", "/v2/zones", &[("name", "example.com".into())], "", "20260101T000000Z");
        let b = signature("secret", "GET", "/v2/zones", &[("name", "example.com".into())], "", "20260101T000000Z");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
    }
}
