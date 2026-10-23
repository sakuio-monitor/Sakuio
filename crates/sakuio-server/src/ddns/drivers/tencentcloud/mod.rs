//! Tencent Cloud DNSPod driver.
//! 腾讯云 DNSPod 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const HOST: &str = "dnspod.tencentcloudapi.com";
const SERVICE: &str = "dnspod";
const VERSION: &str = "2021-03-23";

struct TencentDriver;

fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

async fn call(config: &Value, action: &str, payload: Value) -> Result<Value, String> {
    let secret_id = common::required(config, "secret_id", "SecretId")?;
    let secret_key = common::required(config, "secret_key", "SecretKey")?;
    let client = common::client()?;
    let body = serde_json::to_string(&payload).map_err(|error| error.to_string())?;
    let timestamp = chrono::Utc::now().timestamp();
    let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let canonical = format!(
        "POST\n/\n\ncontent-type:application/json; charset=utf-8\nhost:{HOST}\n\ncontent-type;host\n{}",
        hex::encode(Sha256::digest(body.as_bytes()))
    );
    let scope = format!("{date}/{SERVICE}/tc3_request");
    let string_to_sign = format!(
        "TC3-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let secret_date = hmac(format!("TC3{secret_key}").as_bytes(), &date);
    let secret_service = hmac(&secret_date, SERVICE);
    let secret_signing = hmac(&secret_service, "tc3_request");
    let signature = hex::encode(hmac(&secret_signing, &string_to_sign));
    let authorization = format!(
        "TC3-HMAC-SHA256 Credential={secret_id}/{scope}, SignedHeaders=content-type;host, Signature={signature}"
    );
    let response = client
        .post(format!("https://{HOST}"))
        .header("Authorization", authorization)
        .header("Content-Type", "application/json; charset=utf-8")
        .header("X-TC-Action", action)
        .header("X-TC-Version", VERSION)
        .header("X-TC-Timestamp", timestamp.to_string())
        .body(body)
        .send()
        .await
        .map_err(|error| format!("连接腾讯云 DNSPod 失败：{error}"))?;
    if !response.status().is_success() {
        return Err(common::error_message(response).await);
    }
    let value: Value = response.json().await.map_err(|error| format!("解析腾讯云响应失败：{error}"))?;
    if let Some(error) = value.get("Response").and_then(|response| response.get("Error")) {
        return Err(format!(
            "腾讯云 DNSPod 错误：{}",
            error.get("Message").and_then(Value::as_str).unwrap_or("未知错误")
        ));
    }
    Ok(value)
}

#[async_trait]
impl DdnsDriver for TencentDriver {
    fn id(&self) -> &'static str { "tencentcloud" }
    fn name(&self) -> &'static str { "腾讯云" }
    fn description(&self) -> &'static str { "腾讯云 SecretId / SecretKey，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "secret_id", label: "SecretId", kind: "text", required: true, placeholder: "腾讯云 SecretId", options: None },
            DriverField { key: "secret_key", label: "SecretKey", kind: "password", required: true, placeholder: "腾讯云 SecretKey", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "secret_id", "SecretId")?;
        common::required(config, "secret_key", "SecretKey")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let zone = request.zone_name.trim().trim_end_matches('.');
        let name = if request.record_name.trim().is_empty() { "@" } else { request.record_name.trim() };
        let record_type = request.record_type.to_ascii_uppercase();
        let mut id = request.record_id.clone().filter(|value| !value.is_empty());
        if id.is_none() {
            let value = call(config, "DescribeRecordList", json!({
                "Domain": zone, "Subdomain": name, "RecordType": record_type, "Limit": 100
            })).await?;
            id = value
                .get("Response")
                .and_then(|response| response.get("RecordList"))
                .and_then(Value::as_array)
                .and_then(|items| items.iter().find(|item| {
                    item.get("Name").and_then(Value::as_str) == Some(name)
                        && item.get("Type").and_then(Value::as_str) == Some(record_type.as_str())
                }))
                .and_then(|item| item.get("RecordId"))
                .map(|id| id.to_string());
        }
        if let Some(id) = id {
            let record_id: i64 = id.parse().map_err(|_| "腾讯云记录 ID 格式错误".to_string())?;
            call(config, "ModifyRecord", json!({
                "Domain": zone, "RecordId": record_id, "SubDomain": name,
                "RecordType": record_type, "RecordLine": "默认",
                "Value": request.content, "TTL": request.ttl.max(60)
            })).await?;
            Ok(DdnsResult { zone_id: zone.into(), record_id: record_id.to_string() })
        } else {
            let value = call(config, "CreateRecord", json!({
                "Domain": zone, "SubDomain": name, "RecordType": record_type,
                "RecordLine": "默认", "Value": request.content, "TTL": request.ttl.max(60)
            })).await?;
            let id = value
                .get("Response")
                .and_then(|response| response.get("RecordId"))
                .map(|id| id.to_string())
                .unwrap_or_default();
            Ok(DdnsResult { zone_id: zone.into(), record_id: id })
        }
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(TencentDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_metadata() {
        assert_eq!(TencentDriver.id(), "tencentcloud");
        assert!(TencentDriver.fields().iter().any(|f| f.key == "secret_key"));
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(TencentDriver.validate(&json!({})).await.is_err());
    }
}
