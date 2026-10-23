//! Alibaba Cloud DNS (Aliyun) driver.
//! 阿里云 DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use base64::Engine;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json::Value;
use sha1::Sha1;
use std::collections::BTreeMap;
use std::sync::Arc;

const API: &str = "https://alidns.aliyuncs.com/";
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ').add(b'"').add(b'#').add(b'$').add(b'%').add(b'&')
    .add(b'\'').add(b'(').add(b')').add(b'*').add(b'+').add(b',')
    .add(b'/').add(b':').add(b';').add(b'<').add(b'=').add(b'>')
    .add(b'?').add(b'@').add(b'[').add(b'\\').add(b']').add(b'^')
    .add(b'`').add(b'{').add(b'|').add(b'}');

struct AliyunDriver;

fn encode(value: &str) -> String {
    utf8_percent_encode(value, ENCODE_SET).to_string()
}

fn sign(secret: &str, method: &str, params: &BTreeMap<String, String>) -> String {
    let canonical = params
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let string_to_sign = format!("{method}&%2F&{}", encode(&canonical));
    let mut mac = Hmac::<Sha1>::new_from_slice(format!("{secret}&").as_bytes()).expect("hmac");
    mac.update(string_to_sign.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

async fn call(config: &Value, action: &str, mut params: BTreeMap<String, String>) -> Result<Value, String> {
    let access_key = common::required(config, "access_key_id", "AccessKey ID")?;
    let secret = common::required(config, "access_key_secret", "AccessKey Secret")?;
    let client = common::client()?;
    params.insert("AccessKeyId".into(), access_key);
    params.insert("Action".into(), action.into());
    params.insert("Format".into(), "JSON".into());
    params.insert("SignatureMethod".into(), "HMAC-SHA1".into());
    params.insert("SignatureNonce".into(), uuid::Uuid::new_v4().to_string());
    params.insert("SignatureVersion".into(), "1.0".into());
    params.insert("Timestamp".into(), chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());
    params.insert("Version".into(), "2015-01-09".into());
    let signature = sign(&secret, "GET", &params);
    params.insert("Signature".into(), signature);
    let response = client
        .get(API)
        .query(&params)
        .send()
        .await
        .map_err(|error| format!("连接阿里云 DNS 失败：{error}"))?;
    if !response.status().is_success() {
        return Err(common::error_message(response).await);
    }
    response.json().await.map_err(|error| format!("解析阿里云响应失败：{error}"))
}

fn record_id(value: &Value, rr: &str, record_type: &str) -> Option<String> {
    value
        .get("DomainRecords")
        .and_then(|records| records.get("Record"))
        .and_then(Value::as_array)
        .and_then(|records| {
            records.iter().find(|record| {
                record.get("RR").and_then(Value::as_str) == Some(rr)
                    && record.get("Type").and_then(Value::as_str) == Some(record_type)
            })
        })
        .and_then(|record| record.get("RecordId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[async_trait]
impl DdnsDriver for AliyunDriver {
    fn id(&self) -> &'static str { "aliyun" }
    fn name(&self) -> &'static str { "阿里云" }
    fn description(&self) -> &'static str { "阿里云 AccessKey，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "access_key_id", label: "AccessKey ID", kind: "text", required: true, placeholder: "阿里云 AccessKey ID", options: None },
            DriverField { key: "access_key_secret", label: "AccessKey Secret", kind: "password", required: true, placeholder: "阿里云 AccessKey Secret", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "access_key_id", "AccessKey ID")?;
        common::required(config, "access_key_secret", "AccessKey Secret")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let zone = request.zone_name.trim().trim_end_matches('.');
        let rr = if request.record_name.trim().is_empty() { "@" } else { request.record_name.trim() };
        let record_type = request.record_type.to_ascii_uppercase();
        let mut id = request.record_id.clone().filter(|value| !value.is_empty());
        if id.is_none() {
            let mut params = BTreeMap::new();
            params.insert("DomainName".into(), zone.into());
            params.insert("RRKeyWord".into(), rr.into());
            params.insert("Type".into(), record_type.clone());
            params.insert("PageSize".into(), "100".into());
            let value = call(config, "DescribeDomainRecords", params).await?;
            id = record_id(&value, rr, &record_type);
        }
        let mut params = BTreeMap::new();
        params.insert("DomainName".into(), zone.into());
        params.insert("RR".into(), rr.into());
        params.insert("Type".into(), record_type);
        params.insert("Value".into(), request.content.clone());
        params.insert("TTL".into(), request.ttl.max(60).to_string());
        if let Some(id) = id {
            params.insert("RecordId".into(), id.clone());
            call(config, "UpdateDomainRecord", params).await?;
            Ok(DdnsResult { zone_id: zone.into(), record_id: id })
        } else {
            let value = call(config, "AddDomainRecord", params).await?;
            let id = value.get("RecordId").and_then(Value::as_str).unwrap_or_default().to_string();
            Ok(DdnsResult { zone_id: zone.into(), record_id: id })
        }
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(AliyunDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn encodes_and_metadata() {
        assert_eq!(encode("a b+c"), "a%20b%2Bc");
        assert_eq!(AliyunDriver.id(), "aliyun");
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(AliyunDriver.validate(&json!({})).await.is_err());
    }
}
