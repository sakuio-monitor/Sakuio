//! Volcengine DNS driver.
//! 火山引擎 DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const HOST: &str = "open.volcengineapi.com";
const REGION: &str = "cn-beijing";
const SERVICE: &str = "dns";
const VERSION: &str = "2018-08-01";
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

struct VolcengineDriver;

fn encode(value: &str) -> String {
    utf8_percent_encode(value, ENCODE_SET).to_string()
}

fn sign(secret: &str, date: &str, query: &[(&str, String)], body: &str) -> String {
    let mut pairs = query
        .iter()
        .map(|(k, v)| (encode(k), encode(v)))
        .collect::<Vec<_>>();
    pairs.sort();
    let canonical_query = pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let body_hash = hex::encode(Sha256::digest(body.as_bytes()));
    let canonical_headers = format!(
        "content-type:application/json\nhost:{HOST}\nx-content-sha256:{body_hash}\nx-date:{date}\n"
    );
    let signed_headers = "content-type;host;x-content-sha256;x-date";
    let canonical_request =
        format!("POST\n/\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{body_hash}");
    let date_only = &date[..8];
    let scope = format!("{date_only}/{REGION}/{SERVICE}/request");
    let string_to_sign = format!(
        "HMAC-SHA256\n{date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac");
    mac.update(date_only.as_bytes());
    let key_date = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_date).expect("hmac");
    mac.update(REGION.as_bytes());
    let key_region = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_region).expect("hmac");
    mac.update(SERVICE.as_bytes());
    let key_service = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_service).expect("hmac");
    mac.update(b"request");
    let key_signing = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_signing).expect("hmac");
    mac.update(string_to_sign.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn call(
    config: &Value,
    action: &str,
    mut query: Vec<(String, String)>,
    body: Value,
) -> Result<Value, String> {
    let ak = common::required(config, "access_key", "AccessKey")?;
    let sk = common::required(config, "secret_key", "SecretKey")?;
    let client = common::client()?;
    let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    query.push(("Action".into(), action.into()));
    query.push(("Version".into(), VERSION.into()));
    let payload = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    let query_ref: Vec<(&str, String)> =
        query.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    let signature = sign(&sk, &date, &query_ref, &payload);
    let body_hash = hex::encode(Sha256::digest(payload.as_bytes()));
    let authorization = format!(
        "HMAC-SHA256 Credential={ak}/{}/{REGION}/{SERVICE}/request, SignedHeaders=content-type;host;x-content-sha256;x-date, Signature={signature}",
        &date[..8]
    );
    let response = client
        .post(format!("https://{HOST}/"))
        .query(&query)
        .header("Authorization", authorization)
        .header("Content-Type", "application/json")
        .header("X-Date", date)
        .header("X-Content-Sha256", body_hash)
        .body(payload)
        .send()
        .await
        .map_err(|error| format!("连接火山引擎 DNS 失败：{error}"))?;
    if !response.status().is_success() {
        return Err(common::error_message(response).await);
    }
    let value: Value = response
        .json()
        .await
        .map_err(|error| format!("解析火山引擎响应失败：{error}"))?;
    if let Some(error) = value
        .get("ResponseMetadata")
        .and_then(|meta| meta.get("Error"))
    {
        if error
            .get("Code")
            .and_then(Value::as_str)
            .is_some_and(|code| !code.is_empty())
        {
            return Err(format!(
                "火山引擎 DNS 错误：{}",
                error
                    .get("Message")
                    .and_then(Value::as_str)
                    .unwrap_or("未知错误")
            ));
        }
    }
    Ok(value.get("Result").cloned().unwrap_or(value))
}

#[async_trait]
impl DdnsDriver for VolcengineDriver {
    fn id(&self) -> &'static str {
        "volcengine"
    }
    fn name(&self) -> &'static str {
        "火山引擎"
    }
    fn description(&self) -> &'static str {
        "火山引擎 AccessKey / SecretKey，自动维护 A/AAAA 记录"
    }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField {
                key: "access_key",
                label: "AccessKey",
                kind: "text",
                required: true,
                placeholder: "火山引擎 AccessKey",
                options: None,
            },
            DriverField {
                key: "secret_key",
                label: "SecretKey",
                kind: "password",
                required: true,
                placeholder: "火山引擎 SecretKey",
                options: None,
            },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "access_key", "AccessKey")?;
        common::required(config, "secret_key", "SecretKey")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let zone = request.zone_name.trim().trim_end_matches('.');
        let host = if request.record_name.trim().is_empty() {
            "@"
        } else {
            request.record_name.trim()
        };
        let record_type = request.record_type.to_ascii_uppercase();
        let mut zid = request.zone_id.clone().filter(|value| !value.is_empty());
        if zid.is_none() {
            let value = call(
                config,
                "ListZones",
                vec![
                    ("Key".into(), zone.into()),
                    ("PageSize".into(), "100".into()),
                ],
                Value::Null,
            )
            .await?;
            zid = value
                .get("Zones")
                .and_then(Value::as_array)
                .and_then(|items| {
                    items
                        .iter()
                        .find(|item| item.get("ZoneName").and_then(Value::as_str) == Some(zone))
                })
                .and_then(|item| item.get("ZID"))
                .map(|id| id.to_string());
        }
        let zid = zid.ok_or_else(|| format!("火山引擎中找不到域名 {zone}"))?;
        let mut record_id = request.record_id.clone().filter(|value| !value.is_empty());
        if record_id.is_none() {
            let value = call(
                config,
                "ListRecords",
                vec![
                    ("ZID".into(), zid.clone()),
                    ("Host".into(), host.into()),
                    ("Type".into(), record_type.clone()),
                    ("PageSize".into(), "100".into()),
                ],
                Value::Null,
            )
            .await?;
            record_id = value
                .get("Records")
                .and_then(Value::as_array)
                .and_then(|items| {
                    items.iter().find(|item| {
                        item.get("Host").and_then(Value::as_str) == Some(host)
                            && item.get("Type").and_then(Value::as_str)
                                == Some(record_type.as_str())
                    })
                })
                .and_then(|item| item.get("RecordID"))
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        if let Some(record_id) = record_id {
            call(
                config,
                "UpdateRecord",
                vec![],
                json!({
                    "RecordID": record_id, "Host": host, "Type": record_type,
                    "Value": request.content, "TTL": request.ttl.max(60), "Line": "default"
                }),
            )
            .await?;
            Ok(DdnsResult {
                zone_id: zid,
                record_id,
            })
        } else {
            let value = call(
                config,
                "CreateRecord",
                vec![],
                json!({
                    "ZID": zid.parse::<i64>().unwrap_or(0), "Host": host,
                    "Type": record_type, "Value": request.content,
                    "TTL": request.ttl.max(60), "Line": "default"
                }),
            )
            .await?;
            let id = value
                .get("RecordID")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            Ok(DdnsResult {
                zone_id: zid,
                record_id: id,
            })
        }
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(VolcengineDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_metadata() {
        assert_eq!(VolcengineDriver.id(), "volcengine");
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(VolcengineDriver.validate(&json!({})).await.is_err());
    }
    #[test]
    fn signature_stable() {
        let a = sign(
            "secret",
            "20260101T000000Z",
            &[("Action", "ListZones".into())],
            "",
        );
        assert_eq!(a.len(), 64);
    }
}
