//! West.cn (西部数码) DNS driver.
//! 西部数码 DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const API: &str = "https://api.west.cn/api/v2/domain/";

struct WestcnDriver;

fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().to_string())
        .unwrap_or_default()
}

fn auth(form: &mut Vec<(String, String)>, username: &str, password: &str) {
    let time = timestamp();
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(format!("{username}{password}{time}"));
    let token = hex::encode(hasher.finalize());
    form.push(("username".into(), username.into()));
    form.push(("time".into(), time));
    form.push(("token".into(), token));
}

async fn decode(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status();
    let bytes = response.bytes().await.map_err(|error| error.to_string())?;
    let (text, _, _) = encoding_rs::GBK.decode(&bytes);
    if !status.is_success() {
        return Err(format!("西部数码 API 错误（{status}）：{}", text.chars().take(240).collect::<String>()));
    }
    serde_json::from_str(&text).map_err(|error| format!("解析西部数码响应失败：{error}"))
}

#[async_trait]
impl DdnsDriver for WestcnDriver {
    fn id(&self) -> &'static str { "westcn" }
    fn name(&self) -> &'static str { "西部数码" }
    fn description(&self) -> &'static str { "西部数码账号密码，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "username", label: "账号", kind: "text", required: true, placeholder: "西部数码账号", options: None },
            DriverField { key: "password", label: "密码", kind: "password", required: true, placeholder: "西部数码密码", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "username", "账号")?;
        common::required(config, "password", "密码")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let username = common::required(config, "username", "账号")?;
        let password = common::required(config, "password", "密码")?;
        let client = common::client()?;
        let zone = request.zone_name.trim().trim_end_matches('.');
        let host = request.record_name.trim();
        let host = if host.is_empty() || host == "@" { "@" } else { host };
        if let Some(id) = request.record_id.as_deref().filter(|id| !id.is_empty()) {
            let mut form = vec![("domain".into(), zone.into()), ("id".into(), id.into())];
            auth(&mut form, &username, &password);
            let _ = client.post(format!("{API}?act=deldnsrecord")).form(&form).send().await;
        }
        let mut form = vec![
            ("domain".into(), zone.into()),
            ("host".into(), host.into()),
            ("type".into(), request.record_type.to_ascii_uppercase()),
            ("value".into(), request.content.clone()),
            ("ttl".into(), request.ttl.max(60).to_string()),
        ];
        auth(&mut form, &username, &password);
        let response = client
            .post(format!("{API}?act=adddnsrecord"))
            .form(&form)
            .send()
            .await
            .map_err(|error| format!("连接西部数码失败：{error}"))?;
        let value = decode(response).await?;
        if value.get("result").and_then(Value::as_i64) != Some(200) {
            return Err(value.get("msg").and_then(Value::as_str).unwrap_or("西部数码添加解析失败").to_string());
        }
        let record_id = value
            .get("data")
            .and_then(|data| data.get("id"))
            .map(|id| id.to_string().trim_matches('"').to_string())
            .unwrap_or_default();
        Ok(DdnsResult { zone_id: zone.to_string(), record_id })
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(WestcnDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn driver_metadata() {
        assert_eq!(WestcnDriver.id(), "westcn");
        assert!(WestcnDriver.fields().iter().any(|f| f.key == "password"));
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(WestcnDriver.validate(&json!({})).await.is_err());
    }
}
