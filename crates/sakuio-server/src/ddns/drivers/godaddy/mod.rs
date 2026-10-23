//! GoDaddy DNS driver.
//! GoDaddy DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;

struct GodaddyDriver;

#[async_trait]
impl DdnsDriver for GodaddyDriver {
    fn id(&self) -> &'static str { "godaddy" }
    fn name(&self) -> &'static str { "GoDaddy" }
    fn description(&self) -> &'static str { "GoDaddy API Key / Secret，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "api_key", label: "API Key", kind: "password", required: true, placeholder: "GoDaddy API Key", options: None },
            DriverField { key: "api_secret", label: "API Secret", kind: "password", required: true, placeholder: "GoDaddy API Secret", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "api_key", "API Key")?;
        common::required(config, "api_secret", "API Secret")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let key = common::required(config, "api_key", "API Key")?;
        let secret = common::required(config, "api_secret", "API Secret")?;
        let client = common::client()?;
        let zone = request.zone_name.trim().trim_end_matches('.');
        let record_type = request.record_type.to_ascii_uppercase();
        if record_type != "A" && record_type != "AAAA" {
            return Err("GoDaddy 仅支持 A/AAAA".into());
        }
        let name = if request.record_name.trim().is_empty() { "@" } else { request.record_name.trim() };
        let body = json!([{
            "data": request.content,
            "name": name,
            "ttl": request.ttl.max(600),
        }]);
        let response = client
            .put(format!("https://api.godaddy.com/v1/domains/{zone}/records/{record_type}/{name}"))
            .header("Authorization", format!("sso-key {key}:{secret}"))
            .json(&body)
            .send()
            .await
            .map_err(|error| format!("连接 GoDaddy 失败：{error}"))?;
        if !response.status().is_success() {
            return Err(common::error_message(response).await);
        }
        Ok(DdnsResult { zone_id: zone.to_string(), record_id: String::new() })
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(GodaddyDriver) } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_metadata() {
        assert_eq!(GodaddyDriver.id(), "godaddy");
        assert!(GodaddyDriver.fields().iter().any(|field| field.key == "api_key"));
    }

    #[tokio::test]
    async fn requires_credentials() {
        assert!(GodaddyDriver.validate(&json!({})).await.is_err());
    }
}
