//! Spaceship DNS driver.
//! Spaceship DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;

struct SpaceshipDriver;

#[async_trait]
impl DdnsDriver for SpaceshipDriver {
    fn id(&self) -> &'static str { "spaceship" }
    fn name(&self) -> &'static str { "Spaceship" }
    fn description(&self) -> &'static str { "Spaceship API Key / Secret，自动维护 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "api_key", label: "API Key", kind: "password", required: true, placeholder: "Spaceship API Key", options: None },
            DriverField { key: "api_secret", label: "API Secret", kind: "password", required: true, placeholder: "Spaceship API Secret", options: None },
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
        let name = if request.record_name.trim().is_empty() { "@" } else { request.record_name.trim() };
        let body = json!({ "items": [{
            "type": request.record_type.to_ascii_uppercase(),
            "name": name,
            "value": request.content,
            "ttl": request.ttl.max(60),
        }]});
        let response = client
            .put(format!("https://spaceship.dev/api/v1/dns/records/{zone}"))
            .header("X-Api-Key", key)
            .header("X-Api-Secret", secret)
            .json(&body)
            .send()
            .await
            .map_err(|error| format!("连接 Spaceship 失败：{error}"))?;
        if !response.status().is_success() {
            return Err(common::error_message(response).await);
        }
        Ok(DdnsResult { zone_id: zone.to_string(), record_id: String::new() })
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(SpaceshipDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_metadata() {
        assert_eq!(SpaceshipDriver.id(), "spaceship");
        assert!(SpaceshipDriver.fields().iter().any(|f| f.key == "api_secret"));
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(SpaceshipDriver.validate(&json!({})).await.is_err());
    }
}
