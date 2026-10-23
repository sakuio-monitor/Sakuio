//! Azure DNS driver.
//! Azure DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;

struct AzureDriver;

async fn token(config: &Value) -> Result<(String, String, String), String> {
    let tenant = common::required(config, "tenant_id", "Tenant ID")?;
    let client_id = common::required(config, "client_id", "Client ID")?;
    let secret = common::required(config, "client_secret", "Client Secret")?;
    let subscription = common::required(config, "subscription_id", "Subscription ID")?;
    let group = common::required(config, "resource_group", "资源组")?;
    let client = common::client()?;
    let response = client
        .post(format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"))
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", secret.as_str()),
            ("scope", "https://management.azure.com/.default"),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await
        .map_err(|error| format!("连接 Azure 登录失败：{error}"))?;
    if !response.status().is_success() {
        return Err(common::error_message(response).await);
    }
    let value: Value = response.json().await.map_err(|error| format!("解析 Azure 登录响应失败：{error}"))?;
    let access_token = value.get("access_token").and_then(Value::as_str).unwrap_or_default().to_string();
    if access_token.is_empty() {
        return Err("Azure 未返回 access_token".into());
    }
    Ok((access_token, subscription, group))
}

#[async_trait]
impl DdnsDriver for AzureDriver {
    fn id(&self) -> &'static str { "azure" }
    fn name(&self) -> &'static str { "Azure DNS" }
    fn description(&self) -> &'static str { "Azure 服务主体，自动维护 DNS Zone 的 A/AAAA 记录" }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField { key: "tenant_id", label: "Tenant ID", kind: "text", required: true, placeholder: "Azure Tenant ID", options: None },
            DriverField { key: "client_id", label: "Client ID", kind: "text", required: true, placeholder: "Azure Client ID", options: None },
            DriverField { key: "client_secret", label: "Client Secret", kind: "password", required: true, placeholder: "Azure Client Secret", options: None },
            DriverField { key: "subscription_id", label: "Subscription ID", kind: "text", required: true, placeholder: "Azure Subscription ID", options: None },
            DriverField { key: "resource_group", label: "资源组", kind: "text", required: true, placeholder: "DNS Zone 所在资源组", options: None },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "tenant_id", "Tenant ID")?;
        common::required(config, "client_id", "Client ID")?;
        common::required(config, "client_secret", "Client Secret")?;
        common::required(config, "subscription_id", "Subscription ID")?;
        common::required(config, "resource_group", "资源组")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let (access_token, subscription, group) = token(config).await?;
        let client = common::client()?;
        let zone = request.zone_name.trim().trim_end_matches('.');
        let record_type = request.record_type.to_ascii_uppercase();
        if record_type != "A" && record_type != "AAAA" {
            return Err("Azure DNS 仅支持 A/AAAA".into());
        }
        let name = if request.record_name.trim().is_empty() { "@" } else { request.record_name.trim() };
        let properties = if record_type == "A" {
            json!({"TTL": request.ttl.max(60), "ARecords": [{"ipv4Address": request.content}]})
        } else {
            json!({"TTL": request.ttl.max(60), "AAAARecords": [{"ipv6Address": request.content}]})
        };
        let url = format!(
            "https://management.azure.com/subscriptions/{subscription}/resourceGroups/{group}/providers/Microsoft.Network/dnsZones/{zone}/{record_type}/{name}?api-version=2018-05-01"
        );
        let response = client
            .put(url)
            .bearer_auth(access_token)
            .json(&json!({"properties": properties}))
            .send()
            .await
            .map_err(|error| format!("连接 Azure DNS 失败：{error}"))?;
        if !response.status().is_success() {
            return Err(common::error_message(response).await);
        }
        Ok(DdnsResult { zone_id: zone.to_string(), record_id: String::new() })
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(AzureDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn driver_metadata() {
        assert_eq!(AzureDriver.id(), "azure");
        assert_eq!(AzureDriver.fields().len(), 5);
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(AzureDriver.validate(&json!({})).await.is_err());
    }
}
