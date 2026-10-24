//! Cloudflare authorization and DNS record driver.
//! Cloudflare 授权与 DNS 记录驱动。

use super::super::{
    DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverOption, DriverRegistration,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const API: &str = "https://api.cloudflare.com/client/v4";

struct CloudflareDriver;

#[derive(Clone)]
enum AuthMode {
    Token(String),
    Global { email: String, key: String },
}

fn field<'a>(config: &'a Value, key: &str) -> &'a str {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
}

fn auth_mode(config: &Value) -> Result<AuthMode, String> {
    let mode = field(config, "auth_type");
    if mode == "global" {
        let email = field(config, "email");
        let key = field(config, "api_key");
        if email.is_empty() || key.is_empty() {
            return Err("Global API Key 方式需要同时填写账户邮箱和 API Key".to_string());
        }
        Ok(AuthMode::Global {
            email: email.to_string(),
            key: key.to_string(),
        })
    } else {
        let token = field(config, "token");
        if token.is_empty() {
            return Err("Cloudflare API Token 不能为空".to_string());
        }
        Ok(AuthMode::Token(token.to_string()))
    }
}

fn apply_auth(builder: reqwest::RequestBuilder, auth: &AuthMode) -> reqwest::RequestBuilder {
    match auth {
        AuthMode::Token(token) => builder.bearer_auth(token),
        AuthMode::Global { email, key } => builder
            .header("X-Auth-Email", email)
            .header("X-Auth-Key", key),
    }
}

async fn api_error(response: reqwest::Response) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| value.get("errors").cloned())
        .and_then(|errors| errors.as_array().cloned())
        .and_then(|errors| errors.first().cloned())
        .and_then(|error| error.get("message").cloned())
        .and_then(|message| message.as_str().map(str::to_string))
        .filter(|message| !message.is_empty());
    match message {
        Some(message) => format!("Cloudflare API 错误（{status}）：{message}"),
        None => format!("Cloudflare API 错误（{status}）"),
    }
}

fn http() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|error| format!("创建 HTTP 客户端失败：{error}"))
}

fn fqdn(zone_name: &str, record_name: &str) -> String {
    let zone = zone_name.trim().trim_end_matches('.').to_ascii_lowercase();
    let name = record_name.trim().trim_end_matches('.');
    if name.is_empty() || name == "@" {
        zone
    } else if name.to_ascii_lowercase().ends_with(&zone) {
        name.to_ascii_lowercase()
    } else {
        format!("{}.{}", name.to_ascii_lowercase(), zone)
    }
}

#[async_trait]
impl DdnsDriver for CloudflareDriver {
    fn id(&self) -> &'static str {
        "cloudflare"
    }

    fn name(&self) -> &'static str {
        "Cloudflare"
    }

    fn description(&self) -> &'static str {
        "Cloudflare DNS API Token 授权，用于 A/AAAA 记录自动更新"
    }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField {
                key: "auth_type",
                label: "授权方式",
                kind: "select",
                required: true,
                placeholder: "",
                options: Some(vec![
                    DriverOption {
                        value: "token",
                        label: "API Token",
                    },
                    DriverOption {
                        value: "global",
                        label: "Global API Key + 邮箱",
                    },
                ]),
            },
            DriverField {
                key: "token",
                label: "API Token",
                kind: "password",
                required: false,
                placeholder: "Cloudflare API Token（Zone:DNS:Edit）",
                options: None,
            },
            DriverField {
                key: "email",
                label: "账户邮箱",
                kind: "email",
                required: false,
                placeholder: "Global API Key 对应的 Cloudflare 账户邮箱",
                options: None,
            },
            DriverField {
                key: "api_key",
                label: "Global API Key",
                kind: "password",
                required: false,
                placeholder: "Cloudflare Global API Key",
                options: None,
            },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        let auth = auth_mode(config)?;
        let client = http()?;
        let request = match &auth {
            AuthMode::Token(_) => client.get(format!("{API}/user/tokens/verify")),
            AuthMode::Global { .. } => client.get(format!("{API}/user")),
        };
        let response = apply_auth(request, &auth)
            .send()
            .await
            .map_err(|error| format!("连接 Cloudflare 失败：{error}"))?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        let value: Value = response
            .json()
            .await
            .map_err(|error| format!("解析 Cloudflare 响应失败：{error}"))?;
        if value.get("success").and_then(Value::as_bool) != Some(true) {
            return Err("Cloudflare 授权验证失败".to_string());
        }
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let auth = auth_mode(config)?;
        let client = http()?;
        let zone_name = request.zone_name.trim().trim_end_matches('.');
        if zone_name.is_empty() {
            return Err("域名不能为空".to_string());
        }
        let zone_id = match request.zone_id.as_deref().filter(|value| !value.is_empty()) {
            Some(zone_id) => zone_id.to_string(),
            None => {
                let request = client
                    .get(format!("{API}/zones"))
                    .query(&[("name", zone_name)]);
                let response = apply_auth(request, &auth)
                    .send()
                    .await
                    .map_err(|error| format!("查询 Cloudflare Zone 失败：{error}"))?;
                if !response.status().is_success() {
                    return Err(api_error(response).await);
                }
                let value: Value = response
                    .json()
                    .await
                    .map_err(|error| format!("解析 Cloudflare Zone 响应失败：{error}"))?;
                value
                    .get("result")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("id"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| format!("Cloudflare 中找不到域名 {zone_name}"))?
            }
        };
        let name = fqdn(zone_name, &request.record_name);
        let record_type = request.record_type.to_ascii_uppercase();
        if record_type != "A" && record_type != "AAAA" {
            return Err("DDNS 记录类型仅支持 A 或 AAAA".to_string());
        }
        let payload = json!({
            "type": record_type,
            "name": name,
            "content": request.content,
            "ttl": request.ttl.max(1),
            "proxied": request.proxied,
        });
        let record_id = request
            .record_id
            .as_deref()
            .filter(|value| !value.is_empty());
        if let Some(record_id) = record_id {
            let request = client
                .put(format!("{API}/zones/{zone_id}/dns_records/{record_id}"))
                .json(&payload);
            let response = apply_auth(request, &auth)
                .send()
                .await
                .map_err(|error| format!("更新 Cloudflare 解析失败：{error}"))?;
            if !response.status().is_success() {
                return Err(api_error(response).await);
            }
            return Ok(DdnsResult {
                zone_id,
                record_id: record_id.to_string(),
            });
        }
        let request = client
            .get(format!("{API}/zones/{zone_id}/dns_records"))
            .query(&[("type", record_type.as_str()), ("name", name.as_str())]);
        let response = apply_auth(request, &auth)
            .send()
            .await
            .map_err(|error| format!("查询 Cloudflare 解析失败：{error}"))?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        let value: Value = response
            .json()
            .await
            .map_err(|error| format!("解析 Cloudflare 解析响应失败：{error}"))?;
        if let Some(existing) = value
            .get("result")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("id"))
            .and_then(Value::as_str)
        {
            let request = client
                .put(format!("{API}/zones/{zone_id}/dns_records/{existing}"))
                .json(&payload);
            let response = apply_auth(request, &auth)
                .send()
                .await
                .map_err(|error| format!("更新 Cloudflare 解析失败：{error}"))?;
            if !response.status().is_success() {
                return Err(api_error(response).await);
            }
            return Ok(DdnsResult {
                zone_id,
                record_id: existing.to_string(),
            });
        }
        let request = client
            .post(format!("{API}/zones/{zone_id}/dns_records"))
            .json(&payload);
        let response = apply_auth(request, &auth)
            .send()
            .await
            .map_err(|error| format!("创建 Cloudflare 解析失败：{error}"))?;
        if !response.status().is_success() {
            return Err(api_error(response).await);
        }
        let value: Value = response
            .json()
            .await
            .map_err(|error| format!("解析 Cloudflare 响应失败：{error}"))?;
        let record_id = value
            .get("result")
            .and_then(|result| result.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| "Cloudflare 创建解析成功但未返回记录 ID".to_string())?;
        Ok(DdnsResult { zone_id, record_id })
    }
}

inventory::submit! {
    DriverRegistration { factory: || Arc::new(CloudflareDriver) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_root_record_to_zone() {
        assert_eq!(fqdn("Example.COM", "@"), "example.com");
        assert_eq!(fqdn("example.com", ""), "example.com");
    }

    #[test]
    fn joins_subdomain_record_to_zone() {
        assert_eq!(fqdn("example.com", "www"), "www.example.com");
        assert_eq!(fqdn("example.com", "cdn.www"), "cdn.www.example.com");
    }

    #[test]
    fn avoids_duplicating_full_record_name() {
        assert_eq!(fqdn("example.com", "www.example.com"), "www.example.com");
    }

    #[test]
    fn defaults_to_api_token_mode() {
        let mode = auth_mode(&json!({ "token": "abc" })).expect("token mode");
        assert!(matches!(mode, AuthMode::Token(token) if token == "abc"));
    }

    #[test]
    fn supports_global_api_key_with_email() {
        let mode = auth_mode(&json!({
            "auth_type": "global",
            "email": "user@example.com",
            "api_key": "global-key"
        }))
        .expect("global mode");
        match mode {
            AuthMode::Global { email, key } => {
                assert_eq!(email, "user@example.com");
                assert_eq!(key, "global-key");
            }
            AuthMode::Token(_) => panic!("expected global mode"),
        }
    }

    #[test]
    fn rejects_empty_token() {
        assert!(auth_mode(&json!({})).is_err());
        assert!(auth_mode(&json!({ "auth_type": "token", "token": "  " })).is_err());
    }

    #[test]
    fn rejects_incomplete_global_credentials() {
        assert!(auth_mode(&json!({ "auth_type": "global", "email": "user@example.com" })).is_err());
        assert!(auth_mode(&json!({ "auth_type": "global", "api_key": "key" })).is_err());
    }

    #[test]
    fn exposes_both_authorization_modes() {
        let fields = CloudflareDriver.fields();
        let mode = fields
            .iter()
            .find(|field| field.key == "auth_type")
            .expect("auth_type field");
        let options = mode.options.as_ref().expect("options");
        assert!(options.iter().any(|option| option.value == "token"));
        assert!(options.iter().any(|option| option.value == "global"));
    }
}
