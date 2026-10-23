//! Shared helpers for DNS drivers.
//! DNS 驱动共用辅助函数。

use reqwest::Response;
use serde_json::Value;
use std::time::Duration;

pub fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .map_err(|error| format!("创建 HTTP 客户端失败：{error}"))
}

pub fn text(config: &Value, key: &str) -> String {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string()
}

pub fn required(config: &Value, key: &str, label: &str) -> Result<String, String> {
    let value = text(config, key);
    if value.is_empty() {
        Err(format!("{label}不能为空"))
    } else {
        Ok(value)
    }
}

pub async fn error_message(response: Response) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| {
                    value
                        .get("error")
                        .and_then(|error| error.get("message"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .or_else(|| {
                    value
                        .get("errors")
                        .and_then(Value::as_array)
                        .and_then(|items| items.first())
                        .and_then(|item| item.get("message"))
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
        })
        .filter(|message| !message.is_empty());
    match message {
        Some(message) => format!("DNS API 错误（{status}）：{message}"),
        None => format!("DNS API 错误（{status}）：{}", body.chars().take(240).collect::<String>()),
    }
}

