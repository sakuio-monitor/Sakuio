use crate::registry::{NotifyError, NotifyResult, NotificationChannel};
use crate::types::{ChannelMeta, ConfigField, NotificationMessage};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

pub struct ServerChanChannel;

#[derive(Debug, Deserialize)]
struct ServerChanConfig {
    sendkey: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    openid: String,
}

impl ServerChanChannel {
    pub fn meta_static() -> ChannelMeta {
        ChannelMeta {
            id: "serverchan".into(),
            name: "Server酱 Turbo".into(),
            fields: vec![
                ConfigField {
                    key: "sendkey".into(),
                    label: "SendKey".into(),
                    field_type: "password".into(),
                    required: true,
                    help: Some("在 sct.ftqq.com 获取的 SendKey".into()),
                    default: None,
                },
                ConfigField {
                    key: "channel".into(),
                    label: "消息通道".into(),
                    field_type: "string".into(),
                    required: false,
                    help: Some("可选，多个用 | 分隔，例如 9|66".into()),
                    default: None,
                },
                ConfigField {
                    key: "openid".into(),
                    label: "抄送 openid".into(),
                    field_type: "string".into(),
                    required: false,
                    help: None,
                    default: None,
                },
            ],
        }
    }
}

#[async_trait]
impl NotificationChannel for ServerChanChannel {
    fn meta(&self) -> ChannelMeta {
        Self::meta_static()
    }

    async fn send(
        &self,
        config: &serde_json::Value,
        message: &NotificationMessage,
    ) -> NotifyResult<()> {
        let cfg: ServerChanConfig = serde_json::from_value(config.clone())
            .map_err(|e| NotifyError::InvalidConfig(e.to_string()))?;
        let sendkey = cfg.sendkey.trim();
        if sendkey.is_empty() {
            return Err(NotifyError::InvalidConfig("sendkey is required".into()));
        }
        let title = message.title.trim();
        let body = message.body.trim();
        if title.is_empty() && body.is_empty() {
            return Err(NotifyError::InvalidConfig("title and body are empty".into()));
        }

        let mut payload = json!({
            "title": title,
            "desp": body,
        });
        if !cfg.channel.trim().is_empty() {
            payload["channel"] = json!(cfg.channel.trim());
        }
        if !cfg.openid.trim().is_empty() {
            payload["openid"] = json!(cfg.openid.trim());
        }

        let api_url = format!("https://sctapi.ftqq.com/{}.send", sendkey);
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| NotifyError::Send(e.to_string()))?;
        let resp = client
            .post(api_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| NotifyError::Send(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(NotifyError::Send(format!(
                "HTTP {status}: {}",
                text.trim()
            )));
        }
        Ok(())
    }
}
