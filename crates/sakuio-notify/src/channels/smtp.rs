use crate::registry::{NotificationChannel, NotifyError, NotifyResult};
use crate::types::{ChannelMeta, ConfigField, NotificationMessage};
use async_trait::async_trait;
use lettre::address::AddressError;
use lettre::message::header::ContentType;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::Deserialize;
use serde_json::json;

pub struct SmtpChannel;

#[derive(Debug, Deserialize)]
struct SmtpConfig {
    smtp_host: String,
    #[serde(default = "default_port")]
    smtp_port: u16,
    #[serde(default)]
    smtp_username: String,
    #[serde(default)]
    smtp_password: String,
    from_email: String,
    #[serde(default = "default_from_name")]
    from_name: String,
    #[serde(default = "default_true")]
    use_tls: bool,
    #[serde(default)]
    to_email: String,
}

fn default_port() -> u16 {
    465
}
fn default_from_name() -> String {
    "Sakuio".into()
}
fn default_true() -> bool {
    true
}

fn parse_mailbox(raw: &str) -> NotifyResult<Mailbox> {
    raw.parse::<Mailbox>()
        .map_err(|e: AddressError| NotifyError::InvalidConfig(e.to_string()))
}

impl SmtpChannel {
    pub fn meta_static() -> ChannelMeta {
        ChannelMeta {
            id: "smtp".into(),
            name: "SMTP 邮件".into(),
            fields: vec![
                field("smtp_host", "SMTP 服务器", "string", true, None),
                field("smtp_port", "端口", "number", true, Some(json!(465))),
                field("smtp_username", "用户名", "string", false, None),
                field("smtp_password", "密码", "password", false, None),
                field("from_email", "发件邮箱", "string", true, None),
                field(
                    "from_name",
                    "发件人名称",
                    "string",
                    false,
                    Some(json!("Sakuio")),
                ),
                field(
                    "use_tls",
                    "使用 TLS (465)",
                    "switch",
                    false,
                    Some(json!(true)),
                ),
                field("to_email", "默认收件邮箱", "string", false, Some(json!(""))),
            ],
        }
    }
}

fn field(
    key: &str,
    label: &str,
    field_type: &str,
    required: bool,
    default: Option<serde_json::Value>,
) -> ConfigField {
    ConfigField {
        key: key.into(),
        label: label.into(),
        field_type: field_type.into(),
        required,
        help: None,
        default,
    }
}

#[async_trait]
impl NotificationChannel for SmtpChannel {
    fn meta(&self) -> ChannelMeta {
        Self::meta_static()
    }

    async fn send(
        &self,
        config: &serde_json::Value,
        message: &NotificationMessage,
    ) -> NotifyResult<()> {
        let cfg: SmtpConfig = serde_json::from_value(config.clone())
            .map_err(|e| NotifyError::InvalidConfig(e.to_string()))?;
        let host = cfg.smtp_host.trim();
        if host.is_empty() {
            return Err(NotifyError::InvalidConfig("smtp_host is required".into()));
        }
        let to = cfg.to_email.trim();
        if to.is_empty() {
            return Err(NotifyError::InvalidConfig(
                "to_email is required for SMTP send".into(),
            ));
        }
        let from_email = cfg.from_email.trim();
        if from_email.is_empty() {
            return Err(NotifyError::InvalidConfig("from_email is required".into()));
        }

        let from = format!("{} <{}>", cfg.from_name.trim(), from_email);
        let from_mailbox = parse_mailbox(&from)?;
        let to_mailbox = parse_mailbox(to)?;
        let email = Message::builder()
            .from(from_mailbox)
            .to(to_mailbox)
            .subject(message.title.trim())
            .header(ContentType::TEXT_PLAIN)
            .body(message.body.clone())
            .map_err(|e| NotifyError::Send(e.to_string()))?;

        let creds = if !cfg.smtp_username.trim().is_empty() {
            Some(Credentials::new(
                cfg.smtp_username.trim().to_string(),
                cfg.smtp_password.clone(),
            ))
        } else {
            None
        };

        let mailer = if cfg.use_tls {
            let mut builder = AsyncSmtpTransport::<Tokio1Executor>::relay(host)
                .map_err(|e| NotifyError::Send(e.to_string()))?
                .port(cfg.smtp_port);
            if let Some(c) = creds {
                builder = builder.credentials(c);
            }
            builder.build()
        } else {
            let mut builder =
                AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host).port(cfg.smtp_port);
            if let Some(c) = creds {
                builder = builder.credentials(c);
            }
            builder.build()
        };

        mailer
            .send(email)
            .await
            .map_err(|e| NotifyError::Send(e.to_string()))?;
        Ok(())
    }
}
