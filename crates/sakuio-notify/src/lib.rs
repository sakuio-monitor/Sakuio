mod channels;
pub mod geoip;
pub mod ipgeo;
mod registry;
mod types;

pub use registry::{NotificationChannel, NotifyError, NotifyResult, Registry};
pub use types::{ChannelListItem, ChannelMeta, ConfigField, NotificationMessage};

use std::sync::Arc;

/// Registers built-in notification channels (SMTP, Server酱).
pub async fn register_builtin(registry: &Registry) {
    registry.register(Arc::new(channels::SmtpChannel)).await;
    registry.register(Arc::new(channels::ServerChanChannel)).await;
}
