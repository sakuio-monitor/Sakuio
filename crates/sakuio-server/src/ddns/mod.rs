//! Modular authorization/DDNS drivers.
//! 可插拔的授权 API 与 DDNS 驱动。

mod api;
mod drivers;
mod zone;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

pub use api::{router, DdnsService};

/// One configurable field exposed by an authorization driver.
#[derive(Debug, Clone, Serialize)]
pub struct DriverField {
    pub key: &'static str,
    pub label: &'static str,
    pub kind: &'static str,
    pub required: bool,
    pub placeholder: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<DriverOption>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DriverOption {
    pub value: &'static str,
    pub label: &'static str,
}

/// DNS update request passed to a provider driver.
#[derive(Debug, Clone)]
pub struct DdnsUpdate {
    pub zone_name: String,
    pub record_name: String,
    pub record_type: String,
    pub content: String,
    pub proxied: bool,
    pub ttl: i32,
    pub zone_id: Option<String>,
    pub record_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DdnsResult {
    pub zone_id: String,
    pub record_id: String,
}

/// Authorization/DDNS provider contract. Drivers self-register through
/// `inventory`, so removing a driver module never requires a central match.
#[async_trait]
pub trait DdnsDriver: Send + Sync {
    fn id(&self) -> &'static str;
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
    fn fields(&self) -> Vec<DriverField>;

    /// Validates credentials before an authorization is persisted.
    async fn validate(&self, config: &Value) -> Result<(), String>;

    /// Creates or updates one DNS record.
    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String>;
}

pub struct DriverRegistration {
    pub factory: fn() -> Arc<dyn DdnsDriver>,
}

inventory::collect!(DriverRegistration);

pub fn all_drivers() -> Vec<Arc<dyn DdnsDriver>> {
    inventory::iter::<DriverRegistration>
        .into_iter()
        .map(|registration| (registration.factory)())
        .collect()
}

pub fn driver(id: &str) -> Option<Arc<dyn DdnsDriver>> {
    all_drivers().into_iter().find(|driver| driver.id() == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloudflare_driver_is_registered() {
        let drivers = all_drivers();
        assert!(drivers.iter().any(|driver| driver.id() == "cloudflare"));
    }

    #[test]
    fn unknown_driver_returns_none() {
        assert!(driver("not-installed").is_none());
    }
}
