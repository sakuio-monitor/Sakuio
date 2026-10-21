use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationMessage {
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigField {
    pub key: String,
    pub label: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelMeta {
    pub id: String,
    pub name: String,
    pub fields: Vec<ConfigField>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelListItem {
    pub id: String,
    pub name: String,
    pub fields: Vec<ConfigField>,
    #[serde(default)]
    pub enabled: bool,
}
