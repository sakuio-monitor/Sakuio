//! Komari-compatible managed theme configuration declarations and values.
//! 兼容 Komari 的托管主题配置声明与值处理。

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const MANAGED: &str = "managed";

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ConfigItem {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub name: Value,
    #[serde(default)]
    pub help: Value,
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Value,
    #[serde(default)]
    pub options: String,
}

/// Returns the normalized configuration type (`managed` when omitted).
/// 返回规范化的配置类型（省略时视为 managed）。
pub fn configuration_type(configuration: &Value) -> String {
    configuration
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(MANAGED)
        .to_ascii_lowercase()
}

/// Parses declared managed items; non-managed configurations have no items.
/// 解析声明的托管配置项；非托管配置没有配置项。
pub fn managed_items(configuration: &Value) -> Result<Vec<ConfigItem>, String> {
    if configuration_type(configuration) != MANAGED {
        return Ok(Vec::new());
    }
    let data = configuration
        .get("data")
        .cloned()
        .unwrap_or(Value::Array(Vec::new()));
    let Value::Array(items) = data else {
        return Err("komari-theme.json: configuration.data must be an array".into());
    };
    items
        .into_iter()
        .map(|item| {
            serde_json::from_value::<ConfigItem>(item)
                .map_err(|e| format!("komari-theme.json: invalid configuration item: {e}"))
        })
        .collect()
}

/// Komari-compatible default for a declared item.
/// 与 Komari 一致的配置项默认值。
pub fn default_value(item: &ConfigItem) -> Value {
    if !item.default.is_null() {
        return item.default.clone();
    }
    match item.kind.as_str() {
        "select" => item
            .options
            .split(',')
            .map(str::trim)
            .find(|option| !option.is_empty())
            .map(|option| Value::String(option.to_string()))
            .unwrap_or_else(|| Value::String(String::new())),
        "number" => Value::from(0),
        "switch" => Value::Bool(false),
        "nodes" | "pingtasks" => Value::String("[]".into()),
        _ => Value::String(String::new()),
    }
}

/// Returns the display value: node/ping selections become typed arrays.
/// 返回展示值：节点/任务选择转换为类型化数组。
pub fn display_value(item: &ConfigItem, value: &Value) -> Value {
    match item.kind.as_str() {
        "nodes" => Value::Array(
            parse_array(value)
                .into_iter()
                .map(|item| Value::String(selection_string(&item)))
                .collect(),
        ),
        "pingtasks" => Value::Array(
            parse_array(value)
                .into_iter()
                .filter_map(|item| selection_u64(&item).map(Value::from))
                .collect(),
        ),
        "switch" => Value::Bool(value.as_bool().unwrap_or(false)),
        "number" => value.clone(),
        _ => match value {
            Value::String(_) => value.clone(),
            Value::Null => Value::String(String::new()),
            other => Value::String(other.to_string()),
        },
    }
}

/// Converts submitted values into the canonical storage form.
/// 把提交值转换为规范化的存储形式。
pub fn normalize_values(
    items: &[ConfigItem],
    input: &Map<String, Value>,
) -> Result<Map<String, Value>, String> {
    let mut output = Map::new();
    for item in items {
        if item.key.is_empty() || item.kind == "title" || item.kind == "textbox" {
            continue;
        }
        let value = input
            .get(&item.key)
            .cloned()
            .unwrap_or_else(|| default_value(item));
        let stored = normalize_item(item, &value)?;
        output.insert(item.key.clone(), stored);
    }
    Ok(output)
}

fn normalize_item(item: &ConfigItem, value: &Value) -> Result<Value, String> {
    let label = item.key.as_str();
    match item.kind.as_str() {
        "switch" => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| format!("{label}: expected a boolean")),
        "number" => value
            .as_f64()
            .map(|number| Value::from(number))
            .ok_or_else(|| format!("{label}: expected a number")),
        "select" => {
            let selected = value.as_str().unwrap_or("").trim().to_string();
            let options: Vec<&str> = item
                .options
                .split(',')
                .map(str::trim)
                .filter(|option| !option.is_empty())
                .collect();
            if !options.is_empty() && !options.contains(&selected.as_str()) {
                return Err(format!("{label}: value is not one of the declared options"));
            }
            if item.required && selected.is_empty() {
                return Err(format!("{label}: a value is required"));
            }
            Ok(Value::String(selected))
        }
        "nodes" => {
            let values = selection_array(value, label)?;
            let ids: Vec<String> = values.iter().map(selection_string).collect();
            if item.required && ids.is_empty() {
                return Err(format!("{label}: at least one node is required"));
            }
            Ok(Value::String(
                serde_json::to_string(&ids).unwrap_or_else(|_| "[]".into()),
            ))
        }
        "pingtasks" => {
            let values = selection_array(value, label)?;
            let ids: Vec<u64> = values.iter().filter_map(selection_u64).collect();
            if item.required && ids.is_empty() {
                return Err(format!("{label}: at least one ping task is required"));
            }
            Ok(Value::String(
                serde_json::to_string(&ids).unwrap_or_else(|_| "[]".into()),
            ))
        }
        "richtext" | "string" | "textbox" | "title" | "" => {
            let text = value.as_str().unwrap_or("").to_string();
            if item.required && text.trim().is_empty() {
                return Err(format!("{label}: a value is required"));
            }
            Ok(Value::String(text))
        }
        _ => {
            let text = value.as_str().unwrap_or("").to_string();
            if item.required && text.trim().is_empty() {
                return Err(format!("{label}: a value is required"));
            }
            Ok(Value::String(text))
        }
    }
}

fn selection_array(value: &Value, label: &str) -> Result<Vec<Value>, String> {
    match value {
        Value::Array(items) => Ok(items.clone()),
        Value::String(text) => {
            if text.trim().is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str::<Vec<Value>>(text)
                .map_err(|e| format!("{label}: invalid selection JSON: {e}"))
        }
        Value::Null => Ok(Vec::new()),
        _ => Err(format!("{label}: expected an array of selected ids")),
    }
}

fn parse_array(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items.clone(),
        Value::String(text) => serde_json::from_str::<Vec<Value>>(text).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn selection_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

fn selection_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

/// Merges stored values with declared defaults for display/public output.
/// 合并已存储值与声明默认值，供展示/公开输出使用。
pub fn merged_values(items: &[ConfigItem], saved: &Map<String, Value>) -> Map<String, Value> {
    let mut values = Map::new();
    for item in items {
        if item.key.is_empty() || item.kind == "title" || item.kind == "textbox" {
            continue;
        }
        let raw = saved
            .get(&item.key)
            .cloned()
            .unwrap_or_else(|| default_value(item));
        values.insert(item.key.clone(), display_value(item, &raw));
    }
    values
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(value: Value) -> ConfigItem {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn applies_type_defaults() {
        assert_eq!(
            default_value(&item(json!({"key":"s","type":"switch"}))),
            json!(false)
        );
        assert_eq!(
            default_value(&item(json!({"key":"n","type":"number"}))),
            json!(0)
        );
        assert_eq!(
            default_value(&item(json!({"key":"x","type":"select","options":"A,B"}))),
            json!("A")
        );
        assert_eq!(
            default_value(&item(json!({"key":"p","type":"pingtasks"}))),
            json!("[]")
        );
    }

    #[test]
    fn normalizes_selections_and_validates() {
        let items = vec![
            item(json!({"key":"nodes","type":"nodes","required":true})),
            item(json!({"key":"tasks","type":"pingtasks"})),
            item(json!({"key":"mode","type":"select","options":"Fast,Slow"})),
        ];
        let values = json!({
            "nodes": ["a", "b"],
            "tasks": [1, 2],
            "mode": "Fast"
        });
        let stored = normalize_values(&items, values.as_object().unwrap()).unwrap();
        assert_eq!(stored["nodes"], json!("[\"a\",\"b\"]"));
        assert_eq!(stored["tasks"], json!("[1,2]"));
        let display = merged_values(&items, &stored);
        assert_eq!(display["nodes"], json!(["a", "b"]));
        assert_eq!(display["tasks"], json!([1, 2]));

        let invalid = json!({"nodes": [], "mode": "Nope"});
        assert!(normalize_values(&items, invalid.as_object().unwrap()).is_err());
    }

    #[test]
    fn required_string_rejects_blank() {
        let items = vec![item(json!({"key":"label","type":"string","required":true}))];
        assert!(normalize_values(&items, json!({"label":""}).as_object().unwrap()).is_err());
        assert!(normalize_values(&items, json!({"label":"ok"}).as_object().unwrap()).is_ok());
    }
}
