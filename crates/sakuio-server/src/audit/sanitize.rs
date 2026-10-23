//! Request/response redaction for audit details.
//! 审计详情中的敏感字段脱敏与长度限制。

use serde_json::Value;

const MAX_DETAIL_CHARS: usize = 4000;
const MAX_STRING_CHARS: usize = 512;

fn sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("password")
        || key.contains("passwd")
        || key.contains("secret")
        || key.contains("token")
        || key.contains("authorization")
        || key.contains("cookie")
        || key == "key"
        || key == "code"
        || key == "two_fa_code"
        || key.contains("private_key")
        || key.contains("dsn")
        || key.contains("connection_string")
}

/// Redacts sensitive JSON keys recursively and bounds the serialized size.
/// 递归脱敏敏感 JSON 字段并限制序列化长度。
pub fn redact_json(bytes: &[u8]) -> Option<String> {
    let mut value: Value = serde_json::from_slice(bytes).ok()?;
    redact_value(&mut value);
    let mut text = serde_json::to_string(&value).ok()?;
    truncate(&mut text, MAX_DETAIL_CHARS);
    Some(text)
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, item) in map.iter_mut() {
                if sensitive_key(key) {
                    *item = Value::String("***".into());
                } else {
                    redact_value(item);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_value(item);
            }
        }
        Value::String(text) => {
            if text.chars().count() > MAX_STRING_CHARS {
                *text = text.chars().take(MAX_STRING_CHARS).collect::<String>() + "…";
            }
        }
        _ => {}
    }
}

/// Redacts sensitive query keys and truncates the result.
/// 脱敏查询参数中的敏感键并截断。
pub fn redact_query(query: &str) -> String {
    if query.trim().is_empty() {
        return String::new();
    }
    let mut pairs: Vec<String> = Vec::new();
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if sensitive_key(key) {
            pairs.push(format!("{key}=***"));
        } else {
            let mut value = value.to_string();
            truncate(&mut value, 256);
            pairs.push(format!("{key}={value}"));
        }
    }
    let mut text = pairs.join("&");
    truncate(&mut text, MAX_DETAIL_CHARS);
    text
}

/// Non-JSON bodies are summarized instead of stored.
/// 非 JSON 请求体仅记录摘要。
pub fn body_summary(content_type: &str, length: Option<u64>) -> String {
    match length {
        Some(length) => format!(
            "[{}] {} bytes",
            if content_type.is_empty() {
                "unknown"
            } else {
                content_type
            },
            length
        ),
        None => format!(
            "[{}] non-JSON body",
            if content_type.is_empty() {
                "unknown"
            } else {
                content_type
            }
        ),
    }
}

fn truncate(text: &mut String, max_chars: usize) {
    if text.chars().count() <= max_chars {
        return;
    }
    *text = text.chars().take(max_chars).collect::<String>() + "…";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_nested_secrets() {
        let detail = redact_json(
            br#"{"username":"admin","password":"p","token":"t","nested":{"secret":"s"},"safe":"ok"}"#,
        )
        .expect("json");
        assert!(detail.contains("\"username\":\"admin\""));
        assert!(detail.contains("\"safe\":\"ok\""));
        assert!(detail.contains("\"password\":\"***\""));
        assert!(detail.contains("\"token\":\"***\""));
        assert!(detail.contains("\"secret\":\"***\""));
        assert!(!detail.contains("\"p\""));
    }

    #[test]
    fn redacts_query_secrets() {
        let query = redact_query("token=abc&page=2&password=x");
        assert!(query.contains("token=***"));
        assert!(query.contains("page=2"));
        assert!(!query.contains("abc"));
    }
}
