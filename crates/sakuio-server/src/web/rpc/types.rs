use serde::Deserialize;
use serde_json::Value;

pub const RPC_VERSION: &str = "2.0";
pub const PERMISSION_DENIED: i32 = -32041;

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: Option<String>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    pub id: Option<Value>,
}

impl JsonRpcRequest {
    pub fn validate(&self) -> Option<(i32, &'static str)> {
        if self.jsonrpc.as_deref() != Some(RPC_VERSION) {
            return Some((-32600, "invalid jsonrpc version"));
        }
        if self.method.trim().is_empty() {
            return Some((-32600, "method required"));
        }
        None
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }
}

pub fn parse_request_body(body: &[u8]) -> Result<Vec<JsonRpcRequest>, (i32, &'static str)> {
    if body.is_empty() {
        return Err((-32700, "parse error"));
    }
    let v: Value = serde_json::from_slice(body).map_err(|_| (-32700, "parse error"))?;
    if v.is_array() {
        let arr: Vec<JsonRpcRequest> =
            serde_json::from_value(v).map_err(|_| (-32700, "parse error"))?;
        return Ok(arr);
    }
    let one: JsonRpcRequest = serde_json::from_value(v).map_err(|_| (-32700, "parse error"))?;
    Ok(vec![one])
}

pub fn success_response(id: Option<Value>, result: Value) -> Value {
    serde_json::json!({
        "jsonrpc": RPC_VERSION,
        "result": result,
        "id": id,
    })
}

pub fn error_response(id: Option<Value>, code: i32, message: &str) -> Value {
    serde_json::json!({
        "jsonrpc": RPC_VERSION,
        "error": { "code": code, "message": message },
        "id": id,
    })
}
