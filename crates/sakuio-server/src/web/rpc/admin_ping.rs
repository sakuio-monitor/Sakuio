use crate::AppState;
use sakuio_db::{task_to_public_json, PingRepository, PingTaskRow};
use serde_json::{json, Value};

fn db_err(_: sakuio_db::PingDbError) -> (i32, &'static str) {
    (-32603, "Internal error")
}

pub async fn admin_get_all_ping_tasks(state: &AppState) -> Result<Value, (i32, &'static str)> {
    let tasks = PingRepository::new(&state.db)
        .list_all()
        .await
        .map_err(db_err)?;
    let out: Vec<Value> = tasks.iter().map(task_to_public_json).collect();
    Ok(Value::Array(out))
}

pub async fn admin_add_ping_task(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let p = params.ok_or((-32602, "Invalid params"))?;
    let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("").trim();
    let target = p
        .get("target")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let task_type = p.get("type").and_then(|v| v.as_str()).unwrap_or("").trim();
    let interval = p.get("interval").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let default_on = p
        .get("default_on")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let clients: Vec<String> = p
        .get("clients")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    if name.is_empty() || target.is_empty() || task_type.is_empty() || interval == 0 {
        return Err((-32602, "name, target, type and interval are required"));
    }
    if !default_on && clients.is_empty() {
        return Err((-32602, "clients is required when default_on is false"));
    }

    let repo = PingRepository::new(&state.db);
    let task_id = repo
        .add(&clients, default_on, name, target, task_type, interval)
        .await
        .map_err(db_err)?;
    state.ping_scheduler.reload().await;
    Ok(json!({ "task_id": task_id }))
}

pub async fn admin_delete_ping_task(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let p = params.ok_or((-32602, "Invalid params"))?;
    let ids = parse_id_list(p.get("id").ok_or((-32602, "id is required"))?)?;
    if ids.is_empty() {
        return Err((-32602, "id is required"));
    }
    PingRepository::new(&state.db)
        .delete(&ids)
        .await
        .map_err(|e| match e {
            sakuio_db::PingDbError::Message(m) if m.contains("not found") => {
                (-32603, "Internal error")
            }
            _ => db_err(e),
        })?;
    state.ping_scheduler.reload().await;
    Ok(Value::Null)
}

pub async fn admin_edit_ping_task(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let p = params.ok_or((-32602, "Invalid params"))?;
    let tasks_val = p
        .get("tasks")
        .and_then(|v| v.as_array())
        .ok_or((-32602, "Invalid request data"))?;
    if tasks_val.is_empty() {
        return Err((-32602, "Invalid request data"));
    }
    let mut tasks = Vec::with_capacity(tasks_val.len());
    for item in tasks_val {
        let task: PingTaskRow =
            serde_json::from_value(item.clone()).map_err(|_| (-32602, "Invalid request data"))?;
        if task.id <= 0 {
            return Err((-32602, "Invalid request data"));
        }
        tasks.push(task);
    }
    let repo = PingRepository::new(&state.db);
    for task in &tasks {
        repo.update_task(task).await.map_err(db_err)?;
    }
    state.ping_scheduler.reload().await;
    Ok(Value::Null)
}

pub async fn admin_order_ping_task(
    state: &AppState,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let p = params.ok_or((-32602, "Invalid or missing request body"))?;
    let obj = p
        .as_object()
        .ok_or((-32602, "Invalid or missing request body"))?;
    if obj.is_empty() {
        return Ok(Value::Null);
    }
    let mut order = Vec::new();
    for (id_str, weight_val) in obj {
        if id_str == "jsonrpc" || id_str == "method" || id_str == "id" {
            continue;
        }
        let id: i64 = id_str.parse().map_err(|_| (-32602, "Invalid task id"))?;
        let weight = weight_val
            .as_i64()
            .ok_or((-32602, "Invalid or missing request body"))? as i32;
        order.push((id, weight));
    }
    if order.is_empty() {
        return Ok(Value::Null);
    }
    PingRepository::new(&state.db)
        .update_order_validated(&order)
        .await
        .map_err(db_err)?;
    state.ping_scheduler.reload().await;
    Ok(Value::Null)
}

fn parse_id_list(v: &Value) -> Result<Vec<i64>, (i32, &'static str)> {
    let arr = v.as_array().ok_or((-32602, "id is required"))?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let id = item
            .as_i64()
            .or_else(|| item.as_u64().map(|u| u as i64))
            .ok_or((-32602, "id is required"))?;
        if id > 0 {
            out.push(id);
        }
    }
    Ok(out)
}
