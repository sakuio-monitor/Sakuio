use crate::AppState;
use serde_json::{json, Value};

use super::admin_ping::{
    admin_add_ping_task, admin_delete_ping_task, admin_edit_ping_task, admin_get_all_ping_tasks,
    admin_order_ping_task,
};
use super::data::{
    common_get_nodes_latest_status, common_get_nodes_map, guest_me, param_uuid, public_nodes_list,
    public_settings, version_info,
};
use super::ping::public_ping_tasks;
use super::records::{
    common_get_node_recent_status, common_get_records, public_get_client_recent_records,
    public_get_ping_records, public_get_records_by_uuid,
};

/// Single JSON-RPC method dispatch for theme and admin clients.
pub async fn dispatch(
    state: &AppState,
    method: &str,
    params: Option<&Value>,
) -> Result<Value, (i32, &'static str)> {
    let method = alias_method(method);
    match method.as_str() {
        "rpc.ping" => Ok(Value::String("pong".into())),
        "rpc.version" => Ok(Value::String(super::types::RPC_VERSION.into())),
        "rpc.methods" | "rpc.getMethods" => Ok(json!(list_method_names())),
        "rpc.help" | "rpc.getHelp" => Ok(json!([])),
        "rpc.getVersion" => Ok(version_info()),

        "common:getPublicInfo" | "public:getPublicSettings" => Ok(public_settings(state).await),
        "common:getBackendVersion" | "common:getVersion" | "public:getVersion" => {
            Ok(version_info())
        }
        "common:getMe" | "public:getMe" => Ok(guest_me()),
        "common:getNodes" | "public:getNodesInformation" => {
            if method == "public:getNodesInformation" {
                public_nodes_list(state)
                    .await
                    .map_err(|c| (c, "Internal error"))
            } else {
                common_get_nodes_map(state, param_uuid(params).as_deref()).await
            }
        }
        "common:getNodesLatestStatus" => {
            common_get_nodes_latest_status(state, param_uuid(params).as_deref()).await
        }
        "common:getRecords" => common_get_records(state, params).await,
        "common:getNodeRecentStatus" => common_get_node_recent_status(state, params).await,
        "public:getClientRecentRecords" => public_get_client_recent_records(state, params).await,
        "public:getRecordsByUUID" => public_get_records_by_uuid(state, params).await,
        "public:getPingRecords" => public_get_ping_records(state, params).await,
        "public:getPublicPingTasks" => public_ping_tasks(state).await,

        "admin:getAllPingTasks" => admin_get_all_ping_tasks(state).await,
        "admin:addPingTask" => admin_add_ping_task(state, params).await,
        "admin:deletePingTask" => admin_delete_ping_task(state, params).await,
        "admin:editPingTask" => admin_edit_ping_task(state, params).await,
        "admin:orderPingTask" => admin_order_ping_task(state, params).await,

        _ => Err((-32601, "Method not found")),
    }
}

fn alias_method(method: &str) -> String {
    method.trim().to_string()
}

fn list_method_names() -> Vec<&'static str> {
    vec![
        "rpc.ping",
        "rpc.version",
        "rpc.methods",
        "rpc.help",
        "common:getNodes",
        "common:getNodesLatestStatus",
        "common:getPublicInfo",
        "common:getVersion",
        "common:getRecords",
        "common:getNodeRecentStatus",
        "common:getMe",
        "public:getMe",
        "public:getNodesInformation",
        "public:getPublicSettings",
        "public:getVersion",
        "public:getClientRecentRecords",
        "public:getRecordsByUUID",
        "public:getPingRecords",
        "public:getPublicPingTasks",
        "admin:getAllPingTasks",
        "admin:addPingTask",
        "admin:deletePingTask",
        "admin:editPingTask",
        "admin:orderPingTask",
    ]
}
