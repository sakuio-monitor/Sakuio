use crate::{DbError, DbKind, DbPool, DbResult};
use chrono::Utc;
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Row returned for public node listings.
/// 对外节点列表使用的行结构。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRecord {
    pub uuid: String,
    pub name: String,
    pub online: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_usage: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_used: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mem_total: Option<i64>,
}

/// Admin listing row including registration metadata.
/// 管理端列表行（含注册元数据）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminClientRecord {
    pub uuid: String,
    pub name: String,
    pub online: bool,
    pub disabled: bool,
    /// Agent bearer token (admin-only, used to render the install command).
    /// Agent 令牌（仅管理端，用于渲染安装命令）。
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub region: String,
    /// Country code resolved from the node's IPv6 ("" unknown).
    /// 由节点 IPv6 解析出的国家代码（"" 未知）。
    #[serde(default)]
    pub region6: String,
    pub ipv4: String,
    pub ipv6: String,
    pub agent_version: Option<String>,
    pub group_name: String,
    pub private_remark: String,
    pub price: f64,
    pub billing_cycle: i32,
    pub currency: String,
    pub expired_at: Option<String>,
    pub last_seen_at: Option<String>,
    pub created_at: String,
    /// Parsed agent `basic_info` (hardware / OS details), if reported.
    /// Agent 上报的 basic_info（硬件 / 系统信息），未上报时为空。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basic_info: Option<serde_json::Value>,
    /// Latest runtime snapshot subset (traffic totals, disk, uptime).
    /// 最新运行时快照子集（累计流量、磁盘、在线时长）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<serde_json::Value>,
    /// Auto-renew expiry when expired and online.
    /// 过期且在线时自动续费。
    #[serde(default)]
    pub auto_renewal: bool,
    /// Per-client traffic threshold in bytes (0 = disabled).
    /// 单节点流量阈值（字节，0 = 关闭）。
    #[serde(default)]
    pub traffic_threshold: i64,
    /// Traffic accounting mode: both / max / min / up / down.
    /// 流量统计方式：双向计费 / 取最大 / 取最小 / 仅上传 / 仅下载。
    #[serde(default)]
    pub traffic_stat_mode: String,
    /// Ledger-accumulated upload bytes for the current cycle.
    /// 当前计费周期账本累计的上传字节数。
    #[serde(default)]
    pub traffic_used_up: i64,
    /// Ledger-accumulated download bytes for the current cycle.
    /// 当前计费周期账本累计的下载字节数。
    #[serde(default)]
    pub traffic_used_down: i64,
    /// Current billing cycle start (RFC3339).
    /// 当前流量统计周期起点（RFC3339）。
    #[serde(default)]
    pub traffic_cycle_start: Option<String>,
    /// Traffic reset schedule: cycle / monthly / never.
    /// 流量重置方式：跟随计费周期 / 每月 1 日 / 不自动重置。
    #[serde(default)]
    pub traffic_reset_mode: String,
    /// Last reported cumulative upload bytes (survives board restarts).
    /// 最近一次上报的累计上传字节数（面板重启后仍可展示）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_net_total_up: Option<i64>,
    /// Last reported cumulative download bytes (survives board restarts).
    /// 最近一次上报的累计下载字节数（面板重启后仍可展示）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_net_total_down: Option<i64>,
    /// Last reported OS uptime seconds (survives board restarts).
    /// 最近一次上报的系统运行时长秒数（面板重启后仍可展示）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_uptime_secs: Option<i64>,
    /// Configured bandwidth value (unit in `bandwidth_unit`).
    /// 配置的带宽值（单位见 `bandwidth_unit`）。
    #[serde(default)]
    pub bandwidth: f64,
    /// Bandwidth unit symbol: Kbps / Mbps / Gbps / Tbps.
    /// 带宽单位符号：Kbps / Mbps / Gbps / Tbps。
    #[serde(default)]
    pub bandwidth_unit: String,
    /// Telemetry collection/report interval in milliseconds.
    /// 遥测采集/上报间隔（毫秒）。
    #[serde(default = "default_report_interval_ms")]
    pub report_interval_ms: i64,
}

fn default_report_interval_ms() -> i64 {
    1000
}

/// One page of admin client rows plus optional next cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminClientListPage {
    pub items: Vec<AdminClientRecord>,
    pub next_cursor: Option<String>,
    /// Total client count for pagination display.
    pub total: u64,
}

/// Traffic ledger state for one client (billing watcher input).
/// 单客户端的流量账本状态（账本 watcher 输入）。
#[derive(Debug, Clone)]
pub struct TrafficLedgerRow {
    pub uuid: String,
    pub name: String,
    pub online: bool,
    pub billing_cycle: i32,
    pub expired_at: Option<String>,
    pub auto_renewal: bool,
    pub traffic_threshold: i64,
    pub traffic_stat_mode: String,
    pub traffic_used_up: i64,
    pub traffic_used_down: i64,
    pub traffic_last_json: Option<String>,
    pub traffic_cycle_start: Option<String>,
    pub traffic_reset_mode: String,
}

/// Valid traffic stat modes accepted from admin input.
/// 管理端可提交的合法统计方式。
pub const TRAFFIC_STAT_MODES: [&str; 5] = ["both", "max", "min", "up", "down"];

/// Valid traffic reset schedules.
/// 流量重置方式合法值。
pub const TRAFFIC_RESET_MODES: [&str; 3] = ["cycle", "monthly", "never"];

/// Normalizes a stat mode string to a known value ("max" fallback).
/// 归一化统计方式字符串（非法值回落 "max"）。
pub fn normalize_stat_mode(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if TRAFFIC_STAT_MODES.contains(&trimmed.as_str()) {
        trimmed
    } else {
        "max".to_string()
    }
}

/// Normalizes a reset schedule string ("cycle" fallback).
/// 归一化流量重置方式（非法值回落 "cycle"）。
pub fn normalize_reset_mode(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if TRAFFIC_RESET_MODES.contains(&trimmed.as_str()) {
        trimmed
    } else {
        "cycle".to_string()
    }
}

/// Valid bandwidth unit symbols.
/// 带宽单位合法值。
pub const BANDWIDTH_UNITS: [&str; 4] = ["Kbps", "Mbps", "Gbps", "Tbps"];

/// Normalizes a bandwidth unit string ("Mbps" fallback).
/// 归一化带宽单位（非法值回落 "Mbps"）。
pub fn normalize_bandwidth_unit(raw: &str) -> String {
    let trimmed = raw.trim();
    for unit in BANDWIDTH_UNITS {
        if trimmed.eq_ignore_ascii_case(unit) {
            return unit.to_string();
        }
    }
    "Mbps".to_string()
}

/// Computes displayed traffic usage from ledger parts per stat mode.
/// 按统计方式由账本两部分计算展示用量。
pub fn traffic_used_by_mode(used_up: i64, used_down: i64, mode: &str) -> i64 {
    match mode {
        "both" => used_up.saturating_add(used_down),
        "min" => used_up.min(used_down),
        "up" => used_up,
        "down" => used_down,
        // "max"
        _ => used_up.max(used_down),
    }
}

/// Full client row for the public theme / visitor API (`common:getNodes`, `public:*`).
#[derive(Debug, Clone)]
pub struct PublicClientRow {
    pub uuid: String,
    pub name: String,
    pub sort_order: f64,
    pub online: bool,
    pub disabled: bool,
    pub ipv4: String,
    pub ipv6: String,
    /// Persisted IPv4 geolocation country code ("" unknown).
    /// 持久化的 IPv4 归属地国家代码（"" 未知）。
    pub region: String,
    /// Persisted IPv6 geolocation country code ("" unknown).
    /// 持久化的 IPv6 归属地国家代码（"" 未知）。
    pub region6: String,
    pub agent_version: Option<String>,
    pub group_name: String,
    pub private_remark: String,
    pub price: f64,
    pub billing_cycle: i32,
    pub currency: String,
    pub expired_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub basic_info_json: Option<String>,
    pub auto_renewal: bool,
    pub traffic_threshold: i64,
    pub traffic_stat_mode: String,
    pub traffic_used_up: i64,
    pub traffic_used_down: i64,
    pub traffic_reset_mode: String,
    /// Last reported cumulative upload bytes (survives board restarts).
    /// 最近一次上报的累计上传字节数（面板重启后仍可展示）。
    pub last_net_total_up: Option<i64>,
    /// Last reported cumulative download bytes (survives board restarts).
    /// 最近一次上报的累计下载字节数（面板重启后仍可展示）。
    pub last_net_total_down: Option<i64>,
    /// Last reported OS uptime seconds (survives board restarts).
    /// 最近一次上报的系统运行时长秒数（面板重启后仍可展示）。
    pub last_uptime_secs: Option<i64>,
    pub bandwidth: f64,
    pub bandwidth_unit: String,
}

fn encode_admin_list_cursor(sort_order: f64, created_at: &str, uuid: &str) -> String {
    format!("{sort_order}\x1f{created_at}\x1f{uuid}")
}

/// Returns (sort_order, created_at, uuid). Legacy two-part cursors (no sort
/// order) decode with sort_order = None and fall back to created_at filtering.
fn decode_admin_list_cursor(raw: &str) -> Option<(Option<f64>, String, String)> {
    let mut parts = raw.split('\x1f');
    let first = parts.next()?.trim().to_string();
    let created_at = parts.next()?.trim().to_string();
    let uuid = parts.next()?.trim().to_string();
    if created_at.is_empty() || uuid.is_empty() {
        return None;
    }
    let sort_order = first.parse::<f64>().ok();
    Some((sort_order, created_at, uuid))
}

/// Raw public list row (sqlx FromRow; tuple FromRow caps at 16 fields).
/// 公开列表原始行（sqlx FromRow；tuple FromRow 上限 16 字段）。
#[derive(Debug, sqlx::FromRow)]
struct PublicClientRowRaw {
    uuid: String,
    name: String,
    sort_order: f64,
    online: i64,
    disabled: i64,
    ipv4: String,
    ipv6: String,
    region: String,
    region6: String,
    agent_version: Option<String>,
    group_name: String,
    private_remark: String,
    price: f64,
    billing_cycle: i64,
    currency: String,
    expired_at: Option<String>,
    created_at: String,
    updated_at: String,
    basic_info_json: Option<String>,
    auto_renewal: i64,
    traffic_threshold: i64,
    traffic_stat_mode: String,
    traffic_used_up: i64,
    traffic_used_down: i64,
    traffic_reset_mode: String,
    last_net_total_up: Option<i64>,
    last_net_total_down: Option<i64>,
    last_uptime_secs: Option<i64>,
    bandwidth: f64,
    bandwidth_unit: String,
}

/// Raw admin list row (sqlx FromRow; tuple FromRow caps at 16 fields).
/// 管理端列表原始行（sqlx FromRow；tuple FromRow 上限 16 字段）。
#[derive(Debug, sqlx::FromRow)]
struct AdminClientRowRaw {
    uuid: String,
    name: String,
    token: String,
    online: i64,
    disabled: i64,
    ipv4: String,
    ipv6: String,
    region: String,
    region6: String,
    agent_version: Option<String>,
    group_name: String,
    private_remark: String,
    price: f64,
    billing_cycle: i64,
    currency: String,
    expired_at: Option<String>,
    last_seen_at: Option<String>,
    created_at: String,
    basic_info_json: Option<String>,
    auto_renewal: i64,
    traffic_threshold: i64,
    traffic_stat_mode: String,
    traffic_used_up: i64,
    traffic_used_down: i64,
    traffic_cycle_start: Option<String>,
    traffic_reset_mode: String,
    last_net_total_up: Option<i64>,
    last_net_total_down: Option<i64>,
    last_uptime_secs: Option<i64>,
    bandwidth: f64,
    bandwidth_unit: String,
    report_interval_ms: i64,
    sort_order: f64,
}

fn map_admin_client_row(row: AdminClientRowRaw) -> AdminClientRecord {
    AdminClientRecord {
        uuid: row.uuid,
        name: row.name,
        token: row.token,
        online: row.online != 0,
        disabled: row.disabled != 0,
        region: row.region,
        region6: row.region6,
        ipv4: row.ipv4,
        ipv6: row.ipv6,
        agent_version: row.agent_version,
        group_name: row.group_name,
        private_remark: row.private_remark,
        price: row.price,
        billing_cycle: i32::try_from(row.billing_cycle).unwrap_or(0),
        currency: row.currency,
        expired_at: row.expired_at,
        last_seen_at: row.last_seen_at,
        basic_info: row
            .basic_info_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        runtime: None,
        created_at: row.created_at,
        auto_renewal: row.auto_renewal != 0,
        traffic_threshold: row.traffic_threshold,
        traffic_stat_mode: row.traffic_stat_mode,
        traffic_used_up: row.traffic_used_up,
        traffic_used_down: row.traffic_used_down,
        traffic_cycle_start: row.traffic_cycle_start,
        traffic_reset_mode: row.traffic_reset_mode,
        last_net_total_up: row.last_net_total_up,
        last_net_total_down: row.last_net_total_down,
        last_uptime_secs: row.last_uptime_secs,
        bandwidth: row.bandwidth,
        bandwidth_unit: row.bandwidth_unit,
        report_interval_ms: row.report_interval_ms,
    }
}

/// Admin PATCH body (all fields optional).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AdminClientPatch {
    pub name: Option<String>,
    pub group_name: Option<String>,
    pub private_remark: Option<String>,
    pub price: Option<f64>,
    pub billing_cycle: Option<i32>,
    pub currency: Option<String>,
    pub expired_at: Option<String>,
    pub auto_renewal: Option<bool>,
    pub traffic_threshold: Option<i64>,
    pub traffic_stat_mode: Option<String>,
    pub traffic_reset_mode: Option<String>,
    pub bandwidth: Option<f64>,
    pub bandwidth_unit: Option<String>,
    /// Telemetry collection/report interval in milliseconds.
    /// 遥测采集/上报间隔（毫秒）。
    pub report_interval_ms: Option<i64>,
}

/// Parameters for registering a new agent client.
#[derive(Debug, Clone)]
pub struct NewClient {
    pub name: String,
    /// Optional client-supplied token (64 hex chars, unique); generated when absent.
    /// 可选的客户端预生成 token（64 位十六进制、唯一）；缺省时由服务端生成。
    pub token: Option<String>,
}

/// CRUD helpers for the `clients` table.
/// `clients` 表的 CRUD 辅助。
pub struct ClientRepository<'a> {
    pool: &'a DbPool,
}

fn public_row_from_raw(row: PublicClientRowRaw) -> PublicClientRow {
    PublicClientRow {
        uuid: row.uuid,
        name: row.name,
        sort_order: row.sort_order,
        online: row.online != 0,
        disabled: row.disabled != 0,
        ipv4: row.ipv4,
        ipv6: row.ipv6,
        region: row.region,
        region6: row.region6,
        agent_version: row.agent_version,
        group_name: row.group_name,
        private_remark: row.private_remark,
        price: row.price,
        billing_cycle: i32::try_from(row.billing_cycle).unwrap_or(0),
        currency: row.currency,
        expired_at: row.expired_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        basic_info_json: row.basic_info_json,
        auto_renewal: row.auto_renewal != 0,
        traffic_threshold: row.traffic_threshold,
        traffic_stat_mode: row.traffic_stat_mode,
        traffic_used_up: row.traffic_used_up,
        traffic_used_down: row.traffic_used_down,
        traffic_reset_mode: row.traffic_reset_mode,
        last_net_total_up: row.last_net_total_up,
        last_net_total_down: row.last_net_total_down,
        last_uptime_secs: row.last_uptime_secs,
        bandwidth: row.bandwidth,
        bandwidth_unit: row.bandwidth_unit,
    }
}

impl<'a> ClientRepository<'a> {
    /// Creates a repository bound to the given pool.
    /// 绑定到指定连接池的仓储实例。
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    /// Resolves node uuid from a bearer token (plain storage v1).
    /// 通过 bearer token 解析节点 uuid（v1 明文存储）。
    pub async fn uuid_by_token(&self, token: &str) -> DbResult<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT uuid FROM clients WHERE token = ? LIMIT 1")
                .bind(token)
                .fetch_optional(self.pool)
                .await?;
        Ok(row.map(|(u,)| u))
    }

    pub async fn token_by_uuid(&self, uuid: &str) -> DbResult<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT token FROM clients WHERE uuid = ? LIMIT 1")
                .bind(uuid)
                .fetch_optional(self.pool)
                .await?;
        Ok(row.map(|(t,)| t))
    }

    /// Inserts a client and returns uuid + token for the agent.
    /// 插入客户端并返回供 Agent 使用的 uuid 与 token。
    pub async fn insert(&self, new: &NewClient) -> DbResult<(String, String)> {
        let uuid = Uuid::new_v4().to_string();
        let token = new
            .token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(str::to_string)
            .unwrap_or_else(generate_token);
        let name = normalize_new_client_name(&new.name, &uuid);
        let now = Utc::now().to_rfc3339();
        // MySQL rejects reading the INSERT target from a subquery (error 1093),
        // so compute the next sort order first in the same transaction.
        // MySQL 禁止在 INSERT 的子查询里读取目标表（1093），
        // 因此在同一事务里先算下一个排序值。
        let mut tx = self.pool.begin().await?;
        let (max_order,): (Option<f64>,) = sqlx::query_as("SELECT MAX(sort_order) FROM clients")
            .fetch_one(&mut tx)
            .await?;
        let sort_order = max_order.unwrap_or(0.0) + 1.0;
        sqlx::query(
            "INSERT INTO clients (uuid, name, token, online, created_at, updated_at, sort_order)
             VALUES (?, ?, ?, 0, ?, ?, ?)",
        )
        .bind(&uuid)
        .bind(&name)
        .bind(&token)
        .bind(&now)
        .bind(&now)
        .bind(sort_order)
        .execute(&mut tx)
        .await?;
        tx.commit().await?;
        tracing::info!(node_uuid = %uuid, "client record created");
        Ok((uuid, token))
    }

    /// All clients with fields needed for public theme RPC.
    pub async fn list_for_public_api(&self) -> DbResult<Vec<PublicClientRow>> {
        let rows: Vec<PublicClientRowRaw> = sqlx::query_as(
            "SELECT uuid, name, COALESCE(sort_order, 1e18) AS sort_order, online, disabled, ipv4, ipv6, region, region6,
                    agent_version,
                    group_name, private_remark, price, billing_cycle, currency,
                    expired_at, created_at, updated_at, basic_info_json,
                    auto_renewal, traffic_threshold, traffic_stat_mode,
                    traffic_used_up, traffic_used_down, traffic_reset_mode,
                    last_net_total_up, last_net_total_down, last_uptime_secs,
                    bandwidth, bandwidth_unit
             FROM clients
             ORDER BY COALESCE(sort_order, 1e18) ASC, created_at ASC, uuid ASC",
        )
        .fetch_all(self.pool)
        .await?;
        Ok(rows.into_iter().map(public_row_from_raw).collect())
    }

    /// Loads one public client without scanning every node.
    pub async fn get_for_public_api(&self, uuid: &str) -> DbResult<Option<PublicClientRow>> {
        let row: Option<PublicClientRowRaw> = sqlx::query_as(
            "SELECT uuid, name, COALESCE(sort_order, 1e18) AS sort_order, online, disabled, ipv4, ipv6, region, region6,
                    agent_version,
                    group_name, private_remark, price, billing_cycle, currency,
                    expired_at, created_at, updated_at, basic_info_json,
                    auto_renewal, traffic_threshold, traffic_stat_mode,
                    traffic_used_up, traffic_used_down, traffic_reset_mode,
                    last_net_total_up, last_net_total_down, last_uptime_secs,
                    bandwidth, bandwidth_unit
             FROM clients WHERE uuid = ? LIMIT 1",
        )
        .bind(uuid)
        .fetch_optional(self.pool)
        .await?;
        Ok(row.map(public_row_from_raw))
    }

    /// Lists all clients for dashboard / public API.
    /// 列出全部客户端（管理台与公开 API）。
    pub async fn list(&self) -> DbResult<Vec<ClientRecord>> {
        let rows: Vec<(String, String, i64, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT uuid, name, online, agent_version, last_seen_at FROM clients ORDER BY COALESCE(sort_order, 1e18) ASC, created_at ASC",
        )
        .fetch_all(self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(uuid, name, online, agent_version, last_seen_at)| ClientRecord {
                    uuid,
                    name,
                    online: online != 0,
                    agent_version,
                    last_seen_at,
                    cpu_usage: None,
                    mem_used: None,
                    mem_total: None,
                },
            )
            .collect())
    }

    /// Admin list page (keyset / cursor pagination) with optional backend search.
    /// 管理端列表（键集游标分页），支持服务端搜索。
    pub async fn list_admin_page(
        &self,
        cursor: Option<&str>,
        limit: u32,
        search: Option<&str>,
    ) -> DbResult<AdminClientListPage> {
        let limit = limit.clamp(1, 100) as i64;
        let fetch_limit = limit + 1;
        let pattern = search
            .map(str::trim)
            .filter(|term| !term.is_empty())
            .map(|term| format!("%{term}%"));
        let search_clause = if pattern.is_some() {
            " AND (LOWER(name) LIKE LOWER(?)
                   OR LOWER(uuid) LIKE LOWER(?)
                   OR LOWER(ipv4) LIKE LOWER(?)
                   OR LOWER(ipv6) LIKE LOWER(?)
                   OR LOWER(group_name) LIKE LOWER(?)
                   OR LOWER(private_remark) LIKE LOWER(?))"
        } else {
            ""
        };

        let rows: Vec<AdminClientRowRaw> = if let Some((after_sort, after_created, after_uuid)) =
            cursor.and_then(decode_admin_list_cursor)
        {
            let sql = format!(
                "SELECT uuid, name, token, online, disabled, ipv4, ipv6, region, region6,
                        agent_version,
                        group_name, private_remark, price, billing_cycle, currency,
                        expired_at, last_seen_at, created_at, basic_info_json,
                        auto_renewal, traffic_threshold, traffic_stat_mode,
                        traffic_used_up, traffic_used_down, traffic_cycle_start,
                        traffic_reset_mode, last_net_total_up, last_net_total_down,
                        last_uptime_secs, bandwidth, bandwidth_unit, report_interval_ms,
                        COALESCE(sort_order, 1e18) AS sort_order
                 FROM clients
                 WHERE (COALESCE(sort_order, 1e18) > ?
                    OR (COALESCE(sort_order, 1e18) = ? AND created_at > ?)
                    OR (COALESCE(sort_order, 1e18) = ? AND created_at = ? AND uuid > ?))
                 {search_clause}
                 ORDER BY COALESCE(sort_order, 1e18) ASC, created_at ASC, uuid ASC
                 LIMIT ?"
            );
            let mut query = sqlx::query_as::<_, AdminClientRowRaw>(&sql)
                .bind(after_sort.unwrap_or(1e18))
                .bind(after_sort.unwrap_or(1e18))
                .bind(after_created.clone())
                .bind(after_sort.unwrap_or(1e18))
                .bind(after_created)
                .bind(after_uuid);
            if let Some(pattern) = &pattern {
                for _ in 0..6 {
                    query = query.bind(pattern);
                }
            }
            query.bind(fetch_limit).fetch_all(self.pool).await?
        } else {
            let sql = format!(
                "SELECT uuid, name, token, online, disabled, ipv4, ipv6, region, region6,
                        agent_version,
                        group_name, private_remark, price, billing_cycle, currency,
                        expired_at, last_seen_at, created_at, basic_info_json,
                        auto_renewal, traffic_threshold, traffic_stat_mode,
                        traffic_used_up, traffic_used_down, traffic_cycle_start,
                        traffic_reset_mode, last_net_total_up, last_net_total_down,
                        last_uptime_secs, bandwidth, bandwidth_unit, report_interval_ms,
                        COALESCE(sort_order, 1e18) AS sort_order
                 FROM clients
                 WHERE 1 = 1 {search_clause}
                 ORDER BY COALESCE(sort_order, 1e18) ASC, created_at ASC, uuid ASC
                 LIMIT ?"
            );
            let mut query = sqlx::query_as::<_, AdminClientRowRaw>(&sql);
            if let Some(pattern) = &pattern {
                for _ in 0..6 {
                    query = query.bind(pattern);
                }
            }
            query.bind(fetch_limit).fetch_all(self.pool).await?
        };

        let has_more = rows.len() > limit as usize;
        let page_rows: Vec<AdminClientRowRaw> = rows.into_iter().take(limit as usize).collect();
        let next_cursor = if has_more {
            page_rows
                .last()
                .map(|row| encode_admin_list_cursor(row.sort_order, &row.created_at, &row.uuid))
        } else {
            None
        };

        let count_sql = format!("SELECT COUNT(*) FROM clients WHERE 1 = 1{search_clause}");
        let mut count_query = sqlx::query_as::<_, (i64,)>(&count_sql);
        if let Some(pattern) = &pattern {
            for _ in 0..6 {
                count_query = count_query.bind(pattern);
            }
        }
        let total = count_query.fetch_one(self.pool).await?;

        Ok(AdminClientListPage {
            items: page_rows.into_iter().map(map_admin_client_row).collect(),
            next_cursor,
            total: total.0.max(0) as u64,
        })
    }

    /// Returns whether the client record is disabled.
    /// 客户端是否已禁用。
    pub async fn is_disabled(&self, uuid: &str) -> DbResult<bool> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT disabled FROM clients WHERE uuid = ? LIMIT 1")
                .bind(uuid)
                .fetch_optional(self.pool)
                .await?;
        Ok(row.map(|(d,)| d != 0).unwrap_or(true))
    }

    /// Updates last seen timestamp (RFC3339).
    /// 更新最后活跃时间（RFC3339）。
    pub async fn touch_last_seen(&self, uuid: &str) -> DbResult<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE clients SET last_seen_at = ?, updated_at = ? WHERE uuid = ?")
            .bind(now)
            .bind(Utc::now().to_rfc3339())
            .bind(uuid)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    /// Stores reported agent semver/build string.
    /// 保存 Agent 上报的版本字符串。
    pub async fn set_agent_version(&self, uuid: &str, version: &str) -> DbResult<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE clients SET agent_version = ?, updated_at = ? WHERE uuid = ?")
            .bind(version)
            .bind(now)
            .bind(uuid)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    /// Persists static host metadata as JSON.
    /// 将主机静态信息以 JSON 持久化。
    /// Denormalizes host fields from agent `basic_info` into list columns.
    pub async fn sync_host_from_basic_info(
        &self,
        uuid: &str,
        ipv4: Option<&str>,
        ipv6: Option<&str>,
        agent_version: Option<&str>,
    ) -> DbResult<()> {
        let ipv4 = ipv4.unwrap_or("").trim();
        let ipv6 = ipv6.unwrap_or("").trim();
        let version = agent_version.unwrap_or("").trim();
        let now = Utc::now().to_rfc3339();
        if version.is_empty() {
            sqlx::query("UPDATE clients SET ipv4 = ?, ipv6 = ?, updated_at = ? WHERE uuid = ?")
                .bind(ipv4)
                .bind(ipv6)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        } else {
            sqlx::query(
                "UPDATE clients SET ipv4 = ?, ipv6 = ?, agent_version = ?, updated_at = ? WHERE uuid = ?",
            )
            .bind(ipv4)
            .bind(ipv6)
            .bind(version)
            .bind(&now)
            .bind(uuid)
            .execute(self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn update_basic_info(&self, uuid: &str, basic: &serde_json::Value) -> DbResult<()> {
        let raw = serde_json::to_string(basic).map_err(|e| DbError::Message(e.to_string()))?;
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE clients SET basic_info_json = ?, updated_at = ? WHERE uuid = ?")
            .bind(raw)
            .bind(now)
            .bind(uuid)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    /// Renames a client when the admin left the default placeholder and the agent reported host metadata.
    /// 管理端未填自定义名称时，用 Agent 上报的主机信息自动命名。
    pub async fn apply_auto_name_from_basic_info(
        &self,
        uuid: &str,
        hostname: Option<&str>,
        os_label: Option<&str>,
    ) -> DbResult<()> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT name FROM clients WHERE uuid = ? LIMIT 1")
                .bind(uuid)
                .fetch_optional(self.pool)
                .await?;
        let Some((current,)) = row else {
            return Ok(());
        };
        if !is_default_client_name(&current, uuid) {
            return Ok(());
        }
        let new_name = hostname
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                os_label
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            });
        let Some(new_name) = new_name else {
            return Ok(());
        };
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE clients SET name = ?, updated_at = ? WHERE uuid = ?")
            .bind(&new_name)
            .bind(now)
            .bind(uuid)
            .execute(self.pool)
            .await?;
        tracing::info!(node_uuid = %uuid, name = %new_name, "client auto-named from basic_info");
        Ok(())
    }

    /// Partial update for admin UI (name, billing, remarks).
    pub async fn patch_admin(&self, uuid: &str, patch: &AdminClientPatch) -> DbResult<bool> {
        let exists: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM clients WHERE uuid = ? LIMIT 1")
            .bind(uuid)
            .fetch_optional(self.pool)
            .await?;
        if exists.is_none() {
            return Ok(false);
        }
        let now = Utc::now().to_rfc3339();
        if let Some(name) = &patch.name {
            let name = normalize_new_client_name(name, uuid);
            sqlx::query("UPDATE clients SET name = ?, updated_at = ? WHERE uuid = ?")
                .bind(name)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(group_name) = &patch.group_name {
            sqlx::query("UPDATE clients SET group_name = ?, updated_at = ? WHERE uuid = ?")
                .bind(group_name)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(private_remark) = &patch.private_remark {
            sqlx::query("UPDATE clients SET private_remark = ?, updated_at = ? WHERE uuid = ?")
                .bind(private_remark)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(price) = patch.price {
            sqlx::query("UPDATE clients SET price = ?, updated_at = ? WHERE uuid = ?")
                .bind(price)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(billing_cycle) = patch.billing_cycle {
            sqlx::query("UPDATE clients SET billing_cycle = ?, updated_at = ? WHERE uuid = ?")
                .bind(i64::from(billing_cycle))
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(currency) = &patch.currency {
            sqlx::query("UPDATE clients SET currency = ?, updated_at = ? WHERE uuid = ?")
                .bind(currency)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if patch.expired_at.is_some() {
            let expired = patch.expired_at.as_ref().and_then(|s| {
                let t = s.trim();
                if t.is_empty() {
                    None
                } else {
                    Some(t.to_string())
                }
            });
            sqlx::query("UPDATE clients SET expired_at = ?, updated_at = ? WHERE uuid = ?")
                .bind(expired)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(auto_renewal) = patch.auto_renewal {
            sqlx::query("UPDATE clients SET auto_renewal = ?, updated_at = ? WHERE uuid = ?")
                .bind(if auto_renewal { 1i64 } else { 0 })
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(threshold) = patch.traffic_threshold {
            sqlx::query("UPDATE clients SET traffic_threshold = ?, updated_at = ? WHERE uuid = ?")
                .bind(threshold)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(mode) = &patch.traffic_stat_mode {
            sqlx::query("UPDATE clients SET traffic_stat_mode = ?, updated_at = ? WHERE uuid = ?")
                .bind(normalize_stat_mode(mode))
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(mode) = &patch.traffic_reset_mode {
            sqlx::query("UPDATE clients SET traffic_reset_mode = ?, updated_at = ? WHERE uuid = ?")
                .bind(normalize_reset_mode(mode))
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(bandwidth) = patch.bandwidth {
            sqlx::query("UPDATE clients SET bandwidth = ?, updated_at = ? WHERE uuid = ?")
                .bind(bandwidth)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(unit) = &patch.bandwidth_unit {
            let unit = normalize_bandwidth_unit(unit);
            sqlx::query("UPDATE clients SET bandwidth_unit = ?, updated_at = ? WHERE uuid = ?")
                .bind(unit)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        if let Some(interval) = patch.report_interval_ms {
            let interval = interval.clamp(200, 3_600_000);
            sqlx::query("UPDATE clients SET report_interval_ms = ?, updated_at = ? WHERE uuid = ?")
                .bind(interval)
                .bind(&now)
                .bind(uuid)
                .execute(self.pool)
                .await?;
        }
        Ok(true)
    }

    /// Returns the configured telemetry interval for one client (milliseconds).
    /// 返回单个客户端配置的遥测间隔（毫秒）。
    pub async fn report_interval_ms(&self, uuid: &str) -> DbResult<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT report_interval_ms FROM clients WHERE uuid = ? LIMIT 1")
                .bind(uuid)
                .fetch_optional(self.pool)
                .await?;
        Ok(row.map(|(value,)| value))
    }

    /// Loads traffic ledger state for every client (billing watcher input).
    /// 读取全部客户端的流量账本状态（账本 watcher 输入）。
    pub async fn list_traffic_ledger(&self) -> DbResult<Vec<TrafficLedgerRow>> {
        type Row = (
            String,
            String,
            i64,
            i64,
            Option<String>,
            i64,
            i64,
            String,
            i64,
            i64,
            Option<String>,
            Option<String>,
            String,
        );
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT uuid, name, online, billing_cycle, expired_at,
                    auto_renewal, traffic_threshold, traffic_stat_mode,
                    traffic_used_up, traffic_used_down, traffic_last_json,
                    traffic_cycle_start, traffic_reset_mode
             FROM clients",
        )
        .fetch_all(self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    uuid,
                    name,
                    online,
                    billing_cycle,
                    expired_at,
                    auto_renewal,
                    traffic_threshold,
                    traffic_stat_mode,
                    traffic_used_up,
                    traffic_used_down,
                    traffic_last_json,
                    traffic_cycle_start,
                    traffic_reset_mode,
                )| TrafficLedgerRow {
                    uuid,
                    name,
                    online: online != 0,
                    billing_cycle: i32::try_from(billing_cycle).unwrap_or(0),
                    expired_at,
                    auto_renewal: auto_renewal != 0,
                    traffic_threshold,
                    traffic_stat_mode,
                    traffic_used_up,
                    traffic_used_down,
                    traffic_last_json,
                    traffic_cycle_start,
                    traffic_reset_mode,
                },
            )
            .collect())
    }

    /// Persists ledger accumulation and last-seen raw counters for one node.
    /// 持久化单节点的账本累计与上次原始读数。
    pub async fn update_traffic_ledger(
        &self,
        uuid: &str,
        used_up: i64,
        used_down: i64,
        last_json: &str,
        cycle_start: Option<&str>,
    ) -> DbResult<()> {
        sqlx::query(
            "UPDATE clients SET traffic_used_up = ?, traffic_used_down = ?,
                    traffic_last_json = ?, traffic_cycle_start = ?, updated_at = ?
             WHERE uuid = ?",
        )
        .bind(used_up)
        .bind(used_down)
        .bind(last_json)
        .bind(cycle_start)
        .bind(Utc::now().to_rfc3339())
        .bind(uuid)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Pushes a new expiry timestamp (auto-renewal).
    /// 写入新的到期时间（自动续费）。
    pub async fn set_expired_at(&self, uuid: &str, expired_at: &str) -> DbResult<()> {
        sqlx::query("UPDATE clients SET expired_at = ?, updated_at = ? WHERE uuid = ?")
            .bind(expired_at)
            .bind(Utc::now().to_rfc3339())
            .bind(uuid)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    /// Manual traffic reset: zeroes the ledger and starts a fresh cycle.
    /// 手动重置流量：账本清零并开启新周期。
    pub async fn reset_traffic(&self, uuid: &str) -> DbResult<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE clients SET traffic_used_up = 0, traffic_used_down = 0,
                    traffic_last_json = NULL, traffic_cycle_start = ?, updated_at = ?
             WHERE uuid = ?",
        )
        .bind(&now)
        .bind(&now)
        .bind(uuid)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Removes a client row by uuid.
    pub async fn delete_by_uuid(&self, uuid: &str) -> DbResult<bool> {
        let result = sqlx::query("DELETE FROM clients WHERE uuid = ?")
            .bind(uuid)
            .execute(self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Deletes many clients in one transaction, returns rows removed.
    /// 批量删除客户端（单事务），返回删除行数。
    pub async fn delete_by_uuids(&self, uuids: &[String]) -> DbResult<u64> {
        if uuids.is_empty() {
            return Ok(0);
        }
        let mut tx = self.pool.begin().await?;
        let mut removed = 0u64;
        for uuid in uuids {
            let result = sqlx::query("DELETE FROM clients WHERE uuid = ?")
                .bind(uuid)
                .execute(&mut tx)
                .await?;
            removed += result.rows_affected();
        }
        tx.commit().await?;
        Ok(removed)
    }

    /// Sets one group for many clients in one transaction.
    /// 批量设置分组（单事务），返回更新行数。
    pub async fn set_group_for_many(&self, uuids: &[String], group_name: &str) -> DbResult<u64> {
        if uuids.is_empty() {
            return Ok(0);
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        let mut updated = 0u64;
        for uuid in uuids {
            let result =
                sqlx::query("UPDATE clients SET group_name = ?, updated_at = ? WHERE uuid = ?")
                    .bind(group_name)
                    .bind(&now)
                    .bind(uuid)
                    .execute(&mut tx)
                    .await?;
            updated += result.rows_affected();
        }
        tx.commit().await?;
        Ok(updated)
    }

    /// Moves `uuid` before `before_uuid` (None = end), then rewrites 1..n sort
    /// order for every client. NULL sort_order clients keep sorting last by
    /// created_at until they are reordered.
    /// 拖拽排序：把 uuid 移到 before_uuid 之前（None = 末尾），随后全量重写序号。
    pub async fn reorder_client(&self, uuid: &str, before_uuid: Option<&str>) -> DbResult<()> {
        let mut ordered: Vec<String> = sqlx::query_scalar(
            "SELECT uuid FROM clients ORDER BY COALESCE(sort_order, 1e18) ASC, created_at ASC, uuid ASC",
        )
        .fetch_all(self.pool)
        .await?;
        if !ordered.iter().any(|u| u == uuid) {
            return Ok(());
        }
        let item = ordered.remove(ordered.iter().position(|u| u == uuid).unwrap());
        let target = match before_uuid {
            Some(before) if ordered.iter().any(|u| u == before) => {
                ordered.iter().position(|u| u == before).unwrap()
            }
            _ => ordered.len(),
        };
        ordered.insert(target, item);

        let mut tx = self.pool.begin().await?;
        for (idx, u) in ordered.iter().enumerate() {
            sqlx::query("UPDATE clients SET sort_order = ? WHERE uuid = ?")
                .bind((idx + 1) as f64)
                .bind(u)
                .execute(&mut tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Updates the cached online flag on disconnect / connect.
    /// 在连接/断开时更新缓存的 online flag。
    /// Best-effort node address for audit records (IPv4 preferred).
    /// 审计记录用的节点地址（优先 IPv4）。
    pub async fn address_by_uuid(&self, uuid: &str) -> DbResult<Option<String>> {
        let row: Option<(String, String)> =
            sqlx::query_as("SELECT ipv4, ipv6 FROM clients WHERE uuid = ? LIMIT 1")
                .bind(uuid)
                .fetch_optional(self.pool)
                .await?;
        Ok(row
            .map(|(ipv4, ipv6)| if !ipv4.trim().is_empty() { ipv4 } else { ipv6 })
            .filter(|address| !address.trim().is_empty()))
    }

    pub async fn set_online(&self, uuid: &str, online: bool) -> DbResult<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE clients SET online = ?, updated_at = ? WHERE uuid = ?")
            .bind(if online { 1i64 } else { 0 })
            .bind(now)
            .bind(uuid)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    /// One UPDATE for the telemetry hot path: last-seen timestamp plus the
    /// cumulative traffic/uptime snapshot. This removes one write per batch.
    pub async fn touch_last_seen_with_runtime(
        &self,
        uuid: &str,
        net_total_up: u64,
        net_total_down: u64,
        uptime_secs: u64,
    ) -> DbResult<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "UPDATE clients SET last_seen_at = ?, updated_at = ?, \
             last_net_total_up = ?, last_net_total_down = ?, last_uptime_secs = ? \
             WHERE uuid = ?",
        )
        .bind(now.clone())
        .bind(now)
        .bind(net_total_up as i64)
        .bind(net_total_down as i64)
        .bind(uptime_secs as i64)
        .bind(uuid)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Persists several last_seen/runtime updates in one transaction.
    pub async fn touch_last_seen_batch(&self, updates: &[(String, u64, u64, u64)]) -> DbResult<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = self.pool.begin().await?;
        for (uuid, net_total_up, net_total_down, uptime_secs) in updates {
            sqlx::query(
                "UPDATE clients SET last_seen_at = ?, updated_at = ?, \
                 last_net_total_up = ?, last_net_total_down = ?, last_uptime_secs = ? \
                 WHERE uuid = ?",
            )
            .bind(&now)
            .bind(&now)
            .bind(*net_total_up as i64)
            .bind(*net_total_down as i64)
            .bind(*uptime_secs as i64)
            .bind(uuid)
            .execute(&mut tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Persists the latest reported cumulative traffic and uptime so the
    /// dashboard can still show them after a board restart.
    /// 保存最近上报的累计流量与运行时长，面板重启后仍可展示断开前的最后数据。
    pub async fn update_last_runtime(
        &self,
        uuid: &str,
        net_total_up: u64,
        net_total_down: u64,
        uptime_secs: u64,
    ) -> DbResult<()> {
        sqlx::query(
            "UPDATE clients SET last_net_total_up = ?, last_net_total_down = ?, \
             last_uptime_secs = ? WHERE uuid = ?",
        )
        .bind(net_total_up as i64)
        .bind(net_total_down as i64)
        .bind(uptime_secs as i64)
        .bind(uuid)
        .execute(self.pool)
        .await?;
        Ok(())
    }

    /// Persists resolved geolocation country codes. `None` leaves the
    /// corresponding column untouched; callers pass `None` for failed or
    /// negative-cached lookups so previous values survive.
    /// 持久化解析出的归属地国家代码。`None` 表示该列不动；
    /// 解析失败 / 负缓存时调用方传 `None`，保留旧值。
    pub async fn update_geo_regions(
        &self,
        uuid: &str,
        region: Option<&str>,
        region6: Option<&str>,
    ) -> DbResult<()> {
        match (region, region6) {
            (Some(v4), Some(v6)) => {
                sqlx::query("UPDATE clients SET region = ?, region6 = ? WHERE uuid = ?")
                    .bind(v4)
                    .bind(v6)
                    .bind(uuid)
                    .execute(self.pool)
                    .await?;
            }
            (Some(v4), None) => {
                sqlx::query("UPDATE clients SET region = ? WHERE uuid = ?")
                    .bind(v4)
                    .bind(uuid)
                    .execute(self.pool)
                    .await?;
            }
            (None, Some(v6)) => {
                sqlx::query("UPDATE clients SET region6 = ? WHERE uuid = ?")
                    .bind(v6)
                    .bind(uuid)
                    .execute(self.pool)
                    .await?;
            }
            (None, None) => {}
        }
        Ok(())
    }

    /// Clears the online flag for every client. Called once at board startup,
    /// because no websocket session can exist before the process boots.
    /// 在面板启动时清除所有在线标记（进程启动前不可能存在任何活跃会话）。
    pub async fn set_all_offline(&self) -> DbResult<Vec<String>> {
        let now = Utc::now().to_rfc3339();
        // MySQL has no UPDATE ... RETURNING: select first, then update.
        // MySQL 不支持 UPDATE ... RETURNING：先查询，再更新。
        if self.pool.kind() == DbKind::MySql {
            let rows: Vec<(String,)> = sqlx::query_as("SELECT uuid FROM clients WHERE online = 1")
                .fetch_all(self.pool)
                .await?;
            sqlx::query("UPDATE clients SET online = 0, updated_at = ? WHERE online = 1")
                .bind(now)
                .execute(self.pool)
                .await?;
            return Ok(rows.into_iter().map(|(uuid,)| uuid).collect());
        }
        let rows = sqlx::query_scalar::<_, String>(
            "UPDATE clients SET online = 0, updated_at = ? \
             WHERE online = 1 RETURNING uuid",
        )
        .bind(now)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }

    /// Marks clients whose online flag is set but whose last heartbeat is
    /// older than `stale_secs` as offline. Returns the affected uuids so the
    /// caller can clean in-memory presence/snapshot state too.
    /// 兜底下线：online 仍为 1 但 last_seen_at 超过阈值的客户端置离线，
    /// 返回受影响的 uuid 列表供调用方同步清理内存状态。
    pub async fn mark_stale_offline(&self, stale_secs: i64) -> DbResult<Vec<String>> {
        let now = Utc::now().to_rfc3339();
        let cutoff = (Utc::now() - chrono::Duration::seconds(stale_secs)).to_rfc3339();
        // MySQL has no UPDATE ... RETURNING: select first, then update.
        // MySQL 不支持 UPDATE ... RETURNING：先查询，再更新。
        if self.pool.kind() == DbKind::MySql {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT uuid FROM clients WHERE online = 1 \
                 AND last_seen_at IS NOT NULL AND last_seen_at < ?",
            )
            .bind(&cutoff)
            .fetch_all(self.pool)
            .await?;
            sqlx::query(
                "UPDATE clients SET online = 0, updated_at = ? \
                 WHERE online = 1 AND last_seen_at IS NOT NULL AND last_seen_at < ?",
            )
            .bind(now)
            .bind(&cutoff)
            .execute(self.pool)
            .await?;
            return Ok(rows.into_iter().map(|(uuid,)| uuid).collect());
        }
        let rows = sqlx::query_scalar::<_, String>(
            "UPDATE clients SET online = 0, updated_at = ? \
             WHERE online = 1 AND last_seen_at IS NOT NULL AND last_seen_at < ? \
             RETURNING uuid",
        )
        .bind(now)
        .bind(cutoff)
        .fetch_all(self.pool)
        .await?;
        Ok(rows)
    }
}

/// Default list label before the agent connects (`Server-{uuid prefix}`).
/// Agent 连接前的默认列表名称。
pub fn default_client_name(uuid: &str) -> String {
    let prefix = uuid.get(..8).unwrap_or(uuid);
    format!("Server-{prefix}")
}

fn normalize_new_client_name(name: &str, uuid: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        default_client_name(uuid)
    } else {
        trimmed.to_string()
    }
}

pub fn is_default_client_name(name: &str, uuid: &str) -> bool {
    let trimmed = name.trim();
    trimmed.is_empty() || trimmed == uuid || trimmed == default_client_name(uuid)
}

fn generate_token() -> String {
    let mut bytes = [0u8; 32];
    thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}
