use sakuio_board::BoardStateStore;
use sakuio_core::AgentCommandBus;
use sakuio_db::DbPool;
use sakuio_db::{PingRepository, PingTaskRow};
use futures_util::StreamExt;
use sakuio_protocol::{
    AgentPingBatchParams, AgentPingParams, ControlMessage, ControlOp, METHOD_AGENT_PING,
    METHOD_AGENT_PING_BATCH,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration, MissedTickBehavior};

pub struct PingScheduler {
    pool: DbPool,
    board: Arc<dyn BoardStateStore>,
    bus: AgentCommandBus,
    handle: Arc<RwLock<Option<JoinHandle<()>>>>,
}

use tokio::sync::RwLock;

impl PingScheduler {
    pub fn new(pool: DbPool, board: Arc<dyn BoardStateStore>, bus: AgentCommandBus) -> Self {
        Self {
            pool,
            board,
            bus,
            handle: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn reload(&self) {
        let mut guard = self.handle.write().await;
        if let Some(h) = guard.take() {
            h.abort();
        }
        let pool = self.pool.clone();
        let board = self.board.clone();
        let bus = self.bus.clone();
        let job = tokio::spawn(async move {
            if let Err(e) = run_ping_loops(pool, board, bus).await {
                tracing::error!(error = %e, "ping scheduler stopped");
            }
        });
        *guard = Some(job);
    }
}

async fn run_ping_loops(
    pool: DbPool,
    board: Arc<dyn BoardStateStore>,
    bus: AgentCommandBus,
) -> Result<(), String> {
    let repo = PingRepository::new(&pool);
    let tasks = repo.list_all().await.map_err(|e| e.to_string())?;
    let mut groups: HashMap<i32, Vec<PingTaskRow>> = HashMap::new();
    for task in tasks {
        if task.interval <= 0 {
            continue;
        }
        groups.entry(task.interval).or_default().push(task);
    }
    if groups.is_empty() {
        tracing::info!("ping scheduler: no tasks configured");
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
    for (interval_secs, tasks) in groups {
        let board = board.clone();
        let bus = bus.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs as u64));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            fire_group(&pool, &board, &bus, &tasks).await;
            loop {
                ticker.tick().await;
                fire_group(&pool, &board, &bus, &tasks).await;
            }
        });
    }
    loop {
        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}

async fn fire_group(
    pool: &DbPool,
    board: &Arc<dyn BoardStateStore>,
    bus: &AgentCommandBus,
    tasks: &[PingTaskRow],
) {
    let concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1);
    // Old agents do not understand agent.pingBatch; keep individual commands
    // for them so a protocol upgrade cannot silently lose ping samples.
    let versions: std::collections::HashMap<String, String> =
        sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT uuid, agent_version FROM clients",
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(uuid, version)| (uuid, version.unwrap_or_default()))
        .collect();

    let mut by_node: HashMap<String, Vec<AgentPingParams>> = HashMap::new();
    for task in tasks {
        for uuid in &task.clients {
            by_node.entry(uuid.clone()).or_default().push(AgentPingParams {
                task_id: task.id as u64,
                ping_type: task.task_type.clone(),
                target: task.target.clone(),
            });
        }
    }
    let versions = Arc::new(versions);
    let jobs: Vec<(String, Vec<AgentPingParams>)> = by_node.into_iter().collect();
    futures_util::stream::iter(jobs)
        .map(|(uuid, tasks)| {
            let versions = versions.clone();
            async move {
            let batch = versions
                .get(&uuid)
                .is_some_and(|version| supports_ping_batch(version));
            if batch {
                dispatch_ping_batch(board, bus, &uuid, tasks).await;
            } else {
                for task in tasks {
                    dispatch_ping(board, bus, &uuid, task).await;
                }
            }
        }
        })
        .buffer_unordered(concurrency)
        .for_each(|_| async {})
        .await;
}

fn supports_ping_batch(version: &str) -> bool {
    let mut parts = version.trim_start_matches('v').split('.');
    let major = parts.next().and_then(|v| v.parse::<u32>().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|v| v.parse::<u32>().ok()).unwrap_or(0);
    let patch = parts.next().and_then(|v| v.parse::<u32>().ok()).unwrap_or(0);
    (major, minor, patch) >= (0, 0, 8)
}

async fn dispatch_ping(
    board: &Arc<dyn BoardStateStore>,
    bus: &AgentCommandBus,
    node_uuid: &str,
    task: AgentPingParams,
) {
    let presence = board.resolve_presence(node_uuid).await.ok().flatten();
    if presence.is_none() {
        return;
    }
    let payload = serde_json::to_value(&task).unwrap_or_default();
    let push = ControlMessage::new(
        ControlOp::Cmd,
        serde_json::json!({
            "method": METHOD_AGENT_PING,
            "payload": payload,
        }),
    );
    if bus.push(node_uuid, push.clone()).await {
        return;
    }
    let _ = board
        .enqueue_command(node_uuid, METHOD_AGENT_PING, payload)
        .await;
}

async fn dispatch_ping_batch(
    board: &Arc<dyn BoardStateStore>,
    bus: &AgentCommandBus,
    node_uuid: &str,
    tasks: Vec<AgentPingParams>,
) {
    if tasks.is_empty() {
        return;
    }
    let presence = board.resolve_presence(node_uuid).await.ok().flatten();
    if presence.is_none() {
        return;
    }
    let payload = serde_json::to_value(AgentPingBatchParams { tasks }).unwrap_or_default();
    let push = ControlMessage::new(
        ControlOp::Cmd,
        serde_json::json!({
            "method": METHOD_AGENT_PING_BATCH,
            "payload": payload,
        }),
    );
    if bus.push(node_uuid, push.clone()).await {
        return;
    }
    let _ = board
        .enqueue_command(node_uuid, METHOD_AGENT_PING_BATCH, payload)
        .await;
}
