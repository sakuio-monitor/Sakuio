use sakuio_board::BoardStateStore;
use sakuio_core::AgentCommandBus;
use sakuio_db::DbPool;
use sakuio_db::{PingRepository, PingTaskRow};
use sakuio_protocol::{AgentPingParams, ControlMessage, ControlOp, METHOD_AGENT_PING};
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
            let fire = || async {
                for task in &tasks {
                    for uuid in &task.clients {
                        dispatch_ping(&pool, &board, &bus, uuid, task).await;
                    }
                }
            };
            fire().await;
            loop {
                ticker.tick().await;
                fire().await;
            }
        });
    }
    loop {
        tokio::time::sleep(Duration::from_secs(86400)).await;
    }
}

async fn dispatch_ping(
    _pool: &DbPool,
    board: &Arc<dyn BoardStateStore>,
    bus: &AgentCommandBus,
    node_uuid: &str,
    task: &PingTaskRow,
) {
    let presence = board.resolve_presence(node_uuid).await.ok().flatten();
    if presence.is_none() {
        return;
    }
    let payload = serde_json::to_value(AgentPingParams {
        task_id: task.id as u64,
        ping_type: task.task_type.clone(),
        target: task.target.clone(),
    })
    .unwrap_or_default();
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
