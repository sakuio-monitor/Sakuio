//! In-memory theme install/update jobs with SSE progress fan-out.
//! 内存中的主题安装/更新任务，通过 SSE 广播进度。

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};

/// One progress frame sent to the admin UI.
/// 发送给管理端的一帧进度事件。
#[derive(Debug, Clone, Serialize)]
pub struct ThemeJobEvent {
    pub job_id: String,
    pub short: String,
    pub kind: String,
    /// resolving | downloading | installing | done | error
    pub phase: String,
    /// running | done | error
    pub status: String,
    pub downloaded: u64,
    pub total: Option<u64>,
    pub speed_bps: u64,
    pub percent: Option<f64>,
    pub message: Option<String>,
    /// True when an update actually replaced files; false when already current.
    pub updated: Option<bool>,
}

#[derive(Clone)]
pub struct ThemeJob {
    id: String,
    short: String,
    kind: String,
    state: Arc<Mutex<ThemeJobEvent>>,
    tx: broadcast::Sender<ThemeJobEvent>,
    created_at: Instant,
}

impl ThemeJob {
    fn new(id: String, short: String, kind: String) -> Self {
        let event = ThemeJobEvent {
            job_id: id.clone(),
            short: short.clone(),
            kind: kind.clone(),
            phase: "queued".into(),
            status: "running".into(),
            downloaded: 0,
            total: None,
            speed_bps: 0,
            percent: None,
            message: None,
            updated: None,
        };
        let (tx, _) = broadcast::channel(128);
        Self {
            id,
            short,
            kind,
            state: Arc::new(Mutex::new(event)),
            tx,
            created_at: Instant::now(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn short(&self) -> &str {
        &self.short
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn snapshot(&self) -> ThemeJobEvent {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn is_finished(&self) -> bool {
        self.snapshot().status != "running"
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ThemeJobEvent> {
        self.tx.subscribe()
    }

    pub fn set_phase(&self, phase: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.phase = phase.to_string();
        let event = state.clone();
        drop(state);
        let _ = self.tx.send(event);
    }

    pub fn set_progress(&self, downloaded: u64, total: Option<u64>, speed_bps: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.phase = "downloading".into();
        state.downloaded = downloaded;
        state.total = total;
        state.speed_bps = speed_bps;
        state.percent = total
            .filter(|total| *total > 0)
            .map(|total| (downloaded as f64 / total as f64 * 100.0).clamp(0.0, 100.0));
        let event = state.clone();
        drop(state);
        let _ = self.tx.send(event);
    }

    pub fn finish(&self, updated: Option<bool>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.status = "done".into();
        state.phase = "done".into();
        state.percent = Some(100.0);
        state.speed_bps = 0;
        state.updated = updated;
        state.message = None;
        let event = state.clone();
        drop(state);
        let _ = self.tx.send(event);
    }

    pub fn fail(&self, message: impl Into<String>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.status = "error".into();
        state.phase = "error".into();
        state.speed_bps = 0;
        state.message = Some(message.into());
        let event = state.clone();
        drop(state);
        let _ = self.tx.send(event);
    }
}

/// Registry of live/recent theme jobs.
/// 活动/近期主题任务注册表。
#[derive(Default)]
pub struct ThemeJobs {
    inner: RwLock<HashMap<String, ThemeJob>>,
}

impl ThemeJobs {
    /// Returns a running job for `short`, or creates a new one.
    /// 返回 `short` 的进行中任务，或创建新任务。
    pub async fn create_or_reuse(&self, short: &str, kind: &str) -> ThemeJob {
        let mut jobs = self.inner.write().await;
        let now = Instant::now();
        jobs.retain(|_, job| {
            let snapshot = job.snapshot();
            snapshot.status == "running"
                || now.duration_since(job.created_at) < Duration::from_secs(900)
        });
        if let Some(existing) = jobs
            .values()
            .find(|job| job.short() == short && !job.is_finished())
        {
            return existing.clone();
        }
        let id = uuid::Uuid::new_v4().to_string();
        let job = ThemeJob::new(id.clone(), short.to_string(), kind.to_string());
        jobs.insert(id, job.clone());
        job
    }

    pub async fn get(&self, id: &str) -> Option<ThemeJob> {
        self.inner.read().await.get(id).cloned()
    }
}
