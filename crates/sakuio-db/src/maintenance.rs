//! Process-wide maintenance switch used while the main database is copied.
//! 主库复制期间使用的进程级维护标记。
//!
//! The flag is deliberately global: the switch touches every write path
//! (admin HTTP, agent ingest, ping records, metric flush), and threading a
//! handle through all of them would add state with no benefit for a
//! single-process server.
//!
//! 该标记刻意做成全局：切换会触及管理端写请求、Agent ingest、ping 记录与
//! 指标落盘；单进程服务没有必要为它额外传递状态。

use std::sync::atomic::{AtomicBool, Ordering};

static MAINTENANCE: AtomicBool = AtomicBool::new(false);

/// Enables or disables maintenance mode.
/// 开启或关闭维护模式。
pub fn set_maintenance(active: bool) {
    let previous = MAINTENANCE.swap(active, Ordering::SeqCst);
    if previous != active {
        tracing::info!(active, "database maintenance mode changed");
    }
}

/// Returns true while a main-database switch is in progress.
/// 主库切换进行中时返回 true。
pub fn maintenance_active() -> bool {
    MAINTENANCE.load(Ordering::SeqCst)
}
