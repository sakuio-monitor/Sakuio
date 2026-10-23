//! Traffic ledger + billing watcher.
//! 流量账本与账单后台 watcher。
//!
//! Runs a 60s loop that:
//! - reconciles raw boot counters from the latest snapshot into a persistent
//!   per-cycle ledger (reboot-safe: negative delta means the agent rebooted
//!   and the fresh reading is added to the current period);
//! - resets the ledger per the traffic reset schedule (`cycle` / `monthly` /
//!   `never`);
//! - auto-renews expired nodes that are online (`auto_renewal = 1`), pushing
//!   the expiry by exactly one billing cycle per tick;
//! - raises `traffic_exceeded` notifications at 80% / 100% of the
//!   per-client threshold.
//!
//! 计费周期规则（`billing_cycle`）：
//! - `-1` 一次性付费：不自动续费；
//! - 预设值按日历续费：30=月付、92=季付、183=半年付、365=年付、730=两年付
//!   （到期日加对应自然月数，如 3 月 15 日 + 30 → 4 月 15 日）；
//! - 其他天数严格按天数续费（3 月 15 日 + 45 → 4 月 29 日）。

use crate::notification_api::{dispatch_event, EVENT_TRAFFIC_EXCEEDED};
use crate::AppState;
use chrono::{DateTime, Datelike, Months, TimeZone, Utc};
use sakuio_board::BoardStateStore;
use sakuio_db::ClientRepository;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Seconds between watcher ticks.
const TICK_SECS: u64 = 60;

/// Alert levels for threshold notification de-dup.
/// 阈值通知去重等级。
const LEVEL_NONE: u8 = 0;
const LEVEL_WARN: u8 = 1;
const LEVEL_FULL: u8 = 2;

/// One-time payment marker for `billing_cycle`.
/// 计费周期的一次性付费标记。
const CYCLE_ONCE: i32 = -1;

/// Preset billing cycles (days) that renew on the calendar.
/// 按日历续费的预设计费周期（天 → 自然月数）。
const CALENDAR_CYCLES: [(i32, u32); 5] = [
    (30, 1),   // 月付
    (92, 3),   // 季付
    (183, 6),  // 半年付
    (365, 12), // 年付
    (730, 24), // 两年付
];

#[derive(Default)]
struct WatcherState {
    /// uuid -> highest alerted level (0 none / 1 warn / 2 full).
    alerted: HashMap<String, u8>,
}

/// Spawns the background billing watcher (runs until process exit).
/// 启动后台账单 watcher（随进程常驻）。
pub fn spawn_billing_watcher(state: AppState) {
    let state = Arc::new(state);
    let watcher = Arc::new(Mutex::new(WatcherState::default()));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(TICK_SECS));
        loop {
            ticker.tick().await;
            if sakuio_db::maintenance_active() {
                continue;
            }
            if let Err(e) = tick_billing(&state, &watcher).await {
                tracing::warn!(error = %e, "billing watcher tick failed");
            }
        }
    });
}

fn fmt_gb(bytes: i64) -> String {
    format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
}

fn parse_rfc3339(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw.trim())
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn parse_expire_date(raw: &str) -> Option<DateTime<Utc>> {
    parse_rfc3339(raw).or_else(|| {
        chrono::NaiveDate::parse_from_str(raw.trim(), "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc())
    })
}

/// Parses `{"up":..,"down":..}` last-counter JSON (missing/invalid -> None).
/// 解析上次原始读数 JSON（缺失或非法返回 None）。
fn parse_last_json(raw: Option<&str>) -> Option<(u64, u64)> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    let up = value.get("up")?.as_u64()?;
    let down = value.get("down")?.as_u64()?;
    Some((up, down))
}

/// Adds one billing cycle to a timestamp: preset days renew on the calendar
/// (natural months), other day counts add exact days.
/// 在时间点上推进一个计费周期：预设天数按日历（自然月），其他按确切天数。
fn push_cycle(base: DateTime<Utc>, cycle_days: i32) -> Option<DateTime<Utc>> {
    if let Some((_, months)) = CALENDAR_CYCLES.iter().find(|(d, _)| *d == cycle_days) {
        return base.checked_add_months(Months::new(*months));
    }
    base.checked_add_signed(chrono::Duration::days(i64::from(cycle_days)))
}

async fn tick_billing(state: &AppState, watcher: &Mutex<WatcherState>) -> Result<(), String> {
    let repo = ClientRepository::new(&state.db);
    let ledger = repo
        .list_traffic_ledger()
        .await
        .map_err(|e| e.to_string())?;
    let now = Utc::now();
    let mut guard = watcher.lock().await;

    for row in ledger {
        let mut used_up = row.traffic_used_up;
        let mut used_down = row.traffic_used_down;
        let mut cycle_start = row.traffic_cycle_start.as_deref().and_then(parse_rfc3339);

        // ---- 1. Traffic reset schedule ----
        // `traffic_cycle_start` is the anchor of the current ledger period.
        let reset_due = match row.traffic_reset_mode.as_str() {
            "monthly" => {
                let month_start = Utc
                    .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
                    .single();
                match month_start {
                    Some(month_start) => {
                        if let Some(anchor) = cycle_start {
                            if anchor >= month_start {
                                None
                            } else {
                                Some(month_start)
                            }
                        } else {
                            Some(month_start)
                        }
                    }
                    None => None,
                }
            }
            // Follow the billing cycle; one-time plans never auto-reset.
            "cycle" => match (row.billing_cycle, cycle_start) {
                (cycle, Some(anchor)) if cycle > 0 => {
                    let period_end = push_cycle(anchor, cycle);
                    match period_end {
                        Some(end) if now >= end => Some(now),
                        _ => None,
                    }
                }
                (_, None) => Some(now),
                _ => None,
            },
            _ => None, // "never"
        };
        if let Some(new_anchor) = reset_due {
            used_up = 0;
            used_down = 0;
            cycle_start = Some(new_anchor);
        }

        // ---- 2. Ledger reconciliation against the latest raw counters ----
        let snap = match state.board.get_latest_snapshot(&row.uuid).await {
            Ok(Some(snap)) => snap,
            _ => continue,
        };
        let cur_up = snap.net_total_up.unwrap_or(0);
        let cur_down = snap.net_total_down.unwrap_or(0);

        match parse_last_json(row.traffic_last_json.as_deref()) {
            // Baseline observation: do not import lifetime counters.
            None => {}
            Some((last_up, last_down)) => {
                if cur_up >= last_up {
                    used_up = used_up.saturating_add((cur_up - last_up) as i64);
                } else {
                    // Reboot: counters reset, add the fresh reading wholesale.
                    used_up = used_up.saturating_add(cur_up as i64);
                }
                if cur_down >= last_down {
                    used_down = used_down.saturating_add((cur_down - last_down) as i64);
                } else {
                    used_down = used_down.saturating_add(cur_down as i64);
                }
            }
        }

        let last_json = serde_json::json!({ "up": cur_up, "down": cur_down }).to_string();
        let cycle_ref = cycle_start.map(|dt| dt.to_rfc3339());
        repo.update_traffic_ledger(
            &row.uuid,
            used_up,
            used_down,
            &last_json,
            cycle_ref.as_deref(),
        )
        .await
        .map_err(|e| e.to_string())?;

        // ---- 3. Per-client threshold alerts (80% / 100%) ----
        if row.traffic_threshold > 0 {
            let mode = row.traffic_stat_mode.as_str();
            let total = sakuio_db::traffic_used_by_mode(used_up, used_down, mode);
            let warn_at = row.traffic_threshold / 5 * 4;
            let level = if total >= row.traffic_threshold {
                LEVEL_FULL
            } else if total >= warn_at && warn_at > 0 {
                LEVEL_WARN
            } else {
                LEVEL_NONE
            };
            let prev_level = guard.alerted.get(&row.uuid).copied().unwrap_or(LEVEL_NONE);
            if level > prev_level {
                guard.alerted.insert(row.uuid.clone(), level);
                dispatch_event(
                    state,
                    EVENT_TRAFFIC_EXCEEDED,
                    &row.uuid,
                    &row.name,
                    &[
                        ("traffic_used", fmt_gb(total)),
                        ("traffic_threshold", fmt_gb(row.traffic_threshold)),
                        (
                            "level",
                            if level >= LEVEL_FULL {
                                "100%".to_string()
                            } else {
                                "80%".to_string()
                            },
                        ),
                    ],
                )
                .await;
            } else if level < prev_level {
                // New cycle / manual reset: re-arm alerts.
                if level == LEVEL_NONE {
                    guard.alerted.remove(&row.uuid);
                } else {
                    guard.alerted.insert(row.uuid.clone(), level);
                }
            }
        } else if reset_due.is_some() {
            guard.alerted.remove(&row.uuid);
        }

        // ---- 4. Auto renewal (expired + online, one cycle per tick) ----
        if !row.auto_renewal || !row.online || row.billing_cycle == CYCLE_ONCE {
            continue;
        }
        let Some(expired_raw) = row.expired_at.clone().filter(|s| !s.trim().is_empty()) else {
            continue;
        };
        let Some(expire_at) = parse_expire_date(&expired_raw) else {
            continue;
        };
        if expire_at >= now || row.billing_cycle <= 0 {
            continue;
        }
        let Some(next) = push_cycle(expire_at, row.billing_cycle) else {
            continue;
        };
        repo.set_expired_at(&row.uuid, &next.to_rfc3339())
            .await
            .map_err(|e| e.to_string())?;
        tracing::info!(
            node_uuid = %row.uuid,
            new_expiry = %next.to_rfc3339(),
            "auto-renewal pushed expiry"
        );
    }
    Ok(())
}
