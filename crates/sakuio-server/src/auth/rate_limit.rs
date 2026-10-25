//! In-memory login throttling. Cleared automatically on process restart.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAX_FAILURES: u32 = 5;
const FREEZE: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Copy)]
struct Entry {
    failures: u32,
    blocked_until: Option<Instant>,
    last_failure: Instant,
}

fn table() -> &'static Mutex<HashMap<String, Entry>> {
    static TABLE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Returns how long the IP is still blocked, if any.
pub fn blocked_for(ip: &str) -> Option<Duration> {
    let now = Instant::now();
    let guard = table().lock().ok()?;
    let entry = guard.get(ip)?;
    match entry.blocked_until {
        Some(until) if until > now => Some(until - now),
        _ => None,
    }
}

pub fn record_failure(ip: &str) {
    let now = Instant::now();
    let Ok(mut guard) = table().lock() else {
        return;
    };
    // Drop entries that have been inactive longer than the freeze window.
    // Without this, every failed source address stayed in the map forever.
    guard.retain(|_, entry| {
        entry
            .blocked_until
            .map_or_else(|| now.duration_since(entry.last_failure) < FREEZE, |until| until > now)
    });
    let entry = guard.entry(ip.to_string()).or_insert(Entry {
        failures: 0,
        blocked_until: None,
        last_failure: now,
    });
    entry.last_failure = now;
    entry.failures = entry.failures.saturating_add(1);
    if entry.failures >= MAX_FAILURES {
        entry.blocked_until = Some(now + FREEZE);
        entry.failures = 0;
    }
}

pub fn record_success(ip: &str) {
    if let Ok(mut guard) = table().lock() {
        guard.remove(ip);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freezes_after_five_failures_and_clears_on_success() {
        let ip = "203.0.113.77";
        for _ in 0..4 {
            record_failure(ip);
        }
        assert!(blocked_for(ip).is_none());
        record_failure(ip);
        assert!(blocked_for(ip).is_some());
        record_success(ip);
        assert!(blocked_for(ip).is_none());
    }
}
