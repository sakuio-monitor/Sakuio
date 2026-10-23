//! In-memory windowed aggregation of telemetry samples into metric buckets.
//! 遥测采样的内存窗口聚合（降采样为指标桶）。

use crate::keys::{sample_value, MetricDef, TrafficDelta, METRIC_KEYS};
use crate::store::BucketRow;
use sakuio_protocol::TelemetrySample;
use std::collections::HashMap;

/// Bucket windows in seconds; daily buckets follow per-metric retention.
/// 桶窗口（秒）；日桶保留遵循各指标保留天数。
pub const WINDOW_SECONDS: [i64; 4] = [60, 300, 3600, 86_400];

/// Retention policy per metric key; 0 disables persistence for that metric.
/// 各指标保留策略；0 表示禁用该指标持久化。
pub type RetentionPolicy = std::collections::HashMap<String, i64>;

#[derive(Default)]
struct WindowState {
    start_ms: i64,
    sums: HashMap<&'static str, f64>,
    mins: HashMap<&'static str, f64>,
    maxs: HashMap<&'static str, f64>,
    counts: HashMap<&'static str, i64>,
}

impl WindowState {
    fn reset(&mut self, start_ms: i64) {
        self.start_ms = start_ms;
        self.sums.clear();
        self.mins.clear();
        self.maxs.clear();
        self.counts.clear();
    }
}

#[derive(Default)]
struct NodeWindows {
    windows: HashMap<i64, WindowState>,
    last_totals: (u64, u64),
    has_totals: bool,
}

/// Aggregates samples into per-metric bucket rows for all windows.
/// 将采样聚合为各窗口的按指标桶行。
#[derive(Default)]
pub struct BucketAggregator {
    nodes: HashMap<String, NodeWindows>,
}

impl BucketAggregator {
    /// Pushes one sample and returns any buckets completed by it.
    /// 推入一条采样，返回因它而完成的桶行。
    pub fn push(
        &mut self,
        node: &str,
        sample: &TelemetrySample,
        policy: &RetentionPolicy,
    ) -> Vec<BucketRow> {
        let state = self.nodes.entry(node.to_string()).or_default();
        let delta = traffic_delta(state, sample);
        let mut rows = Vec::new();
        for window in WINDOW_SECONDS {
            let start = sample.observed_at_ms - sample.observed_at_ms.rem_euclid(window * 1000);
            let win = state.windows.entry(window).or_default();
            if win.counts.values().sum::<i64>() > 0 && start > win.start_ms {
                rows.extend(flush_window(node, window, win));
                win.reset(start);
            } else if win.counts.values().sum::<i64>() == 0 {
                win.start_ms = start;
            }
            for def in METRIC_KEYS {
                if policy.get(def.key).copied().unwrap_or(90) == 0 {
                    continue;
                }
                if let Some(v) = sample_value(def.key, sample, delta) {
                    accumulate(win, def, v);
                }
            }
        }
        // ping 类指标恒为 None：保留表仍存在，聚合自然跳过。
        rows
    }

    /// Flushes the trailing (not yet rotated) buckets for all nodes.
    /// 冲刷所有节点尚未翻转的尾桶（关闭/迁移时调用）。
    pub fn flush_all(&mut self) -> Vec<BucketRow> {
        let mut rows = Vec::new();
        for (node, state) in &mut self.nodes {
            for (window, win) in state.windows.iter_mut() {
                if win.counts.values().sum::<i64>() > 0 {
                    rows.extend(flush_window(node, *window, win));
                }
                win.reset(i64::MAX);
            }
        }
        rows
    }
}

fn traffic_delta(state: &mut NodeWindows, sample: &TelemetrySample) -> TrafficDelta {
    let now_totals = (sample.network.total_up, sample.network.total_down);
    let delta = if state.has_totals {
        TrafficDelta {
            down: sample
                .network
                .total_down
                .saturating_sub(state.last_totals.1) as f64,
            up: sample.network.total_up.saturating_sub(state.last_totals.0) as f64,
        }
    } else {
        TrafficDelta::default()
    };
    state.last_totals = now_totals;
    state.has_totals = true;
    delta
}

fn accumulate(win: &mut WindowState, def: &MetricDef, value: f64) {
    *win.sums.entry(def.key).or_default() += value;
    let min = win.mins.entry(def.key).or_insert(value);
    if value < *min {
        *min = value;
    }
    let max = win.maxs.entry(def.key).or_insert(value);
    if value > *max {
        *max = value;
    }
    *win.counts.entry(def.key).or_default() += 1;
}

fn flush_window(node: &str, window: i64, win: &WindowState) -> Vec<BucketRow> {
    let mut rows = Vec::new();
    for def in METRIC_KEYS {
        let Some(&count) = win.counts.get(def.key) else {
            continue;
        };
        if count <= 0 {
            continue;
        }
        let sum = win.sums.get(def.key).copied().unwrap_or(0.0);
        rows.push(BucketRow {
            node_uuid: node.to_string(),
            metric_key: def.key.to_string(),
            bucket_seconds: window,
            bucket_start_ms: win.start_ms,
            avg_val: sum / count as f64,
            min_val: win.mins.get(def.key).copied().unwrap_or(0.0),
            max_val: win.maxs.get(def.key).copied().unwrap_or(0.0),
            sample_count: count,
        });
    }
    rows
}
