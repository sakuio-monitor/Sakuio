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

#[derive(Clone, Copy)]
struct P2Quantile {
    q: f64,
    n: i64,
    marks: [f64; 5],
    positions: [f64; 5],
    desired: [f64; 5],
}

impl P2Quantile {
    fn new(q: f64) -> Self {
        Self {
            q,
            n: 0,
            marks: [0.0; 5],
            positions: [1.0, 2.0, 3.0, 4.0, 5.0],
            desired: [0.0; 5],
        }
    }

    fn from_value(q: f64, value: f64) -> Self {
        Self {
            q,
            n: 5,
            marks: [value; 5],
            positions: [1.0, 2.0, 3.0, 4.0, 5.0],
            desired: [1.0, 1.0 + 2.0 * q, 1.0 + 4.0 * q, 3.0 + 2.0 * q, 5.0],
        }
    }

    fn add(&mut self, value: f64) {
        if self.n < 5 {
            self.marks[self.n as usize] = value;
            self.n += 1;
            if self.n == 5 {
                self.marks.sort_by(|a, b| a.total_cmp(b));
                self.positions = [1.0, 2.0, 3.0, 4.0, 5.0];
                self.desired = [
                    1.0,
                    1.0 + 2.0 * self.q,
                    1.0 + 4.0 * self.q,
                    3.0 + 2.0 * self.q,
                    5.0,
                ];
            }
            return;
        }
        let k = if value < self.marks[0] {
            self.marks[0] = value;
            0
        } else if value >= self.marks[4] {
            self.marks[4] = value;
            3
        } else {
            (1..4).find(|&i| value < self.marks[i]).unwrap_or(3)
        };
        for i in (k + 1)..5 {
            self.positions[i] += 1.0;
        }
        for i in 0..5 {
            self.desired[i] += [0.0, 0.5 * self.q, self.q, 0.5 + 0.5 * self.q, 1.0][i];
        }
        for i in 1..4 {
            let d = self.desired[i] - self.positions[i];
            let p = self.positions[i - 1];
            let n = self.positions[i + 1];
            let pp = self.positions[i];
            if (d >= 1.0 && n - pp > 1.0) || (d <= -1.0 && pp - p > 1.0) {
                let dir = if d > 0.0 { 1.0 } else { -1.0 };
                let candidate = self.marks[i]
                    + dir / (n - p)
                        * ((pp - p + dir) * (self.marks[i + 1] - self.marks[i]) / (n - pp)
                            + (n - pp - dir) * (self.marks[i] - self.marks[i - 1]) / (pp - p));
                if candidate > self.marks[i - 1] && candidate < self.marks[i + 1] {
                    self.marks[i] = candidate;
                }
                self.positions[i] += dir;
            }
        }
    }

    fn value(&self) -> f64 {
        if self.n == 0 {
            return 0.0;
        }
        if self.n < 5 {
            let mut values = self.marks;
            values.sort_by(|a, b| a.total_cmp(b));
            let idx = ((self.n as f64 - 1.0) * self.q).round() as usize;
            return values[idx.min((self.n - 1) as usize)];
        }
        let qpos = self.q * (self.n as f64 - 1.0) + 1.0;
        for i in 1..5 {
            if qpos <= self.positions[i] {
                let span = self.positions[i] - self.positions[i - 1];
                let frac = if span <= 0.0 {
                    0.0
                } else {
                    (qpos - self.positions[i - 1]) / span
                };
                return self.marks[i - 1] + frac * (self.marks[i] - self.marks[i - 1]);
            }
        }
        self.marks[4]
    }
}

#[derive(Default)]
struct WindowState {
    start_ms: i64,
    sums: HashMap<&'static str, f64>,
    sums_sq: HashMap<&'static str, f64>,
    mins: HashMap<&'static str, f64>,
    maxs: HashMap<&'static str, f64>,
    counts: HashMap<&'static str, i64>,
    quantiles: HashMap<&'static str, [P2Quantile; 3]>,
}

impl WindowState {
    fn reset(&mut self, start_ms: i64) {
        self.start_ms = start_ms;
        self.sums.clear();
        self.sums_sq.clear();
        self.mins.clear();
        self.maxs.clear();
        self.counts.clear();
        self.quantiles.clear();
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

    /// Restores open-window counts from a durable checkpoint. Exact sums,
    /// min/max/count are restored; quantiles are seeded from the persisted
    /// p-values and continue updating with new samples.
    pub fn restore(&mut self, node: &str, rows: &[BucketRow]) {
        let state = self.nodes.entry(node.to_string()).or_default();
        for row in rows {
            if !WINDOW_SECONDS.contains(&row.bucket_seconds) || row.sample_count <= 0 {
                continue;
            }
            let key = metric_key_static(&row.metric_key);
            if key.is_empty() {
                continue;
            }
            let win = state.windows.entry(row.bucket_seconds).or_default();
            if win.start_ms != row.bucket_start_ms {
                win.reset(row.bucket_start_ms);
            }
            win.sums.insert(
                metric_key_static(&row.metric_key),
                row.avg_val * row.sample_count as f64,
            );
            win.sums_sq
                .insert(metric_key_static(&row.metric_key), row.sum_sq);
            win.mins
                .insert(metric_key_static(&row.metric_key), row.min_val);
            win.maxs
                .insert(metric_key_static(&row.metric_key), row.max_val);
            win.counts
                .insert(metric_key_static(&row.metric_key), row.sample_count);
            win.quantiles.insert(
                metric_key_static(&row.metric_key),
                [
                    P2Quantile::from_value(0.50, row.p50_val),
                    P2Quantile::from_value(0.95, row.p95_val),
                    P2Quantile::from_value(0.99, row.p99_val),
                ],
            );
        }
    }

    /// Snapshot of all currently open windows without resetting them.
    /// Used for periodic durability checkpoints.
    pub fn snapshot(&self) -> Vec<BucketRow> {
        let mut rows = Vec::new();
        for (node, state) in &self.nodes {
            for (window, win) in &state.windows {
                if win.counts.values().sum::<i64>() > 0 {
                    rows.extend(flush_window(node, *window, win));
                }
            }
        }
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

fn metric_key_static(key: &str) -> &'static str {
    METRIC_KEYS
        .iter()
        .find(|def| def.key == key)
        .map(|def| def.key)
        .unwrap_or("")
}

fn traffic_delta(state: &mut NodeWindows, sample: &TelemetrySample) -> TrafficDelta {
    let now_totals = (sample.network.total_up, sample.network.total_down);
    let delta = if state.has_totals {
        // A counter that goes backwards means the NIC / agent restarted. Count
        // the new value as traffic since the reset instead of dropping the
        // whole interval with saturating_sub.
        let (up, down) = if sample.network.total_up < state.last_totals.0
            || sample.network.total_down < state.last_totals.1
        {
            (
                sample.network.total_up as f64,
                sample.network.total_down as f64,
            )
        } else {
            (
                sample.network.total_up.saturating_sub(state.last_totals.0) as f64,
                sample
                    .network
                    .total_down
                    .saturating_sub(state.last_totals.1) as f64,
            )
        };
        TrafficDelta { up, down }
    } else {
        TrafficDelta::default()
    };
    state.last_totals = now_totals;
    state.has_totals = true;
    delta
}

fn accumulate(win: &mut WindowState, def: &MetricDef, value: f64) {
    *win.sums.entry(def.key).or_default() += value;
    *win.sums_sq.entry(def.key).or_default() += value * value;
    let q = win.quantiles.entry(def.key).or_insert_with(|| {
        [
            P2Quantile::new(0.50),
            P2Quantile::new(0.95),
            P2Quantile::new(0.99),
        ]
    });
    for estimator in q.iter_mut() {
        estimator.add(value);
    }
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
        let avg = sum / count as f64;
        let sum_sq = win.sums_sq.get(def.key).copied().unwrap_or(0.0);
        let variance = (sum_sq / count as f64 - avg * avg).max(0.0);
        let quantiles = win.quantiles.get(def.key);
        rows.push(BucketRow {
            sum_sq: sum_sq,
            p50_val: quantiles.map(|q| q[0].value()).unwrap_or(0.0),
            p95_val: quantiles.map(|q| q[1].value()).unwrap_or(0.0),
            p99_val: quantiles.map(|q| q[2].value()).unwrap_or(0.0),
            variance,
            node_uuid: node.to_string(),
            metric_key: def.key.to_string(),
            bucket_seconds: window,
            bucket_start_ms: win.start_ms,
            avg_val: avg,
            min_val: win.mins.get(def.key).copied().unwrap_or(0.0),
            max_val: win.maxs.get(def.key).copied().unwrap_or(0.0),
            sample_count: count,
        });
    }
    rows
}
