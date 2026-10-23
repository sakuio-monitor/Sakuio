//! Monitored metric keys and value extraction from telemetry samples.
//! 监控指标清单与从遥测采样提取数值。

use sakuio_protocol::TelemetrySample;

/// Static definition of one monitored metric key.
/// 单个监控指标的静态定义。
pub struct MetricDef {
    pub key: &'static str,
    pub kind: &'static str,
    pub unit: &'static str,
}

/// All metric keys tracked by the store. Order is stable for UI tables.
/// 存储跟踪的全部指标 key。顺序固定，供页面表格使用。
pub const METRIC_KEYS: &[MetricDef] = &[
    MetricDef {
        key: "connections.tcp",
        kind: "gauge",
        unit: "count",
    },
    MetricDef {
        key: "connections.udp",
        kind: "gauge",
        unit: "count",
    },
    MetricDef {
        key: "cpu.usage",
        kind: "gauge",
        unit: "%",
    },
    MetricDef {
        key: "disk.used",
        kind: "gauge",
        unit: "bytes",
    },
    MetricDef {
        key: "gpu.device.usage",
        kind: "gauge",
        unit: "%",
    },
    MetricDef {
        key: "gpu.memory.total",
        kind: "gauge",
        unit: "bytes",
    },
    MetricDef {
        key: "gpu.memory.used",
        kind: "gauge",
        unit: "bytes",
    },
    MetricDef {
        key: "gpu.temperature",
        kind: "gauge",
        unit: "°C",
    },
    MetricDef {
        key: "gpu.usage",
        kind: "gauge",
        unit: "%",
    },
    MetricDef {
        key: "load.average",
        kind: "gauge",
        unit: "",
    },
    MetricDef {
        key: "memory.used",
        kind: "gauge",
        unit: "bytes",
    },
    MetricDef {
        key: "net.in.rate",
        kind: "gauge",
        unit: "bytes/s",
    },
    MetricDef {
        key: "net.out.rate",
        kind: "gauge",
        unit: "bytes/s",
    },
    MetricDef {
        key: "net.total.down",
        kind: "counter",
        unit: "bytes",
    },
    MetricDef {
        key: "net.total.up",
        kind: "counter",
        unit: "bytes",
    },
    MetricDef {
        key: "ping.latency_ms",
        kind: "gauge",
        unit: "ms",
    },
    MetricDef {
        key: "ping.loss",
        kind: "gauge",
        unit: "ratio",
    },
    MetricDef {
        key: "process.count",
        kind: "gauge",
        unit: "count",
    },
    MetricDef {
        key: "swap.used",
        kind: "gauge",
        unit: "bytes",
    },
    MetricDef {
        key: "traffic.down",
        kind: "gauge",
        unit: "bytes",
    },
    MetricDef {
        key: "traffic.up",
        kind: "gauge",
        unit: "bytes",
    },
];

/// Per-node traffic deltas derived from cumulative counters.
/// 由累计计数器推导的单节点流量增量。
#[derive(Debug, Clone, Copy, Default)]
pub struct TrafficDelta {
    pub down: f64,
    pub up: f64,
}

/// Extracts the value of one metric key from a sample.
/// 从采样中提取单个指标的数值；无值返回 None（如 ping 类指标）。
pub fn sample_value(key: &str, sample: &TelemetrySample, delta: TrafficDelta) -> Option<f64> {
    match key {
        "connections.tcp" => Some(sample.connections.tcp as f64),
        "connections.udp" => Some(sample.connections.udp as f64),
        "cpu.usage" => Some(sample.cpu.usage),
        "disk.used" => Some(sample.disk.used as f64),
        "gpu.device.usage" | "gpu.usage" => gpu_average(sample),
        "gpu.memory.total" => gpu_sum(sample, "memory_total"),
        "gpu.memory.used" => gpu_sum(sample, "memory_used"),
        "gpu.temperature" => gpu_max_temp(sample),
        "load.average" => Some(sample.load.load1),
        "memory.used" => Some(sample.ram.used as f64),
        "net.in.rate" => Some(sample.network.down as f64),
        "net.out.rate" => Some(sample.network.up as f64),
        "net.total.down" => Some(sample.network.total_down as f64),
        "net.total.up" => Some(sample.network.total_up as f64),
        // ping 指标由 Ping 任务链路单独持久化，不参与遥测采样聚合。
        "ping.latency_ms" | "ping.loss" => None,
        "process.count" => Some(sample.process as f64),
        "swap.used" => Some(sample.swap.used as f64),
        "traffic.down" => Some(delta.down),
        "traffic.up" => Some(delta.up),
        _ => None,
    }
}

fn gpu_average(sample: &TelemetrySample) -> Option<f64> {
    sample.gpu.as_ref()?.get("average_usage")?.as_f64()
}

fn gpu_sum(sample: &TelemetrySample, field: &str) -> Option<f64> {
    let details = sample.gpu.as_ref()?.get("detailed_info")?.as_array()?;
    let mut total = 0.0_f64;
    let mut any = false;
    for d in details {
        if let Some(v) = d.get(field).and_then(|v| v.as_f64()) {
            total += v;
            any = true;
        }
    }
    any.then_some(total)
}

fn gpu_max_temp(sample: &TelemetrySample) -> Option<f64> {
    let details = sample.gpu.as_ref()?.get("detailed_info")?.as_array()?;
    details
        .iter()
        .filter_map(|d| d.get("temperature").and_then(|v| v.as_f64()))
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
}
