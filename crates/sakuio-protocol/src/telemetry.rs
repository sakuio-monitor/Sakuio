use serde::{Deserialize, Serialize};

/// CPU usage block (`cpu.usage`).
/// CPU 使用率块。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuReport {
    pub usage: f64,
}

/// Total/used pair for ram, swap, disk.
/// 内存、交换区、磁盘的总计/已用对。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct UsageReport {
    pub total: u64,
    pub used: u64,
}

/// Load averages (`load1`, `load5`, `load15`).
/// 负载均值。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct LoadReport {
    pub load1: f64,
    pub load5: f64,
    pub load15: f64,
}

/// Network throughput and cumulative counters.
/// 网络速率与累计计数。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct NetworkReport {
    pub up: u64,
    pub down: u64,
    #[serde(rename = "totalUp")]
    pub total_up: u64,
    #[serde(rename = "totalDown")]
    pub total_down: u64,
}

/// Socket counts.
/// 套接字数量。
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ConnectionsReport {
    pub tcp: i32,
    pub udp: i32,
}

/// GPU model list fallback payload.
/// GPU 型号列表（降级形态）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuModelsReport {
    pub models: Vec<String>,
}

/// Per-device GPU metrics.
/// 单块 GPU 指标。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuDeviceReport {
    pub name: String,
    pub memory_total: u64,
    pub memory_used: u64,
    pub utilization: f64,
    pub temperature: u64,
}

/// Detailed GPU aggregate payload.
/// 详细 GPU 汇总载荷。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuDetailedReport {
    pub count: i32,
    pub average_usage: f64,
    pub detailed_info: Vec<GpuDeviceReport>,
}

/// One observation sample (same field layout as agent periodic JSON report).
/// 单条采样（字段布局与 Agent 周期 JSON 上报一致）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetrySample {
    pub observed_at_ms: i64,
    pub cpu: CpuReport,
    pub ram: UsageReport,
    pub swap: UsageReport,
    pub load: LoadReport,
    pub disk: UsageReport,
    pub network: NetworkReport,
    pub connections: ConnectionsReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<serde_json::Value>,
    pub uptime: u64,
    pub process: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// CBOR payload body inside an SKTL frame.
/// SKTL 帧内的 CBOR 载荷体。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryBatch {
    pub node_uuid: String,
    pub samples: Vec<TelemetrySample>,
}

impl TelemetryBatch {
    /// Maximum samples allowed per batch on the wire.
    /// 单批允许的最大采样条数。
    pub const MAX_SAMPLES: usize = 64;

    /// Validates batch invariants before ingest.
    /// 入库前校验批次约束。
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.samples.is_empty() {
            return Err("empty samples");
        }
        if self.samples.len() > Self::MAX_SAMPLES {
            return Err("too many samples");
        }
        Ok(())
    }
}
