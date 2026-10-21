use sakuio_board::{BoardStateStore, RuntimeSnapshot, StateError};
use sakuio_metric::MetricWriter;
use sakuio_protocol::{TelemetryBatch, TelemetrySample};
use std::{sync::Arc, time::Duration};

/// Converts the latest sample in a batch into a dashboard snapshot.
/// 将批次中最新采样转为仪表盘快照。
pub fn snapshot_from_sample(node_uuid: &str, sample: &TelemetrySample) -> RuntimeSnapshot {
    RuntimeSnapshot {
        node_uuid: node_uuid.to_string(),
        observed_at_ms: sample.observed_at_ms,
        cpu_usage: Some(sample.cpu.usage),
        mem_used: Some(sample.ram.used as i64),
        mem_total: Some(sample.ram.total as i64),
        swap_used: Some(sample.swap.used as i64),
        swap_total: Some(sample.swap.total as i64),
        load1: Some(sample.load.load1),
        load5: Some(sample.load.load5),
        load15: Some(sample.load.load15),
        disk_used: Some(sample.disk.used as i64),
        disk_total: Some(sample.disk.total as i64),
        net_up: Some(sample.network.up),
        net_down: Some(sample.network.down),
        net_total_up: Some(sample.network.total_up),
        net_total_down: Some(sample.network.total_down),
        process: Some(sample.process),
        connections_tcp: Some(sample.connections.tcp),
        connections_udp: Some(sample.connections.udp),
        uptime_secs: Some(sample.uptime),
        gpu_usage: gpu_average(sample),
        online: true,
    }
}

fn gpu_average(sample: &TelemetrySample) -> Option<f32> {
    sample.gpu.as_ref().and_then(|v| {
        v.get("average_usage")
            .and_then(|x| x.as_f64())
            .map(|f| f as f32)
    })
}

/// Shared telemetry ingest: snapshot in board store + async metric persistence.
/// 共享遥测 ingest：写入 board 快照并异步持久化指标。
pub struct IngestPipeline {
    board: Arc<dyn BoardStateStore>,
    metrics: Arc<MetricWriter>,
    snapshot_ttl: Duration,
}

impl IngestPipeline {
    /// Builds a pipeline with board store and metric writer handles.
    /// 使用 board 存储与指标写入器构造 pipeline。
    pub fn new(
        board: Arc<dyn BoardStateStore>,
        metrics: Arc<MetricWriter>,
        snapshot_ttl: Duration,
    ) -> Self {
        Self {
            board,
            metrics,
            snapshot_ttl,
        }
    }

    /// Ingests a validated batch; returns after snapshot + enqueue (not disk flush).
    /// 入库已校验批次；在快照与入队完成后返回（不等待落盘）。
    pub async fn ingest_telemetry(
        &self,
        node_uuid: &str,
        batch: &TelemetryBatch,
    ) -> Result<(), StateError> {
        batch.validate().map_err(|e| StateError::Message(e.to_string()))?;
        if let Some(sample) = batch.samples.last() {
            let snap = snapshot_from_sample(node_uuid, sample);
            self.board
                .put_latest_snapshot(node_uuid, snap, self.snapshot_ttl)
                .await?;
        }
        self.board
            .append_recent_telemetry(node_uuid, &batch.samples)
            .await?;
        self.metrics
            .enqueue(node_uuid.to_string(), batch.samples.clone());
        Ok(())
    }
}
