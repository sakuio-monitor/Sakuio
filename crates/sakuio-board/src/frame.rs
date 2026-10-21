use sakuio_protocol::{ControlMessage, SkltPayload};

/// Multiplexed board link frame (transport-agnostic).
/// 与传输无关的多路复用板级链路帧。
#[derive(Debug, Clone)]
pub enum BoardFrame {
    Control(ControlMessage),
    Telemetry(SkltPayload),
}
