//! Agent-board session layer: frames, state store, and transport registry.
//! Agent 与中控之间的会话层：帧、状态存储与传输注册表。

mod frame;
mod memory;
mod registry;
mod relay;
mod session;
mod state;
mod transport;

pub use frame::BoardFrame;
pub use memory::{BoardMemoryStats, MemoryBoardStore};
pub use registry::{BoardTransportRegistry, TransportPlugin};
pub use relay::RelayKind;
pub use session::{BoardSession, BoardSessionHandler, SharedBoardSessionHandler};
pub use state::{
    BoardStateStore, CommandCursor, CommandEnvelope, Presence, RuntimeSnapshot, StateError,
    RECENT_TELEMETRY_MS,
};
pub use transport::{BoardConnectInfo, BoardTransport, BoardTransportServer, TransportError};
