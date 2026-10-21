//! Board session business logic: hello, basic info, telemetry ingest.
//! 板级会话业务逻辑：hello、静态信息与遥测入库。

mod command_bus;
mod ingest;
mod link_handler;
mod ping_dispatch;
mod remote;
mod session_id;

pub use command_bus::AgentCommandBus;
pub use ping_dispatch::flush_pending_ping_commands;
pub use ingest::{snapshot_from_sample, IngestPipeline};
pub use link_handler::LinkSessionHandler;
pub use remote::{parse_file_result, RemoteError, RemoteHub};
pub use session_id::new_session_id;
