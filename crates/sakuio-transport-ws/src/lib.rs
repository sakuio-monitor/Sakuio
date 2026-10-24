//! WebSocket transport plugin for SakuLink (agent ↔ board).
//! SakuLink 的 WebSocket 传输插件（Agent ↔ 中控）。

mod plugin;
mod session;

pub use plugin::{ws_upgrade, LinkQuery, WsBoardTransportPlugin, WsLinkState, WS_LINK_PATH};
