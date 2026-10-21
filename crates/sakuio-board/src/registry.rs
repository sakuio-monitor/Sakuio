use crate::transport::{BoardTransport, BoardTransportServer, TransportError};
use std::{collections::HashMap, sync::Arc};

/// Registry of agent board transports and their server mounts.
/// Agent 板级传输及其服务端挂载的注册表。
pub struct BoardTransportRegistry {
    transports: HashMap<&'static str, Arc<dyn BoardTransport>>,
    servers: HashMap<&'static str, Arc<dyn BoardTransportServer>>,
}

impl Default for BoardTransportRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BoardTransportRegistry {
    /// Creates an empty registry.
    /// 创建空注册表。
    pub fn new() -> Self {
        Self {
            transports: HashMap::new(),
            servers: HashMap::new(),
        }
    }

    /// Registers an outbound transport implementation.
    /// 注册出站传输实现。
    pub fn register_transport(&mut self, transport: Arc<dyn BoardTransport>) {
        self.transports.insert(transport.name(), transport);
    }

    /// Registers an inbound transport server.
    /// 注册入站传输服务。
    pub fn register_server(&mut self, server: Arc<dyn BoardTransportServer>) {
        self.servers.insert(server.name(), server);
    }

    /// Looks up an outbound transport by plugin name.
    /// 按插件名查找出站传输。
    pub fn get_transport(&self, name: &str) -> Option<Arc<dyn BoardTransport>> {
        self.transports.get(name).cloned()
    }

    /// Iterates registered inbound servers.
    /// 遍历已注册的入站服务。
    pub fn servers(&self) -> impl Iterator<Item = Arc<dyn BoardTransportServer>> + '_ {
        self.servers.values().cloned()
    }

    /// Returns registered outbound transport names.
    /// 返回已注册的出站传输名称列表。
    pub fn registered_names(&self) -> Vec<&'static str> {
        self.transports.keys().copied().collect()
    }
}

/// Crate that plugs one wire protocol into the board registry.
/// 将一种线缆协议接入板级注册表的插件 crate。
pub trait TransportPlugin: Send + Sync {
    /// Returns the transport plugin name (e.g. `websocket`).
    /// 返回传输插件名称（如 `websocket`）。
    fn name(&self) -> &'static str;
    /// Registers client and/or server halves on the registry.
    /// 在注册表上注册客户端和/或服务端实现。
    fn register(&self, registry: &mut BoardTransportRegistry) -> Result<(), TransportError>;
}
