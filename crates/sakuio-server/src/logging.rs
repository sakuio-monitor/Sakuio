/// Initializes `tracing` from `RUST_LOG` with sensible defaults.
/// 根据 `RUST_LOG` 初始化 `tracing`，并设置合理默认值。
pub fn init() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sakuio_server=debug,sakuio_transport_ws=debug".into()),
        )
        .init();
}
