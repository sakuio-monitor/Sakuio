mod api;
mod auth;
mod billing_watcher;
mod geoip_resolver;
mod logging;
mod notification_api;
mod ping_scheduler;
mod presence_watcher;
mod relay;
mod remote;
mod site_settings;
mod web;

use anyhow::Context;
use axum::{
    middleware,
    routing::{get, patch, post, put},
    Router,
};
use ping_scheduler::PingScheduler;
use relay::RelayBroker;
use sakuio_board::{BoardStateStore, MemoryBoardStore};
use sakuio_core::{AgentCommandBus, LinkSessionHandler, RemoteHub};
use sakuio_db::{open_bootstrap_db, ClientRepository, InstallService, PingRepository};
use sakuio_metric::{MetricWriter, MetricWriterConfig};
use sakuio_notify::Registry;
use sakuio_transport_ws::{WsLinkState, WS_LINK_PATH};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;

/// Process-wide application state.
/// 进程级应用状态。
#[derive(Clone)]
pub struct AppState {
    pub db: sqlx::SqlitePool,
    pub install_gate: Arc<RwLock<bool>>,
    pub board: Arc<MemoryBoardStore>,
    pub command_bus: AgentCommandBus,
    pub ping_scheduler: Arc<PingScheduler>,
    pub remote: Arc<RemoteHub>,
    pub relay: RelayBroker,
    pub exec_streams: remote::ExecStreamRegistry,
    pub notify: Registry,
    pub link: WsLinkState,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init();
    site_settings::apply_staged_restore();
    let db = open_bootstrap_db()
        .await
        .context("open bootstrap database")?;
    let pool = db.pool().clone();
    let install_status = InstallService::new(&pool).status().await?;
    let install_gate = Arc::new(RwLock::new(install_status.required));

    let board = Arc::new(MemoryBoardStore::default());
    let board_dyn: Arc<dyn BoardStateStore> = board.clone();
    let command_bus = AgentCommandBus::default();
    let ping_scheduler = Arc::new(PingScheduler::new(
        pool.clone(),
        board_dyn.clone(),
        command_bus.clone(),
    ));
    let metric_writer = Arc::new(MetricWriter::start(
        pool.clone(),
        MetricWriterConfig::default(),
    ));
    let remote = Arc::new(RemoteHub::default());
    let relay = RelayBroker::new();
    let link_handler = Arc::new(
        LinkSessionHandler::with_memory_board(pool.clone(), board.clone(), metric_writer)
            .with_remote_hub(remote.clone()),
    );

    let link = WsLinkState {
        db: pool.clone(),
        handler: link_handler,
        command_bus: command_bus.clone(),
        board: board_dyn,
    };

    if let Ok(clients) = ClientRepository::new(&pool).list().await {
        let ping_repo = PingRepository::new(&pool);
        for c in clients {
            let _ = ping_repo.append_default_on_client(&c.uuid).await;
        }
    }
    ping_scheduler.reload().await;

    let notify = notification_api::init_notify_registry().await;

    let app_state = AppState {
        db: pool,
        install_gate,
        board: board.clone(),
        command_bus,
        ping_scheduler,
        remote,
        relay,
        exec_streams: remote::ExecStreamRegistry::default(),
        notify,
        link,
    };

    notification_api::spawn_notification_watcher(app_state.clone());
    geoip_resolver::spawn_geoip_resolver(app_state.clone());
    billing_watcher::spawn_billing_watcher(app_state.clone());
    presence_watcher::spawn_presence_watcher(app_state.clone()).await;
    site_settings::bootstrap_defaults(&app_state).await;

    let admin_protected = remote::fs_routes()
        .route("/api/admin/nodes/:uuid/shell", get(remote::shell_ws))
        .route(
            "/api/admin/clients",
            get(api::list_admin_clients).post(api::add_client),
        )
        .route("/api/admin/clients/stream", get(api::stream_admin_clients))
        .route("/api/admin/clients/sync", post(api::sync_admin_clients))
        .route(
            "/api/admin/clients/:uuid",
            patch(api::patch_admin_client).delete(api::delete_admin_client),
        )
        .route("/api/admin/clients/reorder", put(api::reorder_client))
        .route("/api/admin/clients/batch-delete", post(api::batch_delete_clients))
        .route("/api/admin/clients/batch-group", post(api::batch_set_group))
        .route(
            "/api/admin/clients/:uuid/reset-traffic",
            post(api::reset_client_traffic),
        )
        .route("/api/admin/ping", get(web::rpc::admin_ping_list))
        .route("/api/admin/ping/add", post(web::rpc::admin_ping_add))
        .route("/api/admin/ping/delete", post(web::rpc::admin_ping_delete))
        .route("/api/admin/ping/edit", post(web::rpc::admin_ping_edit))
        .route("/api/admin/ping/order", post(web::rpc::admin_ping_order))
        .route("/api/admin/task/exec", post(remote::exec_task))
        .route(
            "/api/admin/task/:task_id/result",
            get(remote::exec_task_results),
        )
        .route(
            "/api/admin/task/:task_id/clients/:uuid/stream",
            get(remote::exec_stream_ws),
        )
        .route(
            "/api/admin/notification/types",
            get(notification_api::list_channel_types),
        )
        .route(
            "/api/admin/notification/instances",
            get(notification_api::list_instances).post(notification_api::create_instance),
        )
        .route(
            "/api/admin/notification/instances/:id",
            get(notification_api::get_instance)
                .put(notification_api::update_instance)
                .delete(notification_api::delete_instance),
        )
        .route(
            "/api/admin/notification/instances/:id/test",
            post(notification_api::test_instance),
        )
        .route(
            "/api/admin/notification/rules",
            get(notification_api::list_rules).post(notification_api::create_rule),
        )
        .route(
            "/api/admin/notification/rules/:id",
            get(notification_api::get_rule)
                .put(notification_api::update_rule)
                .delete(notification_api::delete_rule),
        )
        .route("/api/admin/2fa/setup", post(auth::setup_2fa))
        .route("/api/admin/2fa/enable", post(auth::enable_2fa))
        .route("/api/admin/2fa/disable", post(auth::disable_2fa))
        .route(
            "/api/admin/settings/site",
            get(site_settings::get_site_settings).put(site_settings::put_site_settings),
        )
        .route(
            "/api/admin/settings/site/share-token",
            post(site_settings::regenerate_share_token),
        )
        .route(
            "/api/admin/settings/geoip/update",
            post(site_settings::update_geoip_db),
        )
        .route(
            "/api/admin/settings/geoip/test",
            post(site_settings::test_geoip),
        )
        .route(
            "/api/admin/settings/backup",
            get(site_settings::download_backup),
        )
        .route(
            "/api/admin/settings/restore",
            post(site_settings::restore_backup),
        )
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            auth::admin_auth_layer,
        ));

    let public_with_guards = web::public_api_router()
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            site_settings::origin_guard,
        ))
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            site_settings::private_site_layer,
        ));

    let app = Router::new()
        .merge(public_with_guards)
        .route("/api/share/:token", get(site_settings::share_entry))
        .route("/api/login", post(auth::login))
        .route("/api/logout", get(auth::logout))
        .route("/api/me", get(auth::session_me))
        .merge(admin_protected)
        .route(
            "/api/agent/relay/:relay_id",
            get(remote::agent_relay_ws),
        )
        .route(WS_LINK_PATH, get(api::agent_link))
        .route("/api/install/status", get(api::install_status))
        .route("/api/install/complete", post(api::install_complete))
        .route("/api/install/test-db", post(api::install_test_db))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    let addr: SocketAddr = "0.0.0.0:8080".parse().unwrap();
    tracing::info!(%addr, path = WS_LINK_PATH, "sakuio-server listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
