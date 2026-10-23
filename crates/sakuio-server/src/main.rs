mod alloc_metrics;
mod api;
mod audit;
mod audit_api;
mod auth;
mod billing_watcher;
mod dashboard;
mod database_api;
mod db_task;
mod ddns;
mod geoip_resolver;
mod logging;
mod maintenance_guard;
mod memory_map;
mod notification_api;
mod ping_scheduler;
mod pprof_api;
mod presence_watcher;
mod relay;
mod remote;
mod site_settings;
mod theme;
mod theme_api;
mod web;

use axum::{
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, patch, post, put},
    Router,
};
use ping_scheduler::PingScheduler;
use relay::RelayBroker;
use sakuio_board::{BoardStateStore, MemoryBoardStore};
use sakuio_core::{AgentCommandBus, LinkSessionHandler, RemoteHub};
use sakuio_db::{open_bootstrap_db, ClientRepository, DbPool, InstallService, PingRepository};
use sakuio_metric::{MetricWriter, MetricWriterConfig};
use sakuio_notify::Registry;
use sakuio_transport_ws::{WsLinkState, WS_LINK_PATH};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;

#[global_allocator]
static GLOBAL_ALLOCATOR: alloc_metrics::TrackingAllocator = alloc_metrics::TrackingAllocator;

/// Process-wide application state.
/// 进程级应用状态。
#[derive(Clone)]
pub struct AppState {
    pub db: DbPool,
    pub install_gate: Arc<RwLock<bool>>,
    pub board: Arc<MemoryBoardStore>,
    pub command_bus: AgentCommandBus,
    pub ping_scheduler: Arc<PingScheduler>,
    pub remote: Arc<RemoteHub>,
    pub relay: RelayBroker,
    pub exec_streams: remote::ExecStreamRegistry,
    pub notify: Registry,
    pub link: WsLinkState,
    pub metric: Arc<database_api::MetricRuntime>,
    pub theme: Arc<theme::ThemeService>,
    pub audit: audit::AuditLogger,
    pub geo: Arc<geoip_resolver::GeoEngine>,
    pub dashboard: Arc<dashboard::DashboardService>,
    pub ddns: Arc<ddns::DdnsService>,
}

/// Serves the active Komari theme and its SPA fallback.
/// 提供当前 Komari 主题及其 SPA 回退。
async fn theme_serve(State(state): State<AppState>, request: Request) -> Response {
    if request.method() != Method::GET && request.method() != Method::HEAD {
        return StatusCode::NOT_FOUND.into_response();
    }
    let path = request.uri().path().to_string();
    if path == "/api"
        || path.starts_with("/api/")
        || path.starts_with("/admin")
        || path.starts_with("/terminal")
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (file, page) = if let Some(rest) = path.strip_prefix("/themes/") {
        let (short, relative) = rest.split_once('/').unwrap_or((rest, ""));
        (state.theme.read_theme_asset(short, relative).await, false)
    } else {
        let file = state.theme.read_active(path.trim_start_matches('/')).await;
        (file, true)
    };
    let Some(file) = file else {
        return (StatusCode::NOT_FOUND, "theme asset not found").into_response();
    };
    let is_page = page && file.mime.starts_with("text/html");
    let bytes = if is_page {
        let settings = crate::site_settings::load(&state).await;
        let html = String::from_utf8_lossy(&file.bytes);
        theme::inject_index(
            &html,
            &theme::SiteInject {
                title: settings.site_name,
                description: settings.site_description,
                custom_head: settings.custom_head,
                custom_body: settings.custom_body,
            },
        )
        .into_bytes()
    } else {
        file.bytes
    };
    let cache = if is_page {
        "no-cache"
    } else if path.contains('.') {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let body = if request.method() == Method::HEAD {
        axum::body::Body::empty()
    } else {
        axum::body::Body::from(bytes)
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, file.mime),
            (header::CACHE_CONTROL, cache.to_string()),
        ],
        body,
    )
        .into_response()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init();
    site_settings::apply_staged_restore();
    // 主库 DSN：环境变量优先，其次安装时写入的设置（默认本机 SQLite）。
    // 主库 DSN：env 优先，其次安装设置；缺省本机 SQLite。
    let app_db = if let Ok(dsn) = std::env::var("SAKUIO_DATABASE_DSN") {
        let max = std::env::var("SAKUIO_DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(16);
        sakuio_db::AppDatabase::connect_with_max(&dsn, true, max).await?
    } else {
        let boot = open_bootstrap_db().await?;
        let settings = sakuio_db::SettingsRepository::new(boot.pool());
        let dsn: Option<String> = settings.get_json("app_db_dsn").await?;
        let max = settings
            .get_json::<u32>("app_db_max_connections")
            .await?
            .unwrap_or(16)
            .clamp(1, 64);
        match dsn.filter(|dsn| !dsn.trim().is_empty()) {
            Some(dsn) => {
                // Any configured DSN wins, including a non-default SQLite path.
                // 只要配置了 DSN 就使用它（包括非默认的 SQLite 路径）。
                boot.shutdown().await;
                sakuio_db::AppDatabase::connect_with_max(&dsn, true, max).await?
            }
            None if max != 16 => {
                let dsn = boot.pool().dsn().to_string();
                boot.shutdown().await;
                sakuio_db::AppDatabase::connect_with_max(&dsn, true, max).await?
            }
            None => boot,
        }
    };
    let pool = app_db.pool().clone();
    let geo = Arc::new(geoip_resolver::GeoEngine::new(pool.clone()));
    let audit = audit::AuditLogger::start(pool.clone(), geo.clone());
    let install_status = InstallService::new(&pool).status().await?;
    let install_gate = Arc::new(RwLock::new(install_status.required));

    // 指标库：DSN 留空 = 与主库合并；独立 DSN 时单独连接。
    let metric_cfg = sakuio_metric::MetricDbConfig::load(&pool).await?;
    let metric_store = Arc::new(sakuio_metric::MetricStore::connect(&pool, &metric_cfg).await?);
    let (metric_writer, metric_handle) = MetricWriter::start(
        pool.clone(),
        metric_store.clone(),
        MetricWriterConfig::default(),
    );
    metric_handle.reload_policy(&metric_store).await;
    let theme = Arc::new(
        theme::ThemeService::new(pool.clone(), "./data/theme")
            .await
            .map_err(|e| anyhow::anyhow!("initialize theme service: {e}"))?,
    );
    let metric_runtime = Arc::new(database_api::MetricRuntime {
        store: metric_handle.receiver(),
        handle: metric_handle,
        tasks: db_task::DbTaskManager::default(),
        main_kind: app_db.kind(),
        main_dsn: app_db.dsn().to_string(),
    });

    let dashboard_service = Arc::new(dashboard::DashboardService::new(
        pool.clone(),
        metric_runtime.clone(),
    ));
    let ddns_service = Arc::new(ddns::DdnsService::new(pool.clone()));
    let board = Arc::new(MemoryBoardStore::default());
    let board_dyn: Arc<dyn BoardStateStore> = board.clone();
    let command_bus = AgentCommandBus::default();
    let ping_scheduler = Arc::new(PingScheduler::new(
        pool.clone(),
        board_dyn.clone(),
        command_bus.clone(),
    ));
    let metric_writer = Arc::new(metric_writer);
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
        metric: metric_runtime,
        theme,
        audit,
        geo,
        dashboard: dashboard_service,
        ddns: ddns_service.clone(),
    };

    notification_api::spawn_notification_watcher(app_state.clone());
    geoip_resolver::spawn_geoip_resolver(app_state.clone());
    billing_watcher::spawn_billing_watcher(app_state.clone());
    presence_watcher::spawn_presence_watcher(app_state.clone()).await;
    app_state.dashboard.spawn();
    app_state.ddns.spawn();
    site_settings::bootstrap_defaults(&app_state).await;

    let admin_protected = remote::fs_routes()
        .route("/api/admin/nodes/:uuid/shell", get(remote::shell_ws))
        .route(
            "/api/admin/clients",
            get(api::list_admin_clients).post(api::add_client),
        )
        .route("/api/admin/clients/stream", get(api::stream_admin_clients))
        .route("/api/admin/agents/version", get(api::agent_version))
        .route(
            "/api/admin/clients/:uuid/update-agent",
            post(api::update_admin_agent),
        )
        .route("/api/admin/clients/sync", post(api::sync_admin_clients))
        .route(
            "/api/admin/clients/:uuid",
            patch(api::patch_admin_client).delete(api::delete_admin_client),
        )
        .route("/api/admin/clients/reorder", put(api::reorder_client))
        .route(
            "/api/admin/clients/batch-delete",
            post(api::batch_delete_clients),
        )
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
            "/api/admin/settings/database",
            get(database_api::get_database_info).put(database_api::update_database),
        )
        .route(
            "/api/admin/settings/database/main",
            put(database_api::update_main_database),
        )
        .route(
            "/api/admin/settings/database/migrate",
            post(database_api::start_migration),
        )
        .route(
            "/api/admin/settings/database/migrate/status",
            get(database_api::migration_status),
        )
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
            "/api/admin/notification/instances/:id/enabled",
            patch(notification_api::set_instance_enabled),
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
        .route(
            "/api/admin/notification/rules/:id/enabled",
            patch(notification_api::set_rule_enabled),
        )
        .route("/api/admin/pprof/summary", get(pprof_api::summary_endpoint))
        .route("/api/admin/pprof/stream", get(pprof_api::stream_endpoint))
        .route("/api/admin/dashboard", get(dashboard::bootstrap))
        .route("/api/admin/dashboard/stream", get(dashboard::stream))
        .route("/api/admin/dashboard/:component", get(dashboard::component))
        .route(
            "/api/admin/dashboard/layout",
            get(dashboard::get_layout).put(dashboard::put_layout),
        )
        .route("/api/admin/logs", get(audit_api::list_logs))
        .route("/api/admin/logs/ip-geo", get(audit_api::ip_geo))
        .route("/api/admin/themes", get(theme_api::list))
        .route(
            "/api/admin/themes/upload",
            post(theme_api::upload).layer(axum::extract::DefaultBodyLimit::max(128 << 20)),
        )
        .route("/api/admin/themes/market", get(theme_api::market))
        .route("/api/admin/themes/jobs/:id", get(theme_api::job_status))
        .route(
            "/api/admin/themes/jobs/:id/events",
            get(theme_api::job_events),
        )
        .route(
            "/api/admin/themes/market/:short/install",
            post(theme_api::install_market),
        )
        .route(
            "/api/admin/themes/:short/config",
            get(theme_api::get_config).put(theme_api::save_config),
        )
        .route("/api/admin/themes/:short/preview", get(theme_api::preview))
        .route("/api/admin/themes/:short/apply", post(theme_api::apply))
        .route("/api/admin/themes/:short/update", post(theme_api::update))
        .route(
            "/api/admin/themes/:short",
            axum::routing::delete(theme_api::delete),
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
        .merge(ddns::router())
        // Maintenance runs after auth (auth is the outer layer) and before
        // the handler, so authenticated writes get a clean 503.
        // 维护门禁在鉴权之后、处理器之前执行，已认证的写请求会得到 503。
        .route_layer(middleware::from_fn_with_state(
            app_state.clone(),
            audit::audit_admin_layer,
        ))
        .route_layer(middleware::from_fn(maintenance_guard::guard))
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
        .route("/api/agent/relay/:relay_id", get(remote::agent_relay_ws))
        .route(WS_LINK_PATH, get(api::agent_link))
        .route("/api/install/status", get(api::install_status))
        .route("/api/install/complete", post(api::install_complete))
        .route("/api/install/test-db", post(api::install_test_db))
        .fallback(theme_serve)
        .layer(TraceLayer::new_for_http())
        .with_state(app_state);

    let addr: SocketAddr = match std::env::var("SAKUIO_LISTEN") {
        Ok(value) => value
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid SAKUIO_LISTEN {value:?}: {e}"))?,
        Err(_) => "0.0.0.0:8080".parse().unwrap(),
    };
    tracing::info!(%addr, path = WS_LINK_PATH, "sakuio-server listening");
    // 自愈重启（主库切换）时子进程可能与旧进程短暂争抢端口，重试几次。
    let listener = 'bind: {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                let mut last = e;
                for _ in 0..10 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    match tokio::net::TcpListener::bind(addr).await {
                        Ok(l) => break 'bind l,
                        Err(e2) => last = e2,
                    }
                }
                return Err(last.into());
            }
            Err(e) => return Err(e.into()),
        }
    };
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
