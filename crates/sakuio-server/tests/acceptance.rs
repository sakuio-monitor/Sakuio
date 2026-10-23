//! End-to-end acceptance: run the real server with SQLite / PostgreSQL /
//! MySQL / MariaDB as its main database, and hot-switch the metric database
//! across all of them.
//! 端到端验收：以 SQLite / PostgreSQL / MySQL / MariaDB 作为主库启动真实服务，
//! 并在所有引擎之间热切换指标库。
//!
//! The environment variables mirror `scripts/test-backends.sh`; unavailable
//! engines are skipped so `cargo test` stays green without containers.

use sakuio_db::{
    complete_install, normalize_dsn, AppDatabase, CompleteInstallRequest, DbKind,
    SettingsRepository, TABLES,
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct Backend {
    name: &'static str,
    driver: &'static str,
    kind: DbKind,
    dsn: String,
    sqlite_file: Option<PathBuf>,
}

impl Backend {
    fn all() -> Vec<Self> {
        let sqlite_path = std::env::temp_dir().join(format!("{}.db", unique("sakuio-e2e-main-")));
        let mut backends = vec![Self {
            name: "sqlite",
            driver: "sqlite",
            kind: DbKind::Sqlite,
            dsn: format!("sqlite://{}?mode=rwc", sqlite_path.display()),
            sqlite_file: Some(sqlite_path),
        }];
        for (name, driver, var, kind) in [
            (
                "postgres",
                "postgresql",
                "SAKUIO_TEST_POSTGRES_DSN",
                DbKind::Postgres,
            ),
            ("mysql", "mysql", "SAKUIO_TEST_MYSQL_DSN", DbKind::MySql),
            ("mariadb", "mysql", "SAKUIO_TEST_MARIADB_DSN", DbKind::MySql),
        ] {
            if let Ok(dsn) = std::env::var(var) {
                let dsn = dsn.trim().to_string();
                if !dsn.is_empty() {
                    backends.push(Self {
                        name,
                        driver,
                        kind,
                        dsn,
                        sqlite_file: None,
                    });
                }
            }
        }
        backends
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        if let Some(path) = &self.sqlite_file {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(path.with_extension("db-wal"));
            let _ = std::fs::remove_file(path.with_extension("db-shm"));
        }
    }
}

struct ServerGuard {
    child: Child,
    dir: PathBuf,
    port: u16,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // A restart re-execs the server, so the port may belong to a new pid.
        // 重启会重新 exec 服务，端口可能已属于新进程。
        kill_port(self.port);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Both acceptance tests mutate the same containers; run them serially even
/// when the harness uses threads.
/// 两个验收测试会改动同一批容器，测试框架多线程运行时也强制串行。
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn server_runs_on_every_main_database_and_switches_metrics() {
    let _serial = SERIAL.lock().await;
    let mains = Backend::all();
    let metrics = Backend::all();
    let mut index = 0usize;

    for main in &mains {
        for metric in &metrics {
            index += 1;
            eprintln!(
                "e2e matrix [{index}]: main={} metrics={}",
                main.name, metric.name
            );
            run_case(main, metric, index).await;
        }
    }
}

async fn run_case(main: &Backend, metric: &Backend, index: usize) {
    let admin = format!("admin{}", unique(""));
    let password = "it-password-123";
    let prefix = format!("it{index}_");

    let dir = std::env::temp_dir().join(unique("sakuio-e2e-run-"));
    std::fs::create_dir_all(&dir).expect("create run dir");

    let db = AppDatabase::connect(&main.dsn, true)
        .await
        .unwrap_or_else(|e| panic!("main {} connect: {e}", main.name));
    reset_database(&db).await;
    db.shutdown().await;

    install_into(main, &dir, &admin, password).await;

    let port = free_port();
    let log_path = dir.join("server.log");
    let log = std::fs::File::create(&log_path).expect("create server log");
    let child = Command::new(env!("CARGO_BIN_EXE_sakuio-server"))
        .current_dir(&dir)
        .env("SAKUIO_LISTEN", format!("127.0.0.1:{port}"))
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn server");
    let mut server = ServerGuard { child, dir, port };
    wait_ready(port, &mut server, &log_path);

    let jar = server.dir.join("cookies.txt");
    let login = curl(
        port,
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-c",
            jar.to_str().unwrap(),
            "-d",
            &format!(r#"{{"username":"{admin}","password":"{password}"}}"#),
            "/api/login",
        ],
        None,
    );
    assert_eq!(login.0, 200, "{} login body: {}", main.name, login.1);

    let info = curl(
        port,
        &["-b", jar.to_str().unwrap(), "/api/admin/settings/database"],
        None,
    );
    assert_eq!(info.0, 200, "{} database info: {}", main.name, info.1);
    let info: serde_json::Value = serde_json::from_str(&info.1).expect("database info json");
    assert_eq!(
        info["main"]["kind"].as_str(),
        Some(main.kind.as_str()),
        "{} main kind",
        main.name
    );
    assert_eq!(
        info["main"]["dsn"].as_str().map(normalize_dsn),
        Some(normalize_dsn(&main.dsn)),
        "{} main dsn",
        main.name
    );

    // Switching to the database already in use must be rejected before any
    // table is cleared.
    // 切换到当前正在使用的库必须在清表前被拒绝。
    let same_body = serde_json::json!({
        "driver": main.driver,
        "dsn": main.dsn,
    })
    .to_string();
    let same = curl(
        port,
        &[
            "-X",
            "PUT",
            "-H",
            "Content-Type: application/json",
            "-b",
            jar.to_str().unwrap(),
            "-d",
            &same_body,
            "/api/admin/settings/database/main",
        ],
        None,
    );
    assert_eq!(
        same.0, 400,
        "{}: switching to the current database must fail: {}",
        main.name, same.1
    );

    // Hot-switch the metric store to the target engine.
    // 热切换指标库到目标引擎。
    let body = serde_json::json!({
        "dsn": metric.dsn,
        "table_prefix": prefix,
    })
    .to_string();
    let switched = curl(
        port,
        &[
            "-X",
            "PUT",
            "-H",
            "Content-Type: application/json",
            "-b",
            jar.to_str().unwrap(),
            "-d",
            &body,
            "/api/admin/settings/database",
        ],
        Some(60),
    );
    assert_eq!(
        switched.0, 200,
        "{}->{} metric switch: {}",
        main.name, metric.name, switched.1
    );

    let expected = match metric.kind {
        DbKind::Sqlite => "sqlite",
        DbKind::MySql => "mysql",
        DbKind::Postgres => "postgres",
    };
    let mut last = String::new();
    let mut ok = false;
    for _ in 0..60 {
        let info = curl(
            port,
            &["-b", jar.to_str().unwrap(), "/api/admin/settings/database"],
            None,
        );
        last = info.1.clone();
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&info.1) {
            if value["metrics"]["kind"].as_str() == Some(expected)
                && value["migration"]["running"].as_bool() == Some(false)
            {
                ok = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(
        ok,
        "{}->{} metric kind {} not observed: {}",
        main.name, metric.name, expected, last
    );

    // Metric tables must exist on the target with the custom prefix.
    // 目标库中必须已按自定义前缀建好指标表。
    let metric_db = AppDatabase::connect(&metric.dsn, true)
        .await
        .unwrap_or_else(|e| panic!("{} metric db connect: {e}", metric.name));
    let probe = format!("SELECT COUNT(*) FROM {}{}", prefix, "buckets");
    let counted: (i64,) = sqlx::query_as(&probe)
        .fetch_one(metric_db.pool())
        .await
        .unwrap_or_else(|e| panic!("{} prefixed metric table missing: {e}", metric.name));
    assert!(counted.0 >= 0);
    metric_db.shutdown().await;

    drop(server);
}

/// Writes the admin/site data into the selected database and persists the
/// locator in a temporary bootstrap SQLite, exactly like the install API.
/// 把管理员/站点数据写入所选数据库，并在临时 bootstrap SQLite 中写入定位信息。
async fn install_into(main: &Backend, run_dir: &Path, admin: &str, password: &str) {
    // The server resolves the real database from `./data/sakuio.db` at startup.
    // 服务启动时从 `./data/sakuio.db` 解析真正的主库。
    let bootstrap_path = run_dir.join("data/sakuio.db");
    let bootstrap = AppDatabase::connect(
        &format!("sqlite://{}?mode=rwc", bootstrap_path.display()),
        true,
    )
    .await
    .expect("bootstrap connect");
    let target = AppDatabase::connect(&main.dsn, true)
        .await
        .unwrap_or_else(|e| panic!("{} target connect: {e}", main.name));

    let outcome = complete_install(
        target.pool(),
        bootstrap.pool(),
        CompleteInstallRequest {
            username: admin.to_string(),
            password: password.to_string(),
            sitename: Some("Sakuio IT".into()),
            description: None,
            app_db_driver: Some(main.driver.into()),
            app_db_dsn: Some(main.dsn.clone()),
            metric_colocation: Some("same".into()),
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{} install failed: {e}", main.name));

    let locator = SettingsRepository::new(bootstrap.pool());
    let driver: Option<String> = locator
        .get_json("app_db_driver")
        .await
        .expect("locator driver");
    let dsn: Option<String> = locator.get_json("app_db_dsn").await.expect("locator dsn");
    assert_eq!(driver.as_deref(), Some(main.driver));
    assert_eq!(
        dsn.as_deref().map(normalize_dsn),
        Some(normalize_dsn(&main.dsn))
    );
    assert_eq!(
        outcome.restart_required,
        normalize_dsn(bootstrap.dsn()) != normalize_dsn(target.dsn())
    );

    // Install is idempotent-safe: a second call is rejected.
    // 安装不可重复：第二次调用会被拒绝。
    let again = complete_install(
        target.pool(),
        bootstrap.pool(),
        CompleteInstallRequest {
            username: admin.to_string(),
            password: password.to_string(),
            sitename: None,
            description: None,
            app_db_driver: Some(main.driver.into()),
            app_db_dsn: Some(main.dsn.clone()),
            metric_colocation: Some("same".into()),
        },
    )
    .await;
    assert!(again.is_err(), "{}: duplicate install accepted", main.name);

    target.shutdown().await;
    bootstrap.shutdown().await;
}

/// Drops every application and metric table, leaving an empty database.
/// 删除全部应用与指标表，得到空库。
async fn reset_database(db: &AppDatabase) {
    let kind = db.kind();
    let pool = db.pool();
    let existing: Vec<String> = match kind {
        DbKind::Sqlite => {
            sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
                .fetch_all(pool)
                .await
                .unwrap_or_default()
        }
        DbKind::Postgres => {
            sqlx::query_scalar("SELECT table_name::text FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'")
                .fetch_all(pool)
                .await
                .unwrap_or_default()
        }
        DbKind::MySql => {
            sqlx::query_scalar("SELECT CAST(table_name AS CHAR) FROM information_schema.tables WHERE table_schema = DATABASE()")
                .fetch_all(pool)
                .await
                .unwrap_or_default()
        }
    };
    if existing.is_empty() {
        return;
    }
    // Children before parents for SQLite/MySQL foreign keys.
    // 先子表后父表，兼容 SQLite/MySQL 的外键。
    let mut ordered: Vec<String> = TABLES
        .iter()
        .rev()
        .map(|table| table.name.to_string())
        .filter(|name| existing.contains(name))
        .collect();
    for name in &existing {
        if !ordered.contains(name) {
            ordered.push(name.clone());
        }
    }
    for name in ordered {
        let quoted = sakuio_db::quote_ident(kind, &name);
        let suffix = if kind == DbKind::Postgres {
            " CASCADE"
        } else {
            ""
        };
        sqlx::query(&format!("DROP TABLE IF EXISTS {quoted}{suffix}"))
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("drop {name}: {e}"));
    }
}

fn wait_ready(port: u16, server: &mut ServerGuard, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(status) = server.child.try_wait().expect("try_wait") {
            let output = std::fs::read_to_string(log).unwrap_or_default();
            panic!("server exited early ({status}):\n{output}");
        }
        if curl(port, &["/ping"], None).0 == 200 {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let output = std::fs::read_to_string(log).unwrap_or_default();
    panic!("server did not become ready:\n{output}");
}

/// Minimal curl wrapper: returns `(status, body)`.
/// 极简 curl 封装：返回 `(状态码, 响应体)`。
fn curl(port: u16, args: &[&str], timeout_secs: Option<u64>) -> (u16, String) {
    let body_path = std::env::temp_dir().join(format!("{}.body", unique("sakuio-curl-")));
    let url = format!("http://127.0.0.1:{port}");
    let mut command = Command::new("curl");
    command
        .arg("-sS")
        .arg("--max-time")
        .arg(timeout_secs.unwrap_or(10).to_string())
        .arg("-o")
        .arg(&body_path)
        .arg("-w")
        .arg("%{http_code}");
    // The last argument is the request path; the rest are curl flags.
    // 最后一个参数是请求路径，其余是 curl 参数。
    let (path, flags) = args.split_last().expect("path");
    for arg in flags {
        command.arg(arg);
    }
    command.arg(format!("{url}{path}"));
    let output = command.output().expect("run curl");
    let status = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u16>()
        .unwrap_or(0);
    let body = std::fs::read_to_string(&body_path).unwrap_or_default();
    let _ = std::fs::remove_file(&body_path);
    if status == 0 && std::env::var_os("SAKUIO_TEST_VERBOSE").is_some() {
        let mut stderr = std::io::stderr();
        let _ = writeln!(
            stderr,
            "curl failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    (status, body)
}

/// The install wizard writes into the selected PostgreSQL database, restarts,
/// and the restarted process finds it through the bootstrap locator.
/// 安装向导写入所选 PostgreSQL，重启后新进程通过 bootstrap 定位信息连上它。
#[tokio::test]
async fn install_can_target_postgres_and_restart() {
    let _serial = SERIAL.lock().await;
    let Ok(pg_dsn) = std::env::var("SAKUIO_TEST_POSTGRES_DSN") else {
        eprintln!("skipping postgres install test: SAKUIO_TEST_POSTGRES_DSN not set");
        return;
    };
    let pg_dsn = pg_dsn.trim().to_string();
    if pg_dsn.is_empty() {
        eprintln!("skipping postgres install test: empty SAKUIO_TEST_POSTGRES_DSN");
        return;
    }

    let db = AppDatabase::connect(&pg_dsn, true)
        .await
        .expect("postgres connect");
    reset_database(&db).await;
    db.shutdown().await;

    let port = free_port();
    let dir = std::env::temp_dir().join(unique("sakuio-pg-install-"));
    std::fs::create_dir_all(&dir).expect("create run dir");
    let log_path = dir.join("server.log");
    let log = std::fs::File::create(&log_path).expect("create server log");
    let child = Command::new(env!("CARGO_BIN_EXE_sakuio-server"))
        .current_dir(&dir)
        .env("SAKUIO_LISTEN", format!("127.0.0.1:{port}"))
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn server");
    let server = ServerGuard { child, dir, port };
    wait_ready_http(port, &log_path);

    let admin = format!("pg{}", unique(""));
    let password = "it-password-123";
    let install = curl(
        port,
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            &serde_json::json!({
                "username": admin,
                "password": password,
                "sitename": "PG Install IT",
                "app_db_driver": "postgresql",
                "app_db_dsn": pg_dsn,
                "metric_colocation": "same",
            })
            .to_string(),
            "/api/install/complete",
        ],
        Some(30),
    );
    assert_eq!(install.0, 200, "install into postgres: {}", install.1);
    wait_installed(port, &log_path);

    let jar = server.dir.join("cookies.txt");
    let jar = jar.to_str().expect("cookie path");
    assert_eq!(
        login(port, jar, &admin, password),
        200,
        "login after pg install"
    );

    let info = curl(port, &["-b", jar, "/api/admin/settings/database"], None);
    assert_eq!(info.0, 200, "database info: {}", info.1);
    let info: serde_json::Value = serde_json::from_str(&info.1).expect("info json");
    assert_eq!(info["main"]["kind"].as_str(), Some("postgres"));
    assert_eq!(
        info["main"]["dsn"].as_str().map(normalize_dsn),
        Some(normalize_dsn(&pg_dsn))
    );

    drop(server);
}

/// Full panel flow: install into a non-default SQLite database (restart),
/// then switch the main database to PostgreSQL (copy, verify, restart) and
/// confirm the session and client rows survived.
/// 完整面板流程：安装到非默认 SQLite（重启），再把主库切到 PostgreSQL
/// （复制、校验、重启），确认会话与客户端数据仍在。
#[tokio::test]
async fn main_database_switch_restarts_into_target() {
    let _serial = SERIAL.lock().await;
    let Ok(pg_dsn) = std::env::var("SAKUIO_TEST_POSTGRES_DSN") else {
        eprintln!("skipping main switch test: SAKUIO_TEST_POSTGRES_DSN not set");
        return;
    };
    let pg_dsn = pg_dsn.trim().to_string();
    if pg_dsn.is_empty() {
        eprintln!("skipping main switch test: empty SAKUIO_TEST_POSTGRES_DSN");
        return;
    }

    let port = free_port();
    let dir = std::env::temp_dir().join(unique("sakuio-switch-run-"));
    std::fs::create_dir_all(&dir).expect("create run dir");
    let sqlite_target = dir.join("target.db");
    let target_dsn = format!("sqlite://{}?mode=rwc", sqlite_target.display());
    let log_path = dir.join("server.log");
    let log = std::fs::File::create(&log_path).expect("create server log");
    let child = Command::new(env!("CARGO_BIN_EXE_sakuio-server"))
        .current_dir(&dir)
        .env("SAKUIO_LISTEN", format!("127.0.0.1:{port}"))
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn server");
    let server = ServerGuard { child, dir, port };
    wait_ready_http(port, &log_path);

    let admin = format!("switch{}", unique(""));
    let password = "it-password-123";
    let install = curl(
        port,
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            &serde_json::json!({
                "username": admin,
                "password": password,
                "sitename": "Switch IT",
                "app_db_driver": "sqlite",
                "app_db_dsn": target_dsn,
                "metric_colocation": "same",
            })
            .to_string(),
            "/api/install/complete",
        ],
        Some(30),
    );
    assert_eq!(
        install.0, 200,
        "install into selected sqlite: {}",
        install.1
    );
    wait_installed(port, &log_path);

    let jar = server.dir.join("cookies.txt");
    let jar = jar.to_str().expect("cookie path");
    let login_status = login(port, jar, &admin, password);
    assert_eq!(login_status, 200, "login after install restart");

    let created = curl(
        port,
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-b",
            jar,
            "-d",
            r#"{"name":"switch-node"}"#,
            "/api/admin/clients",
        ],
        None,
    );
    assert_eq!(created.0, 200, "create client: {}", created.1);
    let created: serde_json::Value = serde_json::from_str(&created.1).expect("client json");
    let uuid = created["uuid"].as_str().expect("client uuid").to_string();

    let switch = curl(
        port,
        &[
            "-X",
            "PUT",
            "-H",
            "Content-Type: application/json",
            "-b",
            jar,
            "-d",
            &serde_json::json!({ "driver": "postgresql", "dsn": pg_dsn }).to_string(),
            "/api/admin/settings/database/main",
        ],
        Some(30),
    );
    assert_eq!(switch.0, 200, "start main switch: {}", switch.1);

    let mut info = serde_json::Value::Null;
    let mut switched = false;
    for _ in 0..240 {
        let response = curl(port, &["-b", jar, "/api/admin/settings/database"], Some(3));
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&response.1) {
            if value["main"]["kind"].as_str() == Some("postgres")
                && value["migration"]["running"].as_bool() == Some(false)
            {
                info = value;
                switched = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(switched, "main database did not become postgres: {}", info);

    // The session row and the client row were copied to PostgreSQL.
    // 会话行与客户端行都已复制到 PostgreSQL。
    let clients = curl(port, &["-b", jar, "/api/admin/clients"], None);
    assert_eq!(clients.0, 200, "clients after switch: {}", clients.1);
    assert!(
        clients.1.contains(&uuid),
        "client {uuid} missing after switch: {}",
        clients.1
    );
    assert_eq!(
        login(port, jar, &admin, password),
        200,
        "login after switch"
    );

    // Same-DSN switch must be rejected now that the running database is PG.
    // 当前主库已是 PG，原地切换必须被拒绝。
    let same = curl(
        port,
        &[
            "-X",
            "PUT",
            "-H",
            "Content-Type: application/json",
            "-b",
            jar,
            "-d",
            &serde_json::json!({ "driver": "postgresql", "dsn": info["main"]["dsn"] }).to_string(),
            "/api/admin/settings/database/main",
        ],
        None,
    );
    assert_eq!(same.0, 400, "same database switch: {}", same.1);

    // A second hop (PostgreSQL -> MySQL) proves the copy path into MySQL too.
    // 再切一跳（PostgreSQL -> MySQL），覆盖复制到 MySQL 的路径。
    if let Ok(mysql_dsn) = std::env::var("SAKUIO_TEST_MYSQL_DSN") {
        let mysql_dsn = mysql_dsn.trim().to_string();
        if !mysql_dsn.is_empty() {
            let switch = curl(
                port,
                &[
                    "-X",
                    "PUT",
                    "-H",
                    "Content-Type: application/json",
                    "-b",
                    jar,
                    "-d",
                    &serde_json::json!({ "driver": "mysql", "dsn": mysql_dsn }).to_string(),
                    "/api/admin/settings/database/main",
                ],
                Some(30),
            );
            assert_eq!(switch.0, 200, "start mysql switch: {}", switch.1);

            let mut switched = false;
            for _ in 0..240 {
                let response = curl(port, &["-b", jar, "/api/admin/settings/database"], Some(3));
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&response.1) {
                    if value["main"]["kind"].as_str() == Some("mysql")
                        && value["migration"]["running"].as_bool() == Some(false)
                    {
                        switched = true;
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            assert!(switched, "main database did not become mysql");
            let clients = curl(port, &["-b", jar, "/api/admin/clients"], None);
            assert_eq!(clients.0, 200, "clients after mysql switch: {}", clients.1);
            assert!(
                clients.1.contains(&uuid),
                "client {uuid} missing after mysql switch: {}",
                clients.1
            );
            assert_eq!(
                login(port, jar, &admin, password),
                200,
                "login after mysql switch"
            );
        }
    }

    drop(server);
}

fn login(port: u16, jar: &str, admin: &str, password: &str) -> u16 {
    curl(
        port,
        &[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-c",
            jar,
            "-d",
            &serde_json::json!({ "username": admin, "password": password }).to_string(),
            "/api/login",
        ],
        None,
    )
    .0
}

fn wait_installed(port: u16, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let status = curl(port, &["/api/install/status"], Some(2));
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&status.1) {
            if value["required"].as_bool() == Some(false) {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!(
        "install did not complete after restart:\n{}",
        std::fs::read_to_string(log).unwrap_or_default()
    );
}

fn wait_ready_http(port: u16, log: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if curl(port, &["/ping"], None).0 == 200 {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!(
        "server did not become ready:\n{}",
        std::fs::read_to_string(log).unwrap_or_default()
    );
}

/// Kills whatever listens on `port` (the re-exec'd server included).
/// 结束监听该端口的进程（包括重启后的服务）。
fn kill_port(port: u16) {
    let Ok(output) = Command::new("lsof")
        .args(["-ti", &format!("tcp:{port}")])
        .output()
    else {
        return;
    };
    for pid in String::from_utf8_lossy(&output.stdout).split_whitespace() {
        let _ = Command::new("kill")
            .args(["-9", pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local addr").port()
}

fn unique(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let sequence = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}{}{nanos}-{sequence}", std::process::id())
}
