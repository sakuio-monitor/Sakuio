//! Portable SQL execution over sqlx's runtime `Any` driver.
//! 基于 sqlx 运行时 `Any` 驱动的可移植 SQL 执行层。
//!
//! `sqlx::Any` forwards SQL verbatim to the selected backend, so PostgreSQL
//! would receive `?` placeholders it cannot parse. `DbPool` centralises the
//! dialect fix-ups:
//!
//! - `?` placeholders are numbered (`$1`, `$2`, …) for PostgreSQL.
//! - Identifier quoting helper for reserved words such as MySQL `key`.
//!
//! Because all repositories execute through this wrapper, call sites keep
//! writing portable `?` SQL and never branch on the backend.
//!
//! `sqlx::Any` 会把 SQL 原样转交后端，PostgreSQL 无法解析 `?` 占位符。
//! `DbPool` 统一做方言修正：PostgreSQL 下把 `?` 编号为 `$1`、`$2`…，
//! 并提供保留字标识符引用。仓储层因此始终写可移植的 `?` SQL。

use crate::schema::DbKind;
use futures_util::{stream, StreamExt};
use sqlx::any::{Any, AnyPoolOptions, AnyQueryResult, AnyRow, AnyTypeInfo, AnyTypeInfoKind};
use sqlx::{AnyPool, Either, Error, Executor, Transaction};
use sqlx_core::any::AnyValueKind;
use std::borrow::Cow;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Process-wide database operation counters (all pools/transactions).
/// 进程级数据库操作计数（所有连接池/事务）。
pub static DB_QUERIES: AtomicU64 = AtomicU64::new(0);
pub static DB_WRITES: AtomicU64 = AtomicU64::new(0);

fn note_query(sql: &str) {
    DB_QUERIES.fetch_add(1, Ordering::Relaxed);
    let first = sql
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if matches!(
        first.as_str(),
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "CREATE" | "ALTER" | "DROP" | "TRUNCATE"
    ) {
        DB_WRITES.fetch_add(1, Ordering::Relaxed);
    }
}

/// Connection pool bound to a concrete backend at runtime.
/// 运行时绑定具体后端的连接池。
#[derive(Clone)]
pub struct DbPool {
    inner: AnyPool,
    kind: DbKind,
    dsn: String,
    max_connections: u32,
}

impl fmt::Debug for DbPool {
    // Never print the DSN: it may carry a password.
    // 不打印 DSN：可能包含密码。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbPool").field("kind", &self.kind).finish()
    }
}

impl DbPool {
    /// Opens a pool with the given pool options and detects the backend from `dsn`.
    /// 使用给定连接池参数打开连接，并从 `dsn` 识别后端。
    pub async fn connect_with(options: AnyPoolOptions, dsn: &str) -> Result<Self, Error> {
        sqlx::any::install_default_drivers();
        let kind = DbKind::detect(dsn);
        let max_connections = options.get_max_connections();
        let inner = options.connect(dsn).await?;
        // WAL is a file-level setting; enabling it once at open time avoids
        // writer/reader contention on the default SQLite journal.
        // WAL 是文件级设置，打开时启用一次即可降低 SQLite 读写争用。
        if kind == DbKind::Sqlite {
            if let Err(error) = sqlx::query("PRAGMA journal_mode=WAL").execute(&inner).await {
                tracing::warn!(error = %error, "enable sqlite WAL failed");
            }
        }
        Ok(Self {
            inner,
            kind,
            dsn: dsn.to_string(),
            max_connections,
        })
    }

    /// Opens a pool with default limits.
    /// 使用默认连接数打开连接池。
    pub async fn connect(dsn: &str) -> Result<Self, Error> {
        Self::connect_with(
            AnyPoolOptions::new()
                .max_connections(16)
                .min_connections(0)
                .idle_timeout(Duration::from_secs(60))
                .max_lifetime(Duration::from_secs(30 * 60))
                .acquire_timeout(Duration::from_secs(15)),
            dsn,
        )
        .await
    }

    pub fn kind(&self) -> DbKind {
        self.kind
    }

    /// Effective DSN exactly as handed to sqlx.
    /// 交给 sqlx 的实际 DSN。
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }

    pub fn inner(&self) -> &AnyPool {
        &self.inner
    }

    /// Starts a transaction whose statements are dialect-rewritten as well.
    /// 开启事务；事务内的语句同样做方言转换。
    pub async fn begin(&self) -> Result<DbTx<'_>, Error> {
        Ok(DbTx {
            inner: self.inner.begin().await?,
            kind: self.kind,
        })
    }

    /// Closes the pool, waiting for in-flight statements.
    /// 关闭连接池并等待在途语句结束。
    pub async fn close(&self) {
        self.inner.close().await;
    }

    /// Runs a connectivity probe.
    /// 执行连通性探测。
    pub async fn ping(&self) -> Result<(), Error> {
        sqlx::query("SELECT 1").execute(self).await?;
        Ok(())
    }
}

impl std::ops::Deref for DbPool {
    type Target = AnyPool;

    /// Raw pool access for APIs that only need pool internals (never SQL).
    /// 仅供需要连接池内部信息的 API 使用（不用于执行 SQL）。
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'c> Executor<'c> for &'c DbPool {
    type Database = Any;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> futures_util::stream::BoxStream<'e, Result<Either<AnyQueryResult, AnyRow>, Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Any>,
    {
        let kind = self.kind;
        let pool = self.inner.clone();
        let sql = query.sql().to_string();
        let arguments = query.take_arguments();
        note_query(&sql);
        let future = async move {
            let rendered = render_sql(kind, &sql);
            match arguments {
                Ok(Some(arguments)) => {
                    let query = sqlx::query_with::<Any, _>(&rendered, arguments);
                    normalize_rows(
                        kind,
                        Executor::fetch_many(&pool, query).collect::<Vec<_>>().await,
                    )
                }
                Ok(None) => {
                    let query = sqlx::query::<Any>(&rendered);
                    normalize_rows(
                        kind,
                        Executor::fetch_many(&pool, query).collect::<Vec<_>>().await,
                    )
                }
                Err(error) => vec![Err(Error::Encode(error))],
            }
        };
        stream::once(future).flat_map(stream::iter).boxed()
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_util::future::BoxFuture<'e, Result<Option<AnyRow>, Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Any>,
    {
        let mut stream = Executor::fetch_many(self, query);
        Box::pin(async move {
            while let Some(step) = stream.next().await {
                if let Either::Right(row) = step? {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [sqlx::any::AnyTypeInfo],
    ) -> futures_util::future::BoxFuture<'e, Result<sqlx::any::AnyStatement<'q>, Error>> {
        // Prepared statements are not rewritten; runtime execution always goes
        // through `fetch_many`, so portable `?` SQL is handled there.
        // 预编译语句不做重写；运行时执行统一走 `fetch_many`，可移植 SQL 在那里处理。
        Executor::prepare_with(&self.inner, sql, parameters)
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures_util::future::BoxFuture<'e, Result<sqlx::Describe<Any>, Error>> {
        Executor::describe(&self.inner, sql)
    }
}

/// Transaction wrapper with the same dialect rewriting as [`DbPool`].
/// 与 [`DbPool`] 同样做方言转换的事务包装。
pub struct DbTx<'a> {
    inner: Transaction<'a, Any>,
    kind: DbKind,
}

impl fmt::Debug for DbTx<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbTx").field("kind", &self.kind).finish()
    }
}

impl DbTx<'_> {
    pub async fn commit(self) -> Result<(), Error> {
        self.inner.commit().await
    }

    pub async fn rollback(self) -> Result<(), Error> {
        self.inner.rollback().await
    }
}

impl<'c, 'a: 'c> Executor<'c> for &'c mut DbTx<'a> {
    type Database = Any;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> futures_util::stream::BoxStream<'e, Result<Either<AnyQueryResult, AnyRow>, Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Any>,
    {
        let kind = self.kind;
        let sql = query.sql().to_string();
        let arguments = query.take_arguments();
        note_query(&sql);
        let transaction: &'c mut Transaction<'a, Any> = &mut self.inner;
        let future = async move {
            let rendered = render_sql(kind, &sql);
            match arguments {
                Ok(Some(arguments)) => {
                    let query = sqlx::query_with::<Any, _>(&rendered, arguments);
                    normalize_rows(
                        kind,
                        Executor::fetch_many(&mut **transaction, query)
                            .collect::<Vec<_>>()
                            .await,
                    )
                }
                Ok(None) => {
                    let query = sqlx::query::<Any>(&rendered);
                    normalize_rows(
                        kind,
                        Executor::fetch_many(&mut **transaction, query)
                            .collect::<Vec<_>>()
                            .await,
                    )
                }
                Err(error) => vec![Err(Error::Encode(error))],
            }
        };
        stream::once(future).flat_map(stream::iter).boxed()
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> futures_util::future::BoxFuture<'e, Result<Option<AnyRow>, Error>>
    where
        'c: 'e,
        E: 'q + sqlx::Execute<'q, Any>,
    {
        let mut stream = Executor::fetch_many(self, query);
        Box::pin(async move {
            while let Some(step) = stream.next().await {
                if let Either::Right(row) = step? {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [sqlx::any::AnyTypeInfo],
    ) -> futures_util::future::BoxFuture<'e, Result<sqlx::any::AnyStatement<'q>, Error>>
    where
        'a: 'e,
        'c: 'e,
    {
        let transaction: &'c mut Transaction<'a, Any> = &mut self.inner;
        Box::pin(async move { Executor::prepare_with(&mut **transaction, sql, parameters).await })
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> futures_util::future::BoxFuture<'e, Result<sqlx::Describe<Any>, Error>>
    where
        'a: 'e,
        'c: 'e,
    {
        let transaction: &'c mut Transaction<'a, Any> = &mut self.inner;
        Box::pin(async move { Executor::describe(&mut **transaction, sql).await })
    }
}

/// MySQL reports `TEXT` columns as `BLOB`, which the `Any` driver exposes as
/// `AnyValueKind::Blob`; `String` therefore refuses to decode them. Since this
/// application stores no binary columns, valid UTF-8 blobs are promoted to
/// text values (and their column type updated) so one portable `String` query
/// works on every backend.
/// MySQL 把 `TEXT` 列报告为 `BLOB`，`Any` 驱动以 `Blob` 值返回，导致
/// `String` 拒绝解码。本项目没有二进制列，因此把合法 UTF-8 的 Blob 提升为
/// 文本值并同步列类型，使同一份 `String` 查询在三端通用。
fn normalize_rows(
    kind: DbKind,
    rows: Vec<Result<Either<AnyQueryResult, AnyRow>, Error>>,
) -> Vec<Result<Either<AnyQueryResult, AnyRow>, Error>> {
    if kind != DbKind::MySql {
        return rows;
    }
    rows.into_iter()
        .map(|row| {
            row.map(|step| match step {
                Either::Right(row) => Either::Right(normalize_row(row)),
                left => left,
            })
        })
        .collect()
}

fn normalize_row(mut row: AnyRow) -> AnyRow {
    for (index, value) in row.values.iter_mut().enumerate() {
        let AnyValueKind::Blob(bytes) = &value.kind else {
            continue;
        };
        let Ok(text) = std::str::from_utf8(bytes) else {
            continue;
        };
        value.kind = AnyValueKind::Text(std::borrow::Cow::Owned(text.to_string()));
        if let Some(column) = row.columns.get_mut(index) {
            column.type_info = AnyTypeInfo {
                kind: AnyTypeInfoKind::Text,
            };
        }
    }
    row
}

/// Rewrites `?` placeholders to `$1..$n` for PostgreSQL; other backends are
/// returned untouched. Placeholders inside string literals, quoted
/// identifiers and comments are ignored.
/// PostgreSQL 下把 `?` 重写为 `$1..$n`；其他后端原样返回。
/// 字符串字面量、引号标识符与注释内的 `?` 不会被处理。
pub fn render_sql(kind: DbKind, sql: &str) -> Cow<'_, str> {
    if kind != DbKind::Postgres || !sql.contains('?') {
        return Cow::Borrowed(sql);
    }
    let mut out = String::with_capacity(sql.len() + 8);
    let mut chars = sql.chars().peekable();
    let mut placeholder = 0usize;
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                out.push(ch);
                copy_quoted(&mut chars, &mut out, '\'');
                continue;
            }
            '"' => {
                out.push(ch);
                copy_quoted(&mut chars, &mut out, '"');
                continue;
            }
            '-' if chars.peek() == Some(&'-') => {
                out.push(ch);
                out.push(chars.next().unwrap_or('-'));
                for next in chars.by_ref() {
                    out.push(next);
                    if next == '\n' {
                        break;
                    }
                }
                continue;
            }
            '/' if chars.peek() == Some(&'*') => {
                out.push(ch);
                out.push(chars.next().unwrap_or('*'));
                let mut prev = '\0';
                for next in chars.by_ref() {
                    out.push(next);
                    if prev == '*' && next == '/' {
                        break;
                    }
                    prev = next;
                }
                continue;
            }
            '?' => {
                placeholder += 1;
                out.push('$');
                out.push_str(&placeholder.to_string());
                continue;
            }
            _ => out.push(ch),
        }
    }
    Cow::Owned(out)
}

fn copy_quoted(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    out: &mut String,
    quote: char,
) {
    // Doubled quotes are escapes: `''` / `""`.
    // 连续两个引号是转义写法。
    while let Some(ch) = chars.next() {
        out.push(ch);
        if ch != quote {
            continue;
        }
        if chars.peek() == Some(&quote) {
            out.push(chars.next().unwrap_or(quote));
            continue;
        }
        break;
    }
}

/// Quotes an identifier for the backend. Only needed for reserved words, e.g.
/// MySQL's `key`.
/// 按后端引用标识符，仅用于保留字（如 MySQL 的 `key`）。
pub fn quote_ident(kind: DbKind, name: &str) -> String {
    match kind {
        DbKind::MySql => format!("`{name}`"),
        DbKind::Sqlite | DbKind::Postgres => format!("\"{name}\""),
    }
}

/// Normalises a DSN so the same physical database compares equal: SQLite
/// paths are canonicalised, network DSNs drop credentials and normalise
/// scheme/host casing.
/// 规范化 DSN：SQLite 路径取真实路径，网络 DSN 去掉账号密码并统一
/// scheme/主机大小写。
pub fn normalize_dsn(dsn: &str) -> String {
    let dsn = dsn.trim();
    let lower = dsn.to_ascii_lowercase();
    if lower.starts_with("sqlite:") || !lower.contains("://") {
        return normalize_sqlite_dsn(dsn);
    }
    let (scheme, rest) = match dsn.split_once("://") {
        Some((scheme, rest)) => (scheme.to_ascii_lowercase(), rest),
        None => return dsn.to_ascii_lowercase(),
    };
    // Drop userinfo (everything before the last '@' in the authority).
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, database) = rest.split_at(authority_end);
    let host = authority.rsplit('@').next().unwrap_or(authority);
    let scheme = match scheme.as_str() {
        "postgresql" => "postgres",
        "mariadb" => "mysql",
        other => other,
    };
    let database = database.trim_end_matches('/');
    format!("{scheme}://{}{database}", host.to_ascii_lowercase())
}

fn normalize_sqlite_dsn(dsn: &str) -> String {
    let body = dsn
        .strip_prefix("sqlite://")
        .or_else(|| dsn.strip_prefix("sqlite:"))
        .unwrap_or(dsn);
    let body = body.split('?').next().unwrap_or(body);
    let body = body.trim_start_matches("//");
    let body = if body.is_empty() { ":memory:" } else { body };
    if body == ":memory:" {
        return "sqlite::memory:".into();
    }
    let path = std::path::Path::new(body);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    // Canonicalize when the file already exists; otherwise normalise lexically.
    // 文件存在时取真实路径，否则做词法归一化。
    let canonical =
        std::fs::canonicalize(&absolute).unwrap_or_else(|_| lexical_normalize(&absolute));
    canonical.to_string_lossy().into_owned()
}

fn lexical_normalize(path: &std::path::Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Returns true when both pools point at the same physical database.
/// 判断两个连接池是否指向同一个物理数据库。
pub async fn is_same_database(left: &DbPool, right: &DbPool) -> Result<bool, Error> {
    if left.kind != right.kind {
        return Ok(false);
    }
    if left.kind == DbKind::Sqlite {
        return Ok(normalize_dsn(left.dsn()) == normalize_dsn(right.dsn()));
    }
    let (sql, columns) = match left.kind {
        DbKind::Postgres => (
            "SELECT current_database()::text, \
             COALESCE(inet_server_addr()::text, ''), \
             COALESCE(inet_server_port(), 0)::text",
            3usize,
        ),
        DbKind::MySql => (
            "SELECT DATABASE(), @@hostname, CAST(@@port AS CHAR)",
            3usize,
        ),
        DbKind::Sqlite => unreachable!(),
    };
    let left_row = sqlx::query(sql).fetch_one(left).await?;
    let right_row = sqlx::query(sql).fetch_one(right).await?;
    use sqlx::Row;
    let mut equal = true;
    for index in 0..columns {
        let a: Option<String> = left_row.try_get(index).unwrap_or(None);
        let b: Option<String> = right_row.try_get(index).unwrap_or(None);
        if a != b {
            equal = false;
            break;
        }
    }
    Ok(equal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_placeholders_for_postgres() {
        let sql = "SELECT * FROM clients WHERE uuid = ? AND online = ?";
        assert_eq!(
            render_sql(DbKind::Postgres, sql),
            "SELECT * FROM clients WHERE uuid = $1 AND online = $2"
        );
    }

    #[test]
    fn ignores_placeholders_in_literals_and_comments() {
        let sql = "SELECT '?' AS q, \"?\" AS c FROM t WHERE a = ? -- ?\n AND b = ?";
        assert_eq!(
            render_sql(DbKind::Postgres, sql),
            "SELECT '?' AS q, \"?\" AS c FROM t WHERE a = $1 -- ?\n AND b = $2"
        );
    }

    #[test]
    fn leaves_other_dialects_untouched() {
        let sql = "SELECT ?";
        assert_eq!(render_sql(DbKind::Sqlite, sql), sql);
        assert_eq!(render_sql(DbKind::MySql, sql), sql);
    }

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident(DbKind::MySql, "key"), "`key`");
        assert_eq!(quote_ident(DbKind::Postgres, "key"), "\"key\"");
    }

    #[test]
    fn normalizes_sqlite_paths() {
        let a = normalize_dsn("sqlite://data/sakuio.db?mode=rwc");
        let b = normalize_dsn("./data/sakuio.db");
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn executes_bound_queries_through_wrapper() {
        let path = std::env::temp_dir().join(format!(
            "sakuio-dialect-{}-{}.db",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let dsn = format!("sqlite://{}?mode=rwc", path.display());
        let pool = DbPool::connect(&dsn).await.expect("connect sqlite");
        sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .execute(&pool)
            .await
            .expect("create");
        let mut tx = pool.begin().await.expect("begin");
        sqlx::query("INSERT INTO t (name) VALUES (?)")
            .bind("a")
            .execute(&mut tx)
            .await
            .expect("insert");
        tx.commit().await.expect("commit");
        let rows: Vec<(i64, String)> = sqlx::query_as("SELECT id, name FROM t WHERE name = ?")
            .bind("a")
            .fetch_all(&pool)
            .await
            .expect("select");
        assert_eq!(rows, vec![(1, "a".to_string())]);
        pool.close().await;
        let _ = std::fs::remove_file(path);
    }
}
