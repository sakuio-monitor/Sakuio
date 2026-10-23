//! HTTP guard that rejects mutating admin requests during a main-database
//! switch. Read-only requests and the database settings endpoints stay open.
//! 主库切换期间拒绝管理端写请求的 HTTP 门禁；只读请求与数据库设置端点保持可用。

use axum::{
    extract::Request,
    http::{header::RETRY_AFTER, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Rejects writes with `503` + `Retry-After` while maintenance is active.
/// 维护期间以 `503` + `Retry-After` 拒绝写请求。
pub async fn guard(request: Request, next: Next) -> Response {
    if !sakuio_db::maintenance_active() {
        return next.run(request).await;
    }
    let read_only = matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    // The switch itself, metric reconfiguration and task polling must work.
    // 切换本身、指标库重配与任务轮询必须保持可用。
    let database_api = request
        .uri()
        .path()
        .starts_with("/api/admin/settings/database");
    if read_only || database_api {
        return next.run(request).await;
    }
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(RETRY_AFTER, "2")],
        Json(json!({ "message": "database maintenance in progress" })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        routing::{get, put},
        Router,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn blocks_admin_writes_but_keeps_reads_and_database_api() {
        sakuio_db::set_maintenance(true);
        let app = Router::new()
            .route(
                "/api/admin/clients",
                get(|| async { "read" }).post(|| async { "write" }),
            )
            .route("/api/admin/settings/database", put(|| async { "db" }))
            .layer(axum::middleware::from_fn(guard));

        let write = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/admin/clients")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(write.status(), StatusCode::SERVICE_UNAVAILABLE);

        let read = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/admin/clients")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);

        let database = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/admin/settings/database")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(database.status(), StatusCode::OK);

        sakuio_db::set_maintenance(false);
    }
}
