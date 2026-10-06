//! 内嵌前端静态资源的处理器：把 `frontend/public` 打进二进制并提供服务。
//!
//! 职责：按请求路径返回内嵌资源；对非 API、且路径末段不含 `.` 的 GET/HEAD
//! 请求回退到 `index.html`（SPA 前端路由）。非 GET/HEAD 一律 404 JSON——
//! SPA 回退只服务于页面导航，绝不把代理端点（POST）伪装成 200 HTML。
//! 边界：只处理静态资源与 SPA 回退，不涉及 `/api`、`/v1`、`/v1beta`
//! 等由 API 路由负责的请求。
//! 关键不变量：任意路径都不会 panic——命中就返回资源，未命中返回 404 JSON，
//! 构造响应失败时回落 500。

use axum::{
    body::Body,
    http::{Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../../frontend/public"]
struct Frontend;

pub async fn serve(method: Method, uri: Uri) -> Response {
    let requested = uri.path().trim_start_matches('/');
    let asset_path = if requested.is_empty() {
        "index.html"
    } else {
        requested
    };
    if let Some(asset) = Frontend::get(asset_path) {
        return asset_response(asset_path, asset.data.into_owned());
    }
    if (method == Method::GET || method == Method::HEAD)
        && !is_api_path(requested)
        && !requested
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .contains('.')
        && let Some(index) = Frontend::get("index.html")
    {
        return asset_response("index.html", index.data.into_owned());
    }
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        r#"{"detail":"Not Found"}"#,
    )
        .into_response()
}

fn asset_response(path: &str, data: Vec<u8>) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let cache = if path.starts_with("assets/") {
        "no-cache, no-store, must-revalidate"
    } else {
        "no-cache"
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::CACHE_CONTROL, cache)
        .body(Body::from(data))
        // 头部值全部来自常量/mime 表，理论不会失败；真失败也只回一个 500，
        // 不在请求路径 panic。
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "static response error").into_response()
        })
}

fn is_api_path(path: &str) -> bool {
    ["api", "v1", "v1beta"]
        .iter()
        .any(|prefix| path == *prefix || path.starts_with(&format!("{prefix}/")))
}
