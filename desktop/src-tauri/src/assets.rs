use axum::{body::Body, http::{StatusCode, Uri, header}, response::{IntoResponse, Response}};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../../frontend/public"]
struct Frontend;

pub async fn serve(uri: Uri) -> Response {
    let requested = uri.path().trim_start_matches('/');
    let asset_path = if requested.is_empty() { "index.html" } else { requested };
    if let Some(asset) = Frontend::get(asset_path) {
        return asset_response(asset_path, asset.data.into_owned());
    }
    if !is_api_path(requested) && !requested.rsplit('/').next().unwrap_or_default().contains('.') {
        if let Some(index) = Frontend::get("index.html") {
            return asset_response("index.html", index.data.into_owned());
        }
    }
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        r#"{"detail":"Not Found"}"#,
    ).into_response()
}

fn asset_response(path: &str, data: Vec<u8>) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let cache = if path.starts_with("assets/") { "no-cache, no-store, must-revalidate" } else { "no-cache" };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::CACHE_CONTROL, cache)
        .body(Body::from(data))
        .expect("static response")
}

fn is_api_path(path: &str) -> bool {
    ["api", "v1", "v1beta", "claudecode", "codex"]
        .iter()
        .any(|prefix| path == *prefix || path.starts_with(&format!("{prefix}/")))
}
