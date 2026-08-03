use anyhow::{Context, Result, bail};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use url::Url;

pub const PROTOCOL_ORDER: [&str; 4] = ["openai_compatible", "openai_responses", "claude", "gemini"];
pub const PROTOCOL_ENDPOINTS: [(&str, &[&str]); 4] = [
    (
        "openai_compatible",
        &["/v1/chat/completions", "/v1/completions", "/v1/embeddings"],
    ),
    ("openai_responses", &["/v1/responses"]),
    ("claude", &["/v1/messages"]),
    (
        "gemini",
        &[
            "/v1beta/models/{model}:generateContent",
            "/v1beta/models/{model}:streamGenerateContent",
        ],
    ),
];

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

pub fn valid_protocol(value: &str) -> bool {
    PROTOCOL_ORDER.contains(&value)
}

pub fn normalize_base_url(value: &str) -> Result<String> {
    let mut url =
        Url::parse(value.trim_end_matches('/')).context("base_url 必须是绝对 HTTP(S) URL")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("base_url 必须是绝对 HTTP(S) URL");
    }
    let mut path = url.path().trim_end_matches('/').to_owned();
    for suffix in ["/v1beta", "/v1"] {
        if path.ends_with(suffix) {
            path.truncate(path.len() - suffix.len());
            break;
        }
    }
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

pub fn upstream_url(
    base_url: &str,
    path: &str,
    raw_query: Option<&str>,
    protocol: &str,
) -> Result<Url> {
    let mut base = Url::parse(base_url)?;
    let base_path = base.path().trim_end_matches('/');
    let suffix = format!("/{}", path.trim_start_matches('/'));
    let joined = if !base_path.is_empty()
        && (suffix == base_path || suffix.starts_with(&format!("{base_path}/")))
    {
        suffix
    } else {
        format!("{base_path}{suffix}")
    };
    base.set_path(&joined);
    base.set_query(raw_query.filter(|query| !query.is_empty()));
    if protocol == "gemini" {
        let retained = base
            .query_pairs()
            .filter(|(key, _)| key != "key")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();
        base.set_query(None);
        if !retained.is_empty() {
            base.query_pairs_mut().extend_pairs(retained);
        }
    }
    Ok(base)
}

pub fn outbound_headers(inbound: &HeaderMap, protocol: &str, api_key: &str) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (name, value) in inbound {
        let key = name.as_str();
        if HOP_BY_HOP.contains(&key)
            || matches!(
                key,
                "authorization" | "x-api-key" | "x-goog-api-key" | "x-local-gateway-key"
            )
        {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
    match protocol {
        "openai_compatible" | "openai_responses" => {
            headers.insert(
                axum::http::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {api_key}"))?,
            );
        }
        "claude" => {
            headers.insert(
                HeaderName::from_static("x-api-key"),
                HeaderValue::from_str(api_key)?,
            );
            headers
                .entry(HeaderName::from_static("anthropic-version"))
                .or_insert(HeaderValue::from_static("2023-06-01"));
        }
        "gemini" => {
            headers.insert(
                HeaderName::from_static("x-goog-api-key"),
                HeaderValue::from_str(api_key)?,
            );
        }
        _ => bail!("不支持的协议 {protocol}"),
    }
    Ok(headers)
}

pub fn response_headers(inbound: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in inbound {
        if HOP_BY_HOP.contains(&name.as_str()) {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }
    headers
}

pub fn inspect_request(
    protocol: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
) -> (Option<String>, bool) {
    if protocol == "gemini" {
        let model = path
            .split("/models/")
            .nth(1)
            .and_then(|value| value.split(':').next())
            .map(|value| {
                percent_encoding::percent_decode_str(value)
                    .decode_utf8_lossy()
                    .into_owned()
            });
        let stream = path.contains(":streamGenerateContent")
            || query.unwrap_or_default().contains("alt=sse");
        return (model, stream);
    }
    let value: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => return (None, false),
    };
    (
        value
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        value
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    )
}

pub fn discovery_path(protocol: &str) -> &'static str {
    if protocol == "gemini" {
        "/v1beta/models"
    } else {
        "/v1/models"
    }
}
