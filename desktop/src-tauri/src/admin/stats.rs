//! admin API 域模块：统计汇总与时间序列（stats 域）
//! 每个域文件包含该资源的 handler（薄壳）与输入/输出类型；
//! 直写 SQL 的域逻辑正逐步收敛到 super::AdminService。

use super::*;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) const CACHE_PROVIDER_PROTOCOLS: [(&str, &str); 5] = [
    ("openai_compatible", "OpenAI"),
    ("openai_responses", "OpenAI"),
    ("claude", "Claude"),
    ("gemini", "Gemini"),
    ("command_code", "Command Code"),
];
pub(super) const CACHE_PROVIDER_ORDER: [&str; 4] = ["OpenAI", "Claude", "Gemini", "Command Code"];

pub(super) fn cache_provider(protocol: &str) -> String {
    CACHE_PROVIDER_PROTOCOLS
        .iter()
        .find(|(id, _)| *id == protocol)
        .map(|(_, provider)| (*provider).to_string())
        .unwrap_or_else(|| protocol.to_string())
}

#[derive(Deserialize, Default)]
pub(super) struct SummaryQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

pub(super) struct TokenWindow {
    pub from: chrono::DateTime<Utc>,
    pub to: chrono::DateTime<Utc>,
}

/// Parses an offset-aware RFC3339 timestamp and normalizes it to UTC.
/// Naive timestamps without a timezone offset are rejected.
pub(super) fn parse_utc_rfc3339(value: &str) -> Result<chrono::DateTime<Utc>, ()> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ())
}

/// Formats a UTC instant in the canonical representation used by
/// `token_usage.occurred_at`: fixed milliseconds, Z suffix. Range comparisons
/// are only exact when bounds share this representation.
pub(super) fn format_utc_millis(value: chrono::DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Resolves the optional token time window. Both bounds must be provided
/// together (an unbounded half-open range would be ambiguous), and the range
/// must be non-empty. All bounds are normalized to UTC.
pub(super) fn resolve_token_window(query: &SummaryQuery) -> Result<Option<TokenWindow>, ApiError> {
    match (&query.from, &query.to) {
        (None, None) => Ok(None),
        (Some(from), Some(to)) => {
            let from = parse_utc_rfc3339(from).map_err(|_| {
                ApiError::validation("from must be an RFC3339 timestamp with timezone")
            })?;
            let to = parse_utc_rfc3339(to).map_err(|_| {
                ApiError::validation("to must be an RFC3339 timestamp with timezone")
            })?;
            if from >= to {
                return Err(ApiError::validation("from must be earlier than to"));
            }
            Ok(Some(TokenWindow { from, to }))
        }
        _ => Err(ApiError::validation(
            "from and to must be provided together",
        )),
    }
}

/// Appends the `[from, to)` filter on `token_usage.occurred_at` to a query
/// builder, using the same canonical representation as stored values so the
/// index stays usable.
pub(super) fn push_token_window<'a>(
    builder: &mut QueryBuilder<'a, sqlx::Sqlite>,
    window: &Option<TokenWindow>,
) {
    if let Some(window) = window {
        builder.push(" WHERE occurred_at >= ");
        builder.push_bind(format_utc_millis(window.from));
        builder.push(" AND occurred_at < ");
        builder.push_bind(format_utc_millis(window.to));
    }
}

pub(super) async fn stats_summary(
    _: AdminAuth,
    State(state): State<Context>,
    Query(query): Query<SummaryQuery>,
) -> ApiResult {
    let window = resolve_token_window(&query)?;
    // requests / success_rate / average_duration stay log-scoped: they are
    // intentionally NOT filtered by the token window.
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM request_logs")
        .fetch_one(state.db.pool())
        .await?;
    let success: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM request_logs WHERE outcome='success'")
            .fetch_one(state.db.pool())
            .await?;
    let avg_duration: Option<f64> =
        sqlx::query_scalar("SELECT AVG(total_duration_ms) FROM request_logs")
            .fetch_one(state.db.pool())
            .await?;
    // Token-derived fields come from the log-independent token_usage table, so
    // they survive log cleanup/expiry. The optional window filters them only.
    let mut token_query = QueryBuilder::new(
        "SELECT \
         COALESCE(SUM(cache_read_tokens),0) cache_read, \
         COALESCE(SUM(cache_write_tokens),0) cache_write, \
         COALESCE(SUM(cache_miss_input_tokens),0) cache_miss, \
         COALESCE(SUM(output_tokens),0) output_tokens, \
         AVG(first_token_ms) avg_first_token, \
         COALESCE(SUM(CASE WHEN output_tokens IS NOT NULL AND duration_ms > 0 THEN output_tokens ELSE 0 END),0) tps_tokens, \
         COALESCE(SUM(CASE WHEN output_tokens IS NOT NULL AND duration_ms > 0 THEN duration_ms ELSE 0 END),0) tps_duration \
         FROM token_usage",
    );
    push_token_window(&mut token_query, &window);
    let token_row = token_query.build().fetch_one(state.db.pool()).await?;
    let cache_read: i64 = token_row.try_get("cache_read")?;
    let cache_write: i64 = token_row.try_get("cache_write")?;
    let cache_miss: i64 = token_row.try_get("cache_miss")?;
    let output_tokens: i64 = token_row.try_get("output_tokens")?;
    let avg_first_token: Option<f64> = token_row.try_get("avg_first_token")?;
    let token_sum: i64 = token_row.try_get("tps_tokens")?;
    let duration_sum: i64 = token_row.try_get("tps_duration")?;
    let channels: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels")
        .fetch_one(state.db.pool())
        .await?;
    let active_channels: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM channel_health WHERE state='active'")
            .fetch_one(state.db.pool())
            .await?;
    let mut cache_query = QueryBuilder::new(
        "SELECT protocol, COUNT(*) request_count, \
         COALESCE(SUM(cache_read_tokens),0) cache_read, \
         COALESCE(SUM(cache_write_tokens),0) cache_write, \
         COALESCE(SUM(cache_miss_input_tokens),0) cache_miss \
         FROM token_usage",
    );
    push_token_window(&mut cache_query, &window);
    cache_query.push(" GROUP BY protocol");
    let cache_rows = cache_query.build().fetch_all(state.db.pool()).await?;
    let mut by_provider: HashMap<String, (i64, i64, i64, i64)> = HashMap::new();
    for row in cache_rows {
        let protocol: String = row.get(0);
        let request_count: i64 = row.get(1);
        let read: i64 = row.get::<Option<i64>, _>(2).unwrap_or(0);
        let write: i64 = row.get::<Option<i64>, _>(3).unwrap_or(0);
        let miss: i64 = row.get::<Option<i64>, _>(4).unwrap_or(0);
        let provider = cache_provider(&protocol);
        let entry = by_provider.entry(provider).or_insert((0, 0, 0, 0));
        entry.0 += request_count;
        entry.1 += read;
        entry.2 += write;
        entry.3 += miss;
    }
    let mut extra: Vec<String> = by_provider
        .keys()
        .filter(|provider| !CACHE_PROVIDER_ORDER.contains(&provider.as_str()))
        .cloned()
        .collect();
    extra.sort_unstable();
    let cache_provider_items: Vec<Value> = CACHE_PROVIDER_ORDER
        .iter()
        .map(|provider| provider.to_string())
        .chain(extra)
        .filter_map(|provider| {
            let (request_count, read, write, miss) = by_provider.get(&provider).copied()?;
            let total_input = read + write + miss;
            Some(json!({
                "provider": provider,
                "request_count": request_count,
                "cache_read_tokens": read,
                "cache_write_tokens": write,
                "cache_miss_input_tokens": miss,
                "total_input_tokens": total_input,
                "cache_hit_rate": if total_input > 0 {
                    Some((read as f64 / total_input as f64 * 10000.0).round() / 10000.0)
                } else {
                    None
                },
            }))
        })
        .collect();
    Ok(ok(json!({
        "requests": total,
        "success_rate": if total > 0 {
            Some((success as f64 / total as f64 * 10000.0).round() / 10000.0)
        } else {
            None
        },
        "average_duration_ms": avg_duration.map(|value| (value * 100.0).round() / 100.0),
        "average_first_token_ms": avg_first_token.map(|value| (value * 100.0).round() / 100.0),
        "average_tps": if duration_sum > 0 {
            Some((token_sum as f64 * 1000.0 / duration_sum as f64 * 1000.0).round() / 1000.0)
        } else {
            None
        },
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "cache_miss_input_tokens": cache_miss,
        "output_tokens": output_tokens,
        "cache_by_provider": cache_provider_items,
        "channels": channels,
        "active_channels": active_channels,
        "token_range": window.as_ref().map(|window| json!({
            "from": format_utc_millis(window.from),
            "to": format_utc_millis(window.to),
        })),
    })))
}
pub(super) async fn stats_cache(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    // Token counts come from the log-independent token_usage table: clearing
    // or expiring request logs never erases them. unknown_attempts counts
    // responded attempts whose usage carried no token field at all.
    let row=sqlx::query("SELECT COALESCE(SUM(cache_read_tokens),0) cache_read,COALESCE(SUM(cache_write_tokens),0) cache_write,COALESCE(SUM(cache_miss_input_tokens),0) cache_miss,COALESCE(SUM(output_tokens),0) output_tokens,COALESCE(SUM(CASE WHEN input_tokens IS NULL AND cache_read_tokens IS NULL AND cache_write_tokens IS NULL AND cache_miss_input_tokens IS NULL AND output_tokens IS NULL THEN 1 ELSE 0 END),0) unknown FROM token_usage").fetch_one(state.db.pool()).await?;
    Ok(ok(
        json!({"cache_read_tokens":row.get::<i64,_>("cache_read"),"cache_write_tokens":row.get::<i64,_>("cache_write"),"cache_miss_input_tokens":row.get::<i64,_>("cache_miss"),"output_tokens":row.get::<i64,_>("output_tokens"),"unknown_attempts":row.get::<i64,_>("unknown")}),
    ))
}
pub(super) async fn stats_models(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    let rows=sqlx::query("SELECT COALESCE(model_id,'') model_id,COUNT(*) requests FROM request_logs GROUP BY model_id ORDER BY requests DESC").fetch_all(state.db.pool()).await?;
    Ok(ok(
        json!({"items":rows.iter().map(|row|json!({"model_id":row.get::<String,_>("model_id"),"requests":row.get::<i64,_>("requests")})).collect::<Vec<_>>()}),
    ))
}
pub(super) async fn stats_channels(_: AdminAuth, State(state): State<Context>) -> ApiResult {
    let rows=sqlx::query("SELECT COALESCE(final_channel_id,'') channel_id,COUNT(*) requests FROM request_logs GROUP BY final_channel_id ORDER BY requests DESC").fetch_all(state.db.pool()).await?;
    Ok(ok(
        json!({"items":rows.iter().map(|row|json!({"channel_id":row.get::<String,_>("channel_id"),"requests":row.get::<i64,_>("requests")})).collect::<Vec<_>>()}),
    ))
}
#[derive(Deserialize, Default)]
pub(super) struct TimeseriesQuery {
    interval: Option<String>,
}
pub(super) async fn stats_timeseries(
    _: AdminAuth,
    State(state): State<Context>,
    Query(query): Query<TimeseriesQuery>,
) -> ApiResult {
    let format = if query.interval.as_deref() == Some("day") {
        "%Y-%m-%dT00:00:00Z"
    } else {
        "%Y-%m-%dT%H:00:00Z"
    };
    let rows=sqlx::query("SELECT strftime(?,started_at) bucket,COUNT(*) requests FROM request_logs GROUP BY bucket ORDER BY bucket").bind(format).fetch_all(state.db.pool()).await?;
    Ok(ok(
        json!({"items":rows.iter().map(|row|json!({"bucket":row.get::<Option<String>,_>("bucket"),"requests":row.get::<i64,_>("requests")})).collect::<Vec<_>>()}),
    ))
}
