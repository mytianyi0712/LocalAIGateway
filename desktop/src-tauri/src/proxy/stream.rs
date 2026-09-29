//! 流式响应路径：透明转发（`transparent_stream`）与映射转换
//! （`mapped_stream`），以及可观测性扫描用的 usage 归集。

use std::{
    convert::Infallible,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_stream::stream;
use axum::{
    body::Body,
    http::{Response, StatusCode},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::Value;

use crate::{
    compression, convert,
    domain::{TransportFailure, Usage},
    protocol,
};

// 同目录兄弟模块的内部项（`pub(super)` / `pub(crate)`）。
use super::attempt::*;
use super::error::*;
use super::nonstream::*;

/// 由原始字段拼出 Usage，并按协议推导 cache-miss。
///
/// 换算只有一条公式（[`crate::domain::cache_miss_input`]），此处只按协议决定
/// 输入字段的口径：`claude` 流式上报的 `input` 本身就是未命中部分，
/// `gemini` 没有 cache-write 概念，其余协议用「总量 - 缓存读 - 缓存写」。
pub(super) fn usage_from_parts(
    stream_protocol: &str,
    input: Option<i64>,
    output: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Usage {
    let cache_miss = match stream_protocol {
        "claude" => input,
        "gemini" => crate::domain::cache_miss_input(input, cache_read, None),
        _ => crate::domain::cache_miss_input(input, cache_read, cache_write),
    };
    Usage {
        input_tokens: input,
        cache_read_tokens: cache_read,
        cache_write_tokens: cache_write,
        cache_miss_input_tokens: cache_miss,
        output_tokens: output,
        raw: None,
    }
}

/// 从待处理缓冲中解析 SSE `data:` 行，用于可观测性（首 token 检测、
/// usage 合并）与终态判定。完整的、以 `\n` 结尾的行会被消费；
/// `tail` 为 true 时还会处理最后一段没有换行的残留行（有些上游结尾
/// 不带换行，而最后一条 usage 事件可能就在那里）。返回 true 表示
/// 看到了终态事件。观测到的首 token/usage 值会镜像进 `stats`，
/// 这样客户端中途断开时仍能记录它们。
/// 被消费的行会从 `pending` 中移除。
/// 终态判定委托给 [`convert::stream_completed`]——流式转换路径用同一
/// 函数决定「流已完成」，两处若各有一套判定集合，客户端断开时的
/// `cancel_completed_flag` 就会与真实结果漂移。
#[allow(clippy::too_many_arguments)]
pub(super) fn scan_observable_lines(
    pending: &mut Vec<u8>,
    tail: bool,
    stream_protocol: &str,
    usage: &mut Usage,
    first_token_ms: &mut Option<i64>,
    first_token_seen: &mut bool,
    stats: &SharedStreamStats,
    now: chrono::DateTime<chrono::Utc>,
    attempt_started: chrono::DateTime<chrono::Utc>,
) -> bool {
    let mut terminal = false;
    let mut consumed = 0usize;
    while consumed < pending.len() {
        let line_end = match pending[consumed..].iter().position(|byte| *byte == b'\n') {
            Some(offset) => consumed + offset,
            None => {
                if !tail {
                    break;
                }
                pending.len()
            }
        };
        let line = &pending[consumed..line_end];
        consumed = if line_end < pending.len() {
            line_end + 1
        } else {
            line_end
        };
        let Some(data) = crate::sse::line_data(line) else {
            continue;
        };
        if data.is_empty() {
            continue;
        }
        if stream_protocol == "openai_compatible" && data == b"[DONE]" {
            terminal = true;
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(data) else {
            continue;
        };
        if first_token_ms.is_none() && convert::chunk_has_content(stream_protocol, &value) {
            *first_token_ms =
                Some(now.signed_duration_since(attempt_started).num_milliseconds());
            *stats.first_token_ms.lock() = *first_token_ms;
            *first_token_seen = true;
        }
        if convert::stream_completed(stream_protocol, &value) {
            terminal = true;
        }
        if let Some(raw_usage) = protocol::usage_value(stream_protocol, &value) {
            let normalized = protocol::normalize_usage(stream_protocol, &raw_usage);
            usage.merge(&normalized);
            *stats.usage.lock() = usage.clone();
        }
    }
    pending.drain(..consumed);
    terminal
}

/// 由映射流转换器取出 usage 快照：仅在转换干净时有意义。
/// cache-miss 由 [`crate::domain::cache_miss_input`] 统一推导（见 [`usage_from_parts`]）。
/// 转换失败时返回默认（空）usage。
pub(super) fn converter_usage(
    converter: &convert::MappedStreamConverter,
    stream_protocol: &str,
    decode_failed: bool,
) -> Usage {
    if decode_failed {
        return Usage::default();
    }
    let (input, output, cache_read, cache_write) = converter.usage();
    usage_from_parts(stream_protocol, input, output, cache_read, cache_write)
}

/// 把转换器当前的 usage 镜像进共享 stats，
/// 使客户端中途断开时能记录已经观测到的值。
pub(super) fn sync_converter_usage(
    converter: &convert::MappedStreamConverter,
    stats: &SharedStreamStats,
    stream_protocol: &str,
) {
    let (input, output, cache_read, cache_write) = converter.usage();
    *stats.usage.lock() = usage_from_parts(stream_protocol, input, output, cache_read, cache_write);
}

/// 透明流式转发路径的累计解码上限（8 GiB 硬上限）。
/// 流本身从不整体缓冲——这个上限只是让解码器的计数保持合理；
/// 每次 feed 的输出受 `compression::FEED_LIMIT` 约束，
/// 因此无论该值多大，内存占用都是有界的。
pub(super) const STREAM_FORWARD_DECODE_MAX_TOTAL: usize = 8 * 1024 * 1024 * 1024;

/// 抽干解码器的滞留输入（每次 feed 的输出上限），让最终扫描看到完整正文。
/// 返回 `false` 表示解码失败——下游必须中断，绝不能转发损坏的字节。
fn drain_decoder(
    decoder: &mut compression::RequiredDecoder,
    pending: &mut Vec<u8>,
    response_bytes: &mut i64,
    stats: &SharedStreamStats,
    request_id: &str,
) -> bool {
    loop {
        match decoder.feed_required(&[]) {
            Ok(more) if !more.is_empty() => {
                *response_bytes += more.len() as i64;
                stats.bytes.store(*response_bytes, Ordering::SeqCst);
                pending.extend_from_slice(&more);
            }
            Ok(_) => return true,
            Err(error) => {
                tracing::warn!(
                    request_id = %request_id,
                    error = ?error,
                    "transparent stream decode failed at EOF; interrupting downstream"
                );
                return false;
            }
        }
    }
}

/// 非映射 2xx 的流式路径：透明转发，明文读取有界。压缩的上游响应
/// 被增量解码后转发，因此被截断的上游流在下游表现为一个干净中断的
/// 明文流，而不会是一个客户端无法解压的损坏压缩体
/// （如 omp 的 `ZlibError`）。本路径从不对正文整体缓冲；
/// 中途失败会以 `stream_interrupted` 终态事件呈现。
/// 同一份明文也会被喂给观测扫描器（首 token 与 usage 解析），
/// 但转发与扫描都不改变正文。
pub(super) fn transparent_stream(
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> Response<Body> {
    let AttemptEnv {
        telemetry,
        clock,
        request_id,
        started,
        candidate,
        attempts,
        attempt_started,
        runtime,
        upstream_protocol,
        upstream_model,
        ..
    } = env;
    let upstream_stream = response.body.into_stream();
    let mut response_headers_for_client = response_headers.clone();
    response_headers_for_client.remove("content-length");
    // 转发出去的正文是明文；上游的传输编码（content-encoding）
    // 不能泄漏到下游——否则客户端会尝试解压它，
    // 并在任何截断处失败。
    response_headers_for_client.remove("content-encoding");
    // 无损增量解码：解出的明文既转发也扫描。累计上限是 8 GiB
    // 的硬上限——流式路径从不整体缓冲正文，
    // 只有每次 feed 的输出受上限约束。
    let decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        STREAM_FORWARD_DECODE_MAX_TOTAL,
    );
    let telemetry = telemetry.clone();
    let request_id_stream = request_id.to_owned();
    let candidate_stream = candidate.clone();
    let upstream_protocol_stream = upstream_protocol.to_owned();
    let upstream_model_stream = upstream_model.to_owned();
    let stream_protocol = upstream_protocol_stream.clone();
    let failure_threshold = runtime.failure_threshold;
    let circuit_open_seconds = runtime.circuit_open_seconds;
    // 首 token 记账覆盖整次尝试：截止时间锚定在尝试开始，
    // 因此缓慢的响应头无法延长窗口。发送阶段本身受
    // first_byte_timeout 约束。
    let first_token_deadline = env.attempt_started_instant
        + Duration::from_secs(runtime.first_token_timeout_seconds.max(1) as u64);
    let stream_idle_timeout =
        Duration::from_secs(runtime.stream_idle_timeout_seconds.max(1) as u64);
    let stats = Arc::new(SharedStreamStats::default());
    let cancel_completed = Arc::new(AtomicBool::new(false));
    let cancel_finalized = Arc::new(AtomicBool::new(false));
    let cancel_completed_flag = cancel_completed.clone();
    let cancel_responded = Arc::new(AtomicBool::new(false));
    let on_cancel = cancel_on_cancel(
        telemetry.clone(),
        Arc::clone(&clock),
        request_id.to_owned(),
        candidate.clone(),
        attempts,
        attempt_started,
        started,
        status.as_u16(),
        failure_threshold,
        circuit_open_seconds,
        upstream_protocol_stream.clone(),
        upstream_model_stream.clone(),
        Arc::clone(&stats),
        cancel_responded.clone(),
    );
    let cancel_aware = CancelAware {
        inner: upstream_stream,
        completed: cancel_completed,
        finalized: cancel_finalized,
        responded: cancel_responded,
        on_cancel: Some(on_cancel),
    };
    let stream_body = stream! {
        let mut upstream_stream = cancel_aware;
        let mut decoder = decoder;
        let mut response_bytes = 0i64;
        let mut ok = true;
        let mut idle_timeout = false;
        let mut pending: Vec<u8> = Vec::new();
        let mut first_byte_ms: Option<i64> = None;
        let mut first_token_ms: Option<i64> = None;
        let mut first_token_seen = false;
        let mut usage: Usage = Usage::default();
        loop {
            // 首 token 之前由绝对截止时间主导。截止时间被优先轮询
            // （biased），因此即使字节已经缓冲，
            // 晚于窗口到达的首 token 仍会超时。
            // （发送阶段本身由 first_byte_timeout 约束。）
            let next = if first_token_seen {
                tokio::time::timeout(stream_idle_timeout, upstream_stream.next())
                    .await
                    .map_err(|_| ())
            } else {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(first_token_deadline) => Err(()),
                    next = upstream_stream.next() => Ok(next),
                }
            };
            match next {
                Ok(Some(Ok(chunk))) => {
                    let now = clock.now_utc();
                    if first_byte_ms.is_none() {
                        first_byte_ms = Some(
                            now.signed_duration_since(attempt_started).num_milliseconds(),
                        );
                        *stats.first_byte_ms.lock() = first_byte_ms;
                    }
                    // 无损解码：解出的明文既转发也扫描。
                    // 损坏或截断的帧对中继而言是终态——客户端绝不能收到
                    // 它无法完成解压的压缩字节（截断的正文会让
                    // 客户端解压崩溃，如 omp 的 ZlibError）。
                    // 每次 feed 的输出受 compression::FEED_LIMIT 约束，
                    // 因此下面的 drain 会逐片把滞留的输入流出，
                    // 任何时刻都不会缓冲超过一片，
                    // 也就不会为对齐而无限增长内存。
                    let mut piece = match decoder.feed_required(&chunk) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            tracing::warn!(
                                request_id = %request_id_stream,
                                error = ?error,
                                "transparent stream decode failed; interrupting downstream"
                            );
                            ok = false;
                            break;
                        }
                    };
                    loop {
                        if !piece.is_empty() {
                            response_bytes += piece.len() as i64;
                            stats.bytes.store(response_bytes, Ordering::SeqCst);
                            pending.extend_from_slice(&piece);
                            let terminal = scan_observable_lines(
                                &mut pending,
                                false,
                                &stream_protocol,
                                &mut usage,
                                &mut first_token_ms,
                                &mut first_token_seen,
                                &stats,
                                now,
                                attempt_started,
                            );
                            if terminal {
                                // 终态标记（如 `data: [DONE]`）就在这一片里：
                                // 响应已完成。必须在把最后一批字节交给客户端
                                // 之前记录终态遥测——客户端若在最后一个事件
                                // 之后立刻挂断，已完成的流不能因此
                                // 退化成一条带零值的 `cancelled`。
                                // 记录后即置位 completed，
                                // 生成器随之返回，
                                // Drop 路径便不会再追加取消终结。
                                cancel_completed_flag.store(true, Ordering::SeqCst);
                                let finished = clock.now_utc();
                                finalize_stream_attempt(
                                    &telemetry,
                                    &request_id_stream,
                                    started,
                                    failure_threshold,
                                    circuit_open_seconds,
                                    &candidate_stream,
                                    attempts,
                                    attempt_started,
                                    finished,
                                    AttemptOutcome::Success,
                                    status.as_u16() as i64,
                                    None,
                                    false,
                                    first_byte_ms,
                                    first_token_ms,
                                    usage,
                                    response_bytes,
                                    upstream_protocol_stream.clone(),
                                    upstream_model_stream.clone(),
                                );
                                yield Ok::<Bytes, Infallible>(piece.into());
                                return;
                            }
                            yield Ok::<Bytes, Infallible>(piece.into());
                        }
                        // 首次 feed 可能留下滞留输入（每次 feed 的输出上限）；
                        // 在下一个上游 chunk 之前先抽干它。
                        // `feed_required` 即使传入空切片
                        // 也会继续处理滞留输入。
                        match decoder.feed_required(&[]) {
                            Ok(more) => piece = more,
                            Err(error) => {
                                tracing::warn!(
                                    request_id = %request_id_stream,
                                    error = ?error,
                                    "transparent stream decode failed while draining; interrupting downstream"
                                );
                                ok = false;
                                break;
                            }
                        }
                        if piece.is_empty() {
                            break;
                        }
                    }
                    if !ok {
                        break;
                    }
                }
                Ok(Some(Err(_))) => {
                    ok = false;
                    break;
                }
                Ok(None) => {
                    // 抽干解码器滞留的输入（每次 feed 的输出上限），
                    // 让最终扫描看到完整正文。
                    let now = clock.now_utc();
                    if !drain_decoder(
                        &mut decoder,
                        &mut pending,
                        &mut response_bytes,
                        &stats,
                        &request_id_stream,
                    ) {
                        ok = false;
                        break;
                    }
                    // 解析最后一段没有换行的残留行：当上游结尾不带换行时，
                    // usage（偶尔还有终态标记）可能就在这最后一行里，
                    // 因此必须把它也交给扫描器。
                    let terminal = scan_observable_lines(
                        &mut pending,
                        true,
                        &stream_protocol,
                        &mut usage,
                        &mut first_token_ms,
                        &mut first_token_seen,
                        &stats,
                        now,
                        attempt_started,
                    );
                    if terminal {
                        cancel_completed_flag.store(true, Ordering::SeqCst);
                        let finished = clock.now_utc();
                        finalize_stream_attempt(
                            &telemetry,
                            &request_id_stream,
                            started,
                            failure_threshold,
                            circuit_open_seconds,
                            &candidate_stream,
                            attempts,
                            attempt_started,
                            finished,
                            AttemptOutcome::Success,
                            status.as_u16() as i64,
                            None,
                            false,
                            first_byte_ms,
                            first_token_ms,
                            usage,
                            response_bytes,
                            upstream_protocol_stream.clone(),
                            upstream_model_stream.clone(),
                        );
                        return;
                    }
                    if !decoder.finished() {
                        // 压缩流在帧中途结束：明文不完整（例如缺少 gzip 尾部）。
                        // 已经转发出去的明文前缀是干净的，
                        // 但把这次尝试计为中断，
                        // 这样渠道判定与客户端实际观测到的结果一致，
                        // 不会把一次截断误记为成功。
                        tracing::warn!(
                            request_id = %request_id_stream,
                            "transparent stream ended before the compressed stream finished"
                        );
                        ok = false;
                    }
                    break;
                }
                Err(_) => {
                    ok = false;
                    idle_timeout = true;
                    break;
                }
            }
        }
        // 生成器已运行到其终态遥测（成功、上游错误、空闲/首 token 超时，
        // 或从未产出首 token 的干净结束），因此这里把 completed 置位：
        // Drop 路径不得再追加一次重复的 `cancelled` 终结。
        // （置位后即使客户端立刻挂断也不会触发取消回调。）
        cancel_completed_flag.store(true, Ordering::SeqCst);
        let finished = clock.now_utc();
        let (outcome, error_kind, final_status): (AttemptOutcome, Option<String>, i64) = if !ok {
            if idle_timeout {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("transport_timeout".into()),
                    504,
                )
            } else {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("stream_interrupted".into()),
                    status.as_u16() as i64,
                )
            }
        } else if first_token_ms.is_none() {
            // 上游关闭了流，却从未产出首 token，也没有终态标记
            // （例如一个空的 200 正文）。这与停滞一样
            // 违反了首 token 保护——必须计入熔断，
            // 让损坏的上游被打开而不是以空响应
            // 悄悄“成功”。
            (
                AttemptOutcome::StreamInterrupted,
                Some("no_first_token".into()),
                status.as_u16() as i64,
            )
        } else {
            (AttemptOutcome::Success, None, status.as_u16() as i64)
        };
        finalize_stream_attempt(
            &telemetry,
            &request_id_stream,
            started,
            failure_threshold,
            circuit_open_seconds,
            &candidate_stream,
            attempts,
            attempt_started,
            finished,
            outcome,
            final_status,
            error_kind,
            true,
            first_byte_ms,
            first_token_ms,
            usage,
            response_bytes,
            upstream_protocol_stream,
            upstream_model_stream,
        );
    };
    let mut result = Response::new(Body::from_stream(stream_body));
    *result.status_mut() = status;
    *result.headers_mut() = response_headers_for_client;
    result
}
/// 映射流构造器的结果：要么返回一个响应，要么是一个 prelude 级失败——
/// 后者已经发出终态遥测，
/// 应 failover 到下一个候选。
pub(super) enum MappedStreamResult {
    Respond(Response<Body>),
    FailOver(Option<TransportFailure>),
}

/// prelude 扫描的结果：就绪时带回回放用的 `(原始 chunk, 明文)` 列表与
/// 已缓冲字节数；失败时只带回已缓冲字节数（遥测口径）。
pub(super) enum PreludeOutcome {
    /// 缓冲就绪：`(原始 chunk, 明文)` 列表，按顺序回放给转换器。
    Ready(Vec<(Bytes, Vec<u8>)>),
    /// 200 正文携带上游错误事件。
    UpstreamError(usize),
    /// 正文无法解码（帧损坏或超过缓冲上限）。
    DecodeError(usize),
    /// 绝对首 token 窗口内没有首 token。
    Deadline(usize),
}

/// 缓冲一小段 prelude：最多 1 MiB 或直到首 token。
/// 这样 200 的错误正文仍能 failover，并且像历史实现一样遵守
/// `first_token_timeout_seconds`（截止时间优先轮询，因此即使字节已经可用，
/// 晚到的首 token 仍算超时）。
async fn mapped_prelude(
    env: &AttemptEnv<'_>,
    upstream_stream: &mut (impl futures_util::Stream<
        Item = Result<Bytes, crate::ports::UpstreamError>,
    > + Unpin),
    decoder: &mut compression::RequiredDecoder,
    adapter: &mut UpstreamStreamAdapter,
    scan: &mut convert::StreamScan,
    converter_protocol: &str,
    first_token_deadline: tokio::time::Instant,
) -> PreludeOutcome {
    let mut prelude: Vec<(Bytes, Vec<u8>)> = Vec::new();
    let mut prelude_bytes = 0usize;
    loop {
        let next = tokio::select! {
            biased;
            _ = tokio::time::sleep_until(first_token_deadline) => {
                return PreludeOutcome::Deadline(prelude_bytes);
            }
            next = upstream_stream.next() => next,
        };
        match next {
            Some(Ok(chunk)) => {
                prelude_bytes += chunk.len();
                let decoded = match decoder.feed_required(&chunk) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        tracing::warn!(
                            request_id = %env.request_id,
                            channel_id = %env.candidate.channel_id,
                            error = ?error,
                            "mapped response decode failed during prelude"
                        );
                        return PreludeOutcome::DecodeError(prelude_bytes);
                    }
                };
                let adapted = adapter.feed(&decoded);
                prelude.push((chunk.clone(), adapted.clone().into_owned()));
                if scan.feed(converter_protocol, &adapted).is_some() {
                    return PreludeOutcome::UpstreamError(prelude_bytes);
                }
                if scan.first_token_now || prelude_bytes >= 1024 * 1024 {
                    return PreludeOutcome::Ready(prelude);
                }
            }
            Some(Err(_)) => return PreludeOutcome::Ready(prelude),
            None => return PreludeOutcome::Ready(prelude),
        }
    }
}

/// prelude 失败的终态遥测：三类失败（上游错误 / 解码失败 / 首 token 超时）
/// 的收尾完全同构，只有 outcome、状态码与错误标签不同。
fn finalize_prelude_failure(
    env: &AttemptEnv<'_>,
    outcome: AttemptOutcome,
    status: Option<i64>,
    error_kind: &'static str,
    response_bytes: i64,
) {
    let finished = env.clock.now_utc();
    AttemptFinalizer::new(
        Arc::new(env.telemetry.clone()),
        env.request_id,
        env.started,
        env.runtime.failure_threshold,
        env.runtime.circuit_open_seconds,
    )
    .finalize(
        env.candidate,
        env.attempts,
        env.attempt_started,
        finished,
        outcome,
        status,
        Some(error_kind.into()),
        env.attempts < env.candidates_len,
        false,
        None,
        None,
        Usage::default(),
        response_bytes,
        Some(env.upstream_protocol.into()),
        Some(env.upstream_model.into()),
        true,
    );
}

/// 上游字节流与转换矩阵所消费的规范协议之间的适配层。
/// Command Code 的 `/alpha/generate` 返回 NDJSON；
/// 下游全部消费者（scan、流转换器、usage）都期望 OpenAI SSE，
/// 因此解码器放在这里。官方 Provider API 传输返回普通 SSE/JSON，
/// 无需任何适配。
/// （`Plain` 分支对字节零拷贝借用，`CommandCode` 分支才做解码。）
pub(super) enum UpstreamStreamAdapter {
    Plain,
    CommandCode(Box<convert::CommandCodeDecoder>),
}

impl UpstreamStreamAdapter {
    pub(super) fn new(
        upstream_protocol: &str,
        transport: Option<crate::commandcode::Transport>,
        model: &str,
    ) -> Self {
        if upstream_protocol == "command_code"
            && transport == Some(crate::commandcode::Transport::Generate)
        {
            Self::CommandCode(Box::new(convert::CommandCodeDecoder::new(model)))
        } else {
            Self::Plain
        }
    }

    pub(super) fn feed<'a>(&mut self, decoded: &'a [u8]) -> std::borrow::Cow<'a, [u8]> {
        match self {
            // 普通流量直接重新借用：无需适配的协议，
            // 映射管线不应为每个 chunk 多付一次拷贝。
            Self::Plain => std::borrow::Cow::Borrowed(decoded),
            Self::CommandCode(decoder) => std::borrow::Cow::Owned(decoder.feed(decoded)),
        }
    }

    /// 在上游 EOF 时冲刷最后一段没有换行的 NDJSON 行。
    pub(super) fn finish_input(&mut self) -> Vec<u8> {
        match self {
            Self::Plain => Vec::new(),
            Self::CommandCode(decoder) => decoder.flush(),
        }
    }
}

/// 映射流式：先缓冲一小段 prelude（这样 200 的错误正文仍能 failover），
/// 再对实时流做增量转换。prelude 解码是无损的（`RequiredDecoder`），
/// 损坏或超限的正文会让该次尝试失败，
/// 而不是把流截断。
pub(super) async fn mapped_stream(
    env: AttemptEnv<'_>,
    response: crate::ports::UpstreamResponse,
    status: axum::http::StatusCode,
    response_headers: axum::http::HeaderMap,
) -> MappedStreamResult {
    // prelude 的失败收尾需要完整的尝试上下文；`AttemptEnv` 只含引用与廉价拷贝，
    // 因此先留一份再解构。
    let prelude_env = env.clone();
    let AttemptEnv {
        telemetry,
        clock,
        request_id,
        started,
        candidate,
        attempts,
        attempt_started,
        attempt_started_instant,
        runtime,
        entry_protocol,
        entry_model,
        upstream_protocol,
        upstream_model,
        response_model,
        command_code_transport,
        ..
    } = env;
    let failure_threshold = runtime.failure_threshold;
    let circuit_open_seconds = runtime.circuit_open_seconds;
    // Command Code 的 NDJSON 先被解码成规范 OpenAI SSE；
    // 之后每个下游消费者（scanner、converter、usage）
    // 都说 `openai_compatible`，复用既有转换矩阵。
    let converter_protocol = if upstream_protocol == "command_code" {
        "openai_compatible"
    } else {
        upstream_protocol
    };
    let mut adapter =
        UpstreamStreamAdapter::new(upstream_protocol, command_code_transport, response_model);
    // 绝对首 token 窗口锚定在尝试开始：
    // 缓慢的响应头无法延长它。
    let first_token_deadline = attempt_started_instant
        + Duration::from_secs(runtime.first_token_timeout_seconds.max(1) as u64);
    // Command Code 流在工具输入事件之间可能长时间静默；
    // 对它使用专门的空闲窗口。
    let stream_idle_timeout = Duration::from_secs(
        if upstream_protocol == "command_code" {
            runtime.command_code_idle_timeout_seconds.max(1)
        } else {
            runtime.stream_idle_timeout_seconds.max(1)
        } as u64,
    );
    let stats = Arc::new(SharedStreamStats::default());
    let mut upstream_stream = response.body.into_stream();
    // 映射转换必须有明文——解码失败
    // 必须让该次尝试失败，绝不能退化成静默。
    let mut decoder = compression::RequiredDecoder::new(
        compression::ContentDecoder::from_encoding(response_headers.get("content-encoding")),
        (runtime.max_buffered_upstream_body_mb.max(1) as usize) * 1024 * 1024,
    );
    let mut scan = convert::StreamScan::new(converter_protocol);
    // 绝对截止时间的 prelude 扫描：见 [`mapped_prelude`]。
    // 原始 chunk 与其解码后的明文成对回放——prelude 只经转换器回放一次，
    // 重新喂给解码器会让它的状态双倍前进，从而破坏输出。
    let prelude = match mapped_prelude(
        &prelude_env,
        &mut upstream_stream,
        &mut decoder,
        &mut adapter,
        &mut scan,
        converter_protocol,
        first_token_deadline,
    )
    .await
    {
        PreludeOutcome::Ready(prelude) => prelude,
        PreludeOutcome::UpstreamError(bytes) => {
            // 2xx 正文携带上游错误：记录并 failover 到下一个候选。
            finalize_prelude_failure(
                &prelude_env,
                AttemptOutcome::UpstreamError,
                Some(502),
                "upstream_error",
                bytes as i64,
            );
            return MappedStreamResult::FailOver(None);
        }
        PreludeOutcome::DecodeError(bytes) => {
            // 这个 200 正文无法解码（帧损坏或超过缓冲正文上限）：
            // 转换所需的明文并不存在。failover。
            finalize_prelude_failure(
                &prelude_env,
                AttemptOutcome::UpstreamError,
                Some(502),
                "upstream_decode_error",
                bytes as i64,
            );
            return MappedStreamResult::FailOver(None);
        }
        PreludeOutcome::Deadline(bytes) => {
            // 在绝对尝试窗口内没有首 token：failover。归类为超时，
            // 这样单候选运行在尾部映射为 504。
            finalize_prelude_failure(
                &prelude_env,
                AttemptOutcome::TransportError,
                None,
                "timeout",
                bytes as i64,
            );
            return MappedStreamResult::FailOver(Some(TransportFailure::FirstTokenTimeout));
        }
    };
    let converter = match convert::MappedStreamConverter::new(
        entry_protocol,
        converter_protocol,
        entry_model,
    ) {
        Ok(converter) => converter,
        Err(error) => {
            tracing::error!(request_id = %request_id, error = %error, "stream converter init failed");
            return MappedStreamResult::Respond(gateway_error(
                entry_protocol,
                StatusCode::INTERNAL_SERVER_ERROR,
                "conversion_error",
                "Conversion error",
                request_id,
            ));
        }
    };
    let stream_protocol = converter_protocol.to_owned();
    let response_headers_for_client = response_headers.clone();
    let telemetry = telemetry.clone();
    let request_id_stream = request_id.to_owned();
    let candidate_stream = candidate.clone();
    let upstream_protocol_stream = upstream_protocol.to_owned();
    let upstream_model_stream = upstream_model.to_owned();
    let cancel_completed = Arc::new(AtomicBool::new(false));
    let cancel_finalized = Arc::new(AtomicBool::new(false));
    let cancel_completed_flag = cancel_completed.clone();
    let cancel_responded = Arc::new(AtomicBool::new(false));
    let on_cancel = cancel_on_cancel(
        telemetry.clone(),
        Arc::clone(&clock),
        request_id.to_owned(),
        candidate.clone(),
        attempts,
        attempt_started,
        started,
        status.as_u16(),
        failure_threshold,
        circuit_open_seconds,
        upstream_protocol_stream.clone(),
        upstream_model_stream.clone(),
        Arc::clone(&stats),
        cancel_responded.clone(),
    );
    let cancel_aware = CancelAware {
        inner: upstream_stream,
        completed: cancel_completed,
        finalized: cancel_finalized,
        responded: cancel_responded,
        on_cancel: Some(on_cancel),
    };
    let stream_body = stream! {
        let mut upstream_stream = cancel_aware;
        let mut converter = converter;
        let mut scan = scan;
        let mut adapter = adapter;
        let mut decoder = decoder;
        let mut response_bytes = 0i64;
        let mut ok = true;
        let mut idle_timeout = false;
        let mut decode_failed = false;
        let mut received_any = !prelude.is_empty();
        let mut first_byte_ms: Option<i64> = None;
        let mut first_token_ms: Option<i64> = None;
        // 把缓冲的 prelude 经转换器回放。扫描阶段解码器已经
        // 在这些字节上前进过；这里只喂入
        // 存下的明文。
        for (raw, decoded) in prelude {
            response_bytes += raw.len() as i64;
            stats.bytes.store(response_bytes, Ordering::SeqCst);
            if first_byte_ms.is_none() {
                first_byte_ms = Some(
                    clock.now_utc()
                        .signed_duration_since(attempt_started)
                        .num_milliseconds(),
                );
                *stats.first_byte_ms.lock() = first_byte_ms;
            }
            let converted = converter.feed(&decoded);
            sync_converter_usage(&converter, &stats, &stream_protocol);
            if converter.finished() {
                // 整条流都落在 prelude 里：在客户端看到任何字节之前
                // 就记录终态遥测——最后一个事件之后的断开
                // 不得把已完成的流降级为
                // `cancelled`。
                if first_token_ms.is_none() && scan.first_token() {
                    first_token_ms = Some(
                        clock.now_utc()
                            .signed_duration_since(attempt_started)
                            .num_milliseconds(),
                    );
                    *stats.first_token_ms.lock() = first_token_ms;
                }
                cancel_completed_flag.store(true, Ordering::SeqCst);
                finalize_stream_attempt(
                    &telemetry,
                    &request_id_stream,
                    started,
                    failure_threshold,
                    circuit_open_seconds,
                    &candidate_stream,
                    attempts,
                    attempt_started,
                    clock.now_utc(),
                    AttemptOutcome::Success,
                    status.as_u16() as i64,
                    None,
                    false,
                    first_byte_ms,
                    first_token_ms,
                    converter_usage(&converter, &stream_protocol, false),
                    response_bytes,
                    upstream_protocol_stream.clone(),
                    upstream_model_stream.clone(),
                );
                if !converted.is_empty() {
                    yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                }
                return;
            }
            if !converted.is_empty() {
                yield Ok::<Bytes, Infallible>(Bytes::from(converted));
            }
        }
        if first_token_ms.is_none() && scan.first_token() {
            first_token_ms = Some(
                clock.now_utc()
                    .signed_duration_since(attempt_started)
                    .num_milliseconds(),
            );
            *stats.first_token_ms.lock() = first_token_ms;
        }
        loop {
            let next = tokio::time::timeout(stream_idle_timeout, upstream_stream.next()).await;
            match next {
                Ok(Some(Ok(chunk))) => {
                    response_bytes += chunk.len() as i64;
                    stats.bytes.store(response_bytes, Ordering::SeqCst);
                    received_any = true;
                    let now = clock.now_utc();
                    if first_byte_ms.is_none() {
                        first_byte_ms = Some(
                            now.signed_duration_since(attempt_started)
                                .num_milliseconds(),
                        );
                        *stats.first_byte_ms.lock() = first_byte_ms;
                    }
                    let decoded = match decoder.feed_required(&chunk) {
                        Ok(decoded) => decoded,
                        Err(error) => {
                            // 转换所需的明文已不存在——发出一个固定的
                            // 网关错误事件并停止，
                            // 绝不静默截断流。
                            // （错误事件即终态事件。）
                            tracing::warn!(
                                request_id = %request_id_stream,
                                channel_id = %candidate_stream.channel_id,
                                error = ?error,
                                "mapped response decode failed mid-stream"
                            );
                            let converted = converter.error_event(
                                "Upstream response could not be decoded",
                            );
                            // 错误事件就是终态事件：在客户端可能挂断之前
                            // 记录 outcome。
                            cancel_completed_flag.store(true, Ordering::SeqCst);
                            finalize_stream_attempt(
                                &telemetry,
                                &request_id_stream,
                                started,
                                failure_threshold,
                                circuit_open_seconds,
                                &candidate_stream,
                                attempts,
                                attempt_started,
                                clock.now_utc(),
                                AttemptOutcome::GatewayError,
                                status.as_u16() as i64,
                                Some("upstream_decode_error".into()),
                                true,
                                first_byte_ms,
                                first_token_ms,
                                Usage::default(),
                                response_bytes,
                                upstream_protocol_stream.clone(),
                                upstream_model_stream.clone(),
                            );
                            if !converted.is_empty() {
                                yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                            }
                            return;
                        }
                    };
                    // 在 scanner 与 converter 看到之前，
                    // 先把 Command Code 的 NDJSON 转成规范 OpenAI SSE。
                    let adapted = adapter.feed(&decoded);
                    if let Some(message) = scan.feed(&stream_protocol, &adapted) {
                        let converted = converter.error_event(&message);
                        // 这个 2xx 流携带上游错误；
                        // 转换后的错误事件即终态。
                        cancel_completed_flag.store(true, Ordering::SeqCst);
                        finalize_stream_attempt(
                            &telemetry,
                            &request_id_stream,
                            started,
                            failure_threshold,
                            circuit_open_seconds,
                            &candidate_stream,
                            attempts,
                            attempt_started,
                            clock.now_utc(),
                            AttemptOutcome::UpstreamError,
                            status.as_u16() as i64,
                            Some("upstream_error".into()),
                            true,
                            first_byte_ms,
                            first_token_ms,
                            converter_usage(&converter, &stream_protocol, false),
                            response_bytes,
                            upstream_protocol_stream.clone(),
                            upstream_model_stream.clone(),
                        );
                        if !converted.is_empty() {
                            yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                        }
                        return;
                    }
                    if first_token_ms.is_none() && scan.first_token_now {
                        first_token_ms = Some(
                            now.signed_duration_since(attempt_started)
                                .num_milliseconds(),
                        );
                        *stats.first_token_ms.lock() = first_token_ms;
                    }
                    let converted = converter.feed(&adapted);
                    sync_converter_usage(&converter, &stats, &stream_protocol);
                    if converter.finished() {
                        // 已产出终态事件（如 `response.completed`）：
                        // 在 yield 它之前先记录 outcome，
                        // 这样在最后一个事件后立刻挂断的客户端
                        // 仍会留下一条成功记录。
                        cancel_completed_flag.store(true, Ordering::SeqCst);
                        finalize_stream_attempt(
                            &telemetry,
                            &request_id_stream,
                            started,
                            failure_threshold,
                            circuit_open_seconds,
                            &candidate_stream,
                            attempts,
                            attempt_started,
                            clock.now_utc(),
                            AttemptOutcome::Success,
                            status.as_u16() as i64,
                            None,
                            false,
                            first_byte_ms,
                            first_token_ms,
                            converter_usage(&converter, &stream_protocol, false),
                            response_bytes,
                            upstream_protocol_stream.clone(),
                            upstream_model_stream.clone(),
                        );
                        if !converted.is_empty() {
                            yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                        }
                        return;
                    }
                    if !converted.is_empty() {
                        yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                    }
                }
                Ok(Some(Err(_))) => {
                    ok = false;
                    break;
                }
                Ok(None) => {
                    if !decoder.finished() && received_any {
                        // 正文在帧中途结束：明文不完整。
                        // 完全没有携带字节的正文是空流
                        // （即无首 token 的情形），
                        // 而不是解码失败。
                        decode_failed = true;
                    }
                    break;
                }
                Err(_) => {
                    ok = false;
                    idle_timeout = true;
                    break;
                }
            }
        }
        // Command Code EOF：最后一段 NDJSON 行可能缺少结尾换行。
        // 把适配器的尾部按普通 chunk 一样走
        // 同一条 scanner/converter 路径
        // （字节记账在原始字节到达时已经完成）。
        if !decode_failed && ok {
            let adapted_tail = adapter.finish_input();
            if !adapted_tail.is_empty() {
                if let Some(message) = scan.feed(&stream_protocol, &adapted_tail) {
                    let converted = converter.error_event(&message);
                    cancel_completed_flag.store(true, Ordering::SeqCst);
                    finalize_stream_attempt(
                        &telemetry,
                        &request_id_stream,
                        started,
                        failure_threshold,
                        circuit_open_seconds,
                        &candidate_stream,
                        attempts,
                        attempt_started,
                        clock.now_utc(),
                        AttemptOutcome::UpstreamError,
                        status.as_u16() as i64,
                        Some("upstream_error".into()),
                        true,
                        first_byte_ms,
                        first_token_ms,
                        converter_usage(&converter, &stream_protocol, false),
                        response_bytes,
                        upstream_protocol_stream.clone(),
                        upstream_model_stream.clone(),
                    );
                    if !converted.is_empty() {
                        yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                    }
                    return;
                }
                if first_token_ms.is_none() && scan.first_token_now {
                    first_token_ms = Some(
                        clock.now_utc()
                            .signed_duration_since(attempt_started)
                            .num_milliseconds(),
                    );
                    *stats.first_token_ms.lock() = first_token_ms;
                }
                let converted = converter.feed(&adapted_tail);
                sync_converter_usage(&converter, &stats, &stream_protocol);
                if converter.finished() {
                    cancel_completed_flag.store(true, Ordering::SeqCst);
                    finalize_stream_attempt(
                        &telemetry,
                        &request_id_stream,
                        started,
                        failure_threshold,
                        circuit_open_seconds,
                        &candidate_stream,
                        attempts,
                        attempt_started,
                        clock.now_utc(),
                        AttemptOutcome::Success,
                        status.as_u16() as i64,
                        None,
                        false,
                        first_byte_ms,
                        first_token_ms,
                        converter_usage(&converter, &stream_protocol, false),
                        response_bytes,
                        upstream_protocol_stream.clone(),
                        upstream_model_stream.clone(),
                    );
                    if !converted.is_empty() {
                        yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                    }
                    return;
                }
                if !converted.is_empty() {
                    yield Ok::<Bytes, Infallible>(Bytes::from(converted));
                }
            }
        }
        // 干净结束时收尾所有剩余项。转换器若在此结束
        // （上游在没有终态标记的情况下结束——裸关闭），
        // 就在尾部事件抵达客户端之前记录 outcome。
        // 一个从未产出首 token 的裸关闭属于首 token 保护
        // 违规，而不是成功。
        if !decode_failed && ok {
            let tail = converter.flush();
            if converter.finished() {
                let (outcome, error_kind, countable) = if first_token_ms.is_none() {
                    (
                        AttemptOutcome::StreamInterrupted,
                        Some("no_first_token".into()),
                        true,
                    )
                } else {
                    (AttemptOutcome::Success, None, false)
                };
                cancel_completed_flag.store(true, Ordering::SeqCst);
                finalize_stream_attempt(
                    &telemetry,
                    &request_id_stream,
                    started,
                    failure_threshold,
                    circuit_open_seconds,
                    &candidate_stream,
                    attempts,
                    attempt_started,
                    clock.now_utc(),
                    outcome,
                    status.as_u16() as i64,
                    error_kind,
                    countable,
                    first_byte_ms,
                    first_token_ms,
                    converter_usage(&converter, &stream_protocol, false),
                    response_bytes,
                    upstream_protocol_stream.clone(),
                    upstream_model_stream.clone(),
                );
                if !tail.is_empty() {
                    yield Ok::<Bytes, Infallible>(Bytes::from(tail));
                }
                return;
            }
            if !tail.is_empty() {
                yield Ok::<Bytes, Infallible>(Bytes::from(tail));
            }
        }
        // 生成器已运行到其终态遥测（帧中途结束、上游错误，
        // 或空闲/首 token 超时）：Drop 路径
        // 不得再追加一次重复的 `cancelled` 终结。
        cancel_completed_flag.store(true, Ordering::SeqCst);
        let finished = clock.now_utc();
        let (outcome, error_kind, final_status, countable): (
            AttemptOutcome,
            Option<String>,
            i64,
            bool,
        ) = if decode_failed {
            (
                AttemptOutcome::GatewayError,
                Some("upstream_decode_error".into()),
                status.as_u16() as i64,
                true,
            )
        } else if !ok {
            if idle_timeout {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("transport_timeout".into()),
                    504,
                    true,
                )
            } else {
                (
                    AttemptOutcome::StreamInterrupted,
                    Some("stream_interrupted".into()),
                    status.as_u16() as i64,
                    true,
                )
            }
        } else if first_token_ms.is_none() {
            // 直通转换（entry == 上游协议）：没有可检测的终态标记，
            // 且上游在从未产出首 token 的情况下关闭。
            // 像透明路径一样把它计入熔断，
            // 避免以空响应悄悄“成功”。
            (
                AttemptOutcome::StreamInterrupted,
                Some("no_first_token".into()),
                status.as_u16() as i64,
                true,
            )
        } else {
            (AttemptOutcome::Success, None, status.as_u16() as i64, false)
        };
        finalize_stream_attempt(
            &telemetry,
            &request_id_stream,
            started,
            failure_threshold,
            circuit_open_seconds,
            &candidate_stream,
            attempts,
            attempt_started,
            finished,
            outcome,
            final_status,
            error_kind,
            countable,
            first_byte_ms,
            first_token_ms,
            converter_usage(&converter, &stream_protocol, decode_failed),
            response_bytes,
            upstream_protocol_stream,
            upstream_model_stream,
        );
    };
    MappedStreamResult::Respond(transformed_body_response(
        status,
        response_headers_for_client,
        "text/event-stream",
        Body::from_stream(stream_body),
    ))
}
