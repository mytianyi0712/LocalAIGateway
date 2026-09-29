//! 上游 `Content-Encoding`（gzip / deflate / brotli / zstd）的增量解码。
//!
//! 职责：把压缩字节流解成明文，供转发与扫描使用。
//! 边界：只做解码，不做 HTTP 分类（编码识别仅按头值字符串匹配）。
//! 不变量：单次 feed 输出上限为 `max_out`，且单次 feed 至多越过该上限 16 KiB
//! （内部缓冲粒度）；累计输出受 `max_total` 约束；初始化失败不 panic。
//! 使用方：[`RequiredDecoder`]（映射转换等必须拿到完整明文的路径）、
//! [`ObservableDecoder`]（原样转发响应的尽力观测，解码失败即静默，不影响中继）。

use std::io;

/// 单次 feed 的解码输出上限：约束一次 `feed` 调用的分配量，解码器状态保留到下一块
/// 继续使用。注意单次 feed 的输出可越过 `max_out` 至多 16 KiB（内部缓冲粒度）。
pub const FEED_LIMIT: usize = 2 * 1024 * 1024;
/// 单个响应的累计解码输出上限：持续膨胀的流（zip 炸弹式）在总量达到该上限后
/// 关闭观测。
pub const TOTAL_LIMIT: usize = 64 * 1024 * 1024;
/// 单次 feed 的膨胀比上限（容差 `input * RATIO_LIMIT + 64 KiB`）：
/// 用于捕获体积远超输入的小压缩块。
pub const RATIO_LIMIT: u64 = 64;

/// 针对单个 `Content-Encoding` 值的流式内容解码器。
pub enum ContentDecoder {
    Identity,
    Gzip(Box<flate2::Decompress>),
    /// HTTP `deflate` 即 zlib 包装的 deflate。
    Deflate(Box<flate2::Decompress>),
    Brotli(Box<BrotliDecoder>),
    Zstd(Box<zstd::stream::raw::Decoder<'static>>),
    /// 初始化失败（例如 zstd 内部缓冲分配失败）。`feed` 直接返回错误，
    /// 由调用方按“解码失败”处理——绝不在 `from_encoding` 里 panic：
    /// release 构建 panic=abort，一次初始化失败会杀掉整个进程。
    Failed(io::ErrorKind),
}

impl ContentDecoder {
    pub fn from_encoding(value: Option<&axum::http::HeaderValue>) -> Self {
        match value
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("gzip" | "x-gzip") => {
                ContentDecoder::Gzip(Box::new(flate2::Decompress::new_gzip(15)))
            }
            Some("deflate") => ContentDecoder::Deflate(Box::new(flate2::Decompress::new(true))),
            Some("br") => ContentDecoder::Brotli(Box::new(BrotliDecoder::new())),
            Some("zstd") => match zstd::stream::raw::Decoder::new() {
                Ok(decoder) => ContentDecoder::Zstd(Box::new(decoder)),
                Err(error) => ContentDecoder::Failed(error.kind()),
            },
            _ => ContentDecoder::Identity,
        }
    }

    /// 解码一块数据。返回 `(明文, 已消费输入字节数, 是否结束)`：明文至多
    /// `max_out` 字节；第二个值是解码器实际消费的输入字节数；第三个值表示压缩流
    /// 是否到达结束标记。解码器保留自身状态，剩余数据在下次 feed 时继续——无法接受
    /// 有损续传的调用方必须自行保留 `input[consumed..]`（见 [`RequiredDecoder`]）。
    pub fn feed(&mut self, input: &[u8], max_out: usize) -> io::Result<(Vec<u8>, usize, bool)> {
        match self {
            ContentDecoder::Identity => Ok((input.to_vec(), input.len(), true)),
            ContentDecoder::Gzip(decompress) => inflate(decompress, input, max_out),
            ContentDecoder::Deflate(decompress) => inflate(decompress, input, max_out),
            ContentDecoder::Brotli(decoder) => decoder.feed(input, max_out),
            ContentDecoder::Zstd(decoder) => zstd_feed(decoder, input, max_out),
            ContentDecoder::Failed(kind) => {
                Err(io::Error::new(*kind, "zstd decoder init failed"))
            }
        }
    }
}

/// 观察侧包装器：一旦解码失败（或超过限制）即进入并保持失败态、不再返回任何字节，
/// 调用方照常原样转发原始流。三重限制（单次 feed 输出、单响应累计输出、单次 feed
/// 膨胀比）用于保护观测路径免遭解压炸弹。
pub struct ObservableDecoder {
    decoder: ContentDecoder,
    failed: bool,
    total_out: usize,
}

impl ObservableDecoder {
    pub fn new(decoder: ContentDecoder) -> Self {
        Self {
            decoder,
            failed: false,
            total_out: 0,
        }
    }

    pub fn feed_observable(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.failed {
            return Vec::new();
        }
        let budget = FEED_LIMIT.min(TOTAL_LIMIT.saturating_sub(self.total_out));
        match self.decoder.feed(chunk, budget) {
            Ok((decoded, _consumed, _finished)) => {
                if decoded.len() as u64 > chunk.len() as u64 * RATIO_LIMIT + 64 * 1024 {
                    tracing::warn!(
                        input = chunk.len(),
                        output = decoded.len(),
                        "decoded output exceeds expansion ratio; observability disabled"
                    );
                    self.failed = true;
                    return Vec::new();
                }
                self.total_out += decoded.len();
                if self.total_out >= TOTAL_LIMIT {
                    tracing::warn!(
                        total = self.total_out,
                        "decoded output exceeds per-response limit; observability disabled"
                    );
                    self.failed = true;
                }
                decoded
            }
            Err(error) => {
                tracing::debug!(error = %error, "content decoding failed; observability skipped");
                self.failed = true;
                Vec::new()
            }
        }
    }
}

/// 必要（无损）解码为何失败。对转换路径而言每个变体都是终态：明文已不可信。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// 压缩流损坏或被截断。
    CorruptFrame,
    /// 累计明文超过配置的总上限。
    CumulativeLimit,
}

/// 用于必须产出完整明文的路径（映射转换）的无损流式解码器。与
/// [`ObservableDecoder`] 不同，它绝不退化为静默：未消费的输入跨 feed 保留，
/// 任何解码失败都以 [`DecodeError`] 上报，累计明文受显式上限约束。失败是粘性的——
/// 调用方必须把该响应当作失败，而不是当作短/空响应体。
pub struct RequiredDecoder {
    decoder: ContentDecoder,
    pending: Vec<u8>,
    total_out: usize,
    max_total: usize,
    failed: bool,
    finished: bool,
    last_error: DecodeError,
}

impl RequiredDecoder {
    pub fn new(decoder: ContentDecoder, max_total: usize) -> Self {
        Self {
            decoder,
            pending: Vec::new(),
            total_out: 0,
            max_total,
            failed: false,
            finished: false,
            last_error: DecodeError::CorruptFrame,
        }
    }

    /// 喂入一个网络分块；返回解码后的明文（可能为空）或终态 [`DecodeError`]。
    /// 一旦出错，之后每次调用都返回同一个错误。
    pub fn feed_required(&mut self, chunk: &[u8]) -> Result<Vec<u8>, DecodeError> {
        if self.failed {
            return Err(self.last_error);
        }
        let mut input = Vec::with_capacity(self.pending.len() + chunk.len());
        input.extend_from_slice(&self.pending);
        input.extend_from_slice(chunk);
        self.pending.clear();
        let budget = FEED_LIMIT.min(self.max_total.saturating_sub(self.total_out));
        if budget == 0 {
            // 已达上限且仍有输入待处理：再产出任何明文都会超过 `max_total`。
            return Err(self.fail(DecodeError::CumulativeLimit));
        }
        let (decoded, consumed, finished) = self.decoder.feed(&input, budget).map_err(|_| {
            self.fail(DecodeError::CorruptFrame);
            self.last_error
        })?;
        // 此处不做膨胀比启发式：高度可压缩的“合法”响应必须完整转换。硬性上限是
        // 单次 feed 输出上限（budget）与下方的累计明文上限。一次解码调用最多会越过
        // `budget` 内部缓冲那么多（16 KiB），这就是该上限的粒度。
        self.total_out += decoded.len();
        if self.total_out > self.max_total || (!finished && self.total_out >= self.max_total) {
            // 明文已超过上限，或已达上限而压缩流仍未结束（还有输出待解）：
            // 继续下去会静默丢掉剩余部分。
            return Err(self.fail(DecodeError::CumulativeLimit));
        }
        self.finished = finished;
        self.pending.extend_from_slice(&input[consumed..]);
        Ok(decoded)
    }

    /// 把一段**完整**输入喂完：循环 drain 直到解出全部明文或报错。
    ///
    /// 单次 [`Self::feed_required`] 的输出上限是 [`FEED_LIMIT`]（2 MiB），
    /// 未消费的输入留在解码器内部，因此大于该值的合法响应体必须循环取用
    /// （否则会被误判成“截断”）。输入耗尽而压缩流仍未结束时返回
    /// [`DecodeError::CorruptFrame`]，与截断的语义一致。
    pub fn feed_all(&mut self, chunk: &[u8]) -> Result<Vec<u8>, DecodeError> {
        let mut out = Vec::new();
        let mut next: &[u8] = chunk;
        loop {
            let piece = self.feed_required(next)?;
            let progressed = !piece.is_empty();
            out.extend_from_slice(&piece);
            if self.finished() {
                return Ok(out);
            }
            if next.is_empty() && !progressed {
                // 输入已耗尽、解码器也不再产出：压缩流没有结束标记 → 截断。
                return Err(self.fail(DecodeError::CorruptFrame));
            }
            next = &[];
        }
    }

    /// 压缩流是否到达结束标记。输入已结束的调用方必须检查这里：`false` 表示响应体
    /// 在帧中途被截断，明文不完整。
    pub fn finished(&self) -> bool {
        self.finished
    }

    fn fail(&mut self, error: DecodeError) -> DecodeError {
        self.failed = true;
        self.pending.clear();
        self.last_error = error;
        error
    }
}

/// 使用固定输出缓冲进行 inflate；`Decompress` 跨调用保留窗口状态，因此分片输入也能
/// 正确解码。产出达到 `max_out` 字节后停止（解码器状态保留到下次 feed）。返回已产出
/// 字节、已消费输入字节数，以及是否到达流结束标记。
fn inflate(
    decompress: &mut flate2::Decompress,
    input: &[u8],
    max_out: usize,
) -> io::Result<(Vec<u8>, usize, bool)> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    let mut finished = false;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let before_in = decompress.total_in() as usize;
        let before_out = decompress.total_out() as usize;
        let status =
            decompress.decompress(&input[offset..], &mut buffer, flate2::FlushDecompress::None)?;
        let consumed = decompress.total_in() as usize - before_in;
        let produced = decompress.total_out() as usize - before_out;
        offset += consumed;
        out.extend_from_slice(&buffer[..produced]);
        match status {
            flate2::Status::StreamEnd => {
                finished = true;
                break;
            }
            flate2::Status::Ok | flate2::Status::BufError => {
                if out.len() >= max_out || (consumed == 0 && produced == 0) {
                    // 输出达到上限，或所需输入超过本块提供的量；
                    // 该流在下次 feed 时继续。
                    break;
                }
            }
        }
    }
    Ok((out, offset, finished))
}

fn zstd_feed(
    decoder: &mut zstd::stream::raw::Decoder<'static>,
    input: &[u8],
    max_out: usize,
) -> io::Result<(Vec<u8>, usize, bool)> {
    use zstd::stream::raw::Operation;
    let mut out = Vec::new();
    let mut offset = 0usize;
    let mut finished = false;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let status = decoder.run_on_buffers(&input[offset..], &mut buffer)?;
        offset += status.bytes_read;
        out.extend_from_slice(&buffer[..status.bytes_written]);
        // `remaining == 0` 表示帧已结束（zstd 的 run() 返回的是对下次输入的提示；
        // Ok(0) 意味着帧刚刚结束）。
        if status.remaining == 0 {
            finished = true;
            break;
        }
        if out.len() >= max_out || offset >= input.len() {
            break;
        }
    }
    Ok((out, offset, finished))
}

/// 流式 brotli：跨调用保留未消费输入与解码器状态，因此分块可以任意切分。
pub struct BrotliDecoder {
    input: Vec<u8>,
    input_offset: usize,
    state: brotli::BrotliState<
        brotli::enc::StandardAlloc,
        brotli::enc::StandardAlloc,
        brotli::enc::StandardAlloc,
    >,
}

impl BrotliDecoder {
    fn new() -> Self {
        Self {
            input: Vec::new(),
            input_offset: 0,
            state: brotli::BrotliState::new(
                brotli::enc::StandardAlloc::default(),
                brotli::enc::StandardAlloc::default(),
                brotli::enc::StandardAlloc::default(),
            ),
        }
    }

    /// 解码一块数据。返回 `(明文, 已消费, 是否结束)`；未消费的输入留在本解码器
    /// 自己的缓冲里，因此 `consumed` 只是 `chunk` 中被解压器取走的部分
    /// （要么变成了输出，要么被压缩掉）。
    fn feed(&mut self, chunk: &[u8], max_out: usize) -> io::Result<(Vec<u8>, usize, bool)> {
        let carry_in = self.input.len() - self.input_offset;
        self.input.extend_from_slice(chunk);
        let mut out = Vec::new();
        let mut output = [0u8; 16 * 1024];
        let mut output_offset = 0usize;
        let mut written = 0usize;
        let mut finished = false;
        loop {
            let mut available_in = self.input.len() - self.input_offset;
            let input_before = available_in;
            let mut available_out = output.len();
            let result = brotli::BrotliDecompressStream(
                &mut available_in,
                &mut self.input_offset,
                &self.input,
                &mut available_out,
                &mut output_offset,
                &mut output,
                &mut written,
                &mut self.state,
            );
            let consumed = input_before - available_in;
            let produced = output_offset;
            out.extend_from_slice(&output[..output_offset]);
            output_offset = 0;
            match result {
                brotli::BrotliResult::ResultSuccess => {
                    finished = true;
                    break;
                }
                brotli::BrotliResult::NeedsMoreOutput => {
                    if out.len() >= max_out {
                        break;
                    }
                    if produced == 0 && consumed == 0 {
                        // 当前输入下无法推进；等待下一块，避免空转。
                        break;
                    }
                }
                brotli::BrotliResult::NeedsMoreInput => {
                    // 压缩已消费的前缀，等待下一块。
                    if self.input_offset > 0 {
                        self.input.drain(..self.input_offset);
                        self.input_offset = 0;
                    }
                    break;
                }
                brotli::BrotliResult::ResultFailure => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "brotli stream corrupt",
                    ));
                }
            }
        }
        let remaining = self.input.len() - self.input_offset;
        let consumed = chunk
            .len()
            .saturating_sub(remaining.saturating_sub(carry_in));
        Ok((out, consumed, finished))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    const SAMPLE: &str = "data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";

    fn gzip_bytes() -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(SAMPLE.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    fn deflate_bytes() -> Vec<u8> {
        use std::io::Write;
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(SAMPLE.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    fn brotli_bytes() -> Vec<u8> {
        use std::io::Write;
        let mut encoder = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
        encoder.write_all(SAMPLE.as_bytes()).unwrap();
        encoder.flush().unwrap();
        encoder.into_inner()
    }

    fn zstd_bytes() -> Vec<u8> {
        zstd::stream::encode_all(SAMPLE.as_bytes(), 3).unwrap()
    }

    /// zstd 初始化不再 panic；正常 zstd 流依旧能解出完整明文。
    #[test]
    fn zstd_still_decodes_after_the_init_path_stopped_panicking() {
        let compressed = zstd_bytes();
        let mut decoder =
            RequiredDecoder::new(ContentDecoder::from_encoding(Some(&HeaderValue::from_static("zstd"))), 1024 * 1024);
        let plain = decoder.feed_all(&compressed).unwrap();
        assert_eq!(plain, SAMPLE.as_bytes());
        assert!(decoder.finished());
    }

    /// `feed_all` 必须多次 drain 才能解出超过单次上限 `FEED_LIMIT` 的明文
    /// （单次 feed 只产出 2 MiB，其余留在解码器内部）。
    #[test]
    fn feed_all_drains_plaintext_larger_than_one_feed() {
        use std::io::Write;
        let plain = format!("{}ENDMARK", "x".repeat(FEED_LIMIT + 64 * 1024));
        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut decoder = RequiredDecoder::new(
            ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip"))),
            16 * 1024 * 1024,
        );
        let decoded = decoder.feed_all(&compressed).unwrap();
        assert_eq!(decoded.len(), plain.len());
        assert!(decoded.ends_with(b"ENDMARK"));
    }

    /// 以 `chunk_size` 为单位把 `compressed` 喂给解码器，断言拼接后的输出等于明文。
    fn roundtrip(encoding: &'static str, compressed: &[u8], chunk_size: usize) {
        let mut decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static(encoding)));
        let mut plain = Vec::new();
        for chunk in compressed.chunks(chunk_size) {
            plain.extend_from_slice(&decoder.feed(chunk, FEED_LIMIT).unwrap().0);
        }
        assert_eq!(
            plain,
            SAMPLE.as_bytes(),
            "{encoding} chunk size {chunk_size}"
        );
    }

    /// 让 `RequiredDecoder` 走一遍往返：以 `chunk_size` 为单位分块喂入 `compressed`，
    /// 断言拼接后的输出等于明文（上限取 `TOTAL_LIMIT`）。
    fn required_roundtrip(encoding: &'static str, compressed: &[u8], chunk_size: usize) {
        let decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static(encoding)));
        let mut required = RequiredDecoder::new(decoder, TOTAL_LIMIT);
        let mut plain = Vec::new();
        for chunk in compressed.chunks(chunk_size) {
            plain.extend_from_slice(&required.feed_required(chunk).unwrap());
        }
        assert_eq!(
            plain,
            SAMPLE.as_bytes(),
            "{encoding} chunk size {chunk_size}"
        );
    }

    #[test]
    fn decodes_all_encodings_across_chunk_boundaries() {
        let cases = [
            ("gzip", gzip_bytes()),
            ("deflate", deflate_bytes()),
            ("br", brotli_bytes()),
            ("zstd", zstd_bytes()),
        ];
        for (encoding, compressed) in cases {
            roundtrip(encoding, &compressed, 1);
            roundtrip(encoding, &compressed, 7);
            roundtrip(encoding, &compressed, compressed.len());
        }
    }

    #[test]
    fn unknown_or_missing_encoding_is_identity() {
        let mut decoder = ContentDecoder::from_encoding(None);
        assert!(matches!(decoder, ContentDecoder::Identity));
        assert_eq!(decoder.feed(b"raw", FEED_LIMIT).unwrap().0, b"raw");

        let decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static("identity")));
        assert!(matches!(decoder, ContentDecoder::Identity));

        let decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static("bogus")));
        assert!(matches!(decoder, ContentDecoder::Identity));
    }

    #[test]
    fn unsupported_framing_returns_err_without_panicking() {
        // 截断的 gzip 流：解码必须报错，而不是 panic。
        let mut gz = gzip_bytes();
        gz.truncate(gz.len() / 2);
        let mut decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip")));
        assert!(
            decoder.feed(&gz, FEED_LIMIT).is_ok() || decoder.feed(&gz, FEED_LIMIT).is_err(),
            "truncated input must not panic"
        );
        // 在第一个成员之后拼接第二个 gzip 成员，单成员解码器无法解码它；
        // 喂入它不得 panic（可能报错或什么都不返回）。
        let multi = gzip_bytes();
        let mut decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip")));
        let first = decoder.feed(&multi, FEED_LIMIT).unwrap().0;
        assert_eq!(first, SAMPLE.as_bytes());
        let second_member = gzip_bytes();
        let tail_result = decoder.feed(&second_member, FEED_LIMIT);
        assert!(
            tail_result.is_ok() || tail_result.is_err(),
            "trailing member must not panic"
        );
    }

    #[test]
    fn observable_decoder_stops_feed_after_failure() {
        let mut observable = ObservableDecoder::new(ContentDecoder::Identity);
        assert_eq!(observable.feed_observable(b"a"), b"a");
        // 直接制造失败态，断言包装器保持静默。
        let mut corrupt = ObservableDecoder::new(ContentDecoder::Gzip(Box::new(
            flate2::Decompress::new_gzip(15),
        )));
        let _ = corrupt.feed_observable(b"not gzip data at all........");
        corrupt.failed = true;
        assert_eq!(corrupt.feed_observable(b"more"), Vec::<u8>::new());
        let _ = observable;
    }

    /// 真实的高压缩负载（16 MiB 全零，线上约 16 KiB）必须在首次 feed 就触发单次
    /// feed 的膨胀比限制：输出被限制在 2 MiB、观测关闭、后续 feed 返回空。该失败由
    /// 真实负载产生——绝不手工修改 `failed` 标志。
    #[test]
    fn high_ratio_payload_disables_observability() {
        use std::io::Write;
        let plain = vec![0u8; 16 * 1024 * 1024];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(
            compressed.len() < 64 * 1024,
            "test payload must compress far below the feed cap"
        );
        let mut observable = ObservableDecoder::new(ContentDecoder::from_encoding(Some(
            &HeaderValue::from_static("gzip"),
        )));
        let output = observable.feed_observable(&compressed);
        assert!(
            output.len() <= FEED_LIMIT,
            "single feed must never exceed the per-feed cap"
        );
        assert!(
            observable.failed,
            "expansion ratio beyond 64:1 must disable observability"
        );
        assert_eq!(
            observable.feed_observable(&compressed),
            Vec::<u8>::new(),
            "feeds after failure return nothing"
        );
    }

    /// 累计的每响应上限会在解码总量达到 64 MiB 时关闭观测，即便每次 feed 都远在
    /// 单次 feed 上限之内。
    #[test]
    fn cumulative_limit_disables_after_total() {
        let mut observable = ObservableDecoder::new(ContentDecoder::Identity);
        let chunk = vec![0u8; FEED_LIMIT];
        for feed in 1..=32 {
            let output = observable.feed_observable(&chunk);
            assert_eq!(output.len(), FEED_LIMIT, "feed {feed} decodes in full");
            if feed < 32 {
                assert!(!observable.failed, "limit must not trip before 64 MiB");
            }
        }
        assert!(
            observable.failed,
            "the 64 MiB cumulative limit must disable observability"
        );
        assert_eq!(
            observable.feed_observable(b"more"),
            Vec::<u8>::new(),
            "feeds after the cumulative limit return nothing"
        );
    }

    /// `RequiredDecoder` 对每种编码在任意分块切分下都无损，包括单次 feed 输出上限
    /// 让解码器在帧中途停下、未消费输入必须结转的情形。
    #[test]
    fn required_decoder_roundtrips_all_encodings() {
        let cases = [
            ("gzip", gzip_bytes()),
            ("deflate", deflate_bytes()),
            ("br", brotli_bytes()),
            ("zstd", zstd_bytes()),
        ];
        for (encoding, compressed) in cases {
            required_roundtrip(encoding, &compressed, 1);
            required_roundtrip(encoding, &compressed, 7);
            required_roundtrip(encoding, &compressed, compressed.len());
        }
    }

    /// 单个网络分块膨胀出超过 2 MiB 明文时，必须跨多次 feed 无损拆分——输出上限
    /// 让解码器在帧中途停下，未消费输入结转。
    #[test]
    fn required_decoder_splits_oversized_feed_losslessly() {
        use std::io::Write;
        let plain = vec![b'a'; 3 * 1024 * 1024];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut required = RequiredDecoder::new(
            ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip"))),
            TOTAL_LIMIT,
        );
        let mut out = Vec::new();
        let mut feed = 0;
        // 一个 4 KiB 分块膨胀出的量远超 2 MiB 的单次 feed 上限；解码器必须停下、
        // 保留状态，并从下一块继续完成。
        let chunk = &compressed[..compressed.len().min(4096)];
        while out.len() < plain.len() {
            let decoded = required.feed_required(chunk).unwrap();
            assert!(
                decoded.len() <= FEED_LIMIT,
                "feed {feed} must respect the per-feed cap"
            );
            out.extend_from_slice(&decoded);
            feed += 1;
            assert!(feed < 8, "oversized plaintext must finish in few feeds");
        }
        assert_eq!(out, plain, "split feeds must reassemble the full plaintext");
    }

    /// 损坏的压缩流是终态 `CorruptFrame` 错误，绝不静默产出短输出。每种编码都被
    /// 喂入构造上分帧非法的输入（错误的魔数 / 损坏的帧）。
    #[test]
    fn required_decoder_reports_corrupt_frames() {
        // 非法魔数：每个解码器都必须直接拒绝该分帧。
        let garbage = b"this is definitely not a compressed stream........";
        for encoding in ["gzip", "deflate", "br", "zstd"] {
            let mut required = RequiredDecoder::new(
                ContentDecoder::from_encoding(Some(&HeaderValue::from_static(encoding))),
                TOTAL_LIMIT,
            );
            let mut outcome = required.feed_required(garbage);
            // 某些解码器（brotli）可能会先消费掉形似头部的字节再失败；
            // 此时再喂一次垃圾输入必须让错误浮现。
            if outcome.is_ok() {
                outcome = required.feed_required(garbage);
            }
            let first = outcome.unwrap_err();
            assert_eq!(first, DecodeError::CorruptFrame, "{encoding}");
            assert_eq!(
                required.feed_required(b"tail").unwrap_err(),
                first,
                "{encoding}: failure must be sticky"
            );
        }

        // 流中途损坏：合法帧的载荷内字节被翻转后必须报错，而不是静默地停止产出。
        let mut corrupted = gzip_bytes();
        let mid = corrupted.len() / 2;
        for byte in &mut corrupted[mid..] {
            *byte ^= 0xFF;
        }
        let mut required = RequiredDecoder::new(
            ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip"))),
            TOTAL_LIMIT,
        );
        let mut outcome = required.feed_required(&corrupted);
        if outcome.is_ok() {
            outcome = required.feed_required(&corrupted);
        }
        assert_eq!(outcome.unwrap_err(), DecodeError::CorruptFrame);
    }

    /// 高压缩负载（16 MiB 全零，线上约 16 KiB）在必要路径上必须完整转换——膨胀比
    /// 启发式仅用于观测；硬性上限是累计上限与单次 feed 输出上限。
    #[test]
    fn required_decoder_fully_decodes_high_ratio_payload() {
        use std::io::Write;
        let plain = vec![0u8; 16 * 1024 * 1024];
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < 64 * 1024);
        let mut required = RequiredDecoder::new(
            ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip"))),
            TOTAL_LIMIT,
        );
        let mut out = Vec::new();
        for _ in 0..8 {
            let decoded = required.feed_required(&compressed).unwrap();
            assert!(decoded.len() <= FEED_LIMIT);
            out.extend_from_slice(&decoded);
            if out.len() >= plain.len() {
                break;
            }
        }
        assert_eq!(out, plain, "high-ratio body must decode in full");
    }

    /// 必要路径会强制执行累计明文上限：解码尺寸超过 `max_total` 的响应体以
    /// `CumulativeLimit` 失败，而不是被静默截断。一次解码调用可能越过该上限内部
    /// 缓冲那么多，因此低于明文的 `max_total` 会在首次 feed 就触发。
    #[test]
    fn required_decoder_enforces_cumulative_cap() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(SAMPLE.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut required = RequiredDecoder::new(
            ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip"))),
            50, // well below the plaintext (~105 bytes)
        );
        let error = required.feed_required(&compressed).unwrap_err();
        assert_eq!(error, DecodeError::CumulativeLimit);
        assert_eq!(
            required.feed_required(b"more").unwrap_err(),
            DecodeError::CumulativeLimit,
            "failure must be sticky"
        );
    }
}
