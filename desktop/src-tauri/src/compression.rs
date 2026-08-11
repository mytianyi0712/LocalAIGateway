//! Bypass decoding of upstream `Content-Encoding` for observability.
//!
//! The proxy forwards upstream bytes to the client verbatim (keeping the
//! `Content-Encoding` header), but usage/scanning needs the plaintext. This
//! module decodes gzip / deflate / brotli / zstd incrementally so a streamed
//! response can be observed chunk by chunk without buffering the whole body.
//!
//! Decoding is best-effort: when a body is not decodable (truncated,
//! multi-member gzip, unknown framing), [`ObservableDecoder`] stops feeding
//! the observer and the raw bytes are forwarded unchanged — parsing failure
//! never affects forwarding (requirements.md:153).

use std::io;

/// Single-feed decoded-output cap: bounds the allocation of one `feed` call
/// while the decoder state is preserved for the next chunk.
pub const FEED_LIMIT: usize = 2 * 1024 * 1024;
/// Per-response cumulative decoded-output cap: a stream that keeps expanding
/// (zip-bomb style) disables observability once the total is exceeded.
pub const TOTAL_LIMIT: usize = 64 * 1024 * 1024;
/// Single-feed expansion ratio cap (`input * RATIO_LIMIT + 64 KiB` tolerance).
/// Catches small compressed chunks that balloon far beyond their input.
pub const RATIO_LIMIT: u64 = 64;

/// Streaming content decoder for one `Content-Encoding` value.
pub enum ContentDecoder {
    Identity,
    Gzip(Box<flate2::Decompress>),
    /// HTTP `deflate` = zlib-wrapped deflate.
    Deflate(Box<flate2::Decompress>),
    Brotli(Box<BrotliDecoder>),
    Zstd(Box<zstd::stream::raw::Decoder<'static>>),
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
            Some("zstd") => ContentDecoder::Zstd(Box::new(
                zstd::stream::raw::Decoder::new().expect("zstd decoder init"),
            )),
            _ => ContentDecoder::Identity,
        }
    }

    /// Decode one chunk. Returns `(plaintext, consumed, finished)`: at most
    /// `max_out` plaintext bytes, the number of input bytes the decoder
    /// actually consumed, and whether the compressed stream reached its end
    /// marker. The decoder keeps its state, so the remainder arrives with
    /// the next feed — callers that cannot afford lossy continuation MUST
    /// retain `input[consumed..]` themselves ([`RequiredDecoder`]).
    pub fn feed(&mut self, input: &[u8], max_out: usize) -> io::Result<(Vec<u8>, usize, bool)> {
        match self {
            ContentDecoder::Identity => Ok((input.to_vec(), input.len(), true)),
            ContentDecoder::Gzip(decompress) => inflate(decompress, input, max_out),
            ContentDecoder::Deflate(decompress) => inflate(decompress, input, max_out),
            ContentDecoder::Brotli(decoder) => decoder.feed(input, max_out),
            ContentDecoder::Zstd(decoder) => zstd_feed(decoder, input, max_out),
        }
    }
}

/// Observer-side wrapper: once decoding fails (or exceeds a limit) it stays
/// failed and returns no bytes, so the caller keeps forwarding the raw stream
/// untouched. Limits guard the observability path against decompression
/// bombs: single-feed output, per-response cumulative output, and per-feed
/// expansion ratio are all bounded.
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

/// Why a required (lossless) decode failed. Every variant is terminal for
/// the conversion path: the plaintext can no longer be trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The compressed stream is corrupt or truncated.
    CorruptFrame,
    /// The cumulative plaintext exceeded the configured total cap.
    CumulativeLimit,
}

/// Lossless streaming decoder for paths that MUST produce the full
/// plaintext (mapped conversion). Unlike [`ObservableDecoder`] it never
/// degrades to silence: unconsumed input is retained across feeds, any
/// decode failure is reported as [`DecodeError`], and the cumulative
/// plaintext is bounded by an explicit cap. A failure is sticky — the
/// caller must treat the response as failed, never as a short/empty body.
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

    /// Feed one network chunk; returns decoded plaintext (possibly empty)
    /// or a terminal [`DecodeError`]. After an error every subsequent call
    /// returns the same error.
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
            // Cap already reached with more input pending: any further
            // plaintext would exceed `max_total`.
            return Err(self.fail(DecodeError::CumulativeLimit));
        }
        let (decoded, consumed, finished) = self.decoder.feed(&input, budget).map_err(|_| {
            self.fail(DecodeError::CorruptFrame);
            self.last_error
        })?;
        // No expansion-ratio heuristic here: highly compressible *legit*
        // responses must convert in full. The hard bounds are the per-feed
        // output cap (budget) and the cumulative plaintext cap below. One
        // decode call may overshoot `budget` by up to its internal buffer
        // (16 KiB), which is the cap granularity.
        self.total_out += decoded.len();
        if self.total_out > self.max_total || (!finished && self.total_out >= self.max_total) {
            // Plaintext past the cap, or the cap hit with the stream still
            // open (more output pending): continuing would silently drop
            // the remainder.
            return Err(self.fail(DecodeError::CumulativeLimit));
        }
        self.finished = finished;
        self.pending.extend_from_slice(&input[consumed..]);
        Ok(decoded)
    }

    /// Whether the compressed stream reached its end marker. A caller whose
    /// input ended MUST check this: `false` means the body was truncated
    /// mid-frame and the plaintext is incomplete.
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

/// Inflate with a fixed output buffer; `Decompress` keeps its window state
/// across calls, so split inputs decode correctly. Stops once `max_out`
/// bytes were produced (the decoder state is preserved for the next feed).
/// Returns the produced bytes, how many input bytes were consumed, and
/// whether the stream end marker was reached.
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
                    // Output cap reached or needs more input than this chunk
                    // provides; the stream continues on the next feed.
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
        // `remaining == 0` signals a finished frame (zstd run() returns a
        // hint for the next input; Ok(0) means the frame just finished).
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

/// Streaming brotli: keeps the unconsumed input and decoder state across
/// calls so chunks can be arbitrarily split.
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

    /// Decode one chunk. Returns `(plaintext, consumed, finished)`; the
    /// unconsumed input stays in this decoder's own buffer, so `consumed` is
    /// only the share of `chunk` the decompressor has taken (either turned
    /// into output or compacted away).
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
                        // No progress possible with the current input; wait
                        // for the next chunk instead of spinning.
                        break;
                    }
                }
                brotli::BrotliResult::NeedsMoreInput => {
                    // Compact the consumed prefix and wait for the next chunk.
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

    /// Feed `compressed` through a decoder in `chunk_size` pieces and assert
    /// the concatenated output equals the plaintext.
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

    /// Roundtrip through `RequiredDecoder` with an artificial output cap so
    /// every feed stops early and the unconsumed input must be carried over.
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
        // Truncated gzip stream: decoding must error, not panic.
        let mut gz = gzip_bytes();
        gz.truncate(gz.len() / 2);
        let mut decoder = ContentDecoder::from_encoding(Some(&HeaderValue::from_static("gzip")));
        assert!(
            decoder.feed(&gz, FEED_LIMIT).is_ok() || decoder.feed(&gz, FEED_LIMIT).is_err(),
            "truncated input must not panic"
        );
        // A second gzip member concatenated after the first is not decodable
        // by a single-member decoder; feeding it must not panic (it may
        // error or return nothing).
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
        // Force a failure state directly and assert the wrapper stays silent.
        let mut corrupt = ObservableDecoder::new(ContentDecoder::Gzip(Box::new(
            flate2::Decompress::new_gzip(15),
        )));
        let _ = corrupt.feed_observable(b"not gzip data at all........");
        corrupt.failed = true;
        assert_eq!(corrupt.feed_observable(b"more"), Vec::<u8>::new());
        let _ = observable;
    }

    /// P1-4: a real high-compression payload (16 MiB of zeros, ~16 KiB on the
    /// wire) must trip the per-feed expansion-ratio limit on the first feed:
    /// output is capped at 2 MiB, observability turns off, and later feeds
    /// return nothing. The failure is produced by the actual payload — the
    /// `failed` flag is never poked by hand.
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

    /// P1-4: the cumulative per-response cap turns observability off once
    /// the decoded total reaches 64 MiB, even when every single feed is
    /// well within the per-feed limit.
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

    /// P1-2: RequiredDecoder is lossless across arbitrary chunk splits for
    /// every encoding, including when a single feed's output cap stops the
    /// decoder mid-frame and the unconsumed input must carry over.
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

    /// P1-2: a single network chunk expanding to more than 2 MiB plaintext
    /// must be split across feeds losslessly — the output cap stops the
    /// decoder mid-frame and the unconsumed input carries over.
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
        // One 4 KiB chunk expands far past the 2 MiB per-feed cap; the
        // decoder must stop, keep its state, and finish from the next chunk.
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

    /// P1-2: a corrupt compressed stream is a terminal `CorruptFrame`
    /// error, never a silent short output. Each encoding gets input whose
    /// framing is invalid by construction (wrong magic / corrupted frame).
    #[test]
    fn required_decoder_reports_corrupt_frames() {
        // Invalid magic: every decoder must reject the framing outright.
        let garbage = b"this is definitely not a compressed stream........";
        for encoding in ["gzip", "deflate", "br", "zstd"] {
            let mut required = RequiredDecoder::new(
                ContentDecoder::from_encoding(Some(&HeaderValue::from_static(encoding))),
                TOTAL_LIMIT,
            );
            let mut outcome = required.feed_required(garbage);
            // Some decoders (brotli) may consume the header-looking prefix
            // before failing; a second garbage feed must then surface it.
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

        // Mid-stream corruption: a valid frame with bytes flipped inside the
        // payload must error, not silently stop producing output.
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

    /// P1-2: a high-compression payload (16 MiB of zeros, ~16 KiB on the
    /// wire) must convert in FULL on the required path — the expansion-ratio
    /// heuristic is observability-only; the hard bounds are the cumulative
    /// cap and the per-feed output cap.
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

    /// P1-2: the cumulative plaintext cap is enforced for the required path:
    /// a body whose decoded size exceeds `max_total` fails with
    /// `CumulativeLimit` instead of being silently truncated. One decode
    /// call may overshoot the cap by its internal buffer, so a cap below
    /// the plaintext trips on the first feed.
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
