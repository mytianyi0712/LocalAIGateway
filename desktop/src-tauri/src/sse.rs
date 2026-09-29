//! SSE 分帧与 `data:` 负载提取的单一实现（convert / proxy / remote_compaction 共用）。
//!
//! 边界：只做字节级切分与 `data:` 前缀处理，不解析 JSON、不管连接与错误码。
//! 关键不变量：`block_data` 的负载与「逐行 append + 行尾换行 + 整体 trim」等价，
//! 因此原本分散在三处的四套实现（`convert::sse_block_events`、
//! `remote_compaction::consume_block`、`proxy::scan_observable_lines`、
//! `convert::parse_gemini_events`）行为一致。

/// 去掉首尾 ASCII 空白。
pub(crate) fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

/// 按空行（`\n\n`）切出完整块。不足一块的残留留在 `buffer` 中，
/// 由调用方在 EOF（tail）时自行处理。
pub(crate) fn split_blocks(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut blocks = Vec::new();
    while let Some(marker) = buffer.windows(2).position(|window| window == b"\n\n") {
        let block = buffer.drain(..marker).collect::<Vec<_>>();
        buffer.drain(..2);
        if !block.is_empty() {
            blocks.push(block);
        }
    }
    blocks
}

/// 一个块内的 `data:` 负载：各行 trim 后以 `\n` 连接，再 trim 首尾。
/// 块内没有 `data:` 行时返回 `None`。
pub(crate) fn block_data(block: &[u8]) -> Option<Vec<u8>> {
    let mut data_lines: Vec<&[u8]> = Vec::new();
    for line in block.split(|byte| *byte == b'\n') {
        if let Some(rest) = trim_ascii(line).strip_prefix(b"data:") {
            data_lines.push(trim_ascii(rest));
        }
    }
    if data_lines.is_empty() {
        return None;
    }
    let payload = data_lines.join(&b'\n');
    Some(trim_ascii(&payload).to_vec())
}

/// 单行的 `data:` 负载：去掉前缀并 trim；没有该前缀时返回 `None`。
pub(crate) fn line_data(line: &[u8]) -> Option<&[u8]> {
    trim_ascii(line)
        .strip_prefix(b"data:")
        .map(trim_ascii)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_data_joins_multiple_data_lines_with_newline() {
        let block = b"event: x\ndata: {\"a\":1}\ndata: {\"b\":2}";
        assert_eq!(
            block_data(block).as_deref(),
            Some(br#"{"a":1}
{"b":2}"#.as_slice())
        );
    }

    /// 统一实现必须与两处旧实现逐字节一致：旧 `convert` 版「收集行 → join('\n') → trim」，
    /// 旧 `remote_compaction` 版「逐行 append + 行尾 '\n' → trim」。
    #[test]
    fn block_data_matches_the_two_legacy_implementations() {
        fn legacy_convert(block: &[u8]) -> Option<Vec<u8>> {
            let mut data_lines: Vec<&[u8]> = Vec::new();
            for line in block.split(|byte| *byte == b'\n') {
                let line = trim_ascii(line);
                if let Some(rest) = line.strip_prefix(b"data:") {
                    data_lines.push(trim_ascii(rest));
                }
            }
            if data_lines.is_empty() {
                return None;
            }
            let payload = data_lines.join(&b'\n');
            Some(trim_ascii(&payload).to_vec())
        }
        fn legacy_compaction(block: &[u8]) -> Vec<u8> {
            let mut data = Vec::new();
            for line in block.split(|byte| *byte == b'\n') {
                let line = trim_ascii(line);
                if let Some(rest) = line.strip_prefix(b"data:") {
                    data.extend_from_slice(trim_ascii(rest));
                    data.push(b'\n');
                }
            }
            trim_ascii(&data).to_vec()
        }
        let samples: &[&[u8]] = &[
            b"event: x\ndata: {\"a\":1}\ndata: {\"b\":2}",
            b"data: [DONE]",
            b"data:\n",
            b"event: ping\n",
            b"data:   {\"a\": 1}  \r",
            b"data: one\r\ndata: two",
            b"",
        ];
        for sample in samples {
            let unified = block_data(sample);
            assert_eq!(
                unified.as_deref(),
                legacy_convert(sample).as_deref(),
                "convert 语义漂移: {sample:?}"
            );
            // 旧 compaction 版没有「无 data 行」的概念（空负载继续走 JSON 解析），
            // 只在有 data 行时对比。
            if let Some(payload) = unified {
                assert_eq!(
                    payload,
                    legacy_compaction(sample),
                    "compaction 语义漂移: {sample:?}"
                );
            }
        }
    }

    #[test]
    fn split_blocks_keeps_tail_in_buffer() {
        let mut buffer = b"data: a\n\ndata: b".to_vec();
        let blocks = split_blocks(&mut buffer);
        assert_eq!(blocks, vec![b"data: a".to_vec()]);
        assert_eq!(buffer, b"data: b".to_vec());
    }

    #[test]
    fn line_data_requires_the_prefix() {
        assert_eq!(line_data(b"data: {\"a\":1}\r"), Some(&b"{\"a\":1}"[..]));
        assert_eq!(line_data(b"event: x"), None);
    }
}
