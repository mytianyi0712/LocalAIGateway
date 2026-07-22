import gzip
import json
import zlib

import brotli
import zstandard

from app.adapters import get_adapter
from app.adapters.base import StreamObserver


def test_base_url_joining_and_model_extraction():
    adapter = get_adapter("openai_compatible")
    body = b'{"model":"model-x","messages":[]}'
    assert (
        adapter.proxy_url("https://example.test", "/v1/chat/completions", "")
        == "https://example.test/v1/chat/completions"
    )
    assert (
        adapter.proxy_url("https://example.test/v1/", "/v1/chat/completions", "")
        == "https://example.test/v1/chat/completions"
    )
    assert adapter.discovery_url("https://example.test/v1") == "https://example.test/v1/models"
    assert adapter.extract_model(body, "/v1/chat/completions") == "model-x"
    assert body == b'{"model":"model-x","messages":[]}'


def test_protocol_usage_normalization():
    openai = get_adapter("openai_compatible").normalize_usage(
        {
            "prompt_tokens": 100,
            "completion_tokens": 12,
            "prompt_tokens_details": {"cached_tokens": 70},
        }
    )
    assert (openai.cache_read_tokens, openai.cache_miss_input_tokens, openai.output_tokens) == (
        70,
        30,
        12,
    )

    claude = get_adapter("claude").normalize_usage(
        {
            "input_tokens": 20,
            "cache_read_input_tokens": 60,
            "cache_creation_input_tokens": 10,
            "output_tokens": 8,
        }
    )
    assert (claude.input_tokens, claude.cache_read_tokens, claude.cache_write_tokens) == (
        90,
        60,
        10,
    )

    gemini = get_adapter("gemini").normalize_usage(
        {"promptTokenCount": 40, "cachedContentTokenCount": 15, "candidatesTokenCount": 7}
    )
    assert (gemini.cache_miss_input_tokens, gemini.output_tokens) == (25, 7)


def test_tps_uses_full_attempt_duration_instead_of_first_token_tail():
    observer = StreamObserver(started_at=100.0, streaming=True)
    observer.first_token_at = 109.999
    observer.finished_at = 110.0
    observer.usage.output_tokens = 500

    assert observer.duration_ms == 10000
    assert observer.tps == 50.0


def test_gemini_key_is_removed_from_forwarded_query():
    adapter = get_adapter("gemini")
    url = adapter.proxy_url(
        "https://example.test",
        "/v1beta/models/gemini-test:generateContent",
        "key=local-secret&alt=sse",
    )
    assert "local-secret" not in url
    assert url.endswith("?alt=sse")


def test_all_protocols_expose_model_discovery_contracts():
    openai_payload = b'{"data":[{"id":"model-a"},{"id":"model-b"}]}'
    for protocol in ("openai_compatible", "openai_responses", "claude"):
        adapter = get_adapter(protocol)
        assert adapter.discovery_url("https://example.test").endswith("/v1/models")
        assert [item["id"] for item in adapter.parse_models(openai_payload)] == [
            "model-a",
            "model-b",
        ]

    gemini = get_adapter("gemini")
    assert gemini.discovery_url("https://example.test").endswith("/v1beta/models")
    models = gemini.parse_models(
        b'{"models":[{"name":"models/gemini-a","displayName":"Gemini A"}]}'
    )
    assert models[0]["id"] == "gemini-a"


def test_claude_stream_usage_is_merged_across_events():
    adapter = get_adapter("claude")
    observer = StreamObserver(streaming=True)
    adapter.observe_chunk(
        observer,
        b'data: {"type":"message_start","message":{"usage":{"input_tokens":4,"cache_read_input_tokens":10,"cache_creation_input_tokens":2}}}\n\n',
    )
    adapter.observe_chunk(
        observer,
        b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"OK"}}\n\n',
    )
    adapter.observe_chunk(
        observer,
        b'data: {"type":"message_delta","usage":{"output_tokens":2}}\n\n',
    )
    usage = adapter.finish_observer(observer)
    assert observer.first_token_ms is not None
    assert usage.input_tokens == 16
    assert usage.cache_read_tokens == 10
    assert usage.cache_write_tokens == 2
    assert usage.cache_miss_input_tokens == 4
    assert usage.output_tokens == 2


def test_compressed_responses_usage_is_observed_for_common_content_encodings():
    adapter = get_adapter("openai_responses")
    payload = json.dumps(
        {
            "usage": {
                "input_tokens": 4389,
                "input_tokens_details": {
                    "cached_tokens": 3840,
                    "cache_write_tokens": 12,
                },
                "output_tokens": 5,
                "total_tokens": 4394,
            }
        }
    ).encode()
    encoded_payloads = {
        "gzip": gzip.compress(payload),
        "deflate": zlib.compress(payload),
        "br": brotli.compress(payload),
        "zstd": zstandard.ZstdCompressor().compress(payload),
    }

    for content_encoding, encoded in encoded_payloads.items():
        observer = StreamObserver()
        observer.set_content_encoding(content_encoding)
        for offset in range(0, len(encoded), 7):
            adapter.observe_chunk(observer, encoded[offset : offset + 7])
        usage = adapter.finish_observer(observer)

        assert usage.input_tokens == 4389
        assert usage.cache_read_tokens == 3840
        assert usage.cache_write_tokens == 12
        assert usage.cache_miss_input_tokens == 537
        assert usage.output_tokens == 5
        assert usage.raw == json.loads(payload)["usage"]


def test_compressed_responses_stream_tracks_first_token_and_final_usage():
    adapter = get_adapter("openai_responses")
    payload = b"".join(
        [
            b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n',
            b'data: {"type":"response.completed","response":{"usage":',
            b'{"input_tokens":10,"input_tokens_details":{"cached_tokens":4},',
            b'"output_tokens":2}}}\n\n',
        ]
    )
    encoded = brotli.compress(payload)
    observer = StreamObserver(streaming=True)
    observer.set_content_encoding("br")

    for offset in range(0, len(encoded), 5):
        adapter.observe_chunk(observer, encoded[offset : offset + 5])
    usage = adapter.finish_observer(observer)

    assert observer.first_token_ms is not None
    assert usage.input_tokens == 10
    assert usage.cache_read_tokens == 4
    assert usage.cache_miss_input_tokens == 6
    assert usage.output_tokens == 2


def test_missing_usage_remains_unknown():
    usage = get_adapter("openai_responses").normalize_usage(None)

    assert usage.input_tokens is None
    assert usage.output_tokens is None
    assert usage.raw is None
