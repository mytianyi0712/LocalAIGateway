//! 代理流水线的端到端单元测试（真实迁移 + 裸 TCP 假上游）。
//!
//! 覆盖拆分后的全部子模块内部项，因此按兄弟模块 glob 导入。

use std::{
    convert::Infallible,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    extract::State,
    http::{Response, StatusCode},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{Value, json};

use crate::{
    crypto::SecretStore,
    domain::Usage,
    protocol,
    state::AppState,
    telemetry::Telemetry,
};


use super::attempt::*;
use super::catalog::*;
use super::error::*;
use super::stream::*;

use crate::ports::FailoverNotice;
use crate::{config::AppConfig, db::Database};
use futures_util::stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;


    /// 记录每条 failover 通知的内存内 Notifier，供断言使用。
    #[derive(Clone, Default)]
    struct FakeNotifier(Arc<parking_lot::Mutex<Vec<FailoverNotice>>>);

    impl FakeNotifier {
        fn new() -> Self {
            Self::default()
        }
        fn notices(&self) -> Vec<FailoverNotice> {
            self.0.lock().clone()
        }
    }

    impl crate::ports::Notifier for FakeNotifier {
        fn notify_failover(&self, notice: FailoverNotice) {
            self.0.lock().push(notice);
        }
    }

    /// 无内存依赖的测试脚手架：临时目录、真实迁移、
    /// 一条指向我们控制的裸 TCP 上游的 openai_compatible 路由。
    struct TestGateway {
        db: Database,
        state: AppState,
        secrets: SecretStore,
        notifier: FakeNotifier,
        writer_cancel: CancellationToken,
        writer: tokio::task::JoinHandle<()>,
        _dir: crate::test_support::TempDir,
    }

    impl TestGateway {
        /// 停止 writer 任务：先释放遥测发送端
        /// （writer 只在通道关闭后才退出），再 join 它。
        async fn shutdown(self) {
            drop(self.state);
            self.writer_cancel.cancel();
            let _ = self.writer.await;
        }
    }

    async fn test_gateway(upstream_port: u16, extra_settings: &[(&str, &str)]) -> TestGateway {
        test_gateway_inner(upstream_port, extra_settings, false, false).await
    }

    /// 同一脚手架，另加一条 claude 协议路由（`claude-model`
    /// 走同一渠道模型），使目录聚合覆盖
    /// 不止 openai 一族。
    async fn test_gateway_claude(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
    ) -> TestGateway {
        test_gateway_inner(upstream_port, extra_settings, true, false).await
    }

    /// Command Code 脚手架：`command_code` 路由 + provider kind 标记；
    /// 请求经普通入口（`/v1/messages`、`/v1/chat/completions`）静默转换抵达。
    /// 该集成默认关闭，此处统一开启（调用方无需重复传入）。
    async fn test_gateway_command_code(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
    ) -> TestGateway {
        let mut settings: Vec<(&str, &str)> = vec![("command_code_enabled", "true")];
        settings.extend(
            extra_settings
                .iter()
                .copied()
                .filter(|(key, _)| *key != "command_code_enabled"),
        );
        test_gateway_inner(upstream_port, &settings, false, true).await
    }

    async fn test_gateway_inner(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
        claude_route: bool,
        command_code: bool,
    ) -> TestGateway {
        let dir = crate::test_support::TempDir::new("proxy");
        let db = Database::open(&dir.path().join("test.db")).await.unwrap();
        let secrets = crate::crypto::SecretStore::load(&dir.path().join("master.key"))
            .await
            .unwrap();
        let time = crate::test_support::SEED_TIME;
        crate::test_support::seed_provider_with_kind(
            &db,
            "prov-1",
            "mock",
            &format!("http://127.0.0.1:{upstream_port}"),
            command_code.then_some("command_code"),
        )
        .await;
        let primary_protocol = if command_code { "command_code" } else { "openai_compatible" };
        let primary_model = if command_code { "cc-model" } else { "test-model" };
        crate::test_support::seed_channel(
            &db,
            &secrets,
            "ch-1",
            "prov-1",
            primary_protocol,
            "test-key",
        )
        .await;
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-1','ch-1',?,'Test Model','discovered',1,?,?,?)")
            .bind(primary_model)
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1',?)")
            .bind(primary_protocol)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-1',?,?,1,?,?)")
            .bind(primary_protocol)
            .bind(primary_model)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-1','route-1','cm-1',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-1','active',0,?)")
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        if claude_route {
            sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','claude')")
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-2','claude','claude-model',1,?,?)")
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-2','cm-1',1,1,?,?)")
                .bind(time)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
        }
        for (key, raw) in extra_settings {
            sqlx::query("INSERT INTO settings(key,value_json,updated_at) VALUES(?,?,?)")
                .bind(key)
                .bind(raw)
                .bind(time)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let (telemetry, rx) = Telemetry::new(1000);
        let cancel = CancellationToken::new();
        let writer = tokio::spawn(Telemetry::run_writer(
            db.clone(),
            rx,
            cancel.clone(),
            telemetry.dropped_handle(),
        ));
        let http: Arc<dyn crate::ports::UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let routes: Arc<dyn crate::ports::RouteRepository> =
            crate::infrastructure::SqliteRouteRepository::new(db.clone());
        let channels: Arc<dyn crate::ports::ChannelRepository> =
            crate::infrastructure::SqliteChannelRepository::new(db.clone());
        let clock: Arc<dyn crate::ports::Clock> = Arc::new(crate::infrastructure::SystemClock);
        let background = crate::infrastructure::RuntimeSupervisor::new(
            tokio_util::sync::CancellationToken::new(),
        );
        let limits = Arc::new(crate::runtime::RuntimeLimits::default());
        let discovery = crate::discovery::DiscoveryService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&background),
            Arc::clone(&limits),
        );
        let recorder = FakeNotifier::new();
        let notifier: Arc<dyn crate::ports::Notifier> = Arc::new(recorder.clone());
        let settings_reader: Arc<dyn crate::ports::SettingsReader> =
            crate::infrastructure::SettingsStore::new(db.clone(), secrets.clone());
        let command_code_state: Arc<dyn crate::ports::CommandCodeState> =
            crate::infrastructure::CommandCodeStore::new(db.clone(), Arc::clone(&http));
        let proxy = crate::proxy::ProxyService::new(crate::proxy::ProxyServiceDeps {
            settings: Arc::clone(&settings_reader),
            command_code: Arc::clone(&command_code_state),
            secrets: secrets.clone(),
            http: Arc::clone(&http),
            routes: routes.clone(),
            telemetry: telemetry.clone(),
            clock: Arc::clone(&clock),
            limits: Arc::clone(&limits),
            notifier: notifier.clone(),
        });
        let balance = crate::balance::BalanceService::new(
            db.clone(),
            secrets.clone(),
            Arc::clone(&http),
            Arc::clone(&channels),
            Arc::clone(&clock),
            Arc::clone(&limits),
        );
        let command_code_login = crate::commandcode_login::CommandCodeLogin::new(std::sync::Arc::clone(&http));
        let admin =
            crate::admin::AdminService::new(db.clone(), secrets.clone(), Arc::clone(&channels));
        let ctx = crate::application::Context {
            config: Arc::new(AppConfig::default()),
            db: db.clone(),
            secrets: secrets.clone(),
            http,
            routes,
            channels,
            clock,
            notifier,
            telemetry,
            background,
            limits,
        };
        let state = crate::state::AppState {
            ctx,
            proxy,
            admin,
            balance,
            discovery,
            command_code_login,
            recovery: crate::auth::RecoverySession::new(),
        };
        TestGateway {
            db,
            state,
            secrets,
            notifier: recorder,
            writer_cancel: cancel,
            writer,
            _dir: dir,
        }
    }

    /// Codex 压缩测试骨架：在默认单候选之上加一条 `openai_responses` 路由
    /// （`test-model`），请求直接走常规入口 `/v1/responses`。
    async fn test_gateway_codex_compaction(
        upstream_port: u16,
        extra_settings: &[(&str, &str)],
    ) -> TestGateway {
        let gateway = test_gateway_inner(upstream_port, extra_settings, false, false).await;
        let time = "2026-08-04T01:00:00+00:00";
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1','openai_responses')")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES('route-codex','openai_responses','test-model',1,?,?)")
            .bind(time)
            .bind(time)
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-codex','route-codex','cm-1',1,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(gateway.db.pool())
            .await
            .unwrap();
        gateway
    }

    /// V2 远程压缩请求：`input` 以 `compaction_trigger` 结尾，走常规 `/v1/responses` 入口。
    fn codex_compaction_request() -> axum::extract::Request {
        let body = r#"{"model":"test-model","stream":true,"input":[{"type":"message","role":"user","content":[]},{"type":"compaction_trigger"}]}"#;
        axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    /// Command Code 渠道上的 claude 入口请求（`/v1/messages`）：
    /// 模型只有 `command_code` 路由，网关静默转换。
    fn cc_claude_chat_request(stream: bool) -> axum::extract::Request {
        cc_claude_request(&format!(
            r#"{{"model":"cc-model","max_tokens":10,"stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
        ))
    }

    /// 对每个被接受的连接都返回 `response` 的上游，
    /// 使备用渠道在连续请求间保持可达。
    async fn spawn_upstream_reusable(response: Vec<u8>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let response = Arc::new(response);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let response = Arc::clone(&response);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let mut total = 0usize;
                    loop {
                        match stream.read(&mut buf[total..]).await {
                            Ok(0) => break,
                            Ok(n) => {
                                total += n;
                                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        port
    }

    /// 裸 HTTP/1.1 上游：读取请求头，写入 `response`，
    /// 然后要么关闭（默认）要么保持连接打开。
    async fn spawn_upstream(response: Vec<u8>, hang: bool) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let _ = stream.write_all(&response).await;
                if hang {
                    let mut sink = [0u8; 1024];
                    let _ = stream.read(&mut sink).await;
                }
            }
        });
        port
    }

    /// 裸 HTTP/1.1 上游，按顺序写入 `parts`（写入之间间隔 `gap`）
    /// 后关闭，使 reqwest 观察到多个正文分块。
    async fn spawn_upstream_parts(parts: Vec<Vec<u8>>, gap: Duration) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                for part in parts {
                    let _ = stream.write_all(&part).await;
                    if !gap.is_zero() {
                        tokio::time::sleep(gap).await;
                    }
                }
            }
        });
        port
    }

    /// 裸 HTTP/1.1 上游，只在 `delay` 过去后（读取请求头之后）
    /// 才作答，然后写入 `response` 并关闭。
    async fn spawn_upstream_delayed(response: Vec<u8>, delay: Duration) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                tokio::time::sleep(delay).await;
                let _ = stream.write_all(&response).await;
            }
        });
        port
    }

    fn sse_event(payload: &str) -> String {
        format!("data: {payload}\n\n")
    }

    fn stream_response(body: &str, content_length: Option<usize>) -> String {
        let mut head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n".to_owned();
        match content_length {
            Some(len) => head.push_str(&format!("content-length: {len}\r\n")),
            None => head.push_str("transfer-encoding: chunked\r\n"),
        }
        head.push_str("\r\n");
        head + body
    }

    fn chat_request(stream: bool) -> axum::extract::Request {
        let body = format!(
            r#"{{"model":"test-model","stream":{stream},"messages":[{{"role":"user","content":"hi"}}]}}"#
        );
        axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    async fn latest_log(db: &Database) -> Option<(String, String, i64, Option<i64>)> {
        sqlx::query_as("SELECT id,outcome,attempt_count,final_status_code FROM request_logs ORDER BY started_at DESC LIMIT 1")
            .fetch_optional(db.pool())
            .await
            .unwrap()
    }

    async fn wait_for_outcome(
        db: &Database,
        expected: &str,
        timeout: Duration,
    ) -> (String, String, i64, Option<i64>) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some((id, outcome, attempts, status)) = latest_log(db).await
                && outcome == expected
            {
                return (id, outcome, attempts, status);
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for outcome {expected:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// 鉴权失败是可计数的熔断失败；普通 4xx 不是。
    /// 透明转发流的终态判定与流式转换路径共用 `convert::stream_completed`：
    /// 两处若各有一套判定集合，客户端断开时的 `cancel_completed_flag` 会与
    /// 上游真实结果漂移（把已完成的流记成 cancelled）。
    #[test]
    fn terminal_detection_follows_stream_completed() {
        let stats = SharedStreamStats::default();
        let now = chrono::Utc::now();
        let cases = [
            ("openai_responses", r#"{"type":"response.completed"}"#),
            ("openai_responses", r#"{"response":{"status":"completed"}}"#),
            ("gemini", r#"{"candidates":[{"finishReason":"STOP"}]}"#),
            ("gemini", r#"{"candidates":[{"finish_reason":"STOP"}]}"#),
            ("openai_compatible", r#"{"choices":[{"finish_reason":"stop"}]}"#),
            ("claude", r#"{"type":"message_stop"}"#),
        ];
        for (protocol, event) in cases {
            let mut pending = format!("data: {event}\n").into_bytes();
            let mut usage = Usage::default();
            let mut first_token_ms = None;
            let mut first_token_seen = false;
            assert!(
                scan_observable_lines(
                    &mut pending,
                    true,
                    protocol,
                    &mut usage,
                    &mut first_token_ms,
                    &mut first_token_seen,
                    &stats,
                    now,
                    now,
                ),
                "{protocol} 的终态事件未被识别: {event}"
            );
            assert!(pending.is_empty(), "被消费的行必须从 pending 中移除");
        }
        // 非终态事件不得被误判。
        let mut pending = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n".to_vec();
        let mut usage = Usage::default();
        let mut first_token_ms = None;
        let mut first_token_seen = false;
        assert!(!scan_observable_lines(
            &mut pending,
            true,
            "openai_compatible",
            &mut usage,
            &mut first_token_ms,
            &mut first_token_seen,
            &stats,
            now,
            now,
        ));
    }

    #[test]
    fn status_kind_classifies_auth_errors_countable() {
        assert_eq!(status_kind(StatusCode::UNAUTHORIZED), ("auth_error", true));
        assert_eq!(status_kind(StatusCode::FORBIDDEN), ("auth_error", true));
        assert_eq!(
            status_kind(StatusCode::BAD_REQUEST),
            ("upstream_4xx", false)
        );
        assert_eq!(
            status_kind(StatusCode::TOO_MANY_REQUESTS),
            ("rate_limit", true)
        );
        assert_eq!(status_kind(StatusCode::REQUEST_TIMEOUT), ("timeout", true));
        assert_eq!(status_kind(StatusCode::GATEWAY_TIMEOUT), ("timeout", true));
        assert_eq!(
            status_kind(StatusCode::INTERNAL_SERVER_ERROR),
            ("upstream_5xx", true)
        );
    }

    #[tokio::test]
    async fn cancel_aware_terminates_exactly_once() {
        use std::sync::atomic::AtomicUsize;

        let calls = Arc::new(AtomicUsize::new(0));

        // （i）流被轮询到 None 后再丢弃：不触发取消回调。
        let completed = Arc::new(AtomicBool::new(false));
        let finalized = Arc::new(AtomicBool::new(false));
        let responded = Arc::new(AtomicBool::new(false));
        let calls_i = calls.clone();
        let cancel_aware = CancelAware {
            inner: stream::iter(vec![
                Ok::<Bytes, Infallible>(Bytes::from("a")),
                Ok::<Bytes, Infallible>(Bytes::from("b")),
            ]),
            completed: completed.clone(),
            finalized: finalized.clone(),
            responded,
            on_cancel: Some(Box::new(move || {
                calls_i.fetch_add(1, Ordering::SeqCst);
            })),
        };
        let mut cancel_aware = Box::pin(cancel_aware);
        while cancel_aware.next().await.is_some() {}
        drop(cancel_aware);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(completed.load(Ordering::SeqCst));

        // （ii）未完成即丢弃：取消回调恰好触发一次。
        let completed = Arc::new(AtomicBool::new(false));
        let finalized = Arc::new(AtomicBool::new(false));
        let responded = Arc::new(AtomicBool::new(false));
        let calls_ii = calls.clone();
        let cancel_aware = CancelAware {
            inner: stream::iter(vec![
                Ok::<Bytes, Infallible>(Bytes::from("a")),
                Ok::<Bytes, Infallible>(Bytes::from("b")),
            ]),
            completed: completed.clone(),
            finalized: finalized.clone(),
            responded,
            on_cancel: Some(Box::new(move || {
                calls_ii.fetch_add(1, Ordering::SeqCst);
            })),
        };
        drop(cancel_aware);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // （iii）丢弃前已 finalized：不触发取消回调。
        let completed = Arc::new(AtomicBool::new(false));
        let finalized = Arc::new(AtomicBool::new(true));
        let responded = Arc::new(AtomicBool::new(false));
        let calls_iii = calls.clone();
        let cancel_aware = CancelAware {
            inner: stream::iter(vec![
                Ok::<Bytes, Infallible>(Bytes::from("a")),
                Ok::<Bytes, Infallible>(Bytes::from("b")),
            ]),
            completed: completed.clone(),
            finalized: finalized.clone(),
            responded,
            on_cancel: Some(Box::new(move || {
                calls_iii.fetch_add(1, Ordering::SeqCst);
            })),
        };
        drop(cancel_aware);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// 上游在流中途关闭。该请求必须恰好终结一次为 `stream_interrupted`——
    /// 绝不被生成器 Drop 路径追加的一次伪 `cancelled` 覆盖。
    /// （即：终态只能记录一次。）
    #[tokio::test]
    async fn upstream_error_mid_stream_records_single_interrupted_finish() {
        let body =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            !bytes.is_empty(),
            "partial upstream data must reach the client"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(5)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let attempt_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM request_attempts WHERE request_id=(SELECT id FROM request_logs ORDER BY started_at DESC LIMIT 1)")
                .fetch_one(gateway.db.pool())
                .await
                .unwrap();
        assert_eq!(attempt_count, 1);
        let attempt_outcome: String = sqlx::query_scalar(
            "SELECT outcome FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(attempt_outcome, "stream_interrupted");
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 1, "mid-stream errors count toward the circuit");
        gateway.shutdown().await;
    }

    /// 客户端在流中途断开。恰好记录一次 `cancelled` 终结，
    /// 且该取消不计入熔断。
    #[tokio::test]
    async fn client_disconnect_records_single_cancelled_finish() {
        let body =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(5), frames.next())
            .await
            .expect("first frame must arrive")
            .expect("stream must not end");
        assert!(!first.unwrap().is_empty());
        drop(frames);
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "cancelled", Duration::from_secs(5)).await;
        assert_eq!(outcome, "cancelled");
        assert_eq!(attempts, 1);
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 0, "client cancellation must not open the circuit");
        gateway.shutdown().await;
    }

    /// 客户端在流中途断开时必须记录网关实际观测到的内容——
    /// 已经流转的字节与已经解析的 usage——
    /// 而不是一条零值 `cancelled` 记录（旧实现把 Drop 路径硬编码为 0/空）。
    #[tokio::test]
    async fn cancelled_stream_records_observed_bytes_and_usage() {
        let body = sse_event(
            r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}],"usage":{"prompt_tokens":42,"completion_tokens":7,"total_tokens":49}}"#,
        );
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        // 读取到上游停滞（所有缓冲分块处理完），
        // 然后像中途放弃的客户端一样挂断。
        while let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(300), frames.next()).await
        {
            assert!(!chunk.is_empty());
        }
        drop(frames);
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "cancelled", Duration::from_secs(5)).await;
        assert_eq!(outcome, "cancelled");
        assert_eq!(attempts, 1);
        let (input, output, response_bytes, first_token_ms): (
            Option<i64>,
            Option<i64>,
            i64,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, output_tokens, response_bytes, first_token_ms \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            input, Some(42),
            "usage seen before the disconnect must be recorded"
        );
        assert_eq!(output, Some(7));
        assert!(
            response_bytes > 0,
            "bytes observed before the disconnect must be recorded"
        );
        assert!(
            first_token_ms.is_some(),
            "the observed first token must be recorded"
        );
        gateway.shutdown().await;
    }

    /// 转换路径上的同一保证：客户端在转换器消费完内容 + usage 后挂断，
    /// 留下的 cancelled 记录携带真实的字节与 usage，
    /// 而不是零值。
    #[tokio::test]
    async fn mapped_cancelled_stream_records_observed_bytes_and_usage() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{},"finish_reason":null}],"usage":{"prompt_tokens":42,"completion_tokens":7,"total_tokens":49}}"#),
        );
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway_command_code(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(true), "claude", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        while let Ok(Some(Ok(chunk))) =
            tokio::time::timeout(Duration::from_millis(300), frames.next()).await
        {
            assert!(!chunk.is_empty());
        }
        drop(frames);
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "cancelled", Duration::from_secs(5)).await;
        assert_eq!(outcome, "cancelled");
        assert_eq!(attempts, 1);
        let (input, output, response_bytes, first_token_ms): (
            Option<i64>,
            Option<i64>,
            i64,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, output_tokens, response_bytes, first_token_ms \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            input, Some(42),
            "converter usage must survive the disconnect"
        );
        assert_eq!(output, Some(7));
        assert!(
            response_bytes > 0,
            "upstream bytes must survive the disconnect"
        );
        assert!(
            first_token_ms.is_some(),
            "the observed first token must survive the disconnect"
        );
        gateway.shutdown().await;
    }

    /// 回归护栏：干净的流只记录一次成功。
    #[tokio::test]
    async fn normal_stream_completes_with_single_success() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}"#),
            sse_event(
                r#"{"id":"2","choices":[{"delta":{"content":"lo"},"finish_reason":null}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}}"#
            )
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 0);
        gateway.shutdown().await;
    }

    /// 映射流客户端（如 Codex CLI）在收到终态事件
    /// （`response.completed`/`message_stop`）后立刻挂断时，
    /// 必须留下一条成功记录，并带上其 usage 与字节——
    /// outcome 在终态事件交付之前就已记录，
    /// 因此 Drop 路径无法把它降级为零值 `cancelled`。
    #[tokio::test]
    async fn mapped_client_close_after_terminal_event_records_success() {
        let body = format!(
            "{}{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}"#),
            "data: [DONE]\n\n",
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway_command_code(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(true), "claude", None)
            .await;
        let mut frames = response.into_body().into_data_stream();
        // 读取帧直到终态 claude 事件到达，然后像认为本轮结束的 CLI
        // 一样丢弃正文。
        let mut saw_terminal = false;
        for _ in 0..64 {
            let frame = tokio::time::timeout(Duration::from_secs(5), frames.next())
                .await
                .expect("frame must arrive")
                .expect("stream must not end yet");
            let bytes = frame.unwrap();
            if String::from_utf8_lossy(&bytes).contains("message_stop") {
                saw_terminal = true;
                break;
            }
        }
        assert!(saw_terminal, "the client must observe the terminal event");
        drop(frames);
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success", "the completed stream must stay a success");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let (input, output, response_bytes): (Option<i64>, Option<i64>, i64) = sqlx::query_as(
            "SELECT input_tokens, output_tokens, response_bytes FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(10), "usage must be recorded, not zeroed");
        assert_eq!(output, Some(5));
        assert!(
            response_bytes > 0,
            "response bytes must be recorded, not zeroed"
        );
        gateway.shutdown().await;
    }

    /// 流式 usage 会跨多个 chunk 上报；后面某个省略缓存细节的 chunk
    /// 不得抹掉前一个 chunk 携带的缓存字段
    /// （逐字段合并，最新的非 None 值胜出）。
    #[tokio::test]
    async fn passthrough_usage_merges_fields_across_chunks() {
        let body = format!(
            "{}{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}],"usage":{"prompt_tokens":100,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":60},"total_tokens":100}}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}"#),
            "data: [DONE]\n\n",
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        let (input, cache_read, cache_miss, output): (
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
        ) = sqlx::query_as(
            "SELECT input_tokens, cache_read_tokens, cache_miss_input_tokens, output_tokens \
             FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(100), "input from the first chunk must survive");
        assert_eq!(
            cache_read, Some(60),
            "cache_read from the first chunk must survive the second chunk"
        );
        assert_eq!(cache_miss, Some(40), "cache_miss derives from merged values");
        assert_eq!(output, Some(50), "output from the last chunk wins");
        gateway.shutdown().await;
    }

    /// 最后一行 SSE 可能缺少结尾换行；
    /// 上游结束时其中的 usage 仍必须被捕获。
    #[tokio::test]
    async fn passthrough_tail_line_without_newline_captures_usage() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            r#"data: {"id":"2","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}"#,
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        let (input, output): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT input_tokens, output_tokens FROM request_attempts LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(input, Some(7), "usage in the unterminated tail line");
        assert_eq!(output, Some(3));
        gateway.shutdown().await;
    }

    /// 首 token 保护：一个干净关闭、却从未产出首 token 的 200 流
    /// （空正文、无终态标记）是保护违规，不是成功——
    /// 它必须计入熔断，
    /// 而不是悄悄“成功”。
    #[tokio::test]
    async fn plain_stream_empty_close_without_token_fails_circuit() {
        let response = stream_response("", Some(0));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[("failure_threshold", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(bytes.is_empty(), "the client receives the empty stream");
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(5)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            error_kind.as_deref(),
            Some("no_first_token"),
            "the missing first token must be named"
        );
        let (failures, state): (i64, String) = sqlx::query_as(
            "SELECT consecutive_failures, state FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 1, "no-first-token must trip the circuit");
        assert_eq!(state, "open");
        gateway.shutdown().await;
    }

    /// 携带显式终态标记但没有任何内容的流（只有 `data: [DONE]`）
    /// 是合法的空完成——上游明确结束了，
    /// 因此不得触发熔断。
    #[tokio::test]
    async fn plain_stream_done_only_is_success() {
        let body = "data: [DONE]\n\n";
        let response = stream_response(body, Some(body.len()));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway(port, &[("failure_threshold", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        assert_eq!(outcome, "success");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let failures: i64 = sqlx::query_scalar(
            "SELECT consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 0, "an explicit [DONE] is a normal completion");
        gateway.shutdown().await;
    }

    /// 转换路径上的首 token 保护：转换路径（entry != 上游协议）
    /// 的上游若未产出任何 token 就关闭，
    /// 同样必须使熔断失败。
    #[tokio::test]
    async fn mapped_stream_empty_close_fails_circuit() {
        let response = stream_response("", Some(0));
        let port = spawn_upstream(response.into_bytes(), false).await;
        let gateway = test_gateway_command_code(port, &[("failure_threshold", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(true), "claude", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            !String::from_utf8_lossy(&bytes).contains("content_block_delta"),
            "an empty upstream must never be turned into answer content"
        );
        let (_, outcome, attempts, _) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(5)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("no_first_token"));
        let (failures, state): (i64, String) = sqlx::query_as(
            "SELECT consecutive_failures, state FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(failures, 1);
        assert_eq!(state, "open");
        gateway.shutdown().await;
    }

    /// 首 token 之后停滞的流由生成器终结为 504 transport_timeout，
    /// 而不是永远挂起。
    #[tokio::test]
    async fn plain_stream_idle_timeout_finalizes_as_504() {
        let body =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#);
        let response = stream_response(&body, Some(1000));
        let port = spawn_upstream(response.into_bytes(), true).await;
        let gateway = test_gateway(port, &[("stream_idle_timeout_seconds", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(!bytes.is_empty());
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(10)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504), "idle timeout must finalize as 504");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("transport_timeout"));
        gateway.shutdown().await;
    }

    /// 过大的请求正文被以稳定的 413 载荷拒绝
    /// （不含内部缓冲错误文本），而不是被缓冲。
    #[tokio::test]
    async fn oversized_body_rejected_with_stable_413() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway(port, &[("max_request_body_mb", "1")]).await;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from("x".repeat(2 * 1024 * 1024)))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.pointer("/error/message").and_then(Value::as_str),
            Some("Request body exceeds the configured limit.")
        );
        gateway.shutdown().await;
    }

    /// gzip 编码的 SSE 响应在转发前被解码：客户端收到的是
    /// 不含 `content-encoding` 头的明文，同时 usage 可观测性
    /// 在同一份解码流上运行。原始压缩转发已被废弃，
    /// 因为被截断的上游流会表现为一个损坏的压缩体
    /// （客户端解压失败，
    /// 如 omp 的 `ZlibError`）。
    #[tokio::test]
    async fn gzip_stream_is_decoded_and_forwarded_with_usage_observed() {
        use std::io::Write;
        let plain = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}"#),
            sse_event(
                r#"{"id":"2","choices":[{"delta":{"content":"lo"},"finish_reason":null}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}"#
            )
        );
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut response_bytes =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: gzip\r\n"
                .as_bytes()
                .to_vec();
        response_bytes
            .extend_from_slice(format!("content-length: {}\r\n\r\n", compressed.len()).as_bytes());
        response_bytes.extend_from_slice(&compressed);
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        assert_eq!(
            response
                .headers()
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
            None,
            "the encoding header must not leak into the decoded stream"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            plain.as_bytes(),
            "the client must receive the decoded plaintext"
        );
        wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        let raw_usage: Option<String> = sqlx::query_scalar(
            "SELECT raw_usage_json FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert!(
            raw_usage.is_some(),
            "usage must be extracted from the decoded stream"
        );
        gateway.shutdown().await;
    }

    /// 被截断的 gzip 流会解码到截断点，
    /// 并以不含 `content-encoding` 头的干净明文转发；
    /// 该次尝试记录为 `stream_interrupted`，使渠道判定
    /// 与客户端观测一致（正文结束时未携带
    /// 终态标记）。修复前被截断的压缩字节会原样转发
    /// 并让客户端解压崩溃（ZlibError），
    /// 而尝试却被记为成功。
    #[tokio::test]
    async fn truncated_gzip_stream_ends_cleanly_downstream_and_counts_as_interrupted() {
        use std::io::Write;
        let plain = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{"content":"lo"},"finish_reason":null}]}"#)
        );
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        // 切掉尾部（以及最后一块的一部分），
        // 让压缩流无法到达其结束标记。
        let truncated = &compressed[..compressed.len() - 12];
        let mut response_bytes =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: gzip\r\n"
                .as_bytes()
                .to_vec();
        response_bytes
            .extend_from_slice(format!("content-length: {}\r\n\r\n", truncated.len()).as_bytes());
        response_bytes.extend_from_slice(truncated);
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        assert_eq!(
            response
                .headers()
                .get("content-encoding")
                .and_then(|v| v.to_str().ok()),
            None,
            "no content-encoding header on a decoded relay"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            plain.as_bytes().starts_with(&bytes),
            "the client receives a clean prefix of the plaintext, never compressed bytes"
        );
        let (_, outcome, _, _) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(10)).await;
        assert_eq!(outcome, "stream_interrupted");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("stream_interrupted"));
        gateway.shutdown().await;
    }

    /// 首 token 窗口覆盖整次尝试。上游在请求开始后 2s 才返回
    /// 响应头 + 内容，而首 token 预算为 1s，
    /// 必须终结为 504（截止时间在响应头到达前就已过期），
    /// 而不是因为内容紧接着响应头到达就判成功。
    /// （即：绝对窗口不能被响应头“刷新”。）
    #[tokio::test]
    async fn first_token_deadline_counts_from_attempt_start_plain() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{"content":"!"},"finish_reason":null}]}"#),
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream_delayed(response.into_bytes(), Duration::from_secs(2)).await;
        let gateway = test_gateway(
            port,
            &[
                ("first_byte_timeout_seconds", "3"),
                ("first_token_timeout_seconds", "1"),
            ],
        )
        .await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(true), "openai_compatible", None)
            .await;
        // 把流驱动到结束：截止时间过期后，
        // 生成器运行其终态遥测（504, transport_timeout）。
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let _ = bytes;
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "stream_interrupted", Duration::from_secs(10)).await;
        assert_eq!(outcome, "stream_interrupted");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504), "deadline runs from attempt start");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("transport_timeout"));
        gateway.shutdown().await;
    }

    /// 转换路径同样如此：prelude 窗口也锚定在尝试开始。
    /// 200 + 内容在 2s 后才到，1s 预算早已过期，
    /// 因此单候选 failover 进 504 尾部，而不是成功。
    #[tokio::test]
    async fn first_token_deadline_counts_from_attempt_start_mapped() {
        let body = format!(
            "{}{}",
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"hi"},"finish_reason":null}]}"#),
            sse_event(r#"{"id":"2","choices":[{"delta":{"content":"!"},"finish_reason":null}]}"#),
        );
        let response = stream_response(&body, Some(body.len()));
        let port = spawn_upstream_delayed(response.into_bytes(), Duration::from_secs(2)).await;
        let gateway = test_gateway_command_code(
            port,
            &[
                ("first_byte_timeout_seconds", "3"),
                ("first_token_timeout_seconds", "1"),
            ],
        )
        .await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(true), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::GATEWAY_TIMEOUT,
            "expired prelude deadline + single candidate -> 504"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(10)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504));
        gateway.shutdown().await;
    }

    /// 转换流的 200 响应头已到达但从未产出首 token 属于超时，
    /// 因此单候选运行以 504 结束
    /// （修复前：prelude 超时未被分类，
    /// 尾部错误地产生 502）。
    #[tokio::test]
    async fn mapped_prelude_timeout_classifies_as_504() {
        let headers_only =
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 1000\r\n\r\n"
                .as_bytes()
                .to_vec();
        let port = spawn_upstream(headers_only, true).await;
        let gateway =
            test_gateway_command_code(port, &[("first_token_timeout_seconds", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(true), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::GATEWAY_TIMEOUT,
            "prelude timeout must classify as 504, not 502"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.pointer("/error/type").and_then(Value::as_str),
            Some("upstream_timeout")
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(10)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(504));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("timeout"));
        gateway.shutdown().await;
    }

    /// 非流式 200 的正文在读取中途死亡（承诺了 content-length，
    /// 连接却提前关闭）时会记录自己的尝试事件，
    /// 并终结为 502——修复前它被并进超时分支
    /// （504）且没有尝试记录。
    #[tokio::test]
    async fn non_stream_connection_reset_returns_502_with_attempt() {
        let mut response_bytes =
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 1000\r\n\r\n"
                .as_bytes()
                .to_vec();
        response_bytes.extend_from_slice(&[b'x'; 40]);
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_GATEWAY,
            "connection reset mid-body is 502, not 504"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM request_attempts WHERE request_id=(SELECT id FROM request_logs ORDER BY started_at DESC LIMIT 1)",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1, "the reset must record its own attempt event");
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("transport_error"));
        gateway.shutdown().await;
    }

    /// 映射流 prelude 只解码一次。压缩的上游正文
    /// （全部四种编码）必须以转换后的明文 SSE 到达客户端，
    /// 并携带两段内容文本；用 prelude 字节重新喂解码器
    /// 会破坏其状态并产生空流。
    #[tokio::test]
    async fn mapped_compressed_stream_decodes_prelude_once() {
        use std::io::Write;
        let plain = format!(
            "{}{}{}",
            sse_event(
                r#"{"id":"1","choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}"#
            ),
            sse_event(
                r#"{"id":"2","choices":[{"delta":{"content":"World"},"finish_reason":null}]}"#
            ),
            "data: [DONE]\n\n",
        );
        let gzip = {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(plain.as_bytes()).unwrap();
            encoder.finish().unwrap()
        };
        let deflate = {
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(plain.as_bytes()).unwrap();
            encoder.finish().unwrap()
        };
        let brotli = {
            let mut encoder = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
            encoder.write_all(plain.as_bytes()).unwrap();
            encoder.flush().unwrap();
            encoder.into_inner()
        };
        let zstd = zstd::stream::encode_all(plain.as_bytes(), 3).unwrap();
        for (encoding, compressed) in [
            ("gzip", gzip),
            ("deflate", deflate),
            ("br", brotli),
            ("zstd", zstd),
        ] {
            let mut response_bytes = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-encoding: {encoding}\r\ncontent-length: {}\r\n\r\n",
                compressed.len()
            )
            .into_bytes();
            response_bytes.extend_from_slice(&compressed);
            let port = spawn_upstream(response_bytes, false).await;
            let gateway = test_gateway_command_code(port, &[]).await;
            let response = gateway
                .state
                .proxy
                .proxy(cc_claude_chat_request(true), "claude", None)
                .await;
            assert_eq!(
                response.headers().get("content-encoding"),
                None,
                "{encoding}: the raw encoding header must not leak into the converted stream"
            );
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(
                text.contains("Hello") && text.contains("World"),
                "{encoding}: converted stream must carry both content texts, got {text:?}"
            );
            let (_, outcome, attempts, status) =
                wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
            assert_eq!(outcome, "success", "{encoding}");
            assert_eq!(attempts, 1, "{encoding}");
            assert_eq!(status, Some(200), "{encoding}");
            let first_token_ms: Option<i64> = sqlx::query_scalar(
                "SELECT first_token_ms FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
            )
            .fetch_one(gateway.db.pool())
            .await
            .unwrap();
            assert!(
                first_token_ms.is_some(),
                "{encoding}: first_token_ms must be recorded"
            );
            gateway.shutdown().await;
        }
    }

    /// 无法转换的映射非流式响应（上游正文对上游协议不是合法 JSON）
    /// 必须以固定网关错误消息到达客户端——
    /// 绝不是上游的原始文本
    /// 或内部转换错误。
    #[tokio::test]
    async fn mapped_conversion_failure_does_not_leak_internal_text() {
        let upstream_body = "not json at all, definitely not a chat completion";
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{upstream_body}",
            upstream_body.len()
        )
        .into_bytes();
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway_command_code(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(false), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "conversion failure keeps the 200 protocol-compat status"
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("Response conversion failed"),
            "client must see the fixed message, got: {text:?}"
        );
        assert!(
            !text.contains(upstream_body),
            "upstream body text must not leak into the converted response"
        );
        // 转换失败是网关错误——绝不是成功——
        // 且 request/attempt/channel 事件必须一致。
        let (_, _outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200), "protocol-compat 200 is preserved");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM request_attempts WHERE request_id=(SELECT id FROM request_logs ORDER BY started_at DESC LIMIT 1)",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(count, 1, "the conversion attempt must be recorded");
        let (attempt_outcome, error_kind): (String, Option<String>) = sqlx::query_as(
            "SELECT outcome, error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(attempt_outcome, "gateway_error");
        assert_eq!(error_kind.as_deref(), Some("conversion_error"));
        // 未发出 ChannelSuccess：渠道判定必须保持
        // 不动（既无成功，也无计数失败）。
        let (state, failures): (String, i64) = sqlx::query_as(
            "SELECT state, consecutive_failures FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(state, "active");
        assert_eq!(failures, 0);
        gateway.shutdown().await;
    }

    /// 声明 Content-Length 超过缓冲正文上限的映射非流式响应
    /// 会被直接拒绝——不读取任何内容。
    #[tokio::test]
    async fn mapped_non_stream_declared_oversized_body_returns_502() {
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            2 * 1024 * 1024
        )
        .into_bytes();
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway_command_code(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("upstream_response_too_large"));
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_response_too_large"));
        gateway.shutdown().await;
    }

    /// 没有 Content-Length、却在读取中途超过上限的映射非流式正文
    /// （原始累积）会以 502 截断——
    /// 缓冲区从不超出上限。
    #[tokio::test]
    async fn mapped_non_stream_unbounded_raw_body_hits_cap() {
        let mut response_bytes = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n"
            .as_bytes()
            .to_vec();
        response_bytes.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024));
        let port = spawn_upstream(response_bytes, false).await;
        let gateway = test_gateway_command_code(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_response_too_large"));
        gateway.shutdown().await;
    }

    /// 明文超过上限的压缩映射正文
    /// 会触发 RequiredDecoder 的累计限制 → 稳定的 502，
    /// 而不是一次截断的转换。
    #[tokio::test]
    async fn mapped_non_stream_plaintext_over_cap_returns_502() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&vec![b'x'; 2 * 1024 * 1024]).unwrap();
        let compressed = encoder.finish().unwrap();
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        let mut body = response_bytes;
        body.extend_from_slice(&compressed);
        let port = spawn_upstream(body, false).await;
        let gateway = test_gateway_command_code(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_decode_error"));
        gateway.shutdown().await;
    }

    /// 在帧中途被截断的映射非流式 gzip 正文会部分解码
    /// 但永远不会 finished → 502，绝不产生部分转换。
    #[tokio::test]
    async fn mapped_non_stream_truncated_gzip_returns_502() {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(br#"{"id":"1","choices":[{"finish_reason":"stop"}]}"#)
            .unwrap();
        let mut compressed = encoder.finish().unwrap();
        compressed.truncate(compressed.len() / 2);
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        let mut body = response_bytes;
        body.extend_from_slice(&compressed);
        let port = spawn_upstream(body, false).await;
        let gateway = test_gateway_command_code(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(false), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let (_, _, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(502));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_decode_error"));
        gateway.shutdown().await;
    }

    /// 映射压缩请求下，上游 200 的 SSE 不含 compaction → 最终响应必须说出
    /// 真实的网关侧原因（`upstream_compaction_format_error`）；修复前会退化成
    /// 笼统的 `upstream_unreachable`。
    #[tokio::test]
    async fn compaction_without_compaction_item_reports_format_error() {
        let upstream = concat!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        );
        let port = spawn_upstream(upstream.as_bytes().to_vec(), false).await;
        let gateway = test_gateway_codex_compaction(port, &[]).await;
        let response = gateway
            .state
            .proxy
            .proxy(codex_compaction_request(), "openai_responses", None)
            .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("upstream_compaction_format_error"),
            "the gateway-side cause must survive to the client: {text}"
        );
        gateway.shutdown().await;
    }

    /// V2 上游发完 `response.completed` 后保持连接不关闭，网关必须在校验器
    /// 确认完整时立即返回；修复前会等连接 EOF，直到
    /// `non_stream_total_timeout_seconds` 超时（504）。
    #[tokio::test]
    async fn compaction_v2_returns_without_waiting_for_connection_close() {
        let upstream = concat!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":5}}}\n\n",
        );
        // hang=true：事件发完后连接保持打开，只有早停能让请求返回。
        let port = spawn_upstream(upstream.as_bytes().to_vec(), true).await;
        let gateway =
            test_gateway_codex_compaction(port, &[("non_stream_total_timeout_seconds", "5")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(codex_compaction_request(), "openai_responses", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a complete compaction stream must not wait for connection close"
        );
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("response.completed"),
            "the compaction stream must reach the client"
        );
        gateway.shutdown().await;
    }

    /// 压缩路径的非 2xx 错误体按 `error_body_max`（1 MiB）截断，而不是按更大的
    /// 缓冲上限；修复前 2 MiB 的错误体会被完整回放。
    #[tokio::test]
    async fn compaction_error_body_is_capped_at_error_body_max() {
        let filler = "x".repeat(2 * 1024 * 1024);
        let upstream = format!(
            "HTTP/1.1 429 Too Many Requests\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n{filler}",
            filler.len()
        );
        let port = spawn_upstream(upstream.into_bytes(), false).await;
        let gateway =
            test_gateway_codex_compaction(port, &[("max_buffered_upstream_body_mb", "8")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(codex_compaction_request(), "openai_responses", None)
            .await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = to_bytes(response.into_body(), 8 * 1024 * 1024).await.unwrap();
        assert!(
            body.len() <= 1024 * 1024,
            "the replayed error body must be capped at error_body_max (len={})",
            body.len()
        );
        gateway.shutdown().await;
    }

    /// 映射非流式响应解压后的明文超过单次 `FEED_LIMIT`（2 MiB）、但未超过
    /// 总体上限时，必须完整解码并转换成功——单发解码只拿到第一段却把合法的大
    /// 响应判成“截断”，修复前这里返回 502 `upstream_decode_error`。
    #[tokio::test]
    async fn mapped_non_stream_plaintext_over_feed_limit_decodes_fully() {
        use std::io::Write;
        // 3 MiB 正文 + 尾部标记：只要尾部标记出现在响应里，就说明整段明文
        // （而不只是第一段）都经过了解码与转换。
        let filler = format!("{}ENDMARK", "x".repeat(3 * 1024 * 1024));
        let plain = format!(
            r#"{{"id":"1","choices":[{{"finish_reason":"stop","message":{{"role":"assistant","content":"{filler}"}}}}]}}"#
        );
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(plain.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let response_bytes = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\ncontent-length: {}\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        let mut body = response_bytes;
        body.extend_from_slice(&compressed);
        let port = spawn_upstream(body, false).await;
        let gateway = test_gateway_command_code(port, &[("max_buffered_upstream_body_mb", "8")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(false), "claude", None)
            .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a valid compressed body larger than one feed must still convert"
        );
        let body = to_bytes(response.into_body(), 16 * 1024 * 1024).await.unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("ENDMARK"),
            "the full plaintext must reach the converter (response len={})",
            body.len()
        );
        assert!(
            body.len() > 3 * 1024 * 1024,
            "the converted body must carry the whole 3 MiB content"
        );
        gateway.shutdown().await;
    }

    /// 累计明文在流中途超过上限的映射流会
    /// 以一个固定网关错误事件与 `gateway_error` outcome 终止——
    /// 流绝不会被静默截断。
    /// 正文是单个 gzip 流（单成员解码器忽略任何
    /// 尾部成员），拆成两次 TCP 写入，
    /// 使上限在响应开始后才触发。
    #[tokio::test]
    async fn mapped_stream_cumulative_limit_fails_attempt() {
        use std::io::Write;
        let event1 =
            sse_event(r#"{"id":"1","choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}"#);
        let event2 = sse_event(&format!(
            r#"{{"id":"2","choices":[{{"delta":{{"content":"{}"}},"finish_reason":null}}]}}"#,
            "x".repeat(2 * 1024 * 1024)
        ));
        let mut plain = event1.into_bytes();
        plain.extend_from_slice(event2.as_bytes());
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&plain).unwrap();
        let compressed = encoder.finish().unwrap();
        assert!(compressed.len() < 16 * 1024, "test payload must stay small");
        // 在 512 字节处切分：第一部分携带响应头 + event1
        // （几百字节明文），因此 prelude 解码它
        // 不会触及上限；其余部分在流中途触发上限。
        let split = 512usize.min(compressed.len() / 2);
        let mut part1 =
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-encoding: gzip\r\n\r\n"
                .to_vec();
        part1.extend_from_slice(&compressed[..split]);
        let parts = vec![part1, compressed[split..].to_vec()];
        let port = spawn_upstream_parts(parts, Duration::from_millis(30)).await;
        let gateway = test_gateway_command_code(port, &[("max_buffered_upstream_body_mb", "1")]).await;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_chat_request(true), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            text.contains("could not be decoded"),
            "client must see the fixed gateway error event, got: {text:?}"
        );
        let (_, outcome, attempts, status) =
            wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
        assert_eq!(outcome, "gateway_error");
        assert_eq!(attempts, 1);
        assert_eq!(status, Some(200));
        let error_kind: Option<String> = sqlx::query_scalar(
            "SELECT error_kind FROM request_attempts ORDER BY attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(error_kind.as_deref(), Some("upstream_decode_error"));
        gateway.shutdown().await;
    }

    /// 两个并发的超限非流式响应都被有界处理，
    /// 都以 502 终止——既不无界增长，也不死锁。
    #[tokio::test]
    async fn concurrent_oversized_responses_stay_bounded() {
        let mut response_bytes = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n"
            .as_bytes()
            .to_vec();
        response_bytes.extend(std::iter::repeat_n(b'x', 2 * 1024 * 1024));
        let port_a = spawn_upstream(response_bytes.clone(), false).await;
        let port_b = spawn_upstream(response_bytes, false).await;
        let gateway_a =
            test_gateway_command_code(port_a, &[("max_buffered_upstream_body_mb", "1")]).await;
        let gateway_b =
            test_gateway_command_code(port_b, &[("max_buffered_upstream_body_mb", "1")]).await;
        let (response_a, response_b) = tokio::join!(
            gateway_a
                .state
                .proxy
                .proxy(cc_claude_chat_request(false), "claude", None),
            gateway_b
                .state
                .proxy
                .proxy(cc_claude_chat_request(false), "claude", None),
        );
        assert_eq!(response_a.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response_b.status(), StatusCode::BAD_GATEWAY);
        for gateway in [gateway_a, gateway_b] {
            let (_, _, attempts, status) =
                wait_for_outcome(&gateway.db, "gateway_error", Duration::from_secs(5)).await;
            assert_eq!(attempts, 1);
            assert_eq!(status, Some(502));
            gateway.shutdown().await;
        }
    }

    async fn catalog_ids(response: Response<Body>) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        (status, value)
    }

    fn catalog_entry<'a>(value: &'a Value, id: &str) -> &'a Value {
        value["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["id"] == id)
            .unwrap()
    }

    fn catalog_model_ids(value: &Value) -> Vec<String> {
        value["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["id"].as_str().unwrap().to_owned())
            .collect()
    }

    /// 未指定协议选择器时 `/v1/models` 聚合每个协议池
    /// （此处为 openai_compatible + claude），按模型 id 去重，
    /// 每个条目携带它可路由经过的每个协议的端点。
    #[tokio::test]
    async fn v1_models_aggregates_all_protocols() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        let request = axum::extract::Request::builder()
            .uri("/v1/models")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["object"], "list");
        assert_eq!(
            catalog_model_ids(&value),
            vec!["claude-model", "test-model"],
            "aggregate must list every protocol pool ordered by id"
        );
        assert_eq!(
            catalog_entry(&value, "test-model")["x_local_gateway"]["supported_endpoints"],
            json!(["/v1/chat/completions"])
        );
        assert_eq!(
            catalog_entry(&value, "claude-model")["x_local_gateway"]["supported_endpoints"],
            json!(["/v1/messages"])
        );
        gateway.shutdown().await;
    }

    /// `/v1/models?protocol=…` 恰好返回该协议的池。
    #[tokio::test]
    async fn v1_models_protocol_query_selects_protocol() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        for (query, expected) in [
            ("?protocol=claude", vec!["claude-model".to_owned()]),
            ("?protocol=openai_compatible", vec!["test-model".to_owned()]),
            ("?protocol=openai_responses", Vec::<String>::new()),
        ] {
            let request = axum::extract::Request::builder()
                .uri(format!("/v1/models{query}"))
                .body(Body::empty())
                .unwrap();
            let (status, value) =
                catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
            assert_eq!(status, StatusCode::OK);
            if query == "?protocol=claude" {
                assert_eq!(value["data"][0]["type"], "model");
            } else {
                assert_eq!(value["object"], "list");
            }
            assert_eq!(
                catalog_model_ids(&value),
                expected,
                "query {query} must select exactly its protocol"
            );
        }
        gateway.shutdown().await;
    }

    /// `X-Local-Gateway-Protocol` 与 `anthropic-version` 都会在 `/v1/models` 上
    /// 选择 Claude 目录；未知协议被拒绝。
    #[tokio::test]
    async fn v1_models_headers_select_or_reject_protocol() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        let request = axum::extract::Request::builder()
            .uri("/v1/models")
            .header("x-local-gateway-protocol", "claude")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalog_model_ids(&value), vec!["claude-model"]);

        let request = axum::extract::Request::builder()
            .uri("/v1/models")
            .header("anthropic-version", "2023-06-01")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalog_model_ids(&value), vec!["claude-model"]);

        let request = axum::extract::Request::builder()
            .uri("/v1/models?protocol=gemini")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(openai_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            value.pointer("/error/message").and_then(Value::as_str),
            Some("Unsupported model catalog protocol.")
        );
        gateway.shutdown().await;
    }

    /// `/v1/messages/models` 以 Claude 原生列表形状作答，
    /// 携带渠道真实的显示名与 `x_local_gateway` 元数据。
    #[tokio::test]
    async fn claude_catalog_uses_native_shape() {
        let port = spawn_upstream(Vec::new(), false).await;
        let gateway = test_gateway_claude(port, &[]).await;
        let request = axum::extract::Request::builder()
            .uri("/v1/messages/models")
            .body(Body::empty())
            .unwrap();
        let (status, value) =
            catalog_ids(claude_models(State(gateway.state.clone()), request).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(catalog_model_ids(&value), vec!["claude-model"]);
        assert_eq!(value["has_more"], false);
        assert_eq!(value["first_id"], "claude-model");
        assert_eq!(value["last_id"], "claude-model");
        let entry = &value["data"][0];
        assert_eq!(entry["type"], "model");
        assert_eq!(entry["id"], "claude-model");
        assert_eq!(entry["display_name"], "Test Model");
        assert_eq!(entry["created_at"], "2026-08-04T01:00:00+00:00");
        assert_eq!(
            entry["x_local_gateway"]["supported_endpoints"],
            json!(["/v1/messages"])
        );
        gateway.shutdown().await;
    }

    /// 为同一模型播种第二个 openai_compatible 候选（优先级 2），
    /// 指向 `port_b`——`test-model` 的 failover 目标。
    async fn seed_backup_candidate(db: &Database, secrets: &SecretStore, port_b: u16) {
        let time = crate::test_support::SEED_TIME;
        crate::test_support::seed_provider(
            db,
            "prov-2",
            "mock-b",
            &format!("http://127.0.0.1:{port_b}"),
        )
        .await;
        crate::test_support::seed_channel_named(
            db,
            secrets,
            "ch-2",
            "prov-2",
            "chan-b",
            "openai_compatible",
            "test-key",
        )
        .await;
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-2','ch-2','test-model','Test Model B','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-2','openai_compatible')")
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-1','cm-2',2,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-2','active',0,?)")
            .bind(time)
            .execute(db.pool())
            .await
            .unwrap();
    }

    fn http_error_upstream(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    fn json_ok_upstream(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_bytes()
    }

    /// 在主候选失败、备用候选成功的请求上，
    /// 恰好发出一条描述交接的 failover 通知。
    #[tokio::test]
    async fn failover_emits_notification_with_http_error() {
        let port_a = spawn_upstream(
            http_error_upstream("500 Internal Server Error", r#"{"error":"boom"}"#),
            false,
        )
        .await;
        let port_b = spawn_upstream(
            json_ok_upstream(r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#),
            false,
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;
        seed_backup_candidate(&gateway.db, &gateway.secrets, port_b).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let notices = gateway.notifier.notices();
        assert_eq!(notices.len(), 1, "one failover -> one notice");
        assert_eq!(notices[0].model_id, "test-model");
        assert_eq!(notices[0].failed_channel_name, "chan");
        assert_eq!(notices[0].next_channel_name, "chan-b");
        assert_eq!(notices[0].error_kind.as_deref(), Some("HTTP 500"));
        gateway.shutdown().await;
    }

    /// 每次 failover 都被上报：主候选损坏时两次请求发出
    /// 两条通知——notifier 只是批处理，从不限流。
    #[tokio::test]
    async fn consecutive_failovers_each_emit_notice() {
        let port_a = spawn_upstream(
            http_error_upstream("500 Internal Server Error", r#"{"error":"boom"}"#),
            false,
        )
        .await;
        let port_b = spawn_upstream_reusable(
            json_ok_upstream(r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#),
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;
        seed_backup_candidate(&gateway.db, &gateway.secrets, port_b).await;

        for _ in 0..2 {
            let response = gateway
                .state
                .proxy
                .proxy(chat_request(false), "openai_compatible", None)
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_eq!(gateway.notifier.notices().len(), 2);
        gateway.shutdown().await;
    }

    /// 主候选上的传输失败（连接重置）在通知中
    /// 以传输种类标注。
    #[tokio::test]
    async fn failover_transport_reset_labels_error_kind() {
        // 空响应后关闭：HTTP 解析看到 EOF -> 传输错误。
        let port_a = spawn_upstream(Vec::new(), false).await;
        let port_b = spawn_upstream(
            json_ok_upstream(r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#),
            false,
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;
        seed_backup_candidate(&gateway.db, &gateway.secrets, port_b).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let notices = gateway.notifier.notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].error_kind.as_deref(), Some("connection_reset"));
        gateway.shutdown().await;
    }

    /// 没有备用候选时不存在 failover：单候选失败
    /// 返回 502 且不发通知。
    #[tokio::test]
    async fn single_candidate_failure_emits_no_notice() {
        let port_a = spawn_upstream(
            http_error_upstream("500 Internal Server Error", r#"{"error":"boom"}"#),
            false,
        )
        .await;
        let gateway = test_gateway(port_a, &[]).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        // 无 failover：上游的 500 错误响应被原样透传。
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(gateway.notifier.notices().is_empty());
        gateway.shutdown().await;
    }

    /// 把捕获到的请求头报告给测试、并以最小 OpenAI completion
    /// 作答的裸 HTTP/1.1 上游。
    async fn spawn_upstream_capturing_head() -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 8192];
                let mut total = 0usize;
                loop {
                    match stream.read(&mut buf[total..]).await {
                        Ok(0) => break,
                        Ok(n) => {
                            total += n;
                            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let _ = tx.send(String::from_utf8_lossy(&buf[..total]).into_owned());
                let response = json_ok_upstream(
                    r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#,
                );
                let _ = stream.write_all(&response).await;
            }
        });
        (port, rx)
    }

    fn session_header_value(head: &str) -> Option<String> {
        head.lines()
            .find(|line| {
                line.to_ascii_lowercase()
                    .starts_with(&format!("{}:", protocol::OPENCODE_SESSION_HEADER))
            })
            .and_then(|line| line.split_once(':'))
            .map(|(_, value)| value.trim().to_owned())
    }

    fn session_header_count(head: &str) -> usize {
        head.lines()
            .filter(|line| {
                line.to_ascii_lowercase()
                    .starts_with(&format!("{}:", protocol::OPENCODE_SESSION_HEADER))
            })
            .count()
    }

    /// OpenCode Zen/Go 上游会拒绝不带会话头的请求：
    /// 客户端未提供时，网关必须注入
    /// 其持久化的 install id。
    #[tokio::test]
    async fn opencode_upstream_receives_injected_session_header() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;
        sqlx::query("UPDATE providers SET base_url=? WHERE id='prov-1'")
            .bind(format!("http://127.0.0.1:{port}/zen/go"))
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        let session = session_header_value(&head).expect("x-opencode-session must be injected");
        assert!(!session.is_empty(), "injected session id must not be empty");
        assert_eq!(session_header_count(&head), 1, "exactly one session header");
        let stored: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='opencode_session_id'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(
            session,
            stored.trim_matches('"'),
            "injected value must be the persisted install id"
        );
        gateway.shutdown().await;
    }

    /// 非 OpenCode 上游必须保持不动：
    /// 该会话头是 opencode.ai/zen 端点专用的。
    #[tokio::test]
    async fn non_opencode_upstream_has_no_session_header() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;

        let response = gateway
            .state
            .proxy
            .proxy(chat_request(false), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        assert!(
            session_header_value(&head).is_none(),
            "plain upstreams must not receive x-opencode-session: {head}"
        );
        gateway.shutdown().await;
    }

    /// 客户端提供的会话头总是胜过
    /// 网关自己的兜底 id。
    #[tokio::test]
    async fn client_session_header_is_forwarded_unchanged() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;
        sqlx::query("UPDATE providers SET base_url=? WHERE id='prov-1'")
            .bind(format!("http://127.0.0.1:{port}/zen/go"))
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let body =
            r#"{"model":"test-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header(protocol::OPENCODE_SESSION_HEADER, "client-session-42")
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        assert_eq!(
            session_header_value(&head).as_deref(),
            Some("client-session-42"),
            "client-supplied session id must be forwarded unchanged"
        );
        assert_eq!(
            session_header_count(&head),
            1,
            "client-supplied header must not be duplicated"
        );
        gateway.shutdown().await;
    }

    #[tokio::test]
    async fn opencode_client_headers_pass_through_non_opencode_upstreams() {
        let (port, mut heads) = spawn_upstream_capturing_head().await;
        let gateway = test_gateway(port, &[]).await;

        let body =
            r#"{"model":"test-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("x-opencode-client", "cli")
            .header("x-opencode-project", "project-7")
            .header("x-opencode-request", "request-9")
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let head = tokio::time::timeout(Duration::from_secs(5), heads.recv())
            .await
            .expect("captured upstream request timed out")
            .expect("captured upstream request missing");
        for (name, value) in [
            ("x-opencode-client", "cli"),
            ("x-opencode-project", "project-7"),
            ("x-opencode-request", "request-9"),
        ] {
            let forwarded = head.lines().find_map(|line| {
                let (header, found) = line.split_once(':')?;
                if header.eq_ignore_ascii_case(name) {
                    Some(found.trim().to_owned())
                } else {
                    None
                }
            });
            assert_eq!(
                forwarded.as_deref(),
                Some(value),
                "{name} must pass through unchanged"
            );
        }
        assert!(session_header_value(&head).is_none());
        gateway.shutdown().await;
    }
    // -----------------------------------------------------------------
    // Command Code Go（路由 B）集成
    // -----------------------------------------------------------------

    /// Command Code mock 服务的假上游行为。
    #[derive(Clone, Copy)]
    enum CcUpstreamMode {
        /// Go 套餐：Provider API 返回 403 upgrade_required，随后
        /// 网关切换到 `/alpha/generate`（NDJSON）。
        GoDowngrade,
        /// 付费套餐：Provider API 返回普通 OpenAI SSE。
        PaidProvider,
        /// `test-key` 命中 429 配额窗口；其它任何 key 都成功。
        QuotaAware,
    }

    fn cc_claude_request(body: &str) -> axum::extract::Request {
        axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    fn cc_header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|line| {
            let (header, value) = line.split_once(':')?;
            header.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    /// 裸 TCP Command Code mock：记录每个请求的 `(path, head, body)`，
    /// 并按模式返回 Provider API / `/alpha/generate`。
    async fn spawn_command_code_upstream(
        mode: CcUpstreamMode,
    ) -> (
        u16,
        tokio::sync::mpsc::UnboundedReceiver<(String, String, String)>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        let n = match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                    let content_length = cc_header(&head, "content-length")
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    while buf.len() < head_end + content_length {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body = String::from_utf8_lossy(&buf[head_end..]).into_owned();
                    let path = head
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_owned();
                    let authorization = cc_header(&head, "authorization")
                        .unwrap_or_default()
                        .to_owned();
                    let _ = tx.send((path.clone(), head, body));
                    if path.contains("/alpha/fingerprint/record")
                        || path.contains("/alpha/lifecycle-events")
                    {
                        let payload = b"{}";
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                            payload.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(payload).await;
                    } else if path.contains("/provider/v1/chat/completions") {
                        match mode {
                            CcUpstreamMode::GoDowngrade | CcUpstreamMode::QuotaAware => {
                                let payload = br#"{"error":{"code":"upgrade_required","message":"upgrade required"}}"#;
                                let response = format!(
                                    "HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                                    payload.len()
                                );
                                let _ = stream.write_all(response.as_bytes()).await;
                                let _ = stream.write_all(payload).await;
                            }
                            CcUpstreamMode::PaidProvider => {
                                let payload = concat!(
                                    "data: {\"id\":\"chat-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"cc-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Paid\"},\"finish_reason\":null}]}\n\n",
                                    "data: {\"id\":\"chat-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"cc-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":1,\"total_tokens\":6}}\n\n",
                                    "data: [DONE]\n\n"
                                );
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n\r\n",
                                    payload.len()
                                );
                                let _ = stream.write_all(response.as_bytes()).await;
                                let _ = stream.write_all(payload.as_bytes()).await;
                            }
                        }
                    } else if path.contains("/alpha/generate") {
                        if matches!(mode, CcUpstreamMode::QuotaAware)
                            && authorization.contains("test-key")
                        {
                            let payload = br#"{"error":{"code":"rate_limit_exceeded","message":"quota exhausted for the 5h window","resetAt":"2099-01-01T00:00:00Z"}}"#;
                            let response = format!(
                                "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                                payload.len()
                            );
                            let _ = stream.write_all(response.as_bytes()).await;
                            let _ = stream.write_all(payload).await;
                            return;
                        }
                        let payload = concat!(
                            "{\"type\":\"start\"}\n",
                            "{\"type\":\"reasoning-delta\",\"text\":\"plan\"}\n",
                            "{\"type\":\"text-delta\",\"text\":\"Hello\"}\n",
                            "{\"type\":\"finish\",\"finishReason\":\"stop\",\"totalUsage\":{\"inputTokens\":120,\"outputTokens\":2,\"cachedInputTokens\":100,\"inputTokenDetails\":{\"noCacheTokens\":20,\"cacheReadTokens\":100,\"cacheWriteTokens\":0}}}\n"
                        );
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\n\r\n",
                            payload.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(payload.as_bytes()).await;
                    } else {
                        let payload = b"{}";
                        let response = format!(
                            "HTTP/1.1 404 Not Found\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                            payload.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(payload).await;
                    }
                });
            }
        });
        (port, rx)
    }

    /// Go 套餐：首个请求命中官方 Provider API，拿到
    /// 文档化的 403 upgrade_required，随后同一候选带 CLI 身份头
    /// 在 `/alpha/generate` 上重试。NDJSON 响应被
    /// 转换成 Claude 流（thinking + signature + 缓存校正后的
    /// usage）。
    #[tokio::test]
    async fn command_code_go_downgrades_to_generate_with_identity_headers() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway =
            test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let request = cc_claude_request(
            r#"{"model":"cc-model","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
        );
        let response = gateway
            .state
            .proxy
            .proxy(request, "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let stream = String::from_utf8_lossy(&bytes);

        // 尝试序列为：Provider API 探测 -> 403 ->
        // 指纹/生命周期初始化 -> /alpha/generate。
        let mut captured = Vec::new();
        for _ in 0..4 {
            captured.push(
                tokio::time::timeout(Duration::from_secs(5), requests.recv())
                    .await
                    .expect("upstream request timed out")
                    .expect("upstream request missing"),
            );
        }
        let first = captured
            .iter()
            .find(|(path, _, _)| path.contains("/provider/v1/chat/completions"))
            .expect("provider request missing");
        let second = captured
            .iter()
            .find(|(path, _, _)| path.contains("/alpha/generate"))
            .expect("generate request missing");
        assert!(
            captured
                .iter()
                .any(|(path, _, _)| path.contains("/alpha/fingerprint/record")),
            "fingerprint must be recorded before the reverse path"
        );
        // 官方 API 尝试不携带 CLI 身份。
        assert!(cc_header(&first.1, "x-cli-environment").is_none());
        // 反向路径携带完整的 CLI 身份。
        for name in [
            "x-cli-environment",
            "x-command-code-version",
            "x-session-id",
            "x-co-flag",
            "x-taste-learning",
            "traceparent",
            "x-project-slug",
            "authorization",
        ] {
            assert!(cc_header(&second.1, name).is_some(), "{name} must be injected");
        }
        assert_eq!(cc_header(&second.1, "x-cli-environment"), Some("production"));
        assert_eq!(cc_header(&second.1, "x-co-flag"), Some("false"));
        assert_eq!(cc_header(&second.1, "x-taste-learning"), Some("false"));
        assert!(
            cc_header(&second.1, "authorization")
                .unwrap()
                .starts_with("Bearer ")
        );
        // 请求体是 Command Code 形状（系统占位符，
        // 已应用模型映射）。
        let cc_body: Value = serde_json::from_str(&second.2).unwrap();
        assert_eq!(cc_body.pointer("/params/model").unwrap(), "cc-model");
        assert_eq!(cc_body.pointer("/params/system").unwrap(), " ");
        assert_eq!(cc_body.pointer("/params/stream").unwrap(), true);
        assert_eq!(
            cc_body.pointer("/params/messages/0/content/0/text").unwrap(),
            "hi"
        );
        // Claude 流：带合成 signature 的 thinking、文本，
        // 以及缓存校正后的 Anthropic usage。
        assert!(stream.contains("\"type\":\"message_start\""), "{stream}");
        assert!(stream.contains("\"thinking_delta\""), "{stream}");
        assert!(stream.contains("\"signature_delta\""), "{stream}");
        assert!(stream.contains("\"text_delta\""), "{stream}");
        assert!(stream.contains("\"input_tokens\":20"), "{stream}");
        assert!(stream.contains("\"cache_read_input_tokens\":100"), "{stream}");
        assert!(stream.contains("\"text\":\"Hello\""), "{stream}");
        // 传输路由器记忆 + 每 key 身份状态被持久化。
        let transport: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='command_code_transport_ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert!(transport.contains("generate"), "{transport}");
        let fingerprint: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='command_code_fingerprint_ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert!(fingerprint.contains("thumbmark"), "{fingerprint}");
        let session: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='command_code_session_ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert!(session.contains("expires_at"), "{session}");
        gateway.shutdown().await;
    }

    /// 付费套餐：官方 Provider API 成功并被记住；
    /// 反向路径从不被触及（无指纹、无 CLI 身份）。
    #[tokio::test]
    async fn command_code_paid_plan_stays_on_provider_api() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::PaidProvider).await;
        let gateway =
            test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let request = cc_claude_request(
            r#"{"model":"cc-model","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
        );
        let response = gateway
            .state
            .proxy
            .proxy(request, "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let stream = String::from_utf8_lossy(&bytes);
        assert!(stream.contains("Paid"), "{stream}");
        let first = tokio::time::timeout(Duration::from_secs(5), requests.recv())
            .await
            .expect("provider request timed out")
            .expect("provider request missing");
        assert!(first.0.contains("/provider/v1/chat/completions"));
        assert!(cc_header(&first.1, "x-cli-environment").is_none());
        assert!(cc_header(&first.1, "x-command-code-version").is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(300), requests.recv())
                .await
                .is_err(),
            "paid plans must never fall back to /alpha/generate"
        );
        let transport: String = sqlx::query_scalar(
            "SELECT CAST(value_json AS TEXT) FROM settings WHERE key='command_code_transport_ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert!(transport.contains("provider"), "{transport}");
        // 无反向路径副作用：无指纹、无初始化节流。
        let fingerprint: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM settings WHERE key='command_code_fingerprint_ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(fingerprint, 0);
        gateway.shutdown().await;
    }

    /// 非流式 Claude 客户端得到一条聚合的 Claude 消息，
    /// 尽管 `/alpha/generate` 上游始终以 NDJSON 流式返回。
    #[tokio::test]
    async fn command_code_non_stream_aggregates_ndjson() {
        let (port, _requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway =
            test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let request = cc_claude_request(
            r#"{"model":"cc-model","max_tokens":10,"stream":false,"messages":[{"role":"user","content":"hi"}]}"#,
        );
        let response = gateway
            .state
            .proxy
            .proxy(request, "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.get("type").and_then(Value::as_str), Some("message"));
        assert_eq!(
            body.pointer("/content/0/type").and_then(Value::as_str),
            Some("thinking")
        );
        assert_eq!(
            body.pointer("/content/1/text").and_then(Value::as_str),
            Some("Hello")
        );
        assert_eq!(body.pointer("/usage/input_tokens").and_then(Value::as_i64), Some(20));
        assert_eq!(
            body.pointer("/usage/cache_read_input_tokens").and_then(Value::as_i64),
            Some(100)
        );
        gateway.shutdown().await;
    }

    /// 总开关默认关闭：关闭期间网关
    /// 以清晰错误作答，且执行零个上游请求。
    #[tokio::test]
    async fn command_code_disabled_blocks_all_upstream_requests() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        // 集成关闭的裸脚手架（helper 会自动开启该开关，这里刻意不用它）。
        let gateway = test_gateway_inner(port, &[], false, true).await;
        let request = cc_claude_request(
            r#"{"model":"cc-model","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
        );
        let response = gateway
            .state
            .proxy
            .proxy(request, "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body.pointer("/error/type").and_then(Value::as_str),
            Some("command_code_disabled")
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), requests.recv())
                .await
                .is_err(),
            "disabled integration must not touch the upstream"
        );
        gateway.shutdown().await;
    }

    /// Command Code 配额错误（429 + resetAt）会冷却该
    /// 渠道到窗口重置，请求 failover 到
    /// 第二个账号，后续请求完全跳过被冷却的渠道。
    #[tokio::test]
    async fn command_code_quota_error_cools_channel_until_window_reset() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::QuotaAware).await;
        let gateway =
            test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let time = crate::test_support::SEED_TIME;
        crate::test_support::seed_channel_named(
            &gateway.db,
            &gateway.secrets,
            "ch-2",
            "prov-1",
            "chan-2",
            "command_code",
            "good-key",
        )
        .await;
        sqlx::query("INSERT INTO channel_models(id,channel_id,model_id,display_name,source,available,first_seen_at,created_at,updated_at) VALUES('cm-2','ch-2','cc-model','CC 2','discovered',1,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(time)
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-2','command_code')")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES('rc-2','route-1','cm-2',2,1,?,?)")
            .bind(time)
            .bind(time)
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO channel_health(channel_id,state,consecutive_failures,updated_at) VALUES('ch-2','active',0,?)")
            .bind(time)
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let body = r#"{"model":"cc-model","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_request(body), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("Hello"));

        let (state, disabled_until): (String, Option<String>) = sqlx::query_as(
            "SELECT state, disabled_until FROM channel_health WHERE channel_id='ch-1'",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(state, "open", "quota error must open the channel");
        assert!(
            disabled_until
                .unwrap_or_default()
                .starts_with("2099-01-01"),
            "cooldown must target the window resetAt"
        );

        // 排干首个请求的流量；第二个请求必须只
        // 路由到健康账号。
        while tokio::time::timeout(Duration::from_millis(30), requests.recv())
            .await
            .is_ok()
        {}
        let response = gateway
            .state
            .proxy
            .proxy(cc_claude_request(body), "claude", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        // 消费正文：未消费的流式响应会让
        // 流生成器（及其遥测发送端克隆）保持存活，
        // 这会使遥测 writer 在关闭时永远等待。
        let _ = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let mut seen: Vec<(String, String)> = Vec::new();
        while let Ok(Some((path, head, _))) =
            tokio::time::timeout(Duration::from_millis(300), requests.recv()).await
        {
            seen.push((
                path,
                cc_header(&head, "authorization").unwrap_or_default().to_owned(),
            ));
        }
        assert!(
            !seen.is_empty(),
            "second request must reach the healthy channel"
        );
        for (path, authorization) in &seen {
            assert!(
                authorization.contains("good-key"),
                "{path} was routed to the cooled channel: {authorization}"
            );
        }
        gateway.shutdown().await;
    }

    /// generate 正文会被一直保持打开、直到测试释放的
    /// Command Code 上游——用于验证每账号突发保护。
    async fn spawn_gated_command_code_upstream() -> (
        u16,
        tokio::sync::mpsc::UnboundedReceiver<(String, String, String)>,
        Arc<tokio::sync::Notify>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(tokio::sync::Notify::new());
        let gate_for_mock = Arc::clone(&gate);
        // 只有第一个 generate 正文被保持打开；
        // 槽位释放后后续请求直接流过。
        let first_generate = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let first_for_mock = Arc::clone(&first_generate);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                let gate = Arc::clone(&gate_for_mock);
                let first_generate = Arc::clone(&first_for_mock);
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        let n = match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                    let path = head
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_owned();
                    let _ = tx.send((path.clone(), head, String::new()));
                    if path.contains("/provider/v1/chat/completions") {
                        let payload = br#"{"error":{"code":"upgrade_required"}}"#;
                        let response = format!(
                            "HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                            payload.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(payload).await;
                    } else if path.contains("/alpha/generate") {
                        let full = concat!(
                            "{\"type\":\"text-delta\",\"text\":\"Hello\"}\n",
                            "{\"type\":\"finish\",\"finishReason\":\"stop\",\"totalUsage\":{\"inputTokens\":5,\"outputTokens\":2,\"cachedInputTokens\":0,\"inputTokenDetails\":{\"noCacheTokens\":5,\"cacheReadTokens\":0,\"cacheWriteTokens\":0}}}\n"
                        );
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/x-ndjson\r\ncontent-length: {}\r\n\r\n",
                            full.len()
                        );
                        let half = full.len() / 2;
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(&full.as_bytes()[..half]).await;
                        // 把第一个正文保持打开，直到测试释放它。
                        if first_generate.swap(false, std::sync::atomic::Ordering::SeqCst) {
                            gate.notified().await;
                        }
                        let _ = stream.write_all(&full.as_bytes()[half..]).await;
                    } else {
                        let payload = b"{}";
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                            payload.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.write_all(payload).await;
                    }
                });
            }
        });
        (port, rx, gate)
    }

    /// 一个账号（渠道）不得把突发流量扇出成并行的
    /// `/alpha/generate` 调用：请求 A 持有槽位时请求 B
    /// 等待；A 的正文结束后 B 继续。
    #[tokio::test]
    async fn command_code_single_account_concurrency_is_capped() {
        let (port, mut requests, gate) = spawn_gated_command_code_upstream().await;
        let gateway = test_gateway_command_code(
            port,
            &[
                ("command_code_enabled", "true"),
                ("command_code_max_concurrency", "1"),
            ],
        )
        .await;
        let body = r#"{"model":"cc-model","max_tokens":10,"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;

        let proxy_a = Arc::clone(&gateway.state.proxy);
        let task_a = tokio::spawn(async move {
            proxy_a
                .proxy(cc_claude_request(body), "claude", None)
                .await
        });
        // 等待 A 真正到达 `/alpha/generate`。
        let mut a_seen = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !a_seen && tokio::time::Instant::now() < deadline {
            if let Ok(Some((path, _, _))) =
                tokio::time::timeout(Duration::from_millis(100), requests.recv()).await
            {
                a_seen = path.contains("/alpha/generate");
            }
        }
        assert!(a_seen, "first request must reach /alpha/generate");

        let proxy_b = Arc::clone(&gateway.state.proxy);
        let task_b = tokio::spawn(async move {
            proxy_b
                .proxy(cc_claude_request(body), "claude", None)
                .await
        });
        // B 可以探测 provider，但在 A 持有唯一槽位时
        // 不得发起第二次 generate。
        let wait_until = tokio::time::Instant::now() + Duration::from_millis(250);
        while tokio::time::Instant::now() < wait_until {
            if let Ok(Some((path, _, _))) =
                tokio::time::timeout(Duration::from_millis(50), requests.recv()).await
            {
                assert!(
                    !path.contains("/alpha/generate"),
                    "second generate started while the account slot was held"
                );
            }
        }

        gate.notify_one();
        // 先排干 A 的响应正文：槽位附在 A 的上游
        // 正文上，只有该正文被完整消费后才释放。
        let response = task_a.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();

        // A 已完成，槽位释放；B 随后必须到达 generate。
        let mut b_seen = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !b_seen && tokio::time::Instant::now() < deadline {
            if let Ok(Some((path, _, _))) =
                tokio::time::timeout(Duration::from_millis(100), requests.recv()).await
            {
                b_seen = path.contains("/alpha/generate");
            }
        }
        assert!(b_seen, "second request must reach /alpha/generate after the slot frees");

        let response = task_b.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        gateway.shutdown().await;
    }

    /// Responses 入口（`/v1/responses`）同样驱动 Command Code
    /// 管线；Responses usage 保留输入 token 总数，
    /// 而缓存计数是其子集。
    #[tokio::test]
    async fn command_code_codex_entry_converts_ndjson_to_responses() {
        let (port, _requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway =
            test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let body = r#"{"model":"cc-model","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("content-type", "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_responses", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let stream = String::from_utf8_lossy(&bytes);
        assert!(stream.contains("\"type\":\"response.created\""), "{stream}");
        assert!(stream.contains("\"type\":\"response.completed\""), "{stream}");
        assert!(stream.contains("Hello"), "{stream}");
        assert!(stream.contains("\"input_tokens\":120"), "{stream}");
        assert!(stream.contains("\"cached_tokens\":100"), "{stream}");
        gateway.shutdown().await;
    }

    /// 无映射 OAI 聊天入口直连 `command_code` 路由：网关必须静默回落协议，
    /// 把 OpenAI chat 请求转成 `/alpha/generate` body，再把 NDJSON 解码回
    /// OpenAI SSE 交给客户端。
    #[tokio::test]
    async fn command_code_openai_chat_entry_silently_converts() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway = test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let body = r#"{"model":"cc-model","stream":true,"max_tokens":16,"reasoning_effort":"low",
            "messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let stream = String::from_utf8_lossy(&bytes);
        assert!(
            stream.contains("\"object\":\"chat.completion.chunk\""),
            "{stream}"
        );
        assert!(stream.contains("Hello"), "{stream}");
        assert!(stream.contains("\"reasoning_content\":\"plan\""), "{stream}");
        assert!(stream.contains("\"usage\""), "{stream}");

        // 上游收到的是 CC 信封，而不是原始 OpenAI body。
        let mut generate_body = None;
        while let Ok((path, _head, request_body)) = requests.try_recv() {
            if path.contains("/alpha/generate") {
                generate_body = Some(request_body);
            }
        }
        let generate_body = generate_body.expect("an /alpha/generate request");
        let value: serde_json::Value = serde_json::from_str(&generate_body).unwrap();
        assert_eq!(
            value.pointer("/params/system").and_then(serde_json::Value::as_str),
            Some("be brief")
        );
        assert_eq!(
            value.pointer("/params/model").and_then(serde_json::Value::as_str),
            Some("cc-model")
        );
        assert_eq!(
            value.pointer("/params/reasoning_effort").and_then(serde_json::Value::as_str),
            Some("low")
        );
        assert_eq!(
            value
                .pointer("/params/messages/0/content/0/type")
                .and_then(serde_json::Value::as_str),
            Some("text")
        );
        gateway.shutdown().await;
    }

    /// 无映射 OAI 聊天入口 + 非流式：NDJSON 必须聚合成一个 OpenAI completion
    /// （而不是把 NDJSON 原样转发），并清掉上游的 content-length/encoding。
    #[tokio::test]
    async fn command_code_openai_chat_entry_non_stream_aggregates() {
        let (port, _requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway = test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        let body = r#"{"model":"cc-model","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert!(response.headers().get("content-length").is_none());
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["object"], "chat.completion");
        assert_eq!(value["model"], "cc-model");
        assert_eq!(
            value.pointer("/choices/0/message/content").and_then(Value::as_str),
            Some("Hello")
        );
        assert_eq!(
            value
                .pointer("/choices/0/message/reasoning_content")
                .and_then(Value::as_str),
            Some("plan")
        );
        assert_eq!(value.pointer("/usage/prompt_tokens").and_then(Value::as_i64), Some(120));
        assert_eq!(value.pointer("/usage/completion_tokens").and_then(Value::as_i64), Some(2));
        assert_eq!(
            value
                .pointer("/usage/prompt_tokens_details/cached_tokens")
                .and_then(Value::as_i64),
            Some(100)
        );
        gateway.shutdown().await;
    }

    /// 把 Command Code 渠道模型绑定到 discovery 写入的入口协议，
    /// 并添加两条入口协议路由，其请求模型 id
    /// （`route-openai` / `route-claude`）不同于候选真实的
    /// 上游 id（`cc-model`）。
    async fn add_command_code_entry_routes(gateway: &TestGateway) {
        let time = "2026-08-04T01:00:00+00:00";
        for protocol in ["openai_compatible", "openai_responses", "claude"] {
            sqlx::query("INSERT OR IGNORE INTO channel_model_protocols(channel_model_id,protocol) VALUES('cm-1',?)")
                .bind(protocol)
                .execute(gateway.db.pool())
                .await
                .unwrap();
        }
        for (route_id, candidate_id, protocol, requested) in [
            ("route-oai", "rc-oai", "openai_compatible", "route-openai"),
            ("route-claude", "rc-claude", "claude", "route-claude"),
        ] {
            sqlx::query("INSERT INTO model_routes(id,protocol,requested_model_id,enabled,created_at,updated_at) VALUES(?,?,?,1,?,?)")
                .bind(route_id)
                .bind(protocol)
                .bind(requested)
                .bind(time)
                .bind(time)
                .execute(gateway.db.pool())
                .await
                .unwrap();
            sqlx::query("INSERT INTO route_candidates(id,route_id,channel_model_id,priority,enabled,created_at,updated_at) VALUES(?,?,'cm-1',1,1,?,?)")
                .bind(candidate_id)
                .bind(route_id)
                .bind(time)
                .bind(time)
                .execute(gateway.db.pool())
                .await
                .unwrap();
        }
    }

    /// 普通 `openai_compatible` 路由上的 Command Code 候选端到端说
    /// CC 线格式：网关备好 `/alpha/generate`
    /// 正文，把候选真实的模型 id 改写进去，
    /// 并把一条 OpenAI chat 流交给客户端。
    #[tokio::test]
    async fn command_code_candidate_serves_an_openai_route() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway = test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        add_command_code_entry_routes(&gateway).await;
        let body = r#"{"model":"route-openai","stream":true,"max_tokens":16,
            "messages":[{"role":"system","content":"be brief"},{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let stream = String::from_utf8_lossy(&bytes);
        assert!(
            stream.contains("\"object\":\"chat.completion.chunk\""),
            "{stream}"
        );
        assert!(stream.contains("Hello"), "{stream}");

        let mut captured = Vec::new();
        while let Ok(Some(request)) =
            tokio::time::timeout(Duration::from_millis(500), requests.recv()).await
        {
            captured.push(request);
        }
        let generate = captured
            .iter()
            .find(|(path, _, _)| path.contains("/alpha/generate"))
            .expect("the CC generate request is missing");
        let cc_body: Value = serde_json::from_str(&generate.2).unwrap();
        assert_eq!(
            cc_body.pointer("/params/model").and_then(Value::as_str),
            Some("cc-model"),
            "the candidate's real upstream id must win over the route name"
        );
        assert_eq!(
            cc_body.pointer("/params/system").and_then(Value::as_str),
            Some("be brief"),
            "the entry body is converted, not forwarded verbatim"
        );
        assert!(
            captured
                .iter()
                .any(|(path, _, _)| path.contains("/provider/v1/chat/completions")),
            "the official Provider API is still probed first"
        );
        gateway.shutdown().await;
    }

    /// 同一候选在普通 `claude` 路由下返回 Claude 流。
    #[tokio::test]
    async fn command_code_candidate_serves_a_claude_route() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway = test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        add_command_code_entry_routes(&gateway).await;
        let body = r#"{"model":"route-claude","max_tokens":16,"stream":true,
            "messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = gateway.state.proxy.proxy(request, "claude", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let stream = String::from_utf8_lossy(&bytes);
        assert!(stream.contains("\"type\":\"message_start\""), "{stream}");
        assert!(stream.contains("\"type\":\"text_delta\""), "{stream}");
        assert!(stream.contains("\"text\":\"Hello\""), "{stream}");
        let mut captured = Vec::new();
        while let Ok(Some(request)) =
            tokio::time::timeout(Duration::from_millis(500), requests.recv()).await
        {
            captured.push(request);
        }
        let generate = captured
            .iter()
            .find(|(path, _, _)| path.contains("/alpha/generate"))
            .expect("the CC generate request is missing");
        let cc_body: Value = serde_json::from_str(&generate.2).unwrap();
        assert_eq!(
            cc_body.pointer("/params/model").and_then(Value::as_str),
            Some("cc-model")
        );
        assert_eq!(
            cc_body.pointer("/params/system").and_then(Value::as_str),
            Some(" "),
            "Command Code needs the system placeholder"
        );
        gateway.shutdown().await;
    }

    /// 非流式 Claude 入口：聚合后的 OpenAI 正文被转回
    /// Claude 消息形状（解码 + 响应转换）。
    #[tokio::test]
    async fn command_code_candidate_serves_a_non_stream_claude_route() {
        let (port, _requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        let gateway = test_gateway_command_code(port, &[("command_code_enabled", "true")]).await;
        add_command_code_entry_routes(&gateway).await;
        let body = r#"{"model":"route-claude","max_tokens":16,"stream":false,
            "messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/messages")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = gateway.state.proxy.proxy(request, "claude", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["type"], "message");
        assert_eq!(value["model"], "route-claude");
        assert_eq!(
            value.pointer("/content/0/type").and_then(Value::as_str),
            Some("thinking"),
            "{value}"
        );
        assert_eq!(
            value.pointer("/content/0/thinking").and_then(Value::as_str),
            Some("plan")
        );
        let texts: Vec<&str> = value["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|block| block["type"] == "text")
            .filter_map(|block| block["text"].as_str())
            .collect();
        assert_eq!(texts, vec!["Hello"], "{value}");
        gateway.shutdown().await;
    }

    /// 集成关闭时，Command Code 候选即使在普通 OpenAI 路由上
    /// 也被丢弃：零上游请求，返回通常的 503。
    #[tokio::test]
    async fn command_code_candidate_is_dropped_while_the_integration_is_off() {
        let (port, mut requests) = spawn_command_code_upstream(CcUpstreamMode::GoDowngrade).await;
        // 集成关闭的裸脚手架（helper 会自动开启该开关，这里刻意不用它）。
        let gateway = test_gateway_inner(port, &[], false, true).await;
        add_command_code_entry_routes(&gateway).await;
        let body = r#"{"model":"route-openai","stream":true,"max_tokens":16,
            "messages":[{"role":"user","content":"hi"}]}"#;
        let request = axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let response = gateway
            .state
            .proxy
            .proxy(request, "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value.pointer("/error/code").and_then(Value::as_str),
            Some("no_active_channel"),
            "{value}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), requests.recv())
                .await
                .is_err(),
            "a disabled integration must issue zero upstream requests"
        );
        gateway.shutdown().await;
    }

    /// 捕获完整请求（头 + 正文）的上游，
    /// 使测试能断言网关转发的确切字节。
    async fn spawn_upstream_capturing_request(
    ) -> (u16, tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
                    let length: usize = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < head_end + length {
                        match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let _ = tx.send(buf);
                    let response = json_ok_upstream(
                        r#"{"choices":[{"finish_reason":"stop","message":{"content":"OK"}}]}"#,
                    );
                    let _ = stream.write_all(&response).await;
                });
            }
        });
        (port, rx)
    }

    fn captured_body(request: &[u8]) -> Vec<u8> {
        let head_end = request
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("captured request must carry headers")
            + 4;
        request[head_end..].to_vec()
    }

    fn raw_chat_request(model: &str, body: &[u8]) -> axum::extract::Request {
        let payload = if body.is_empty() {
            format!(r#"{{"model":"{model}","stream":false,"messages":[{{"role":"user","content":"hi"}}]}}"#)
                .into_bytes()
        } else {
            body.to_vec()
        };
        axum::extract::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(payload))
            .unwrap()
    }

    /// 自定义模型（面向网关的 id != 上游 id）以
    /// 候选真实的模型 id 到达上游。
    #[tokio::test]
    async fn custom_model_rewrites_upstream_model_id() {
        let (port, mut bodies) = spawn_upstream_capturing_request().await;
        let gateway = test_gateway(port, &[]).await;
        sqlx::query("UPDATE model_routes SET requested_model_id='my-gpt' WHERE id='route-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE channel_models SET model_id='gpt-4o' WHERE id='cm-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let response = gateway
            .state
            .proxy
            .proxy(raw_chat_request("my-gpt", &[]), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let captured = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .unwrap()
            .unwrap();
        let body = String::from_utf8(captured_body(&captured)).unwrap();
        assert!(body.contains(r#""model":"gpt-4o""#), "body: {body}");
        assert!(!body.contains("my-gpt"), "body: {body}");
        gateway.shutdown().await;
    }

    /// 普通情形（请求 id == 候选 id）逐字节转发，
    /// 包括不寻常的空白，
    /// 从而为既有配置保留直通契约。
    #[tokio::test]
    async fn matching_model_id_is_forwarded_byte_exact() {
        let (port, mut bodies) = spawn_upstream_capturing_request().await;
        let gateway = test_gateway(port, &[]).await;
        let raw = b"{\n  \"model\" : \"test-model\",\n  \"stream\":false,\n  \"messages\":[]\n}";

        let response = gateway
            .state
            .proxy
            .proxy(
                raw_chat_request("test-model", raw),
                "openai_compatible",
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let captured = tokio::time::timeout(Duration::from_secs(5), bodies.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(captured_body(&captured), raw.to_vec());
        gateway.shutdown().await;
    }

    /// 尝试遥测记录实际发往上游的模型
    /// （不是面向网关的自定义名）。
    #[tokio::test]
    async fn custom_model_records_sent_upstream_model() {
        let (port, mut bodies) = spawn_upstream_capturing_request().await;
        let gateway = test_gateway(port, &[]).await;
        sqlx::query("UPDATE model_routes SET requested_model_id='my-gpt' WHERE id='route-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE channel_models SET model_id='gpt-4o' WHERE id='cm-1'")
            .execute(gateway.db.pool())
            .await
            .unwrap();

        let response = gateway
            .state
            .proxy
            .proxy(raw_chat_request("my-gpt", &[]), "openai_compatible", None)
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = bodies.recv().await;
        wait_for_outcome(&gateway.db, "success", Duration::from_secs(5)).await;
        let recorded: Option<String> = sqlx::query_scalar(
            "SELECT upstream_model_id FROM request_attempts ORDER BY started_at DESC, attempt_no DESC LIMIT 1",
        )
        .fetch_one(gateway.db.pool())
        .await
        .unwrap();
        assert_eq!(recorded.as_deref(), Some("gpt-4o"));
        gateway.shutdown().await;
    }
