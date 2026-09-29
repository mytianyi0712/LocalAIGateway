use std::time::Duration;

use anyhow::Result;
use parking_lot::RwLock;

use futures_util::StreamExt;

use crate::ports::{
    UpstreamBody, UpstreamClient, UpstreamError, UpstreamRequest, UpstreamResponse,
};

/// 按连接超时分档的 reqwest 客户端池。reqwest 不支持逐请求设置连接超时，
/// 因此运行时 `connect_timeout_seconds` 设置通过“选取按该超时构建的客户端”生效。
pub struct HttpClientPool {
    clients: RwLock<std::collections::HashMap<i64, reqwest::Client>>,
}

impl Default for HttpClientPool {
    fn default() -> Self {
        Self {
            clients: RwLock::new(std::collections::HashMap::new()),
        }
    }
}

impl HttpClientPool {
    /// 取（或构建）该连接超时对应的客户端。构建失败返回错误而不是 panic
    /// （release 构建 `panic=abort`，请求路径上的 panic 会杀掉整个进程；
    /// 例如 TLS 后端初始化失败就属于运行时可发生的情况）。
    pub fn for_connect_timeout(&self, seconds: i64) -> Result<reqwest::Client, reqwest::Error> {
        let seconds = seconds.max(1);
        if let Some(client) = self.clients.read().get(&seconds) {
            return Ok(client.clone());
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(seconds as u64))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(20)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        self.clients.write().insert(seconds, client.clone());
        Ok(client)
    }
}

impl UpstreamClient for HttpClientPool {
    fn send(
        &self,
        request: UpstreamRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<UpstreamResponse, UpstreamError>> {
        // 客户端构建失败按传输错误上报：与“连不上上游”同样是 502 语义，
        // 且不再从请求路径 panic。
        let client = match self.for_connect_timeout(request.connect_timeout.as_secs() as i64) {
            Ok(client) => client,
            Err(error) => {
                return Box::pin(async move {
                    Err(UpstreamError::Transport(format!(
                        "http client build failed: {error}"
                    )))
                });
            }
        };
        Box::pin(async move {
            let method = request.method;
            let mut builder = client.request(method, request.url).headers(request.headers);
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            let response = match tokio::time::timeout(request.deadline, builder.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    return Err(if error.is_timeout() {
                        UpstreamError::ConnectTimeout
                    } else {
                        UpstreamError::Transport(error.to_string())
                    });
                }
                Err(_) => return Err(UpstreamError::Deadline),
            };
            let status = response.status();
            let headers = response.headers().clone();
            let body =
                UpstreamBody::new(Box::pin(response.bytes_stream().map(|item| {
                    item.map_err(|error| UpstreamError::Transport(error.to_string()))
                })));
            Ok(UpstreamResponse {
                status,
                headers,
                body,
            })
        })
    }
}
