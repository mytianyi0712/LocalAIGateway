//! 桌面通知端口实现：把一次窗口内的故障转移合并成一条系统通知。
//!
//! 职责：`DesktopNotifier` 把故障转移通知投递到无界 channel，单个 worker 收集
//! 一个 `flush_window`（生产 1 秒）内到达的全部通知并合并渲染成一条系统通知。
//! 边界：仅做桌面通知；无通知守护进程（无头服务器、缺少 D-Bus 会话）时发送失败
//! 只记 debug 日志，告警是尽力而为，绝不影响请求路径。
//! 不变量：无界 channel 保证投递永不阻塞请求路径；限流只合并不丢弃；worker 在
//! 生产路径由 `RuntimeSupervisor` 持有，测试/独立使用才用裸 task。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::ports::{FailoverNotice, Notifier};

/// 一条通知正文中最多渲染的故障转移条目数。
const MAX_LINES: usize = 4;

/// 默认收集窗口：该时间跨度内的故障转移共用一个告警。
pub const DEFAULT_FLUSH_WINDOW: Duration = Duration::from_secs(1);

/// 渲染并发送一条合并后的通知。
type SendFn = Box<dyn Fn(&str, &str) + Send>;

pub struct DesktopNotifier {
    tx: mpsc::UnboundedSender<FailoverNotice>,
    /// 生产路径由 [`crate::infrastructure::RuntimeSupervisor`] 持有 worker，
    /// 因此这里是 `None`；测试与独立使用场景用 [`Self::new`] 的裸 task，句柄
    /// 留在这里只为让 worker 不被提前 drop（channel 关闭后它会自行退出）。
    _worker: Option<tokio::task::JoinHandle<()>>,
}

impl DesktopNotifier {
    /// 起一个裸 worker task（测试/独立使用）。必须处于 Tokio 运行时内。
    pub fn new(flush_window: Duration) -> Arc<Self> {
        Self::with_sender(flush_window, Box::new(send_notification))
    }

    /// 构造并把 worker **注册进运行时监督器**：任务由 supervisor 持有，
    /// `shutdown` 会等到它真正结束，不会留下一个无人等待的后台任务。
    /// 已经进入关停流程时返回 [`crate::infrastructure::ShuttingDown`]。
    pub async fn registered(
        flush_window: Duration,
        supervisor: &Arc<crate::infrastructure::RuntimeSupervisor>,
    ) -> Result<Arc<Self>, crate::infrastructure::ShuttingDown> {
        let (tx, rx) = mpsc::unbounded_channel();
        supervisor
            .spawn(async move {
                worker_loop(rx, flush_window, Box::new(send_notification)).await;
            })
            .await?;
        Ok(Arc::new(Self { tx, _worker: None }))
    }

    /// 测试接缝：用自定义的发送函数驱动同一套 worker 循环。
    fn with_sender(flush_window: Duration, send: SendFn) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(worker_loop(rx, flush_window, send));
        Arc::new(Self {
            tx,
            _worker: Some(worker),
        })
    }
}

impl Notifier for DesktopNotifier {
    fn notify_failover(&self, notice: FailoverNotice) {
        // 无界 channel：请求路径永不阻塞或等待。
        let _ = self.tx.send(notice);
    }
}

/// 合并循环：收到第一条通知后，在一个窗口内持续排空，然后 flush 该批次，如此
/// 往复。channel 关闭（所有发送端 drop）时，flush 剩余批次后退出循环。
async fn worker_loop<F>(mut rx: mpsc::UnboundedReceiver<FailoverNotice>, window: Duration, send: F)
where
    F: Fn(&str, &str) + Send + 'static,
{
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        let deadline = tokio::time::sleep(window);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => break,
                next = rx.recv() => match next {
                    Some(notice) => batch.push(notice),
                    // 窗口中途发送端关闭：把已收到的内容 flush 出去。
                    None => break,
                },
            }
        }
        send(&render_title(&batch), &render_body(&batch));
    }
}

/// 在阻塞线程上实际投递（D-Bus/toast 往返绝不能触碰异步请求路径）。
#[cfg(not(target_os = "windows"))]
fn send_notification(title: &str, body: &str) {
    let title = title.to_owned();
    let body = body.to_owned();
    tokio::task::spawn_blocking(move || {
        let result = notify_rust::Notification::new()
            .summary(&title)
            .body(&body)
            .appname("Local AI Gateway")
            .show();
        if let Err(error) = result {
            // 没有可用的通知守护进程（无头服务器、无 D-Bus 会话）：告警是尽力而为
            // 的，绝不能让它出现在请求路径上。
            tracing::debug!(error = %error, "failover notification not delivered");
        }
    });
}

/// Windows toast 投递。`POWERSHELL_APP_ID` 是每个 Windows 10/11 系统都注册的
/// AUMID，因此即使网关不是打包应用，toast 也能正常渲染。
#[cfg(target_os = "windows")]
fn send_notification(title: &str, body: &str) {
    let title = title.to_owned();
    let body = body.to_owned();
    tokio::task::spawn_blocking(move || {
        use winrt_notification::{Duration, Toast};
        let result = Toast::new(Toast::POWERSHELL_APP_ID)
            .title(&title)
            .text1(&body)
            .duration(Duration::Short)
            .show();
        if let Err(error) = result {
            tracing::debug!(error = %error, "failover notification not delivered");
        }
    });
}

fn render_title(entries: &[FailoverNotice]) -> String {
    if entries.len() == 1 {
        "Local AI Gateway：故障转移".to_owned()
    } else {
        format!("Local AI Gateway：故障转移 ×{}", entries.len())
    }
}

/// 每次故障转移一行，超过 `MAX_LINES` 截断并附一行尾部汇总。
fn render_body(entries: &[FailoverNotice]) -> String {
    let mut lines: Vec<String> = entries
        .iter()
        .map(|entry| {
            let error = entry.error_kind.as_deref().unwrap_or("unknown error");
            format!(
                "{}：{} → {}（{}）",
                entry.model_id, entry.failed_channel_name, entry.next_channel_name, error
            )
        })
        .collect();
    if lines.len() > MAX_LINES {
        lines.truncate(MAX_LINES);
        lines.push(format!("…等 {} 条", entries.len()));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    fn notice(model: &str, failed: &str, next: &str, error: Option<&str>) -> FailoverNotice {
        FailoverNotice {
            model_id: model.to_owned(),
            failed_channel_name: failed.to_owned(),
            next_channel_name: next.to_owned(),
            error_kind: error.map(str::to_owned),
        }
    }

    /// `registered()` 的 worker 归监督器所有——计数 +1、shutdown 后归零。
    #[tokio::test]
    async fn registered_worker_is_owned_by_the_supervisor() {
        let supervisor = Arc::new(crate::infrastructure::RuntimeSupervisor::new(
            tokio_util::sync::CancellationToken::new(),
        ));
        let notifier = DesktopNotifier::registered(Duration::from_millis(20), &supervisor)
            .await
            .expect("supervisor accepts tasks before shutdown");
        assert_eq!(
            supervisor.active_task_count(),
            1,
            "the notifier worker must be visible to the supervisor"
        );
        // 释放发送端 → worker 的 channel 关闭 → worker 自行退出。
        drop(notifier);
        supervisor
            .shutdown(Duration::from_secs(2))
            .await;
        assert_eq!(supervisor.active_task_count(), 0);
    }

    #[test]
    fn single_entry_body_has_no_counter() {
        let entries = vec![notice("m1", "a", "b", Some("connect_timeout"))];
        assert_eq!(render_title(&entries), "Local AI Gateway：故障转移");
        assert_eq!(render_body(&entries), "m1：a → b（connect_timeout）");
    }

    #[test]
    fn unknown_error_kind_renders_fallback() {
        let entries = vec![notice("m1", "a", "b", None)];
        assert_eq!(render_body(&entries), "m1：a → b（unknown error）");
    }

    #[test]
    fn many_entries_truncate_with_tail_summary() {
        let entries: Vec<FailoverNotice> = (0..6)
            .map(|i| notice(&format!("m{i}"), "a", "b", Some("timeout")))
            .collect();
        assert_eq!(render_title(&entries), "Local AI Gateway：故障转移 ×6");
        let body = render_body(&entries);
        assert_eq!(body.lines().count(), MAX_LINES + 1);
        assert!(body.ends_with("…等 6 条"));
    }

    /// 同一窗口内的两条通知只产生一条通知、同时携带两条条目；
    /// 之后到达的通知会开启第二个批次。
    #[tokio::test]
    async fn window_coalesces_and_new_window_flushes() {
        let sent: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&sent);
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(worker_loop(
            rx,
            Duration::from_millis(30),
            move |title, body| {
                recorder.lock().push((title.to_owned(), body.to_owned()));
            },
        ));
        tx.send(notice("m1", "a", "b", Some("timeout"))).unwrap();
        tx.send(notice("m2", "c", "d", None)).unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        tx.send(notice("m3", "e", "f", Some("HTTP 500"))).unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(tx);
        let _ = worker.await;

        let sent = sent.lock();
        assert_eq!(sent.len(), 2, "two windows -> two notifications");
        assert!(sent[0].0.contains("×2"));
        assert!(sent[0].1.contains("m1：a → b") && sent[0].1.contains("m2：c → d"));
        // 单条目批次不带计数后缀。
        assert!(!sent[1].0.contains("×"));
        assert!(sent[1].1.contains("m3：e → f"));
    }

    /// 窗口尚未结束就关闭 channel，仍会 flush 出这批不完整的批次。
    #[tokio::test]
    async fn channel_close_flushes_partial_batch() {
        let sent: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&sent);
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(worker_loop(
            rx,
            Duration::from_millis(10_000),
            move |title, body| {
                recorder.lock().push((title.to_owned(), body.to_owned()));
            },
        ));
        tx.send(notice("m1", "a", "b", None)).unwrap();
        drop(tx); // close before the window elapses
        let _ = worker.await;

        let sent = sent.lock();
        assert_eq!(sent.len(), 1, "partial batch must still be flushed");
        assert!(sent[0].1.contains("m1：a → b"));
    }
}
