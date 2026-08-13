//! Desktop notification port implementation (P2-1): coalesced failover alerts.
//!
//! `DesktopNotifier` queues failover notices on an unbounded channel; a single
//! worker collects every notice that arrives within one `flush_window` (1s in
//! production) and renders them into ONE system notification. A network blip
//! that flips every channel in the pool therefore produces a single message
//! instead of a notification storm, while every distinct failover is still
//! reported — nothing is dropped by throttling, only batched.
//!
//! Delivery uses `notify-rust`: D-Bus `org.freedesktop.Notifications` on
//! Linux (implemented by GNOME/KDE/XFCE/...), toast on Windows, osascript on
//! macOS. When no notification daemon exists (headless server, missing D-Bus
//! session) the send fails and is logged at debug level — the gateway keeps
//! running, the alert is best-effort.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::ports::{FailoverNotice, Notifier};

/// Maximum failover entries rendered in one notification body.
const MAX_LINES: usize = 4;

/// Default collection window: failovers inside this span share one alert.
pub const DEFAULT_FLUSH_WINDOW: Duration = Duration::from_secs(1);

/// Renders and sends one coalesced notification.
type SendFn = Box<dyn Fn(&str, &str) + Send>;

pub struct DesktopNotifier {
    tx: mpsc::UnboundedSender<FailoverNotice>,
    // Keep the handle alive so the worker is never dropped mid-loop; the
    // worker exits on its own once the channel closes (all senders dropped).
    _worker: tokio::task::JoinHandle<()>,
}

impl DesktopNotifier {
    /// Spawns the coalescing worker. Must be called from a Tokio runtime.
    pub fn new(flush_window: Duration) -> Arc<Self> {
        Self::with_sender(flush_window, Box::new(send_notification))
    }

    /// Test seam: drive the same worker loop with a custom send function.
    fn with_sender(flush_window: Duration, send: SendFn) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(worker_loop(rx, flush_window, send));
        Arc::new(Self {
            tx,
            _worker: worker,
        })
    }
}

impl Notifier for DesktopNotifier {
    fn notify_failover(&self, notice: FailoverNotice) {
        // Unbounded channel: the request path never blocks or waits.
        let _ = self.tx.send(notice);
    }
}

/// Coalescing loop: receive the first notice, keep draining for one window,
/// flush the batch, repeat. On channel close (all senders dropped) the
/// remaining batch is flushed and the loop exits.
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
                    // Sender closed mid-window: flush what we have.
                    None => break,
                },
            }
        }
        send(&render_title(&batch), &render_body(&batch));
    }
}

/// Actual delivery on a blocking thread (D-Bus/toast round-trips must never
/// touch the async request path).
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
            // No daemon available (headless server, no D-Bus session): the
            // alert is best-effort, never surface it on the request path.
            tracing::debug!(error = %error, "failover notification not delivered");
        }
    });
}

/// Windows toast delivery. `POWERSHELL_APP_ID` is a registered AUMID that
/// every Windows 10/11 system has, so the toast renders without the gateway
/// being a packaged app.
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

/// One line per failover, truncated at `MAX_LINES` with a tail summary.
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

    /// Two notices inside one window produce exactly one notification
    /// carrying both entries; a later notice opens a second batch.
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
        // Single-entry batches carry no counter suffix.
        assert!(!sent[1].0.contains("×"));
        assert!(sent[1].1.contains("m3：e → f"));
    }

    /// Closing the channel mid-window still flushes the partial batch.
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
