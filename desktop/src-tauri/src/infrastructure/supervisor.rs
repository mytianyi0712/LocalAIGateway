use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::Result;
use tokio_util::sync::CancellationToken;

/// 持有全部后台任务——一次性探测/discovery 与长驻监管者（health、maintenance、
/// 遥测 writer）——因此关闭是一次有界、可 join 的操作。
///
/// 注册在 `JoinSet` 锁内同步完成：任务要么在 `shutdown()` 开始排空前注册成功，
/// 要么以 [`ShuttingDown`] 被拒——不存在第二个未注册的 `tokio::spawn` 窗口
/// 让任务活过运行时。
pub struct RuntimeSupervisor {
    tasks: Arc<tokio::sync::Mutex<tokio::task::JoinSet<()>>>,
    pub cancel: CancellationToken,
    shutting_down: AtomicBool,
    failed_tasks: Arc<AtomicU64>,
    /// 当前正在运行（已 spawn、尚未结束或中止）的任务数。drop 守卫会递减它，
    /// 因此 deadline 排空期间被 abort 的任务也计入——
    /// `shutdown()` 之后 `active_task_count() == 0` 即证明没有任务存活。
    active_tasks: Arc<AtomicU64>,
}

/// 被包裹任务完成或中止时递减监管者的活动任务计数。守卫持在注册任务内部，
/// 因此超时中止（丢弃任务 future）也会释放计数。
struct ActiveTaskGuard(Arc<AtomicU64>);

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 一次性后台任务的结果，由监管者记录，使任何后台边界都不会无声结束。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Success,
    Failed,
    Cancelled,
}

/// `shutdown()` 已开始后由 [`RuntimeSupervisor::spawn`] 返回。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuttingDown;

impl std::fmt::Display for ShuttingDown {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "runtime is shutting down")
    }
}
impl std::error::Error for ShuttingDown {}

impl RuntimeSupervisor {
    pub fn new(cancel: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            tasks: Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
            cancel,
            shutting_down: AtomicBool::new(false),
            failed_tasks: Arc::new(AtomicU64::new(0)),
            active_tasks: Arc::new(AtomicU64::new(0)),
        })
    }

    /// 尚未完成或中止的已注册任务数。`shutdown()` 返回后它恒为 0——
    /// 即“没有任务存活”的排空证明。
    pub fn active_task_count(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed)
    }

    /// 统计以 [`TaskOutcome::Failed`] 结束的一次性任务：供静默失败审计的
    /// 持久化可观测信号。
    pub fn failed_task_count(&self) -> u64 {
        self.failed_tasks.load(Ordering::Relaxed)
    }

    /// 记录一个不在本监管者 `JoinSet` 内的任务（如 health 监管者内部的探测
    /// 任务）的失败。
    pub fn record_failure(&self) {
        self.failed_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// 注册一个后台任务。`shutdown()` 开始后拒绝（取 set 锁前后各检查一次，
    /// 避免任务溜进正在排空的运行时）。
    pub async fn spawn(
        self: &Arc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ShuttingDown> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let mut tasks = self.tasks.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let active = Arc::clone(&self.active_tasks);
        active.fetch_add(1, Ordering::Relaxed);
        tasks.spawn(async move {
            let _guard = ActiveTaskGuard(active);
            future.await;
        });
        Ok(())
    }

    /// 注册一个受跟踪的一次性任务：任务返回 [`TaskOutcome`]，监管者对每个
    /// 非成功结束都记录日志并计数，使任何后台边界都不会无声结束。
    pub async fn spawn_tracked(
        self: &Arc<Self>,
        name: &'static str,
        future: impl Future<Output = TaskOutcome> + Send + 'static,
    ) -> Result<(), ShuttingDown> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let mut tasks = self.tasks.lock().await;
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(ShuttingDown);
        }
        let failed = Arc::clone(&self.failed_tasks);
        let active = Arc::clone(&self.active_tasks);
        active.fetch_add(1, Ordering::Relaxed);
        tasks.spawn(async move {
            let _guard = ActiveTaskGuard(active);
            let outcome = future.await;
            if outcome != TaskOutcome::Success {
                tracing::warn!(task = name, outcome = ?outcome, "background task finished unsuccessfully");
                failed.fetch_add(1, Ordering::Relaxed);
            }
        });
        Ok(())
    }

    /// 回收已完成的一次性任务，避免 `JoinSet` 无界增长。收到取消信号时直接退出：
    /// 属主的 `shutdown()` 是唯一的 abort/排空权威，且它已持有 set 锁——
    /// 本任务在排空期间绝不可去争该锁（会死锁）。
    pub async fn reap_loop(self: Arc<Self>, interval: std::time::Duration) {
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => break,
                _ = tokio::time::sleep(interval) => {
                    let mut guard = self.tasks.lock().await;
                    while guard.try_join_next().is_some() {}
                }
            }
        }
    }

    /// 停止整个运行时：
    /// 1. 拒绝任何新注册，
    /// 2. 取消共享 token（serve、health、maintenance、writer 都会观察到），
    /// 3. 在绝对 `deadline` 内 join 所有任务，
    /// 4. 超时则 abort 所有剩余任务并再次 join。
    ///
    /// 仅在没有任务仍在运行时返回——调用方永不 detach。
    pub async fn shutdown(self: &Arc<Self>, deadline: std::time::Duration) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.cancel.cancel();
        let drained = tokio::time::timeout(deadline, async {
            let mut guard = self.tasks.lock().await;
            while guard.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!("shutdown deadline exceeded; aborting remaining tasks");
            let mut guard = self.tasks.lock().await;
            guard.shutdown().await;
        }
    }
}
