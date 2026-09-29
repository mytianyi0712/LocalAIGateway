//! 桌面入口 bin：Tauri 桌面外壳的可执行入口。
//!
//! 职责：`--version`/`-V` 短路打印版本号并退出；初始化 tracing 日志；随后调用
//! `run_desktop`，失败则打印错误并以退出码 1 结束。
//! 边界：仅负责进程启动引导；Tauri 应用、托盘与网关装配都在 `lib.rs`。
//! 关键不变量：`--version` 必须在初始化日志、启动 Tauri 之前返回。

// 桌面 bin 是 GUI 应用：缺了下面这行，Windows 会把 exe 构建成控制台子系统
// （CUI）程序，启动时弹出 cmd 窗口，关掉该控制台会连托盘一起杀掉整个进程。
// Debug 构建保留控制台以便看日志；无头 bin 不受影响（它有独立的 main）。
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    if std::env::args().any(|arg| arg == "--version" || arg == "-V") {
        println!("Local AI Gateway {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    if let Err(error) = local_ai_gateway::run_desktop() {
        eprintln!("Local AI Gateway failed: {error:#}");
        std::process::exit(1);
    }
}
