//! Tauri 桌面外壳 crate：把网关进程包装成带托盘与启动器的 GUI 应用。
//!
//! 职责：注册系统托盘与原生菜单、暴露启动器所需的 Tauri 命令（启动/停止/改端口/
//! 开机自启/最小化到托盘等）、在操作系统的自启动机制中登记应用。
//! 边界：只负责桌面壳层；网关的请求处理路径不在此 crate 的这一层，由 `server`/`proxy`
//! 等模块承担。
//! 关键不变量：关闭主窗口只隐藏窗口、网关随后台托盘继续运行，不停止 controller；
//! 只有托盘的“退出”或显式退出码才会真正停止进程。

pub mod admin;
pub mod api_error;
pub mod application;
pub mod assets;
pub mod auth;
pub mod balance;
pub mod capabilities;
pub mod commandcode;
pub mod commandcode_login;
pub mod compression;
pub mod config;
pub mod controller;
pub mod convert;
pub mod crypto;
pub mod db;
pub mod discovery;
pub mod domain;
pub mod health;
pub mod infrastructure;
pub mod maintenance;
pub mod notification;
pub mod ports;
pub mod protocol;
pub mod proxy;
pub mod remote_compaction;
pub mod routing;
pub mod runtime;
pub mod server;
pub mod settings;
pub mod state;
mod sse;
pub mod telemetry;
#[cfg(test)]
mod test_support;

use std::sync::Arc;

use anyhow::Result;
use controller::{LauncherState, ServerController};
use tauri::{
    AppHandle, Manager, State, WindowEvent,
    menu::{Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_autostart::ManagerExt;

const MAIN_WINDOW_LABEL: &str = "main";
const TRAY_MENU_SHOW_ID: &str = "show";
const TRAY_MENU_QUIT_ID: &str = "quit";

#[tauri::command]
async fn launcher_state(
    controller: State<'_, Arc<ServerController>>,
) -> Result<LauncherState, String> {
    Ok(controller.state().await)
}

#[tauri::command]
async fn set_port(controller: State<'_, Arc<ServerController>>, port: u16) -> Result<(), String> {
    controller
        .inner()
        .set_port(port)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn open_dashboard(controller: State<'_, Arc<ServerController>>) -> Result<(), String> {
    controller
        .open_dashboard()
        .await
        .map_err(|error| error.to_string())
}

#[derive(serde::Serialize)]
struct LauncherSettings {
    autostart_enabled: bool,
    start_to_tray: bool,
}

#[tauri::command]
fn launcher_settings(
    app: AppHandle,
    controller: State<'_, Arc<ServerController>>,
) -> Result<LauncherSettings, String> {
    let autostart_enabled = app.autolaunch().is_enabled().map_err(|e| e.to_string())?;
    Ok(LauncherSettings {
        autostart_enabled,
        start_to_tray: tauri::async_runtime::block_on(controller.start_to_tray()),
    })
}

/// 在操作系统的自启动机制中登记/取消登记本应用
/// （Windows 使用 Run 注册表键，Linux 使用 autostart desktop 条目，macOS 使用 LaunchAgent）。
#[tauri::command]
fn set_autostart(app: AppHandle, enabled: bool) -> Result<(), String> {
    let autolaunch = app.autolaunch();
    if enabled {
        autolaunch.enable()
    } else {
        autolaunch.disable()
    }
    .map_err(|error| error.to_string())
}

#[tauri::command]
async fn set_start_to_tray(
    controller: State<'_, Arc<ServerController>>,
    enabled: bool,
) -> Result<(), String> {
    controller
        .set_start_to_tray(enabled)
        .await
        .map_err(|error| error.to_string())
}

/// 最小化启动器窗口（自定义标题栏按钮）。
#[tauri::command]
fn minimize_window(window: tauri::WebviewWindow) -> Result<(), String> {
    window.minimize().map_err(|error| error.to_string())
}

/// 把启动器窗口隐藏到托盘（自定义标题栏关闭按钮）；
/// 语义与通过窗口管理器关闭一致。
#[tauri::command]
fn close_to_tray(window: tauri::WebviewWindow) -> Result<(), String> {
    window.hide().map_err(|error| error.to_string())
}

/// 把主窗口恢复到前台：先显示、再取消最小化，最后聚焦。
fn show_main_window(app: &tauri::AppHandle) {
    let window = match app.get_webview_window(MAIN_WINDOW_LABEL) {
        Some(window) => window,
        None => {
            let Some(config) = app
                .config()
                .app
                .windows
                .iter()
                .find(|config| config.label == MAIN_WINDOW_LABEL)
            else {
                tracing::error!(
                    label = MAIN_WINDOW_LABEL,
                    "tray: main window config not found"
                );
                return;
            };
            match tauri::WebviewWindowBuilder::from_config(app, config)
                .and_then(|builder| builder.build())
            {
                Ok(window) => window,
                Err(error) => {
                    tracing::error!(?error, "tray: failed to recreate main window");
                    return;
                }
            }
        }
    };
    if let Err(error) = window.show() {
        tracing::error!(?error, "tray: failed to show main window");
    }
    if let Err(error) = window.unminimize() {
        tracing::error!(?error, "tray: failed to unminimize main window");
    }
    if let Err(error) = window.set_focus() {
        tracing::error!(?error, "tray: failed to focus main window");
    }
}

/// 注册系统托盘及其原生菜单（菜单 id 为 `show` 与 `quit`）。
///
/// 窗口隐藏后托盘让应用保持存活，它也是停止网关的唯一显式入口：
/// `quit` 请求 controller 停止并退出进程。Linux 要求托盘必须带菜单，
/// 这里始终为其挂上菜单。
fn build_tray(app: &tauri::AppHandle) -> anyhow::Result<()> {
    let show = MenuItem::with_id(app, TRAY_MENU_SHOW_ID, "显示主界面", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, TRAY_MENU_QUIT_ID, "退出", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&show, &quit])?;
    let icon = app
        .default_window_icon()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("tray: no default window icon available"))?;

    TrayIconBuilder::with_id("main-tray")
        .icon(icon)
        .tooltip("Local AI Gateway")
        .menu(&menu)
        // 左键点击恢复窗口；右键仍会弹出菜单。
        // （Linux 上不支持：点击处理由桌面 shell 接管。）
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| match event.id().as_ref() {
            TRAY_MENU_SHOW_ID => show_main_window(app),
            TRAY_MENU_QUIT_ID => {
                if let Some(controller) = app.try_state::<Arc<ServerController>>() {
                    // `stop` 在内部受运行时绝对关闭截止时间约束：它会先排空
                    // 在途请求与 telemetry 尾部数据，然后中止并 join 掉卡住的
                    // 任务——因此 await 它不可能挂住退出流程。
                    let controller = Arc::clone(&controller);
                    let handle = app.clone();
                    tauri::async_runtime::spawn(async move {
                        controller.stop().await;
                        handle.exit(0);
                    });
                } else {
                    app.exit(0);
                }
            }
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                show_main_window(tray.app_handle());
            }
        })
        .build(app)?;
    Ok(())
}

pub fn run_desktop() -> Result<()> {
    tauri::Builder::default()
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            None,
        ))
        .setup(|app| {
            #[cfg(target_os = "linux")]
            {
                // auto-launch（autostart 插件的后端）用 `create_dir` 创建
                // ~/.config/autostart，当 ~/.config 本身不存在（全新用户配置）时
                // 会以 ENOENT 失败。预先建好该目录，确保启用自启动不会失败。
                if let Some(home) = std::env::var_os("HOME") {
                    let _ = std::fs::create_dir_all(
                        std::path::PathBuf::from(home)
                            .join(".config")
                            .join("autostart"),
                    );
                }
            }
            let data_dir = app.path().app_data_dir()?;
            let controller = tauri::async_runtime::block_on(ServerController::load(data_dir))?;
            app.manage(Arc::clone(&controller));
            build_tray(app.handle())?;
            // 窗口是以隐藏方式创建的（`visible: false`）；除非用户选择
            // 最小化到托盘启动，否则现在把它显示出来。
            let start_to_tray = tauri::async_runtime::block_on(controller.start_to_tray());
            if !start_to_tray
                && let Some(window) = app.get_webview_window(MAIN_WINDOW_LABEL)
                && let Err(error) = window.show()
            {
                tracing::error!(?error, "failed to show main window");
            }
            tauri::async_runtime::spawn(async move {
                if let Err(error) = controller.start().await {
                    controller.record_error(error.to_string()).await;
                }
            });
            Ok(())
        })
        .on_window_event(|window, event| {
            // 关闭窗口只是隐藏它：网关继续在托盘里运行。
            // 此路径上有意不停止 controller。
            if let WindowEvent::CloseRequested { api, .. } = event
                && window.label() == MAIN_WINDOW_LABEL
            {
                api.prevent_close();
                if let Err(error) = window.hide() {
                    tracing::error!(?error, "failed to hide main window on close");
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            launcher_state,
            set_port,
            open_dashboard,
            launcher_settings,
            set_autostart,
            set_start_to_tray,
            minimize_window,
            close_to_tray
        ])
        .build(tauri::generate_context!())?
        .run(|app, event| {
            if let tauri::RunEvent::ExitRequested { code, api, .. } = event {
                match code {
                    // 显式退出（托盘 `quit` -> `app.exit(code)`）：停止网关，
                    // 然后让退出继续。
                    Some(_) => {
                        if let Some(controller) = app.try_state::<Arc<ServerController>>() {
                            controller.request_stop();
                        }
                    }
                    // 因最后一个窗口被关闭/销毁而触发的自动退出：
                    // 网关必须继续在托盘里运行。
                    None => api.prevent_exit(),
                }
            }
        });
    Ok(())
}
