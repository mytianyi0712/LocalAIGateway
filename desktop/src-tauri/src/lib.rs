pub mod admin;
pub mod api_error;
pub mod application;
pub mod assets;
pub mod auth;
pub mod capabilities;
pub mod compression;
pub mod config;
pub mod controller;
pub mod convert;
pub mod crypto;
pub mod db;
pub mod discovery;
pub mod health;
pub mod infrastructure;
pub mod maintenance;
pub mod notification;
pub mod ports;
pub mod protocol;
pub mod proxy;
pub mod routing;
pub mod runtime;
pub mod server;
pub mod settings;
pub mod telemetry;

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

/// Registers or unregisters the app in the OS autostart mechanism
/// (Windows Run key, Linux autostart desktop entry, macOS LaunchAgent).
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

/// Minimizes the launcher window (custom titlebar button).
#[tauri::command]
fn minimize_window(window: tauri::WebviewWindow) -> Result<(), String> {
    window.minimize().map_err(|error| error.to_string())
}

/// Hides the launcher window to the tray (custom titlebar close button);
/// same semantics as closing via the window manager.
#[tauri::command]
fn close_to_tray(window: tauri::WebviewWindow) -> Result<(), String> {
    window.hide().map_err(|error| error.to_string())
}

/// Restores the main window to the foreground: show, unminimize, then focus.
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

/// Registers the system tray with its native menu (menu ids `show` and `quit`).
///
/// The tray keeps the app alive after the window is hidden and is the only
/// explicit way to stop the gateway: `quit` requests the controller stop and
/// exits the process. Linux requires the tray to carry a menu, which is
/// always attached here.
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
        // Left click restores the window; the menu still opens on right click.
        // (Unsupported on Linux, where the desktop shell owns click handling.)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| match event.id().as_ref() {
            TRAY_MENU_SHOW_ID => show_main_window(app),
            TRAY_MENU_QUIT_ID => {
                if let Some(controller) = app.try_state::<Arc<ServerController>>() {
                    // `stop` is internally bounded by the runtime's absolute
                    // shutdown deadline (P1-3): it drains in-flight requests
                    // and the telemetry tail, then aborts and joins anything
                    // stuck — so awaiting it can never hang the exit.
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
                // auto-launch (the autostart plugin backend) creates
                // ~/.config/autostart with `create_dir`, which fails with
                // ENOENT when ~/.config itself is missing (fresh profiles).
                // Pre-create the directory so enabling autostart cannot fail.
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
            // The window is created hidden (`visible: false`); show it now
            // unless the user opted to start minimized to the tray.
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
            // Closing the window only hides it: the gateway keeps running in the tray.
            // The controller is intentionally not stopped on this path.
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
                    // Explicit exit (tray `quit` -> `app.exit(code)`): stop the
                    // gateway, then let the exit proceed.
                    Some(_) => {
                        if let Some(controller) = app.try_state::<Arc<ServerController>>() {
                            controller.request_stop();
                        }
                    }
                    // Auto-exit raised because the last window was closed/destroyed:
                    // the gateway must keep running in the tray.
                    None => api.prevent_exit(),
                }
            }
        });
    Ok(())
}
