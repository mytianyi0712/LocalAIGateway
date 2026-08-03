pub mod admin;
pub mod api_error;
pub mod assets;
pub mod auth;
pub mod config;
pub mod controller;
pub mod convert;
pub mod crypto;
pub mod db;
pub mod discovery;
pub mod health;
pub mod protocol;
pub mod proxy;
pub mod routing;
pub mod server;
pub mod settings;
pub mod telemetry;

use std::sync::Arc;

use anyhow::Result;
use controller::{LauncherState, ServerController};
use tauri::{
    Manager, State, WindowEvent,
    menu::{Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};

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
                    controller.request_stop();
                }
                app.exit(0);
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
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            let controller = tauri::async_runtime::block_on(ServerController::load(data_dir))?;
            app.manage(Arc::clone(&controller));
            build_tray(app.handle())?;
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
            if let WindowEvent::CloseRequested { api, .. } = event {
                if window.label() == MAIN_WINDOW_LABEL {
                    api.prevent_close();
                    if let Err(error) = window.hide() {
                        tracing::error!(?error, "failed to hide main window on close");
                    }
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            launcher_state,
            set_port,
            open_dashboard
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
