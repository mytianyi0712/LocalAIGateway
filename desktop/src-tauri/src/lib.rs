pub mod assets;
pub mod admin;
pub mod api_error;
pub mod auth;
pub mod config;
pub mod discovery;
pub mod health;
pub mod controller;
pub mod crypto;
pub mod db;
pub mod protocol;
pub mod routing;
pub mod settings;
pub mod telemetry;
pub mod server;

use std::sync::Arc;

use anyhow::Result;
use controller::{LauncherState, ServerController};
use tauri::{Manager, State};

#[tauri::command]
async fn launcher_state(controller: State<'_, Arc<ServerController>>) -> Result<LauncherState, String> {
    Ok(controller.state().await)
}

#[tauri::command]
async fn set_port(controller: State<'_, Arc<ServerController>>, port: u16) -> Result<(), String> {
    controller.inner().set_port(port).await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn toggle_server(controller: State<'_, Arc<ServerController>>) -> Result<(), String> {
    controller.inner().toggle().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn open_dashboard(controller: State<'_, Arc<ServerController>>) -> Result<(), String> {
    controller.open_dashboard().await.map_err(|error| error.to_string())
}

pub fn run_desktop() -> Result<()> {
    tauri::Builder::default()
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            let controller = tauri::async_runtime::block_on(ServerController::load(data_dir))?;
            app.manage(Arc::clone(&controller));
            tauri::async_runtime::spawn(async move {
                if let Err(error) = controller.start().await {
                    controller.record_error(error.to_string()).await;
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![launcher_state, set_port, toggle_server, open_dashboard])
        .build(tauri::generate_context!())?
        .run(|app, event| {
            if let tauri::RunEvent::ExitRequested { .. } = event {
                if let Some(controller) = app.try_state::<Arc<ServerController>>() {
                    controller.request_stop();
                }
            }
        });
    Ok(())
}
