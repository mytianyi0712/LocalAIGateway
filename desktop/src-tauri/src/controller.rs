use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{AppConfig, validate_port},
    server,
};

#[derive(Clone, Debug, Default)]
struct RuntimeStatus {
    running: bool,
    error: Option<String>,
}

struct RunningServer {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

pub struct ServerController {
    platform_data_dir: PathBuf,
    config: RwLock<AppConfig>,
    runtime: RwLock<RuntimeStatus>,
    server: Mutex<Option<RunningServer>>,
}

#[derive(Serialize)]
pub struct LauncherState {
    port: u16,
    url: String,
    running: bool,
    error: Option<String>,
    version: &'static str,
}

impl ServerController {
    pub async fn load(platform_data_dir: PathBuf) -> Result<Arc<Self>> {
        let config = AppConfig::load(&platform_data_dir).await?;
        Ok(Arc::new(Self {
            platform_data_dir,
            config: RwLock::new(config),
            runtime: RwLock::new(RuntimeStatus::default()),
            server: Mutex::new(None),
        }))
    }

    pub async fn state(&self) -> LauncherState {
        let config = self.config.read().await;
        let runtime = self.runtime.read().await;
        LauncherState {
            port: config.port,
            url: config.local_url(),
            running: runtime.running,
            error: runtime.error.clone(),
            version: env!("CARGO_PKG_VERSION"),
        }
    }
    pub async fn record_error(&self, error: impl Into<String>) {
        let mut runtime = self.runtime.write().await;
        runtime.running = false;
        runtime.error = Some(error.into());
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        let mut slot = self.server.lock().await;
        if slot.is_some() {
            return Ok(());
        }
        let config = self.config.read().await.clone();
        let router = server::build(config.clone()).await?;
        let listener = server::bind(&config).await?;
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let controller = Arc::clone(self);
        let task = tokio::spawn(async move {
            {
                let mut runtime = controller.runtime.write().await;
                runtime.running = true;
                runtime.error = None;
            }
            let result = server::serve(listener, router, task_cancel).await;
            let mut runtime = controller.runtime.write().await;
            runtime.running = false;
            if let Err(error) = result {
                runtime.error = Some(error.to_string());
            }
        });
        *slot = Some(RunningServer { cancel, task });
        Ok(())
    }

    pub async fn stop(&self) {
        let running = self.server.lock().await.take();
        if let Some(running) = running {
            running.cancel.cancel();
            let _ = running.task.await;
        }
        self.runtime.write().await.running = false;
    }

    pub fn request_stop(&self) {
        if let Ok(slot) = self.server.try_lock()
            && let Some(running) = slot.as_ref()
        {
            running.cancel.cancel();
        }
    }

    pub async fn set_port(self: &Arc<Self>, port: u16) -> Result<()> {
        validate_port(port)?;
        self.stop().await;
        {
            let mut config = self.config.write().await;
            config.port = port;
            config.save().await.context("无法保存端口配置")?;
        }
        if let Err(error) = self.start().await {
            self.runtime.write().await.error = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    pub async fn open_dashboard(&self) -> Result<()> {
        let state = self.state().await;
        if !state.running {
            anyhow::bail!("网关尚未运行");
        }
        open::that(&state.url).context("无法打开系统浏览器")
    }

    pub async fn start_to_tray(&self) -> bool {
        self.config.read().await.start_to_tray
    }

    pub async fn set_start_to_tray(&self, enabled: bool) -> Result<()> {
        let mut config = self.config.write().await;
        config.start_to_tray = enabled;
        config.save().await.context("无法保存启动选项")
    }

    pub fn platform_data_dir(&self) -> &PathBuf {
        &self.platform_data_dir
    }
}
