use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::fs;

pub const DEFAULT_PORT: u16 = 3000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(skip)]
    pub data_dir: PathBuf,
}

fn default_host() -> String {
    "0.0.0.0".into()
}
const fn default_port() -> u16 {
    DEFAULT_PORT
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: DEFAULT_PORT,
            data_dir: PathBuf::from("data"),
        }
    }
}

impl AppConfig {
    pub async fn load(platform_data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = resolve_data_dir(platform_data_dir.as_ref());
        fs::create_dir_all(&data_dir)
            .await
            .with_context(|| format!("无法创建数据目录 {}", data_dir.display()))?;
        let path = data_dir.join("launcher.json");
        let mut config = if path.exists() {
            let bytes = fs::read(&path)
                .await
                .with_context(|| format!("无法读取 {}", path.display()))?;
            serde_json::from_slice::<Self>(&bytes).context("launcher.json 格式错误")?
        } else {
            Self::default()
        };
        if let Ok(host) = env::var("AI_GATEWAY_HOST") {
            config.host = host;
        }
        if let Ok(value) = env::var("AI_GATEWAY_PORT") {
            config.port = validate_port(value.parse().context("AI_GATEWAY_PORT 必须是端口号")?)?;
        }
        config.data_dir = data_dir;
        Ok(config)
    }

    pub async fn save(&self) -> Result<()> {
        validate_port(self.port)?;
        fs::create_dir_all(&self.data_dir).await?;
        let path = self.data_dir.join("launcher.json");
        let temporary = self.data_dir.join("launcher.json.tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(self)?).await?;
        fs::rename(&temporary, &path).await?;
        Ok(())
    }

    pub fn database_path(&self) -> PathBuf {
        self.data_dir.join("gateway.db")
    }
    pub fn key_path(&self) -> PathBuf {
        self.data_dir.join("master.key")
    }
    pub fn local_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

fn resolve_data_dir(platform_data_dir: &Path) -> PathBuf {
    if let Some(value) = env::var_os("AI_GATEWAY_DATA_DIR") {
        return PathBuf::from(value);
    }
    let legacy = PathBuf::from("data");
    if legacy.join("gateway.db").exists() || legacy.join("master.key").exists() {
        legacy
    } else {
        platform_data_dir.to_path_buf()
    }
}

pub fn validate_port(port: u16) -> Result<u16> {
    if port < 1024 {
        bail!("端口必须在 1024–65535 之间");
    }
    Ok(port)
}
