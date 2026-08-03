use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use fernet::Fernet;
use tokio::fs;

#[derive(Clone)]
pub struct SecretStore {
    cipher: Fernet,
}

impl SecretStore {
    pub async fn load(path: &Path) -> Result<Self> {
        let key = load_or_create_key(path.to_path_buf()).await?;
        let key = String::from_utf8(key).context("主密钥不是有效的 UTF-8 Fernet key")?;
        let cipher = Fernet::new(key.trim()).context("主密钥不是有效的 Fernet key")?;
        Ok(Self { cipher })
    }

    pub fn encrypt(&self, value: &str) -> Vec<u8> {
        self.cipher.encrypt(value.as_bytes()).into_bytes()
    }

    pub fn decrypt(&self, value: &[u8]) -> Result<String> {
        let token = std::str::from_utf8(value).context("密钥密文不是 UTF-8")?;
        let plain = self
            .cipher
            .decrypt(token)
            .context("无法解密已保存的 API Key")?;
        String::from_utf8(plain).context("API Key 解密结果不是 UTF-8")
    }

    pub fn hint(value: &str) -> String {
        let suffix: String = value
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        if value.chars().count() >= 4 {
            format!("...{suffix}")
        } else {
            "...".into()
        }
    }
}

async fn load_or_create_key(path: PathBuf) -> Result<Vec<u8>> {
    match fs::read(&path).await {
        Ok(key) => {
            secure_permissions(&path).await?;
            return Ok(key);
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("无法读取主密钥 {}", path.display()));
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let key = Fernet::generate_key();
    let temporary = path.with_extension("key.tmp");
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await
    {
        Ok(mut file) => {
            use tokio::io::AsyncWriteExt;
            file.write_all(key.as_bytes()).await?;
            file.flush().await?;
            drop(file);
            secure_permissions(&temporary).await?;
            fs::rename(&temporary, &path).await?;
            secure_permissions(&path).await?;
            Ok(key.into_bytes())
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temporary).await;
            if path.exists() {
                fs::read(&path).await.map_err(Into::into)
            } else {
                bail!("主密钥初始化冲突")
            }
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
async fn secure_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    Ok(())
}

#[cfg(not(unix))]
async fn secure_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_never_exposes_short_keys() {
        assert_eq!(SecretStore::hint("abc"), "...");
        assert_eq!(SecretStore::hint("secret-value"), "...alue");
    }
}
