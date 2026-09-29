//! 渠道余额 / 用量查询旁路。
//!
//! 余额是可选启用的逐渠道旁路：`channel_balance_configs` 无记录的渠道不发任何
//! 上游余额请求；用户从内置适配器（当前 14 种，另有 `custom` 模板）中选一种并
//! 开启后，maintenance supervisor 默认每小时刷新已启用渠道；Command Code 集成
//! 启用时按 `command_code_quota_interval_minutes`（默认 15 分钟）缩短。
//! 管理 API 也可手动查询单个渠道。
//! 边界与不变量：查询是旁路、失败绝不触碰代理主路径，只写 `status=error` +
//! 稳定 `error_kind` 的快照；快照不存原始响应体与 token；事务内不发网络请求。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use futures_util::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::Url;

use crate::{
    api_error::ApiError,
    application::Context,
    crypto::SecretStore,
    db::Database,
    ports::{ChannelRepository, ChannelRow, Clock, UpstreamClient, UpstreamError, UpstreamRequest},
    protocol,
    runtime::RuntimeLimits,
};

/// New API 的额度以每 USD 500,000 单位计。
pub const NEW_API_QUOTA_PER_USD: f64 = 500_000.0;

/// New API 账户余额端点，需仪表盘 Personal Access Token 才能访问
/// （渠道的 sk- key 只能访问 token 作用域的用量）。
const NEW_API_ACCOUNT_PATH: &str = "/api/user/self";

/// 小米 MiMo 控制台（Token Plan 没有 API Key 查询路径，只有账号 Cookie 可用）。
const MIMO_CONSOLE_URL: &str = "https://platform.xiaomimimo.com";
/// 控制台接口前的浏览器 UA；缺省 UA 会被网关拒绝。
const MIMO_USER_AGENT: &str = "Mozilla/5.0";
/// OpenRouter 控制面主机（账户余额与 key 额度都在它下面）。
const OPENROUTER_API_URL: &str = "https://openrouter.ai";
/// Novita AI 用量/余额主机的固定域名（余额单位是 0.0001 USD）。
const NOVITA_BALANCE_URL: &str = "https://api.novita.ai/v3/user/balance";
/// Novita 的 `availableBalance`/`cashBalance` 等字段以 0.0001 USD 计。
const NOVITA_UNITS_PER_USD: f64 = 10_000.0;
/// MiniMax Coding Plan 额度接口（官方域名是唯一公开出处）。
const MINIMAX_REMAINS_URL: &str =
    "https://api.minimaxi.com/v1/api/openplatform/coding_plan/remains";
/// Kimi For Coding 额度接口（官方域名是唯一公开出处）。
const KIMI_USAGES_URL: &str = "https://api.kimi.com/coding/v1/usages";

/// 余额响应很小；像其它旁路一样给响应体设上限。
const BALANCE_BODY_MAX: usize = 256 * 1024;
/// 自定义请求模板的上限（保存时校验）。
const MAX_CUSTOM_HEADERS: usize = 8;
const MAX_CUSTOM_BODY_BYTES: usize = 16 * 1024;
const MAX_CUSTOM_PATH_CHARS: usize = 1024;
const MAX_MAPPING_CHARS: usize = 256;
/// 后台刷新与批量刷新的并发上限。
const REFRESH_CONCURRENCY: usize = 4;

/// 稳定的失败分类（存入 `channel_balance_snapshots.error_kind`）。
/// 除 [`DISABLED`] 外均为上游/传输失败分类；`DISABLED` 是本地闸门，
/// 表示 Command Code 集成被全局关闭、根本没有发起上游请求。
pub mod error_kind {
    pub const HTTP_401: &str = "http_401";
    pub const HTTP_403: &str = "http_403";
    pub const HTTP_5XX: &str = "http_5xx";
    pub const HTTP_ERROR: &str = "http_error";
    pub const TRANSPORT_ERROR: &str = "transport_error";
    pub const TIMEOUT: &str = "timeout";
    pub const INVALID_PAYLOAD: &str = "invalid_payload";
    pub const CUSTOM_PATH_MISSING: &str = "custom_path_missing";
    /// Command Code 集成被全局关闭（`command_code_enabled`），未发起任何上游请求。
    pub const DISABLED: &str = "disabled";
}

/// 管理 handler 用的稳定服务级失败。
#[derive(Debug)]
pub enum BalanceError {
    /// 对未保存适配器的渠道手动查询（默认关闭）。
    NotConfigured,
    ChannelNotFound,
    /// 用户输入非法（422）。
    Invalid(String),
    Internal(anyhow::Error),
}

impl std::fmt::Display for BalanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BalanceError::NotConfigured => write!(formatter, "balance_not_configured"),
            BalanceError::ChannelNotFound => write!(formatter, "Channel not found"),
            BalanceError::Invalid(message) => write!(formatter, "{message}"),
            BalanceError::Internal(error) => write!(formatter, "{error:#}"),
        }
    }
}

impl std::error::Error for BalanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BalanceError::Internal(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for BalanceError {
    fn from(error: anyhow::Error) -> Self {
        BalanceError::Internal(error)
    }
}

impl From<sqlx::Error> for BalanceError {
    fn from(error: sqlx::Error) -> Self {
        BalanceError::Internal(error.into())
    }
}

impl From<serde_json::Error> for BalanceError {
    fn from(error: serde_json::Error) -> Self {
        BalanceError::Internal(error.into())
    }
}

impl From<BalanceError> for ApiError {
    fn from(error: BalanceError) -> Self {
        match error {
            BalanceError::NotConfigured => ApiError::balance_not_configured(),
            BalanceError::ChannelNotFound => ApiError::not_found("Channel not found"),
            BalanceError::Invalid(message) => ApiError::validation(message),
            BalanceError::Internal(error) => ApiError::internal(error),
        }
    }
}

/// 支持的适配器。序列化名即持久化值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BalanceAdapter {
    #[serde(rename = "newapi")]
    NewApi,
    #[serde(rename = "sub2api")]
    Sub2Api,
    #[serde(rename = "opencode_go")]
    OpencodeGo,
    #[serde(rename = "deepseek")]
    DeepSeek,
    /// Command Code Go 四步只读查询：`whoami` → `billing/credits` → `usage/summary`
    /// → `billing/subscriptions`（四个只读 GET，不产生生成请求）。
    #[serde(rename = "command_code")]
    CommandCode,
    /// 小米 MiMo Token Plan：控制台 Cookie，`tokenPlan/usage` +
    /// `balance`（可选 `tokenPlan/detail`）。
    #[serde(rename = "mimo")]
    Mimo,
    /// OpenRouter：Management Key 走 `/credits`，普通 key 走 `/key`。
    #[serde(rename = "openrouter")]
    OpenRouter,
    /// SiliconFlow `/v1/user/info`（币种随站点域名而定）。
    #[serde(rename = "siliconflow")]
    SiliconFlow,
    /// StepFun 账户余额（`/v1/accounts`）；Step Plan 的月度额度池没有公开端点。
    #[serde(rename = "stepfun")]
    StepFun,
    /// Novita AI `/v3/user/balance`（单位 0.0001 USD）。
    #[serde(rename = "novita")]
    Novita,
    /// Moonshot Kimi 开放平台 `/v1/users/me/balance`。
    #[serde(rename = "moonshot")]
    Moonshot,
    /// 智谱 GLM Coding Plan 额度窗口（`/api/monitor/usage/quota/limit`，
    /// 裸 token——智谱不加 `Bearer` 前缀）。
    #[serde(rename = "zhipu")]
    ZhipuGlm,
    /// MiniMax Coding Plan 剩余额度（5h / 每周百分比窗口）。
    #[serde(rename = "minimax")]
    MiniMax,
    /// Kimi For Coding `/coding/v1/usages`（5h / 每周窗口）。
    #[serde(rename = "kimi_code")]
    KimiCode,
    #[serde(rename = "custom")]
    Custom,
}

impl BalanceAdapter {
    pub fn parse(value: &str) -> Result<Self, BalanceError> {
        match value.trim() {
            "newapi" => Ok(BalanceAdapter::NewApi),
            "sub2api" => Ok(BalanceAdapter::Sub2Api),
            "opencode_go" => Ok(BalanceAdapter::OpencodeGo),
            "deepseek" => Ok(BalanceAdapter::DeepSeek),
            "command_code" => Ok(BalanceAdapter::CommandCode),
            "mimo" => Ok(BalanceAdapter::Mimo),
            "openrouter" => Ok(BalanceAdapter::OpenRouter),
            "siliconflow" => Ok(BalanceAdapter::SiliconFlow),
            "stepfun" => Ok(BalanceAdapter::StepFun),
            "novita" => Ok(BalanceAdapter::Novita),
            "moonshot" => Ok(BalanceAdapter::Moonshot),
            "zhipu" => Ok(BalanceAdapter::ZhipuGlm),
            "minimax" => Ok(BalanceAdapter::MiniMax),
            "kimi_code" => Ok(BalanceAdapter::KimiCode),
            "custom" => Ok(BalanceAdapter::Custom),
            other => Err(BalanceError::Invalid(format!(
                "不支持的余额适配器：{other}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BalanceAdapter::NewApi => "newapi",
            BalanceAdapter::Sub2Api => "sub2api",
            BalanceAdapter::OpencodeGo => "opencode_go",
            BalanceAdapter::DeepSeek => "deepseek",
            BalanceAdapter::CommandCode => "command_code",
            BalanceAdapter::Mimo => "mimo",
            BalanceAdapter::OpenRouter => "openrouter",
            BalanceAdapter::SiliconFlow => "siliconflow",
            BalanceAdapter::StepFun => "stepfun",
            BalanceAdapter::Novita => "novita",
            BalanceAdapter::Moonshot => "moonshot",
            BalanceAdapter::ZhipuGlm => "zhipu",
            BalanceAdapter::MiniMax => "minimax",
            BalanceAdapter::KimiCode => "kimi_code",
            BalanceAdapter::Custom => "custom",
        }
    }

    /// 每个内置适配器保存的预设端点（`custom` 没有）。
    pub fn default_path(self) -> Option<&'static str> {
        match self {
            BalanceAdapter::NewApi => Some("/api/usage/token/"),
            BalanceAdapter::Sub2Api => Some("/v1/usage"),
            BalanceAdapter::OpencodeGo => Some("v1/usage"),
            BalanceAdapter::DeepSeek => Some("/user/balance"),
            // 多步流程（whoami → credits → summary）；对适配器而言存储的 path
            // 未被使用。
            BalanceAdapter::CommandCode => None,
            // 多步控制台流程（Cookie + 三个 GET）。
            BalanceAdapter::Mimo => None,
            // 多步（`/credits`，回退到 `/key`）。
            BalanceAdapter::OpenRouter => None,
            BalanceAdapter::SiliconFlow => Some("/v1/user/info"),
            BalanceAdapter::StepFun => Some("/v1/accounts"),
            BalanceAdapter::Novita => Some(NOVITA_BALANCE_URL),
            BalanceAdapter::Moonshot => Some("/v1/users/me/balance"),
            BalanceAdapter::ZhipuGlm => Some("/api/monitor/usage/quota/limit"),
            BalanceAdapter::MiniMax => Some(MINIMAX_REMAINS_URL),
            BalanceAdapter::KimiCode => Some(KIMI_USAGES_URL),
            BalanceAdapter::Custom => None,
        }
    }
}

/// 余额请求的鉴权模式。内置适配器用 `bearer`（智谱控制台用裸 token，不加
/// 前缀）；`none` 供只用头模板鉴权的自定义端点使用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceAuth {
    Bearer,
    /// token 原样写入 `Authorization`（不加 `Bearer ` 前缀）。
    Raw,
    None,
}

impl BalanceAuth {
    pub fn parse(value: &str) -> Result<Self, BalanceError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "bearer" => Ok(BalanceAuth::Bearer),
            "raw" => Ok(BalanceAuth::Raw),
            "none" => Ok(BalanceAuth::None),
            other => Err(BalanceError::Invalid(format!("不支持的鉴权方式：{other}"))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            BalanceAuth::Bearer => "bearer",
            BalanceAuth::Raw => "raw",
            BalanceAuth::None => "none",
        }
    }
}

/// 一个用量窗口（OpenCode Go 的滚动 / 每周 / 每月）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotaWindow {
    pub label: String,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub resets_at: Option<String>,
}

/// 单个适配器解析器的归一化结果。
#[derive(Debug, Clone, Default)]
pub struct BalanceReading {
    pub remaining: Option<f64>,
    pub currency: Option<String>,
    pub used: Option<f64>,
    pub total: Option<f64>,
    pub unlimited: bool,
    pub label: Option<String>,
    pub windows: Vec<QuotaWindow>,
    pub detail: Value,
}

/// 用户为 `custom` 适配器配置的余额字段映射。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BalanceMapping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// 存储的配置。`adapter=None` 表示渠道没有配置行，不得发出任何余额请求。
#[derive(Debug, Clone)]
pub struct BalanceConfig {
    pub adapter: Option<BalanceAdapter>,
    pub enabled: bool,
    pub method: String,
    pub path: Option<String>,
    pub auth: BalanceAuth,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
    pub mapping: BalanceMapping,
    pub has_token: bool,
    pub token_hint: Option<String>,
    /// 密文只被 `prepare` 使用，绝不序列化。
    token_encrypted: Option<Vec<u8>>,
}

impl Default for BalanceConfig {
    fn default() -> Self {
        Self {
            adapter: None,
            enabled: false,
            method: "GET".into(),
            path: None,
            auth: BalanceAuth::Bearer,
            headers: Vec::new(),
            body: None,
            mapping: BalanceMapping::default(),
            has_token: false,
            token_hint: None,
            token_encrypted: None,
        }
    }
}

impl BalanceConfig {
    /// 管理端 JSON 视图：除 token 密文外的全部字段。
    pub fn to_json(&self) -> Value {
        let headers: BTreeMap<&str, &str> = self
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        json!({
            "configured": self.adapter.is_some(),
            "adapter": self.adapter.map(BalanceAdapter::as_str),
            "enabled": self.enabled,
            "method": self.method,
            "path": self.path,
            "auth": self.auth.as_str(),
            "headers": headers,
            "body": self.body,
            "mapping": self.mapping,
            "has_token": self.has_token,
            "token_hint": self.token_hint,
        })
    }
}

/// [`BalanceConfigInput`] 内的 JSON 路径映射。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BalanceMappingInput {
    #[serde(default)]
    pub remaining: Option<String>,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub used: Option<String>,
    #[serde(default)]
    pub total: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
}

/// `PUT /channels/{id}/balance-config` 的请求体。
#[derive(Debug, Clone, Deserialize)]
pub struct BalanceConfigInput {
    pub adapter: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub auth: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub mapping: BalanceMappingInput,
    /// 新的专用 token。空/缺省字符串保留原 token；删除需 `clear_token: true`。
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub clear_token: bool,
}

/// 某渠道的最新快照。
#[derive(Debug, Clone, Serialize)]
pub struct BalanceSnapshot {
    pub channel_id: String,
    pub adapter: String,
    pub status: &'static str,
    pub remaining: Option<f64>,
    pub currency: Option<String>,
    pub used: Option<f64>,
    pub total: Option<f64>,
    pub unlimited: bool,
    pub label: Option<String>,
    pub windows: Vec<QuotaWindow>,
    pub detail: Value,
    pub error_kind: Option<String>,
    pub status_code: Option<i64>,
    pub duration_ms: i64,
    pub checked_at: String,
}

/// 校验后交给持久化的归一化配置。
struct NormalizedConfig {
    adapter: BalanceAdapter,
    enabled: bool,
    method: String,
    path: Option<String>,
    auth: BalanceAuth,
    headers: Vec<(String, String)>,
    body: Option<String>,
    mapping: BalanceMapping,
}

/// 一次配置保存请求的 token 变更。
enum TokenUpdate {
    Keep,
    Set(String),
    Clear,
}

/// 由组装根注入的服务依赖。
pub struct BalanceService {
    db: Database,
    secrets: SecretStore,
    http: Arc<dyn UpstreamClient>,
    channels: Arc<dyn ChannelRepository>,
    clock: Arc<dyn Clock>,
    limits: Arc<RuntimeLimits>,
}

impl BalanceService {
    pub fn new(
        db: Database,
        secrets: SecretStore,
        http: Arc<dyn UpstreamClient>,
        channels: Arc<dyn ChannelRepository>,
        clock: Arc<dyn Clock>,
        limits: Arc<RuntimeLimits>,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            secrets,
            http,
            channels,
            clock,
            limits,
        })
    }

    /// 每个公开入口的渠道存在性守卫。
    async fn ensure_channel(&self, channel_id: &str) -> Result<(), BalanceError> {
        if !self.channels.exists(channel_id).await? {
            return Err(BalanceError::ChannelNotFound);
        }
        Ok(())
    }

    /// 读取存储的配置；无记录时 `adapter=None`（默认关闭，不发请求）。
    pub async fn config(&self, channel_id: &str) -> Result<BalanceConfig, BalanceError> {
        self.ensure_channel(channel_id).await?;
        Ok(self.load_config(channel_id).await?.unwrap_or_default())
    }

    async fn load_config(&self, channel_id: &str) -> Result<Option<BalanceConfig>, BalanceError> {
        let row = sqlx::query_as::<_, ConfigRow>(
            "SELECT adapter, enabled, method, path, auth, headers_json, body_json, \
                    mapping_json, balance_token_encrypted, balance_token_hint \
             FROM channel_balance_configs WHERE channel_id=?",
        )
        .bind(channel_id)
        .fetch_optional(self.db.pool())
        .await?;
        row.map(ConfigRow::into_config).transpose()
    }

    /// 读取（若有）最新快照供管理端查看。
    pub async fn snapshot(
        &self,
        channel_id: &str,
    ) -> Result<Option<BalanceSnapshot>, BalanceError> {
        self.ensure_channel(channel_id).await?;
        Ok(load_snapshot(&self.db, channel_id).await?)
    }

    /// 保存（upsert）渠道的余额配置。
    pub async fn save_config(
        &self,
        channel_id: &str,
        input: BalanceConfigInput,
    ) -> Result<BalanceConfig, BalanceError> {
        self.ensure_channel(channel_id).await?;
        let (normalized, token_update) = normalize_config(&input)?;
        let now = self.clock.now_utc().to_rfc3339();
        let headers_json = serde_json::to_string(&normalized.headers)?;
        let mapping_json = serde_json::to_string(&normalized.mapping)?;
        let mut tx = self.db.pool().begin().await?;
        sqlx::query(
            "INSERT INTO channel_balance_configs \
             (channel_id, adapter, enabled, method, path, auth, headers_json, body_json, \
              mapping_json, balance_token_encrypted, balance_token_hint, created_at, updated_at) \
             VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,?,?) \
             ON CONFLICT(channel_id) DO UPDATE SET \
               adapter=excluded.adapter, enabled=excluded.enabled, method=excluded.method, \
               path=excluded.path, auth=excluded.auth, headers_json=excluded.headers_json, \
               body_json=excluded.body_json, mapping_json=excluded.mapping_json, \
               updated_at=excluded.updated_at",
        )
        .bind(channel_id)
        .bind(normalized.adapter.as_str())
        .bind(normalized.enabled)
        .bind(&normalized.method)
        .bind(normalized.path.as_deref())
        .bind(normalized.auth.as_str())
        .bind(&headers_json)
        .bind(normalized.body.as_deref())
        .bind(&mapping_json)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        match token_update {
            TokenUpdate::Keep => {}
            TokenUpdate::Set(token) => {
                let encrypted = self.secrets.encrypt(&token);
                let hint = SecretStore::hint(&token);
                sqlx::query(
                    "UPDATE channel_balance_configs \
                     SET balance_token_encrypted=?, balance_token_hint=?, updated_at=? \
                     WHERE channel_id=?",
                )
                .bind(encrypted)
                .bind(hint)
                .bind(&now)
                .bind(channel_id)
                .execute(&mut *tx)
                .await?;
            }
            TokenUpdate::Clear => {
                sqlx::query(
                    "UPDATE channel_balance_configs \
                     SET balance_token_encrypted=NULL, balance_token_hint=NULL, updated_at=? \
                     WHERE channel_id=?",
                )
                .bind(&now)
                .bind(channel_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        self.config(channel_id).await
    }

    /// 删除配置与快照——渠道回到默认关闭状态。
    pub async fn delete_config(&self, channel_id: &str) -> Result<(), BalanceError> {
        self.ensure_channel(channel_id).await?;
        let mut tx = self.db.pool().begin().await?;
        sqlx::query("DELETE FROM channel_balance_snapshots WHERE channel_id=?")
            .bind(channel_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM channel_balance_configs WHERE channel_id=?")
            .bind(channel_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// 立即查询单个渠道。
    ///
    /// 已保存但处于禁用状态的配置在这里仍会查询：用户点“立即查询”即为显式
    /// 意图。只有后台/批量刷新才按 `enabled=1` 过滤。没有任何配置的渠道会得到
    /// [`BalanceError::NotConfigured`] 错误。
    pub async fn query(
        &self,
        state: &Context,
        channel_id: &str,
    ) -> Result<BalanceSnapshot, BalanceError> {
        let channel: ChannelRow = self
            .channels
            .load_channel(channel_id)
            .await?
            .ok_or(BalanceError::ChannelNotFound)?;
        let config = self.config(channel_id).await?;
        let Some(adapter) = config.adapter else {
            return Err(BalanceError::NotConfigured);
        };
        let checked_at = self.clock.now_utc().to_rfc3339();
        let started = Instant::now();
        let outcome = match adapter {
            BalanceAdapter::CommandCode => {
                self.command_code_reading(state, &channel, &config).await
            }
            BalanceAdapter::Mimo => self.mimo_reading(state, &channel, &config).await,
            BalanceAdapter::OpenRouter => self.openrouter_reading(state, &channel, &config).await,
            _ => match self.prepare(&channel, &config, adapter).await {
                Ok(prepared) => self.exchange(prepared).await,
                Err(failure) => Err(failure),
            },
        };
        let duration_ms = started.elapsed().as_millis().min(i64::MAX as u128) as i64;
        let snapshot = match outcome {
            Ok((status_code, reading)) => snapshot_ok(
                channel_id,
                adapter,
                reading,
                status_code,
                duration_ms,
                checked_at,
            ),
            Err(BalanceFailure::Classified { kind, status_code }) => snapshot_error(
                channel_id,
                adapter,
                kind,
                status_code,
                duration_ms,
                checked_at,
            ),
            Err(BalanceFailure::Fatal(error)) => return Err(BalanceError::Internal(error)),
        };
        self.store_snapshot(&snapshot).await?;
        Ok(snapshot)
    }

    /// 手动批量刷新与每小时维护任务共用此方法：只查询 `enabled=1` 的渠道，
    /// 并受并发上限约束。单个失败静默处理（其快照仍会写入）。
    pub async fn refresh_enabled(
        &self,
        state: &Context,
    ) -> Result<Vec<BalanceSnapshot>, BalanceError> {
        let channel_ids: Vec<String> = sqlx::query_scalar(
            "SELECT channel_id FROM channel_balance_configs WHERE enabled=1 ORDER BY channel_id",
        )
        .fetch_all(self.db.pool())
        .await?;
        if channel_ids.is_empty() {
            return Ok(Vec::new());
        }
        let results: Vec<Result<BalanceSnapshot, BalanceError>> = futures_util::stream::iter(
            channel_ids
                .into_iter()
                .map(|channel_id| async move { self.query(state, &channel_id).await }),
        )
        .buffer_unordered(REFRESH_CONCURRENCY)
        .collect()
        .await;
        let mut snapshots = Vec::new();
        for result in results {
            match result {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => {
                    // 在列出与查询之间被删除/未配置，或本地存储失败：绝不因
                    // 单个坏渠道打扰维护循环或批量响应。
                    tracing::debug!(%error, "channel balance refresh skipped");
                }
            }
        }
        Ok(snapshots)
    }

    /// 管理端视图：配置（若有）加最新快照。
    pub async fn balance_json(&self, channel_id: &str) -> Result<Value, BalanceError> {
        self.ensure_channel(channel_id).await?;
        let config = self.load_config(channel_id).await?.unwrap_or_default();
        let snapshot = load_snapshot(&self.db, channel_id).await?;
        let mut value = config.to_json();
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "snapshot".into(),
                serde_json::to_value(&snapshot).unwrap_or(Value::Null),
            );
        }
        Ok(value)
    }

    /// `POST /channels/{id}/balance` 的响应体。
    pub async fn query_json(
        &self,
        state: &Context,
        channel_id: &str,
    ) -> Result<Value, BalanceError> {
        let snapshot = self.query(state, channel_id).await?;
        Ok(serde_json::to_value(&snapshot)?)
    }

    /// `POST /balances/refresh` 的响应体。
    pub async fn refresh_json(&self, state: &Context) -> Result<Value, BalanceError> {
        let snapshots = self.refresh_enabled(state).await?;
        let ok = snapshots.iter().filter(|item| item.status == "ok").count();
        let failed = snapshots.len() - ok;
        Ok(json!({
            "items": snapshots,
            "total": ok + failed,
            "ok": ok,
            "failed": failed,
        }))
    }

    /// 配置的专用 token，缺省回退到渠道 key。渠道 key 由调用方解密一次
    /// （模板渲染也可能需要它）。
    fn balance_token(
        &self,
        api_key: String,
        config: &BalanceConfig,
    ) -> Result<String, BalanceFailure> {
        match config.token_encrypted.as_deref() {
            Some(ciphertext) if !ciphertext.is_empty() => self
                .secrets
                .decrypt(ciphertext)
                .map_err(BalanceFailure::Fatal),
            _ => Ok(api_key),
        }
    }

    /// 渠道自身的 API Key（解密后）。
    ///
    /// 解密失败是本地密钥/密文问题，属 [`BalanceFailure::Fatal`]（服务错误），
    /// 不是上游失败；余额适配器的模板渲染与鉴权都用这一个来源。
    fn channel_token(&self, channel: &ChannelRow) -> Result<String, BalanceFailure> {
        self.secrets
            .decrypt(&channel.api_key_encrypted)
            .map_err(BalanceFailure::Fatal)
    }

    /// 运行时设置里的连接超时（与 `prepare` 同源，唯一读取点）。
    ///
    /// 读取失败或设置行损坏时回落到 10s：余额查询是旁路，设置问题不应把
    /// 整次查询升级成服务错误。缺省值不改（`connect_timeout_seconds` 默认 10）。
    async fn runtime_connect_timeout(&self) -> Duration {
        match crate::settings::runtime_settings_from(&self.db).await {
            Ok(runtime) => Duration::from_secs(runtime.connect_timeout_seconds.max(1) as u64),
            Err(_) => Duration::from_secs(10),
        }
    }

    /// 一次上游发送 + 统一失败分类（超时 / 传输 / 非 2xx）+ 响应体上限。
    ///
    /// `fetch`（多步适配器）与 `exchange`（通用单步路径）共用这一段；两侧
    /// 各自保留请求构造与响应解析，分类字符串与错误码不得放宽。
    async fn classified_send(
        &self,
        request: UpstreamRequest,
        deadline: Duration,
    ) -> Result<(i64, StatusCode, Vec<u8>), BalanceFailure> {
        let exchange = async {
            let response = self.http.send(request).await?;
            let status_code = response.status.as_u16() as i64;
            let (body, _truncated) = response.body.read_capped(BALANCE_BODY_MAX).await;
            Ok::<(i64, StatusCode, Vec<u8>), UpstreamError>((status_code, response.status, body))
        };
        let (status_code, status, body) =
            match tokio::time::timeout(deadline, exchange).await {
                Err(_) => {
                    return Err(BalanceFailure::Classified {
                        kind: error_kind::TIMEOUT,
                        status_code: None,
                    });
                }
                Ok(Err(UpstreamError::Deadline)) | Ok(Err(UpstreamError::ConnectTimeout)) => {
                    return Err(BalanceFailure::Classified {
                        kind: error_kind::TIMEOUT,
                        status_code: None,
                    });
                }
                Ok(Err(UpstreamError::Transport(_))) => {
                    return Err(BalanceFailure::Classified {
                        kind: error_kind::TRANSPORT_ERROR,
                        status_code: None,
                    });
                }
                Ok(Ok(value)) => value,
            };
        if !status.is_success() {
            return Err(BalanceFailure::Classified {
                kind: status_error_kind(status.as_u16()),
                status_code: Some(status_code),
            });
        }
        Ok((status_code, status, body))
    }

    /// 多步适配器的一次只读 GET：连接超时取运行时设置，其余交给
    /// [`Self::classified_send`]（成功时只回状态码与响应体）。
    async fn fetch(
        &self,
        url: Url,
        headers: HeaderMap,
        deadline: Duration,
    ) -> Result<(i64, Vec<u8>), BalanceFailure> {
        let request = UpstreamRequest {
            url,
            headers,
            method: Method::GET,
            body: None,
            // R11：跟随运行时 `connect_timeout_seconds`，不再写死 10s。
            connect_timeout: self.runtime_connect_timeout().await,
            deadline,
        };
        let (status_code, _status, body) = self.classified_send(request, deadline).await?;
        Ok((status_code, body))
    }

    /// 构造请求。这里的失败已被分类，可直接写成错误快照。
    async fn prepare(
        &self,
        channel: &ChannelRow,
        config: &BalanceConfig,
        adapter: BalanceAdapter,
    ) -> Result<PreparedRequest, BalanceFailure> {
        let api_key = self.channel_token(channel)?;
        let token = self.balance_token(api_key.clone(), config)?;
        // New API：专用仪表盘 token（PAT）才能解锁账户余额端点；否则用
        // sk- key 只能访问 token 作用域的用量端点。
        let account_mode = adapter == BalanceAdapter::NewApi && config.has_token;
        let raw_path = if account_mode {
            NEW_API_ACCOUNT_PATH
        } else {
            config
                .path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .or_else(|| adapter.default_path())
                .ok_or(BalanceFailure::Classified {
                    kind: error_kind::CUSTOM_PATH_MISSING,
                    status_code: None,
                })?
        };
        let path = render_template(raw_path, &api_key, &token);
        let url =
            resolve_url(&channel.base_url, &path).map_err(|_| BalanceFailure::Classified {
                kind: error_kind::INVALID_PAYLOAD,
                status_code: None,
            })?;
        let method = match adapter {
            BalanceAdapter::Custom => parse_method(&config.method).unwrap_or(Method::GET),
            _ => Method::GET,
        };
        let rendered_body = config
            .body
            .as_deref()
            .filter(|body| !body.is_empty())
            .map(|body| render_template(body, &api_key, &token));
        let body = if method == Method::GET {
            None
        } else {
            rendered_body.map(Bytes::from)
        };
        let mut headers = HeaderMap::new();
        if adapter == BalanceAdapter::Custom {
            for (name, value) in &config.headers {
                let rendered = render_template(value, &api_key, &token);
                let name = HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
                    BalanceFailure::Classified {
                        kind: error_kind::INVALID_PAYLOAD,
                        status_code: None,
                    }
                })?;
                let value =
                    HeaderValue::from_str(&rendered).map_err(|_| BalanceFailure::Classified {
                        kind: error_kind::INVALID_PAYLOAD,
                        status_code: None,
                    })?;
                headers.insert(name, value);
            }
        }
        // 一般用 Bearer；智谱控制台用裸 token（不加 `Bearer ` 前缀，即 `raw`），
        // `none` 仅用于只用头模板鉴权的自定义端点。
        let authorization = match config.auth {
            BalanceAuth::Bearer => Some(format!("Bearer {token}")),
            BalanceAuth::Raw => Some(token),
            BalanceAuth::None => None,
        };
        if let Some(value) = authorization {
            let value = HeaderValue::from_str(&value).map_err(|_| {
                BalanceFailure::Classified {
                    kind: error_kind::INVALID_PAYLOAD,
                    status_code: None,
                }
            })?;
            headers.insert(axum::http::header::AUTHORIZATION, value);
        }
        if body.is_some() && !headers.contains_key(axum::http::header::CONTENT_TYPE) {
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }
        if protocol::requires_opencode_session(&channel.base_url) {
            let session_id = crate::settings::opencode_session_id(&self.db).await;
            let _ = protocol::apply_opencode_session(&mut headers, &channel.base_url, &session_id);
        }
        let connect_timeout = self.runtime_connect_timeout().await;
        Ok(PreparedRequest {
            adapter,
            account_mode,
            mapping: config.mapping.clone(),
            url,
            method,
            headers,
            body,
            connect_timeout,
        })
    }

    /// 在单一总超时下执行发送并分类结果。响应体绝不返回给调用方，也不持久化。
    async fn exchange(
        &self,
        prepared: PreparedRequest,
    ) -> Result<(i64, BalanceReading), BalanceFailure> {
        let PreparedRequest {
            adapter,
            account_mode,
            mapping,
            url,
            method,
            headers,
            body,
            connect_timeout,
        } = prepared;
        // 解析前先拿到主机名（SiliconFlow 用域名区分币种）。
        let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
        let request = UpstreamRequest {
            url,
            headers,
            method,
            body,
            connect_timeout,
            deadline: self.limits.balance_timeout,
        };
        let deadline = self.limits.balance_timeout;
        let (status_code, _status, body) = self.classified_send(request, deadline).await?;
        let reading = if account_mode {
            parse_newapi_user(&body)
        } else {
            match adapter {
                BalanceAdapter::Custom => parse_custom(&body, &mapping),
                _ => parse_reading(adapter, &body, &host),
            }
        };
        reading
            .map(|reading| (status_code, reading))
            .map_err(|_| BalanceFailure::Classified {
                kind: error_kind::INVALID_PAYLOAD,
                status_code: Some(status_code),
            })
    }

    /// Command Code 额度：`whoami` → `billing/credits` → `usage/summary`
    /// → `billing/subscriptions`。四个都是只读 GET（不产生生成、不消耗
    /// token），并受全局 `command_code_enabled` 开关约束。
    async fn command_code_reading(
        &self,
        state: &Context,
        channel: &ChannelRow,
        _config: &BalanceConfig,
    ) -> Result<(i64, BalanceReading), BalanceFailure> {
        let runtime = crate::settings::runtime_settings(state)
            .await
            .unwrap_or_default();
        if !runtime.command_code_enabled {
            return Err(BalanceFailure::Classified {
                kind: error_kind::DISABLED,
                status_code: None,
            });
        }
        let api_key = self.channel_token(channel)?;
        let deadline = self.limits.balance_timeout;
        let (_, whoami) = self
            .command_code_get(&channel.base_url, "/alpha/whoami", &api_key, deadline)
            .await?;
        // 账号可能没有 org（whoami 的 `org` 为 null，个人 key）。此时官方
        // 语义是**省略 orgId 参数**（拼成 `?orgId=` 会得到 400 Invalid UUID）；
        // patlux 的 buildUrl 同样会丢掉 undefined 参数。
        let org_id = command_code_org_id(&whoami);
        let with_org = |path: &str| match &org_id {
            Some(org_id) => format!("{path}?orgId={org_id}"),
            None => path.to_owned(),
        };
        let credits_path = with_org("/alpha/billing/credits");
        let (credits_status, credits) = self
            .command_code_get(&channel.base_url, &credits_path, &api_key, deadline)
            .await?;
        // 用量汇总可选：它失败时仍返回 credits/窗口读数，而不是完全没有余额。
        let summary_path = with_org("/alpha/usage/summary");
        let summary = match self
            .command_code_get(&channel.base_url, &summary_path, &api_key, deadline)
            .await
        {
            Ok((_, body)) => Some(body),
            Err(_error) => {
                tracing::debug!("command code usage summary unavailable");
                None
            }
        };
        // 订阅信息用于展示 planId/状态，并作为 windowLimits 之外的计划来源；
        // 查询失败不影响额度读数。
        let subscriptions_path = with_org("/alpha/billing/subscriptions");
        let subscription = match self
            .command_code_get(&channel.base_url, &subscriptions_path, &api_key, deadline)
            .await
        {
            Ok((_, body)) => Some(body),
            Err(_error) => {
                tracing::debug!("command code subscriptions unavailable");
                None
            }
        };
        let reading = parse_command_code(
            &whoami,
            &credits,
            summary.as_deref(),
            subscription.as_deref(),
            org_id.as_deref().unwrap_or_default(),
        )
        .map_err(|_| BalanceFailure::Classified {
            kind: error_kind::INVALID_PAYLOAD,
            status_code: Some(credits_status),
        })?;
        Ok((credits_status, reading))
    }

    async fn command_code_get(
        &self,
        base_url: &str,
        path: &str,
        api_key: &str,
        deadline: Duration,
    ) -> Result<(i64, Vec<u8>), BalanceFailure> {
        let url = crate::protocol::upstream_url(base_url, path, None, "command_code")
            .map_err(|_| invalid_payload_failure())?;
        let headers = bearer_headers(api_key)?;
        self.fetch(url, headers, deadline).await
    }

    /// 小米 MiMo Token Plan：控制台 Cookie 的三步只读 GET。
    ///
    /// MiMo 没有 API Key 查询路径（`tokenPlan/usage` 无 Cookie 会 401 +
    /// `loginUrl`），所以「独立令牌」按浏览器 Cookie 串处理，缺省回落到渠道
    /// API Key（那时必然 401，属预期）。`tokenPlan/usage` 与 `balance` 必需，
    /// `tokenPlan/detail` 只补套餐名与周期，失败不影响读数。
    async fn mimo_reading(
        &self,
        _state: &Context,
        channel: &ChannelRow,
        config: &BalanceConfig,
    ) -> Result<(i64, BalanceReading), BalanceFailure> {
        let api_key = self.channel_token(channel)?;
        let token = self.balance_token(api_key, config)?;
        let mut headers = json_headers();
        headers.insert(
            axum::http::header::USER_AGENT,
            HeaderValue::from_static(MIMO_USER_AGENT),
        );
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&token).map_err(|_| invalid_payload_failure())?,
        );
        let deadline = self.limits.balance_timeout;
        let usage_url = balance_url(MIMO_CONSOLE_URL, "/api/v1/tokenPlan/usage")?;
        let (usage_status, usage) = self.fetch(usage_url, headers.clone(), deadline).await?;
        let balance_endpoint = balance_url(MIMO_CONSOLE_URL, "/api/v1/balance")?;
        let (_, balance) = self.fetch(balance_endpoint, headers.clone(), deadline).await?;
        // 套餐明细可选：查询失败只是没有套餐名/周期。
        let detail = match balance_url(MIMO_CONSOLE_URL, "/api/v1/tokenPlan/detail") {
            Ok(url) => match self.fetch(url, headers, deadline).await {
                Ok((_, body)) => Some(body),
                Err(_error) => None,
            },
            Err(_error) => None,
        };
        let reading = parse_mimo(&usage, &balance, detail.as_deref()).map_err(|_| {
            BalanceFailure::Classified {
                kind: error_kind::INVALID_PAYLOAD,
                status_code: Some(usage_status),
            }
        })?;
        Ok((usage_status, reading))
    }

    /// OpenRouter：`/credits`（需要 Management Key）给出账户余额，失败时
    /// 回落到 `/key`（任何 Key 都能读自己的额度）。
    async fn openrouter_reading(
        &self,
        _state: &Context,
        channel: &ChannelRow,
        config: &BalanceConfig,
    ) -> Result<(i64, BalanceReading), BalanceFailure> {
        let token = self.balance_token(self.channel_token(channel)?, config)?;
        let headers = bearer_headers(&token)?;
        let deadline = self.limits.balance_timeout;
        let credits_url = balance_url(OPENROUTER_API_URL, "/api/v1/credits")?;
        let (status, body) = match self.fetch(credits_url, headers.clone(), deadline).await {
            Ok(success) => success,
            // 普通 Key 读不了账户视图（403）：改用 key 额度视图；两步都失败
            // 时返回后一次的分类。
            Err(_first) => {
                let key_url = balance_url(OPENROUTER_API_URL, "/api/v1/key")?;
                self.fetch(key_url, headers, deadline).await?
            }
        };
        let reading = parse_openrouter(&body).map_err(|_| BalanceFailure::Classified {
            kind: error_kind::INVALID_PAYLOAD,
            status_code: Some(status),
        })?;
        Ok((status, reading))
    }

    async fn store_snapshot(&self, snapshot: &BalanceSnapshot) -> Result<(), BalanceError> {
        let windows = serde_json::to_string(&snapshot.windows)?;
        let detail = serde_json::to_string(&snapshot.detail)?;
        sqlx::query(
            "INSERT INTO channel_balance_snapshots \
             (channel_id, adapter, status, remaining, currency, used, total, unlimited, label, \
              windows_json, detail_json, error_kind, status_code, duration_ms, checked_at) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(channel_id) DO UPDATE SET \
               adapter=excluded.adapter, status=excluded.status, remaining=excluded.remaining, \
               currency=excluded.currency, used=excluded.used, total=excluded.total, \
               unlimited=excluded.unlimited, label=excluded.label, \
               windows_json=excluded.windows_json, detail_json=excluded.detail_json, \
               error_kind=excluded.error_kind, status_code=excluded.status_code, \
               duration_ms=excluded.duration_ms, checked_at=excluded.checked_at",
        )
        .bind(&snapshot.channel_id)
        .bind(&snapshot.adapter)
        .bind(snapshot.status)
        .bind(snapshot.remaining)
        .bind(snapshot.currency.as_deref())
        .bind(snapshot.used)
        .bind(snapshot.total)
        .bind(snapshot.unlimited)
        .bind(snapshot.label.as_deref())
        .bind(&windows)
        .bind(&detail)
        .bind(snapshot.error_kind.as_deref())
        .bind(snapshot.status_code)
        .bind(snapshot.duration_ms)
        .bind(&snapshot.checked_at)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }
}

/// 由配置构造、可直接进入网络阶段的请求。
struct PreparedRequest {
    adapter: BalanceAdapter,
    /// New API 账户余额模式（`/api/user/self` + 专用 PAT）。
    account_mode: bool,
    mapping: BalanceMapping,
    url: Url,
    method: Method,
    headers: HeaderMap,
    body: Option<Bytes>,
    connect_timeout: Duration,
}

/// 已分类的余额失败：`Classified` 写成快照，`Fatal` 保持服务错误
/// （本地损坏，绝不是上游应答导致的）。
enum BalanceFailure {
    Classified {
        kind: &'static str,
        status_code: Option<i64>,
    },
    Fatal(anyhow::Error),
}

fn snapshot_ok(
    channel_id: &str,
    adapter: BalanceAdapter,
    reading: BalanceReading,
    status_code: i64,
    duration_ms: i64,
    checked_at: String,
) -> BalanceSnapshot {
    BalanceSnapshot {
        channel_id: channel_id.to_owned(),
        adapter: adapter.as_str().into(),
        status: "ok",
        remaining: reading.remaining,
        currency: reading.currency,
        used: reading.used,
        total: reading.total,
        unlimited: reading.unlimited,
        label: reading.label,
        windows: reading.windows,
        detail: reading.detail,
        error_kind: None,
        status_code: Some(status_code),
        duration_ms,
        checked_at,
    }
}

fn snapshot_error(
    channel_id: &str,
    adapter: BalanceAdapter,
    error_kind: &'static str,
    status_code: Option<i64>,
    duration_ms: i64,
    checked_at: String,
) -> BalanceSnapshot {
    BalanceSnapshot {
        channel_id: channel_id.to_owned(),
        adapter: adapter.as_str().into(),
        status: "error",
        remaining: None,
        currency: None,
        used: None,
        total: None,
        unlimited: false,
        label: None,
        windows: Vec::new(),
        detail: Value::Null,
        error_kind: Some(error_kind.to_owned()),
        status_code,
        duration_ms,
        checked_at,
    }
}

/// 持久化的配置行。
#[derive(sqlx::FromRow)]
struct ConfigRow {
    adapter: String,
    enabled: bool,
    method: String,
    path: Option<String>,
    auth: String,
    headers_json: Option<String>,
    body_json: Option<String>,
    mapping_json: Option<String>,
    balance_token_encrypted: Option<Vec<u8>>,
    balance_token_hint: Option<String>,
}

impl ConfigRow {
    fn into_config(self) -> Result<BalanceConfig, BalanceError> {
        let adapter = BalanceAdapter::parse(&self.adapter)?;
        let auth = BalanceAuth::parse(&self.auth).unwrap_or(BalanceAuth::Bearer);
        let headers: Vec<(String, String)> = self
            .headers_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let mapping: BalanceMapping = self
            .mapping_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        let token_encrypted = self
            .balance_token_encrypted
            .filter(|ciphertext| !ciphertext.is_empty());
        Ok(BalanceConfig {
            adapter: Some(adapter),
            enabled: self.enabled,
            method: if self.method.trim().is_empty() {
                "GET".into()
            } else {
                self.method
            },
            path: self.path,
            auth,
            headers,
            body: self.body_json,
            mapping,
            has_token: token_encrypted.is_some(),
            token_hint: self.balance_token_hint,
            token_encrypted,
        })
    }
}

/// 持久化的快照行。
#[derive(sqlx::FromRow)]
struct SnapshotRow {
    adapter: String,
    status: String,
    remaining: Option<f64>,
    currency: Option<String>,
    used: Option<f64>,
    total: Option<f64>,
    unlimited: bool,
    label: Option<String>,
    windows_json: Option<String>,
    detail_json: Option<String>,
    error_kind: Option<String>,
    status_code: Option<i64>,
    duration_ms: Option<i64>,
    checked_at: String,
}

impl SnapshotRow {
    fn into_snapshot(self, channel_id: &str) -> BalanceSnapshot {
        BalanceSnapshot {
            channel_id: channel_id.to_owned(),
            adapter: self.adapter,
            status: if self.status == "ok" { "ok" } else { "error" },
            remaining: self.remaining,
            currency: self.currency,
            used: self.used,
            total: self.total,
            unlimited: self.unlimited,
            label: self.label,
            windows: self
                .windows_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or_default(),
            detail: self
                .detail_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or(Value::Null),
            error_kind: self.error_kind,
            status_code: self.status_code,
            duration_ms: self.duration_ms.unwrap_or(0),
            checked_at: self.checked_at,
        }
    }
}

async fn load_snapshot(
    db: &Database,
    channel_id: &str,
) -> Result<Option<BalanceSnapshot>, sqlx::Error> {
    let row = sqlx::query_as::<_, SnapshotRow>(
        "SELECT adapter, status, remaining, currency, used, total, unlimited, label, \
                windows_json, detail_json, error_kind, status_code, duration_ms, checked_at \
         FROM channel_balance_snapshots WHERE channel_id=?",
    )
    .bind(channel_id)
    .fetch_optional(db.pool())
    .await?;
    Ok(row.map(|row| row.into_snapshot(channel_id)))
}

/// 嵌入每个渠道 JSON（管理端列表 / 详情）的只读余额摘要。没有配置行的渠道
/// 绝不报告 `configured=true`。
pub async fn channel_balance_json(db: &Database, channel_id: &str) -> Result<Value, sqlx::Error> {
    let config: Option<(String, bool)> =
        sqlx::query_as("SELECT adapter, enabled FROM channel_balance_configs WHERE channel_id=?")
            .bind(channel_id)
            .fetch_optional(db.pool())
            .await?;
    let snapshot = load_snapshot(db, channel_id).await?;
    Ok(json!({
        "configured": config.is_some(),
        "enabled": config.as_ref().map(|(_, enabled)| *enabled).unwrap_or(false),
        "adapter": config.as_ref().map(|(adapter, _)| adapter.clone()),
        "snapshot": snapshot
            .map(|snapshot| serde_json::to_value(snapshot).unwrap_or(Value::Null))
            .unwrap_or(Value::Null),
    }))
}

/// 校验并归一化一个 `PUT balance-config` 请求体。
fn normalize_config(
    input: &BalanceConfigInput,
) -> Result<(NormalizedConfig, TokenUpdate), BalanceError> {
    let adapter = BalanceAdapter::parse(&input.adapter)?;
    let method = match adapter {
        BalanceAdapter::Custom => {
            let value = input
                .method
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("GET")
                .to_ascii_uppercase();
            if !matches!(value.as_str(), "GET" | "POST" | "PUT") {
                return Err(BalanceError::Invalid(
                    "自定义余额查询只支持 GET / POST / PUT".into(),
                ));
            }
            value
        }
        _ => "GET".into(),
    };
    let path = match adapter {
        BalanceAdapter::Custom => {
            let value = input
                .path
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| BalanceError::Invalid("自定义余额查询必须填写路径".into()))?;
            if value.chars().count() > MAX_CUSTOM_PATH_CHARS {
                return Err(BalanceError::Invalid(format!(
                    "余额查询路径不能超过 {MAX_CUSTOM_PATH_CHARS} 个字符"
                )));
            }
            Some(value.to_owned())
        }
        built_in => Some(built_in.default_path().unwrap_or_default().to_owned()),
    };
    let auth = match adapter {
        BalanceAdapter::Custom => BalanceAuth::parse(input.auth.as_deref().unwrap_or("bearer"))?,
        // 智谱监控接口用裸 token（不加 Bearer 前缀）；其余内置适配器走 Bearer。
        BalanceAdapter::ZhipuGlm => BalanceAuth::Raw,
        _ => BalanceAuth::Bearer,
    };
    if input.headers.len() > MAX_CUSTOM_HEADERS {
        return Err(BalanceError::Invalid(format!(
            "余额查询请求头不能超过 {MAX_CUSTOM_HEADERS} 条"
        )));
    }
    let mut headers = Vec::with_capacity(input.headers.len());
    for (name, value) in &input.headers {
        let name = name.trim();
        if name.is_empty()
            || HeaderName::from_bytes(name.as_bytes()).is_err()
            || HeaderValue::from_str(value).is_err()
        {
            return Err(BalanceError::Invalid(format!("余额查询请求头无效：{name}")));
        }
        headers.push((name.to_owned(), value.clone()));
    }
    let body = match adapter {
        BalanceAdapter::Custom => match input.body.as_deref().map(str::trim) {
            Some(body) if !body.is_empty() => {
                if body.len() > MAX_CUSTOM_BODY_BYTES {
                    return Err(BalanceError::Invalid(format!(
                        "余额查询请求体不能超过 {} KiB",
                        MAX_CUSTOM_BODY_BYTES / 1024
                    )));
                }
                Some(body.to_owned())
            }
            _ => None,
        },
        _ => None,
    };
    let mapping = match adapter {
        BalanceAdapter::Custom => BalanceMapping {
            remaining: mapping_path(&input.mapping.remaining)?,
            currency: mapping_path(&input.mapping.currency)?,
            used: mapping_path(&input.mapping.used)?,
            total: mapping_path(&input.mapping.total)?,
            label: mapping_path(&input.mapping.label)?,
        },
        _ => BalanceMapping::default(),
    };
    let token_update = if input.clear_token {
        TokenUpdate::Clear
    } else {
        match input.token.as_deref().map(str::trim) {
            Some(token) if !token.is_empty() => {
                if token.chars().count() > 65535 {
                    return Err(BalanceError::Invalid("余额令牌长度无效".into()));
                }
                TokenUpdate::Set(token.to_owned())
            }
            _ => TokenUpdate::Keep,
        }
    };
    Ok((
        NormalizedConfig {
            adapter,
            enabled: input.enabled,
            method,
            path,
            auth,
            headers,
            body,
            mapping,
        },
        token_update,
    ))
}

fn mapping_path(value: &Option<String>) -> Result<Option<String>, BalanceError> {
    match value.as_deref().map(str::trim) {
        Some(path) if !path.is_empty() => {
            if path.chars().count() > MAX_MAPPING_CHARS {
                return Err(BalanceError::Invalid(format!(
                    "映射路径不能超过 {MAX_MAPPING_CHARS} 个字符"
                )));
            }
            if !path.starts_with('$') {
                return Err(BalanceError::Invalid(format!(
                    "映射路径必须以 $ 开头：{path}"
                )));
            }
            Ok(Some(path.to_owned()))
        }
        _ => Ok(None),
    }
}

fn parse_method(value: &str) -> Option<Method> {
    match value.trim().to_ascii_uppercase().as_str() {
        "GET" => Some(Method::GET),
        "POST" => Some(Method::POST),
        "PUT" => Some(Method::PUT),
        _ => None,
    }
}

fn status_error_kind(status: u16) -> &'static str {
    match status {
        401 => error_kind::HTTP_401,
        403 => error_kind::HTTP_403,
        value if value >= 500 => error_kind::HTTP_5XX,
        _ => error_kind::HTTP_ERROR,
    }
}

/// 把配置的 path 解析到渠道 base URL 上。
///
/// * `https://…` 为绝对地址；
/// * `/xxx` 相对站点根；
/// * 其它则追加到 base URL 的路径后；
/// * 三种形式的 query string 都会保留。
pub fn resolve_url(base_url: &str, path: &str) -> anyhow::Result<Url> {
    let path = path.trim();
    if path.is_empty() {
        return Err(anyhow!("empty balance path"));
    }
    if let Ok(url) = Url::parse(path)
        && url.host_str().is_some()
    {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(anyhow!("balance path must be an HTTP(S) URL"));
        }
        return Ok(url);
    }
    let base = Url::parse(base_url).context("channel base_url is not a valid URL")?;
    if !matches!(base.scheme(), "http" | "https") || base.host_str().is_none() {
        return Err(anyhow!("channel base_url is not an absolute HTTP(S) URL"));
    }
    if let Some(rest) = path.strip_prefix('/') {
        let (path_part, query) = split_query(rest);
        let mut url = base;
        url.set_path(&format!("/{path_part}"));
        url.set_query(query);
        url.set_fragment(None);
        Ok(url)
    } else {
        let (path_part, query) = split_query(path);
        protocol::upstream_url(base_url, path_part, query, "openai_compatible")
    }
}

fn split_query(path: &str) -> (&str, Option<&str>) {
    match path.split_once('?') {
        Some((path, query)) if !query.is_empty() => (path, Some(query)),
        Some((path, _)) => (path, None),
        None => (path, None),
    }
}

/// 用于自定义 header 与 body 的 `${api_key}` / `${token}` 替换。
/// 渲染后的值只在网络请求中使用，绝不持久化。
pub fn render_template(template: &str, api_key: &str, token: &str) -> String {
    template
        .replace("${api_key}", api_key)
        .replace("${token}", token)
}

/// 最小 JSON 路径子集：`$`、`.field` 与 `[index]`，
/// 例如 `$.data.items[0].balance`。
pub fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut rest = path.trim().strip_prefix('$')?;
    let mut current = value;
    while !rest.is_empty() {
        if let Some(tail) = rest.strip_prefix('.') {
            let end = tail.find(['.', '[']).unwrap_or(tail.len());
            let key = &tail[..end];
            if key.is_empty() {
                return None;
            }
            current = current.get(key)?;
            rest = &tail[end..];
        } else {
            let tail = rest.strip_prefix('[')?;
            let end = tail.find(']')?;
            let index: usize = tail[..end].trim().parse().ok()?;
            current = current.get(index)?;
            rest = &tail[end + 1..];
        }
    }
    Some(current)
}

/// 数字或数字字符串转 `f64`（上游对 JSON 类型不统一）。
pub fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|value| value.is_finite()),
        Value::String(value) => value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite()),
        _ => None,
    }
}

/// 分发到适配器解析器。`host` 是解析后的请求主机
/// （SiliconFlow 按域名报告币种）。
fn parse_reading(
    adapter: BalanceAdapter,
    body: &[u8],
    host: &str,
) -> Result<BalanceReading, BalanceError> {
    match adapter {
        BalanceAdapter::NewApi => parse_newapi(body),
        BalanceAdapter::Sub2Api => parse_sub2api(body),
        BalanceAdapter::OpencodeGo => parse_opencode_go(body),
        BalanceAdapter::DeepSeek => parse_deepseek(body),
        BalanceAdapter::SiliconFlow => parse_siliconflow(body, host),
        BalanceAdapter::StepFun => parse_stepfun(body),
        BalanceAdapter::Novita => parse_novita(body),
        BalanceAdapter::Moonshot => parse_moonshot(body),
        BalanceAdapter::ZhipuGlm => parse_zhipu(body),
        BalanceAdapter::MiniMax => parse_minimax(body),
        BalanceAdapter::KimiCode => parse_kimi_code(body),
        BalanceAdapter::CommandCode => Err(BalanceError::Invalid(
            "command_code uses a multi-step query".into(),
        )),
        BalanceAdapter::Mimo => Err(BalanceError::Invalid(
            "mimo uses a multi-step query".into(),
        )),
        BalanceAdapter::OpenRouter => Err(BalanceError::Invalid(
            "openrouter uses a multi-step query".into(),
        )),
        BalanceAdapter::Custom => Err(BalanceError::Invalid("custom needs a mapping".into())),
    }
}

fn invalid_payload() -> BalanceError {
    BalanceError::Invalid("invalid_payload".into())
}

/// 解析上游 JSON 响应体；解析失败统一归类为 [`invalid_payload`]。
///
/// 只服务 `parse_*` 解析器（返回值是 [`BalanceError`]）；网络阶段的失败分类
/// 用 [`invalid_payload_failure`]。
fn decode_body(body: &[u8]) -> Result<Value, BalanceError> {
    serde_json::from_slice(body).map_err(|_| invalid_payload())
}

/// 只读适配器通用的 JSON 请求头（`Accept: application/json`）。
fn json_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::ACCEPT,
        HeaderValue::from_static("application/json"),
    );
    headers
}

/// [`json_headers`] 再加上 `Authorization: Bearer <token>`。
///
/// 令牌含非法头字符时按既有分类返回 `invalid_payload`
/// （错误来自 [`crate::protocol::bearer_header`] 的头值构造）。
fn bearer_headers(token: &str) -> Result<HeaderMap, BalanceFailure> {
    let mut headers = json_headers();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        crate::protocol::bearer_header(token).map_err(|_| invalid_payload_failure())?,
    );
    Ok(headers)
}

/// 请求构造阶段 [`invalid_payload`] 的分类版本。
fn invalid_payload_failure() -> BalanceFailure {
    BalanceFailure::Classified {
        kind: error_kind::INVALID_PAYLOAD,
        status_code: None,
    }
}

/// 把某个适配器端点解析到固定主机上，失败像其它请求构造失败一样
/// 归类为 `invalid_payload`（写入快照）。
fn balance_url(base: &str, path: &str) -> Result<Url, BalanceFailure> {
    resolve_url(base, path).map_err(|_| invalid_payload_failure())
}

/// New API / one-api 系：额度以每 USD 500,000 单位计。
pub fn parse_newapi(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let granted = data.get("total_granted").and_then(number);
    let used = data.get("total_used").and_then(number);
    let available = data
        .get("total_available")
        .and_then(number)
        .or(match (granted, used) {
            (Some(granted), Some(used)) => Some(granted - used),
            _ => None,
        });
    if granted.is_none() && used.is_none() && available.is_none() {
        return Err(invalid_payload());
    }
    let unlimited_flag = data
        .get("unlimited_quota")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    // 具体额度总是优先于 unlimited 标志：若干分支会把 `unlimited_quota`
    // 置真，却仍返回真实的 granted/available 值（CCTQ 在套餐透支时返回
    // granted > 0 且 available 为负）。只有显式 `total_granted < 0` 哨兵值
    // 且没有任何可用数字时，才保留 unlimited 视图。
    let numeric_quota = granted.is_some_and(|granted| granted >= 0.0)
        || available.is_some_and(|available| available >= 0.0);
    let unlimited =
        (unlimited_flag || granted.is_some_and(|granted| granted < 0.0)) && !numeric_quota;
    let scale = |value: Option<f64>| value.map(|value| value / NEW_API_QUOTA_PER_USD);
    let mut detail = serde_json::Map::new();
    if let Some(expires_at) = data.get("expires_at").and_then(number) {
        detail.insert("expires_at".into(), json!(expires_at));
    }
    Ok(BalanceReading {
        remaining: if unlimited { None } else { scale(available) },
        currency: Some("USD".into()),
        used: scale(used),
        total: scale(granted),
        unlimited,
        label: data
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|name| !name.is_empty()),
        windows: Vec::new(),
        detail: Value::Object(detail),
    })
}

/// New API 账户余额（`GET /api/user/self`），使用仪表盘 PAT。
/// `quota` 是账户剩余额度，`used_quota` 是已消耗额度，均以每 USD 500,000 单位计。
pub fn parse_newapi_user(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let quota = data.get("quota").and_then(number);
    let used = data.get("used_quota").and_then(number);
    if quota.is_none() && used.is_none() {
        return Err(invalid_payload());
    }
    let scale = |value: Option<f64>| value.map(|value| value / NEW_API_QUOTA_PER_USD);
    let total = match (quota, used) {
        (Some(quota), Some(used)) => scale(Some(quota + used)),
        _ => scale(quota),
    };
    let mut detail = serde_json::Map::new();
    if let Some(request_count) = data.get("request_count").and_then(number) {
        detail.insert("request_count".into(), json!(request_count));
    }
    if let Some(group) = data.get("group").and_then(Value::as_str) {
        detail.insert("group".into(), json!(group));
    }
    Ok(BalanceReading {
        remaining: scale(quota),
        currency: Some("USD".into()),
        used: scale(used),
        total,
        unlimited: false,
        label: data
            .get("display_name")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty())
            .or_else(|| data.get("username").and_then(Value::as_str))
            .map(str::to_owned),
        windows: Vec::new(),
        detail: Value::Object(detail),
    })
}

/// Sub2API `/v1/usage`（quota_limited / unrestricted 模式）。
pub fn parse_sub2api(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let mode = value
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let quota = value.get("quota").filter(|quota| quota.is_object());
    let remaining = quota
        .and_then(|quota| quota.get("remaining"))
        .and_then(number)
        .or_else(|| value.get("remaining").and_then(number));
    let total = quota
        .and_then(|quota| quota.get("limit"))
        .and_then(number)
        .or_else(|| value.get("total").and_then(number));
    let used = quota
        .and_then(|quota| quota.get("used"))
        .and_then(number)
        .or_else(|| value.get("used").and_then(number));
    if remaining.is_none() && total.is_none() && used.is_none() {
        // 只有不含任何具体数字的响应才算 “unrestricted”；当上游返回了额度
        // 数值时就照常展示，哪怕 mode 写着 unrestricted。
        if mode == "unrestricted" {
            return Ok(BalanceReading {
                unlimited: true,
                label: value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                detail: sub2api_detail(&value),
                ..BalanceReading::default()
            });
        }
        return Err(invalid_payload());
    }
    let currency = quota
        .and_then(|quota| quota.get("unit"))
        .and_then(Value::as_str)
        .or_else(|| value.get("unit").and_then(Value::as_str))
        .map(str::to_owned);
    Ok(BalanceReading {
        remaining,
        currency,
        used,
        total,
        unlimited: false,
        label: value
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        windows: Vec::new(),
        detail: sub2api_detail(&value),
    })
}

fn sub2api_detail(value: &Value) -> Value {
    let mut detail = serde_json::Map::new();
    if let Some(mode) = value.get("mode") {
        detail.insert("mode".into(), mode.clone());
    }
    if let Some(valid) = value.get("isValid") {
        detail.insert("is_valid".into(), valid.clone());
    }
    if let Some(usage) = value.get("usage") {
        detail.insert("usage".into(), usage.clone());
    }
    Value::Object(detail)
}

/// OpenCode Zen / Go `/v1/usage`：三个用量窗口（rolling 5h / weekly / monthly），
/// 只有已用百分比。
pub fn parse_opencode_go(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let usage = value
        .get("usage")
        .filter(|usage| usage.is_object())
        .ok_or_else(invalid_payload)?;
    let mut windows = Vec::new();
    for (key, label) in [("rolling", "5h"), ("weekly", "周"), ("monthly", "月")] {
        let Some(window) = usage.get(key) else {
            continue;
        };
        let status_ok = window
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status == "ok");
        let Some(used_percent) = window.get("percent").and_then(number) else {
            continue;
        };
        if !status_ok || !(0.0..=100.0).contains(&used_percent) {
            continue;
        }
        windows.push(quota_window(
            label,
            used_percent,
            100.0 - used_percent,
            // OpenCode 的 `resetsAt` 已是 RFC3339，直接透传（不做时间戳归一化）。
            window
                .get("resetsAt")
                .and_then(Value::as_str)
                .map(str::to_owned),
        ));
    }
    Ok(BalanceReading {
        windows: require_windows(windows)?,
        ..BalanceReading::default()
    })
}

/// Command Code `whoami`：`org.id`、`orgId` 或 `org.orgId`。
pub fn command_code_org_id(whoami: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(whoami).ok()?;
    // 官方 CLI 读取 `data.org.id`（`/alpha/whoami` 的 usage 包装），社区实现
    // 读取顶层 `org.id`；两种形状都必须兼容。
    [
        value.pointer("/data/org/id"),
        value.pointer("/data/orgId"),
        value.pointer("/data/org_id"),
        value.pointer("/org/id"),
        value.get("orgId"),
        value.pointer("/org/orgId"),
        value.get("org_id"),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| value.as_str().map(str::to_owned).filter(|id| !id.is_empty()))
}

/// Command Code 账号名（whoami 顶层 `user`/`data.user` 两种形状）。
pub fn command_code_account_name(whoami: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(whoami).ok()?;
    [
        value.pointer("/user/userName"),
        value.pointer("/user/name"),
        value.pointer("/data/user/userName"),
        value.pointer("/data/user/name"),
        value.pointer("/org/login"),
        value.pointer("/data/org/login"),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| value.as_str().map(str::to_owned).filter(|name| !name.is_empty()))
}

/// 把窗口重置时间统一成 RFC3339（秒/毫秒时间戳或 ISO 字符串）。
fn quota_reset_at(value: &Value) -> Option<String> {
    // `resetAt: 0`（以及负数）表示“窗口尚未消耗、没有待重置时间”，不能
    // 当成 1970 年展示。
    let normalize = |epoch: i64| {
        if epoch <= 0 {
            return None;
        }
        let (seconds, nanos) = if epoch > 1_000_000_000_000 {
            (epoch / 1000, ((epoch % 1000) * 1_000_000) as u32)
        } else {
            (epoch, 0)
        };
        chrono::DateTime::from_timestamp(seconds, nanos).map(|value| value.to_rfc3339())
    };
    match value {
        Value::String(text) => {
            let text = text.trim();
            if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
                let parsed = parsed.with_timezone(&chrono::Utc);
                return (parsed.timestamp() > 0).then(|| parsed.to_rfc3339());
            }
            let epoch = text.parse::<i64>().ok()?;
            normalize(epoch)
        }
        Value::Number(number) => normalize(number.as_i64()?),
        _ => None,
    }
}

/// Command Code 额度：credits + 5h/周窗口 + 可选用量汇总/订阅。
///
/// Go 套餐的 `monthlyCredits/purchasedCredits/freeCredits` 可能全为 0，额度体现
/// 在 `windowLimits.fiveHour/weekly`；因此**零 credits 不是无效负载**。
///
/// - `remaining = monthlyCredits + purchasedCredits + freeCredits`（>0 才展示）
/// - `used` 取 `summary.totalCost`
/// - `total = remaining + used`（credits 未知时为 None，由窗口展示）
/// - `fiveHour` / `weekly` → `QuotaWindow{label:"5h"/"周"}`，
///   `used_percent = used/cap * 100`，`resets_at` 统一为 RFC3339。
pub fn parse_command_code(
    whoami: &[u8],
    credits: &[u8],
    summary: Option<&[u8]>,
    subscription: Option<&[u8]>,
    org_id: &str,
) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(credits)?;
    let credits_object = value.get("credits").filter(|value| value.is_object());
    let monthly = credits_object
        .and_then(|value| value.get("monthlyCredits"))
        .and_then(number)
        .unwrap_or(0.0);
    let purchased = credits_object
        .and_then(|value| value.get("purchasedCredits"))
        .and_then(number)
        .unwrap_or(0.0);
    let free = credits_object
        .and_then(|value| value.get("freeCredits"))
        .and_then(number)
        .unwrap_or(0.0);
    // 兼容 windowLimits 嵌套与顶层两种形状。
    let limits = value
        .get("windowLimits")
        .filter(|value| value.is_object())
        .or_else(|| Some(&value));
    let mut windows: Vec<QuotaWindow> = Vec::new();
    if let Some(limits) = limits {
        for (key, label) in [("fiveHour", "5h"), ("weekly", "周")] {
            let Some(window) = limits.get(key).filter(|value| value.is_object()) else {
                continue;
            };
            let used = window.get("used").and_then(number).unwrap_or(0.0);
            let cap = window.get("cap").and_then(number).unwrap_or(0.0);
            if cap <= 0.0 {
                continue;
            }
            let used_percent = (used / cap * 100.0).clamp(0.0, 100.0);
            windows.push(used_window(label, used_percent, window.get("resetAt")));
        }
    }
    let summary_value: Option<Value> =
        summary.and_then(|body| serde_json::from_slice::<Value>(body).ok());
    let subscription_value: Option<Value> =
        subscription.and_then(|body| serde_json::from_slice::<Value>(body).ok());
    let used = summary_value
        .as_ref()
        .and_then(|value| value.get("totalCost"))
        .and_then(number);
    let plan_id = value
        .get("planId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            subscription_value
                .as_ref()
                .and_then(|value| value.pointer("/data/planId"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let plan_status = subscription_value
        .as_ref()
        .and_then(|value| value.pointer("/data/status"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    // 只有“既没有 credits 结构、也没有窗口、没有汇总/订阅”才视为无效负载：
    // 单个账号 credits 全为 0 是完全合法的 Go 套餐。
    if credits_object.is_none() && windows.is_empty() && summary_value.is_none() {
        return Err(invalid_payload());
    }
    let credits_total = monthly + purchased + free;
    let remaining = (credits_total > 0.0).then_some(credits_total);
    Ok(BalanceReading {
        remaining,
        currency: None,
        used,
        total: remaining.map(|value| value + used.unwrap_or(0.0)),
        unlimited: false,
        label: remaining.map(|_| "Credits".to_owned()),
        windows,
        detail: json!({
            "orgId": org_id,
            "account": command_code_account_name(whoami),
            "planId": plan_id,
            "planStatus": plan_status,
            "monthlyCredits": monthly,
            "purchasedCredits": purchased,
            "freeCredits": free,
            "totalCost": used,
            "totalCount": summary_value
                .as_ref()
                .and_then(|value| value.get("totalCount"))
                .and_then(number),
        }),
    })
}

/// DeepSeek `/user/balance`：金额为字符串，每种币种一条；第一条是主展示余额。
pub fn parse_deepseek(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let infos = value
        .get("balance_infos")
        .filter(|infos| infos.is_array())
        .ok_or_else(invalid_payload)?;
    let balances: Vec<Value> = infos.as_array().cloned().unwrap_or_default();
    let first = balances.first();
    let remaining = first
        .and_then(|entry| entry.get("total_balance"))
        .and_then(number);
    let currency = first
        .and_then(|entry| entry.get("currency"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let detail = json!({
        "is_available": value.get("is_available").cloned().unwrap_or(Value::Null),
        "balances": balances,
    });
    if first.is_none() && value.get("is_available").and_then(Value::as_bool) == Some(true) {
        return Err(invalid_payload());
    }
    Ok(BalanceReading {
        remaining,
        currency,
        unlimited: false,
        detail,
        ..BalanceReading::default()
    })
}

/// 窗口构造的唯一出口：`used`/`remaining` 百分比与已归一化的 `resets_at`
/// 都由调用方算好，这里只做纯组装。
///
/// 各上游的归一化规则不同（有的夹 used、有的夹 remaining、OpenCode 的
/// `resetsAt` 已是 RFC3339 需直接透传），因此不做统一夹取，避免改动读数。
fn quota_window(
    label: &str,
    used_percent: f64,
    remaining_percent: f64,
    resets_at: Option<String>,
) -> QuotaWindow {
    QuotaWindow {
        label: label.into(),
        used_percent,
        remaining_percent,
        resets_at,
    }
}

/// 上游成功响应里一个窗口都没有 = 无效负载（形状变更或该套餐无窗口）。
fn require_windows(windows: Vec<QuotaWindow>) -> Result<Vec<QuotaWindow>, BalanceError> {
    if windows.is_empty() {
        return Err(invalid_payload());
    }
    Ok(windows)
}

/// 由上游 “已用” 百分比构造窗口（夹取到 0–100）。
fn used_window(label: &str, used_percent: f64, reset: Option<&Value>) -> QuotaWindow {
    let used_percent = used_percent.clamp(0.0, 100.0);
    quota_window(
        label,
        used_percent,
        100.0 - used_percent,
        reset.and_then(quota_reset_at),
    )
}

/// 由上游 “剩余” 百分比构造窗口（夹取到 0–100）。
fn percent_window(label: &str, remaining_percent: f64, reset: Option<&Value>) -> QuotaWindow {
    let remaining_percent = remaining_percent.clamp(0.0, 100.0);
    quota_window(
        label,
        100.0 - remaining_percent,
        remaining_percent,
        reset.and_then(quota_reset_at),
    )
}

/// `{limit, remaining, resetTime}` → 已用百分比窗口（`limit <= 0` 时无读数）。
fn quota_from_limit_pair(value: &Value, label: &str) -> Option<QuotaWindow> {
    let limit = value.get("limit").and_then(number)?;
    let remaining = value.get("remaining").and_then(number)?;
    if limit <= 0.0 {
        return None;
    }
    Some(used_window(
        label,
        (limit - remaining) / limit * 100.0,
        value.get("resetTime"),
    ))
}

/// SiliconFlow `/v1/user/info`：`data.totalBalance`；币种由域名决定
/// （`.cn` 计 CNY，`.com` 计 USD）。
pub fn parse_siliconflow(body: &[u8], host: &str) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let remaining = data
        .get("totalBalance")
        .and_then(number)
        .ok_or_else(invalid_payload)?;
    let currency = if host.contains("siliconflow.com") {
        "USD"
    } else {
        "CNY"
    };
    Ok(BalanceReading {
        remaining: Some(remaining),
        currency: Some(currency.into()),
        detail: json!({
            "balance": data.get("balance").cloned().unwrap_or(Value::Null),
            "chargeBalance": data.get("chargeBalance").cloned().unwrap_or(Value::Null),
        }),
        ..BalanceReading::default()
    })
}

/// StepFun `/v1/accounts`：预付费账户余额（CNY）。Step Plan 的月度额度池没有
/// 公开端点（仅控制台），所以这里是账户余额，不是订阅额度。
pub fn parse_stepfun(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let remaining = value
        .get("balance")
        .and_then(number)
        .ok_or_else(invalid_payload)?;
    Ok(BalanceReading {
        remaining: Some(remaining),
        currency: Some("CNY".into()),
        detail: json!({
            "type": value.get("type").cloned().unwrap_or(Value::Null),
            "total_cash_balance": value.get("total_cash_balance").cloned().unwrap_or(Value::Null),
            "total_voucher_balance": value
                .get("total_voucher_balance")
                .cloned()
                .unwrap_or(Value::Null),
        }),
        ..BalanceReading::default()
    })
}

/// Novita AI `/v3/user/balance`：所有金额均以 0.0001 USD 为单位。
pub fn parse_novita(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let scaled = |field: &str| {
        value
            .get(field)
            .and_then(number)
            .map(|amount| amount / NOVITA_UNITS_PER_USD)
    };
    let remaining = scaled("availableBalance").ok_or_else(invalid_payload)?;
    Ok(BalanceReading {
        remaining: Some(remaining),
        currency: Some("USD".into()),
        detail: json!({
            "cashBalance": scaled("cashBalance"),
            "creditLimit": scaled("creditLimit"),
            "outstandingInvoices": scaled("outstandingInvoices"),
        }),
        ..BalanceReading::default()
    })
}

/// Moonshot（Kimi 开放平台）`/v1/users/me/balance`：CNY 账户余额。
pub fn parse_moonshot(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let remaining = data
        .get("available_balance")
        .and_then(number)
        .ok_or_else(invalid_payload)?;
    Ok(BalanceReading {
        remaining: Some(remaining),
        currency: Some("CNY".into()),
        detail: json!({
            "voucher_balance": data.get("voucher_balance").cloned().unwrap_or(Value::Null),
            "cash_balance": data.get("cash_balance").cloned().unwrap_or(Value::Null),
        }),
        ..BalanceReading::default()
    })
}

/// 智谱 GLM Coding Plan `/api/monitor/usage/quota/limit`：以已用百分比表示的
/// 订阅窗口（`unit` 3 = 5 小时，6 = 周；缺失时按数组顺序退化为 5h / 周）。
/// `data.level` 作为读数标签。
pub fn parse_zhipu(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let mut windows = Vec::new();
    let mut untyped = 0usize;
    if let Some(limits) = data.get("limits").and_then(Value::as_array) {
        for item in limits {
            let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
            if !(kind.eq_ignore_ascii_case("TOKENS_LIMIT")
                || kind.eq_ignore_ascii_case("CREDIT_LIMIT"))
            {
                continue;
            }
            let Some(percentage) = item.get("percentage").and_then(number) else {
                continue;
            };
            let label = match item.get("unit").and_then(number).map(|unit| unit as i64) {
                Some(3) => "5h",
                Some(6) => "周",
                _ => {
                    let label = if untyped == 0 { "5h" } else { "周" };
                    untyped += 1;
                    label
                }
            };
            windows.push(used_window(label, percentage, item.get("nextResetTime")));
        }
    }
    Ok(BalanceReading {
        label: data.get("level").and_then(Value::as_str).map(str::to_owned),
        windows: require_windows(windows)?,
        detail: json!({"level": data.get("level").cloned().unwrap_or(Value::Null)}),
        ..BalanceReading::default()
    })
}

/// MiniMax Coding Plan 剩余额度端点 `/v1/api/openplatform/coding_plan/remains`：
/// `general` 模型的剩余百分比（5h 间隔 + 每周）。
pub fn parse_minimax(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let status = value
        .pointer("/base_resp/status_code")
        .and_then(number)
        .unwrap_or(0.0) as i64;
    if status != 0 {
        return Err(invalid_payload());
    }
    let mut windows = Vec::new();
    if let Some(remains) = value.get("model_remains").and_then(Value::as_array) {
        for item in remains {
            if item.get("model_name").and_then(Value::as_str) != Some("general") {
                continue;
            }
            if let Some(remaining) = item
                .get("current_interval_remaining_percent")
                .and_then(number)
            {
                windows.push(percent_window("5h", remaining, item.get("end_time")));
            }
            let weekly_active = item
                .get("current_weekly_status")
                .and_then(number)
                .map(|status| status as i64)
                == Some(1);
            if weekly_active
                && let Some(remaining) = item
                    .get("current_weekly_remaining_percent")
                    .and_then(number)
            {
                windows.push(percent_window("周", remaining, item.get("weekly_end_time")));
            }
        }
    }
    Ok(BalanceReading {
        windows: require_windows(windows)?,
        ..BalanceReading::default()
    })
}

/// Kimi For Coding `/coding/v1/usages`：5h 窗口取 `limits[0].detail`，
/// 每周窗口取顶层 `usage`（`limit`/`remaining` 对）。
pub fn parse_kimi_code(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let mut windows = Vec::new();
    let detail = value
        .get("limits")
        .and_then(Value::as_array)
        .and_then(|limits| limits.first())
        .and_then(|item| item.get("detail"))
        .filter(|detail| detail.is_object());
    if let Some(window) = detail.and_then(|detail| quota_from_limit_pair(detail, "5h")) {
        windows.push(window);
    }
    if let Some(usage) = value.get("usage").filter(|usage| usage.is_object())
        && let Some(window) = quota_from_limit_pair(usage, "周")
    {
        windows.push(window);
    }
    Ok(BalanceReading {
        windows: require_windows(windows)?,
        ..BalanceReading::default()
    })
}

/// OpenRouter：两种视图共用一个解析器。`data.total_credits`（Management
/// Key → `/api/v1/credits`）是账户视图（`remaining = credits - usage`，USD）；
/// 否则 `data.limit_remaining`/`data.usage`（任意 key → `/api/v1/key`）是
/// key 视图（`limit` 为 null 表示无限）。
pub fn parse_openrouter(body: &[u8]) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let data = value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let detail = json!({
        "is_free_tier": data.get("is_free_tier").cloned().unwrap_or(Value::Null),
        "label": data.get("label").cloned().unwrap_or(Value::Null),
        "limit": data.get("limit").cloned().unwrap_or(Value::Null),
        "limit_remaining": data.get("limit_remaining").cloned().unwrap_or(Value::Null),
        "usage": data.get("usage").cloned().unwrap_or(Value::Null),
        "total_credits": data.get("total_credits").cloned().unwrap_or(Value::Null),
        "total_usage": data.get("total_usage").cloned().unwrap_or(Value::Null),
    });
    if let Some(total_credits) = data.get("total_credits").and_then(number) {
        let total_usage = data.get("total_usage").and_then(number).unwrap_or(0.0);
        return Ok(BalanceReading {
            remaining: Some(total_credits - total_usage),
            currency: Some("USD".into()),
            used: Some(total_usage),
            total: Some(total_credits),
            unlimited: false,
            label: None,
            windows: Vec::new(),
            detail,
        });
    }
    let remaining = data.get("limit_remaining").and_then(number);
    let used = data.get("usage").and_then(number);
    let limit = data.get("limit").and_then(number);
    if remaining.is_none() && used.is_none() && limit.is_none() {
        return Err(invalid_payload());
    }
    Ok(BalanceReading {
        remaining,
        currency: Some("USD".into()),
        used,
        total: limit,
        unlimited: limit.is_none(),
        label: data.get("label").and_then(Value::as_str).map(str::to_owned),
        windows: Vec::new(),
        detail,
    })
}
/// 小米 MiMo Token Plan：`tokenPlan/usage` + `balance`（另可选 detail 步骤
/// 即 `tokenPlan/detail`）。
///
/// - `data.usage.items[]`：`plan_total_token` → 「套餐积分」、
///   `compensation_total_token`（`limit > 0`）→ 「补偿积分」；
/// - items 缺失时回落 `data.usage.percent`（0–1 分数）生成单个「套餐积分」窗口；
/// - `data.balance`（字符串数字）→ `remaining`，币种取 `data.currency`；
/// - `detail.data.planName`/`currentPeriodEnd` → 标签与窗口 `resets_at`。
pub fn parse_mimo(
    usage: &[u8],
    balance: &[u8],
    detail: Option<&[u8]>,
) -> Result<BalanceReading, BalanceError> {
    let usage_value: Value = decode_body(usage)?;
    let balance_value: Value = decode_body(balance)?;
    let usage_data = usage_value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let balance_data = balance_value
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(invalid_payload)?;
    let detail_data = detail
        .and_then(|body| serde_json::from_slice::<Value>(body).ok())
        .and_then(|value| value.get("data").filter(|data| data.is_object()).cloned());
    let resets_at = detail_data
        .as_ref()
        .and_then(|data| data.get("currentPeriodEnd"))
        .filter(|value| !value.is_null());
    let mut windows = Vec::new();
    if let Some(items) = usage_data
        .pointer("/usage/items")
        .and_then(Value::as_array)
    {
        for item in items {
            let (Some(used), Some(limit)) = (
                item.get("used").and_then(number),
                item.get("limit").and_then(number),
            ) else {
                continue;
            };
            if limit <= 0.0 {
                continue;
            }
            let label = match item.get("name").and_then(Value::as_str) {
                Some("plan_total_token") => "套餐积分",
                Some("compensation_total_token") => "补偿积分",
                _ => continue,
            };
            windows.push(used_window(label, used / limit * 100.0, resets_at));
        }
    }
    if windows.is_empty()
        && let Some(percent) = usage_data.pointer("/usage/percent").and_then(number)
    {
        // 分数（0–1）形式的整体用量。
        windows.push(used_window("套餐积分", percent * 100.0, resets_at));
    }
    let remaining = balance_data
        .get("balance")
        .and_then(number)
        .ok_or_else(invalid_payload)?;
    let label = detail_data.as_ref().and_then(|data| {
        let plan = data.get("planName").and_then(Value::as_str)?;
        let period = data
            .get("currentPeriodEnd")
            .and_then(Value::as_str)
            .map(|end| end.chars().take(10).collect::<String>());
        Some(match period {
            Some(period) => format!("MiMo {plan}（到期 {period}）"),
            None => format!("MiMo {plan}"),
        })
    });
    Ok(BalanceReading {
        remaining: Some(remaining),
        currency: Some(
            balance_data
                .get("currency")
                .and_then(Value::as_str)
                .unwrap_or("CNY")
                .to_owned(),
        ),
        unlimited: false,
        label,
        windows,
        detail: json!({
            "planName": detail_data.as_ref().and_then(|data| data.get("planName")).cloned().unwrap_or(Value::Null),
            "planCode": detail_data.as_ref().and_then(|data| data.get("planCode")).cloned().unwrap_or(Value::Null),
            "currentPeriodEnd": detail_data
                .as_ref()
                .and_then(|data| data.get("currentPeriodEnd"))
                .cloned()
                .unwrap_or(Value::Null),
            "cashBalance": balance_data.get("cashBalance").cloned().unwrap_or(Value::Null),
            "giftBalance": balance_data.get("giftBalance").cloned().unwrap_or(Value::Null),
        }),
        ..BalanceReading::default()
    })
}

/// `custom` 适配器：按配置的 JSON 路径抽取字段。
pub fn parse_custom(body: &[u8], mapping: &BalanceMapping) -> Result<BalanceReading, BalanceError> {
    let value: Value = decode_body(body)?;
    let read = |path: &Option<String>| -> Option<Value> {
        path.as_deref()
            .and_then(|path| json_path(&value, path))
            .cloned()
    };
    let remaining = read(&mapping.remaining).as_ref().and_then(number);
    let used = read(&mapping.used).as_ref().and_then(number);
    let total = read(&mapping.total).as_ref().and_then(number);
    if remaining.is_none() && used.is_none() && total.is_none() {
        return Err(invalid_payload());
    }
    Ok(BalanceReading {
        remaining,
        currency: read(&mapping.currency).and_then(|value| value.as_str().map(str::to_owned)),
        used,
        total,
        unlimited: false,
        label: read(&mapping.label).and_then(|value| match value {
            Value::String(label) => Some(label),
            Value::Number(label) => Some(label.to_string()),
            _ => None,
        }),
        windows: Vec::new(),
        detail: Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;
    use crate::ports::{UpstreamBody, UpstreamResponse};
    use crate::runtime::RuntimeLimits;
    use crate::test_support::TempDir;
    use futures_util::future::BoxFuture;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio_util::sync::CancellationToken;

    // ---------------------------------------------------------------- 纯函数

    #[test]
    fn resolve_url_handles_absolute_root_relative_and_query() {
        let base = "https://relay.example.com/zen/go";
        assert_eq!(
            resolve_url(base, "https://absolute.example.com/api?x=1")
                .unwrap()
                .as_str(),
            "https://absolute.example.com/api?x=1"
        );
        assert_eq!(
            resolve_url(base, "/api/usage/token/").unwrap().as_str(),
            "https://relay.example.com/api/usage/token/"
        );
        assert_eq!(
            resolve_url(base, "v1/usage").unwrap().as_str(),
            "https://relay.example.com/zen/go/v1/usage"
        );
        assert_eq!(
            resolve_url(base, "/user/balance?currency=USD")
                .unwrap()
                .as_str(),
            "https://relay.example.com/user/balance?currency=USD"
        );
        assert_eq!(
            resolve_url(base, "v1/usage?window=month").unwrap().as_str(),
            "https://relay.example.com/zen/go/v1/usage?window=month"
        );
        assert!(resolve_url(base, "  ").is_err());
        assert!(resolve_url(base, "ftp://host/x").is_err());
    }

    const NEWAPI_BODY: &str = r#"{"code":true,"message":"ok","data":{"object":"token_usage","name":"默认令牌","total_granted":5000000,"total_used":1250000,"total_available":3750000,"unlimited_quota":false,"expires_at":0}}"#;

    #[test]
    fn newapi_converts_quota_and_handles_unlimited() {
        let reading = parse_newapi(NEWAPI_BODY.as_bytes()).unwrap();
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.remaining, Some(7.5));
        assert_eq!(reading.used, Some(2.5));
        assert_eq!(reading.total, Some(10.0));
        assert!(!reading.unlimited);
        assert_eq!(reading.label.as_deref(), Some("默认令牌"));

        let body = r#"{"data":{"total_granted":-1,"total_used":10,"total_available":-11,"unlimited_quota":true}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(reading.unlimited, "unlimited_quota must win");
        assert_eq!(reading.remaining, None);

        // 仅 total_granted < 0 也算无限。
        let body = r#"{"data":{"total_granted":-1,"total_used":10}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(reading.unlimited);
        assert_eq!(reading.remaining, None);

        // 具体且非负的余额优先，即使上游置了 unlimited 标志
        // （分支数据：标志为真但额度是数值）。
        let body =
            r#"{"data":{"total_granted":5000000,"total_used":1250000,"unlimited_quota":true}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        assert_eq!(reading.remaining, Some(7.5));

        // ……包括 total_granted 用了 -1 哨兵值、但存在真实 available 金额的情况。
        let body =
            r#"{"data":{"total_granted":-1,"total_available":3750000,"unlimited_quota":true}}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        assert_eq!(reading.remaining, Some(7.5));

        // 真实 CCTQ 形状：套餐透支（available < 0），但 total_granted > 0 证明额度
        // 是数值。`unlimited_quota` 绝不能隐藏具体的（负的）available 金额。
        let body = r#"{"code":true,"data":{"name":"codex","total_granted":3276396,"total_used":4640192,"total_available":-1363796,"unlimited_quota":true},"message":"ok"}"#;
        let reading = parse_newapi(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        let remaining = reading.remaining.expect("negative available must be kept");
        assert!((remaining + 2.727592).abs() < 1e-9, "remaining={remaining}");
        assert!((reading.total.unwrap() - 6.552792).abs() < 1e-9);
        assert!((reading.used.unwrap() - 9.280384).abs() < 1e-9);

        assert!(parse_newapi(b"{}").is_err());
        assert!(parse_newapi(b"not json").is_err());
    }

    #[test]
    fn newapi_user_reads_account_balance() {
        let body = r#"{"success":true,"message":"","data":{"username":"alice","display_name":"Alice","quota":12925000,"used_quota":4640192,"request_count":321,"group":"default"}}"#;
        let reading = parse_newapi_user(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(25.85));
        assert_eq!(reading.used, Some(9.280384));
        let total = reading.total.unwrap();
        assert!((total - 35.130384).abs() < 1e-9, "total={total}");
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.label.as_deref(), Some("Alice"));
        assert_eq!(reading.detail["request_count"].as_f64(), Some(321.0));

        let body = r#"{"success":true,"data":{"username":"bob","quota":500000,"used_quota":0}}"#;
        let reading = parse_newapi_user(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(1.0));
        assert_eq!(reading.label.as_deref(), Some("bob"));

        assert!(parse_newapi_user(b"{}").is_err());
        assert!(parse_newapi_user(b"not json").is_err());
    }

    #[test]
    fn sub2api_reads_limited_and_unrestricted_modes() {
        let body = r#"{"mode":"quota_limited","isValid":true,"status":"active","quota":{"limit":10,"used":1.2,"remaining":8.8,"unit":"USD"},"remaining":8.8,"unit":"USD","usage":{"today":{"requests":12,"cost":0.3,"tokens":15234},"total":{"requests":300,"cost":1.2,"tokens":402133}}}"#;
        let reading = parse_sub2api(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(8.8));
        assert_eq!(reading.used, Some(1.2));
        assert_eq!(reading.total, Some(10.0));
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.label.as_deref(), Some("active"));
        assert_eq!(reading.detail["usage"]["total"]["cost"], 1.2);

        let body = r#"{"mode":"unrestricted","isValid":true,"status":"active"}"#;
        let reading = parse_sub2api(body.as_bytes()).unwrap();
        assert!(reading.unlimited);
        assert_eq!(reading.remaining, None);

        // unrestricted 模式下有具体数字时仍会展示。
        let body = r#"{"mode":"unrestricted","isValid":true,"status":"active","remaining":12.5,"unit":"USD"}"#;
        let reading = parse_sub2api(body.as_bytes()).unwrap();
        assert!(!reading.unlimited);
        assert_eq!(reading.remaining, Some(12.5));
        assert_eq!(reading.currency.as_deref(), Some("USD"));

        assert!(parse_sub2api(b"{\"mode\":\"quota_limited\"}").is_err());
    }

    #[test]
    fn deepseek_reads_string_amounts_and_lists_currencies() {
        let body = r#"{"is_available":true,"balance_infos":[{"currency":"CNY","total_balance":"110.00","granted_balance":"10.00","topped_up_balance":"100.00"}]}"#;
        let reading = parse_deepseek(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(110.0));
        assert_eq!(reading.currency.as_deref(), Some("CNY"));
        assert_eq!(reading.detail["balances"].as_array().unwrap().len(), 1);
        assert_eq!(reading.detail["balances"][0]["granted_balance"], "10.00");

        let body = r#"{"is_available":false,"balance_infos":[]}"#;
        let reading = parse_deepseek(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, None);
        assert_eq!(reading.detail["is_available"], false);

        assert!(parse_deepseek(b"{}").is_err());
    }

    const OPENCODE_BODY: &str = r#"{"usage":{"rolling":{"status":"ok","percent":37.5,"resetsAt":"2026-09-11T20:00:00Z"},"weekly":{"status":"error","percent":12.0,"resetsAt":null},"monthly":{"status":"ok","percent":4.5,"resetsAt":"2026-10-01T00:00:00Z"}}}"#;

    #[test]
    fn opencode_windows_convert_percent_and_keep_stable_order() {
        let reading = parse_opencode_go(OPENCODE_BODY.as_bytes()).unwrap();
        let labels: Vec<&str> = reading
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "月"], "non-ok windows are skipped");
        assert_eq!(reading.windows[0].used_percent, 37.5);
        assert_eq!(reading.windows[0].remaining_percent, 62.5);
        assert_eq!(reading.windows[1].remaining_percent, 95.5);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-11T20:00:00Z")
        );

        let body = r#"{"usage":{"rolling":{"status":"ok","percent":120},"weekly":{"status":"ok","percent":-1}}}"#;
        assert!(parse_opencode_go(body.as_bytes()).is_err());
        assert!(parse_opencode_go(b"{}").is_err());
    }

    const ZHIPU_BODY: &str = r#"{"data":{"level":"pro","limits":[{"type":"TOKENS_LIMIT","unit":3,"percentage":12.5,"nextResetTime":1790000000000},{"type":"TOKENS_LIMIT","unit":6,"percentage":40,"nextResetTime":1795000000000}]}}"#;

    #[test]
    fn zhipu_windows_classify_unit_and_fall_back_to_array_order() {
        let reading = parse_zhipu(ZHIPU_BODY.as_bytes()).unwrap();
        assert_eq!(reading.label.as_deref(), Some("pro"));
        let labels: Vec<&str> = reading
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "周"]);
        assert_eq!(reading.windows[0].used_percent, 12.5);
        assert_eq!(reading.windows[0].remaining_percent, 87.5);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-21T14:13:20+00:00"),
            "millisecond nextResetTime becomes RFC3339"
        );
        assert_eq!(reading.windows[1].used_percent, 40.0);

        // 没有 `unit`：按数组顺序退化为 5h / 周。其它 limit 类型
        // （以及没有 percentage 的条目）会被跳过。
        let body = r#"{"data":{"level":"lite","limits":[{"type":"OTHER","unit":3,"percentage":9},{"type":"CREDIT_LIMIT","percentage":25},{"type":"TOKENS_LIMIT","percentage":50}]}}"#;
        let reading = parse_zhipu(body.as_bytes()).unwrap();
        let labels: Vec<&str> = reading
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "周"]);
        assert_eq!(reading.windows[0].used_percent, 25.0);
        assert!(reading.windows[0].resets_at.is_none());

        assert!(parse_zhipu(b"{}").is_err());
        assert!(parse_zhipu(br#"{"data":{"limits":[]}}"#).is_err());
    }

    #[test]
    fn minimax_reads_interval_and_weekly_remaining_percent() {
        let body = r#"{"base_resp":{"status_code":0},"model_remains":[{"model_name":"general","current_interval_remaining_percent":80.0,"end_time":1790000000000,"current_weekly_status":1,"current_weekly_remaining_percent":50.0,"weekly_end_time":1795000000000}]}"#;
        let reading = parse_minimax(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, None, "subscription windows only");
        let labels: Vec<&str> = reading
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "周"]);
        assert_eq!(reading.windows[0].remaining_percent, 80.0);
        assert_eq!(reading.windows[0].used_percent, 20.0);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-21T14:13:20+00:00")
        );
        assert_eq!(reading.windows[1].remaining_percent, 50.0);
        assert_eq!(
            reading.windows[1].resets_at.as_deref(),
            Some("2026-11-18T11:06:40+00:00")
        );

        // 每周额度池未激活：只保留间隔窗口。
        let body = r#"{"base_resp":{"status_code":0},"model_remains":[{"model_name":"general","current_interval_remaining_percent":10,"current_weekly_status":0,"current_weekly_remaining_percent":99}]}"#;
        let reading = parse_minimax(body.as_bytes()).unwrap();
        assert_eq!(reading.windows.len(), 1);
        assert_eq!(reading.windows[0].remaining_percent, 10.0);

        let error = parse_minimax(br#"{"base_resp":{"status_code":1004}}"#).unwrap_err();
        assert_eq!(error.to_string(), "invalid_payload");
        assert!(parse_minimax(br#"{"base_resp":{"status_code":0}}"#).is_err());
    }

    #[test]
    fn kimi_code_reads_limit_and_usage_windows() {
        let body = r#"{"limits":[{"detail":{"limit":100,"remaining":70,"resetTime":"2026-09-27T06:00:00Z"}}],"usage":{"limit":1000,"remaining":250,"resetTime":"2026-10-01T00:00:00Z"}}"#;
        let reading = parse_kimi_code(body.as_bytes()).unwrap();
        let labels: Vec<&str> = reading
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "周"]);
        assert_eq!(reading.windows[0].used_percent, 30.0);
        assert_eq!(reading.windows[0].remaining_percent, 70.0);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-27T06:00:00+00:00")
        );
        assert_eq!(reading.windows[1].used_percent, 75.0);

        // 透支的窗口夹取到 0% 剩余，而不是变成负数。
        let body = r#"{"usage":{"limit":100,"remaining":-20}}"#;
        let reading = parse_kimi_code(body.as_bytes()).unwrap();
        assert_eq!(reading.windows[0].used_percent, 100.0);
        assert_eq!(reading.windows[0].remaining_percent, 0.0);

        assert!(parse_kimi_code(b"{}").is_err());
        assert!(parse_kimi_code(br#"{"limits":[]}"#).is_err());
    }

    #[test]
    fn moonshot_and_stepfun_read_cny_balances() {
        let body = r#"{"code":0,"data":{"available_balance":49.58894,"voucher_balance":46.58893,"cash_balance":3.00001},"scode":"0x0","status":true}"#;
        let reading = parse_moonshot(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(49.58894));
        assert_eq!(reading.currency.as_deref(), Some("CNY"));
        assert_eq!(reading.detail["voucher_balance"], 46.58893);
        assert!(parse_moonshot(b"{}").is_err());

        let body = r#"{"object":"account","type":"prepaid","balance":8.0,"total_cash_balance":0,"total_voucher_balance":26.0}"#;
        let reading = parse_stepfun(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(8.0));
        assert_eq!(reading.currency.as_deref(), Some("CNY"));
        assert_eq!(reading.detail["type"], "prepaid");
        assert_eq!(reading.detail["total_voucher_balance"], 26.0);
        assert!(parse_stepfun(br#"{"object":"account"}"#).is_err());
    }

    #[test]
    fn novita_scales_ten_thousand_units_to_usd() {
        let reading = parse_novita(br#"{"availableBalance":123456}"#).unwrap();
        assert_eq!(reading.remaining, Some(12.3456));
        assert_eq!(reading.currency.as_deref(), Some("USD"));

        let body = r#"{"availableBalance":10000,"cashBalance":5000,"creditLimit":20000,"outstandingInvoices":1500}"#;
        let reading = parse_novita(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(1.0));
        assert_eq!(reading.detail["cashBalance"], 0.5);
        assert_eq!(reading.detail["creditLimit"], 2.0);
        assert_eq!(reading.detail["outstandingInvoices"], 0.15);
        assert!(parse_novita(br#"{"cashBalance":1}"#).is_err());
    }

    #[test]
    fn siliconflow_currency_follows_the_site_domain() {
        let body = r#"{"code":20000,"data":{"totalBalance":12.5,"balance":10.0,"chargeBalance":2.5}}"#;
        let reading = parse_siliconflow(body.as_bytes(), "api.siliconflow.cn").unwrap();
        assert_eq!(reading.remaining, Some(12.5));
        assert_eq!(reading.currency.as_deref(), Some("CNY"));
        assert_eq!(reading.detail["chargeBalance"], 2.5);
        let reading = parse_siliconflow(body.as_bytes(), "api.siliconflow.com").unwrap();
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        let reading = parse_siliconflow(body.as_bytes(), "siliconflow.internal").unwrap();
        assert_eq!(reading.currency.as_deref(), Some("CNY"));

        assert!(parse_siliconflow(br#"{"code":20000}"#, "api.siliconflow.cn").is_err());
    }

    #[test]
    fn openrouter_account_and_key_views() {
        let body = r#"{"data":{"total_credits":100,"total_usage":37.5}}"#;
        let reading = parse_openrouter(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(62.5));
        assert_eq!(reading.used, Some(37.5));
        assert_eq!(reading.total, Some(100.0));
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert!(!reading.unlimited);

        let body = r#"{"data":{"label":"sk-or-v1-abc","usage":3.5,"limit":10,"limit_remaining":6.5,"is_free_tier":false}}"#;
        let reading = parse_openrouter(body.as_bytes()).unwrap();
        assert_eq!(reading.remaining, Some(6.5));
        assert_eq!(reading.total, Some(10.0));
        assert!(!reading.unlimited);
        assert_eq!(reading.label.as_deref(), Some("sk-or-v1-abc"));

        // 没有 limit 的 key 为无限（无剩余计数器）。
        let body = r#"{"data":{"label":"sk-or-v1-free","usage":0,"limit":null,"limit_remaining":null,"is_free_tier":true}}"#;
        let reading = parse_openrouter(body.as_bytes()).unwrap();
        assert!(reading.unlimited);
        assert_eq!(reading.remaining, None);
        assert!(parse_openrouter(b"{}").is_err());
        assert!(parse_openrouter(br#"{"data":{"is_free_tier":true}}"#).is_err());
    }

    #[test]
    fn mimo_three_step_reading_uses_cookie_plan_and_balance() {
        const USAGE: &str = r#"{"code":0,"data":{"usage":{"percent":0.42,"items":[{"name":"plan_total_token","used":1100000000,"limit":11000000000},{"name":"compensation_total_token","used":0,"limit":0}]}}}"#;
        const BALANCE: &str = r#"{"code":0,"data":{"balance":"12.34","cashBalance":"10.00","giftBalance":"2.34","currency":"CNY"}}"#;
        const DETAIL: &str = r#"{"code":0,"data":{"planName":"Pro","planCode":"pro:month","currentPeriodEnd":"2026-10-01T00:00:00Z"}}"#;
        let reading =
            parse_mimo(USAGE.as_bytes(), BALANCE.as_bytes(), Some(DETAIL.as_bytes())).unwrap();
        assert_eq!(reading.remaining, Some(12.34));
        assert_eq!(reading.currency.as_deref(), Some("CNY"));
        assert_eq!(
            reading.label.as_deref(),
            Some("MiMo Pro（到期 2026-10-01）")
        );
        assert_eq!(reading.windows.len(), 1, "zero-limit compensation is skipped");
        assert_eq!(reading.windows[0].label, "套餐积分");
        assert_eq!(reading.windows[0].used_percent, 10.0);
        assert_eq!(reading.windows[0].remaining_percent, 90.0);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-10-01T00:00:00+00:00")
        );
        assert_eq!(reading.detail["cashBalance"], "10.00");

        // 没有可选 detail 步骤时：回落到百分比窗口，无标签。
        let reading = parse_mimo(USAGE.as_bytes(), BALANCE.as_bytes(), None).unwrap();
        assert_eq!(reading.label, None);
        assert_eq!(reading.windows[0].used_percent, 10.0);

        // 完全没有 items 时：0–1 分数变成单个窗口。
        let usage = r#"{"code":0,"data":{"usage":{"percent":0.42}}}"#;
        let reading = parse_mimo(usage.as_bytes(), BALANCE.as_bytes(), None).unwrap();
        assert_eq!(reading.windows.len(), 1);
        assert_eq!(reading.windows[0].used_percent, 42.0);

        assert!(parse_mimo(b"{}", BALANCE.as_bytes(), None).is_err());
        assert!(parse_mimo(USAGE.as_bytes(), b"{}", None).is_err());
    }

    #[test]
    fn new_adapter_ids_round_trip_with_their_presets() {
        let adapters = [
            BalanceAdapter::Mimo,
            BalanceAdapter::OpenRouter,
            BalanceAdapter::SiliconFlow,
            BalanceAdapter::StepFun,
            BalanceAdapter::Novita,
            BalanceAdapter::Moonshot,
            BalanceAdapter::ZhipuGlm,
            BalanceAdapter::MiniMax,
            BalanceAdapter::KimiCode,
        ];
        for adapter in adapters {
            assert_eq!(BalanceAdapter::parse(adapter.as_str()).unwrap(), adapter);
        }
        assert_eq!(BalanceAdapter::Mimo.as_str(), "mimo");
        assert_eq!(BalanceAdapter::OpenRouter.as_str(), "openrouter");
        assert_eq!(BalanceAdapter::SiliconFlow.as_str(), "siliconflow");
        assert_eq!(BalanceAdapter::StepFun.as_str(), "stepfun");
        assert_eq!(BalanceAdapter::Novita.as_str(), "novita");
        assert_eq!(BalanceAdapter::Moonshot.as_str(), "moonshot");
        assert_eq!(BalanceAdapter::ZhipuGlm.as_str(), "zhipu");
        assert_eq!(BalanceAdapter::MiniMax.as_str(), "minimax");
        assert_eq!(BalanceAdapter::KimiCode.as_str(), "kimi_code");

        assert_eq!(BalanceAdapter::Mimo.default_path(), None);
        assert_eq!(BalanceAdapter::OpenRouter.default_path(), None);
        assert_eq!(
            BalanceAdapter::SiliconFlow.default_path(),
            Some("/v1/user/info")
        );
        assert_eq!(BalanceAdapter::StepFun.default_path(), Some("/v1/accounts"));
        assert_eq!(
            BalanceAdapter::Novita.default_path(),
            Some("https://api.novita.ai/v3/user/balance")
        );
        assert_eq!(
            BalanceAdapter::Moonshot.default_path(),
            Some("/v1/users/me/balance")
        );
        assert_eq!(
            BalanceAdapter::ZhipuGlm.default_path(),
            Some("/api/monitor/usage/quota/limit")
        );
        assert_eq!(
            BalanceAdapter::MiniMax.default_path(),
            Some("https://api.minimaxi.com/v1/api/openplatform/coding_plan/remains")
        );
        assert_eq!(
            BalanceAdapter::KimiCode.default_path(),
            Some("https://api.kimi.com/coding/v1/usages")
        );
    }

    #[test]
    fn zhipu_config_stores_raw_auth_and_others_store_bearer() {
        let (config, _) = normalize_config(&base_input("zhipu", true)).unwrap();
        assert_eq!(config.auth, BalanceAuth::Raw);
        assert_eq!(config.auth.as_str(), "raw");
        assert_eq!(
            config.path.as_deref(),
            Some("/api/monitor/usage/quota/limit")
        );
        assert_eq!(BalanceAuth::parse("raw").unwrap(), BalanceAuth::Raw);
        assert_eq!(BalanceAuth::parse(" RAW ").unwrap(), BalanceAuth::Raw);

        let (config, _) = normalize_config(&base_input("minimax", true)).unwrap();
        assert_eq!(config.auth, BalanceAuth::Bearer);
        let mut custom = base_input("custom", true);
        custom.path = Some("/v1/usage".into());
        let (config, _) = normalize_config(&custom).unwrap();
        assert_eq!(config.auth, BalanceAuth::Bearer, "custom defaults to bearer");
        custom.auth = Some("raw".into());
        let (config, _) = normalize_config(&custom).unwrap();
        assert_eq!(config.auth, BalanceAuth::Raw, "custom may pick raw explicitly");
    }

    #[test]
    fn json_path_supports_documented_subset() {
        let value = serde_json::json!({"data":{"items":[{"balance":"12.34"}],"flag":true}});
        assert_eq!(
            json_path(&value, "$.data.items[0].balance").unwrap(),
            &serde_json::json!("12.34")
        );
        assert_eq!(json_path(&value, "$").unwrap(), &value);
        assert!(json_path(&value, "$.data.items[1]").is_none());
        assert!(json_path(&value, "data.items").is_none());
        assert!(json_path(&value, "$.data.items[x]").is_none());
    }

    #[test]
    fn numbers_accept_strings_and_reject_non_finite() {
        assert_eq!(number(&serde_json::json!(12.5)), Some(12.5));
        assert_eq!(number(&serde_json::json!(" 12.50 ")), Some(12.5));
        assert_eq!(number(&serde_json::json!("abc")), None);
        assert_eq!(number(&serde_json::json!(null)), None);
    }

    #[test]
    fn render_template_replaces_both_placeholders() {
        let rendered = render_template(
            "Bearer ${token}; key=${api_key}",
            "sk-channel",
            "panel-token",
        );
        assert_eq!(rendered, "Bearer panel-token; key=sk-channel");
        assert_eq!(render_template("plain", "a", "b"), "plain");
    }

    #[test]
    fn custom_parser_maps_optional_fields() {
        let body = br#"{"result":{"left":"8.80","unit":"USD","spent":1.2}}"#;
        let mapping = BalanceMapping {
            remaining: Some("$.result.left".into()),
            currency: Some("$.result.unit".into()),
            used: Some("$.result.spent".into()),
            ..BalanceMapping::default()
        };
        let reading = parse_custom(body, &mapping).unwrap();
        assert_eq!(reading.remaining, Some(8.8));
        assert_eq!(reading.currency.as_deref(), Some("USD"));
        assert_eq!(reading.used, Some(1.2));
        assert_eq!(reading.total, None);

        let empty = BalanceMapping::default();
        assert!(parse_custom(body, &empty).is_err());
        assert!(parse_custom(b"nope", &mapping).is_err());
    }

    // ------------------------------------------------------- mock 上游

    #[derive(Clone)]
    struct RecordedRequest {
        method: Method,
        url: String,
        body: Option<Vec<u8>>,
        headers: HeaderMap,
        connect_timeout: Duration,
    }

    enum MockReply {
        Json(u16, String),
        Error(UpstreamError),
        Pending,
    }

    struct MockUpstream {
        calls: AtomicUsize,
        requests: Mutex<Vec<RecordedRequest>>,
        handler: Box<dyn Fn(&RecordedRequest) -> MockReply + Send + Sync>,
    }

    impl MockUpstream {
        fn new(
            handler: impl Fn(&RecordedRequest) -> MockReply + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                handler: Box::new(handler),
            })
        }

        fn always(status: u16, body: &'static str) -> Arc<Self> {
            Self::new(move |_| MockReply::Json(status, body.to_owned()))
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().clone()
        }
    }

    impl UpstreamClient for MockUpstream {
        fn send(
            &self,
            request: UpstreamRequest,
        ) -> BoxFuture<'static, Result<UpstreamResponse, UpstreamError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let recorded = RecordedRequest {
                method: request.method.clone(),
                url: request.url.to_string(),
                body: request.body.as_ref().map(|body| body.to_vec()),
                headers: request.headers.clone(),
                connect_timeout: request.connect_timeout,
            };
            self.requests.lock().push(recorded.clone());
            let reply = (self.handler)(&recorded);
            Box::pin(async move {
                match reply {
                    MockReply::Json(status, body) => Ok(UpstreamResponse {
                        status: StatusCode::from_u16(status).expect("valid test status"),
                        headers: HeaderMap::new(),
                        body: UpstreamBody::new(Box::pin(futures_util::stream::iter(vec![Ok(
                            Bytes::from(body),
                        )]))),
                    }),
                    MockReply::Error(error) => Err(error),
                    MockReply::Pending => {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        Err(UpstreamError::Transport("pending test upstream".into()))
                    }
                }
            })
        }
    }

    async fn test_state(
        mock: Arc<dyn UpstreamClient>,
        limits: RuntimeLimits,
    ) -> (AppState, TempDir) {
        // 统一夹具：临时目录 + 整套 Context；此处注入假上游与自定义运行限额。
        let crate::test_support::TestEnv {
            dir,
            context: state,
        } = crate::test_support::context_with("balance", mock, limits).await;
        (state, dir)
    }

    async fn seed_channel(state: &Context, base_url: &str) {
        crate::test_support::seed_provider(&state.db, "prov-1", "mock", base_url).await;
        crate::test_support::seed_channel(
            &state.db,
            &state.secrets,
            "ch-1",
            "prov-1",
            "openai_compatible",
            "sk-channel-secret",
        )
        .await;
    }

    fn base_input(adapter: &str, enabled: bool) -> BalanceConfigInput {
        BalanceConfigInput {
            adapter: adapter.into(),
            enabled,
            method: None,
            path: None,
            auth: None,
            headers: BTreeMap::new(),
            body: None,
            mapping: BalanceMappingInput::default(),
            token: None,
            clear_token: false,
        }
    }

    async fn save_balance(state: &AppState, adapter: &str, enabled: bool) -> BalanceConfig {
        state
            .balance
            .save_config("ch-1", base_input(adapter, enabled))
            .await
            .unwrap()
    }

    // ------------------------------------------------------ 服务路径

    #[tokio::test]
    async fn default_off_sends_no_request_at_all() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;

        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert!(refreshed.is_empty(), "no config row -> nothing to refresh");
        assert_eq!(mock.calls(), 0, "default-off must not touch the upstream");

        let error = state.balance.query(&state, "ch-1").await.unwrap_err();
        assert!(matches!(error, BalanceError::NotConfigured));
        assert_eq!(mock.calls(), 0, "manual query without config sends nothing");
    }

    #[tokio::test]
    async fn refresh_skips_disabled_but_manual_query_still_runs() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", false).await;

        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert!(refreshed.is_empty());
        assert_eq!(mock.calls(), 0, "enabled=0 is the background switch");

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(mock.calls(), 1, "manual query is explicit intent");

        save_balance(&state, "newapi", true).await;
        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert_eq!(refreshed.len(), 1);
        assert_eq!(mock.calls(), 2);
    }

    #[tokio::test]
    async fn refresh_enabled_covers_multiple_channels_in_parallel() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        let time = "2026-08-04T01:00:00+00:00";
        for index in 1..=3 {
            let provider_id = format!("prov-{index}");
            let channel_id = format!("ch-{index}");
            let base_url = format!("https://relay{index}.example.com");
            crate::test_support::seed_provider(
                &state.db,
                &provider_id,
                &provider_id,
                &base_url,
            )
            .await;
            crate::test_support::seed_channel(
                &state.db,
                &state.secrets,
                &channel_id,
                &provider_id,
                "openai_compatible",
                "sk-channel-secret",
            )
            .await;
            sqlx::query("INSERT INTO channel_balance_configs(channel_id,adapter,enabled,method,path,auth,headers_json,body_json,mapping_json,created_at,updated_at) VALUES(?,'newapi',1,'GET','/api/usage/token/','bearer',NULL,NULL,NULL,?,?)")
                .bind(&channel_id)
                .bind(time)
                .bind(time)
                .execute(state.db.pool())
                .await
                .unwrap();
        }

        let refreshed = state.balance.refresh_enabled(&state).await.unwrap();
        assert_eq!(refreshed.len(), 3, "all enabled channels are covered");
        assert_eq!(mock.calls(), 3);
        for index in 1..=3 {
            let stored = state
                .balance
                .snapshot(&format!("ch-{index}"))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.status, "ok");
            assert_eq!(stored.remaining, Some(7.5));
        }
    }

    #[tokio::test]
    async fn query_newapi_persists_normalized_snapshot() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(7.5));
        assert_eq!(snapshot.used, Some(2.5));
        assert_eq!(snapshot.total, Some(10.0));
        assert_eq!(snapshot.currency.as_deref(), Some("USD"));
        assert_eq!(snapshot.error_kind, None);
        assert_eq!(snapshot.status_code, Some(200));
        assert!(!snapshot.checked_at.is_empty());

        assert_eq!(
            mock.requests()[0].url,
            "https://relay.example.com/api/usage/token/"
        );
        let stored = state.balance.snapshot("ch-1").await.unwrap().unwrap();
        assert_eq!(stored.remaining, Some(7.5));
        assert_eq!(stored.status, "ok");

        let embedded = crate::balance::channel_balance_json(&state.db, "ch-1")
            .await
            .unwrap();
        assert_eq!(embedded["configured"], true);
        assert_eq!(embedded["enabled"], true);
        assert_eq!(embedded["snapshot"]["remaining"], 7.5);
    }

    #[tokio::test]
    async fn newapi_with_dedicated_token_queries_account_balance() {
        const USER_SELF_BODY: &str = r#"{"success":true,"message":"","data":{"username":"alice","display_name":"Alice","quota":12925000,"used_quota":4640192,"request_count":321,"group":"default"}}"#;
        let mock = MockUpstream::new(|request| {
            if request.url.ends_with("/api/user/self") {
                MockReply::Json(200, USER_SELF_BODY.to_owned())
            } else {
                MockReply::Json(200, NEWAPI_BODY.to_owned())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        let mut input = base_input("newapi", true);
        input.token = Some("cctq-pat-token".into());
        state.balance.save_config("ch-1", input).await.unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(25.85));
        assert_eq!(snapshot.used, Some(9.280384));
        assert_eq!(snapshot.status_code, Some(200));
        let recorded = mock.requests();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].url, "https://relay.example.com/api/user/self");
        assert_eq!(
            recorded[0]
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer cctq-pat-token",
            "account balance must use the dedicated PAT, not the sk- channel key"
        );
    }

    #[tokio::test]
    async fn query_classifies_http_errors_without_failing_the_api() {
        let mock = MockUpstream::always(401, r#"{"message":"bad key"}"#);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("http_401"));
        assert_eq!(snapshot.status_code, Some(401));
        assert_eq!(snapshot.remaining, None);
        assert!(state.balance.snapshot("ch-1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn query_classifies_invalid_json() {
        let mock = MockUpstream::always(200, "definitely not json");
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "deepseek", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("invalid_payload"));
        assert_eq!(snapshot.status_code, Some(200));
    }

    #[tokio::test]
    async fn query_classifies_transport_error() {
        let mock =
            MockUpstream::new(|_| MockReply::Error(UpstreamError::Transport("reset".into())));
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("transport_error"));
        assert_eq!(snapshot.status_code, None);
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn query_classifies_timeout() {
        let mock = MockUpstream::new(|_| MockReply::Pending);
        let limits = RuntimeLimits {
            balance_timeout: Duration::from_millis(60),
            ..RuntimeLimits::default()
        };
        let (state, _dir) = test_state(mock.clone(), limits).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let started = std::time::Instant::now();
        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("timeout"));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn query_opencode_returns_three_window_snapshot() {
        let body = r#"{"usage":{"rolling":{"status":"ok","percent":37.5,"resetsAt":"2026-09-11T20:00:00Z"},"weekly":{"status":"ok","percent":12.0,"resetsAt":null},"monthly":{"status":"ok","percent":4.5,"resetsAt":"2026-10-01T00:00:00Z"}}}"#;
        let mock = MockUpstream::always(200, body);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://opencode.ai/zen/go").await;
        save_balance(&state, "opencode_go", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        let labels: Vec<&str> = snapshot
            .windows
            .iter()
            .map(|window| window.label.as_str())
            .collect();
        assert_eq!(labels, vec!["5h", "周", "月"]);
        assert_eq!(snapshot.windows[0].remaining_percent, 62.5);
        assert_eq!(snapshot.windows[2].remaining_percent, 95.5);
        assert_eq!(
            mock.requests()[0].url,
            "https://opencode.ai/zen/go/v1/usage"
        );
        assert!(
            mock.requests()[0]
                .headers
                .contains_key("x-opencode-session"),
            "OpenCode upstreams need the session header"
        );
    }

    /// zhipu：预设 path 解析到渠道主机上，请求携带裸 token
    /// （智谱拒绝 `Bearer` 前缀）。
    #[tokio::test]
    async fn zhipu_queries_console_quota_with_a_raw_token() {
        let mock = MockUpstream::always(200, ZHIPU_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://open.bigmodel.cn").await;
        save_balance(&state, "zhipu", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.label.as_deref(), Some("pro"));
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.status_code, Some(200));
        let recorded = mock.requests();
        assert_eq!(recorded.len(), 1);
        assert_eq!(
            recorded[0].url,
            "https://open.bigmodel.cn/api/monitor/usage/quota/limit"
        );
        assert_eq!(
            recorded[0]
                .headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "sk-channel-secret",
            "raw auth must not add the Bearer prefix"
        );
    }

    /// openrouter：`/credits` 需要 Management Key；普通 key 在那里会得到 403，
    /// 流程回落到 key 视图。
    #[tokio::test]
    async fn openrouter_falls_back_from_credits_to_the_key_view() {
        const KEY_BODY: &str =
            r#"{"data":{"label":"sk-or-v1-abc","usage":3.5,"limit":10,"limit_remaining":6.5}}"#;
        let mock = MockUpstream::new(|request| {
            if request.url.ends_with("/credits") {
                MockReply::Json(403, r#"{"error":{"message":"management key required"}}"#.into())
            } else {
                MockReply::Json(200, KEY_BODY.into())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        let mut input = base_input("openrouter", true);
        input.token = Some("sk-or-v1-abc".into());
        state.balance.save_config("ch-1", input).await.unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(6.5));
        assert_eq!(snapshot.used, Some(3.5));
        assert_eq!(snapshot.total, Some(10.0));
        let urls: Vec<String> = mock
            .requests()
            .into_iter()
            .map(|request| request.url)
            .collect();
        assert_eq!(
            urls,
            [
                "https://openrouter.ai/api/v1/credits",
                "https://openrouter.ai/api/v1/key"
            ]
        );
    }

    /// 多步适配器（openrouter / mimo / command_code）构造请求时的连接超时必须
    /// 取运行时设置，而不是写死的 10s。
    #[tokio::test]
    async fn multi_step_connect_timeout_follows_the_runtime_setting() {
        const KEY_BODY: &str =
            r#"{"data":{"label":"sk-or-v1-abc","usage":3.5,"limit":10,"limit_remaining":6.5}}"#;
        let mock = MockUpstream::always(200, KEY_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "openrouter", true).await;
        sqlx::query(
            "INSERT INTO settings(key,value_json,updated_at) VALUES('connect_timeout_seconds','7',?) \
             ON CONFLICT(key) DO UPDATE SET value_json='7'",
        )
        .bind("2026-08-04T01:00:00+00:00")
        .execute(state.db.pool())
        .await
        .unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        let recorded = mock.requests();
        assert_eq!(recorded.len(), 1, "openrouter 成功路径只发一次请求");
        assert_eq!(
            recorded[0].connect_timeout,
            Duration::from_secs(7),
            "连接超时必须跟随运行时设置（connect_timeout_seconds=7），不是写死的 10s"
        );
    }

    /// mimo：控制台 Cookie 流程（usage + balance 必需，detail 可选）。
    #[tokio::test]
    async fn mimo_queries_console_cookie_and_skips_a_failing_detail_step() {
        const USAGE: &str = r#"{"code":0,"data":{"usage":{"percent":0.42,"items":[{"name":"plan_total_token","used":1100000000,"limit":11000000000}]}}}"#;
        const BALANCE: &str = r#"{"code":0,"data":{"balance":"12.34","currency":"CNY"}}"#;
        let mock = MockUpstream::new(|request| {
            if request.url.ends_with("/api/v1/tokenPlan/usage") {
                MockReply::Json(200, USAGE.into())
            } else if request.url.ends_with("/api/v1/balance") {
                MockReply::Json(200, BALANCE.into())
            } else {
                MockReply::Json(404, r#"{"code":404}"#.into())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        let mut input = base_input("mimo", true);
        input.token = Some("SESSION=abc; other=1".into());
        state.balance.save_config("ch-1", input).await.unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok", "detail is optional");
        assert_eq!(snapshot.remaining, Some(12.34));
        assert_eq!(snapshot.currency.as_deref(), Some("CNY"));
        assert_eq!(snapshot.windows[0].used_percent, 10.0);
        let recorded = mock.requests();
        assert_eq!(recorded.len(), 3, "usage + balance + the failing detail");
        assert_eq!(
            recorded[0].url,
            "https://platform.xiaomimimo.com/api/v1/tokenPlan/usage"
        );
        assert_eq!(
            recorded[1].url,
            "https://platform.xiaomimimo.com/api/v1/balance"
        );
        for request in &recorded {
            assert_eq!(
                request.headers.get("cookie").unwrap().to_str().unwrap(),
                "SESSION=abc; other=1",
                "the dedicated token is the console Cookie string"
            );
            assert!(
                !request.headers.contains_key("authorization"),
                "MiMo authenticates by Cookie only"
            );
        }
    }

    #[tokio::test]
    async fn delete_config_returns_to_default_off() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;
        state.balance.query(&state, "ch-1").await.unwrap();
        state.balance.delete_config("ch-1").await.unwrap();

        assert!(state.balance.snapshot("ch-1").await.unwrap().is_none());
        assert!(matches!(
            state.balance.query(&state, "ch-1").await.unwrap_err(),
            BalanceError::NotConfigured
        ));
        assert_eq!(mock.calls(), 1, "delete must not produce requests");
    }

    // ------------------------------------------------- 自定义与密钥

    #[tokio::test]
    async fn custom_adapter_renders_templates_and_never_persists_secrets() {
        let mock = MockUpstream::always(200, r#"{"data":{"balance":"12.34","spent":"1.66"}}"#);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com/prefix").await;
        let mut headers = BTreeMap::new();
        headers.insert("X-Api-Key".into(), "${token}".into());
        let input = BalanceConfigInput {
            adapter: "custom".into(),
            enabled: true,
            method: Some("PUT".into()),
            path: Some("balance/current?source=gw".into()),
            auth: Some("none".into()),
            headers,
            body: Some(r#"{"key":"${api_key}","label":"${token}"}"#.into()),
            mapping: BalanceMappingInput {
                remaining: Some("$.data.balance".into()),
                used: Some("$.data.spent".into()),
                ..BalanceMappingInput::default()
            },
            token: None,
            clear_token: false,
        };
        let saved = state.balance.save_config("ch-1", input).await.unwrap();
        assert_eq!(saved.adapter, Some(BalanceAdapter::Custom));
        assert_eq!(saved.method, "PUT");
        assert_eq!(saved.auth, BalanceAuth::None);

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(12.34));
        assert_eq!(snapshot.used, Some(1.66));
        assert_eq!(snapshot.currency, None);

        let recorded = mock.requests()[0].clone();
        assert_eq!(recorded.method, Method::PUT);
        assert_eq!(
            recorded.url,
            "https://relay.example.com/prefix/balance/current?source=gw"
        );
        assert_eq!(
            recorded.headers.get("x-api-key").unwrap().to_str().unwrap(),
            "sk-channel-secret"
        );
        assert_eq!(
            recorded.body.as_deref(),
            Some(br#"{"key":"sk-channel-secret","label":"sk-channel-secret"}"#.as_slice())
        );

        // 渲染后的请求（乃至渠道 key）绝不能写入快照表。
        let stored: (String,) = sqlx::query_as(
            "SELECT COALESCE(windows_json,'') || COALESCE(detail_json,'') || \
                    COALESCE(label,'') || COALESCE(currency,'') || COALESCE(error_kind,'') \
             FROM channel_balance_snapshots WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(!stored.0.contains("sk-channel-secret"));
        let config_dump: (String,) = sqlx::query_as(
            "SELECT COALESCE(headers_json,'') || COALESCE(body_json,'') || \
                    COALESCE(mapping_json,'') || COALESCE(balance_token_hint,'') \
             FROM channel_balance_configs WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert!(!config_dump.0.contains("sk-channel-secret"));
    }

    /// 最小的一次性 HTTP 上游：捕获原始请求文本并返回 `body`。
    async fn spawn_http_upstream(
        body: &'static str,
    ) -> (u16, tokio::sync::mpsc::UnboundedReceiver<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = Vec::new();
            let mut header_end: Option<usize> = None;
            loop {
                let mut chunk = [0u8; 4096];
                let read = match stream.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                buf.extend_from_slice(&chunk[..read]);
                if header_end.is_none() {
                    header_end = buf
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|index| index + 4);
                }
                if let Some(end) = header_end {
                    let headers = String::from_utf8_lossy(&buf[..end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    if buf.len() >= end + content_length {
                        break;
                    }
                }
            }
            let _ = tx.send(String::from_utf8_lossy(&buf).to_string());
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        (port, rx)
    }

    /// `custom` 适配器的 PUT（及其请求体）必须经共享 HTTP 客户端端口到达真实上游。
    #[tokio::test]
    async fn custom_put_reaches_a_real_http_upstream_with_body() {
        let (port, mut requests) = spawn_http_upstream(r#"{"data":{"remaining":3.5}}"#).await;
        let http: Arc<dyn UpstreamClient> =
            Arc::new(crate::infrastructure::HttpClientPool::default());
        let (state, _dir) = test_state(http, RuntimeLimits::default()).await;
        seed_channel(&state, &format!("http://127.0.0.1:{port}/prefix")).await;
        let mut input = base_input("custom", true);
        input.method = Some("PUT".into());
        input.path = Some("balance".into());
        input.auth = Some("none".into());
        input.body = Some(r#"{"probe":"yes"}"#.into());
        input.mapping.remaining = Some("$.data.remaining".into());
        state.balance.save_config("ch-1", input).await.unwrap();

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(3.5));
        let request = requests
            .recv()
            .await
            .expect("upstream must see the request");
        assert!(
            request.starts_with("PUT /prefix/balance HTTP/1.1"),
            "unexpected request line: {request}"
        );
        assert!(
            request.contains(r#"{"probe":"yes"}"#),
            "PUT body missing: {request}"
        );
    }

    #[tokio::test]
    async fn dedicated_token_is_stored_encrypted_and_clearable() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;
        let mut input = base_input("newapi", true);
        input.token = Some("panel-token-secret".into());
        let saved = state.balance.save_config("ch-1", input).await.unwrap();
        assert!(saved.has_token);
        assert!(saved.token_hint.is_some());

        let ciphertext: Vec<u8> = sqlx::query_scalar(
            "SELECT balance_token_encrypted FROM channel_balance_configs WHERE channel_id='ch-1'",
        )
        .fetch_one(state.db.pool())
        .await
        .unwrap();
        assert_ne!(ciphertext, b"panel-token-secret");
        assert_eq!(
            state.secrets.decrypt(&ciphertext).unwrap(),
            "panel-token-secret"
        );

        // 空 token 字符串保留原值；clear_token 则删除它。
        let mut keep = base_input("newapi", true);
        keep.token = Some(String::new());
        state.balance.save_config("ch-1", keep).await.unwrap();
        assert!(state.balance.config("ch-1").await.unwrap().has_token);

        let mut clear = base_input("newapi", true);
        clear.clear_token = true;
        let cleared = state.balance.save_config("ch-1", clear).await.unwrap();
        assert!(!cleared.has_token);
        assert!(cleared.token_hint.is_none());
    }

    #[tokio::test]
    async fn save_config_rejects_invalid_custom_inputs() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://relay.example.com").await;

        // custom 缺少 path
        let error = state
            .balance
            .save_config("ch-1", base_input("custom", true))
            .await
            .unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // 未知适配器
        let error = state
            .balance
            .save_config("ch-1", base_input("auto", true))
            .await
            .unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // header 过多
        let mut too_many = BTreeMap::new();
        for index in 0..=MAX_CUSTOM_HEADERS {
            too_many.insert(format!("X-Test-{index}"), "value".into());
        }
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.headers = too_many;
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // body 过大
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.body = Some("x".repeat(MAX_CUSTOM_BODY_BYTES + 1));
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // mapping path 必须是 JSON 路径
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.mapping.remaining = Some("data.balance".into());
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));

        // 不支持的方法
        let mut input = base_input("custom", true);
        input.path = Some("/balance".into());
        input.method = Some("DELETE".into());
        let error = state.balance.save_config("ch-1", input).await.unwrap_err();
        assert!(matches!(error, BalanceError::Invalid(_)));
        assert_eq!(mock.calls(), 0, "validation must never hit the network");
    }

    // ------------------------------------------------------ 维护任务

    #[tokio::test]
    async fn maintenance_refreshes_enabled_channels_on_its_interval() {
        let mock = MockUpstream::always(200, NEWAPI_BODY);
        let limits = RuntimeLimits {
            maintenance_interval: Duration::from_millis(20),
            balance_interval: Duration::from_millis(40),
            balance_timeout: Duration::from_secs(2),
            ..RuntimeLimits::default()
        };
        let (state, _dir) = test_state(mock.clone(), limits).await;
        seed_channel(&state, "https://relay.example.com").await;
        save_balance(&state, "newapi", true).await;

        let cancel = CancellationToken::new();
        let handle = tokio::spawn(crate::maintenance::run_supervisor(
            state.clone(),
            cancel.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(260)).await;
        cancel.cancel();
        let _ = handle.await;

        assert!(
            mock.calls() >= 2,
            "maintenance must run the balance refresh repeatedly, calls={}",
            mock.calls()
        );
        let stored = state.balance.snapshot("ch-1").await.unwrap().unwrap();
        assert_eq!(stored.status, "ok");
    }
    #[test]
    fn command_code_quota_parses_credits_windows_and_summary() {
        let whoami = br#"{"org":{"id":"org_1","login":"me"}}"#;
        let credits = br#"{"credits":{"monthlyCredits":10,"purchasedCredits":5,"freeCredits":1},
            "windowLimits":{"fiveHour":{"used":3,"cap":6,"resetAt":"2026-09-12T10:00:00Z"},
                            "weekly":{"used":40,"cap":100,"resetAt":1789207200}}}"#;
        let summary = br#"{"totalCost":2.5,"totalCount":3,"totalTokens":100}"#;
        let reading =
            parse_command_code(whoami, credits, Some(summary), None, "org_1").unwrap();
        assert_eq!(reading.remaining, Some(16.0));
        assert_eq!(reading.used, Some(2.5));
        assert_eq!(reading.total, Some(18.5));
        assert_eq!(reading.label.as_deref(), Some("Credits"));
        assert_eq!(reading.windows.len(), 2);
        assert_eq!(reading.windows[0].label, "5h");
        assert!((reading.windows[0].used_percent - 50.0).abs() < 1e-9);
        assert_eq!(
            reading.windows[0].resets_at.as_deref(),
            Some("2026-09-12T10:00:00+00:00")
        );
        assert_eq!(reading.windows[1].label, "周");
        assert_eq!(reading.windows[1].used_percent, 40.0);
        assert_eq!(reading.windows[1].remaining_percent, 60.0);
        // 秒级时间戳统一成 RFC3339，前端 formatTime 才能正确显示。
        assert_eq!(
            reading.windows[1].resets_at.as_deref(),
            Some("2026-09-12T10:00:00+00:00")
        );
        assert_eq!(command_code_org_id(whoami).as_deref(), Some("org_1"));
        assert_eq!(
            command_code_org_id(br#"{"orgId":"org_2"}"#).as_deref(),
            Some("org_2")
        );
        // 官方 CLI 的 usage 包装形状：data.org.id。
        assert_eq!(
            command_code_org_id(br#"{"data":{"org":{"id":"org_3"}}}"#).as_deref(),
            Some("org_3")
        );
        assert!(command_code_org_id(br#"{"user":{"userName":"x"}}"#).is_none());
        // 用量汇总可选：credits/窗口仍能解析。
        let fallback = parse_command_code(whoami, credits, None, None, "org_1").unwrap();
        assert_eq!(fallback.used, None);
        assert_eq!(fallback.total, Some(16.0));

        // Go 套餐：credits 全为 0，额度只在 5h/周窗口里 —— 必须解析成功而不是
        // invalid_payload（历史 bug）。
        let go_credits = br#"{"credits":{"monthlyCredits":0,"purchasedCredits":0,"freeCredits":0},
            "windowLimits":{"fiveHour":{"used":10,"cap":60,"resetAt":1789210700},
                            "weekly":{"used":20,"cap":200,"resetAt":"2026-09-19T10:00:00Z"}},
            "planId":"go"}"#;
        let go = parse_command_code(
            br#"{"user":{"userName":"alice"},"org":{"id":"org_1"}}"#,
            go_credits,
            Some(summary),
            Some(br#"{"data":{"planId":"go","status":"active"}}"#),
            "org_1",
        )
        .unwrap();
        assert_eq!(go.remaining, None, "zero credits must not be shown as 0.00");
        assert_eq!(go.used, Some(2.5));
        assert_eq!(go.total, None);
        assert_eq!(go.windows.len(), 2);
        assert!((go.windows[0].used_percent - (10.0 / 60.0 * 100.0)).abs() < 1e-9);
        assert_eq!(go.detail["planId"], "go");
        assert_eq!(go.detail["planStatus"], "active");
        assert_eq!(go.detail["account"], "alice");

        // 既没有 credits 结构、也没有窗口/汇总才是无效负载。
        assert!(parse_command_code(whoami, b"{}", None, None, "org_1").is_err());
    }

    /// Command Code 额度是四步只读流程：whoami → credits → usage summary
    /// → subscriptions，全部用渠道 API key。
    #[tokio::test]
    async fn command_code_balance_queries_whoami_credits_and_summary() {
        let mock = MockUpstream::new(|request| {
            if request.url.contains("/alpha/whoami") {
                MockReply::Json(200, r#"{"org":{"id":"org_9","login":"me"}}"#.into())
            } else if request.url.contains("/alpha/billing/credits") {
                MockReply::Json(
                    200,
                    r#"{"credits":{"monthlyCredits":10,"purchasedCredits":5,"freeCredits":1},
                        "windowLimits":{"fiveHour":{"used":3,"cap":6,"resetAt":"2096-09-12T10:00:00Z"},
                                        "weekly":{"used":40,"cap":100,"resetAt":1789207200}}}"#
                        .into(),
                )
            } else if request.url.contains("/alpha/usage/summary") {
                MockReply::Json(200, r#"{"totalCost":2.5,"totalCount":3,"totalTokens":100}"#.into())
            } else if request.url.contains("/alpha/billing/subscriptions") {
                MockReply::Json(200, r#"{"data":{"planId":"go","status":"active"}}"#.into())
            } else {
                MockReply::Json(404, "{}".into())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        sqlx::query(
            "INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)",
        )
        .bind("2026-08-04T01:00:00+00:00")
        .execute(state.db.pool())
        .await
        .unwrap();
        seed_channel(&state, "https://api.commandcode.ai").await;
        save_balance(&state, "command_code", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok");
        assert_eq!(snapshot.remaining, Some(16.0));
        assert_eq!(snapshot.used, Some(2.5));
        assert_eq!(snapshot.total, Some(18.5));
        assert_eq!(snapshot.label.as_deref(), Some("Credits"));
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5h");
        assert!((snapshot.windows[0].used_percent - 50.0).abs() < 1e-9);
        assert_eq!(
            snapshot.windows[0].resets_at.as_deref(),
            Some("2096-09-12T10:00:00+00:00")
        );
        assert_eq!(snapshot.windows[1].label, "周");
        assert_eq!(snapshot.windows[1].used_percent, 40.0);

        assert_eq!(snapshot.detail["planId"], "go");
        assert_eq!(snapshot.detail["planStatus"], "active");
        let requests = mock.requests();
        assert_eq!(requests.len(), 4, "whoami + credits + summary + subscriptions");
        assert!(requests[0].url.ends_with("/alpha/whoami"));
        assert!(
            requests[1]
                .url
                .contains("/alpha/billing/credits?orgId=org_9"),
            "query must be a real query string, got {}",
            requests[1].url
        );
        assert!(
            requests[2]
                .url
                .contains("/alpha/usage/summary?orgId=org_9"),
            "query must be a real query string, got {}",
            requests[2].url
        );
        assert!(
            requests[3]
                .url
                .contains("/alpha/billing/subscriptions?orgId=org_9"),
            "query must be a real query string, got {}",
            requests[3].url
        );
        for request in &requests {
            assert_eq!(request.method, Method::GET);
            let authorization = request
                .headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            assert_eq!(authorization, "Bearer sk-channel-secret");
        }
    }


    /// 无 org 的个人账号（whoami `org:null`，真实 Go 套餐形状）：billing 系列
    /// 必须**省略 orgId**（不能拼 `?orgId=`），credits 的 5h/周窗口与
    /// `resetAt:0`（无待重置）要正确呈现。
    #[tokio::test]
    async fn command_code_balance_without_org_uses_paramless_billing_requests() {
        let mock = MockUpstream::new(|request| {
            if request.url.contains("/alpha/whoami") {
                MockReply::Json(
                    200,
                    r#"{"success":true,"user":{"id":"u-1","userName":"alice"},"org":null}"#.into(),
                )
            } else if request.url.contains("/alpha/billing/credits") {
                MockReply::Json(
                    200,
                    r#"{"credits":{"monthlyCredits":10,"purchasedCredits":0,"freeCredits":0},
                        "windowLimits":{"limited":true,"fiveHour":{"used":0,"cap":3,"resetAt":0},
                                        "weekly":{"used":0,"cap":6,"resetAt":0}}}"#
                        .into(),
                )
            } else if request.url.contains("/alpha/usage/summary") {
                MockReply::Json(
                    200,
                    r#"{"totalCost":0,"totalCount":0,"totalTokens":0}"#.into(),
                )
            } else {
                MockReply::Json(404, "{}".into())
            }
        });
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        sqlx::query(
            "INSERT INTO settings(key,value_json,updated_at) VALUES('command_code_enabled','true',?)",
        )
        .bind("2026-08-04T01:00:00+00:00")
        .execute(state.db.pool())
        .await
        .unwrap();
        seed_channel(&state, "https://api.commandcode.ai").await;
        save_balance(&state, "command_code", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "ok", "{snapshot:?}");
        assert_eq!(snapshot.remaining, Some(10.0));
        assert_eq!(snapshot.label.as_deref(), Some("Credits"));
        assert_eq!(snapshot.windows.len(), 2);
        assert_eq!(snapshot.windows[0].label, "5h");
        assert_eq!(snapshot.windows[0].used_percent, 0.0);
        assert_eq!(snapshot.windows[0].remaining_percent, 100.0);
        assert_eq!(
            snapshot.windows[0].resets_at, None,
            "resetAt:0 means no scheduled reset"
        );
        assert_eq!(snapshot.windows[1].label, "周");
        assert_eq!(snapshot.detail["account"], "alice");
        assert_eq!(snapshot.detail["orgId"], "");

        let requests = mock.requests();
        for request in &requests {
            if request.url.contains("/alpha/billing/") || request.url.contains("/alpha/usage/") {
                assert!(
                    !request.url.contains("orgId"),
                    "org-less accounts must omit the parameter, got {}",
                    request.url
                );
            }
        }
        assert!(requests.iter().any(|r| r.url.ends_with("/alpha/billing/credits")));
    }

    /// 全局 Command Code 开关同样管控余额旁路：禁用时查询被归类为 `disabled`，
    /// 且不发出任何上游请求。
    #[tokio::test]
    async fn command_code_balance_is_blocked_while_disabled() {
        let mock = MockUpstream::always(200, "{}");
        let (state, _dir) = test_state(mock.clone(), RuntimeLimits::default()).await;
        seed_channel(&state, "https://api.commandcode.ai").await;
        save_balance(&state, "command_code", true).await;

        let snapshot = state.balance.query(&state, "ch-1").await.unwrap();
        assert_eq!(snapshot.status, "error");
        assert_eq!(snapshot.error_kind.as_deref(), Some("disabled"));
        assert_eq!(
            mock.calls(),
            0,
            "disabled integration must not touch the upstream"
        );
    }

}
