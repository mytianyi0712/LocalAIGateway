//! 测试夹具：为各 `#[cfg(test)]` 测试模块提供统一的临时目录与整套 `Context` 装配。
//!
//! 职责：集中构造测试用上下文（数据库、密钥库、遥测、后台监管与各服务），
//! 并提供 `providers` / `channels` 的播种助手，避免每个测试模块各自复制同一套装配。
//! 边界：仅供单元测试使用，生产代码不引用本模块。
//! 关键不变量：`TempDir` 在 `Drop` 时整目录删除，测试不再泄漏 `/tmp/lagw-*-test-*`；
//! 播种出的行与生产路径同形（密钥经 `SecretStore` 加密、`api_key_hint` 走
//! `SecretStore::hint`），避免测试夹具与真实列语义漂移。

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use crate::{
    admin::AdminService,
    application::Context,
    auth::RecoverySession,
    balance::BalanceService,
    commandcode_login::CommandCodeLogin,
    config::AppConfig,
    crypto::SecretStore,
    db::Database,
    discovery::DiscoveryService,
    infrastructure::{
        HttpClientPool, RuntimeSupervisor, SqliteChannelRepository, SqliteRouteRepository,
        SystemClock,
    },
    notification::DesktopNotifier,
    ports::{ChannelRepository, Clock, Notifier, RouteRepository, UpstreamClient},
    proxy::ProxyService,
    runtime::RuntimeLimits,
    state::AppState,
    telemetry::Telemetry,
};

/// 播种行统一使用的时间戳：固定值让断言可以对照 `created_at` / `updated_at`。
pub const SEED_TIME: &str = "2026-08-04T01:00:00+00:00";

/// 测试用临时目录：创建时建目录，`Drop` 时递归删除，避免 `/tmp` 残留。
pub struct TempDir(pub PathBuf);

impl TempDir {
    /// 在系统临时目录下创建 `lagw-<tag>-test-<uuid>` 并建目录。
    pub fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("lagw-{tag}-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("创建测试临时目录失败");
        Self(path)
    }

    /// 目录路径（用于拼接 `test.db` / `master.key`）。
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 一次测试的完整夹具：临时目录（Drop 清理）与装配好的组合根状态。
pub struct TestEnv {
    pub dir: TempDir,
    pub context: AppState,
}

/// 组装一整套测试用 [`Context`]，与 `server.rs` 的生产装配逐字段对应。
///
/// `tag` 只用于临时目录命名（`lagw-<tag>-test-<uuid>`）。
pub async fn context(tag: &str) -> TestEnv {
    context_with(
        tag,
        Arc::new(HttpClientPool::default()),
        RuntimeLimits::default(),
    )
    .await
}

/// [`context`] 的通用形态：允许注入自定义上游端口与运行限额。
///
/// balance 等模块需要假上游（断言上游请求）并会调整运行限额，故在此开放两个入口。
pub async fn context_with(
    tag: &str,
    http: Arc<dyn UpstreamClient>,
    limits: RuntimeLimits,
) -> TestEnv {
    let dir = TempDir::new(tag);
    let db = Database::open(&dir.path().join("test.db")).await.unwrap();
    let secrets = SecretStore::load(&dir.path().join("master.key"))
        .await
        .unwrap();
    let (telemetry, telemetry_rx) = Telemetry::new(1000);
    // 遥测写入 worker 由夹具持有：接收端交给当前测试运行时，事件照常落库。
    // 需要自行控制 worker 生命周期（断言落库时序）的模块仍自行装配。
    tokio::spawn(Telemetry::run_writer(
        db.clone(),
        telemetry_rx,
        CancellationToken::new(),
        telemetry.dropped_handle(),
    ));
    let routes: Arc<dyn RouteRepository> = SqliteRouteRepository::new(db.clone());
    let channels: Arc<dyn ChannelRepository> = SqliteChannelRepository::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let background = RuntimeSupervisor::new(CancellationToken::new());
    let limits = Arc::new(limits);
    let notifier = notifier();
    let discovery = DiscoveryService::new(
        db.clone(),
        secrets.clone(),
        Arc::clone(&http),
        Arc::clone(&channels),
        Arc::clone(&clock),
        Arc::clone(&background),
        Arc::clone(&limits),
    );
    let settings_reader: Arc<dyn crate::ports::SettingsReader> =
        crate::infrastructure::SettingsStore::new(db.clone(), secrets.clone());
    let command_code_state: Arc<dyn crate::ports::CommandCodeState> =
        crate::infrastructure::CommandCodeStore::new(db.clone(), Arc::clone(&http));
    let proxy = ProxyService::new(crate::proxy::ProxyServiceDeps {
        settings: Arc::clone(&settings_reader),
        command_code: Arc::clone(&command_code_state),
        secrets: secrets.clone(),
        http: Arc::clone(&http),
        routes: Arc::clone(&routes),
        telemetry: telemetry.clone(),
        clock: Arc::clone(&clock),
        limits: Arc::clone(&limits),
        notifier: Arc::clone(&notifier),
    });
    let balance = BalanceService::new(
        db.clone(),
        secrets.clone(),
        Arc::clone(&http),
        Arc::clone(&channels),
        Arc::clone(&clock),
        Arc::clone(&limits),
    );
    let admin = AdminService::new(db.clone(), secrets.clone(), Arc::clone(&channels));
    let command_code_login = CommandCodeLogin::new(Arc::clone(&http));
    let ctx = Context {
        config: Arc::new(AppConfig::default()),
        db: db.clone(),
        secrets: secrets.clone(),
        http,
        routes,
        channels,
        clock,
        notifier,
        telemetry,
        background,
        limits,
    };
    let context = AppState {
        ctx,
        proxy,
        admin,
        balance,
        discovery,
        command_code_login,
        recovery: RecoverySession::new(),
    };
    TestEnv { dir, context }
}

/// 测试用桌面通知端口：50ms 合并窗口（与旧测试夹具一致），避免测试等待真实节流。
pub fn notifier() -> Arc<dyn Notifier> {
    DesktopNotifier::new(Duration::from_millis(50))
}

/// 播种一个 provider 行（`providers` 表），时间戳统一取 [`SEED_TIME`]。
pub async fn seed_provider(db: &Database, id: &str, name: &str, base_url: &str) {
    seed_provider_with_kind(db, id, name, base_url, None).await;
}

/// 同上，并指定 `providers.kind`（`Some("command_code")` 会开启 CLI 身份头路径）。
pub async fn seed_provider_with_kind(
    db: &Database,
    id: &str,
    name: &str,
    base_url: &str,
    kind: Option<&str>,
) {
    sqlx::query("INSERT INTO providers(id,name,base_url,kind,created_at,updated_at) VALUES(?,?,?,?,?,?)")
        .bind(id)
        .bind(name)
        .bind(base_url)
        .bind(kind)
        .bind(SEED_TIME)
        .bind(SEED_TIME)
        .execute(db.pool())
        .await
        .unwrap();
}

/// 播种一个渠道及其协议绑定（`channels` + `channel_protocols`）。
///
/// 渠道默认启用（`manual_enabled=1`），密钥用 `secrets` 加密入库，
/// 提示串与生产一致由 [`SecretStore::hint`] 生成。
pub async fn seed_channel(
    db: &Database,
    secrets: &SecretStore,
    id: &str,
    provider_id: &str,
    protocol: &str,
    key: &str,
) {
    seed_channel_named(db, secrets, id, provider_id, "chan", protocol, key).await;
}

/// 同上，但指定渠道显示名（同一 provider 下 `name` 唯一，多候选场景需要区分）。
pub async fn seed_channel_named(
    db: &Database,
    secrets: &SecretStore,
    id: &str,
    provider_id: &str,
    name: &str,
    protocol: &str,
    key: &str,
) {
    sqlx::query("INSERT INTO channels(id,provider_id,name,protocol,api_key_encrypted,api_key_hint,manual_enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?)")
        .bind(id)
        .bind(provider_id)
        .bind(name)
        .bind(protocol)
        .bind(secrets.encrypt(key))
        .bind(SecretStore::hint(key))
        .bind(SEED_TIME)
        .bind(SEED_TIME)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO channel_protocols(channel_id,protocol) VALUES(?,?)")
        .bind(id)
        .bind(protocol)
        .execute(db.pool())
        .await
        .unwrap();
}
