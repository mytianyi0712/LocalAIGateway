//! 跨层共享的领域值类型：只放数据形状与纯计算，不放 I/O。
//!
//! 边界：本模块不得 `use axum|sqlx|reqwest`（`scripts/check-layers.sh` 会检查），
//! 也不得依赖 crate 内的其它业务模块——它是依赖图的叶子，供 `ports`、`telemetry`、
//! `protocol`、`routing`、`proxy` 等模块共用，因此 `Event` 与 `Usage` 之间的
//! 引用、以及 `Candidate` 与 `CompactionSupport` 之间的引用都不再构成模块环。

use serde::Serialize;
use serde_json::Value;

/// 单次尝试的遥测快照，由 [`Event::Attempt`] 携带。
#[derive(Debug)]
pub struct AttemptData {
    pub id: String,
    pub request_id: String,
    pub channel_id: String,
    pub channel_name: String,
    pub attempt_no: i64,
    pub priority: i64,
    pub started_at: String,
    pub finished_at: String,
    pub status: Option<i64>,
    pub outcome: String,
    pub error_kind: Option<String>,
    pub failover: bool,
    pub response_started: bool,
    pub first_byte_ms: Option<i64>,
    pub first_token_ms: Option<i64>,
    pub duration_ms: i64,
    pub usage: Usage,
    pub response_bytes: i64,
    pub upstream_protocol: Option<String>,
    pub upstream_model_id: Option<String>,
}

/// 遥测事件：请求与尝试的生命周期、渠道健康信号。
#[derive(Debug)]
pub enum Event {
    RequestStart {
        id: String,
        protocol: String,
        model_id: Option<String>,
        endpoint: String,
        stream: bool,
        started_at: String,
        request_bytes: i64,
    },
    RequestFinish {
        id: String,
        finished_at: String,
        duration_ms: i64,
        status: Option<i64>,
        outcome: String,
        attempts: i64,
        channel_id: Option<String>,
        response_bytes: i64,
    },
    // Attempt 是最大的变体（含 Usage 共 20 个字段）；把它 box 起来能让 Event
    // 枚举保持较小，使每个入队事件都只付出一次相同大小的分配，而不必把每条消息
    // 都填充到最大变体的大小。
    Attempt(Box<AttemptData>),
    ChannelSuccess {
        channel_id: String,
    },
    ChannelFailure {
        channel_id: String,
        error_kind: String,
        status: Option<i64>,
        threshold: i64,
        open_seconds: i64,
        countable: bool,
    },
}

/// 一次上游交换的 token 用量快照。
#[derive(Clone, Debug, Default)]
pub struct Usage {
    pub input_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_write_tokens: Option<i64>,
    pub cache_miss_input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub raw: Option<Value>,
}

/// 由（含缓存的）输入总量与缓存读写量推导 cache-miss 输入。
///
/// 这是四个协议适配器、流式 `usage_from_parts` 与 [`Usage::merge`] 的唯一来源：
/// 各家上游对「miss」的字段口径不同，但换算公式只有这一条
/// （`total - cache_read - cache_write`，下限 0；缺任一必需项则为 `None`）。
pub fn cache_miss_input(
    input: Option<i64>,
    cache_read: Option<i64>,
    cache_write: Option<i64>,
) -> Option<i64> {
    match (input, cache_read) {
        (Some(total), Some(read)) => Some((total - read - cache_write.unwrap_or(0)).max(0)),
        _ => None,
    }
}

impl Usage {
    /// 把较新的 usage 快照合并进当前快照，逐字段「最新非 None 胜出」：流式上游
    /// 会跨多个 chunk 上报 usage，较晚的 chunk 往往省略较早 chunk 携带过的字段
    /// （例如 `prompt_tokens_details.cached_tokens`），因此整块覆盖会静默丢掉
    /// input/cache 数据。`cache_miss` 由合并后的 input/cache 值重算；`raw` 保留
    /// 最新的快照。
    pub fn merge(&mut self, other: &Usage) {
        if other.input_tokens.is_some() {
            self.input_tokens = other.input_tokens;
        }
        if other.cache_read_tokens.is_some() {
            self.cache_read_tokens = other.cache_read_tokens;
        }
        if other.cache_write_tokens.is_some() {
            self.cache_write_tokens = other.cache_write_tokens;
        }
        if other.output_tokens.is_some() {
            self.output_tokens = other.output_tokens;
        }
        self.cache_miss_input_tokens =
            cache_miss_input(self.input_tokens, self.cache_read_tokens, self.cache_write_tokens);
        if other.raw.is_some() {
            self.raw = other.raw.clone();
        }
    }
}

/// 最近一次纯传输失败的种类（从未收到上游状态码），
/// 让最终网关错误能区分 504 超时与 502 不可达。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportFailure {
    /// reqwest `send` 失败且 `error.is_timeout()`（连接超时）。
    ConnectTimeout,
    /// 发送 future 本身撞上 `first_byte_timeout` 的 tokio 超时，
    /// 或错误响应体读取撞上同一外层超时。
    FirstByteTimeout,
    /// 映射流 prelude 在该次尝试的绝对 `first_token_timeout`
    /// 窗口内没有产出首个 token。
    FirstTokenTimeout,
    /// 非流式成功响应体读取撞上其总超时。
    BodyTimeout,
    /// 其它传输失败（连接重置、本地 key/URL/header 构造失败）——
    /// 尾部把它映射为 502。
    ConnectionReset,
}

impl TransportFailure {
    pub(crate) fn is_timeout(self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout
                | Self::FirstByteTimeout
                | Self::FirstTokenTimeout
                | Self::BodyTimeout
        )
    }

    /// 用于遥测与用户可见 failover 告警的稳定短标签。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ConnectTimeout => "connect_timeout",
            Self::FirstByteTimeout => "first_byte_timeout",
            Self::FirstTokenTimeout => "first_token_timeout",
            Self::BodyTimeout => "body_timeout",
            Self::ConnectionReset => "connection_reset",
        }
    }
}

/// 远端压缩请求模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMode {
    V1,
    V2,
}

impl CompactionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

/// 持久化在 `channel_protocols` 中的三态取值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionSupport {
    Unknown,
    Supported,
    Unsupported,
}

impl CompactionSupport {
    pub fn from_db(value: Option<i64>) -> Self {
        match value {
            Some(1) => Self::Supported,
            Some(2) => Self::Unsupported,
            _ => Self::Unknown,
        }
    }

    pub fn as_db(self) -> i64 {
        match self {
            Self::Unknown => 0,
            Self::Supported => 1,
            Self::Unsupported => 2,
        }
    }

    pub fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }
}

/// 一条已配置的映射：从入口 model 指向某个上游协议 + model
/// （claudecode/codex 入口）。
#[derive(Clone)]
pub struct MappingTarget {
    pub entry: String,
    pub upstream_protocol: String,
    pub upstream_model: String,
}

/// 一次请求尝试的候选渠道：路由解析的输出，proxy 的输入。
/// 字段与 `route_candidates` 查询的列同名（行映射由 infrastructure 手工完成，
/// 因此本类型不依赖 sqlx）。
#[derive(Clone, Debug)]
pub struct Candidate {
    pub candidate_id: String,
    pub channel_id: String,
    pub channel_name: String,
    pub priority: i64,
    pub base_url: String,
    /// `providers.kind`：`command_code` 会选用 CLI 身份头。
    pub kind: Option<String>,
    pub api_key_encrypted: Vec<u8>,
    pub model_id: String,
    /// `channel_protocols.remote_compaction_v1_support` 的原始值：
    /// 0 未知、1 支持、2 不支持。
    pub remote_compaction_v1_support: i64,
    /// `channel_protocols.remote_compaction_v2_support` 的原始值：
    /// 0 未知、1 支持、2 不支持。
    pub remote_compaction_v2_support: i64,
}

impl Candidate {
    pub fn remote_compaction_v1(&self) -> CompactionSupport {
        CompactionSupport::from_db(Some(self.remote_compaction_v1_support))
    }

    pub fn remote_compaction_v2(&self) -> CompactionSupport {
        CompactionSupport::from_db(Some(self.remote_compaction_v2_support))
    }
}

/// 模型目录中的一行：模型 id、展示名与创建时间。
#[derive(Clone, Debug, Serialize)]
pub struct RoutableModel {
    pub id: String,
    pub display_name: String,
    pub created_at: String,
}
