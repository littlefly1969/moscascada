//! Static TOML configuration, validated at startup. An invalid file refuses startup.

use std::fmt;
use std::path::{Path, PathBuf};

use scada_core::{
    ChannelId, ChannelSpec, ConfigError, ConfigSpec, CoreLimits, SourceId, SourceSpec, TagId, TagSpec, ValidatedConfig,
    ValueType,
};
use scada_protocol::frame::MAX_FRAME_BYTES_CEILING;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    ipc: FileIpc,
    #[serde(default)]
    core: FileCore,
    #[serde(default)]
    runtime: FileRuntime,
    tags: Vec<FileTag>,
    sources: Vec<FileSource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIpc {
    socket_path: PathBuf,
    socket_group_gid: Option<u32>,
    #[serde(default = "d_max_frame")]
    max_frame_bytes: u32,
    #[serde(default = "d_max_conn")]
    max_driver_connections: u32,
    #[serde(default = "d_queue")]
    input_queue_capacity: u32,
    #[serde(default = "d_inflight")]
    max_inflight_per_connection: u32,
    #[serde(default = "d_timeout")]
    handshake_timeout_ms: u64,
    #[serde(default = "d_timeout")]
    frame_timeout_ms: u64,
    /// D2: deadline for writing ACKs to a peer; a peer that does not read is closed.
    #[serde(default = "d_write_timeout")]
    ack_write_timeout_ms: u64,
    /// C5: per-connection read buffer (0 = unbuffered, VS-001 behaviour).
    #[serde(default = "d_read_buffer")]
    read_buffer_bytes: u32,
    /// C5: at most this many already-available ACKs per socket write (1 = one per write).
    #[serde(default = "d_ack_coalesce")]
    ack_coalesce_max: u32,
    /// VS-003 (ADR 0010): `"fifo"` (default, VS-002), `"source_capped"`, `"source_fair"`.
    ingress_policy: Option<String>,
    /// VS-003: batches one source may have admitted and not yet applied. Only with a
    /// policy other than `fifo` (required there, forbidden with `fifo`).
    per_source_pending_limit: Option<u32>,
}

fn d_write_timeout() -> u64 {
    5_000
}
fn d_read_buffer() -> u32 {
    8_192
}
fn d_ack_coalesce() -> u32 {
    32
}

fn d_max_frame() -> u32 {
    65_536
}
fn d_max_conn() -> u32 {
    16
}
fn d_queue() -> u32 {
    64
}
fn d_inflight() -> u32 {
    32
}
fn d_timeout() -> u64 {
    2_000
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileCore {
    #[serde(default = "d_max_obs")]
    max_observations_per_batch: u32,
}

impl Default for FileCore {
    fn default() -> Self {
        FileCore { max_observations_per_batch: d_max_obs() }
    }
}

fn d_max_obs() -> u32 {
    128
}

/// `[runtime]`: how the I/O edge is scheduled. Never touches the core or the sequencer
/// model (one OS thread, one writer).
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FileRuntime {
    /// `"current_thread"` (default) or `"multi_thread"`.
    scheduler: Option<String>,
    /// Required with `multi_thread`, forbidden with `current_thread`. Never derived from
    /// the number of CPUs: the operator chooses it (brief §4 B2).
    worker_threads: Option<u32>,
    /// E1: refuse startup if the configured worst-case allocation bound exceeds this.
    memory_budget_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum FileType {
    Bool,
    I64,
}

impl From<FileType> for ValueType {
    fn from(t: FileType) -> Self {
        match t {
            FileType::Bool => ValueType::Bool,
            FileType::I64 => ValueType::I64,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileTag {
    id: u32,
    name: String,
    #[serde(rename = "type")]
    value_type: FileType,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileChannel {
    channel: u32,
    tag: u32,
    #[serde(rename = "type")]
    value_type: FileType,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSource {
    id: u32,
    name: String,
    /// If present, only peers with one of these effective UIDs may open a session for the
    /// source. If absent, access control relies on the socket file permissions only.
    authorized_uids: Option<Vec<u32>>,
    channels: Vec<FileChannel>,
}

/// IPC limits. Guardrails, not performance results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpcLimits {
    pub max_frame_bytes: u32,
    pub max_driver_connections: u32,
    pub input_queue_capacity: u32,
    pub max_inflight_per_connection: u32,
    pub handshake_timeout_ms: u64,
    pub frame_timeout_ms: u64,
    pub ack_write_timeout_ms: u64,
    pub read_buffer_bytes: u32,
    /// ACKs per socket write (C5): only ACKs already available are grouped, never waited for.
    pub ack_coalesce_max: u32,
    /// VS-003 ingress policy (ADR 0010).
    pub ingress: IngressPolicy,
}

/// How admitted requests reach the single writer (ADR 0010). Never changes the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressPolicy {
    /// A: one global bounded FIFO (`input_queue_capacity`), VS-002 behaviour.
    Fifo,
    /// B: the same FIFO, plus at most `per_source` admitted, not yet applied batches per
    /// source. Not fair scheduling: only a quota.
    SourceCapped { per_source: u32 },
    /// C: per-source bounded lanes served round-robin (one request per turn), at most
    /// `per_source` batches per source and `input_queue_capacity` in total.
    SourceFair { per_source: u32 },
}

impl IngressPolicy {
    pub fn per_source(&self) -> Option<u32> {
        match *self {
            IngressPolicy::Fifo => None,
            IngressPolicy::SourceCapped { per_source } | IngressPolicy::SourceFair { per_source } => Some(per_source),
        }
    }
}

impl fmt::Display for IngressPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngressPolicy::Fifo => write!(f, "fifo"),
            IngressPolicy::SourceCapped { per_source } => write!(f, "source_capped({per_source})"),
            IngressPolicy::SourceFair { per_source } => write!(f, "source_fair({per_source})"),
        }
    }
}

fn parse_ingress(policy: Option<&str>, limit: Option<u32>, inflight: u32) -> Result<IngressPolicy, String> {
    let need = |name: &str| -> Result<u32, String> {
        let l = limit.ok_or_else(|| format!("ingress_policy = \"{name}\" requires per_source_pending_limit"))?;
        if l == 0 || l > inflight {
            return Err(format!("per_source_pending_limit={l} outside 1..=max_inflight_per_connection ({inflight})"));
        }
        Ok(l)
    };
    match policy.unwrap_or("fifo") {
        "fifo" if limit.is_some() => {
            Err("per_source_pending_limit is only valid with a per-source ingress_policy".into())
        }
        "fifo" => Ok(IngressPolicy::Fifo),
        "source_capped" => Ok(IngressPolicy::SourceCapped { per_source: need("source_capped")? }),
        "source_fair" => Ok(IngressPolicy::SourceFair { per_source: need("source_fair")? }),
        other => Err(format!("unknown ingress_policy {other:?} (fifo | source_capped | source_fair)")),
    }
}

/// Upper bound for `worker_threads` (a guardrail against typos, not a recommendation).
pub const MAX_WORKER_THREADS: u32 = 64;

/// Tokio scheduler of the I/O edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheduler {
    /// One thread runs every session (VS-001 behaviour, default, Micro profile).
    CurrentThread,
    /// Work-stealing pool with an explicit number of workers (Server profile). Different
    /// sessions may run in parallel; one session is still one task (no intra-session
    /// parallelism, no reordering).
    MultiThread { worker_threads: u32 },
}

impl Scheduler {
    /// Parse the `--scheduler` / `[runtime]` pair. `workers` must be given exactly for
    /// `multi_thread`.
    pub fn parse(name: &str, workers: Option<u32>) -> Result<Scheduler, String> {
        match (name, workers) {
            ("current_thread", None) => Ok(Scheduler::CurrentThread),
            ("current_thread", Some(_)) => Err("worker_threads is only valid with scheduler = \"multi_thread\"".into()),
            ("multi_thread", Some(n)) if (1..=MAX_WORKER_THREADS).contains(&n) => {
                Ok(Scheduler::MultiThread { worker_threads: n })
            }
            ("multi_thread", Some(n)) => Err(format!("worker_threads={n} outside 1..={MAX_WORKER_THREADS}")),
            ("multi_thread", None) => Err("scheduler = \"multi_thread\" requires an explicit worker_threads".into()),
            (other, _) => Err(format!("unknown scheduler {other:?} (current_thread | multi_thread)")),
        }
    }
}

impl fmt::Display for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scheduler::CurrentThread => f.write_str("current_thread"),
            Scheduler::MultiThread { worker_threads } => write!(f, "multi_thread({worker_threads})"),
        }
    }
}

/// `[runtime]` settings after validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub scheduler: Scheduler,
    /// E1: optional ceiling for the configured worst-case allocation bound (`budget.rs`).
    pub memory_budget_bytes: Option<u64>,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        RuntimeSettings { scheduler: Scheduler::CurrentThread, memory_budget_bytes: None }
    }
}

/// Validated runtime configuration.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub socket_path: PathBuf,
    pub socket_group_gid: Option<u32>,
    pub limits: IpcLimits,
    pub runtime: RuntimeSettings,
    pub core: ValidatedConfig,
    /// (source, authorized uids) sorted by source; `None` = socket permissions only.
    pub authorization: Vec<(SourceId, Option<Vec<u32>>)>,
}

impl RuntimeConfig {
    /// `Some(true)` authorized, `Some(false)` refused, `None` unknown source.
    pub fn authorize(&self, source: SourceId, peer_uid: Option<u32>) -> Option<bool> {
        let i = self.authorization.binary_search_by_key(&source, |(s, _)| *s).ok()?;
        let (_, uids) = self.authorization.get(i)?;
        Some(match uids {
            None => true,
            Some(list) => peer_uid.is_some_and(|u| list.contains(&u)),
        })
    }
}

#[derive(Debug)]
pub enum LoadError {
    Io(PathBuf, std::io::Error),
    Parse(String),
    Ipc(String),
    Runtime(String),
    Core(ConfigError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Io(p, e) => write!(f, "cannot read {}: {e}", p.display()),
            LoadError::Parse(e) => write!(f, "invalid TOML: {e}"),
            LoadError::Ipc(e) => write!(f, "invalid [ipc] section: {e}"),
            LoadError::Runtime(e) => write!(f, "invalid [runtime] section: {e}"),
            LoadError::Core(e) => write!(f, "invalid configuration: {e}"),
        }
    }
}

impl std::error::Error for LoadError {}

/// Upper bound of the encoded size of one `Observation` inside a batch
/// (field key + length prefix + channel varint + key + 10-byte varint).
const MAX_OBSERVATION_WIRE_BYTES: u32 = 2 + 6 + 11;
/// Room for the batch envelope (mode, sequence, message keys and lengths).
const BATCH_ENVELOPE_BYTES: u32 = 32;

pub fn load(path: &Path) -> Result<RuntimeConfig, LoadError> {
    let text = std::fs::read_to_string(path).map_err(|e| LoadError::Io(path.to_owned(), e))?;
    parse(&text)
}

pub fn parse(text: &str) -> Result<RuntimeConfig, LoadError> {
    let f: FileConfig = toml::from_str(text).map_err(|e| LoadError::Parse(e.to_string()))?;
    let ipc = f.ipc;
    let limits = IpcLimits {
        max_frame_bytes: ipc.max_frame_bytes,
        max_driver_connections: ipc.max_driver_connections,
        input_queue_capacity: ipc.input_queue_capacity,
        max_inflight_per_connection: ipc.max_inflight_per_connection,
        handshake_timeout_ms: ipc.handshake_timeout_ms,
        frame_timeout_ms: ipc.frame_timeout_ms,
        ack_write_timeout_ms: ipc.ack_write_timeout_ms,
        read_buffer_bytes: ipc.read_buffer_bytes,
        ack_coalesce_max: ipc.ack_coalesce_max,
        ingress: parse_ingress(
            ipc.ingress_policy.as_deref(),
            ipc.per_source_pending_limit,
            ipc.max_inflight_per_connection,
        )
        .map_err(LoadError::Ipc)?,
    };
    let range = |name: &str, v: u64, lo: u64, hi: u64| {
        if v < lo || v > hi { Err(LoadError::Ipc(format!("{name}={v} outside {lo}..={hi}"))) } else { Ok(()) }
    };
    range("max_frame_bytes", limits.max_frame_bytes.into(), 256, MAX_FRAME_BYTES_CEILING.into())?;
    range("max_driver_connections", limits.max_driver_connections.into(), 1, 1024)?;
    range("input_queue_capacity", limits.input_queue_capacity.into(), 1, 65_536)?;
    range("max_inflight_per_connection", limits.max_inflight_per_connection.into(), 1, 1024)?;
    range("handshake_timeout_ms", limits.handshake_timeout_ms, 1, 600_000)?;
    range("frame_timeout_ms", limits.frame_timeout_ms, 1, 600_000)?;
    range("ack_write_timeout_ms", limits.ack_write_timeout_ms, 1, 600_000)?;
    range("read_buffer_bytes", limits.read_buffer_bytes.into(), 0, 1 << 20)?;
    range("ack_coalesce_max", limits.ack_coalesce_max.into(), 1, 1024)?;
    let needed = f
        .core
        .max_observations_per_batch
        .saturating_mul(MAX_OBSERVATION_WIRE_BYTES)
        .saturating_add(BATCH_ENVELOPE_BYTES);
    if limits.max_frame_bytes < needed {
        return Err(LoadError::Ipc(format!(
            "max_frame_bytes={} cannot carry a batch of {} observations (needs {needed})",
            limits.max_frame_bytes, f.core.max_observations_per_batch
        )));
    }

    let scheduler =
        Scheduler::parse(f.runtime.scheduler.as_deref().unwrap_or("current_thread"), f.runtime.worker_threads)
            .map_err(LoadError::Runtime)?;
    let runtime = RuntimeSettings { scheduler, memory_budget_bytes: f.runtime.memory_budget_bytes };

    let spec = ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: f.core.max_observations_per_batch },
        tags: f
            .tags
            .iter()
            .map(|t| TagSpec { tag_id: TagId(t.id), name: t.name.clone(), value_type: t.value_type.into() })
            .collect(),
        sources: f
            .sources
            .iter()
            .map(|s| SourceSpec {
                source_id: SourceId(s.id),
                name: s.name.clone(),
                channels: s
                    .channels
                    .iter()
                    .map(|c| ChannelSpec {
                        channel_id: ChannelId(c.channel),
                        tag_id: TagId(c.tag),
                        value_type: c.value_type.into(),
                    })
                    .collect(),
            })
            .collect(),
    };
    let core = ValidatedConfig::validate(spec).map_err(LoadError::Core)?;
    let mut authorization: Vec<(SourceId, Option<Vec<u32>>)> =
        f.sources.into_iter().map(|s| (SourceId(s.id), s.authorized_uids)).collect();
    authorization.sort_by_key(|(s, _)| *s);
    if authorization.iter().any(|(_, u)| u.as_ref().is_some_and(|l| l.is_empty())) {
        return Err(LoadError::Ipc("authorized_uids must not be empty (omit it instead)".into()));
    }
    Ok(RuntimeConfig {
        socket_path: ipc.socket_path,
        socket_group_gid: ipc.socket_group_gid,
        limits,
        runtime,
        core,
        authorization,
    })
}
