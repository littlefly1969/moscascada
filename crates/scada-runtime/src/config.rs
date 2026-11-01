//! Static TOML configuration, validated at startup. An invalid file refuses startup.

use std::fmt;
use std::path::{Path, PathBuf};

use scada_core::{
    AlarmCondition, AlarmId, AlarmRuleSpec, AlarmSpec, ChannelId, ChannelSpec, ConfigError, ConfigSpec, CoreLimits,
    Severity, SourceId, SourceSpec, Supervision, TagId, TagSpec, ValidatedConfig, ValueType,
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
    /// VS-005: absent = alarms disabled, VS-004 behaviour unchanged.
    alarms: Option<FileAlarms>,
    /// VS-006 (ADR 0013): absent = volatile VS-005 behaviour, nothing opened (J10).
    persistence: Option<FilePersistence>,
    /// VS-006 (ADR 0014): time authority settings; only with `[persistence]`.
    time: Option<FileTime>,
}

/// VS-006 `[persistence]` (ADR 0013 D1, D4, D5, D7, D12, D13).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePersistence {
    path: PathBuf,
    /// `"full"` (default, the only durable mode) or `"normal"` (benchmarks only, needs
    /// `--test-allow-non-durable`).
    durability: Option<String>,
    journal_queue_operations: Option<u32>,
    journal_queue_bytes: Option<u64>,
    commit_max_operations: Option<u32>,
    commit_max_delay_ms: Option<u64>,
    checkpoint_every_operations: Option<u64>,
    checkpoints_keep: Option<u32>,
    /// `"quick"` (default), `"full"` or `"none"`.
    startup_check: Option<String>,
    sqlite_cache_kib: Option<u32>,
    wal_max_bytes: Option<u64>,
    min_disk_free_bytes: Option<u64>,
    max_db_bytes: Option<u64>,
    operator_actions_cache: Option<u32>,
    history_socket_path: Option<PathBuf>,
    history_authorized_uids: Option<Vec<u32>>,
    max_history_connections: Option<u32>,
    max_history_page: Option<u32>,
    history_max_scan: Option<u64>,
}

/// VS-006 `[time]` (ADR 0014).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileTime {
    clock_regression_alert_ms: Option<u64>,
    clock_jump_alert_ms: Option<u64>,
    /// `"none"` (default), `"chronyc"` or `"timedatectl"`.
    host_sync_probe: Option<String>,
    host_sync_probe_period_ms: Option<u64>,
}

/// Integrity check at startup (`[persistence] startup_check`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupCheck {
    None,
    Quick,
    Full,
}

/// VS-006: validated `[persistence]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceSettings {
    pub path: PathBuf,
    /// false = `"normal"` (benchmark only, NOT durable).
    pub durable: bool,
    pub journal_queue_operations: u32,
    pub journal_queue_bytes: u64,
    pub commit_max_operations: u32,
    pub commit_max_delay_ms: u64,
    pub checkpoint_every_operations: u64,
    pub checkpoints_keep: u32,
    pub startup_check: StartupCheck,
    pub sqlite_cache_kib: u32,
    pub wal_max_bytes: u64,
    pub min_disk_free_bytes: u64,
    pub max_db_bytes: Option<u64>,
    pub operator_actions_cache: u32,
    pub history_socket_path: Option<PathBuf>,
    /// Sorted; non-empty when the history socket is configured.
    pub history_authorized_uids: Vec<u32>,
    pub max_history_connections: u32,
    pub max_history_page: u32,
    pub history_max_scan: u64,
}

/// VS-006: host clock probe (ADR 0014 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostSyncProbe {
    None,
    Chronyc,
    Timedatectl,
}

/// VS-006: validated `[time]` (defaults when absent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeSettings {
    pub clock_regression_alert_ms: u64,
    pub clock_jump_alert_ms: u64,
    pub host_sync_probe: HostSyncProbe,
    pub host_sync_probe_period_ms: u64,
}

impl Default for TimeSettings {
    fn default() -> Self {
        TimeSettings {
            clock_regression_alert_ms: 1_000,
            clock_jump_alert_ms: 60_000,
            host_sync_probe: HostSyncProbe::None,
            host_sync_probe_period_ms: 60_000,
        }
    }
}

fn parse_persistence(
    p: Option<FilePersistence>,
    t: Option<FileTime>,
) -> Result<(Option<PersistenceSettings>, TimeSettings), LoadError> {
    let e = |m: String| LoadError::Persistence(m);
    let time = match t {
        None => TimeSettings::default(),
        Some(_) if p.is_none() => return Err(LoadError::Persistence("[time] requires [persistence]".into())),
        Some(t) => {
            let d = TimeSettings::default();
            let probe = match t.host_sync_probe.as_deref().unwrap_or("none") {
                "none" => HostSyncProbe::None,
                "chronyc" => HostSyncProbe::Chronyc,
                "timedatectl" => HostSyncProbe::Timedatectl,
                other => return Err(e(format!("[time] host_sync_probe {other:?} (none | chronyc | timedatectl)"))),
            };
            let ts = TimeSettings {
                clock_regression_alert_ms: t.clock_regression_alert_ms.unwrap_or(d.clock_regression_alert_ms),
                clock_jump_alert_ms: t.clock_jump_alert_ms.unwrap_or(d.clock_jump_alert_ms),
                host_sync_probe: probe,
                host_sync_probe_period_ms: t.host_sync_probe_period_ms.unwrap_or(d.host_sync_probe_period_ms),
            };
            if !(1_000..=3_600_000).contains(&ts.host_sync_probe_period_ms) {
                return Err(e("[time] host_sync_probe_period_ms outside 1000..=3600000".into()));
            }
            ts
        }
    };
    let Some(p) = p else { return Ok((None, time)) };
    let durable = match p.durability.as_deref().unwrap_or("full") {
        "full" => true,
        "normal" => false,
        other => return Err(e(format!("durability {other:?} (full | normal)"))),
    };
    let startup_check = match p.startup_check.as_deref().unwrap_or("quick") {
        "quick" => StartupCheck::Quick,
        "full" => StartupCheck::Full,
        "none" => StartupCheck::None,
        other => return Err(e(format!("startup_check {other:?} (quick | full | none)"))),
    };
    let range = |name: &str, v: u64, lo: u64, hi: u64| {
        if v < lo || v > hi { Err(e(format!("{name}={v} outside {lo}..={hi}"))) } else { Ok(v) }
    };
    let s = PersistenceSettings {
        path: p.path,
        durable,
        journal_queue_operations: range(
            "journal_queue_operations",
            p.journal_queue_operations.unwrap_or(4096).into(),
            16,
            1 << 20,
        )? as u32,
        journal_queue_bytes: range("journal_queue_bytes", p.journal_queue_bytes.unwrap_or(64 << 20), 1 << 16, 4 << 30)?,
        commit_max_operations: range(
            "commit_max_operations",
            p.commit_max_operations.unwrap_or(1024).into(),
            1,
            1 << 20,
        )? as u32,
        commit_max_delay_ms: range("commit_max_delay_ms", p.commit_max_delay_ms.unwrap_or(0), 0, 1_000)?,
        checkpoint_every_operations: range(
            "checkpoint_every_operations",
            p.checkpoint_every_operations.unwrap_or(100_000),
            1,
            1 << 40,
        )?,
        checkpoints_keep: range("checkpoints_keep", p.checkpoints_keep.unwrap_or(2).into(), 1, 16)? as u32,
        startup_check,
        sqlite_cache_kib: range("sqlite_cache_kib", p.sqlite_cache_kib.unwrap_or(8192).into(), 256, 1 << 20)? as u32,
        wal_max_bytes: range("wal_max_bytes", p.wal_max_bytes.unwrap_or(64 << 20), 1 << 20, 16 << 30)?,
        min_disk_free_bytes: p.min_disk_free_bytes.unwrap_or(256 << 20),
        max_db_bytes: p.max_db_bytes,
        operator_actions_cache: range(
            "operator_actions_cache",
            p.operator_actions_cache.unwrap_or(16_384).into(),
            16,
            1 << 20,
        )? as u32,
        history_socket_path: p.history_socket_path,
        history_authorized_uids: Vec::new(),
        max_history_connections: range("max_history_connections", p.max_history_connections.unwrap_or(4).into(), 1, 64)?
            as u32,
        max_history_page: range("max_history_page", p.max_history_page.unwrap_or(1000).into(), 1, 65_536)? as u32,
        history_max_scan: range("history_max_scan", p.history_max_scan.unwrap_or(100_000), 1_000, 100_000_000)?,
    };
    let mut s = s;
    match (&s.history_socket_path, p.history_authorized_uids) {
        (Some(_), Some(u)) if !u.is_empty() => {
            s.history_authorized_uids = u;
            s.history_authorized_uids.sort_unstable();
            s.history_authorized_uids.dedup();
        }
        (Some(_), _) => return Err(e("history_socket_path requires a non-empty history_authorized_uids".into())),
        (None, Some(_)) => return Err(e("history_authorized_uids without history_socket_path".into())),
        (None, None) => {}
    }
    Ok((Some(s), time))
}

/// VS-005 `[alarms]` (ADR 0012).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAlarms {
    /// Configured ceiling of the rule count (default: the number of rules).
    max_rules: Option<u32>,
    /// Outbox capacity in events (slots), required: no default size is imposed.
    outbox_capacity_events: u64,
    /// Occupancy percentage reported as `HIGH` (default 80).
    outbox_high_percent: Option<u32>,
    /// Local control socket (snapshot, events, ACK, capacity). Absent = no control plane.
    control_socket_path: Option<PathBuf>,
    /// Peer uids allowed on the control socket; required with `control_socket_path`.
    control_authorized_uids: Option<Vec<u32>>,
    #[serde(default = "d_alarm_conn")]
    max_control_connections: u32,
    /// Largest page of events / alarms per request (default 1024).
    #[serde(default = "d_alarm_page")]
    control_page_max: u32,
    #[serde(default)]
    rules: Vec<FileRule>,
}

fn d_alarm_conn() -> u32 {
    4
}
fn d_alarm_page() -> u32 {
    1024
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRule {
    id: u32,
    name: String,
    tag_id: u32,
    kind: String,
    trip: Option<i64>,
    reset: Option<i64>,
    expected: Option<bool>,
    severity: String,
}

/// VS-005: runtime side of `[alarms]` (the rules themselves live in the core config).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlarmSettings {
    pub control_socket_path: Option<PathBuf>,
    /// Sorted, non-empty when the socket is configured.
    pub control_authorized_uids: Vec<u32>,
    pub max_control_connections: u32,
    pub control_page_max: u32,
    pub outbox_high_percent: u32,
}

/// VS-005: accepted ranges.
pub const MAX_CONTROL_CONNECTIONS: u32 = 64;
pub const MAX_CONTROL_PAGE: u32 = 65_536;

fn parse_rule(r: &FileRule) -> Result<AlarmRuleSpec, LoadError> {
    let e = |m: String| LoadError::Alarms(format!("rule {}: {m}", r.id));
    let severity = match r.severity.as_str() {
        "info" => Severity::Info,
        "warning" => Severity::Warning,
        "major" => Severity::Major,
        "critical" => Severity::Critical,
        other => return Err(e(format!("unknown severity {other:?} (info | warning | major | critical)"))),
    };
    let none = |name: &str, present: bool| {
        if present { Err(e(format!("{name} is not valid for kind {:?}", r.kind))) } else { Ok(()) }
    };
    let condition = match r.kind.as_str() {
        "bool_equals" => {
            none("trip", r.trip.is_some())?;
            none("reset", r.reset.is_some())?;
            AlarmCondition::BoolEquals {
                expected: r.expected.ok_or_else(|| e("bool_equals requires expected".into()))?,
            }
        }
        "high" | "low" => {
            none("expected", r.expected.is_some())?;
            let trip = r.trip.ok_or_else(|| e(format!("{} requires trip", r.kind)))?;
            let reset = r.reset.ok_or_else(|| e(format!("{} requires reset", r.kind)))?;
            if r.kind == "high" { AlarmCondition::High { trip, reset } } else { AlarmCondition::Low { trip, reset } }
        }
        "bad_quality" => {
            none("trip", r.trip.is_some())?;
            none("reset", r.reset.is_some())?;
            none("expected", r.expected.is_some())?;
            AlarmCondition::BadQuality
        }
        other => return Err(e(format!("unknown kind {other:?} (bool_equals | high | low | bad_quality)"))),
    };
    Ok(AlarmRuleSpec { alarm_id: AlarmId(r.id), name: r.name.clone(), tag_id: TagId(r.tag_id), condition, severity })
}

fn parse_alarms(a: Option<FileAlarms>) -> Result<(Option<AlarmSpec>, Option<AlarmSettings>), LoadError> {
    let Some(a) = a else { return Ok((None, None)) };
    let rules = a.rules.iter().map(parse_rule).collect::<Result<Vec<_>, _>>()?;
    let max_rules = match a.max_rules {
        Some(m) => m,
        None => u32::try_from(rules.len()).map_err(|_| LoadError::Alarms("too many rules".into()))?,
    };
    if !(1..=MAX_CONTROL_CONNECTIONS).contains(&a.max_control_connections) {
        return Err(LoadError::Alarms(format!(
            "max_control_connections={} outside 1..={MAX_CONTROL_CONNECTIONS}",
            a.max_control_connections
        )));
    }
    if !(1..=MAX_CONTROL_PAGE).contains(&a.control_page_max) {
        return Err(LoadError::Alarms(format!(
            "control_page_max={} outside 1..={MAX_CONTROL_PAGE}",
            a.control_page_max
        )));
    }
    let high = a.outbox_high_percent.unwrap_or(80);
    if !(1..=100).contains(&high) {
        return Err(LoadError::Alarms(format!("outbox_high_percent={high} outside 1..=100")));
    }
    let mut uids = match (&a.control_socket_path, a.control_authorized_uids) {
        (Some(_), Some(u)) if !u.is_empty() => u,
        (Some(_), _) => {
            return Err(LoadError::Alarms(
                "control_socket_path requires a non-empty control_authorized_uids (acknowledgements are authenticated)"
                    .into(),
            ));
        }
        (None, Some(_)) => return Err(LoadError::Alarms("control_authorized_uids without control_socket_path".into())),
        (None, None) => Vec::new(),
    };
    uids.sort_unstable();
    uids.dedup();
    Ok((
        Some(AlarmSpec { max_rules, outbox_capacity_events: a.outbox_capacity_events, rules }),
        Some(AlarmSettings {
            control_socket_path: a.control_socket_path,
            control_authorized_uids: uids,
            max_control_connections: a.max_control_connections,
            control_page_max: a.control_page_max,
            outbox_high_percent: high,
        }),
    ))
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
    /// VS-003 (ADR 0010): `"fifo"` (default, VS-002) or `"source_fair"` (experimental).
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
    /// VS-004 (ADR 0011): period of the single watchdog. Used only when at least one source
    /// sets a liveness or freshness timeout (forbidden otherwise). Default 250 ms.
    watchdog_poll_ms: Option<u64>,
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
    /// VS-004: optional liveness supervision (absent = none).
    liveness_timeout_ms: Option<u64>,
    /// VS-004: optional per-tag freshness supervision (absent = none).
    freshness_timeout_ms: Option<u64>,
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
    /// C: per-source bounded lanes served round-robin (one request per turn), at most
    /// `per_source` batches per source and `input_queue_capacity` in total.
    SourceFair { per_source: u32 },
}

impl IngressPolicy {
    pub fn per_source(&self) -> Option<u32> {
        match *self {
            IngressPolicy::Fifo => None,
            IngressPolicy::SourceFair { per_source } => Some(per_source),
        }
    }
}

impl fmt::Display for IngressPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngressPolicy::Fifo => write!(f, "fifo"),
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
        "source_fair" => Ok(IngressPolicy::SourceFair { per_source: need("source_fair")? }),
        other => Err(format!("unknown ingress_policy {other:?} (fifo | source_fair)")),
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

/// VS-004: default `[runtime] watchdog_poll_ms`.
pub const DEFAULT_WATCHDOG_POLL_MS: u64 = 250;
/// VS-004: accepted range of `watchdog_poll_ms`.
pub const WATCHDOG_POLL_RANGE_MS: (u64, u64) = (10, 60_000);
/// VS-004: an enabled timeout must be at least this many watchdog periods (granularity:
/// a timeout is detected up to one period late, ADR 0011 §6).
pub const MIN_TIMEOUT_POLLS: u64 = 3;
/// VS-004: largest accepted timeout (24 h): beyond it a timeout is a configuration error.
pub const MAX_TIMEOUT_MS: u64 = 86_400_000;
/// VS-004: largest `freshness_timeout_ms / watchdog_poll_ms`: bounds the freshness sample
/// ring of a source to `MAX_FRESHNESS_POLLS + 2` entries (24 B each). A longer timeout
/// needs a longer poll period.
pub const MAX_FRESHNESS_POLLS: u64 = 4096;

/// VS-004: timeouts of one supervised source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourcePolicy {
    pub source_id: SourceId,
    pub liveness_timeout_ms: Option<u64>,
    pub freshness_timeout_ms: Option<u64>,
}

/// VS-004: watchdog settings, present only when at least one source is supervised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionConfig {
    pub watchdog_poll_ms: u64,
    /// Supervised sources only, sorted by source id.
    pub sources: Vec<SourcePolicy>,
}

impl SupervisionConfig {
    pub fn policy(&self, source: SourceId) -> Option<&SourcePolicy> {
        self.sources.binary_search_by_key(&source, |p| p.source_id).ok().and_then(|i| self.sources.get(i))
    }
}

impl fmt::Display for SupervisionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let live = self.sources.iter().filter(|p| p.liveness_timeout_ms.is_some()).count();
        let fresh = self.sources.iter().filter(|p| p.freshness_timeout_ms.is_some()).count();
        write!(
            f,
            "watchdog_poll_ms={} supervised_sources={} liveness={live} freshness={fresh} [",
            self.watchdog_poll_ms,
            self.sources.len()
        )?;
        for (i, p) in self.sources.iter().enumerate() {
            let ms = |v: Option<u64>| v.map_or_else(|| "-".to_owned(), |x| x.to_string());
            write!(
                f,
                "{}{}:live={}ms,fresh={}ms",
                if i > 0 { " " } else { "" },
                p.source_id,
                ms(p.liveness_timeout_ms),
                ms(p.freshness_timeout_ms)
            )?;
        }
        f.write_str("]")
    }
}

/// VS-004: validate the supervision settings (fail fast, clear errors).
fn parse_supervision(poll: Option<u64>, sources: &[FileSource]) -> Result<Option<SupervisionConfig>, LoadError> {
    let any = sources.iter().any(|s| s.liveness_timeout_ms.is_some() || s.freshness_timeout_ms.is_some());
    if !any {
        return match poll {
            Some(_) => Err(LoadError::Runtime(
                "watchdog_poll_ms is only valid when a source sets liveness_timeout_ms or freshness_timeout_ms".into(),
            )),
            None => Ok(None),
        };
    }
    let poll = poll.unwrap_or(DEFAULT_WATCHDOG_POLL_MS);
    let (lo, hi) = WATCHDOG_POLL_RANGE_MS;
    if !(lo..=hi).contains(&poll) {
        return Err(LoadError::Runtime(format!("watchdog_poll_ms={poll} outside {lo}..={hi}")));
    }
    let min = poll.saturating_mul(MIN_TIMEOUT_POLLS);
    let check = |id: u32, name: &str, v: Option<u64>| -> Result<(), LoadError> {
        match v {
            Some(t) if t < min || t > MAX_TIMEOUT_MS => Err(LoadError::Runtime(format!(
                "source {id}: {name}={t} outside {min}..={MAX_TIMEOUT_MS} (at least {MIN_TIMEOUT_POLLS} x watchdog_poll_ms={poll})"
            ))),
            _ => Ok(()),
        }
    };
    let mut out = Vec::new();
    for s in sources {
        check(s.id, "liveness_timeout_ms", s.liveness_timeout_ms)?;
        check(s.id, "freshness_timeout_ms", s.freshness_timeout_ms)?;
        if let Some(t) = s.freshness_timeout_ms
            && t.div_ceil(poll) > MAX_FRESHNESS_POLLS
        {
            return Err(LoadError::Runtime(format!(
                "source {}: freshness_timeout_ms={t} is more than {MAX_FRESHNESS_POLLS} x watchdog_poll_ms={poll} \
                 (bounded sample ring); raise watchdog_poll_ms",
                s.id
            )));
        }
        if s.liveness_timeout_ms.is_some() || s.freshness_timeout_ms.is_some() {
            out.push(SourcePolicy {
                source_id: SourceId(s.id),
                liveness_timeout_ms: s.liveness_timeout_ms,
                freshness_timeout_ms: s.freshness_timeout_ms,
            });
        }
    }
    out.sort_by_key(|p| p.source_id);
    Ok(Some(SupervisionConfig { watchdog_poll_ms: poll, sources: out }))
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
    /// VS-004: `None` unless at least one source is supervised (default: none).
    pub supervision: Option<SupervisionConfig>,
    /// VS-005: `None` unless `[alarms]` is present (the rules are in `core`).
    pub alarms: Option<AlarmSettings>,
    /// VS-006: `None` unless `[persistence]` is present (default: volatile, J10).
    pub persistence: Option<PersistenceSettings>,
    /// VS-006: time authority settings (defaults when `[time]` is absent).
    pub time: TimeSettings,
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
    /// VS-005: `[alarms]` (runtime part; rule semantics are `Core` errors).
    Alarms(String),
    /// VS-006: `[persistence]` / `[time]`.
    Persistence(String),
    Core(ConfigError),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Io(p, e) => write!(f, "cannot read {}: {e}", p.display()),
            LoadError::Parse(e) => write!(f, "invalid TOML: {e}"),
            LoadError::Ipc(e) => write!(f, "invalid [ipc] section: {e}"),
            LoadError::Runtime(e) => write!(f, "invalid [runtime] section: {e}"),
            LoadError::Alarms(e) => write!(f, "invalid [alarms] section: {e}"),
            LoadError::Persistence(e) => write!(f, "invalid [persistence] section: {e}"),
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
    let supervision = parse_supervision(f.runtime.watchdog_poll_ms, &f.sources)?;

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
                supervision: Supervision {
                    liveness: s.liveness_timeout_ms.is_some(),
                    freshness: s.freshness_timeout_ms.is_some(),
                },
            })
            .collect(),
    };
    let (alarm_spec, alarms) = parse_alarms(f.alarms)?;
    let (persistence, time) = parse_persistence(f.persistence, f.time)?;
    let core = ValidatedConfig::validate_with_alarms(spec, alarm_spec).map_err(LoadError::Core)?;
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
        supervision,
        alarms,
        persistence,
        time,
    })
}
