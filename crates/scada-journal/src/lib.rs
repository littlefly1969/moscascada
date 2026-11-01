//! VS-006 durable journal (ADR 0013, ADR 0014).
//!
//! One SQLite store per kernel, written by **one** thread ([`writer`]) in group-commit
//! transactions under `synchronous=FULL`. The sequencer never waits for it: it reserves
//! room in the bounded journal queue *before* `Kernel::apply` (J4) and hands over one
//! [`OpRecord`] per applied operation, including zero-event operations. Durable progress
//! is published only after `COMMIT` returned (J1); the sequencer turns it into
//! `AdvanceDurableWatermark` (J3).
//!
//! [`recovery`] rebuilds a kernel from the last complete checkpoint plus a verified replay
//! of the durable suffix (J2, J9). [`reader`] serves the historian on read-only
//! connections. Nothing in this crate reads a clock to decide anything: time values are
//! annotations produced by the runtime's time authority and stored as they are (J7).

pub mod codec;
pub mod fault;
pub mod progress;
pub mod reader;
pub mod reconcile;
pub mod recovery;
pub mod schema;
pub mod store;
pub mod writer;

use std::fmt;

use scada_core::{AlarmEvent, Operation};

pub use progress::{Hist, JournalShared, JournalState, LatSummary};
pub use store::{Store, StoreInfo};

/// Durability label of the store (ADR 0013 D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// `synchronous=FULL`: the only mode that carries `POWER_LOSS_DURABLE`.
    Full,
    /// `synchronous=NORMAL`: benchmark comparison only, never durable receipts.
    NormalBenchmarkOnly,
}

impl Durability {
    pub fn label(self) -> &'static str {
        match self {
            Durability::Full => "POWER_LOSS_DURABLE (synchronous=FULL)",
            Durability::NormalBenchmarkOnly => "NOT_DURABLE_BENCHMARK_ONLY (synchronous=NORMAL)",
        }
    }
}

/// `TimeEnvelope.clock_flags` bits (ADR 0014 §3).
pub mod clock_flags {
    /// Realtime lower than the realtime of the previous operation.
    pub const REGRESSED: u8 = 1;
    /// `logical_time_ns != sequenced_at_utc_ns`.
    pub const LOGICAL_HELD: u8 = 2;
    /// First sample of a boot lower than the last persisted logical time.
    pub const CROSS_BOOT_REGRESSION: u8 = 4;
    /// Forward step larger than the configured alert threshold.
    pub const JUMP_FORWARD: u8 = 8;
}

/// Clock quality codes (same numbers as `scada.driver.v1.ClockQuality` and
/// `scada.history.v1.ClockState`).
pub mod clock_quality {
    pub const UNSPECIFIED: u8 = 0;
    pub const UNKNOWN: u8 = 1;
    pub const SYNCHRONIZED: u8 = 2;
    pub const HOLDOVER: u8 = 3;
    pub const FAULT: u8 = 4;
}

/// Per-operation time annotation sampled once by the runtime before `apply` (ADR 0014
/// §3). Never part of the kernel state or of any digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeEnvelope {
    pub sequenced_at_utc_ns: i64,
    pub boottime_ns: u64,
    pub logical_time_ns: i64,
    pub clock_quality: u8,
    pub clock_flags: u8,
    pub received_at_utc_ns: Option<i64>,
}

/// Source timestamps of a batch (wire 1.2 sidecar, ADR 0014 §6), kept as reported.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceTimes {
    pub acquired_at_utc_ns: Option<i64>,
    pub clock_quality: u8,
    pub clock_id: String,
    /// `(channel_id, source_timestamp_utc_ns)` of the observations that carried one, in
    /// batch order.
    pub observations: Vec<(u32, i64)>,
}

/// Maximum `clock_id` length on the wire.
pub const MAX_CLOCK_ID_BYTES: usize = 64;
/// Maximum operator note length (ADR 0013 D11).
pub const MAX_NOTE_BYTES: usize = 256;

/// Where an event's `origin_utc_ns` came from (`scada.history.v1.TimeSource`).
pub mod time_source {
    pub const NONE: u8 = 0;
    pub const DEVICE: u8 = 1;
    pub const DRIVER: u8 = 2;
}

/// Origin time of one event of the record (same order as `OpRecord::events`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EventOrigin {
    pub utc_ns: Option<i64>,
    pub source: u8,
    pub clock_quality: u8,
}

/// Operator action with an idempotency key (ADR 0013 D11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorAction {
    pub action_id: [u8; 16],
    pub alarm_id: u32,
    pub requested_token: u64,
    pub actor_uid: u32,
    pub note: String,
    /// `scada.alarm.v1.AckStatus` code of the outcome.
    pub outcome: u8,
    pub event_seq: Option<u64>,
    pub occurrence_id: u64,
}

/// Live verification of a source (`scada.history.v1.LiveVerification`).
pub mod live {
    pub const NEVER_CONNECTED: u8 = 1;
    pub const UNVERIFIED_RECOVERED: u8 = 2;
    pub const AWAITING_FULL_STATE: u8 = 3;
    pub const LIVE_VERIFIED: u8 = 4;
    pub const REPLAYING_BUFFERED_HISTORY: u8 = 5;
}

/// History continuity (`scada.history.v1.HistoryContinuity`), J8.
pub mod continuity {
    pub const COMPLETE: u8 = 1;
    pub const UNKNOWN: u8 = 2;
    pub const GAP: u8 = 3;
}

/// Why a gap / unknown continuity was recorded.
pub mod gap_reason {
    /// The SCADA was down; the source kept its epoch.
    pub const SCADA_DOWN: u8 = 1;
    /// The source restarted (new epoch): what happened in between is unknown.
    pub const DRIVER_RESTART: u8 = 2;
    /// Missing source sequence numbers in the same epoch.
    pub const SEQUENCE_GAP: u8 = 3;
    /// Buffered stream: hole or overflow.
    pub const BUFFER_GAP: u8 = 4;
}

/// A recorded gap (never removed by a later full state).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapRecord {
    pub source_epoch: Option<u64>,
    pub from_seq: Option<u64>,
    pub to_seq: Option<u64>,
    pub bounds_unknown: bool,
    pub reason: u8,
}

/// Upsert of `source_recovery` (and optional gap) carried by an operation record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRecoveryUpdate {
    pub source_id: u32,
    pub source_epoch: Option<u64>,
    pub live: u8,
    pub history: u8,
    pub last_source_sequence: Option<u64>,
    /// Operation that verified the source (`LIVE_VERIFIED`), else 0.
    pub verified_at_operation_seq: u64,
    pub gap: Option<GapRecord>,
}

/// Clock anomaly of an operation (ADR 0014 §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeAnomaly {
    /// 1 regression, 2 jump forward, 3 cross-boot regression.
    pub kind: u8,
    pub delta_ns: i64,
}

/// Result reported to a durable-receipt waiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitResult {
    /// The transaction containing the operation returned from `COMMIT` under
    /// `synchronous=FULL`.
    Durable,
    /// Applied in RAM but the journal failed: not durable (the client must reconcile).
    NotDurable,
}

/// Called by the writer once, after the commit that contains the operation (or on fault).
pub type Receipt = Box<dyn FnOnce(CommitResult) + Send>;

/// One applied operation on its way to the writer (ADR 0013 D5).
pub struct OpRecord {
    pub operation_seq: u64,
    pub operation: Operation,
    pub time: TimeEnvelope,
    pub source_times: Option<Box<SourceTimes>>,
    /// Events produced by the operation, in `event_seq` order (copied from the ring).
    pub events: Vec<AlarmEvent>,
    /// Origin time per event (empty when none is known).
    pub origins: Vec<EventOrigin>,
    /// First full state of a recovered source: events are reconciliation (J6).
    pub reconciliation: bool,
    pub action: Option<OperatorAction>,
    pub recovery: Vec<SourceRecoveryUpdate>,
    pub anomaly: Option<TimeAnomaly>,
    /// Bytes still reserved for this record in the journal queue (released after commit).
    pub reserved_bytes: u64,
    pub receipt: Option<Receipt>,
}

impl fmt::Debug for OpRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpRecord")
            .field("operation_seq", &self.operation_seq)
            .field("operation", &self.operation)
            .field("events", &self.events.len())
            .field("reconciliation", &self.reconciliation)
            .field("reserved_bytes", &self.reserved_bytes)
            .finish_non_exhaustive()
    }
}

/// Fixed part of a record in the reservation estimate (row overhead, envelope, digest).
pub const RECORD_FIXED_BYTES: u64 = 160;
/// Bytes of one observation in the payload (`channel u32, type u8, value i64`).
pub const OBSERVATION_BYTES: u64 = 13;
/// Bytes reserved by a [`WriterMsg::Notify`].
pub const NOTIFY_BYTES: u64 = 64;
/// Bytes reserved per event (48 B record + origin + bookkeeping).
pub const EVENT_RECORD_BYTES: u64 = 96;

impl OpRecord {
    /// Bytes this record actually holds in the queue (the reservation after `apply`).
    pub fn queued_bytes(&self) -> u64 {
        let obs = match &self.operation {
            Operation::ObservationBatch(b) => b.observations.len() as u64,
            _ => 0,
        };
        let st =
            self.source_times.as_ref().map_or(0, |s| 16 + s.clock_id.len() as u64 + 12 * s.observations.len() as u64);
        let note = self.action.as_ref().map_or(0, |a| 64 + a.note.len() as u64);
        RECORD_FIXED_BYTES
            + obs * OBSERVATION_BYTES
            + st
            + note
            + self.events.len() as u64 * EVENT_RECORD_BYTES
            + 48 * self.recovery.len() as u64
    }
}

/// A checkpoint exported by the sequencer after operation `operation_seq` (ADR 0013 D7).
#[derive(Debug)]
pub struct CheckpointMsg {
    pub checkpoint: scada_core::KernelCheckpoint,
    pub state_digest: [u8; 32],
    pub event_seq_high: u64,
}

/// Messages to the writer, in sequencer order.
pub enum WriterMsg {
    Op(Box<OpRecord>),
    Checkpoint(Box<CheckpointMsg>),
    /// Call `receipt` once every operation `<= operation_seq` is durable (duplicate
    /// durable-ACK retry of an operation still in flight).
    Notify {
        operation_seq: u64,
        receipt: Receipt,
    },
}

impl fmt::Debug for WriterMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriterMsg::Op(r) => r.fmt(f),
            WriterMsg::Checkpoint(c) => write!(f, "Checkpoint({})", c.checkpoint.last_operation_sequence),
            WriterMsg::Notify { operation_seq, .. } => write!(f, "Notify({operation_seq})"),
        }
    }
}

/// Errors of the store. Every error at startup is fail-closed (ADR 0013 D8).
#[derive(Debug)]
pub enum JournalError {
    Io(String),
    Sqlite(rusqlite::Error),
    /// Integrity, checksum, sequence or replay divergence.
    Corrupt(String),
    /// Unknown schema, other application, other configuration.
    Incompatible(String),
    /// Another process holds the store.
    Locked(String),
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Io(s) => write!(f, "journal I/O error: {s}"),
            JournalError::Sqlite(e) => write!(f, "journal SQLite error: {e}"),
            JournalError::Corrupt(s) => write!(f, "journal corrupt: {s}"),
            JournalError::Incompatible(s) => write!(f, "journal incompatible: {s}"),
            JournalError::Locked(s) => write!(f, "journal locked: {s}"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<rusqlite::Error> for JournalError {
    fn from(e: rusqlite::Error) -> Self {
        JournalError::Sqlite(e)
    }
}

impl From<std::io::Error> for JournalError {
    fn from(e: std::io::Error) -> Self {
        JournalError::Io(e.to_string())
    }
}

/// `u64` → SQLite integer (all journal sequences are `< 2^63`; anything else is a bug and
/// is stored saturated so it fails the later contiguity checks instead of wrapping).
#[inline]
pub fn sq(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// SQLite integer → `u64` (negative values are corrupt rows and map to `u64::MAX`).
#[inline]
pub fn uq(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(u64::MAX)
}

/// Realtime clock in nanoseconds since the Unix epoch (annotations only).
pub fn realtime_ns() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

/// `CLOCK_BOOTTIME` in nanoseconds (suspend-aware; never compared across boots).
pub fn boottime_ns() -> u64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
    u64::try_from(t.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::try_from(t.tv_nsec).unwrap_or(0))
}

/// `/proc/sys/kernel/random/boot_id` (empty when unavailable).
pub fn os_boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id").map(|s| s.trim().to_owned()).unwrap_or_default()
}

/// 16 random bytes from the OS.
pub fn random_id() -> std::io::Result<[u8; 16]> {
    use std::io::Read;
    let mut id = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut id)?;
    Ok(id)
}

/// Lowercase hex.
pub fn hex(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}
