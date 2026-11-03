//! One source session towards the kernel (see the crate documentation).

use std::collections::VecDeque;
use std::fmt;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use scada_protocol::driver_v1::{
    AckStatus, BatchMode, ClockQuality, Continuity, DriverMessage, ErrorCode, Heartbeat, Hello, InputAck,
    KernelMessage, Observation, ObservationBatch, Welcome, driver_message, kernel_message, observation,
};
use scada_protocol::frame::{self, FrameError};
use scada_protocol::{MINOR_DURABLE_RECEIPTS, MINOR_HEARTBEAT, PROTOCOL_MAJOR, PROTOCOL_MINOR};

use crate::frames::{Fill, FrameReader};
use crate::{ChannelType, Value};

/// Bound on any frame read from the kernel (its messages are small).
const READ_MAX_FRAME: u32 = 1 << 20;
/// Absolute bound on the window of unconfirmed batches.
pub const MAX_PENDING_CEILING: usize = 65_536;

/// Explicit link state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LinkState {
    Disconnected,
    Connecting,
    Handshaking,
    /// Connected; the next batch is (or the queued one is) a `FullState`.
    AwaitingFullState,
    /// Connected; a `FullState` of this link was applied, deltas flow.
    Streaming,
    /// A typed refusal (outbox, journal queue, disk) or a full window: retry later.
    Backpressured,
    /// Journal fault, non durable receipt or rejected batch: no new batch until resolved.
    Recovering,
}

impl LinkState {
    pub fn name(self) -> &'static str {
        match self {
            LinkState::Disconnected => "disconnected",
            LinkState::Connecting => "connecting",
            LinkState::Handshaking => "handshaking",
            LinkState::AwaitingFullState => "awaiting_full_state",
            LinkState::Streaming => "streaming",
            LinkState::Backpressured => "backpressured",
            LinkState::Recovering => "recovering",
        }
    }

    pub fn is_connected(self) -> bool {
        !matches!(self, LinkState::Disconnected | LinkState::Connecting | LinkState::Handshaking)
    }
}

/// Bounded exponential backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Backoff {
    fn delay(&self, attempt: u32) -> Duration {
        let f = 1u32.checked_shl(attempt.min(20)).unwrap_or(u32::MAX);
        self.min.saturating_mul(f).min(self.max).max(self.min)
    }
}

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    pub socket: PathBuf,
    /// Diagnostic identity, at most 64 bytes.
    pub driver_id: String,
    pub source_id: u32,
    /// Every configured channel of the source with its type, sorted by id, unique.
    pub channels: Vec<(u32, ChannelType)>,
    /// Ask for durable receipts (minor 1.2). Granted only by a journaling kernel.
    pub request_durable_receipts: bool,
    /// Refuse to stream if durable receipts were requested and not granted.
    pub require_durable_receipts: bool,
    /// Bound on batches not finally confirmed (sent or not yet sent).
    pub max_pending: usize,
    /// Bound on batches sent and not yet answered (also bounded by the kernel's limit).
    pub max_inflight: u32,
    /// Heartbeat period when the link is idle (minor 1.1); `None` = never.
    pub heartbeat_interval: Option<Duration>,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    /// Between failed connection attempts to the kernel.
    pub reconnect_backoff: Backoff,
    /// Before resending a batch refused with a retryable code.
    pub retry_backoff: Backoff,
    /// Transitions kept for diagnostics.
    pub transitions_kept: usize,
}

impl PublisherConfig {
    pub fn new(socket: PathBuf, driver_id: &str, source_id: u32, channels: Vec<(u32, ChannelType)>) -> Self {
        PublisherConfig {
            socket,
            driver_id: driver_id.to_owned(),
            source_id,
            channels,
            request_durable_receipts: false,
            require_durable_receipts: false,
            max_pending: 64,
            max_inflight: 8,
            heartbeat_interval: Some(Duration::from_secs(1)),
            handshake_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            reconnect_backoff: Backoff { min: Duration::from_millis(100), max: Duration::from_secs(10) },
            retry_backoff: Backoff { min: Duration::from_millis(20), max: Duration::from_secs(5) },
            transitions_kept: 64,
        }
    }

    pub fn validate(&self) -> Result<(), SdkError> {
        let bad = |s: String| Err(SdkError::Config(s));
        if self.driver_id.len() > scada_protocol::MAX_DRIVER_ID_BYTES {
            return bad(format!("driver_id longer than {} bytes", scada_protocol::MAX_DRIVER_ID_BYTES));
        }
        if self.channels.is_empty() {
            return bad("a source needs at least one channel".into());
        }
        if !self.channels.windows(2).all(|w| w[0].0 < w[1].0) {
            return bad("channels must be sorted by id and unique".into());
        }
        if self.max_pending == 0 || self.max_pending > MAX_PENDING_CEILING {
            return bad(format!("max_pending must be 1..={MAX_PENDING_CEILING}"));
        }
        if self.max_inflight == 0 {
            return bad("max_inflight must be >= 1".into());
        }
        if self.require_durable_receipts && !self.request_durable_receipts {
            return bad("require_durable_receipts needs request_durable_receipts".into());
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum SdkError {
    Config(String),
    /// The session is not connected: nothing was queued.
    NotConnected,
    /// The scan does not hold exactly one value of the right type per configured channel.
    IncompleteScan(String),
    /// `source_sequence` exhausted for this epoch: restart the driver (new epoch).
    SequenceExhausted,
}

impl fmt::Display for SdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SdkError::Config(s) => write!(f, "configuration: {s}"),
            SdkError::NotConnected => f.write_str("not connected"),
            SdkError::IncompleteScan(s) => write!(f, "incomplete scan: {s}"),
            SdkError::SequenceExhausted => f.write_str("source_sequence exhausted: a new epoch is required"),
        }
    }
}

impl std::error::Error for SdkError {}

/// What [`SourcePublisher::publish_scan`] did with a complete scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Publish {
    FullState {
        seq: u64,
    },
    Delta {
        seq: u64,
        changed: usize,
    },
    /// Nothing changed since the last published state: no batch.
    NoChange,
    /// The window of unconfirmed batches is full, or the session is recovering: the scan is
    /// not published (counted). The next scan carries every change against the last
    /// published state, so no change is lost, only intermediate samples.
    Deferred,
}

/// Monotonic counters of one publisher.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counters {
    pub connects: u64,
    pub connect_failures: u64,
    pub handshakes_rejected: u64,
    pub disconnects: u64,
    pub full_states_sent: u64,
    pub deltas_sent: u64,
    pub batches_resent: u64,
    pub scans_no_change: u64,
    pub scans_deferred: u64,
    pub acks_applied_volatile: u64,
    pub acks_applied_durable: u64,
    pub acks_applied_not_durable: u64,
    pub acks_duplicate: u64,
    pub rejections_retryable: u64,
    pub rejections_collateral: u64,
    pub rejections_fatal: u64,
    pub journal_faults: u64,
    /// Batches whose fate is unknown after a kernel restart (never confirmed durable and
    /// beyond the recovered sequence): replaced by a fresh `FullState`, counted, logged.
    pub batches_lost_kernel_restart: u64,
    pub heartbeats_sent: u64,
    pub protocol_errors: u64,
    pub continuity_baseline: u64,
    pub continuity_resynchronized: u64,
    /// Highest number of batches simultaneously pending.
    pub pending_high_water: u64,
}

/// One recorded state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub at: Instant,
    pub from: LinkState,
    pub to: LinkState,
    pub reason: String,
}

/// Life of a pending batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PState {
    /// To be sent (first time or again, in order).
    Queued,
    /// Sent on the current link, ACK expected.
    InFlight,
    /// Known applied (at or below `last_accepted` after a reconnect, or answered
    /// APPLIED_NOT_DURABLE) but not proven durable: never resent, settled by a durable
    /// answer for a later sequence (prefix order) or by a kernel restart.
    AppliedAwaitingDurable,
}

#[derive(Debug, Clone)]
struct Pending {
    seq: u64,
    full: bool,
    batch: ObservationBatch,
    st: PState,
    /// Answered APPLIED_NOT_DURABLE: cannot become durable in this kernel instance.
    not_durable: bool,
    /// Already sent once on some link (for the resend counter).
    ever_sent: bool,
    /// In flight when an earlier batch was refused: its own refusal is collateral.
    collateral: Option<Collateral>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Collateral {
    /// Earlier batch refused with a retryable code: resend this one after it.
    Retry,
    /// Earlier batch rejected for good: this one (built on top of it) is void too.
    Drop,
}

#[derive(Debug)]
struct Link {
    stream: UnixStream,
    reader: FrameReader,
    wbuf: Vec<u8>,
    welcome: Welcome,
    durable: bool,
    inflight_limit: usize,
    max_frame: u32,
    max_obs: usize,
    heartbeat_seq: u64,
    last_write: Instant,
    full_state_applied: bool,
}

/// A source session (see the crate documentation).
#[derive(Debug)]
pub struct SourcePublisher {
    cfg: PublisherConfig,
    epoch: u64,
    next_seq: u64,
    state: LinkState,
    link: Option<Link>,
    pending: VecDeque<Pending>,
    /// State the kernel will hold once every pending batch is applied (basis of deltas).
    projected: Vec<Option<Value>>,
    needs_full_state: bool,
    last_instance: Option<Vec<u8>>,
    connect_at: Instant,
    connect_attempts: u32,
    /// After a retryable refusal of this sequence: later refusals are collateral, resend
    /// from here after `retry_at`.
    retry_from: Option<u64>,
    retry_at: Instant,
    retry_attempts: u32,
    /// After a fatal refusal: no new batch before this instant (no tight rejection loop).
    hold_until: Instant,
    journal_fault: bool,
    counters: Counters,
    transitions: VecDeque<Transition>,
    last_error: Option<String>,
    last_continuity: Option<Continuity>,
    last_operation: Option<u64>,
    durable_confirmed_seq: Option<u64>,
}

fn is_retryable(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::OutboxCapacityExceeded
            | ErrorCode::JournalQueueFull
            | ErrorCode::DiskFull
            | ErrorCode::JournalFault
            | ErrorCode::KernelShuttingDown
    )
}

fn obs(channel_id: u32, v: Value) -> Observation {
    Observation {
        channel_id,
        value: Some(match v {
            Value::Bool(b) => observation::Value::BoolValue(b),
            Value::I64(i) => observation::Value::I64Value(i),
        }),
    }
}

impl SourcePublisher {
    pub fn new(cfg: PublisherConfig, epoch: u64) -> Result<SourcePublisher, SdkError> {
        cfg.validate()?;
        if epoch == 0 {
            return Err(SdkError::Config("source_epoch must be non-zero".into()));
        }
        let n = cfg.channels.len();
        let now = Instant::now();
        Ok(SourcePublisher {
            cfg,
            epoch,
            next_seq: 1,
            state: LinkState::Disconnected,
            link: None,
            pending: VecDeque::new(),
            projected: vec![None; n],
            needs_full_state: true,
            last_instance: None,
            connect_at: now,
            connect_attempts: 0,
            retry_from: None,
            retry_at: now,
            retry_attempts: 0,
            hold_until: now,
            journal_fault: false,
            counters: Counters::default(),
            transitions: VecDeque::new(),
            last_error: None,
            last_continuity: None,
            last_operation: None,
            durable_confirmed_seq: None,
        })
    }

    pub fn config(&self) -> &PublisherConfig {
        &self.cfg
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn state(&self) -> LinkState {
        self.state
    }

    pub fn counters(&self) -> Counters {
        self.counters
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn inflight_len(&self) -> usize {
        self.pending.iter().filter(|p| p.st == PState::InFlight).count()
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_seq
    }

    pub fn transitions(&self) -> impl Iterator<Item = &Transition> {
        self.transitions.iter()
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn last_continuity(&self) -> Option<Continuity> {
        self.last_continuity
    }

    pub fn last_operation_sequence(&self) -> Option<u64> {
        self.last_operation
    }

    /// Highest sequence confirmed `APPLIED_DURABLE` (or durable by prefix).
    pub fn durable_confirmed_sequence(&self) -> Option<u64> {
        self.durable_confirmed_seq
    }

    /// Negotiated minor of the current link.
    pub fn negotiated_minor(&self) -> Option<u32> {
        self.link.as_ref().map(|l| l.welcome.negotiated_protocol_minor)
    }

    pub fn durable_receipts_active(&self) -> bool {
        self.link.as_ref().is_some_and(|l| l.durable)
    }

    pub fn welcome(&self) -> Option<&Welcome> {
        self.link.as_ref().map(|l| &l.welcome)
    }

    fn set_state(&mut self, to: LinkState, reason: impl Into<String>) {
        if to == self.state {
            return;
        }
        let t = Transition { at: Instant::now(), from: self.state, to, reason: reason.into() };
        if self.transitions.len() >= self.cfg.transitions_kept.max(1) {
            self.transitions.pop_front();
        }
        self.transitions.push_back(t);
        self.state = to;
    }

    fn error(&mut self, e: impl Into<String>) {
        self.last_error = Some(e.into());
    }

    /// State after the link becomes usable again (no refusal pending).
    fn settled_state(&self) -> LinkState {
        if self.journal_fault || self.pending.iter().any(|p| p.not_durable) {
            LinkState::Recovering
        } else if self.retry_from.is_some() {
            LinkState::Backpressured
        } else if self.link.as_ref().is_some_and(|l| l.full_state_applied) && !self.needs_full_state {
            LinkState::Streaming
        } else {
            LinkState::AwaitingFullState
        }
    }

    // ------------------------------------------------------------------ connection

    /// Connect and handshake if disconnected and the reconnect backoff has elapsed.
    /// Returns whether the session is connected afterwards. Failures are recorded
    /// (`last_error`, counters) and retried later; only configuration errors are returned.
    pub fn ensure_connected(&mut self) -> Result<bool, SdkError> {
        if self.link.is_some() {
            return Ok(true);
        }
        let now = Instant::now();
        if now < self.connect_at {
            return Ok(false);
        }
        self.set_state(LinkState::Connecting, "connect");
        match self.handshake() {
            Ok(link) => {
                self.connect_attempts = 0;
                self.counters.connects += 1;
                self.on_welcome(link)?;
                Ok(true)
            }
            Err(e) => {
                self.counters.connect_failures += 1;
                self.connect_attempts = self.connect_attempts.saturating_add(1);
                self.connect_at = Instant::now() + self.cfg.reconnect_backoff.delay(self.connect_attempts - 1);
                self.error(e.clone());
                self.set_state(LinkState::Disconnected, e);
                Ok(false)
            }
        }
    }

    fn handshake(&mut self) -> Result<Link, String> {
        let mut stream = UnixStream::connect(&self.cfg.socket).map_err(|e| format!("connect: {e}"))?;
        stream.set_write_timeout(Some(self.cfg.write_timeout)).map_err(|e| e.to_string())?;
        self.set_state(LinkState::Handshaking, "hello");
        let hello = DriverMessage {
            body: Some(driver_message::Body::Hello(Hello {
                driver_id: self.cfg.driver_id.clone(),
                source_id: self.cfg.source_id,
                source_epoch: self.epoch,
                supported_protocol_majors: vec![PROTOCOL_MAJOR],
                protocol_minor: PROTOCOL_MINOR,
                request_durable_receipts: self.cfg.request_durable_receipts,
            })),
        };
        let mut wbuf = Vec::new();
        frame::encode_frame(&hello, READ_MAX_FRAME, &mut wbuf).map_err(|e| e.to_string())?;
        stream.write_all(&wbuf).map_err(|e| format!("write hello: {e}"))?;
        let mut reader = FrameReader::new(READ_MAX_FRAME);
        let deadline = Instant::now() + self.cfg.handshake_timeout;
        let msg: KernelMessage = loop {
            match reader.next_message() {
                Ok(Some(m)) => break m,
                Ok(None) => {}
                Err(e) => return Err(format!("handshake frame: {e}")),
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err("handshake timeout".into());
            }
            stream.set_read_timeout(Some(left)).map_err(|e| e.to_string())?;
            match reader.fill(&mut stream) {
                Ok(Fill::Data(_)) | Ok(Fill::Timeout) => {}
                Ok(Fill::Eof) => return Err("kernel closed during handshake".into()),
                Err(e) => return Err(format!("read: {e}")),
            }
        };
        let welcome = match msg.body {
            Some(kernel_message::Body::Welcome(w)) => w,
            Some(kernel_message::Body::HandshakeRejected(r)) => {
                self.counters.handshakes_rejected += 1;
                return Err(format!("handshake rejected: {} ({})", r.error_code().as_str_name(), r.detail));
            }
            other => return Err(format!("unexpected handshake answer: {other:?}")),
        };
        let limits = welcome.limits.unwrap_or_default();
        let durable = welcome.durable_receipts
            && welcome.negotiated_protocol_minor >= MINOR_DURABLE_RECEIPTS
            && self.cfg.request_durable_receipts;
        Ok(Link {
            stream,
            reader,
            wbuf,
            durable,
            inflight_limit: (self.cfg.max_inflight.min(limits.max_inflight_batches.max(1))) as usize,
            max_frame: if limits.max_frame_bytes == 0 { 65_536 } else { limits.max_frame_bytes },
            max_obs: limits.max_observations_per_batch as usize,
            heartbeat_seq: 0,
            last_write: Instant::now(),
            full_state_applied: false,
            welcome,
        })
    }

    /// Reconcile the pending window with the kernel's view (contract §5, §5.1, §7, §10.4).
    fn on_welcome(&mut self, link: Link) -> Result<(), SdkError> {
        let w = link.welcome.clone();
        if self.cfg.require_durable_receipts && !link.durable {
            self.connect_at = Instant::now() + self.cfg.reconnect_backoff.max;
            let e = "durable receipts required but not granted by the kernel";
            self.error(e);
            self.set_state(LinkState::Disconnected, e);
            return Ok(());
        }
        if link.max_obs != 0 && self.cfg.channels.len() > link.max_obs {
            return Err(SdkError::Config(format!(
                "source {} has {} channels but the kernel accepts {} observations per batch: a FullState is impossible",
                self.cfg.source_id,
                self.cfg.channels.len(),
                link.max_obs
            )));
        }
        let same_instance = self.last_instance.as_deref() == Some(w.kernel_instance_id.as_slice());
        if !same_instance {
            // A new kernel instance: every ACK and fault of the previous one is void (§7).
            self.journal_fault = false;
        }
        let journaling = !w.timeline_id.is_empty();
        let last = w.last_accepted_source_sequence;
        let durable = link.durable;
        let mut kept = VecDeque::new();
        let mut lost: Vec<u64> = Vec::new();
        for mut p in std::mem::take(&mut self.pending) {
            let at_or_below = last.is_some_and(|l| p.seq <= l);
            if w.full_state_required {
                if at_or_below {
                    // Recovered prefix of a journaling kernel: durable by construction.
                    if journaling && !same_instance {
                        self.durable_confirmed_seq = Some(self.durable_confirmed_seq.unwrap_or(0).max(p.seq));
                    }
                } else {
                    lost.push(p.seq);
                }
                continue;
            }
            p.not_durable = false;
            p.collateral = None;
            if !at_or_below {
                p.st = PState::Queued;
                kept.push_back(p);
            } else if durable {
                // Applied, receipt lost. The batch at `last` itself is resent: its DUPLICATE
                // answer waits for the commit of every applied operation (VS-006), a durable
                // proof for the whole prefix. Earlier ones are settled by that answer.
                p.st = if Some(p.seq) == last { PState::Queued } else { PState::AppliedAwaitingDurable };
                kept.push_back(p);
            }
            // Volatile link: applied is final, nothing to keep.
        }
        if !lost.is_empty() {
            self.counters.batches_lost_kernel_restart += lost.len() as u64;
            self.error(format!(
                "kernel without baseline for this epoch: {} unconfirmed batch(es) {}..={} after the recovered sequence {} are lost; a fresh FullState replaces them",
                lost.len(),
                lost.first().copied().unwrap_or(0),
                lost.last().copied().unwrap_or(0),
                last.map_or("-".into(), |l| l.to_string())
            ));
        }
        self.pending = kept;
        if let Some(l) = last {
            self.next_seq = self.next_seq.max(l.saturating_add(1));
        }
        self.retry_from = None;
        self.last_instance = Some(w.kernel_instance_id.clone());
        // First scan after every (re)connection is a FullState (VS-007 §4).
        self.needs_full_state = true;
        let replay = self.pending.iter().filter(|p| p.st == PState::Queued).count();
        self.link = Some(link);
        self.set_state(
            LinkState::AwaitingFullState,
            format!(
                "welcome minor={} durable={} full_state_required={} last_accepted={} replay={replay} lost={}",
                w.negotiated_protocol_minor,
                durable,
                w.full_state_required,
                last.map_or("-".into(), |l| l.to_string()),
                lost.len()
            ),
        );
        self.flush_sends();
        Ok(())
    }

    /// Close the link (the kernel applies `SourceDisconnected`: the source's tags become
    /// `Uncertain(SourceUnavailable)`). Pending batches are kept for replay on the same
    /// epoch.
    pub fn disconnect(&mut self, reason: &str) {
        if let Some(l) = self.link.take() {
            let _ = l.stream.shutdown(std::net::Shutdown::Both);
            self.counters.disconnects += 1;
        }
        for p in self.pending.iter_mut() {
            if p.st == PState::InFlight {
                p.st = PState::Queued;
            }
            p.collateral = None;
        }
        self.retry_from = None;
        self.set_state(LinkState::Disconnected, reason.to_owned());
    }

    /// Disconnect and wait `backoff` before the next connection attempt.
    fn fail_link(&mut self, reason: String) {
        self.counters.protocol_errors += 1;
        self.error(reason.clone());
        self.disconnect(&reason);
        self.connect_attempts = self.connect_attempts.saturating_add(1);
        self.connect_at = Instant::now() + self.cfg.reconnect_backoff.delay(self.connect_attempts - 1);
    }

    // ------------------------------------------------------------------ publishing

    /// Publish one **complete** scan: `values[i]` is the value of `channels[i]`. The
    /// caller guarantees every value comes from a successful read of this scan; a scan
    /// with any failed read must not be published (VS-007 §4).
    ///
    /// `acquired_at_utc_ns` is the driver's acquisition time (sent only on a 1.2 link,
    /// annotated with clock quality `UNKNOWN`); no per-observation source timestamp is sent.
    pub fn publish_scan(&mut self, values: &[Value], acquired_at_utc_ns: Option<i64>) -> Result<Publish, SdkError> {
        if values.len() != self.cfg.channels.len() {
            return Err(SdkError::IncompleteScan(format!(
                "{} values for {} channels",
                values.len(),
                self.cfg.channels.len()
            )));
        }
        for (v, (ch, t)) in values.iter().zip(&self.cfg.channels) {
            if v.channel_type() != *t {
                return Err(SdkError::IncompleteScan(format!(
                    "channel {ch}: {} value for a {} channel",
                    v.channel_type().name(),
                    t.name()
                )));
            }
        }
        let Some(link) = self.link.as_ref() else { return Err(SdkError::NotConnected) };
        let minor = link.welcome.negotiated_protocol_minor;
        if self.journal_fault
            || self.retry_from.is_some()
            || self.pending.len() >= self.cfg.max_pending
            || Instant::now() < self.hold_until
        {
            self.counters.scans_deferred += 1;
            if self.pending.len() >= self.cfg.max_pending && self.state != LinkState::Recovering {
                self.set_state(LinkState::Backpressured, format!("pending window full ({})", self.pending.len()));
            }
            return Ok(Publish::Deferred);
        }
        let full = self.needs_full_state;
        let observations: Vec<Observation> = if full {
            self.cfg.channels.iter().zip(values).map(|((c, _), v)| obs(*c, *v)).collect()
        } else {
            self.cfg
                .channels
                .iter()
                .zip(values)
                .zip(&self.projected)
                .filter(|((_, v), p)| p.as_ref() != Some(*v))
                .map(|(((c, _), v), _)| obs(*c, *v))
                .collect()
        };
        if observations.is_empty() {
            self.counters.scans_no_change += 1;
            return Ok(Publish::NoChange);
        }
        let seq = self.next_seq;
        if seq == u64::MAX {
            return Err(SdkError::SequenceExhausted);
        }
        self.next_seq += 1;
        let changed = observations.len();
        let with_time = minor >= MINOR_DURABLE_RECEIPTS && acquired_at_utc_ns.is_some();
        let batch = ObservationBatch {
            mode: if full { BatchMode::FullState } else { BatchMode::Delta } as i32,
            source_sequence: seq,
            observations,
            acquired_at_utc_ns: if with_time { acquired_at_utc_ns } else { None },
            source_clock_quality: if with_time { ClockQuality::Unknown as i32 } else { 0 },
            source_clock_id: String::new(),
            source_timestamps_utc_ns: Vec::new(),
        };
        for (p, v) in self.projected.iter_mut().zip(values) {
            *p = Some(*v);
        }
        self.needs_full_state = false;
        self.pending.push_back(Pending {
            seq,
            full,
            batch,
            st: PState::Queued,
            not_durable: false,
            ever_sent: false,
            collateral: None,
        });
        self.counters.pending_high_water = self.counters.pending_high_water.max(self.pending.len() as u64);
        if full {
            self.counters.full_states_sent += 1;
        } else {
            self.counters.deltas_sent += 1;
        }
        self.flush_sends();
        Ok(if full { Publish::FullState { seq } } else { Publish::Delta { seq, changed } })
    }

    /// Send queued batches within the window. A FullState is sent only when nothing is in
    /// flight (so it can never overtake a refused batch and apply as RESYNCHRONIZED).
    fn flush_sends(&mut self) {
        if self.retry_from.is_some() && Instant::now() < self.retry_at {
            return;
        }
        if self.retry_from.is_some() && self.inflight_len() > 0 {
            return; // collateral answers still expected
        }
        if self.retry_from.is_some() {
            self.retry_from = None;
            let s = self.settled_state();
            self.set_state(s, "retry");
        }
        loop {
            let Some(link) = self.link.as_ref() else { return };
            let limit = link.inflight_limit;
            let inflight = self.inflight_len();
            if inflight >= limit {
                return;
            }
            let Some(idx) = self.pending.iter().position(|p| p.st == PState::Queued) else { return };
            let Some(p) = self.pending.get(idx) else { return };
            if p.full && inflight > 0 {
                return;
            }
            let (seq, resend) = (p.seq, p.ever_sent);
            let msg = DriverMessage { body: Some(driver_message::Body::Batch(p.batch.clone())) };
            if let Err(e) = self.write(&msg) {
                self.fail_link(format!("write batch {seq}: {e}"));
                return;
            }
            if resend {
                self.counters.batches_resent += 1;
            }
            if let Some(p) = self.pending.get_mut(idx) {
                p.st = PState::InFlight;
                p.ever_sent = true;
            }
        }
    }

    fn write(&mut self, msg: &DriverMessage) -> Result<(), String> {
        let Some(link) = self.link.as_mut() else { return Err("not connected".into()) };
        frame::encode_frame(msg, link.max_frame, &mut link.wbuf).map_err(|e| match e {
            FrameError::EncodeTooLarge { len, max } => {
                format!("batch of {len} bytes exceeds the kernel frame limit {max}")
            }
            other => other.to_string(),
        })?;
        link.stream.write_all(&link.wbuf).map_err(|e| e.to_string())?;
        link.last_write = Instant::now();
        Ok(())
    }

    // ------------------------------------------------------------------ ACKs

    /// Read and process kernel messages until `wait` elapses, resend after backoff, send a
    /// heartbeat if the link is idle. Returns early once nothing is in flight and nothing is
    /// waiting to be sent, if `return_when_idle`.
    pub fn pump(&mut self, wait: Duration, return_when_idle: bool) {
        let deadline = Instant::now() + wait;
        loop {
            self.flush_sends();
            self.maybe_heartbeat();
            if self.link.is_none() {
                return;
            }
            // Process every complete buffered message first.
            loop {
                let next = match self.link.as_mut() {
                    Some(l) => l.reader.next_message::<KernelMessage>(),
                    None => return,
                };
                match next {
                    Ok(Some(m)) => self.on_message(m),
                    Ok(None) => break,
                    Err(e) => {
                        self.fail_link(format!("kernel frame: {e}"));
                        return;
                    }
                }
                if self.link.is_none() {
                    return;
                }
            }
            self.flush_sends();
            let idle = self.inflight_len() == 0;
            let now = Instant::now();
            if (return_when_idle && idle && !self.has_sendable()) || now >= deadline {
                return;
            }
            let mut until = deadline;
            if self.retry_from.is_some() && self.retry_at > now {
                until = until.min(self.retry_at);
            }
            if let (Some(hb), Some(l)) = (self.cfg.heartbeat_interval, self.link.as_ref()) {
                until = until.min(l.last_write + hb);
            }
            let left = until.saturating_duration_since(now).max(Duration::from_millis(1));
            let r = match self.link.as_mut() {
                Some(l) => l.stream.set_read_timeout(Some(left)).and_then(|_| l.reader.fill(&mut l.stream)),
                None => return,
            };
            match r {
                Ok(Fill::Data(_)) | Ok(Fill::Timeout) => {}
                Ok(Fill::Eof) => {
                    self.error("kernel closed the connection");
                    self.disconnect("kernel closed the connection");
                    return;
                }
                Err(e) => {
                    self.fail_link(format!("read: {e}"));
                    return;
                }
            }
        }
    }

    /// Wait until nothing is in flight (or `timeout`).
    pub fn drain(&mut self, timeout: Duration) -> bool {
        self.pump(timeout, true);
        self.inflight_len() == 0
    }

    fn has_sendable(&self) -> bool {
        self.pending.iter().any(|p| p.st == PState::Queued)
    }

    fn maybe_heartbeat(&mut self) {
        let Some(hb) = self.cfg.heartbeat_interval else { return };
        let Some(l) = self.link.as_mut() else { return };
        if l.welcome.negotiated_protocol_minor < MINOR_HEARTBEAT || l.last_write.elapsed() < hb {
            return;
        }
        l.heartbeat_seq += 1;
        let msg =
            DriverMessage { body: Some(driver_message::Body::Heartbeat(Heartbeat { sequence: l.heartbeat_seq })) };
        match self.write(&msg) {
            Ok(()) => self.counters.heartbeats_sent += 1,
            Err(e) => self.fail_link(format!("write heartbeat: {e}")),
        }
    }

    fn on_message(&mut self, m: KernelMessage) {
        match m.body {
            Some(kernel_message::Body::Ack(a)) => self.on_ack(a),
            Some(kernel_message::Body::ProtocolError(p)) => {
                let reason = format!("protocol error from kernel: {} ({})", p.error_code().as_str_name(), p.detail);
                self.fail_link(reason);
            }
            other => self.fail_link(format!("unexpected kernel message: {other:?}")),
        }
    }

    fn on_ack(&mut self, a: InputAck) {
        let instance_ok = self.link.as_ref().is_some_and(|l| l.welcome.kernel_instance_id == a.kernel_instance_id);
        if !instance_ok {
            self.fail_link("ACK of another kernel instance".into());
            return;
        }
        // ACKs come in send order: it must answer the oldest batch in flight.
        let Some(idx) = self.pending.iter().position(|p| p.st == PState::InFlight) else {
            self.fail_link(format!("ACK for {} with nothing in flight", a.source_sequence));
            return;
        };
        let Some(front) = self.pending.get(idx) else { return };
        if front.seq != a.source_sequence {
            let msg = format!("ACK for {} while {} is the oldest in flight", a.source_sequence, front.seq);
            self.fail_link(msg);
            return;
        }
        let seq = a.source_sequence;
        let was_full = front.full;
        let collateral = front.collateral;
        let status = AckStatus::try_from(a.status).unwrap_or(AckStatus::Unspecified);
        let code = ErrorCode::try_from(a.error_code).unwrap_or(ErrorCode::Unspecified);
        let durable_link = self.link.as_ref().is_some_and(|l| l.durable);
        if let Some(c) = Continuity::try_from(a.continuity).ok().filter(|c| *c != Continuity::Unspecified) {
            self.last_continuity = Some(c);
            match c {
                Continuity::BaselineEstablished => self.counters.continuity_baseline += 1,
                Continuity::Resynchronized => self.counters.continuity_resynchronized += 1,
                _ => {}
            }
        }
        if a.operation_sequence.is_some() {
            self.last_operation = a.operation_sequence;
        }
        match status {
            AckStatus::AppliedVolatile | AckStatus::AppliedDurable | AckStatus::Duplicate => {
                let durable = durable_link && matches!(status, AckStatus::AppliedDurable | AckStatus::Duplicate);
                match status {
                    AckStatus::AppliedVolatile => self.counters.acks_applied_volatile += 1,
                    AckStatus::AppliedDurable => self.counters.acks_applied_durable += 1,
                    _ => self.counters.acks_duplicate += 1,
                }
                if collateral.is_some() {
                    // Applied after a refused earlier batch: only a FullState could, and
                    // none is sent with a batch in flight. Defensive: resynchronise.
                    self.needs_full_state = true;
                }
                // Everything up to `seq` is settled (prefix order).
                while self.pending.front().is_some_and(|p| p.seq <= seq) {
                    self.pending.pop_front();
                }
                if durable {
                    self.durable_confirmed_seq = Some(self.durable_confirmed_seq.unwrap_or(0).max(seq));
                }
                if was_full && let Some(l) = self.link.as_mut() {
                    l.full_state_applied = true;
                }
                if self.retry_from == Some(seq) {
                    self.retry_from = None;
                }
                self.retry_attempts = 0;
                let s = self.settled_state();
                self.set_state(s, format!("ack {seq} {}", status.as_str_name()));
            }
            AckStatus::AppliedNotDurable => {
                // Applied in RAM, the commit failed: keep it (never silently dropped) until
                // a restarted kernel tells what it recovered.
                self.counters.acks_applied_not_durable += 1;
                self.journal_fault = true;
                if let Some(p) = self.pending.get_mut(idx) {
                    p.st = PState::AppliedAwaitingDurable;
                    p.not_durable = true;
                }
                if was_full && let Some(l) = self.link.as_mut() {
                    l.full_state_applied = true;
                }
                self.error(format!("batch {seq} APPLIED_NOT_DURABLE ({})", code.as_str_name()));
                self.set_state(LinkState::Recovering, format!("ack {seq} APPLIED_NOT_DURABLE"));
            }
            AckStatus::Rejected => self.on_rejected(idx, seq, code, a.last_accepted_source_sequence, collateral),
            AckStatus::Unspecified => self.fail_link(format!("ACK {seq} with unknown status {}", a.status)),
        }
    }

    fn on_rejected(
        &mut self,
        idx: usize,
        seq: u64,
        code: ErrorCode,
        last: Option<u64>,
        collateral: Option<Collateral>,
    ) {
        match collateral {
            Some(Collateral::Retry) => {
                // Refused because an earlier batch was: resend in order later.
                self.counters.rejections_collateral += 1;
                if let Some(p) = self.pending.get_mut(idx) {
                    p.st = PState::Queued;
                    p.collateral = None;
                }
                return;
            }
            Some(Collateral::Drop) => {
                self.counters.rejections_collateral += 1;
                self.pending.remove(idx);
                return;
            }
            None => {}
        }
        if is_retryable(code) {
            self.counters.rejections_retryable += 1;
            if code == ErrorCode::JournalFault {
                self.counters.journal_faults += 1;
                self.journal_fault = true;
            }
            if let Some(p) = self.pending.get_mut(idx) {
                p.st = PState::Queued;
            }
            for p in self.pending.iter_mut().filter(|p| p.st == PState::InFlight && p.seq > seq) {
                p.collateral = Some(Collateral::Retry);
            }
            self.retry_from = Some(seq);
            self.retry_at = Instant::now() + self.cfg.retry_backoff.delay(self.retry_attempts);
            self.retry_attempts = self.retry_attempts.saturating_add(1);
            self.error(format!("batch {seq} refused {} (retry same sequence)", code.as_str_name()));
            let to = if self.journal_fault { LinkState::Recovering } else { LinkState::Backpressured };
            self.set_state(to, format!("refused {seq} {}", code.as_str_name()));
            return;
        }
        // Not retryable: the batch is void. Later ones in flight will be refused as gaps.
        self.counters.rejections_fatal += 1;
        self.pending.remove(idx);
        // Unsent later batches were computed on top of the void one: drop them.
        let mut dropped = 0u64;
        self.pending.retain(|p| {
            let keep = p.st != PState::Queued || p.seq < seq;
            if !keep {
                dropped += 1;
            }
            keep
        });
        self.counters.rejections_collateral += dropped;
        for p in self.pending.iter_mut().filter(|p| p.st == PState::InFlight && p.seq > seq) {
            p.collateral = Some(Collateral::Drop);
        }
        self.needs_full_state = true;
        if let Some(l) = last {
            self.next_seq = self.next_seq.max(l.saturating_add(1));
        }
        self.hold_until = Instant::now() + self.cfg.retry_backoff.delay(self.retry_attempts);
        self.retry_attempts = self.retry_attempts.saturating_add(1);
        self.error(format!("batch {seq} rejected {} (FullState follows)", code.as_str_name()));
        self.set_state(LinkState::Recovering, format!("rejected {seq} {}", code.as_str_name()));
    }
}
