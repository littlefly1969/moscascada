//! The deterministic transition function and the in-memory RTDB.

use std::fmt;
use std::sync::Arc;

use crate::alarm::{AlarmEngine, AlarmEvent, AlarmView, EventPage, OutboxStatus, SnapshotPage};
use crate::config::ValidatedConfig;
use crate::digest::CanonicalHasher;
use crate::types::{
    AlarmId, BatchMode, ChannelId, Continuity, Quality, SourceId, Supervision, TagId, UncertainReason, Value, ValueType,
};
use crate::{STATE_DIGEST_VERSION, STATE_DIGEST_VERSION_ALARMS, STATE_DIGEST_VERSION_SUPERVISED};

/// One observation of a channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    pub channel_id: ChannelId,
    pub value: Value,
}

/// A batch of observations from one source on one link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationBatch {
    pub source_id: SourceId,
    pub link_generation: u64,
    pub mode: BatchMode,
    pub source_sequence: u64,
    pub observations: Vec<Observation>,
}

/// Normalized kernel operation. Network events become explicit, ordered inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    SourceConnected {
        source_id: SourceId,
        link_generation: u64,
        source_epoch: u64,
    },
    SourceDisconnected {
        source_id: SourceId,
        link_generation: u64,
    },
    ObservationBatch(ObservationBatch),
    /// VS-004 (liveness supervision): the runtime saw no liveness evidence of the active
    /// link `link_generation` within the source's timeout. Tags with a value move to
    /// `Uncertain(SourceSilent)` (precedence rules); the link stays `Connected`.
    SourceSilenceExpired {
        source_id: SourceId,
        link_generation: u64,
    },
    /// VS-004 (liveness supervision): a heartbeat arrived on the silent active link. Only
    /// the liveness indicator changes; no tag quality changes (a heartbeat is not a
    /// measurement).
    SourceLivenessRestored {
        source_id: SourceId,
        link_generation: u64,
    },
    /// VS-004 (freshness supervision): every `Good` tag of the source whose last applied
    /// observation has an operation sequence `<= observed_up_to` moves to
    /// `Uncertain(StaleData)`. The token is an operation sequence, never a time: a tag
    /// observed again after the runtime computed the token (even with an identical value,
    /// so with an unchanged revision) has a larger marker and is not affected.
    FreshnessExpired {
        source_id: SourceId,
        link_generation: u64,
        observed_up_to: u64,
    },
    /// VS-005 (ADR 0012): an operator acknowledgement, sequenced like every other input.
    /// `expected_activation_token` must be the alarm's current activation token (the
    /// operation sequence of its latest Raised/Retriggered); `actor_uid` is the
    /// authenticated peer uid captured by the runtime, never a value the client declared.
    AlarmAcknowledge {
        alarm_id: AlarmId,
        expected_activation_token: u64,
        actor_uid: u32,
    },
    /// VS-005: the durable sink confirmed every event `<= up_to_event_seq` (continuous
    /// prefix); their outbox slots become reusable. VS-005 has no durable sink: only a
    /// test harness emits it (ADR 0012 §5). Never emitted because an event was *read*.
    AdvanceDurableWatermark {
        up_to_event_seq: u64,
    },
}

/// An operation with the applied-transition number proposed by the external sequencer.
/// It must equal `last_operation_sequence + 1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequencedInput {
    pub operation_sequence: u64,
    pub operation: Operation,
}

/// What an applied transition did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppliedKind {
    Connected {
        /// No baseline for this epoch: the next batch must be a `FullState`.
        full_state_required: bool,
        /// Last accepted `source_sequence` of the same epoch, if any.
        last_accepted_source_sequence: Option<u64>,
        /// Tags whose quality became `Uncertain`.
        degraded_tags: u32,
    },
    Disconnected {
        degraded_tags: u32,
    },
    Batch {
        continuity: Continuity,
        changed_tags: u32,
    },
    /// VS-004.
    SilenceExpired {
        degraded_tags: u32,
    },
    /// VS-004: liveness indicator back to alive; no tag changed.
    LivenessRestored,
    /// VS-004.
    FreshnessExpired {
        expired_tags: u32,
    },
    /// VS-005: the acknowledgement was applied and produced event `event_seq`.
    AlarmAcknowledged {
        alarm_id: AlarmId,
        event_seq: u64,
    },
    /// VS-005: `released` outbox slots became reusable.
    DurableWatermarkAdvanced {
        up_to_event_seq: u64,
        released: u64,
    },
}

/// Why an input was a no-op that is not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoredReason {
    /// Disconnection (or VS-004 supervision operation) of a link generation that is no
    /// longer the active one.
    StaleLinkGeneration,
    /// VS-004: silence of a source that is already silent.
    AlreadySilent,
    /// VS-004: liveness restoration of a source that is not silent.
    NotSilent,
    /// VS-004: freshness expiry that would change no tag.
    NothingExpired,
    /// VS-005: acknowledgement of the current token of an already acknowledged alarm.
    AlreadyAcknowledged,
    /// VS-005: watermark equal to the current one.
    WatermarkUnchanged,
}

/// Successful result of [`Kernel::apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionOutcome {
    /// The transition was applied; `operation_sequence` was consumed.
    Applied { operation_sequence: u64, kind: AppliedKind },
    /// Same sequence and identical canonical payload as the last accepted batch.
    /// Nothing changed; no operation sequence was consumed.
    Duplicate { source_sequence: u64 },
    /// No effect; no operation sequence was consumed.
    Ignored(IgnoredReason),
}

/// Rejection. Whenever `apply` returns an error the logical state is unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyError {
    OperationSequenceMismatch {
        expected: u64,
        got: u64,
    },
    OperationSequenceExhausted,
    UnknownSource(SourceId),
    InvalidEpoch,
    NonMonotonicLinkGeneration {
        current: u64,
        got: u64,
    },
    UnknownLinkGeneration {
        got: u64,
    },
    /// The batch arrived on a link that is not the active one (superseded or closed).
    LinkNotActive {
        got: u64,
    },
    EmptyBatch,
    BatchTooLarge {
        len: usize,
        max: u32,
    },
    UnknownChannel(ChannelId),
    DuplicateChannel(ChannelId),
    TypeMismatch {
        channel_id: ChannelId,
        expected: ValueType,
        got: ValueType,
    },
    IncompleteFullState {
        expected: usize,
        got: usize,
    },
    InvalidSequence,
    ResyncRequired,
    SequenceGap {
        expected: u64,
        got: u64,
    },
    StaleSequence {
        last_accepted: u64,
        got: u64,
    },
    SequenceConflict {
        source_sequence: u64,
    },
    SequenceOverflow,
    RevisionOverflow(TagId),
    /// VS-004: a supervision operation for a source whose configuration does not enable
    /// that kind of supervision.
    SupervisionNotConfigured(SourceId),
    /// VS-004: a freshness token that does not designate an already applied operation.
    InvalidFreshnessToken {
        observed_up_to: u64,
    },
    /// VS-005: an alarm operation on a kernel without `[alarms]`.
    AlarmsNotConfigured,
    UnknownAlarm(AlarmId),
    /// VS-005: the alarm never had an episode.
    NotAcknowledgeable(AlarmId),
    /// VS-005: the ACK quotes a token that is not the alarm's current one (e.g. issued
    /// before a retrigger).
    StaleActivationToken {
        alarm_id: AlarmId,
        current: u64,
    },
    /// VS-005: the transition would produce `required` events but only `free` outbox slots
    /// are free (slots are freed only by the durable watermark). Nothing was applied.
    OutboxCapacityExceeded {
        required: u64,
        free: u64,
    },
    /// VS-005: the event sequence would overflow `u64` (never wraps).
    EventSequenceExhausted,
    /// VS-005: an occurrence counter would overflow.
    AlarmCounterOverflow(AlarmId),
    /// VS-005: more rules touched than the preallocated worst-case fanout (cannot happen
    /// with a validated configuration; typed instead of growing the scratch).
    AlarmFanoutExceeded {
        limit: u64,
    },
    /// VS-005: watermark beyond the last committed event.
    WatermarkBeyondCommitted {
        up_to: u64,
        last_committed: u64,
    },
    /// VS-005: watermark below the current one (it is monotonic).
    WatermarkRegression {
        current: u64,
        got: u64,
    },
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ApplyError {}

/// Link state of a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkState {
    NeverConnected,
    Connected { generation: u64 },
    Disconnected { generation: u64 },
}

impl LinkState {
    fn generation(self) -> Option<u64> {
        match self {
            LinkState::NeverConnected => None,
            LinkState::Connected { generation } | LinkState::Disconnected { generation } => Some(generation),
        }
    }
}

/// Read-only view of a source, for the host (handshake answers, ACK hints).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceStatus {
    pub link: LinkState,
    pub epoch: Option<u64>,
    pub last_accepted_source_sequence: Option<u64>,
    /// VS-004: the active link is silent (liveness supervision only; always false otherwise).
    pub silent: bool,
    /// VS-004: operation sequence of the last applied batch of the source (0 = none;
    /// maintained only for a supervised source).
    pub last_batch_operation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TagCell {
    value: Option<Value>,
    quality: Quality,
    revision: u64,
    /// VS-004: operation sequence of the last applied batch that observed this tag (0 =
    /// never). Maintained only when the owning source has freshness supervision: the
    /// anti-stale token of `FreshnessExpired`.
    observed: u64,
    /// VS-004 freshness order (ADR 0011 D5): links of the intrusive per-source list of the
    /// source's `Good` tags in increasing `observed` order. Meaningful only while the cell
    /// is a member (freshness-supervised source and `quality == Good`); [`NIL`] otherwise.
    prev: u32,
    next: u32,
}

/// End of a freshness list.
const NIL: u32 = u32::MAX;

/// Canonical copy of the last accepted batch of a source, for exact deduplication.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LastBatch {
    present: bool,
    mode: BatchMode,
    source_sequence: u64,
    /// (tag index, value), sorted by tag index. Capacity = channel count of the source.
    entries: Vec<(u32, Value)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceRuntime {
    link: LinkState,
    epoch: Option<u64>,
    last_accepted: Option<u64>,
    continuity: Option<Continuity>,
    last_batch: LastBatch,
    /// VS-004: liveness supervision only.
    silent: bool,
    /// VS-004: supervised sources only (0 = none).
    last_batch_op: u64,
    /// VS-004, freshness-supervised sources only: head (oldest marker) and tail of the list
    /// of this source's `Good` tags ordered by `observed`. Invariant: the list holds
    /// exactly the source's `Good` tags, so an expiry visits only the tags it changes and
    /// costs O(1) when nothing is eligible (no scan of the source's channels). Derived
    /// state: a function of the qualities and markers already in the snapshot and digest.
    fresh_head: u32,
    fresh_tail: u32,
}

/// Remove member `t` from its source's freshness list.
fn fresh_unlink(cells: &mut [TagCell], rt: &mut SourceRuntime, t: u32) {
    let Some(c) = cells.get(t as usize) else { return };
    let (prev, next) = (c.prev, c.next);
    match cells.get_mut(prev as usize) {
        Some(p) => p.next = next,
        None => rt.fresh_head = next,
    }
    match cells.get_mut(next as usize) {
        Some(n) => n.prev = prev,
        None => rt.fresh_tail = prev,
    }
}

/// Append `t` (not a member) at the tail: its marker is the largest of the list.
fn fresh_push_back(cells: &mut [TagCell], rt: &mut SourceRuntime, t: u32) {
    let tail = rt.fresh_tail;
    if let Some(c) = cells.get_mut(t as usize) {
        c.prev = tail;
        c.next = NIL;
    }
    match cells.get_mut(tail as usize) {
        Some(p) => p.next = t,
        None => rt.fresh_head = t,
    }
    rt.fresh_tail = t;
}

#[derive(Debug, Clone, Copy)]
struct Staged {
    tag_index: u32,
    channel_id: ChannelId,
    value: Value,
}

/// Snapshot of one tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TagSnapshot {
    pub tag_id: TagId,
    pub value_type: ValueType,
    pub value: Option<Value>,
    pub quality: Quality,
    pub revision: u64,
    /// VS-004: operation sequence of the last applied observation (0 = never, or source
    /// without freshness supervision).
    pub last_observed_operation: u64,
}

/// Snapshot of one source's session and deduplication state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSnapshot {
    pub source_id: SourceId,
    pub link: LinkState,
    pub epoch: Option<u64>,
    pub last_accepted_source_sequence: Option<u64>,
    pub continuity: Option<Continuity>,
    pub last_batch: Option<LastBatchSnapshot>,
    /// VS-004: configured supervision kinds.
    pub supervision: Supervision,
    /// VS-004: the active link is silent.
    pub silent: bool,
    /// VS-004: operation sequence of the last applied batch (0 = none or unsupervised).
    pub last_batch_operation: u64,
}

/// Last accepted batch of a source in canonical form (sorted by `TagId`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastBatchSnapshot {
    pub mode: BatchMode,
    pub source_sequence: u64,
    pub entries: Vec<(TagId, Value)>,
}

/// Owned logical snapshot of the whole state `S` (no process identity, no metrics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSnapshot {
    pub config_fingerprint: [u8; 32],
    pub last_operation_sequence: u64,
    pub sources: Vec<SourceSnapshot>,
    pub tags: Vec<TagSnapshot>,
    /// VS-005: every alarm (sorted by id) and the outbox sequences; `None` without alarms.
    pub alarms: Option<AlarmsSnapshot>,
}

/// VS-005 part of [`LogicalSnapshot`]. Event contents are compared separately with
/// [`Kernel::alarm_events_digest`] (O(capacity), not part of every snapshot).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlarmsSnapshot {
    pub alarms: Vec<AlarmView>,
    pub outbox: OutboxStatus,
}

/// The kernel state machine. Single owner of all RTDB writes.
#[derive(Debug, Clone)]
pub struct Kernel {
    config: Arc<ValidatedConfig>,
    last_operation_sequence: u64,
    sources: Vec<SourceRuntime>,
    cells: Vec<TagCell>,
    /// Scratch space for validation; not part of the logical state.
    staging: Vec<Staged>,
    /// VS-005: alarm states and outbox; `None` without `[alarms]` (one branch per
    /// transition on the default path).
    alarms: Option<Box<AlarmEngine>>,
}

impl Kernel {
    /// Build a kernel with every tag `Unknown`, every source never connected and
    /// `last_operation_sequence = 0`. All storage is allocated here, once.
    pub fn new(config: Arc<ValidatedConfig>) -> Kernel {
        let cells =
            vec![
                TagCell { value: None, quality: Quality::Unknown, revision: 0, observed: 0, prev: NIL, next: NIL };
                config.tags().len()
            ];
        let sources = config
            .sources()
            .iter()
            .map(|s| SourceRuntime {
                link: LinkState::NeverConnected,
                epoch: None,
                last_accepted: None,
                continuity: None,
                last_batch: LastBatch {
                    present: false,
                    mode: BatchMode::FullState,
                    source_sequence: 0,
                    entries: Vec::with_capacity(s.channel_count()),
                },
                silent: false,
                last_batch_op: 0,
                fresh_head: NIL,
                fresh_tail: NIL,
            })
            .collect();
        let staging = Vec::with_capacity(config.limits().max_observations_per_batch as usize);
        let alarms = config.alarm_table().map(|t| Box::new(AlarmEngine::new(t)));
        Kernel { config, last_operation_sequence: 0, sources, cells, staging, alarms }
    }

    /// Conservative upper bound, in bytes, of the heap the kernel state allocates for
    /// `config` (RTDB cells, per-source runtime and deduplication copy, staging buffer).
    /// All of it is allocated by [`Kernel::new`] and never grows (VS-002 E1). Allocator
    /// overhead is not included.
    pub fn storage_bound_bytes(config: &ValidatedConfig) -> u128 {
        let tags = config.tags().len() as u128;
        let max_obs = u128::from(config.limits().max_observations_per_batch);
        let channels: u128 = config.sources().iter().map(|s| s.channel_count() as u128).sum();
        let sources = config.sources().len() as u128;
        let sz = |n: usize| n as u128;
        tags.saturating_mul(sz(std::mem::size_of::<TagCell>()))
            .saturating_add(sources.saturating_mul(sz(std::mem::size_of::<SourceRuntime>())))
            .saturating_add(channels.saturating_mul(sz(std::mem::size_of::<(u32, Value)>())))
            .saturating_add(max_obs.saturating_mul(sz(std::mem::size_of::<Staged>())))
            .saturating_add(Self::alarm_storage_bytes(config))
    }

    /// VS-005: bytes of the alarm states, listed index, outbox ring and preflight scratch
    /// (0 without alarms). Included in [`Self::storage_bound_bytes`].
    pub fn alarm_storage_bytes(config: &ValidatedConfig) -> u128 {
        config.alarm_table().map_or(0, AlarmEngine::storage_bytes)
    }

    pub fn config(&self) -> &Arc<ValidatedConfig> {
        &self.config
    }

    pub fn last_operation_sequence(&self) -> u64 {
        self.last_operation_sequence
    }

    /// Next operation sequence the sequencer must propose, or `None` when exhausted.
    pub fn next_operation_sequence(&self) -> Option<u64> {
        self.last_operation_sequence.checked_add(1)
    }

    pub fn source_status(&self, source_id: SourceId) -> Option<SourceStatus> {
        let i = self.config.source_index(source_id)?;
        let s = self.sources.get(i)?;
        Some(SourceStatus {
            link: s.link,
            epoch: s.epoch,
            last_accepted_source_sequence: s.last_accepted,
            silent: s.silent,
            last_batch_operation: s.last_batch_op,
        })
    }

    /// Apply one sequenced input. Pure: no I/O, no clock, no randomness, no threads.
    ///
    /// Validation is completed before any mutation; on `Err` the state is unchanged.
    pub fn apply(&mut self, input: SequencedInput) -> Result<TransitionOutcome, ApplyError> {
        let expected = self.last_operation_sequence.checked_add(1).ok_or(ApplyError::OperationSequenceExhausted)?;
        if input.operation_sequence != expected {
            return Err(ApplyError::OperationSequenceMismatch { expected, got: input.operation_sequence });
        }
        let outcome = match input.operation {
            Operation::SourceConnected { source_id, link_generation, source_epoch } => {
                self.connect(source_id, link_generation, source_epoch, expected)?
            }
            Operation::SourceDisconnected { source_id, link_generation } => {
                self.disconnect(source_id, link_generation, expected)?
            }
            Operation::ObservationBatch(batch) => self.batch(&batch, expected)?,
            Operation::SourceSilenceExpired { source_id, link_generation } => {
                self.silence(source_id, link_generation, expected)?
            }
            Operation::SourceLivenessRestored { source_id, link_generation } => {
                self.restore(source_id, link_generation, expected)?
            }
            Operation::FreshnessExpired { source_id, link_generation, observed_up_to } => {
                self.freshness(source_id, link_generation, observed_up_to, expected)?
            }
            Operation::AlarmAcknowledge { alarm_id, expected_activation_token, actor_uid } => {
                self.acknowledge(alarm_id, expected_activation_token, actor_uid, expected)?
            }
            Operation::AdvanceDurableWatermark { up_to_event_seq } => {
                self.advance_watermark(up_to_event_seq, expected)?
            }
        };
        if let TransitionOutcome::Applied { .. } = outcome {
            self.last_operation_sequence = expected;
        }
        Ok(outcome)
    }

    fn source_index(&self, source_id: SourceId) -> Result<usize, ApplyError> {
        self.config.source_index(source_id).ok_or(ApplyError::UnknownSource(source_id))
    }

    /// Check that degrading the tags of `src` to `Uncertain(reason)` cannot overflow a
    /// revision. Returns the number of tags that would change.
    fn check_degrade(&self, src: usize, reason: UncertainReason) -> Result<u32, ApplyError> {
        let meta = self.config.sources().get(src).ok_or(ApplyError::UnknownSource(SourceId(0)))?;
        let mut n: u32 = 0;
        for &(_, t) in meta.channels() {
            let Some(cell) = self.cells.get(t as usize) else { continue };
            if degrades(cell, reason) {
                if cell.revision == u64::MAX {
                    let tag_id = self.config.tags().get(t as usize).map_or(TagId(0), |m| m.tag_id);
                    return Err(ApplyError::RevisionOverflow(tag_id));
                }
                n = n.saturating_add(1);
            }
        }
        Ok(n)
    }

    /// Mark every tag of `src` that has a value and a quality of lower precedence as
    /// `Uncertain(reason)`. Must be preceded by a successful [`Self::check_degrade`].
    fn degrade(&mut self, src: usize, reason: UncertainReason) {
        let Some(meta) = self.config.sources().get(src) else { return };
        for &(_, t) in meta.channels() {
            if let Some(cell) = self.cells.get_mut(t as usize)
                && degrades(cell, reason)
            {
                cell.quality = Quality::Uncertain(reason);
                cell.revision = cell.revision.saturating_add(1);
            }
        }
        // Every degradation reason outranks `Good` and a `Good` tag always has a value:
        // the source has no `Good` tag left, so its freshness list is empty.
        if meta.supervision.freshness
            && let Some(rt) = self.sources.get_mut(src)
        {
            rt.fresh_head = NIL;
            rt.fresh_tail = NIL;
        }
    }

    /// VS-005 preflight of a degradation of `src` to `Uncertain(reason)`: stage every rule
    /// of the tags that would change and check the outbox. Mutates only the scratch.
    fn alarm_preflight_degrade(&mut self, src: usize, reason: UncertainReason, op: u64) -> Result<(), ApplyError> {
        let (Some(eng), Some(table)) = (self.alarms.as_deref_mut(), self.config.alarm_table()) else { return Ok(()) };
        let Some(meta) = self.config.sources().get(src) else { return Ok(()) };
        eng.begin();
        for &(_, t) in meta.channels() {
            if let Some(cell) = self.cells.get(t as usize)
                && degrades(cell, reason)
            {
                eng.stage_tag(table, t, cell.value, Quality::Uncertain(reason), op)?;
            }
        }
        eng.check()
    }

    /// VS-005: commit the checked preflight (infallible). No-op without alarms.
    #[inline]
    fn alarm_commit(&mut self, op: u64) {
        if let (Some(eng), Some(table)) = (self.alarms.as_deref_mut(), self.config.alarm_table()) {
            eng.commit(table, op);
        }
    }

    fn connect(
        &mut self,
        source_id: SourceId,
        generation: u64,
        epoch: u64,
        op: u64,
    ) -> Result<TransitionOutcome, ApplyError> {
        let src = self.source_index(source_id)?;
        if epoch == 0 {
            return Err(ApplyError::InvalidEpoch);
        }
        let rt = self.sources.get(src).ok_or(ApplyError::UnknownSource(source_id))?;
        if let Some(current) = rt.link.generation()
            && generation <= current
        {
            return Err(ApplyError::NonMonotonicLinkGeneration { current, got: generation });
        }
        let degraded = self.check_degrade(src, UncertainReason::SourceUnavailable)?;
        self.alarm_preflight_degrade(src, UncertainReason::SourceUnavailable, op)?;
        // Validation complete: mutate.
        self.degrade(src, UncertainReason::SourceUnavailable);
        self.alarm_commit(op);
        let Some(rt) = self.sources.get_mut(src) else {
            return Err(ApplyError::UnknownSource(source_id));
        };
        if rt.epoch != Some(epoch) {
            rt.epoch = Some(epoch);
            rt.last_accepted = None;
            rt.continuity = None;
            rt.last_batch.present = false;
            rt.last_batch.entries.clear();
        }
        rt.link = LinkState::Connected { generation };
        // VS-004: liveness is a property of the current link; a new link starts alive.
        rt.silent = false;
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::Connected {
                full_state_required: rt.last_accepted.is_none(),
                last_accepted_source_sequence: rt.last_accepted,
                degraded_tags: degraded,
            },
        })
    }

    fn disconnect(&mut self, source_id: SourceId, generation: u64, op: u64) -> Result<TransitionOutcome, ApplyError> {
        let src = self.source_index(source_id)?;
        let rt = self.sources.get(src).ok_or(ApplyError::UnknownSource(source_id))?;
        match rt.link {
            LinkState::Connected { generation: g } if g == generation => {}
            LinkState::Connected { generation: g } | LinkState::Disconnected { generation: g } if generation <= g => {
                return Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration));
            }
            _ => return Err(ApplyError::UnknownLinkGeneration { got: generation }),
        }
        let degraded = self.check_degrade(src, UncertainReason::SourceUnavailable)?;
        self.alarm_preflight_degrade(src, UncertainReason::SourceUnavailable, op)?;
        self.degrade(src, UncertainReason::SourceUnavailable);
        self.alarm_commit(op);
        if let Some(rt) = self.sources.get_mut(src) {
            rt.link = LinkState::Disconnected { generation };
            rt.silent = false;
        }
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::Disconnected { degraded_tags: degraded },
        })
    }

    fn batch(&mut self, b: &ObservationBatch, op: u64) -> Result<TransitionOutcome, ApplyError> {
        let src = self.source_index(b.source_id)?;
        // C4: removing this `Arc::clone` (split borrows) was measured: -8..-18 % on `apply`
        // but not repeatable (p > 0.05 in 4 of 7 cases, ADR 0007); kept as in VS-001.
        let config = Arc::clone(&self.config);
        let meta = config.sources().get(src).ok_or(ApplyError::UnknownSource(b.source_id))?;
        let rt = self.sources.get(src).ok_or(ApplyError::UnknownSource(b.source_id))?;

        // 1. Link.
        if rt.link != (LinkState::Connected { generation: b.link_generation }) {
            return Err(ApplyError::LinkNotActive { got: b.link_generation });
        }

        // 2. Size. Checked before touching the observations so the staging buffer never grows.
        let max = config.limits().max_observations_per_batch;
        if b.observations.is_empty() {
            return Err(ApplyError::EmptyBatch);
        }
        if b.observations.len() > max as usize {
            return Err(ApplyError::BatchTooLarge { len: b.observations.len(), max });
        }

        // 3. Channels and types, staged and canonicalized by tag index.
        self.staging.clear();
        for o in &b.observations {
            let tag_index = meta.tag_index(o.channel_id).ok_or(ApplyError::UnknownChannel(o.channel_id))?;
            let expected =
                config.tags().get(tag_index).map(|t| t.value_type).ok_or(ApplyError::UnknownChannel(o.channel_id))?;
            if o.value.value_type() != expected {
                return Err(ApplyError::TypeMismatch { channel_id: o.channel_id, expected, got: o.value.value_type() });
            }
            // tag_index comes from a u32 in SourceMeta.
            let tag_index = u32::try_from(tag_index).map_err(|_| ApplyError::UnknownChannel(o.channel_id))?;
            self.staging.push(Staged { tag_index, channel_id: o.channel_id, value: o.value });
        }
        // C4: an `is_sorted` pre-check was measured and dropped (within noise in all cases):
        // the unstable sort already handles sorted or reversed input in linear time.
        self.staging.sort_unstable_by_key(|s| s.tag_index);
        for w in self.staging.windows(2) {
            if let [a, c] = w
                && a.tag_index == c.tag_index
            {
                // Channel -> tag bindings are injective, so equal tags mean equal channels.
                return Err(ApplyError::DuplicateChannel(c.channel_id));
            }
        }

        // 4. Completeness of a full state (no duplicates and all known => count suffices).
        if b.mode == BatchMode::FullState && self.staging.len() != meta.channel_count() {
            return Err(ApplyError::IncompleteFullState { expected: meta.channel_count(), got: self.staging.len() });
        }

        // 5. Sequence rules.
        let seq = b.source_sequence;
        if seq == 0 {
            return Err(ApplyError::InvalidSequence);
        }
        let continuity = match rt.last_accepted {
            None => match b.mode {
                BatchMode::FullState => Continuity::BaselineEstablished,
                BatchMode::Delta => return Err(ApplyError::ResyncRequired),
            },
            Some(last) => {
                if seq == last {
                    let lb = &rt.last_batch;
                    let same = lb.present
                        && lb.mode == b.mode
                        && lb.source_sequence == seq
                        && lb.entries.len() == self.staging.len()
                        && lb
                            .entries
                            .iter()
                            .zip(self.staging.iter())
                            .all(|(&(t, v), s)| t == s.tag_index && v == s.value);
                    return if same {
                        Ok(TransitionOutcome::Duplicate { source_sequence: seq })
                    } else {
                        Err(ApplyError::SequenceConflict { source_sequence: seq })
                    };
                }
                // The epoch's counter is exhausted: no successor exists. Explicit, never wraps;
                // the producer must start a new epoch.
                let next = last.checked_add(1).ok_or(ApplyError::SequenceOverflow)?;
                if seq < last {
                    return Err(ApplyError::StaleSequence { last_accepted: last, got: seq });
                }
                if seq == next {
                    Continuity::Continuous
                } else {
                    match b.mode {
                        BatchMode::FullState => Continuity::Resynchronized,
                        BatchMode::Delta => {
                            return Err(ApplyError::SequenceGap { expected: next, got: seq });
                        }
                    }
                }
            }
        };

        // 6. Revision overflow and 7. VS-005 alarm preflight, fused in ONE pass over the
        // staged tags (vs005.md §2.3: the second pass cost ~1 µs per 128-tag batch). Alarm
        // rules are staged for the tags whose value or quality changes, from the predicted
        // post-batch cell (value, Good); an identical refresh changes nothing and is
        // skipped. Precedence is unchanged: any revision overflow wins over an alarm
        // staging error (the first one is kept and returned after the scan), and the
        // outbox admission (`check`) comes last. Refused here = nothing applied, sequence
        // unused.
        let mut alarm = match (self.alarms.as_deref_mut(), config.alarm_table()) {
            (Some(eng), Some(table)) => {
                eng.begin();
                Some((eng, table))
            }
            _ => None,
        };
        let mut alarm_err: Option<ApplyError> = None;
        for s in &self.staging {
            if let Some(cell) = self.cells.get(s.tag_index as usize)
                && (cell.value != Some(s.value) || cell.quality != Quality::Good)
            {
                if cell.revision == u64::MAX {
                    let tag_id = config.tags().get(s.tag_index as usize).map_or(TagId(0), |m| m.tag_id);
                    return Err(ApplyError::RevisionOverflow(tag_id));
                }
                if let Some((eng, table)) = alarm.as_mut()
                    && alarm_err.is_none()
                {
                    // Fast path: the tag's quiet window says no rule changes (exact; no
                    // CSR, rule or state access). Otherwise stage its rules.
                    if eng.is_quiet(s.tag_index, s.value) {
                        #[cfg(debug_assertions)]
                        eng.assert_quiet_exact(table, s.tag_index, s.value, op);
                    } else if let Err(e) = eng.stage_tag(table, s.tag_index, Some(s.value), Quality::Good, op) {
                        alarm_err = Some(e);
                    }
                }
            }
        }
        if let Some(e) = alarm_err {
            return Err(e);
        }
        if let Some((eng, _)) = alarm {
            eng.check()?;
        }

        // Validation complete: mutate, in TagId order.
        let mut changed: u32 = 0;
        if meta.supervision.freshness {
            // VS-004: every observed tag gets the new marker, also when its value and
            // quality are unchanged (same revision): this is what refreshes it. It moves
            // to the tail of the source's freshness list (largest marker), O(1) per tag.
            let (cells, staging) = (&mut self.cells, &self.staging);
            let Some(rt) = self.sources.get_mut(src) else { return Err(ApplyError::UnknownSource(b.source_id)) };
            for s in staging {
                let t = s.tag_index;
                let Some(cell) = cells.get_mut(t as usize) else { continue };
                let member = cell.quality == Quality::Good;
                if cell.value != Some(s.value) || cell.quality != Quality::Good {
                    cell.value = Some(s.value);
                    cell.quality = Quality::Good;
                    cell.revision = cell.revision.saturating_add(1);
                    changed = changed.saturating_add(1);
                }
                cell.observed = op;
                if member {
                    fresh_unlink(cells, rt, t);
                }
                fresh_push_back(cells, rt, t);
            }
        } else {
            // VS-003 path, unchanged.
            for s in &self.staging {
                if let Some(cell) = self.cells.get_mut(s.tag_index as usize)
                    && (cell.value != Some(s.value) || cell.quality != Quality::Good)
                {
                    cell.value = Some(s.value);
                    cell.quality = Quality::Good;
                    cell.revision = cell.revision.saturating_add(1);
                    changed = changed.saturating_add(1);
                }
            }
        }
        let supervised = meta.supervision.any();
        if let Some(rt) = self.sources.get_mut(src) {
            if supervised {
                // An applied batch is liveness evidence of the active link.
                rt.silent = false;
                rt.last_batch_op = op;
            }
            rt.last_accepted = Some(seq);
            rt.continuity = Some(continuity);
            rt.last_batch.present = true;
            rt.last_batch.mode = b.mode;
            rt.last_batch.source_sequence = seq;
            rt.last_batch.entries.clear();
            // len <= channel count == preallocated capacity: never reallocates.
            rt.last_batch.entries.extend(self.staging.iter().map(|s| (s.tag_index, s.value)));
        }
        self.alarm_commit(op);
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::Batch { continuity, changed_tags: changed },
        })
    }

    /// Validation shared by the VS-004 supervision operations: the source exists, enables
    /// the kind of supervision, and `generation` is its active link. `Ok(None)`: a stale
    /// generation (no-op, as for a late disconnection).
    fn supervised_link(
        &self,
        source_id: SourceId,
        generation: u64,
        enabled: impl Fn(Supervision) -> bool,
    ) -> Result<Option<usize>, ApplyError> {
        let src = self.source_index(source_id)?;
        let meta = self.config.sources().get(src).ok_or(ApplyError::UnknownSource(source_id))?;
        if !enabled(meta.supervision) {
            return Err(ApplyError::SupervisionNotConfigured(source_id));
        }
        let rt = self.sources.get(src).ok_or(ApplyError::UnknownSource(source_id))?;
        match rt.link {
            LinkState::Connected { generation: g } if g == generation => Ok(Some(src)),
            LinkState::Connected { generation: g } | LinkState::Disconnected { generation: g } if generation <= g => {
                Ok(None)
            }
            _ => Err(ApplyError::UnknownLinkGeneration { got: generation }),
        }
    }

    fn silence(&mut self, source_id: SourceId, generation: u64, op: u64) -> Result<TransitionOutcome, ApplyError> {
        let Some(src) = self.supervised_link(source_id, generation, |s| s.liveness)? else {
            return Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration));
        };
        if self.sources.get(src).is_some_and(|r| r.silent) {
            return Ok(TransitionOutcome::Ignored(IgnoredReason::AlreadySilent));
        }
        let degraded = self.check_degrade(src, UncertainReason::SourceSilent)?;
        self.alarm_preflight_degrade(src, UncertainReason::SourceSilent, op)?;
        self.degrade(src, UncertainReason::SourceSilent);
        self.alarm_commit(op);
        if let Some(rt) = self.sources.get_mut(src) {
            rt.silent = true;
        }
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::SilenceExpired { degraded_tags: degraded },
        })
    }

    fn restore(&mut self, source_id: SourceId, generation: u64, op: u64) -> Result<TransitionOutcome, ApplyError> {
        let Some(src) = self.supervised_link(source_id, generation, |s| s.liveness)? else {
            return Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration));
        };
        let Some(rt) = self.sources.get_mut(src) else {
            return Err(ApplyError::UnknownSource(source_id));
        };
        if !rt.silent {
            return Ok(TransitionOutcome::Ignored(IgnoredReason::NotSilent));
        }
        // Communication state only: tags keep their quality until a new observation.
        rt.silent = false;
        Ok(TransitionOutcome::Applied { operation_sequence: op, kind: AppliedKind::LivenessRestored })
    }

    fn freshness(
        &mut self,
        source_id: SourceId,
        generation: u64,
        up_to: u64,
        op: u64,
    ) -> Result<TransitionOutcome, ApplyError> {
        let Some(src) = self.supervised_link(source_id, generation, |s| s.freshness)? else {
            return Ok(TransitionOutcome::Ignored(IgnoredReason::StaleLinkGeneration));
        };
        // The token designates an applied operation: 1..=last (op = last + 1).
        if up_to == 0 || up_to >= op {
            return Err(ApplyError::InvalidFreshnessToken { observed_up_to: up_to });
        }
        // The eligible tags are exactly a prefix of the freshness list (ordered by
        // marker): visit only them, never the whole source.
        let rt = self.sources.get_mut(src).ok_or(ApplyError::UnknownSource(source_id))?;
        let mut n: u32 = 0;
        let mut t = rt.fresh_head;
        while let Some(cell) = self.cells.get(t as usize) {
            if !expires(cell, up_to) {
                break;
            }
            if cell.revision == u64::MAX {
                let tag_id = self.config.tags().get(t as usize).map_or(TagId(0), |m| m.tag_id);
                return Err(ApplyError::RevisionOverflow(tag_id));
            }
            n = n.saturating_add(1);
            t = cell.next;
        }
        if n == 0 {
            return Ok(TransitionOutcome::Ignored(IgnoredReason::NothingExpired));
        }
        // VS-005 preflight over the same prefix (value kept, quality StaleData).
        if let (Some(eng), Some(table)) = (self.alarms.as_deref_mut(), self.config.alarm_table()) {
            eng.begin();
            let mut t = rt.fresh_head;
            while let Some(cell) = self.cells.get(t as usize) {
                if !expires(cell, up_to) {
                    break;
                }
                eng.stage_tag(table, t, cell.value, Quality::Uncertain(UncertainReason::StaleData), op)?;
                t = cell.next;
            }
            eng.check()?;
        }
        // Pop the prefix: those tags leave `Good`, hence the list.
        let mut t = rt.fresh_head;
        while t != NIL {
            let Some(cell) = self.cells.get_mut(t as usize) else { break };
            if !expires(cell, up_to) {
                break;
            }
            cell.quality = Quality::Uncertain(UncertainReason::StaleData);
            cell.revision = cell.revision.saturating_add(1);
            t = cell.next;
        }
        rt.fresh_head = t;
        match self.cells.get_mut(t as usize) {
            Some(c) => c.prev = NIL,
            None => rt.fresh_tail = NIL,
        }
        self.alarm_commit(op);
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::FreshnessExpired { expired_tags: n },
        })
    }

    /// VS-005: operator acknowledgement (ADR 0012 §3).
    fn acknowledge(
        &mut self,
        alarm_id: AlarmId,
        token: u64,
        actor_uid: u32,
        op: u64,
    ) -> Result<TransitionOutcome, ApplyError> {
        let (Some(eng), Some(table)) = (self.alarms.as_deref_mut(), self.config.alarm_table()) else {
            return Err(ApplyError::AlarmsNotConfigured);
        };
        eng.begin();
        if let Some(reason) = eng.stage_ack(table, alarm_id, token, actor_uid)? {
            return Ok(TransitionOutcome::Ignored(reason));
        }
        eng.check()?;
        let event_seq = eng.status().next_event_seq;
        eng.commit(table, op);
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::AlarmAcknowledged { alarm_id, event_seq },
        })
    }

    /// VS-005: durable watermark (test harness only in VS-005).
    fn advance_watermark(&mut self, up_to: u64, op: u64) -> Result<TransitionOutcome, ApplyError> {
        let Some(eng) = self.alarms.as_deref_mut() else { return Err(ApplyError::AlarmsNotConfigured) };
        match eng.advance_watermark(up_to)? {
            None => Ok(TransitionOutcome::Ignored(IgnoredReason::WatermarkUnchanged)),
            Some(released) => Ok(TransitionOutcome::Applied {
                operation_sequence: op,
                kind: AppliedKind::DurableWatermarkAdvanced { up_to_event_seq: up_to, released },
            }),
        }
    }

    /// VS-005: whether alarms are configured.
    pub fn alarms_enabled(&self) -> bool {
        self.alarms.is_some()
    }

    /// VS-005: outbox counters and sequences (`None` without alarms). O(1).
    pub fn outbox_status(&self) -> Option<OutboxStatus> {
        self.alarms.as_deref().map(AlarmEngine::status)
    }

    /// VS-005: state of one alarm. O(log rules).
    pub fn alarm(&self, alarm_id: AlarmId) -> Option<AlarmView> {
        let table = self.config.alarm_table()?;
        self.alarms.as_deref()?.view(table, table.rule_index(alarm_id)?)
    }

    /// VS-005: one page of the alarms that are `active || unacknowledged`, with id greater
    /// than `after` (`AlarmId(0)` = from the start), ascending. Fills `out` up to
    /// `min(limit, out.capacity())` without allocating. O(page + skipped empty blocks).
    pub fn alarm_snapshot_page(&self, after: AlarmId, limit: usize, out: &mut Vec<AlarmView>) -> Option<SnapshotPage> {
        let table = self.config.alarm_table()?;
        Some(self.alarms.as_deref()?.snapshot(table, after, limit, out))
    }

    /// VS-005: events after `after_event_seq`, in order, up to `min(limit, out.capacity())`.
    /// Reading never frees a slot and never moves the durable watermark. O(page).
    pub fn read_alarm_events(
        &self,
        after_event_seq: u64,
        limit: usize,
        out: &mut Vec<AlarmEvent>,
    ) -> Option<EventPage> {
        Some(self.alarms.as_deref()?.read(after_event_seq, limit, out))
    }

    /// VS-005: one event, if still in the ring.
    pub fn alarm_event(&self, event_seq: u64) -> Option<AlarmEvent> {
        self.alarms.as_deref()?.event(event_seq)
    }

    /// VS-005: BLAKE3 of every event still readable, for replay comparisons in tests.
    /// O(capacity): never on the hot path.
    pub fn alarm_events_digest(&self) -> Option<[u8; 32]> {
        self.alarms.as_deref().map(AlarmEngine::events_digest)
    }

    /// VS-004, pure query for the runtime watchdog: how many tags a
    /// `FreshnessExpired { observed_up_to }` for `source_id` would change now (0 if the
    /// source is unknown or has no freshness supervision). Lets the runtime avoid emitting
    /// operations that would be no-ops.
    pub fn freshness_candidates(&self, source_id: SourceId, observed_up_to: u64) -> u32 {
        let Some(src) = self.config.source_index(source_id) else { return 0 };
        if !self.config.sources().get(src).is_some_and(|m| m.supervision.freshness) {
            return 0;
        }
        // O(eligible tags): O(1) when nothing would expire, however many tags are stale.
        let mut n: u32 = 0;
        let mut t = self.sources.get(src).map_or(NIL, |rt| rt.fresh_head);
        while let Some(cell) = self.cells.get(t as usize) {
            if !expires(cell, observed_up_to) {
                break;
            }
            n = n.saturating_add(1);
            t = cell.next;
        }
        n
    }

    /// Version of the encoding [`Self::state_digest`] uses: 1 (VS-003 encoding) unless at
    /// least one source is supervised, then 2.
    pub fn state_digest_version(&self) -> u16 {
        if self.alarms.is_some() {
            STATE_DIGEST_VERSION_ALARMS
        } else if self.config.any_supervision() {
            STATE_DIGEST_VERSION_SUPERVISED
        } else {
            STATE_DIGEST_VERSION
        }
    }

    /// Owned logical snapshot, ordered by `SourceId` and `TagId`. Not for the hot path.
    pub fn logical_snapshot(&self) -> LogicalSnapshot {
        let tags = self.config.tags();
        let tag_id = |i: u32| tags.get(i as usize).map_or(TagId(0), |m| m.tag_id);
        LogicalSnapshot {
            config_fingerprint: self.config.fingerprint(),
            last_operation_sequence: self.last_operation_sequence,
            sources: self
                .config
                .sources()
                .iter()
                .zip(&self.sources)
                .map(|(m, r)| SourceSnapshot {
                    source_id: m.source_id,
                    link: r.link,
                    epoch: r.epoch,
                    last_accepted_source_sequence: r.last_accepted,
                    continuity: r.continuity,
                    last_batch: r.last_batch.present.then(|| LastBatchSnapshot {
                        mode: r.last_batch.mode,
                        source_sequence: r.last_batch.source_sequence,
                        entries: r.last_batch.entries.iter().map(|&(t, v)| (tag_id(t), v)).collect(),
                    }),
                    supervision: m.supervision,
                    silent: r.silent,
                    last_batch_operation: r.last_batch_op,
                })
                .collect(),
            tags: tags
                .iter()
                .zip(&self.cells)
                .map(|(m, c)| TagSnapshot {
                    tag_id: m.tag_id,
                    value_type: m.value_type,
                    value: c.value,
                    quality: c.quality,
                    revision: c.revision,
                    last_observed_operation: c.observed,
                })
                .collect(),
            alarms: match (self.alarms.as_deref(), self.config.alarm_table()) {
                (Some(eng), Some(table)) => Some(AlarmsSnapshot {
                    alarms: (0..eng.rules()).filter_map(|r| eng.view(table, r)).collect(),
                    outbox: eng.status(),
                }),
                _ => None,
            },
        }
    }

    /// Value, quality and revision of one tag.
    pub fn tag(&self, tag_id: TagId) -> Option<TagSnapshot> {
        let i = self.config.tag_index(tag_id)?;
        let m = self.config.tags().get(i)?;
        let c = self.cells.get(i)?;
        Some(TagSnapshot {
            tag_id,
            value_type: m.value_type,
            value: c.value,
            quality: c.quality,
            revision: c.revision,
            last_observed_operation: c.observed,
        })
    }

    /// BLAKE3 over the canonical encoding of the logical state (see ADR 0002): v1 for an
    /// unsupervised configuration (byte-identical to VS-003), v2 when at least one source
    /// is supervised (ADR 0011 §7: v1 fields plus the supervision markers, appended in the
    /// same canonical order). Computed on demand; allocation free.
    pub fn state_digest(&self) -> [u8; 32] {
        // v3 (alarms) carries every v2 field, then the alarm section.
        let v2 = self.config.any_supervision() || self.alarms.is_some();
        let mut h = CanonicalHasher::new(b"scada.kernel.state", self.state_digest_version());
        h.raw(&self.config.fingerprint());
        h.u64(self.last_operation_sequence);
        let tags = self.config.tags();
        h.len(self.sources.len());
        for (m, r) in self.config.sources().iter().zip(&self.sources) {
            h.u32(m.source_id.0);
            match r.link {
                LinkState::NeverConnected => {
                    h.u8(0);
                    h.u64(0);
                }
                LinkState::Connected { generation } => {
                    h.u8(1);
                    h.u64(generation);
                }
                LinkState::Disconnected { generation } => {
                    h.u8(2);
                    h.u64(generation);
                }
            }
            h.opt_u64(r.epoch);
            h.opt_u64(r.last_accepted);
            h.u8(match r.continuity {
                None => 0,
                Some(Continuity::Continuous) => 1,
                Some(Continuity::BaselineEstablished) => 2,
                Some(Continuity::Resynchronized) => 3,
            });
            if r.last_batch.present {
                h.u8(1);
                h.u8(mode_code(r.last_batch.mode));
                h.u64(r.last_batch.source_sequence);
                h.len(r.last_batch.entries.len());
                for &(t, v) in &r.last_batch.entries {
                    h.u32(tags.get(t as usize).map_or(0, |m| m.tag_id.0));
                    hash_value(&mut h, Some(v));
                }
            } else {
                h.u8(0);
            }
            if v2 {
                h.u8(u8::from(r.silent));
                h.u64(r.last_batch_op);
            }
        }
        h.len(self.cells.len());
        for (m, c) in tags.iter().zip(&self.cells) {
            h.u32(m.tag_id.0);
            h.u8(m.value_type.code());
            hash_value(&mut h, c.value);
            match c.quality {
                Quality::Unknown => {
                    h.u8(0);
                    h.u8(0);
                }
                Quality::Good => {
                    h.u8(1);
                    h.u8(0);
                }
                // SourceUnavailable keeps the v1 code (2, 1); the VS-004 reasons can only
                // occur in a supervised configuration, i.e. with encoding v2.
                Quality::Uncertain(r) => {
                    h.u8(2);
                    h.u8(r.code());
                }
            }
            h.u64(c.revision);
            if v2 {
                h.u64(c.observed);
            }
        }
        if let Some(eng) = self.alarms.as_deref() {
            eng.hash_into(&mut h);
        }
        h.finish()
    }
}

/// Whether degrading `cell` to `Uncertain(reason)` changes it: it has a value and its
/// quality has lower precedence (`Good`, or an uncertain reason of lower rank).
fn degrades(cell: &TagCell, reason: UncertainReason) -> bool {
    cell.value.is_some()
        && match cell.quality {
            Quality::Good => true,
            Quality::Uncertain(r) => reason.rank() > r.rank(),
            Quality::Unknown => false,
        }
}

/// Whether a freshness token `up_to` expires `cell`: `Good` and last observed at or
/// before the token (never-observed tags, marker 0, are `Unknown` and never expire).
fn expires(cell: &TagCell, up_to: u64) -> bool {
    cell.quality == Quality::Good && cell.observed != 0 && cell.observed <= up_to
}

fn mode_code(m: BatchMode) -> u8 {
    match m {
        BatchMode::FullState => 1,
        BatchMode::Delta => 2,
    }
}

fn hash_value(h: &mut CanonicalHasher, v: Option<Value>) {
    match v {
        None => h.u8(0),
        Some(Value::Bool(b)) => {
            h.u8(1);
            h.u8(u8::from(b));
        }
        Some(Value::I64(x)) => {
            h.u8(2);
            h.i64(x);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ChannelSpec, ConfigSpec, SourceSpec, TagSpec};

    fn kernel() -> Kernel {
        let spec = ConfigSpec {
            limits: Default::default(),
            tags: vec![TagSpec { tag_id: TagId(1), name: "a".into(), value_type: ValueType::I64 }],
            sources: vec![SourceSpec {
                source_id: SourceId(7),
                name: "s".into(),
                channels: vec![ChannelSpec { channel_id: ChannelId(1), tag_id: TagId(1), value_type: ValueType::I64 }],
                supervision: Supervision::NONE,
            }],
        };
        Kernel::new(Arc::new(ValidatedConfig::validate(spec).unwrap()))
    }

    fn input(seq: u64, op: Operation) -> SequencedInput {
        SequencedInput { operation_sequence: seq, operation: op }
    }

    fn batch(mode: BatchMode, sseq: u64, v: i64) -> Operation {
        batch_gen(1, mode, sseq, v)
    }

    fn batch_gen(link_generation: u64, mode: BatchMode, sseq: u64, v: i64) -> Operation {
        Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(7),
            link_generation,
            mode,
            source_sequence: sseq,
            observations: vec![Observation { channel_id: ChannelId(1), value: Value::I64(v) }],
        })
    }

    #[test]
    fn revision_overflow_is_rejected_without_mutation() {
        let mut k = kernel();
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        k.apply(input(2, batch(BatchMode::FullState, 1, 10))).unwrap();
        k.cells[0].revision = u64::MAX;
        let d = k.state_digest();
        assert_eq!(k.apply(input(3, batch(BatchMode::Delta, 2, 11))), Err(ApplyError::RevisionOverflow(TagId(1))));
        assert_eq!(d, k.state_digest());
        // Same value and quality: no revision change needed, so it is applied.
        assert!(matches!(k.apply(input(3, batch(BatchMode::Delta, 2, 10))), Ok(TransitionOutcome::Applied { .. })));
        // Disconnect would need a revision bump: rejected too.
        let d = k.state_digest();
        assert_eq!(
            k.apply(input(4, Operation::SourceDisconnected { source_id: SourceId(7), link_generation: 1 })),
            Err(ApplyError::RevisionOverflow(TagId(1)))
        );
        assert_eq!(d, k.state_digest());
    }

    #[test]
    fn source_sequence_overflow_is_explicit() {
        let mut k = kernel();
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        k.apply(input(2, batch(BatchMode::FullState, u64::MAX, 10))).unwrap();
        let d = k.state_digest();
        assert_eq!(
            k.apply(input(3, batch(BatchMode::Delta, u64::MAX, 11))),
            Err(ApplyError::SequenceConflict { source_sequence: u64::MAX })
        );
        assert_eq!(
            k.apply(input(3, batch(BatchMode::FullState, u64::MAX, 10))),
            Ok(TransitionOutcome::Duplicate { source_sequence: u64::MAX })
        );
        // last == u64::MAX: there is no valid next sequence; the counter is exhausted.
        assert_eq!(k.apply(input(3, batch(BatchMode::Delta, 5, 11))), Err(ApplyError::SequenceOverflow));
        assert_eq!(k.apply(input(3, batch(BatchMode::FullState, 1, 11))), Err(ApplyError::SequenceOverflow));
        // A new epoch resets the counter.
        assert_eq!(d, k.state_digest());
        k.apply(input(3, Operation::SourceConnected { source_id: SourceId(7), link_generation: 2, source_epoch: 10 }))
            .unwrap();
        assert!(matches!(
            k.apply(input(4, batch_gen(2, BatchMode::FullState, 1, 12))),
            Ok(TransitionOutcome::Applied { .. })
        ));
    }

    #[test]
    fn operation_sequence_exhaustion_is_explicit() {
        let mut k = kernel();
        k.last_operation_sequence = u64::MAX;
        assert_eq!(
            k.apply(input(
                0,
                Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }
            )),
            Err(ApplyError::OperationSequenceExhausted)
        );
        assert_eq!(k.next_operation_sequence(), None);
    }

    fn supervised_kernel() -> Kernel {
        let spec = ConfigSpec {
            limits: Default::default(),
            tags: vec![
                TagSpec { tag_id: TagId(1), name: "a".into(), value_type: ValueType::I64 },
                TagSpec { tag_id: TagId(2), name: "b".into(), value_type: ValueType::I64 },
            ],
            sources: vec![SourceSpec {
                source_id: SourceId(7),
                name: "s".into(),
                channels: vec![
                    ChannelSpec { channel_id: ChannelId(1), tag_id: TagId(1), value_type: ValueType::I64 },
                    ChannelSpec { channel_id: ChannelId(2), tag_id: TagId(2), value_type: ValueType::I64 },
                ],
                supervision: Supervision { liveness: true, freshness: true },
            }],
        };
        Kernel::new(Arc::new(ValidatedConfig::validate(spec).unwrap()))
    }

    fn full2(sseq: u64, a: i64, b: i64) -> Operation {
        Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(7),
            link_generation: 1,
            mode: BatchMode::FullState,
            source_sequence: sseq,
            observations: vec![
                Observation { channel_id: ChannelId(1), value: Value::I64(a) },
                Observation { channel_id: ChannelId(2), value: Value::I64(b) },
            ],
        })
    }

    /// VS-004: a silence or freshness expiry that would overflow one revision is refused
    /// as a whole (atomic: no tag of the source changes, no operation sequence, no flag).
    #[test]
    fn supervision_revision_overflow_is_atomic() {
        let mut k = supervised_kernel();
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        k.apply(input(2, full2(1, 10, 20))).unwrap();
        k.cells[1].revision = u64::MAX;
        let (snap, d) = (k.logical_snapshot(), k.state_digest());
        let silence = Operation::SourceSilenceExpired { source_id: SourceId(7), link_generation: 1 };
        assert_eq!(k.apply(input(3, silence.clone())), Err(ApplyError::RevisionOverflow(TagId(2))));
        let expire = Operation::FreshnessExpired { source_id: SourceId(7), link_generation: 1, observed_up_to: 2 };
        assert_eq!(k.apply(input(3, expire.clone())), Err(ApplyError::RevisionOverflow(TagId(2))));
        assert_eq!((snap, d), (k.logical_snapshot(), k.state_digest()));
        assert!(!k.sources[0].silent);
        // Only tag 1 needs a revision: fine once tag 2 is not a candidate any more.
        k.cells[1].revision = 5;
        assert!(matches!(k.apply(input(3, expire)), Ok(TransitionOutcome::Applied { .. })));
    }

    #[test]
    fn supervised_apply_never_grows_internal_storage() {
        let mut k = supervised_kernel();
        let caps = (k.staging.capacity(), k.sources[0].last_batch.entries.capacity(), k.cells.capacity());
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        let mut op = 2;
        for i in 0..5_000u64 {
            k.apply(input(op, full2(1 + i, 1, (i % 3) as i64))).unwrap();
            op += 1;
            for o in [
                Operation::SourceSilenceExpired { source_id: SourceId(7), link_generation: 1 },
                Operation::SourceLivenessRestored { source_id: SourceId(7), link_generation: 1 },
                Operation::FreshnessExpired { source_id: SourceId(7), link_generation: 1, observed_up_to: op - 1 },
            ] {
                if let Ok(TransitionOutcome::Applied { .. }) = k.apply(input(op, o)) {
                    op += 1;
                }
            }
        }
        assert_eq!(caps, (k.staging.capacity(), k.sources[0].last_batch.entries.capacity(), k.cells.capacity()));
    }

    fn alarm_kernel() -> Kernel {
        use crate::alarm::{AlarmCondition, AlarmRuleSpec, AlarmSpec, Severity};
        let spec = ConfigSpec {
            limits: Default::default(),
            tags: vec![TagSpec { tag_id: TagId(1), name: "a".into(), value_type: ValueType::I64 }],
            sources: vec![SourceSpec {
                source_id: SourceId(7),
                name: "s".into(),
                channels: vec![ChannelSpec { channel_id: ChannelId(1), tag_id: TagId(1), value_type: ValueType::I64 }],
                supervision: Supervision::NONE,
            }],
        };
        let rules = vec![
            AlarmRuleSpec {
                alarm_id: AlarmId(1),
                name: "hi".into(),
                tag_id: TagId(1),
                condition: AlarmCondition::High { trip: 10, reset: 5 },
                severity: Severity::Major,
            },
            AlarmRuleSpec {
                alarm_id: AlarmId(2),
                name: "bq".into(),
                tag_id: TagId(1),
                condition: AlarmCondition::BadQuality,
                severity: Severity::Warning,
            },
        ];
        let a = AlarmSpec { max_rules: 2, outbox_capacity_events: 4, rules };
        Kernel::new(Arc::new(ValidatedConfig::validate_with_alarms(spec, Some(a)).unwrap()))
    }

    /// VS-005: the event sequence never wraps; exhaustion is a typed refusal without effect.
    #[test]
    fn event_sequence_exhaustion_is_atomic() {
        let mut k = alarm_kernel();
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        k.apply(input(2, batch(BatchMode::FullState, 1, 0))).unwrap();
        // One event still fits (u64::MAX - 1); two do not.
        k.alarms.as_deref_mut().unwrap().set_next_event_seq_for_test(u64::MAX - 1);
        let (snap, d) = (k.logical_snapshot(), k.state_digest());
        assert_eq!(
            k.apply(input(3, batch(BatchMode::Delta, 2, 11))),
            Ok(TransitionOutcome::Applied {
                operation_sequence: 3,
                kind: AppliedKind::Batch { continuity: Continuity::Continuous, changed_tags: 1 }
            })
        );
        let (snap2, d2) = (k.logical_snapshot(), k.state_digest());
        assert_ne!((snap, d), (snap2.clone(), d2));
        k.alarms.as_deref_mut().unwrap().set_next_event_seq_for_test(u64::MAX);
        let (snap, d) = (k.logical_snapshot(), k.state_digest());
        assert_eq!(k.apply(input(4, batch(BatchMode::Delta, 3, 0))), Err(ApplyError::EventSequenceExhausted));
        assert_eq!((snap, d), (k.logical_snapshot(), k.state_digest()));
        // A transition without events is still possible.
        assert!(k.apply(input(4, batch(BatchMode::Delta, 3, 12))).is_ok());
    }

    #[test]
    fn alarm_apply_never_grows_internal_storage() {
        let mut k = alarm_kernel();
        let cap = |k: &Kernel| {
            let a = k.alarms.as_deref().unwrap();
            (k.staging.capacity(), format!("{:?}", a.status().capacity))
        };
        let c0 = cap(&k);
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        k.apply(input(2, batch(BatchMode::FullState, 1, 0))).unwrap();
        let mut op = 3;
        for i in 0..2_000u64 {
            let r = k.apply(input(op, batch(BatchMode::Delta, 2 + i, if i % 2 == 0 { 20 } else { 0 })));
            match r {
                Ok(_) => op += 1,
                Err(ApplyError::OutboxCapacityExceeded { .. }) => {
                    let last = k.outbox_status().unwrap().next_event_seq - 1;
                    k.apply(input(op, Operation::AdvanceDurableWatermark { up_to_event_seq: last })).unwrap();
                    op += 1;
                    k.apply(input(op, batch(BatchMode::Delta, 2 + i, if i % 2 == 0 { 20 } else { 0 }))).unwrap();
                    op += 1;
                }
                Err(e) => panic!("{e:?}"),
            }
        }
        assert_eq!(c0, cap(&k));
    }

    #[test]
    fn apply_never_grows_internal_storage() {
        let mut k = kernel();
        let caps = (k.staging.capacity(), k.sources[0].last_batch.entries.capacity(), k.cells.capacity());
        k.apply(input(1, Operation::SourceConnected { source_id: SourceId(7), link_generation: 1, source_epoch: 9 }))
            .unwrap();
        k.apply(input(2, batch(BatchMode::FullState, 1, 10))).unwrap();
        for i in 0..10_000u64 {
            k.apply(input(3 + i, batch(BatchMode::Delta, 2 + i, i as i64))).unwrap();
        }
        assert_eq!(caps, (k.staging.capacity(), k.sources[0].last_batch.entries.capacity(), k.cells.capacity()));
    }
}
