//! The deterministic transition function and the in-memory RTDB.

use std::fmt;
use std::sync::Arc;

use crate::STATE_DIGEST_VERSION;
use crate::config::ValidatedConfig;
use crate::digest::CanonicalHasher;
use crate::types::{BatchMode, ChannelId, Continuity, Quality, SourceId, TagId, UncertainReason, Value, ValueType};

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
    SourceConnected { source_id: SourceId, link_generation: u64, source_epoch: u64 },
    SourceDisconnected { source_id: SourceId, link_generation: u64 },
    ObservationBatch(ObservationBatch),
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
}

/// Why an input was a no-op that is not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IgnoredReason {
    /// Disconnection of a link generation that is no longer the active one.
    StaleLinkGeneration,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TagCell {
    value: Option<Value>,
    quality: Quality,
    revision: u64,
}

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
}

impl Kernel {
    /// Build a kernel with every tag `Unknown`, every source never connected and
    /// `last_operation_sequence = 0`. All storage is allocated here, once.
    pub fn new(config: Arc<ValidatedConfig>) -> Kernel {
        let cells = vec![TagCell { value: None, quality: Quality::Unknown, revision: 0 }; config.tags().len()];
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
            })
            .collect();
        let staging = Vec::with_capacity(config.limits().max_observations_per_batch as usize);
        Kernel { config, last_operation_sequence: 0, sources, cells, staging }
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
        Some(SourceStatus { link: s.link, epoch: s.epoch, last_accepted_source_sequence: s.last_accepted })
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
        };
        if let TransitionOutcome::Applied { .. } = outcome {
            self.last_operation_sequence = expected;
        }
        Ok(outcome)
    }

    fn source_index(&self, source_id: SourceId) -> Result<usize, ApplyError> {
        self.config.source_index(source_id).ok_or(ApplyError::UnknownSource(source_id))
    }

    /// Check that degrading the tags of `src` to `Uncertain` cannot overflow a revision.
    /// Returns the number of tags that would change.
    fn check_degrade(&self, src: usize) -> Result<u32, ApplyError> {
        let meta = self.config.sources().get(src).ok_or(ApplyError::UnknownSource(SourceId(0)))?;
        let mut n: u32 = 0;
        for &(_, t) in meta.channels() {
            let Some(cell) = self.cells.get(t as usize) else { continue };
            if cell.value.is_some() && cell.quality != UNAVAILABLE {
                if cell.revision == u64::MAX {
                    let tag_id = self.config.tags().get(t as usize).map_or(TagId(0), |m| m.tag_id);
                    return Err(ApplyError::RevisionOverflow(tag_id));
                }
                n = n.saturating_add(1);
            }
        }
        Ok(n)
    }

    /// Mark every tag of `src` that has a value as `Uncertain(SourceUnavailable)`.
    /// Must be preceded by a successful [`Self::check_degrade`].
    fn degrade(&mut self, src: usize) {
        let Some(meta) = self.config.sources().get(src) else { return };
        for &(_, t) in meta.channels() {
            if let Some(cell) = self.cells.get_mut(t as usize)
                && cell.value.is_some()
                && cell.quality != UNAVAILABLE
            {
                cell.quality = UNAVAILABLE;
                cell.revision = cell.revision.saturating_add(1);
            }
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
        let degraded = self.check_degrade(src)?;
        // Validation complete: mutate.
        self.degrade(src);
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
        let degraded = self.check_degrade(src)?;
        self.degrade(src);
        if let Some(rt) = self.sources.get_mut(src) {
            rt.link = LinkState::Disconnected { generation };
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

        // 6. Revision overflow.
        for s in &self.staging {
            if let Some(cell) = self.cells.get(s.tag_index as usize)
                && (cell.value != Some(s.value) || cell.quality != Quality::Good)
                && cell.revision == u64::MAX
            {
                let tag_id = config.tags().get(s.tag_index as usize).map_or(TagId(0), |m| m.tag_id);
                return Err(ApplyError::RevisionOverflow(tag_id));
            }
        }

        // Validation complete: mutate, in TagId order.
        let mut changed: u32 = 0;
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
        if let Some(rt) = self.sources.get_mut(src) {
            rt.last_accepted = Some(seq);
            rt.continuity = Some(continuity);
            rt.last_batch.present = true;
            rt.last_batch.mode = b.mode;
            rt.last_batch.source_sequence = seq;
            rt.last_batch.entries.clear();
            // len <= channel count == preallocated capacity: never reallocates.
            rt.last_batch.entries.extend(self.staging.iter().map(|s| (s.tag_index, s.value)));
        }
        Ok(TransitionOutcome::Applied {
            operation_sequence: op,
            kind: AppliedKind::Batch { continuity, changed_tags: changed },
        })
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
                })
                .collect(),
        }
    }

    /// Value, quality and revision of one tag.
    pub fn tag(&self, tag_id: TagId) -> Option<TagSnapshot> {
        let i = self.config.tag_index(tag_id)?;
        let m = self.config.tags().get(i)?;
        let c = self.cells.get(i)?;
        Some(TagSnapshot { tag_id, value_type: m.value_type, value: c.value, quality: c.quality, revision: c.revision })
    }

    /// BLAKE3 over the canonical encoding v1 of the logical state (see ADR 0002).
    /// Computed on demand; allocation free.
    pub fn state_digest(&self) -> [u8; 32] {
        let mut h = CanonicalHasher::new(b"scada.kernel.state", STATE_DIGEST_VERSION);
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
                Quality::Uncertain(UncertainReason::SourceUnavailable) => {
                    h.u8(2);
                    h.u8(1);
                }
            }
            h.u64(c.revision);
        }
        h.finish()
    }
}

const UNAVAILABLE: Quality = Quality::Uncertain(UncertainReason::SourceUnavailable);

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
