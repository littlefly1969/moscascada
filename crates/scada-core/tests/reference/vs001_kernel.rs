//! Frozen copy of the VS-001 `Kernel::apply` (commit b17413f, crates/scada-core/src/kernel.rs),
//! used as the executable specification for VS-002 equivalence tests and as the
//! baseline of the core microbenchmarks. Only the digest was removed (it needs the
//! crate-private canonical hasher) and the channel lookup is the VS-001 binary search,
//! inlined so that later changes to `SourceMeta` cannot leak into the reference.
#![allow(dead_code)]

use std::sync::Arc;

use scada_core::{
    AppliedKind, ApplyError, BatchMode, ChannelId, Continuity, IgnoredReason, LastBatchSnapshot, LinkState,
    LogicalSnapshot, ObservationBatch, Operation, Quality, SequencedInput, SourceId, SourceMeta, SourceSnapshot,
    SourceStatus, TagId, TagSnapshot, TransitionOutcome, UncertainReason, ValidatedConfig, Value,
};

fn link_generation(l: LinkState) -> Option<u64> {
    match l {
        LinkState::NeverConnected => None,
        LinkState::Connected { generation } | LinkState::Disconnected { generation } => Some(generation),
    }
}

fn ref_tag_index(meta: &SourceMeta, channel_id: ChannelId) -> Option<usize> {
    let ch = meta.channels();
    ch.binary_search_by_key(&channel_id, |&(c, _)| c).ok().and_then(|i| ch.get(i)).map(|&(_, t)| t as usize)
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

/// The kernel state machine. Single owner of all RTDB writes.
#[derive(Debug, Clone)]
pub struct RefKernel {
    config: Arc<ValidatedConfig>,
    last_operation_sequence: u64,
    sources: Vec<SourceRuntime>,
    cells: Vec<TagCell>,
    /// Scratch space for validation; not part of the logical state.
    staging: Vec<Staged>,
}

impl RefKernel {
    /// Build a kernel with every tag `Unknown`, every source never connected and
    /// `last_operation_sequence = 0`. All storage is allocated here, once.
    pub fn new(config: Arc<ValidatedConfig>) -> RefKernel {
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
        RefKernel { config, last_operation_sequence: 0, sources, cells, staging }
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
            silent: false,
            last_batch_operation: 0,
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
            // VS-004 operations do not exist in VS-001; for an unsupervised configuration
            // (the only kind VS-001 knows) the current kernel refuses them without effect.
            Operation::SourceSilenceExpired { source_id, .. }
            | Operation::SourceLivenessRestored { source_id, .. }
            | Operation::FreshnessExpired { source_id, .. } => {
                self.source_index(source_id)?;
                return Err(ApplyError::SupervisionNotConfigured(source_id));
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
        if let Some(current) = link_generation(rt.link)
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
            let tag_index = ref_tag_index(meta, o.channel_id).ok_or(ApplyError::UnknownChannel(o.channel_id))?;
            let expected =
                config.tags().get(tag_index).map(|t| t.value_type).ok_or(ApplyError::UnknownChannel(o.channel_id))?;
            if o.value.value_type() != expected {
                return Err(ApplyError::TypeMismatch { channel_id: o.channel_id, expected, got: o.value.value_type() });
            }
            // tag_index comes from a u32 in SourceMeta.
            let tag_index = u32::try_from(tag_index).map_err(|_| ApplyError::UnknownChannel(o.channel_id))?;
            self.staging.push(Staged { tag_index, channel_id: o.channel_id, value: o.value });
        }
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
                    // VS-004 fields (not part of VS-001): neutral for unsupervised configs.
                    supervision: m.supervision,
                    silent: false,
                    last_batch_operation: 0,
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
                    last_observed_operation: 0, // VS-004 field, neutral
                })
                .collect(),
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
            last_observed_operation: 0,
        })
    }
}

const UNAVAILABLE: Quality = Quality::Uncertain(UncertainReason::SourceUnavailable);
