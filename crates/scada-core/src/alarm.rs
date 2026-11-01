//! VS-005 (ADR 0012): deterministic alarm engine and bounded, non-overwriting event outbox.
//!
//! Everything here is part of the single writer's transition: rules are evaluated inside
//! [`crate::Kernel::apply`] from the *predicted* post-transition value and quality of the
//! touched tags, into a preallocated staging area (**preflight**), the number of events is
//! checked against the free outbox slots, and only then RTDB, alarm states and events are
//! committed together (infallible commit). An operation that would need more slots than
//! are free is refused with [`crate::ApplyError::OutboxCapacityExceeded`] before anything
//! changes.
//!
//! Layout (all allocated once by [`crate::Kernel::new`], never grown by `apply`):
//!
//! * rule table sorted by [`AlarmId`] (rule index = rank of the id), compact
//!   [`CompiledRule`] records, names kept apart (cold);
//! * CSR index `tag index -> rule indices` (`offsets`, `by_tag`): a transition visits only
//!   the rules of the tags whose value or quality it changes;
//! * one [`AlarmState`] per rule (32 bytes, no strings, no pointers);
//! * a two-level bitset of the rules that are `active || unacknowledged`;
//! * the outbox ring of `outbox_capacity` [`AlarmEvent`] slots (48 bytes each). The slot
//!   of event `N` is reused only when `durable_watermark >= N`; reading never frees slots.
//!
//! Without an `[alarms]` configuration none of this exists (`Kernel::alarms == None`).

use std::fmt;

use crate::config::{ConfigError, SourceMeta, TagMeta};
use crate::kernel::{ApplyError, IgnoredReason};
use crate::types::{AlarmId, Quality, SourceId, TagId, Value, ValueType};

/// Hard ceiling of `max_rules` (rule indices are `u32`; the ceiling keeps the startup
/// tables of a mistyped configuration bounded). Not a measured supported limit.
pub const MAX_ALARM_RULES_CEILING: u32 = 1 << 22;
/// Hard ceiling of `outbox_capacity_events` (2^28 slots = 12 GiB of records): a
/// representation guardrail, not a recommendation. The memory budget is the real limit.
pub const MAX_OUTBOX_CAPACITY_CEILING: u64 = 1 << 28;
/// Maximum length of a rule name, in bytes (same alphabet as tag names).
pub const MAX_ALARM_NAME_BYTES: usize = 64;

/// Static severity of a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    Info,
    Warning,
    Major,
    Critical,
}

impl Severity {
    /// Canonical code (fingerprint, wire).
    pub fn code(self) -> u8 {
        match self {
            Severity::Info => 1,
            Severity::Warning => 2,
            Severity::Major => 3,
            Severity::Critical => 4,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Major => "major",
            Severity::Critical => "critical",
        }
    }
}

/// Condition of a rule. Integer thresholds only (scaled integers, as the tags).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlarmCondition {
    /// `bool` tag: active while the value equals `expected` (quality `Good`).
    BoolEquals { expected: bool },
    /// `i64` tag: becomes active at `value >= trip`, returns at `value <= reset`
    /// (`reset < trip`); in between the previous active/inactive state is kept.
    High { trip: i64, reset: i64 },
    /// `i64` tag: becomes active at `value <= trip`, returns at `value >= reset`
    /// (`reset > trip`).
    Low { trip: i64, reset: i64 },
    /// Any tag: armed by the first `Good` quality of the kernel instance; once armed,
    /// active while the quality is not `Good`.
    BadQuality,
}

impl AlarmCondition {
    fn kind_code(self) -> u8 {
        match self {
            AlarmCondition::BoolEquals { .. } => K_BOOL,
            AlarmCondition::High { .. } => K_HIGH,
            AlarmCondition::Low { .. } => K_LOW,
            AlarmCondition::BadQuality => K_BADQ,
        }
    }
}

/// One rule as written in the configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlarmRuleSpec {
    pub alarm_id: AlarmId,
    pub name: String,
    pub tag_id: TagId,
    pub condition: AlarmCondition,
    pub severity: Severity,
}

/// `[alarms]` as the host parsed it. Absent = alarms disabled (VS-004 behaviour).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlarmSpec {
    /// Configured ceiling of the number of rules (validated against the memory budget by
    /// the host); `rules.len()` must not exceed it.
    pub max_rules: u32,
    /// Outbox capacity in **events** (slots), not bytes.
    pub outbox_capacity_events: u64,
    pub rules: Vec<AlarmRuleSpec>,
}

const K_BOOL: u8 = 1;
const K_HIGH: u8 = 2;
const K_LOW: u8 = 3;
const K_BADQ: u8 = 4;

/// Hot rule record (24 bytes): what the evaluation reads, nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompiledRule {
    /// `High`/`Low`: trip threshold; `BoolEquals`: expected value as 0/1.
    trip: i64,
    reset: i64,
    tag: u32,
    kind: u8,
    severity: Severity,
}

impl CompiledRule {
    fn condition(&self) -> AlarmCondition {
        match self.kind {
            K_BOOL => AlarmCondition::BoolEquals { expected: self.trip != 0 },
            K_HIGH => AlarmCondition::High { trip: self.trip, reset: self.reset },
            K_LOW => AlarmCondition::Low { trip: self.trip, reset: self.reset },
            _ => AlarmCondition::BadQuality,
        }
    }
}

/// Validated, immutable rule set (part of [`crate::ValidatedConfig`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlarmTable {
    ids: Vec<AlarmId>,
    names: Vec<String>,
    tag_ids: Vec<TagId>,
    rules: Vec<CompiledRule>,
    /// CSR: rules of tag index `t` are `by_tag[offsets[t]..offsets[t + 1]]`, ascending.
    offsets: Vec<u32>,
    by_tag: Vec<u32>,
    max_rules: u32,
    outbox_capacity: u64,
    /// Largest number of rules one operation can touch (one source's tags), >= 1.
    worst_fanout: u32,
    worst_source: Option<SourceId>,
}

/// Public, cold view of one rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlarmRuleMeta {
    pub alarm_id: AlarmId,
    pub name: String,
    pub tag_id: TagId,
    pub condition: AlarmCondition,
    pub severity: Severity,
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_ALARM_NAME_BYTES
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
}

impl AlarmTable {
    /// Validate `spec` against the (already validated) tags and sources.
    pub(crate) fn build(spec: AlarmSpec, tags: &[TagMeta], sources: &[SourceMeta]) -> Result<AlarmTable, ConfigError> {
        if spec.max_rules == 0 || spec.max_rules > MAX_ALARM_RULES_CEILING {
            return Err(ConfigError::InvalidLimit { name: "alarms.max_rules", value: u64::from(spec.max_rules) });
        }
        if spec.rules.is_empty() {
            return Err(ConfigError::NoAlarmRules);
        }
        if spec.rules.len() > spec.max_rules as usize {
            return Err(ConfigError::TooManyAlarmRules { count: spec.rules.len(), max: spec.max_rules });
        }
        if spec.outbox_capacity_events == 0 || spec.outbox_capacity_events > MAX_OUTBOX_CAPACITY_CEILING {
            return Err(ConfigError::InvalidLimit {
                name: "alarms.outbox_capacity_events",
                value: spec.outbox_capacity_events,
            });
        }
        let mut rules = spec.rules;
        rules.sort_by_key(|r| r.alarm_id);
        for r in &rules {
            if r.alarm_id.0 == 0 {
                return Err(ConfigError::ZeroAlarmId);
            }
            if !valid_name(&r.name) {
                return Err(ConfigError::InvalidName { name: r.name.clone() });
            }
        }
        for w in rules.windows(2) {
            if let [a, b] = w
                && a.alarm_id == b.alarm_id
            {
                return Err(ConfigError::DuplicateAlarmId(a.alarm_id));
            }
        }
        let mut names: Vec<&str> = rules.iter().map(|r| r.name.as_str()).collect();
        names.sort_unstable();
        for w in names.windows(2) {
            if let [a, b] = w
                && a == b
            {
                return Err(ConfigError::DuplicateAlarmName((*a).to_owned()));
            }
        }
        let mut compiled = Vec::with_capacity(rules.len());
        let mut counts = vec![0u32; tags.len()];
        for r in &rules {
            let Ok(t) = tags.binary_search_by_key(&r.tag_id, |m| m.tag_id) else {
                return Err(ConfigError::AlarmUnknownTag { alarm_id: r.alarm_id, tag_id: r.tag_id });
            };
            let tag_type = tags.get(t).map(|m| m.value_type);
            let need = match r.condition {
                AlarmCondition::BoolEquals { .. } => Some(ValueType::Bool),
                AlarmCondition::High { .. } | AlarmCondition::Low { .. } => Some(ValueType::I64),
                AlarmCondition::BadQuality => None,
            };
            if let (Some(need), Some(have)) = (need, tag_type)
                && need != have
            {
                return Err(ConfigError::AlarmTypeMismatch {
                    alarm_id: r.alarm_id,
                    tag_id: r.tag_id,
                    rule: need,
                    tag: have,
                });
            }
            let (trip, reset) = match r.condition {
                AlarmCondition::BoolEquals { expected } => (i64::from(expected), 0),
                AlarmCondition::High { trip, reset } => {
                    if reset >= trip {
                        return Err(ConfigError::AlarmInvalidThresholds { alarm_id: r.alarm_id, trip, reset });
                    }
                    (trip, reset)
                }
                AlarmCondition::Low { trip, reset } => {
                    if reset <= trip {
                        return Err(ConfigError::AlarmInvalidThresholds { alarm_id: r.alarm_id, trip, reset });
                    }
                    (trip, reset)
                }
                AlarmCondition::BadQuality => (0, 0),
            };
            let t32 = u32::try_from(t)
                .map_err(|_| ConfigError::AlarmUnknownTag { alarm_id: r.alarm_id, tag_id: r.tag_id })?;
            if let Some(c) = counts.get_mut(t) {
                *c = c.saturating_add(1);
            }
            compiled.push(CompiledRule { trip, reset, tag: t32, kind: r.condition.kind_code(), severity: r.severity });
        }
        // CSR, rules of each tag in ascending rule index (= ascending AlarmId).
        let mut offsets = Vec::with_capacity(tags.len().saturating_add(1));
        let mut acc: u32 = 0;
        offsets.push(0);
        for c in &counts {
            acc = acc.saturating_add(*c);
            offsets.push(acc);
        }
        let mut fill: Vec<u32> = offsets.iter().copied().take(tags.len()).collect();
        let mut by_tag = vec![0u32; rules.len()];
        for (i, r) in compiled.iter().enumerate() {
            if let Some(pos) = fill.get_mut(r.tag as usize) {
                if let Some(slot) = by_tag.get_mut(*pos as usize) {
                    *slot = u32::try_from(i).unwrap_or(u32::MAX);
                }
                *pos = pos.saturating_add(1);
            }
        }
        // Worst-case number of rules one operation touches: every operation that changes
        // tags changes tags of one source only (batch, connect, disconnect, silence,
        // freshness); an acknowledgement touches one rule.
        let rules_of = |t: u32| -> u32 {
            let lo = offsets.get(t as usize).copied().unwrap_or(0);
            let hi = offsets.get((t as usize).saturating_add(1)).copied().unwrap_or(lo);
            hi.saturating_sub(lo)
        };
        let mut worst_fanout: u32 = 1;
        let mut worst_source = None;
        for s in sources {
            let n = s.channels().iter().fold(0u32, |a, &(_, t)| a.saturating_add(rules_of(t)));
            if n > worst_fanout || (n == worst_fanout && worst_source.is_none() && n > 0) {
                worst_fanout = n;
                worst_source = Some(s.source_id);
            }
        }
        if spec.outbox_capacity_events < u64::from(worst_fanout) {
            return Err(ConfigError::OutboxTooSmallForFanout {
                capacity: spec.outbox_capacity_events,
                worst_fanout,
                source_id: worst_source.unwrap_or(SourceId(0)),
            });
        }
        Ok(AlarmTable {
            ids: rules.iter().map(|r| r.alarm_id).collect(),
            tag_ids: rules.iter().map(|r| r.tag_id).collect(),
            names: rules.into_iter().map(|r| r.name).collect(),
            rules: compiled,
            offsets,
            by_tag,
            max_rules: spec.max_rules,
            outbox_capacity: spec.outbox_capacity_events,
            worst_fanout,
            worst_source,
        })
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn max_rules(&self) -> u32 {
        self.max_rules
    }

    pub fn outbox_capacity(&self) -> u64 {
        self.outbox_capacity
    }

    /// Largest number of rules (hence of events) one operation can produce, and the
    /// source that reaches it.
    pub fn worst_fanout(&self) -> (u32, Option<SourceId>) {
        (self.worst_fanout, self.worst_source)
    }

    /// Rule index of `alarm_id` (binary search: ACKs only, never the data path).
    pub fn rule_index(&self, alarm_id: AlarmId) -> Option<usize> {
        self.ids.binary_search(&alarm_id).ok()
    }

    /// Rules sorted by id (cold metadata).
    pub fn rule(&self, index: usize) -> Option<AlarmRuleMeta> {
        let r = self.rules.get(index)?;
        Some(AlarmRuleMeta {
            alarm_id: *self.ids.get(index)?,
            name: self.names.get(index)?.clone(),
            tag_id: *self.tag_ids.get(index)?,
            condition: r.condition(),
            severity: r.severity,
        })
    }

    pub fn ids(&self) -> &[AlarmId] {
        &self.ids
    }

    /// Number of rules bound to tag index `t`.
    pub fn rules_of_tag(&self, t: usize) -> usize {
        self.tag_rules(t).len()
    }

    #[inline]
    fn tag_rules(&self, t: usize) -> &[u32] {
        let lo = self.offsets.get(t).copied().unwrap_or(0) as usize;
        let hi = self.offsets.get(t.saturating_add(1)).copied().unwrap_or(0) as usize;
        self.by_tag.get(lo..hi).unwrap_or(&[])
    }

    /// Heap bytes of the table (fingerprinted configuration, cold names included).
    pub fn heap_bytes(&self) -> u128 {
        let n = |len: usize, sz: usize| (len as u128).saturating_mul(sz as u128);
        n(self.ids.capacity(), size_of::<AlarmId>())
            .saturating_add(n(self.tag_ids.capacity(), size_of::<TagId>()))
            .saturating_add(n(self.names.capacity(), size_of::<String>()))
            .saturating_add(self.names.iter().map(|s| s.capacity() as u128).sum::<u128>())
            .saturating_add(n(self.rules.capacity(), size_of::<CompiledRule>()))
            .saturating_add(n(self.offsets.capacity(), 4))
            .saturating_add(n(self.by_tag.capacity(), 4))
    }

    /// Feed the canonical encoding of the rule set (fingerprint v3).
    pub(crate) fn hash_into(&self, h: &mut crate::digest::CanonicalHasher) {
        h.len(self.rules.len());
        for i in 0..self.rules.len() {
            let (Some(id), Some(name), Some(tag), Some(r)) =
                (self.ids.get(i), self.names.get(i), self.tag_ids.get(i), self.rules.get(i))
            else {
                continue;
            };
            h.u32(id.0);
            h.bytes(name.as_bytes());
            h.u32(tag.0);
            h.u8(r.kind);
            h.i64(r.trip);
            h.i64(r.reset);
            h.u8(r.severity.code());
        }
        h.u32(self.max_rules);
    }
}

// ---------------------------------------------------------------------------------------
// Dynamic state.

const F_ACTIVE: u8 = 1;
const F_UNACKED: u8 = 2;
const F_INPUT_VALID: u8 = 4;
const F_ARMED: u8 = 8;

/// Status bits of an alarm (also `status_after` of an event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AlarmFlags(pub u8);

/// The four states of the episode automaton (ADR 0012 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EpisodeState {
    Normal,
    ActiveUnacked,
    ActiveAcked,
    ReturnedUnacked,
}

impl AlarmFlags {
    pub fn active(self) -> bool {
        self.0 & F_ACTIVE != 0
    }
    pub fn acknowledged(self) -> bool {
        self.0 & F_UNACKED == 0
    }
    /// The input is a `Good` measurement (orthogonal to the episode state).
    pub fn input_valid(self) -> bool {
        self.0 & F_INPUT_VALID != 0
    }
    /// `BadQuality` only: the tag was `Good` at least once in this instance.
    pub fn armed(self) -> bool {
        self.0 & F_ARMED != 0
    }
    pub fn episode(self) -> EpisodeState {
        match (self.active(), self.acknowledged()) {
            (false, true) => EpisodeState::Normal,
            (true, false) => EpisodeState::ActiveUnacked,
            (true, true) => EpisodeState::ActiveAcked,
            (false, false) => EpisodeState::ReturnedUnacked,
        }
    }
    /// `active || unacknowledged`: listed by the alarm snapshot.
    pub fn listed(self) -> bool {
        self.0 & (F_ACTIVE | F_UNACKED) != 0
    }
}

/// Per-rule dynamic state (32 bytes). No strings, no pointers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct AlarmState {
    /// Operation sequence of the latest Raised/Retriggered of the current episode (0 =
    /// never activated). An ACK must quote it.
    token: u64,
    /// Episode number of this alarm (0 = never activated); +1 on each Raised.
    occurrence: u64,
    /// Event sequence of the latest event of this alarm (0 = none): its revision.
    last_event_seq: u64,
    flags: u8,
}

/// Kind of an alarm event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlarmEventKind {
    Raised,
    Returned,
    Retriggered,
    Acknowledged,
}

impl AlarmEventKind {
    pub fn code(self) -> u8 {
        match self {
            AlarmEventKind::Raised => 1,
            AlarmEventKind::Returned => 2,
            AlarmEventKind::Retriggered => 3,
            AlarmEventKind::Acknowledged => 4,
        }
    }
}

/// Fixed-size outbox record (48 bytes, `Copy`, no heap). `kernel_instance_id` is not
/// repeated here: it belongs to the reader's envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AlarmEvent {
    /// Total order of events in the kernel instance, from 1, never wraps.
    pub event_seq: u64,
    /// Operation that produced it (all events of one operation are contiguous).
    pub operation_seq: u64,
    pub occurrence_id: u64,
    pub activation_token: u64,
    pub alarm_id: AlarmId,
    /// Position in the operation's events (ordered by `alarm_id`).
    pub ordinal_in_operation: u32,
    /// Authenticated peer uid of an `Acknowledged` (process identity, not necessarily a
    /// person); 0 otherwise.
    pub actor_uid: u32,
    pub kind: AlarmEventKind,
    pub status_after: AlarmFlags,
}

const EMPTY_EVENT: AlarmEvent = AlarmEvent {
    event_seq: 0,
    operation_seq: 0,
    occurrence_id: 0,
    activation_token: 0,
    alarm_id: AlarmId(0),
    ordinal_in_operation: 0,
    actor_uid: 0,
    kind: AlarmEventKind::Raised,
    status_after: AlarmFlags(0),
};

/// Public view of one alarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlarmView {
    pub alarm_id: AlarmId,
    pub severity: Severity,
    pub flags: AlarmFlags,
    pub occurrence_id: u64,
    pub activation_token: u64,
    pub last_event_seq: u64,
}

/// Outbox counters and sequences (logical state except `high_water`, a metric).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxStatus {
    pub capacity: u64,
    /// Events committed and not yet covered by the durable watermark.
    pub used: u64,
    pub free: u64,
    /// Highest `used` ever observed after a commit.
    pub high_water: u64,
    pub next_event_seq: u64,
    /// Last event of the continuous prefix confirmed durable (0 = none). VS-005 has no
    /// durable sink: only a test harness can move it.
    pub durable_watermark: u64,
    /// Oldest event still readable (0 = none).
    pub oldest_visible: u64,
    /// Rules currently `active || unacknowledged`.
    pub active_or_unacked: u64,
    /// Operation sequence of the last operation that changed any alarm state (0 = none).
    pub last_alarm_change_operation: u64,
}

/// Page of events returned by [`crate::Kernel::read_alarm_events`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventPage {
    /// Cursor to pass as `after_event_seq` for the next page.
    pub next_cursor: u64,
    pub oldest_visible: u64,
    pub newest: u64,
    pub has_more: bool,
    /// Events after the requested cursor are no longer in the ring (reclaimed after they
    /// became durable): the page starts at `oldest_visible`.
    pub gap_detected: bool,
    /// The cursor is beyond the newest event: it was not issued by this instance (or
    /// this instance restarted); restart from 0 after checking `kernel_instance_id`.
    pub cursor_ahead: bool,
    pub durable_watermark: u64,
}

/// Page of the alarm snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPage {
    /// Last id returned (cursor for the next page), or the request's cursor.
    pub next_after: AlarmId,
    pub has_more: bool,
    pub total_listed: u64,
    /// Consistency token: a paginated read is consistent only while it stays equal.
    pub as_of_change_operation: u64,
}

/// Two-level bitset over rule indices (`active || unacknowledged`): O(1) update, the
/// listing skips empty 4096-rule blocks with one word test.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ListedIndex {
    l0: Vec<u64>,
    l1: Vec<u64>,
    count: u64,
}

impl ListedIndex {
    fn new(n: usize) -> ListedIndex {
        let w0 = n.div_ceil(64);
        ListedIndex { l0: vec![0; w0], l1: vec![0; w0.div_ceil(64)], count: 0 }
    }

    fn set(&mut self, i: u32, on: bool) {
        let (w, b) = ((i / 64) as usize, i % 64);
        let Some(word) = self.l0.get_mut(w) else { return };
        let was = *word & (1u64 << b) != 0;
        if was == on {
            return;
        }
        if on {
            *word |= 1u64 << b;
            self.count = self.count.saturating_add(1);
        } else {
            *word &= !(1u64 << b);
            self.count = self.count.saturating_sub(1);
        }
        let nonempty = *word != 0;
        if let Some(sw) = self.l1.get_mut(w / 64) {
            if nonempty {
                *sw |= 1u64 << (w % 64);
            } else {
                *sw &= !(1u64 << (w % 64));
            }
        }
    }

    /// First set index `>= from`.
    fn next(&self, from: usize) -> Option<usize> {
        let mut w = from / 64;
        // Partial first word.
        if let Some(&word) = self.l0.get(w) {
            let masked = word & (u64::MAX << (from % 64));
            if masked != 0 {
                return Some(w.saturating_mul(64).saturating_add(masked.trailing_zeros() as usize));
            }
        }
        w = w.saturating_add(1);
        loop {
            let s = w / 64;
            let summary = self.l1.get(s)?;
            let masked = summary & (u64::MAX.checked_shl((w % 64) as u32).unwrap_or(0));
            if masked == 0 {
                w = s.saturating_add(1).saturating_mul(64);
                continue;
            }
            let ww = s.saturating_mul(64).saturating_add(masked.trailing_zeros() as usize);
            let word = self.l0.get(ww)?;
            if *word != 0 {
                return Some(ww.saturating_mul(64).saturating_add(word.trailing_zeros() as usize));
            }
            w = ww.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Staged {
    rule: u32,
    state: AlarmState,
    event: Option<AlarmEventKind>,
    actor_uid: u32,
}

/// Sequencer-owned dynamic alarm state of a kernel.
#[derive(Debug)]
pub(crate) struct AlarmEngine {
    states: Vec<AlarmState>,
    listed: ListedIndex,
    ring: Vec<AlarmEvent>,
    capacity: u64,
    next_event_seq: u64,
    watermark: u64,
    high_water: u64,
    last_change_op: u64,
    /// Preflight scratch: capacity = worst fanout, never grows.
    staging: Vec<Staged>,
    staged_events: u64,
}

/// Result of evaluating one rule against a predicted input.
#[inline]
fn evaluate(
    rule: &CompiledRule,
    st: AlarmState,
    value: Option<Value>,
    quality: Quality,
    op: u64,
) -> Result<Option<(AlarmState, Option<AlarmEventKind>)>, ()> {
    let good = quality == Quality::Good;
    let mut s = st;
    if good {
        s.flags |= F_INPUT_VALID;
    } else {
        s.flags &= !F_INPUT_VALID;
    }
    let active = st.flags & F_ACTIVE != 0;
    // `None`: the condition is indeterminate (process rule on a non-Good input): the
    // latch is kept, never a return to normal (I5).
    let cond: Option<bool> = if rule.kind == K_BADQ {
        if good {
            s.flags |= F_ARMED;
            Some(false)
        } else {
            Some(st.flags & F_ARMED != 0)
        }
    } else if !good {
        None
    } else {
        match (rule.kind, value) {
            (K_BOOL, Some(Value::Bool(b))) => Some(b == (rule.trip != 0)),
            (K_HIGH, Some(Value::I64(v))) => Some(if active { v > rule.reset } else { v >= rule.trip }),
            (K_LOW, Some(Value::I64(v))) => Some(if active { v < rule.reset } else { v <= rule.trip }),
            _ => None,
        }
    };
    let mut event = None;
    match cond {
        Some(true) if !active => {
            if st.flags & F_UNACKED != 0 {
                // ReturnedUnacked + trip: same occurrence reopened, old token stale.
                event = Some(AlarmEventKind::Retriggered);
            } else {
                s.occurrence = st.occurrence.checked_add(1).ok_or(())?;
                s.flags |= F_UNACKED;
                event = Some(AlarmEventKind::Raised);
            }
            s.flags |= F_ACTIVE;
            s.token = op;
        }
        Some(false) if active => {
            s.flags &= !F_ACTIVE;
            event = Some(AlarmEventKind::Returned);
        }
        _ => {}
    }
    Ok(if s == st { None } else { Some((s, event)) })
}

impl Clone for AlarmEngine {
    /// Keeps the preflight scratch capacity (a derived `Clone` would not, and the clone
    /// would then refuse every transition with `AlarmFanoutExceeded`).
    fn clone(&self) -> Self {
        let mut staging = Vec::with_capacity(self.staging.capacity());
        staging.extend_from_slice(&self.staging);
        AlarmEngine {
            states: self.states.clone(),
            listed: self.listed.clone(),
            ring: self.ring.clone(),
            capacity: self.capacity,
            next_event_seq: self.next_event_seq,
            watermark: self.watermark,
            high_water: self.high_water,
            last_change_op: self.last_change_op,
            staging,
            staged_events: self.staged_events,
        }
    }
}

impl AlarmEngine {
    pub(crate) fn new(t: &AlarmTable) -> AlarmEngine {
        let n = t.rules.len();
        AlarmEngine {
            states: vec![AlarmState::default(); n],
            listed: ListedIndex::new(n),
            ring: vec![EMPTY_EVENT; usize::try_from(t.outbox_capacity).unwrap_or(0)],
            capacity: t.outbox_capacity,
            next_event_seq: 1,
            watermark: 0,
            high_water: 0,
            last_change_op: 0,
            staging: Vec::with_capacity(t.worst_fanout.max(1) as usize),
            staged_events: 0,
        }
    }

    /// Bytes the engine allocates for `t` (all of it in `new`).
    pub(crate) fn storage_bytes(t: &AlarmTable) -> u128 {
        let n = t.rules.len() as u128;
        let w0 = n.div_ceil(64);
        n.saturating_mul(size_of::<AlarmState>() as u128)
            .saturating_add(w0.saturating_add(w0.div_ceil(64)).saturating_mul(8))
            .saturating_add(u128::from(t.outbox_capacity).saturating_mul(size_of::<AlarmEvent>() as u128))
            .saturating_add(u128::from(t.worst_fanout.max(1)).saturating_mul(size_of::<Staged>() as u128))
    }

    fn used(&self) -> u64 {
        self.next_event_seq.saturating_sub(1).saturating_sub(self.watermark)
    }

    /// Start a preflight.
    #[inline]
    pub(crate) fn begin(&mut self) {
        self.staging.clear();
        self.staged_events = 0;
    }

    fn push(&mut self, s: Staged) -> Result<(), ApplyError> {
        if self.staging.len() >= self.staging.capacity() {
            // Unreachable with a validated table (the capacity is the worst fanout); a
            // typed refusal instead of growing the scratch inside `apply`.
            return Err(ApplyError::AlarmFanoutExceeded { limit: self.staging.capacity() as u64 });
        }
        if s.event.is_some() {
            self.staged_events = self.staged_events.saturating_add(1);
        }
        self.staging.push(s);
        Ok(())
    }

    /// Stage the rules of tag index `t` for a predicted post-transition `value`/`quality`.
    #[inline]
    pub(crate) fn stage_tag(
        &mut self,
        table: &AlarmTable,
        t: u32,
        value: Option<Value>,
        quality: Quality,
        op: u64,
    ) -> Result<(), ApplyError> {
        for &r in table.tag_rules(t as usize) {
            let (Some(rule), Some(&st)) = (table.rules.get(r as usize), self.states.get(r as usize)) else { continue };
            match evaluate(rule, st, value, quality, op) {
                Ok(None) => {}
                Ok(Some((state, event))) => self.push(Staged { rule: r, state, event, actor_uid: 0 })?,
                Err(()) => {
                    return Err(ApplyError::AlarmCounterOverflow(
                        table.ids.get(r as usize).copied().unwrap_or(AlarmId(0)),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Stage an acknowledgement. `Ok(Some(reason))`: a no-op.
    pub(crate) fn stage_ack(
        &mut self,
        table: &AlarmTable,
        alarm_id: AlarmId,
        token: u64,
        actor_uid: u32,
    ) -> Result<Option<IgnoredReason>, ApplyError> {
        let r = table.rule_index(alarm_id).ok_or(ApplyError::UnknownAlarm(alarm_id))?;
        let st = *self.states.get(r).ok_or(ApplyError::UnknownAlarm(alarm_id))?;
        if st.occurrence == 0 {
            return Err(ApplyError::NotAcknowledgeable(alarm_id));
        }
        if token != st.token {
            return Err(ApplyError::StaleActivationToken { alarm_id, current: st.token });
        }
        if st.flags & F_UNACKED == 0 {
            return Ok(Some(IgnoredReason::AlreadyAcknowledged));
        }
        let mut s = st;
        s.flags &= !F_UNACKED;
        let rule = u32::try_from(r).map_err(|_| ApplyError::UnknownAlarm(alarm_id))?;
        self.push(Staged { rule, state: s, event: Some(AlarmEventKind::Acknowledged), actor_uid })?;
        Ok(None)
    }

    /// Admission of the staged transition: free slots and sequence room for every event.
    pub(crate) fn check(&self) -> Result<(), ApplyError> {
        let required = self.staged_events;
        if required == 0 {
            return Ok(());
        }
        let free = self.capacity.saturating_sub(self.used());
        if required > free {
            return Err(ApplyError::OutboxCapacityExceeded { required, free });
        }
        // The last event takes `next + required - 1`; u64::MAX stays unused as a sentinel.
        match self.next_event_seq.checked_add(required) {
            Some(_) => Ok(()),
            None => Err(ApplyError::EventSequenceExhausted),
        }
    }

    /// Infallible commit of a checked preflight.
    pub(crate) fn commit(&mut self, table: &AlarmTable, op: u64) {
        if self.staging.is_empty() {
            return;
        }
        // Canonical order of the events of one operation: ascending AlarmId (= rule index).
        // Each rule is staged at most once per operation (one tag per rule, each tag once).
        self.staging.sort_unstable_by_key(|s| s.rule);
        let mut seq = self.next_event_seq;
        let mut ordinal: u32 = 0;
        let cap = self.capacity;
        for s in &self.staging {
            let mut state = s.state;
            if let Some(kind) = s.event {
                state.last_event_seq = seq;
                let slot = seq.wrapping_sub(1).checked_rem(cap).unwrap_or(0);
                if let Some(e) = self.ring.get_mut(usize::try_from(slot).unwrap_or(usize::MAX)) {
                    *e = AlarmEvent {
                        event_seq: seq,
                        operation_seq: op,
                        occurrence_id: state.occurrence,
                        activation_token: state.token,
                        alarm_id: table.ids.get(s.rule as usize).copied().unwrap_or(AlarmId(0)),
                        ordinal_in_operation: ordinal,
                        actor_uid: s.actor_uid,
                        kind,
                        status_after: AlarmFlags(state.flags),
                    };
                }
                seq = seq.saturating_add(1);
                ordinal = ordinal.saturating_add(1);
            }
            if let Some(st) = self.states.get_mut(s.rule as usize) {
                *st = state;
            }
            self.listed.set(s.rule, state.flags & (F_ACTIVE | F_UNACKED) != 0);
        }
        self.next_event_seq = seq;
        self.last_change_op = op;
        self.high_water = self.high_water.max(self.used());
        self.staging.clear();
        self.staged_events = 0;
    }

    /// `AdvanceDurableWatermark`: `Ok(None)` = nothing to advance (no-op).
    pub(crate) fn advance_watermark(&mut self, up_to: u64) -> Result<Option<u64>, ApplyError> {
        let last = self.next_event_seq.saturating_sub(1);
        if up_to > last {
            return Err(ApplyError::WatermarkBeyondCommitted { up_to, last_committed: last });
        }
        if up_to < self.watermark {
            return Err(ApplyError::WatermarkRegression { current: self.watermark, got: up_to });
        }
        if up_to == self.watermark {
            return Ok(None);
        }
        let released = up_to.saturating_sub(self.watermark);
        self.watermark = up_to;
        Ok(Some(released))
    }

    pub(crate) fn status(&self) -> OutboxStatus {
        let newest = self.next_event_seq.saturating_sub(1);
        OutboxStatus {
            capacity: self.capacity,
            used: self.used(),
            free: self.capacity.saturating_sub(self.used()),
            high_water: self.high_water,
            next_event_seq: self.next_event_seq,
            durable_watermark: self.watermark,
            oldest_visible: self.oldest_visible(newest),
            active_or_unacked: self.listed.count,
            last_alarm_change_operation: self.last_change_op,
        }
    }

    fn oldest_visible(&self, newest: u64) -> u64 {
        if newest == 0 {
            0
        } else if newest >= self.capacity {
            newest.saturating_sub(self.capacity).saturating_add(1)
        } else {
            1
        }
    }

    pub(crate) fn view(&self, table: &AlarmTable, r: usize) -> Option<AlarmView> {
        let st = self.states.get(r)?;
        Some(AlarmView {
            alarm_id: *table.ids.get(r)?,
            severity: table.rules.get(r)?.severity,
            flags: AlarmFlags(st.flags),
            occurrence_id: st.occurrence,
            activation_token: st.token,
            last_event_seq: st.last_event_seq,
        })
    }

    pub(crate) fn event(&self, seq: u64) -> Option<AlarmEvent> {
        let newest = self.next_event_seq.saturating_sub(1);
        if seq == 0 || seq > newest || seq < self.oldest_visible(newest) {
            return None;
        }
        let slot = seq.wrapping_sub(1).checked_rem(self.capacity)?;
        self.ring.get(usize::try_from(slot).ok()?).copied()
    }

    /// Copy at most `limit` events after `after` into `out` (cleared; its capacity is the
    /// caller's preallocated page). Never moves the watermark. Cost O(page).
    pub(crate) fn read(&self, after: u64, limit: usize, out: &mut Vec<AlarmEvent>) -> EventPage {
        out.clear();
        let newest = self.next_event_seq.saturating_sub(1);
        let oldest = self.oldest_visible(newest);
        let mut start = after.saturating_add(1);
        let gap = newest > 0 && start < oldest;
        if gap {
            start = oldest;
        }
        let limit = limit.min(out.capacity());
        let mut seq = start;
        while seq <= newest && out.len() < limit {
            match self.event(seq) {
                Some(e) => out.push(e),
                None => break,
            }
            seq = seq.saturating_add(1);
        }
        let next_cursor = match out.last() {
            Some(e) => e.event_seq,
            None if gap => start.saturating_sub(1),
            None => after,
        };
        EventPage {
            next_cursor,
            oldest_visible: oldest,
            newest,
            has_more: next_cursor < newest,
            gap_detected: gap,
            cursor_ahead: after > newest,
            durable_watermark: self.watermark,
        }
    }

    /// Alarms `active || unacknowledged` with id > `after`, ascending, at most `limit`.
    pub(crate) fn snapshot(
        &self,
        table: &AlarmTable,
        after: AlarmId,
        limit: usize,
        out: &mut Vec<AlarmView>,
    ) -> SnapshotPage {
        out.clear();
        let limit = limit.min(out.capacity());
        let mut from = match table.ids.binary_search(&after) {
            Ok(i) => i.saturating_add(1),
            Err(i) => i,
        };
        let mut next_after = after;
        let mut has_more = false;
        while let Some(i) = self.listed.next(from) {
            if out.len() >= limit {
                has_more = true;
                break;
            }
            if let Some(v) = self.view(table, i) {
                next_after = v.alarm_id;
                out.push(v);
            }
            from = i.saturating_add(1);
        }
        SnapshotPage {
            next_after,
            has_more,
            total_listed: self.listed.count,
            as_of_change_operation: self.last_change_op,
        }
    }

    pub(crate) fn hash_into(&self, h: &mut crate::digest::CanonicalHasher) {
        h.len(self.states.len());
        for s in &self.states {
            h.u8(s.flags);
            h.u64(s.occurrence);
            h.u64(s.token);
            h.u64(s.last_event_seq);
        }
        h.u64(self.last_change_op);
        h.u64(self.capacity);
        h.u64(self.next_event_seq);
        h.u64(self.watermark);
    }

    /// Digest of the events still readable (tests, replay comparison): O(capacity).
    pub(crate) fn events_digest(&self) -> [u8; 32] {
        let mut h = crate::digest::CanonicalHasher::new(b"scada.alarm.events", 1);
        let newest = self.next_event_seq.saturating_sub(1);
        let mut seq = self.oldest_visible(newest);
        h.u64(seq);
        h.u64(newest);
        while seq != 0 && seq <= newest {
            if let Some(e) = self.event(seq) {
                h.u64(e.event_seq);
                h.u64(e.operation_seq);
                h.u64(e.occurrence_id);
                h.u64(e.activation_token);
                h.u32(e.alarm_id.0);
                h.u32(e.ordinal_in_operation);
                h.u32(e.actor_uid);
                h.u8(e.kind.code());
                h.u8(e.status_after.0);
            }
            seq = seq.saturating_add(1);
        }
        h.finish()
    }

    pub(crate) fn rules(&self) -> usize {
        self.states.len()
    }

    /// Tests only: jump the event sequence (exhaustion tests).
    #[cfg(test)]
    pub(crate) fn set_next_event_seq_for_test(&mut self, next: u64) {
        self.next_event_seq = next;
        self.watermark = next.saturating_sub(1);
    }
}

impl fmt::Display for EpisodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EpisodeState::Normal => "Normal",
            EpisodeState::ActiveUnacked => "ActiveUnacked",
            EpisodeState::ActiveAcked => "ActiveAcked",
            EpisodeState::ReturnedUnacked => "ReturnedUnacked",
        })
    }
}

/// `size_of` of the hot structures (capacity report, ADR 0012 §8).
pub fn layout_sizes() -> [(&'static str, usize); 4] {
    [
        ("AlarmState", size_of::<AlarmState>()),
        ("AlarmEvent", size_of::<AlarmEvent>()),
        ("CompiledRule", size_of::<CompiledRule>()),
        ("StagedAlarm", size_of::<Staged>()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout() {
        assert_eq!(size_of::<AlarmState>(), 32);
        assert_eq!(size_of::<AlarmEvent>(), 48);
        assert_eq!(size_of::<CompiledRule>(), 24);
    }

    #[test]
    fn listed_index_next_matches_a_scan() {
        let n = 10_000;
        let mut ix = ListedIndex::new(n);
        let mut model = vec![false; n];
        let mut x: u64 = 7;
        for _ in 0..20_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let i = ((x >> 33) as usize) % n;
            let on = (x >> 20) & 1 == 1;
            ix.set(i as u32, on);
            model[i] = on;
            let from = ((x >> 40) as usize) % n;
            assert_eq!(ix.next(from), (from..n).find(|&j| model[j]));
        }
        assert_eq!(ix.count, model.iter().filter(|b| **b).count() as u64);
    }

    fn rule(kind: u8, trip: i64, reset: i64) -> CompiledRule {
        CompiledRule { trip, reset, tag: 0, kind, severity: Severity::Major }
    }

    #[test]
    fn occurrence_counter_never_wraps() {
        let r = rule(K_BADQ, 0, 0);
        let st = AlarmState { occurrence: u64::MAX, flags: F_ARMED, ..AlarmState::default() };
        let q = Quality::Uncertain(crate::types::UncertainReason::StaleData);
        assert_eq!(evaluate(&r, st, None, q, 1), Err(()));
        // A retrigger does not need a new occurrence: allowed at the limit.
        let st = AlarmState { occurrence: u64::MAX, flags: F_ARMED | F_UNACKED, ..AlarmState::default() };
        assert_eq!(evaluate(&r, st, None, q, 1).unwrap().unwrap().1, Some(AlarmEventKind::Retriggered));
    }

    #[test]
    fn high_hysteresis_and_limits() {
        let r = rule(K_HIGH, i64::MAX, i64::MAX - 1);
        let s0 = AlarmState::default();
        let g = Quality::Good;
        let (s1, e) = evaluate(&r, s0, Some(Value::I64(i64::MAX)), g, 5).unwrap().unwrap();
        assert_eq!(e, Some(AlarmEventKind::Raised));
        assert_eq!((s1.token, s1.occurrence), (5, 1));
        // reset is inclusive.
        let (s2, e) = evaluate(&r, s1, Some(Value::I64(i64::MAX - 1)), g, 6).unwrap().unwrap();
        assert_eq!(e, Some(AlarmEventKind::Returned));
        assert_eq!(s2.flags & F_ACTIVE, 0);
        let r = rule(K_LOW, i64::MIN, i64::MIN + 1);
        let (s1, e) = evaluate(&r, s0, Some(Value::I64(i64::MIN)), g, 1).unwrap().unwrap();
        assert_eq!(e, Some(AlarmEventKind::Raised));
        assert!(evaluate(&r, s1, Some(Value::I64(i64::MIN)), g, 2).unwrap().is_none());
    }
}
