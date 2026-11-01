//! VS-006: sequencer side of the durable journal (ADR 0013 D4, D5, D7, D9, D10, D11).
//!
//! Owned by the sequencer thread. It reserves journal room **before** `Kernel::apply`
//! (J4), samples the time envelope (ADR 0014), builds one [`OpRecord`] per applied
//! operation (zero-event operations included) and hands it to the writer without ever
//! waiting for it. It never publishes durability itself: only the writer does, after
//! `COMMIT` (J1); the sequencer turns that into `AdvanceDurableWatermark` (J3).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::Instant;

use scada_core::{AlarmId, AppliedKind, Kernel, Operation, SequencedInput, SourceId};
use scada_journal::reconcile::legacy_continuity;
use scada_journal::{
    CheckpointMsg, EVENT_RECORD_BYTES, EventOrigin, JournalShared, JournalState, NOTIFY_BYTES, OBSERVATION_BYTES,
    OpRecord, OperatorAction, RECORD_FIXED_BYTES, Receipt, SourceRecoveryUpdate, SourceTimes, TimeAnomaly,
    TimeEnvelope, WriterMsg, continuity, fault, live, time_source,
};

use crate::config::PersistenceSettings;
use crate::time_authority::TimeAuthority;

/// Why the journal refused an operation (nothing applied, no sequence consumed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    QueueFull,
    DiskFull,
    Fault,
}

impl Refusal {
    pub fn driver_code(self) -> scada_protocol::driver_v1::ErrorCode {
        use scada_protocol::driver_v1::ErrorCode;
        match self {
            Refusal::QueueFull => ErrorCode::JournalQueueFull,
            Refusal::DiskFull => ErrorCode::DiskFull,
            Refusal::Fault => ErrorCode::JournalFault,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Refusal::QueueFull => "journal queue full",
            Refusal::DiskFull => "disk full / journal size cap reached",
            Refusal::Fault => "journal fault (restart required)",
        }
    }
}

/// Room reserved before `apply`.
#[derive(Debug, Clone, Copy)]
pub struct Reservation {
    bytes: u64,
}

/// Per-operation data the caller hands to the journal besides the operation.
#[derive(Default)]
pub struct Extras {
    pub source_times: Option<Box<SourceTimes>>,
    pub received_at: Option<i64>,
    pub action: Option<OperatorAction>,
    /// Keep the record until [`Persist::release_held`] attaches a receipt.
    pub hold: bool,
}

/// Dedup cache entry of an operator action (ADR 0013 D11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedAction {
    pub alarm_id: u32,
    pub token: u64,
    pub operation_seq: u64,
    pub event_seq: Option<u64>,
}

/// Bounded FIFO map of recent action ids.
#[derive(Debug, Default)]
pub struct ActionCache {
    map: HashMap<[u8; 16], CachedAction>,
    order: VecDeque<[u8; 16]>,
    cap: usize,
}

impl ActionCache {
    pub fn new(cap: usize) -> Self {
        ActionCache {
            map: HashMap::with_capacity(cap.min(1 << 16)),
            order: VecDeque::with_capacity(cap.min(1 << 16)),
            cap,
        }
    }
    pub fn get(&self, id: &[u8; 16]) -> Option<CachedAction> {
        self.map.get(id).copied()
    }
    pub fn insert(&mut self, id: [u8; 16], a: CachedAction) {
        if self.map.insert(id, a).is_none() {
            self.order.push_back(id);
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Live verification / continuity of a source as last recorded (runtime view).
#[derive(Debug, Clone, Copy)]
pub struct SourceTrack {
    /// `(epoch, last_accepted)` captured by the recovery fence (pre-restart position).
    pub fenced: Option<(Option<u64>, Option<u64>)>,
    pub live: u8,
    pub history: u8,
}

/// Values readable outside the sequencer (`GetRecoveryStatus`).
#[derive(Debug)]
pub struct PersistPublic {
    pub live: Vec<AtomicU8>,
    pub history: Vec<AtomicU8>,
    pub connected: Vec<AtomicBool>,
    /// Last operation applied (and enqueued) by the sequencer.
    pub applied_op: AtomicU64,
    /// Recovery fence applied and READY not yet reached: projection is recovering.
    pub recovering: AtomicBool,
}

/// Wakes the sequencer after a commit (at most one wake request queued).
pub struct JournalWaker {
    pub waker: crate::ingress::TickWaker,
    pub queued: Arc<AtomicBool>,
}

impl JournalWaker {
    pub fn wake(&self) {
        if !self.queued.swap(true, Relaxed) {
            match self.waker.try_wake_with(crate::sequencer::Request::JournalWake) {
                crate::ingress::Wake::Queued => {}
                _ => self.queued.store(false, Relaxed),
            }
        }
    }
}

pub struct Persist {
    tx: SyncSender<WriterMsg>,
    pub shared: Arc<JournalShared>,
    pub public: Arc<PersistPublic>,
    pub time: TimeAuthority,
    pub settings: PersistenceSettings,
    pub durable: bool,
    pub boot_generation: u64,
    /// Worst fan-out (rules) of each source, by configuration index.
    fanout: Vec<u64>,
    /// Rule index → tag index (origin times).
    rule_tag: Vec<u32>,
    /// Tag index → (source index, channel id).
    tag_channel: Vec<Option<(usize, u32)>>,
    checkpoints_sent: u64,
    last_checkpoint_op: u64,
    pub track: Vec<SourceTrack>,
    pub actions: ActionCache,
    /// Disconnections refused by the journal, retried after durable progress.
    pub deferred_disconnects: Vec<(SourceId, u64)>,
    held: Option<Box<OpRecord>>,
    pub wake_queued: Arc<AtomicBool>,
    pub timeline_id: [u8; 16],
    /// Durable operation sequence restored at startup.
    pub recovered_op: u64,
}

impl std::fmt::Debug for Persist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persist").field("boot_generation", &self.boot_generation).finish_non_exhaustive()
    }
}

/// Construction parameters.
pub struct PersistInit {
    pub tx: SyncSender<WriterMsg>,
    pub shared: Arc<JournalShared>,
    pub time: TimeAuthority,
    pub settings: PersistenceSettings,
    pub boot_generation: u64,
    pub track: Vec<SourceTrack>,
    pub actions: ActionCache,
    pub last_checkpoint_op: u64,
    pub wake_queued: Arc<AtomicBool>,
    pub timeline_id: [u8; 16],
    pub recovered_op: u64,
}

impl Persist {
    pub fn new(k: &Kernel, init: PersistInit) -> Persist {
        let cfg = k.config();
        let n_tags = cfg.tags().len();
        let mut tag_channel = vec![None; n_tags];
        for (si, s) in cfg.sources().iter().enumerate() {
            for &(ch, t) in s.channels() {
                if let Some(slot) = tag_channel.get_mut(t as usize) {
                    *slot = Some((si, ch.0));
                }
            }
        }
        let tag_index: HashMap<u32, u32> =
            cfg.tags().iter().enumerate().map(|(i, t)| (t.tag_id.0, u32::try_from(i).unwrap_or(u32::MAX))).collect();
        let (fanout, rule_tag) = match cfg.alarm_table() {
            Some(t) => {
                let fanout = cfg
                    .sources()
                    .iter()
                    .map(|s| s.channels().iter().map(|&(_, ti)| t.rules_of_tag(ti as usize) as u64).sum())
                    .collect();
                let rule_tag = (0..t.len())
                    .map(|r| t.rule(r).and_then(|m| tag_index.get(&m.tag_id.0).copied()).unwrap_or(u32::MAX))
                    .collect();
                (fanout, rule_tag)
            }
            None => (vec![0; cfg.sources().len()], Vec::new()),
        };
        let ns = cfg.sources().len();
        let public = Arc::new(PersistPublic {
            live: (0..ns).map(|i| AtomicU8::new(init.track.get(i).map_or(live::NEVER_CONNECTED, |t| t.live))).collect(),
            history: (0..ns).map(|i| AtomicU8::new(init.track.get(i).map_or(0, |t| t.history))).collect(),
            connected: (0..ns).map(|_| AtomicBool::new(false)).collect(),
            applied_op: AtomicU64::new(k.last_operation_sequence()),
            recovering: AtomicBool::new(true),
        });
        Persist {
            tx: init.tx,
            shared: init.shared,
            public,
            time: init.time,
            durable: init.settings.durable,
            settings: init.settings,
            boot_generation: init.boot_generation,
            fanout,
            rule_tag,
            tag_channel,
            checkpoints_sent: 0,
            last_checkpoint_op: init.last_checkpoint_op,
            track: init.track,
            actions: init.actions,
            deferred_disconnects: Vec::new(),
            held: None,
            wake_queued: init.wake_queued,
            timeline_id: init.timeline_id,
            recovered_op: init.recovered_op,
        }
    }

    fn fan(&self, k: &Kernel, s: SourceId) -> u64 {
        k.config().source_index(s).and_then(|i| self.fanout.get(i).copied()).unwrap_or(0)
    }

    /// Conservative size of the record of `op` (ADR 0013 D4 `B_max`).
    pub fn estimate(&self, k: &Kernel, op: &Operation, x: &Extras) -> u64 {
        let st = x.source_times.as_ref().map_or(0, |s| 16 + s.clock_id.len() as u64 + 12 * s.observations.len() as u64);
        let note = x.action.as_ref().map_or(0, |a| 64 + a.note.len() as u64);
        RECORD_FIXED_BYTES
            + st
            + note
            + match op {
                Operation::ObservationBatch(b) => {
                    b.observations.len() as u64 * OBSERVATION_BYTES + self.fan(k, b.source_id) * EVENT_RECORD_BYTES + 48
                }
                Operation::SourceConnected { source_id, .. }
                | Operation::SourceDisconnected { source_id, .. }
                | Operation::SourceSilenceExpired { source_id, .. }
                | Operation::SourceLivenessRestored { source_id, .. }
                | Operation::FreshnessExpired { source_id, .. } => self.fan(k, *source_id) * EVENT_RECORD_BYTES + 48,
                Operation::AlarmAcknowledge { .. } => EVENT_RECORD_BYTES,
                Operation::AdvanceDurableWatermark { .. } => 0,
                Operation::RecoveryFence { .. } => 48 * k.config().sources().len() as u64,
            }
    }

    /// J4: reserve room for one record before `apply`. Internal operations
    /// (`AdvanceDurableWatermark`) bypass the disk cap (they free RAM, not disk) but not
    /// the queue bound nor a fault.
    pub fn admit(&self, bytes: u64, internal: bool) -> Result<Reservation, Refusal> {
        let s = &self.shared;
        let refuse = |r: Refusal| {
            match r {
                Refusal::QueueFull => s.refused_queue_full.fetch_add(1, Relaxed),
                Refusal::DiskFull => s.refused_disk_full.fetch_add(1, Relaxed),
                Refusal::Fault => s.refused_fault.fetch_add(1, Relaxed),
            };
            Err(r)
        };
        match s.state() {
            JournalState::Fault => return refuse(Refusal::Fault),
            JournalState::DiskFull if !internal => return refuse(Refusal::DiskFull),
            _ => {}
        }
        if !internal
            && (s.disk_free.load(Relaxed) < self.settings.min_disk_free_bytes
                || self
                    .settings
                    .max_db_bytes
                    .is_some_and(|m| s.db_bytes.load(Relaxed).saturating_add(s.wal_bytes.load(Relaxed)) >= m))
        {
            return refuse(Refusal::DiskFull);
        }
        let ops = s.reserved_ops.load(Relaxed);
        let used = s.reserved_bytes.load(Relaxed);
        if ops >= u64::from(self.settings.journal_queue_operations)
            || used.saturating_add(bytes) > self.settings.journal_queue_bytes
        {
            return refuse(Refusal::QueueFull);
        }
        // Only the sequencer increments: check-then-add is race free (the writer only
        // decrements).
        let ops = s.reserved_ops.fetch_add(1, Relaxed) + 1;
        let used = s.reserved_bytes.fetch_add(bytes, Relaxed) + bytes;
        s.reserved_ops_high.fetch_max(ops, Relaxed);
        s.reserved_bytes_high.fetch_max(used, Relaxed);
        Ok(Reservation { bytes })
    }

    /// The operation was not applied: give the room back.
    pub fn cancel(&self, r: Reservation) {
        self.shared.reserved_ops.fetch_sub(1, Relaxed);
        self.shared.reserved_bytes.fetch_sub(r.bytes, Relaxed);
    }

    fn origins(&self, k: &Kernel, rec: &OpRecord) -> Vec<EventOrigin> {
        let Some(st) = rec.source_times.as_deref() else { return Vec::new() };
        let Operation::ObservationBatch(b) = &rec.operation else { return Vec::new() };
        let Some(si) = k.config().source_index(b.source_id) else { return Vec::new() };
        let t = k.config().alarm_table();
        rec.events
            .iter()
            .map(|e| {
                let tag = t.and_then(|t| t.rule_index(e.alarm_id)).and_then(|r| self.rule_tag.get(r).copied());
                let ch = tag.and_then(|ti| self.tag_channel.get(ti as usize).copied().flatten());
                let dev = ch
                    .filter(|(s, _)| *s == si)
                    .and_then(|(_, c)| st.observations.iter().find(|(oc, _)| *oc == c).map(|(_, ts)| *ts));
                match (dev, st.acquired_at_utc_ns) {
                    (Some(ts), _) => {
                        EventOrigin { utc_ns: Some(ts), source: time_source::DEVICE, clock_quality: st.clock_quality }
                    }
                    (None, Some(ts)) if ch.is_some_and(|(s, _)| s == si) => {
                        EventOrigin { utc_ns: Some(ts), source: time_source::DRIVER, clock_quality: st.clock_quality }
                    }
                    _ => EventOrigin::default(),
                }
            })
            .collect()
    }

    /// Recovery bookkeeping carried by the record (D9/D10).
    fn recovery_updates(
        &mut self,
        k: &Kernel,
        op: &Operation,
        kind: &AppliedKind,
        seq: u64,
    ) -> (bool, Vec<SourceRecoveryUpdate>) {
        let cfg = k.config().clone();
        let mut ups = Vec::new();
        let mut reconciliation = false;
        match (op, kind) {
            (Operation::RecoveryFence { .. }, AppliedKind::RecoveryFenced { .. }) => {
                for (i, s) in cfg.sources().iter().enumerate() {
                    let Some(st) = k.source_status(s.source_id) else { continue };
                    if !st.unverified {
                        continue;
                    }
                    let tr = self.track.get_mut(i);
                    let Some(tr) = tr else { continue };
                    // A second restart before the resync keeps the first pre-restart position.
                    if tr.fenced.is_none() {
                        tr.fenced = Some((st.epoch, st.last_accepted_source_sequence));
                    }
                    tr.live = live::UNVERIFIED_RECOVERED;
                    tr.history = continuity::UNKNOWN;
                    ups.push(SourceRecoveryUpdate {
                        source_id: s.source_id.0,
                        source_epoch: st.epoch,
                        live: tr.live,
                        history: tr.history,
                        last_source_sequence: st.last_accepted_source_sequence,
                        verified_at_operation_seq: 0,
                        gap: None,
                    });
                }
            }
            (Operation::SourceConnected { source_id, source_epoch, .. }, AppliedKind::Connected { .. }) => {
                if let Some(i) = cfg.source_index(*source_id)
                    && let Some(c) = self.public.connected.get(i)
                {
                    c.store(true, Relaxed);
                }
                if let (Some(i), Some(st)) = (cfg.source_index(*source_id), k.source_status(*source_id))
                    && st.unverified
                    && let Some(tr) = self.track.get_mut(i)
                {
                    tr.live = live::AWAITING_FULL_STATE;
                    ups.push(SourceRecoveryUpdate {
                        source_id: source_id.0,
                        source_epoch: Some(*source_epoch),
                        live: tr.live,
                        history: tr.history,
                        last_source_sequence: st.last_accepted_source_sequence,
                        verified_at_operation_seq: 0,
                        gap: None,
                    });
                }
            }
            (Operation::SourceDisconnected { source_id, .. }, _) => {
                if let Some(i) = cfg.source_index(*source_id)
                    && let Some(c) = self.public.connected.get(i)
                    && !matches!(
                        k.source_status(*source_id).map(|s| s.link),
                        Some(scada_core::LinkState::Connected { .. })
                    )
                {
                    c.store(false, Relaxed);
                }
            }
            (Operation::ObservationBatch(b), AppliedKind::Batch { reconciled: true, .. }) => {
                reconciliation = true;
                if let Some(i) = cfg.source_index(b.source_id)
                    && let Some(tr) = self.track.get_mut(i)
                {
                    let epoch = k.source_status(b.source_id).and_then(|s| s.epoch);
                    let v = legacy_continuity(tr.fenced, epoch, b.source_sequence);
                    tr.fenced = None;
                    tr.live = live::LIVE_VERIFIED;
                    tr.history = v.continuity;
                    ups.push(SourceRecoveryUpdate {
                        source_id: b.source_id.0,
                        source_epoch: epoch,
                        live: tr.live,
                        history: tr.history,
                        last_source_sequence: Some(b.source_sequence),
                        verified_at_operation_seq: seq,
                        gap: v.gap,
                    });
                }
            }
            _ => {}
        }
        for u in &ups {
            if let Some(i) = cfg.source_index(SourceId(u.source_id)) {
                if let Some(a) = self.public.live.get(i) {
                    a.store(u.live, Relaxed);
                }
                if let Some(a) = self.public.history.get(i) {
                    a.store(u.history, Relaxed);
                }
            }
        }
        (reconciliation, ups)
    }

    /// Build the record of an applied operation and hand it to the writer (or hold it).
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        k: &Kernel,
        input: SequencedInput,
        kind: &AppliedKind,
        env: TimeEnvelope,
        anomaly: Option<TimeAnomaly>,
        ev_before: u64,
        res: Reservation,
        x: Extras,
    ) {
        let ev_after = k.outbox_status().map_or(ev_before, |s| s.next_event_seq);
        let events: Vec<_> = (ev_before..ev_after).filter_map(|s| k.alarm_event(s)).collect();
        let seq = input.operation_sequence;
        let (reconciliation, recovery) = self.recovery_updates(k, &input.operation, kind, seq);
        let mut action = x.action;
        if let (Some(a), AppliedKind::AlarmAcknowledged { event_seq, .. }) = (action.as_mut(), kind) {
            a.event_seq = Some(*event_seq);
            a.occurrence_id = events.first().map_or(0, |e| e.occurrence_id);
            a.outcome = scada_protocol::alarm_v1::AckStatus::AppliedVolatile as u8;
            self.actions.insert(
                a.action_id,
                CachedAction {
                    alarm_id: a.alarm_id,
                    token: a.requested_token,
                    operation_seq: seq,
                    event_seq: Some(*event_seq),
                },
            );
        }
        let mut env = env;
        env.received_at_utc_ns = x.received_at;
        let mut rec = OpRecord {
            operation_seq: seq,
            operation: input.operation,
            time: env,
            source_times: x.source_times,
            events,
            origins: Vec::new(),
            reconciliation,
            action,
            recovery,
            anomaly,
            reserved_bytes: res.bytes,
            receipt: None,
        };
        rec.origins = self.origins(k, &rec);
        // Shrink the reservation to the actual size (never grow it past the estimate).
        let actual = rec.queued_bytes().min(res.bytes);
        self.shared.reserved_bytes.fetch_sub(res.bytes - actual, Relaxed);
        rec.reserved_bytes = actual;
        fault::hit("after_apply_before_enqueue");
        self.public.applied_op.store(seq, Relaxed);
        // Safety net: a held record is never overtaken by a later one.
        if let Some(h) = self.held.take() {
            self.send(WriterMsg::Op(h));
        }
        if x.hold {
            self.held = Some(Box::new(rec));
        } else {
            self.send(WriterMsg::Op(Box::new(rec)));
        }
        self.maybe_checkpoint(k);
    }

    /// Attach the receipt to the held record (if any) and send it.
    pub fn release_held(&mut self, receipt: Option<Receipt>) {
        if let Some(mut r) = self.held.take() {
            r.receipt = receipt;
            self.send(WriterMsg::Op(r));
        }
    }

    /// Call `receipt` once every operation `<= op` is durable (now if already durable).
    pub fn notify(&self, op: u64, receipt: Receipt) -> Result<(), (Refusal, Receipt)> {
        if self.shared.durable_op.load(std::sync::atomic::Ordering::Acquire) >= op && self.durable {
            receipt(scada_journal::CommitResult::Durable);
            return Ok(());
        }
        match self.admit(NOTIFY_BYTES, true) {
            Ok(_) => {
                self.send(WriterMsg::Notify { operation_seq: op, receipt });
                Ok(())
            }
            Err(r) => Err((r, receipt)),
        }
    }

    fn send(&self, m: WriterMsg) {
        match self.tx.try_send(m) {
            Ok(()) => fault::hit("after_enqueue"),
            Err(TrySendError::Full(m)) | Err(TrySendError::Disconnected(m)) => {
                // Impossible by construction (reservation precedes apply, the channel has
                // room for every reservation): treated as a journal fault (D4).
                eprintln!("journal: enqueue failed after a successful reservation: FAULT ({m:?})");
                self.shared.set_error("enqueue failed after reservation");
                self.shared.state.store(JournalState::Fault as u8, Relaxed);
            }
        }
    }

    fn maybe_checkpoint(&mut self, k: &Kernel) {
        let op = k.last_operation_sequence();
        if op.saturating_sub(self.last_checkpoint_op) < self.settings.checkpoint_every_operations
            || self.shared.checkpoints_done.load(Relaxed) < self.checkpoints_sent
            || self.held.is_some()
        {
            return;
        }
        let t0 = Instant::now();
        let checkpoint = k.export_checkpoint();
        let state_digest = k.state_digest();
        self.shared.checkpoint_pause.record_ns(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let event_seq_high = k.outbox_status().map_or(0, |s| s.next_event_seq.saturating_sub(1));
        self.last_checkpoint_op = op;
        self.checkpoints_sent += 1;
        self.send(WriterMsg::Checkpoint(Box::new(CheckpointMsg { checkpoint, state_digest, event_seq_high })));
    }

    /// Durable event sequence beyond the kernel watermark (J3), if any.
    pub fn watermark_due(&self, k: &Kernel) -> Option<u64> {
        let d = self.shared.durable_event.load(std::sync::atomic::Ordering::Acquire);
        let o = k.outbox_status()?;
        (o.durable_watermark < d).then_some(d.min(o.next_event_seq.saturating_sub(1)))
    }

    /// Alarm id helper for operator actions.
    pub fn conflict(&self, id: &[u8; 16], alarm: AlarmId, token: u64) -> Option<(bool, CachedAction)> {
        let c = self.actions.get(id)?;
        Some((c.alarm_id != alarm.0 || c.token != token, c))
    }
}

impl std::fmt::Debug for Extras {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extras")
            .field("received_at", &self.received_at)
            .field("hold", &self.hold)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for JournalWaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalWaker").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PersistInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistInit").field("boot_generation", &self.boot_generation).finish_non_exhaustive()
    }
}
