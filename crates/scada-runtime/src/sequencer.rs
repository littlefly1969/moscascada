//! The single writer: one OS thread owns the [`Kernel`] and applies admitted requests one
//! at a time, in the order they are taken from the bounded input queue (ingress
//! admission order). It assigns `operation_sequence` and `link_generation`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use scada_core::{
    AppliedKind, Kernel, LinkState, ObservationBatch, Operation, Quality, SequencedInput, SourceId, TransitionOutcome,
    UncertainReason, Value,
};
use scada_protocol::driver_v1::{AckStatus, ErrorCode};
use scada_protocol::introspect_v1 as ipb;
use tokio::sync::oneshot;

use crate::ingress::Inbox;

use crate::alarms::{AckOutcome, AlarmCall, AlarmReply, AlarmRuntime, CapacityReport, Origin};
use crate::capture::CaptureWriter;
use crate::convert;
use crate::diag::{self, Diag, Stage};
use crate::persist::{Extras, Persist, Refusal};
use crate::supervision::{HeartbeatCounter, Supervisor, WatchdogShared};
use scada_journal::{CommitResult, OperatorAction, SourceTimes, fault};

/// VS-006: why an input was not applied: the core refused it, or the journal had no room
/// (J4: decided before `apply`, nothing changed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    Core(scada_core::ApplyError),
    Journal(Refusal),
}

/// The core part of a result (VS-005 bookkeeping ignores journal refusals).
fn core_result(r: &Result<TransitionOutcome, Refused>) -> Option<Result<TransitionOutcome, scada_core::ApplyError>> {
    match r {
        Ok(o) => Some(Ok(*o)),
        Err(Refused::Core(e)) => Some(Err(*e)),
        Err(Refused::Journal(_)) => None,
    }
}

/// Runtime counters. Metrics only: never part of the logical state or the digest.
#[derive(Debug, Default)]
pub struct Counters {
    pub connections_accepted: AtomicU64,
    pub connections_refused_limit: AtomicU64,
    pub handshakes_rejected: AtomicU64,
    pub protocol_errors: AtomicU64,
    pub batches_applied: AtomicU64,
    pub batches_duplicate: AtomicU64,
    pub batches_rejected: AtomicU64,
    pub input_queue_full_waits: AtomicU64,
    pub active_connections: AtomicU64,
    /// D2: connections closed because the peer did not read its ACKs in time.
    pub ack_write_timeouts: AtomicU64,
    /// D3: connections closed because a newer link took over their source.
    pub links_superseded: AtomicU64,
    /// VS-004 (operational, never in the digest).
    pub heartbeats_valid: AtomicU64,
    pub heartbeats_invalid: AtomicU64,
    pub heartbeats_coalesced: AtomicU64,
    pub liveness_expiries: AtomicU64,
    /// Every Silent -> Alive of the same link (heartbeat or batch).
    pub liveness_recoveries: AtomicU64,
    /// The part of `liveness_recoveries` caused by an applied batch.
    pub liveness_recoveries_by_batch: AtomicU64,
    pub freshness_expiries: AtomicU64,
    pub freshness_tags_expired: AtomicU64,
    pub watchdog_ticks: AtomicU64,
    pub watchdog_ticks_coalesced: AtomicU64,
    pub watchdog_ticks_late: AtomicU64,
    pub watchdog_max_late_ns: AtomicU64,
    /// VS-005 (operational, never in the digest): transitions refused for lack of outbox
    /// slots, by origin.
    pub outbox_rejected_batches: AtomicU64,
    pub outbox_rejected_link: AtomicU64,
    pub outbox_rejected_supervision: AtomicU64,
    pub outbox_rejected_acks: AtomicU64,
    /// Gauge: disconnections waiting for outbox slots.
    pub pending_link_transitions: AtomicU64,
    /// Gauge: 1 normal, 2 high, 3 critical.
    pub outbox_overload: AtomicU64,
    pub alarm_acks_applied: AtomicU64,
    /// Control connections refused (uid not allowed or connection limit).
    pub alarm_control_refused: AtomicU64,
}

impl Counters {
    pub fn inc(c: &AtomicU64) {
        c.fetch_add(1, Ordering::Relaxed);
    }
}

/// Answer to a connect request.
#[derive(Debug, Clone, Copy)]
pub enum ConnectReply {
    Welcome { link_generation: u64, full_state_required: bool, last_accepted: Option<u64> },
    Rejected(ErrorCode),
}

/// Answer to a batch, already mapped to protocol terms.
#[derive(Debug, Clone, Copy)]
pub struct BatchReply {
    pub status: AckStatus,
    pub operation_sequence: Option<u64>,
    pub error_code: ErrorCode,
    pub continuity: Option<scada_protocol::driver_v1::Continuity>,
    pub last_accepted: Option<u64>,
    /// When the sequencer sent the reply (diagnostics only, never part of the state).
    pub sent: Option<Instant>,
}

/// VS-006: per-batch data for the journal (only with `[persistence]`).
#[derive(Debug)]
pub struct BatchExt {
    /// Wire 1.2 source time sidecar (journal only, never the core).
    pub times: Option<Box<SourceTimes>>,
    /// `runtime_received_at`, UTC ns.
    pub received_at: i64,
    /// The link negotiated durable receipts: the ACK waits for the commit.
    pub durable: bool,
}

/// Requests admitted into the bounded input queue.
#[derive(Debug)]
pub enum Request {
    /// `superseded` fires (with `()`) when a later link of the same source is welcomed
    /// (D3), so an idle old session is closed instead of holding a connection slot.
    /// `heartbeats`: VS-004 per-link heartbeat counter (liveness-supervised sources only).
    Connect {
        source_id: SourceId,
        source_epoch: u64,
        reply: oneshot::Sender<ConnectReply>,
        superseded: oneshot::Sender<()>,
        heartbeats: Option<HeartbeatCounter>,
    },
    Disconnect {
        source_id: SourceId,
        link_generation: u64,
    },
    /// `submitted`: when the session started to submit it (diagnostics only).
    /// `permits`: VS-003 admission permits (empty with `fifo`), released when the request
    /// is dropped after `apply`.
    Batch {
        batch: ObservationBatch,
        reply: oneshot::Sender<BatchReply>,
        submitted: Option<Instant>,
        permits: crate::ingress::Permits,
        /// VS-006: journal side data, `None` without `[persistence]` (one pointer: the
        /// queued request keeps its VS-005 size, J10).
        ext: Option<Box<BatchExt>>,
    },
    Introspect {
        include_tags: bool,
        reset_diagnostics: bool,
        reply: oneshot::Sender<ipb::IntrospectResponse>,
    },
    /// VS-004: wake-up of an idle sequencer by the watchdog (no payload; the tick itself is
    /// the flag in [`WatchdogShared`]).
    Tick,
    /// VS-005: alarm control plane (snapshot, events, ACK, capacity, test sink).
    Alarm {
        call: AlarmCall,
        reply: oneshot::Sender<AlarmReply>,
    },
    /// VS-006: the journal writer committed something (durable progress to turn into a
    /// watermark, deferred operations to retry). No payload.
    JournalWake,
}

/// Static data the sequencer needs besides the kernel.
#[derive(Debug, Clone)]
pub struct SequencerContext {
    pub kernel_instance_id: [u8; 16],
    pub counters: Arc<Counters>,
    pub input_queue_capacity: u32,
    /// Artificial delay after each request (saturation tests only; 0 in production).
    pub throttle: Duration,
    /// Stage timings (`--diag`); `None` in production.
    pub diag: Option<Arc<Diag>>,
    /// Scheduler description reported by the introspection.
    pub scheduler: String,
    /// Test only (`--test-fail-sequencer-after N`): panic after N requests, to prove that
    /// the host stops instead of running without its single writer.
    pub fail_after: Option<u64>,
    /// VS-004: watchdog flags, `Some` only when a source is supervised.
    pub watchdog: Option<Arc<WatchdogShared>>,
    /// VS-005: page maximum of the control plane (0 without alarms).
    pub alarm_page_max: u32,
}

/// Start the sequencer thread. It ends when every sender of `rx` is dropped. `stopped`
/// resolves when the thread ends for any reason (including a panic, which unwinds and drops
/// it), so the host can stop instead of running without its single writer.
pub fn spawn(
    kernel: Kernel,
    rx: Inbox,
    ctx: SequencerContext,
    capture: Option<CaptureWriter>,
    supervisor: Option<Supervisor>,
    alarms: Option<AlarmRuntime>,
    persist: Option<Persist>,
) -> std::io::Result<(thread::JoinHandle<Kernel>, oneshot::Receiver<()>)> {
    let (alive, stopped) = oneshot::channel::<()>();
    let handle = thread::Builder::new().name("scada-sequencer".into()).spawn(move || {
        let _alive = alive;
        run(kernel, rx, ctx, capture, supervisor, alarms, persist)
    })?;
    Ok((handle, stopped))
}

/// Runtime-only state of the sequencer (never part of the kernel state or digest).
struct Links {
    next_generation: u64,
    /// D5 capture of every input handed to `apply` (diagnostics only).
    capture: Option<CaptureWriter>,
    handled: u64,
    /// Per configured source (config order): active link generation and its takeover signal.
    active: Vec<Option<(u64, oneshot::Sender<()>)>>,
    /// VS-004: `None` unless a source is supervised.
    supervisor: Option<Supervisor>,
    /// VS-005: `None` unless alarms are configured.
    alarms: Option<AlarmRuntime>,
    /// VS-006: `None` unless `[persistence]` is configured (J10).
    persist: Option<Persist>,
}

fn run(
    mut kernel: Kernel,
    mut rx: Inbox,
    ctx: SequencerContext,
    capture: Option<CaptureWriter>,
    supervisor: Option<Supervisor>,
    alarms: Option<AlarmRuntime>,
    persist: Option<Persist>,
) -> Kernel {
    // VS-006: link generations continue after a restore (never reuse one of an earlier boot).
    let next_generation = kernel
        .logical_snapshot()
        .sources
        .iter()
        .map(|s| match s.link {
            LinkState::NeverConnected => 0,
            LinkState::Connected { generation } | LinkState::Disconnected { generation } => generation,
        })
        .max()
        .unwrap_or(0);
    let mut links = Links { next_generation, capture, handled: 0, active: Vec::new(), supervisor, alarms, persist };
    links.active.resize_with(kernel.config().sources().len(), || None);
    // VS-006 (ADR 0013 D8 step 6): after a restore, the watermark up to the durable prefix
    // and the recovery fence are the first operations of this boot, journaled like any
    // other, before any input is served.
    if links.persist.is_some() {
        journal_tick(&mut kernel, &mut links, &ctx);
        if let Some(p) = links.persist.as_ref()
            && kernel.last_operation_sequence() > 0
        {
            let boot_generation = p.boot_generation;
            if let Some(operation_sequence) = kernel.next_operation_sequence() {
                let op = Operation::RecoveryFence { boot_generation };
                let r = apply(
                    &mut kernel,
                    &mut links,
                    SequencedInput { operation_sequence, operation: op },
                    Extras::default(),
                );
                if let Err(e) = r {
                    eprintln!("sequencer: recovery fence not applied: {e:?}");
                }
            }
        }
        if let Some(p) = links.persist.as_ref() {
            p.public.recovering.store(false, Ordering::Relaxed);
        }
    }
    // One request per wake-up, in queue order. A bounded drain (`blocking_recv_many`,
    // N = 4/16/64) was measured in VS-002 and removed: no repeatable effect on throughput
    // or on the rare source's latency (ADR 0005).
    while let Some(req) = rx.blocking_recv() {
        handle(&mut kernel, &mut links, req, &ctx);
        // VS-004: a due tick is served after the request in progress, whatever the queue
        // holds (one relaxed load per request when supervision is on, nothing otherwise).
        if let Some(w) = &ctx.watchdog
            && let Some(raised) = w.take()
        {
            watchdog_tick(&mut kernel, &mut links, w, raised, &ctx);
        }
        // VS-005: link transitions refused for lack of outbox slots are retried, in their
        // original order, as soon as slots were released (never applied out of order with
        // respect to inputs that were already applied: each retry is a new operation).
        if let Some(a) = links.alarms.as_mut()
            && a.released
        {
            a.released = false;
            retry_pending_disconnects(&mut kernel, &mut links, &ctx);
        }
        // VS-006: durable progress → watermark (J3); journal-deferred disconnections.
        if links.persist.is_some() {
            journal_tick(&mut kernel, &mut links, &ctx);
        }
    }
    kernel
}

/// VS-006: advance the outbox watermark to the durable prefix published by the writer
/// (never beyond, J3) and retry disconnections the journal refused. Cheap when nothing
/// changed (two relaxed loads).
fn journal_tick(kernel: &mut Kernel, links: &mut Links, ctx: &SequencerContext) {
    let Some(p) = links.persist.as_ref() else { return };
    if let Some(up_to) = p.watermark_due(kernel)
        && let Some(operation_sequence) = kernel.next_operation_sequence()
    {
        let op = Operation::AdvanceDurableWatermark { up_to_event_seq: up_to };
        let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, Extras::default());
        if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
            a.after_apply(kernel, &rc, Origin::Other, &ctx.counters);
        }
    }
    let Some(p) = links.persist.as_mut() else { return };
    if p.deferred_disconnects.is_empty() {
        return;
    }
    let pending = std::mem::take(&mut p.deferred_disconnects);
    for (source_id, link_generation) in pending {
        let Some(operation_sequence) = kernel.next_operation_sequence() else { return };
        let op = Operation::SourceDisconnected { source_id, link_generation };
        let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, Extras::default());
        match r {
            Err(Refused::Journal(_)) => {
                if let Some(p) = links.persist.as_mut() {
                    p.deferred_disconnects.push((source_id, link_generation));
                }
            }
            Err(Refused::Core(scada_core::ApplyError::OutboxCapacityExceeded { .. })) => {
                if let Some(a) = links.alarms.as_mut() {
                    a.defer_disconnect(source_id, link_generation);
                }
            }
            _ => {}
        }
        if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
            a.after_apply(kernel, &rc, Origin::Link, &ctx.counters);
        }
    }
}

fn retry_pending_disconnects(kernel: &mut Kernel, links: &mut Links, ctx: &SequencerContext) {
    let mut i = 0;
    while let Some(&(source_id, link_generation)) = links.alarms.as_ref().and_then(|a| a.pending_disconnects.get(i)) {
        let Some(operation_sequence) = kernel.next_operation_sequence() else { return };
        let op = Operation::SourceDisconnected { source_id, link_generation };
        let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, Extras::default());
        let Some(a) = links.alarms.as_mut() else { return };
        if matches!(
            r,
            Err(Refused::Core(scada_core::ApplyError::OutboxCapacityExceeded { .. })) | Err(Refused::Journal(_))
        ) {
            i += 1;
        } else {
            a.pending_disconnects.remove(i);
        }
        if let Some(rc) = core_result(&r) {
            a.after_apply(kernel, &rc, Origin::Link, &ctx.counters);
        }
    }
}

/// VS-004: one supervision tick on the sequencer thread. The only clock read of the
/// supervision path; every decision becomes a sequenced, captured operation.
fn watchdog_tick(kernel: &mut Kernel, links: &mut Links, w: &WatchdogShared, raised: Duration, ctx: &SequencerContext) {
    let now = w.now();
    let late = now.saturating_sub(raised);
    let late_ns = u64::try_from(late.as_nanos()).unwrap_or(u64::MAX);
    ctx.counters.watchdog_max_late_ns.fetch_max(late_ns, Ordering::Relaxed);
    if late > w.poll() {
        Counters::inc(&ctx.counters.watchdog_ticks_late);
    }
    if let Some(d) = &ctx.diag {
        d.record_ns(Stage::WatchdogLateness, late_ns);
    }
    let Some(mut sup) = links.supervisor.take() else { return };
    let counters = &ctx.counters;
    let mut apply_one = |k: &mut Kernel, operation: Operation| -> Option<TransitionOutcome> {
        let operation_sequence = k.next_operation_sequence()?;
        let r = apply(k, links, SequencedInput { operation_sequence, operation }, Extras::default());
        if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
            a.after_apply(k, &rc, Origin::Supervision, counters);
        }
        match r {
            Ok(o) => Some(o),
            // VS-005: refused for lack of outbox slots: nothing applied, the next tick
            // decides again (the expiry stays pending; never invented). Counted, and the
            // overload state is reported once on stderr. VS-006: same for a journal
            // refusal (J4).
            Err(Refused::Core(scada_core::ApplyError::OutboxCapacityExceeded { .. })) | Err(Refused::Journal(_)) => {
                None
            }
            Err(Refused::Core(e)) => {
                eprintln!("sequencer: supervision operation rejected: {e}");
                None
            }
        }
    };
    sup.tick(now, kernel, &mut apply_one, &ctx.counters);
    links.supervisor = Some(sup);
}

/// The only place where the sequencer calls `Kernel::apply` (the VS-005 bookkeeping is
/// done by the callers). VS-006: with persistence, journal room is reserved **before**
/// `apply` (J4), the time envelope is sampled once, and every applied operation (zero
/// events included) is handed to the writer; a refusal changes nothing.
fn apply(
    kernel: &mut Kernel,
    links: &mut Links,
    input: SequencedInput,
    x: Extras,
) -> Result<TransitionOutcome, Refused> {
    let Some(p) = links.persist.as_mut() else {
        if let Some(c) = &mut links.capture {
            c.record(&input);
        }
        return kernel.apply(input).map_err(Refused::Core);
    };
    fault::hit("before_apply");
    let internal = matches!(input.operation, Operation::AdvanceDurableWatermark { .. });
    let est = p.estimate(kernel, &input.operation, &x);
    let res = p.admit(est, internal).map_err(Refused::Journal)?;
    if let Some(c) = &mut links.capture {
        c.record(&input);
    }
    let (env, anomaly) = p.time.sample();
    let ev_before = kernel.outbox_status().map_or(1, |s| s.next_event_seq);
    match kernel.apply_ref(&input) {
        Ok(o @ TransitionOutcome::Applied { kind, .. }) => {
            p.time.accept(&env, anomaly.as_ref());
            p.record(kernel, input, &kind, env, anomaly, ev_before, res, x);
            Ok(o)
        }
        other => {
            p.cancel(res);
            other.map_err(Refused::Core)
        }
    }
}

fn handle(kernel: &mut Kernel, links: &mut Links, req: Request, ctx: &SequencerContext) {
    links.handled = links.handled.saturating_add(1);
    if ctx.fail_after.is_some_and(|n| links.handled > n) {
        panic!("--test-fail-sequencer-after reached: simulated sequencer failure");
    }
    match req {
        Request::Connect { source_id, source_epoch, reply, superseded, heartbeats } => {
            let r = connect(kernel, links, source_id, source_epoch, &ctx.counters);
            if let ConnectReply::Welcome { link_generation, .. } = r
                && let Some(i) = kernel.config().source_index(source_id)
            {
                if let Some(sup) = links.supervisor.as_mut() {
                    sup.on_connected(i, link_generation, heartbeats, kernel);
                }
                if let Some(slot) = links.active.get_mut(i)
                    && let Some((_, old)) = slot.replace((link_generation, superseded))
                {
                    // D3: tell the previous link of this source to close. Its own
                    // Disconnect stays queued after its batches and is ignored as stale.
                    let _ = old.send(());
                }
            }
            // A dropped receiver means the session died; its link will be disconnected
            // by that session's cleanup, which is always queued after this request.
            let _ = reply.send(r);
        }
        Request::Disconnect { source_id, link_generation } => {
            if let Some(slot) = kernel.config().source_index(source_id).and_then(|i| links.active.get_mut(i))
                && slot.as_ref().is_some_and(|(g, _)| *g == link_generation)
            {
                *slot = None;
            }
            let Some(operation_sequence) = kernel.next_operation_sequence() else {
                eprintln!("sequencer: operation sequence exhausted; disconnect of {source_id} not applied");
                return;
            };
            let op = Operation::SourceDisconnected { source_id, link_generation };
            let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, Extras::default());
            match &r {
                Err(Refused::Journal(why)) => {
                    // VS-006: deferred like an outbox refusal; retried after durable progress.
                    eprintln!(
                        "sequencer: disconnect of source {source_id} gen {link_generation} deferred: {}",
                        why.name()
                    );
                    if let Some(p) = links.persist.as_mut() {
                        p.deferred_disconnects.retain(|&(s, g)| s != source_id || g > link_generation);
                        p.deferred_disconnects.push((source_id, link_generation));
                    }
                }
                Err(Refused::Core(scada_core::ApplyError::OutboxCapacityExceeded { required, free })) => {
                    // VS-005: not applied (atomicity); the link stays Connected in the kernel
                    // and its tags keep their quality until slots are released. Retried
                    // then; reported as CRITICAL meanwhile.
                    eprintln!(
                        "sequencer: disconnect of source {source_id} gen {link_generation} deferred: outbox full (required {required}, free {free})"
                    );
                    if let Some(a) = links.alarms.as_mut() {
                        a.defer_disconnect(source_id, link_generation);
                    }
                }
                Err(Refused::Core(e)) => {
                    eprintln!("sequencer: disconnect of source {source_id} gen {link_generation} rejected: {e}")
                }
                Ok(_) => {}
            }
            if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
                a.after_apply(kernel, &rc, Origin::Link, &ctx.counters);
            }
        }
        Request::JournalWake => {
            if let Some(p) = links.persist.as_ref() {
                p.wake_queued.store(false, Ordering::Relaxed);
            }
        }
        Request::Batch { batch, reply, submitted, permits, ext } => {
            let (times, received_at, durable) = match ext {
                Some(e) => {
                    let BatchExt { times, received_at, durable } = *e;
                    (times, Some(received_at), durable)
                }
                None => (None, None, false),
            };
            let d = ctx.diag.as_deref();
            let probe = d.is_some_and(|d| d.is_probe(batch.source_id.0));
            if let (Some(d), Some(t)) = (d, submitted) {
                d.since(Stage::QueueWait, t);
                if probe {
                    d.since(Stage::ProbeQueueWait, t);
                }
            }
            let t0 = diag::now(d);
            let x = Extras { source_times: times, received_at, action: None, hold: durable };
            let source_id = batch.source_id;
            let mut r = apply_batch(kernel, links, batch, &ctx.counters, x);
            if let (Some(d), Some(t0)) = (d, t0) {
                d.since(Stage::Apply, t0);
                r.sent = Some(Instant::now());
            }
            if durable {
                durable_batch_reply(kernel, links, source_id, r, reply);
            } else {
                // ACK only after `apply` returned; if the session is gone the ACK is moot.
                let _ = reply.send(r);
            }
            drop(permits); // VS-003: the batch is applied, its admission slots are free
        }
        Request::Introspect { include_tags, reset_diagnostics, reply } => {
            // Everything applied before this answer is on disk for a test that replays it.
            if let Some(c) = &mut links.capture {
                c.flush();
            }
            let _ = reply.send(introspect(kernel, include_tags, ctx, links.alarms.as_ref(), links.persist.as_ref()));
            if reset_diagnostics && let Some(d) = &ctx.diag {
                d.reset();
            }
        }
        Request::Tick => {
            if let Some(w) = &ctx.watchdog {
                w.woken();
            }
        }
        Request::Alarm { call, reply } => alarm_call(kernel, links, call, ctx, reply),
    }
    if !ctx.throttle.is_zero() {
        thread::sleep(ctx.throttle);
    }
}

/// VS-005: one control-plane request, at the current position of the sequence. The reply
/// is sent here, or later by the journal writer for a durable acknowledgement (VS-006).
fn alarm_call(
    kernel: &mut Kernel,
    links: &mut Links,
    call: AlarmCall,
    ctx: &SequencerContext,
    reply: oneshot::Sender<AlarmReply>,
) {
    if links.alarms.is_none() {
        let _ = reply.send(AlarmReply::Unsupported("alarms are not configured"));
        return;
    }
    let page = |limit: u32| {
        let max = ctx.alarm_page_max as usize;
        if limit == 0 { max } else { (limit as usize).min(max) }
    };
    let r = match call {
        AlarmCall::Snapshot { after, limit, expected_change, mut buf } => {
            let current = kernel.outbox_status().map_or(0, |s| s.last_alarm_change_operation);
            let operation_sequence = kernel.last_operation_sequence();
            if expected_change != 0 && expected_change != current {
                buf.clear();
                AlarmReply::Snapshot { page: Err(current), buf, operation_sequence }
            } else {
                match kernel.alarm_snapshot_page(after, page(limit), &mut buf) {
                    Some(p) => AlarmReply::Snapshot { page: Ok(p), buf, operation_sequence },
                    None => AlarmReply::Unsupported("alarms are not configured"),
                }
            }
        }
        AlarmCall::Events { after, limit, mut buf } => match kernel.read_alarm_events(after, page(limit), &mut buf) {
            Some(p) => AlarmReply::Events { page: p, buf },
            None => AlarmReply::Unsupported("alarms are not configured"),
        },
        AlarmCall::Ack { alarm_id, token, actor_uid, action_id, note, require_durable } => {
            let a = AckArgs { alarm_id, token, actor_uid, action_id, note, require_durable };
            ack(kernel, links, ctx, a, reply);
            return;
        }
        AlarmCall::Capacity => {
            let Some(outbox) = kernel.outbox_status() else {
                let _ = reply.send(AlarmReply::Unsupported("alarms are not configured"));
                return;
            };
            let c = &ctx.counters;
            let ld = |x: &AtomicU64| x.load(Ordering::Relaxed);
            let t = kernel.config().alarm_table();
            let Some(a) = links.alarms.as_ref() else { return };
            AlarmReply::Capacity(CapacityReport {
                outbox,
                overload: a.overload(&outbox),
                simulated_sink: a.simulated_sink,
                journal: links.persist.as_ref().map(|p| p.durable),
                rejected_batches: ld(&c.outbox_rejected_batches),
                rejected_link: ld(&c.outbox_rejected_link),
                rejected_supervision: ld(&c.outbox_rejected_supervision),
                rejected_acks: ld(&c.outbox_rejected_acks),
                pending_link_transitions: a.pending_disconnects.len() as u32,
                rules: t.map_or(0, |t| t.len() as u32),
                worst_fanout: t.map_or(0, |t| t.worst_fanout().0),
                memory_bound: a.memory_bound,
                high_percent: a.high_percent,
                operation_sequence: kernel.last_operation_sequence(),
            })
        }
        AlarmCall::SimulatedDurableCommit { up_to } => {
            if !links.alarms.as_ref().is_some_and(|a| a.simulated_sink) {
                let _ = reply.send(AlarmReply::Unsupported(
                    "no simulated sink; it exists only with --test-simulated-durable-sink (never with [persistence])",
                ));
                return;
            }
            let Some(operation_sequence) = kernel.next_operation_sequence() else {
                let _ = reply.send(AlarmReply::Watermark(Err("operation sequence exhausted".into())));
                return;
            };
            let op = Operation::AdvanceDurableWatermark { up_to_event_seq: up_to };
            let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, Extras::default());
            if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
                a.after_apply(kernel, &rc, Origin::Other, &ctx.counters);
            }
            let wm = kernel.outbox_status().map_or(0, |s| s.durable_watermark);
            match r {
                Ok(TransitionOutcome::Applied {
                    operation_sequence,
                    kind: AppliedKind::DurableWatermarkAdvanced { released, .. },
                }) => AlarmReply::Watermark(Ok((wm, released, Some(operation_sequence)))),
                Ok(_) => AlarmReply::Watermark(Ok((wm, 0, None))),
                Err(Refused::Core(e)) => AlarmReply::Watermark(Err(e.to_string())),
                Err(Refused::Journal(j)) => AlarmReply::Watermark(Err(j.name().into())),
            }
        }
    };
    let _ = reply.send(r);
}

/// Arguments of an acknowledgement.
struct AckArgs {
    alarm_id: scada_core::AlarmId,
    token: u64,
    actor_uid: u32,
    action_id: Option<[u8; 16]>,
    note: String,
    require_durable: bool,
}

/// A receipt that answers the control session once the commit is known.
fn ack_receipt(
    reply: oneshot::Sender<AlarmReply>,
    operation_sequence: u64,
    event_seq: u64,
    deduplicated: bool,
) -> scada_journal::Receipt {
    Box::new(move |c| {
        let _ = reply.send(AlarmReply::Ack(match c {
            CommitResult::Durable => AckOutcome::Applied { operation_sequence, event_seq, durable: true, deduplicated },
            CommitResult::NotDurable => AckOutcome::NotDurable { operation_sequence, event_seq },
        }));
    })
}

/// VS-005 acknowledgement, VS-006 idempotency key and durable outcome (ADR 0013 D11).
/// Answers `reply` now, or from the journal writer once the operation is durable.
fn ack(kernel: &mut Kernel, links: &mut Links, ctx: &SequencerContext, a: AckArgs, reply: oneshot::Sender<AlarmReply>) {
    let AckArgs { alarm_id, token, actor_uid, action_id, note, require_durable } = a;
    let now = |reply: oneshot::Sender<AlarmReply>, o: AckOutcome| {
        let _ = reply.send(AlarmReply::Ack(o));
    };
    let durable_store = links.persist.as_ref().is_some_and(|p| p.durable);
    if require_durable && !durable_store {
        return now(reply, AckOutcome::DurabilityUnavailable);
    }
    // Retry of a known action id: same outcome, no new event.
    if let (Some(id), Some(p)) = (action_id, links.persist.as_ref())
        && let Some((conflict, c)) = p.conflict(&id, alarm_id, token)
    {
        if conflict {
            return now(reply, AckOutcome::ActionIdConflict);
        }
        let (op, ev) = (c.operation_seq, c.event_seq.unwrap_or(0));
        let durable_now = durable_store && p.shared.durable_op.load(Ordering::Acquire) >= op;
        if durable_now || !require_durable {
            return now(
                reply,
                AckOutcome::Applied { operation_sequence: op, event_seq: ev, durable: durable_now, deduplicated: true },
            );
        }
        if let Err((j, _)) = p.notify(op, ack_receipt(reply, op, ev, true)) {
            // The receipt (and its reply sender) was dropped: the session sees the
            // channel closed. Count it; the client retries with the same action id.
            eprintln!("sequencer: durable ACK retry not queued: {}", j.name());
        }
        return;
    }
    let Some(operation_sequence) = kernel.next_operation_sequence() else {
        return now(reply, AckOutcome::SequenceExhausted);
    };
    let op = Operation::AlarmAcknowledge { alarm_id, expected_activation_token: token, actor_uid };
    let x = Extras {
        action: action_id.map(|id| OperatorAction {
            action_id: id,
            alarm_id: alarm_id.0,
            requested_token: token,
            actor_uid,
            note,
            outcome: 0,
            event_seq: None,
            occurrence_id: 0,
        }),
        hold: require_durable,
        ..Extras::default()
    };
    let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, x);
    if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
        a.after_apply(kernel, &rc, Origin::Ack, &ctx.counters);
    }
    let o = match &r {
        Err(Refused::Journal(j)) => AckOutcome::JournalUnavailable(*j),
        Ok(o) => crate::alarms::ack_outcome(&Ok(*o)),
        Err(Refused::Core(e)) => crate::alarms::ack_outcome(&Err(*e)),
    };
    if let AckOutcome::Applied { operation_sequence, event_seq, .. } = o {
        Counters::inc(&ctx.counters.alarm_acks_applied);
        if require_durable && let Some(p) = links.persist.as_mut() {
            p.release_held(Some(ack_receipt(reply, operation_sequence, event_seq, false)));
            return;
        }
    } else if let Some(p) = links.persist.as_mut() {
        p.release_held(None);
    }
    now(reply, o)
}

/// VS-006: reply to a batch of a link that negotiated durable receipts.
fn durable_batch_reply(
    kernel: &Kernel,
    links: &mut Links,
    source_id: SourceId,
    r: BatchReply,
    reply: oneshot::Sender<BatchReply>,
) {
    let Some(p) = links.persist.as_mut() else {
        let _ = reply.send(r);
        return;
    };
    match r.status {
        AckStatus::AppliedVolatile => {
            let mut r = r;
            p.release_held(Some(Box::new(move |c| {
                r.status = match c {
                    CommitResult::Durable => AckStatus::AppliedDurable,
                    CommitResult::NotDurable => AckStatus::AppliedNotDurable,
                };
                let _ = reply.send(r);
            })));
        }
        AckStatus::Duplicate => {
            p.release_held(None);
            // The original may still be in flight: confirm the duplicate once the operation
            // that accepted it is durable.
            let target = kernel.source_status(source_id).map_or(0, |s| s.last_batch_operation);
            let mut d = r;
            let res = p.notify(
                target,
                Box::new(move |c| {
                    if c == CommitResult::NotDurable {
                        d.status = AckStatus::Rejected;
                        d.error_code = ErrorCode::JournalFault;
                    }
                    let _ = reply.send(d);
                }),
            );
            if let Err((_, receipt)) = res {
                receipt(CommitResult::NotDurable);
            }
        }
        _ => {
            p.release_held(None);
            let _ = reply.send(r);
        }
    }
}

fn connect(
    kernel: &mut Kernel,
    links: &mut Links,
    source_id: SourceId,
    source_epoch: u64,
    counters: &Counters,
) -> ConnectReply {
    let Some(operation_sequence) = kernel.next_operation_sequence() else {
        return ConnectReply::Rejected(ErrorCode::OperationSequenceExhausted);
    };
    let Some(link_generation) = links.next_generation.checked_add(1) else {
        return ConnectReply::Rejected(ErrorCode::Internal);
    };
    let op = Operation::SourceConnected { source_id, link_generation, source_epoch };
    let r = apply(kernel, links, SequencedInput { operation_sequence, operation: op }, Extras::default());
    if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&r)) {
        a.after_apply(kernel, &rc, Origin::Link, counters);
    }
    match r {
        Ok(TransitionOutcome::Applied {
            kind: AppliedKind::Connected { full_state_required, last_accepted_source_sequence, .. },
            ..
        }) => {
            links.next_generation = link_generation;
            ConnectReply::Welcome { link_generation, full_state_required, last_accepted: last_accepted_source_sequence }
        }
        Ok(other) => {
            eprintln!("sequencer: unexpected connect outcome {other:?}");
            ConnectReply::Rejected(ErrorCode::Internal)
        }
        Err(Refused::Core(e)) => ConnectReply::Rejected(convert::error_code(&e)),
        Err(Refused::Journal(j)) => ConnectReply::Rejected(j.driver_code()),
    }
}

fn apply_batch(
    kernel: &mut Kernel,
    links: &mut Links,
    batch: ObservationBatch,
    counters: &Counters,
    x: Extras,
) -> BatchReply {
    let source_id = batch.source_id;
    let last = |k: &Kernel| k.source_status(source_id).and_then(|s| s.last_accepted_source_sequence);
    let Some(operation_sequence) = kernel.next_operation_sequence() else {
        Counters::inc(&counters.batches_rejected);
        return BatchReply {
            status: AckStatus::Rejected,
            operation_sequence: None,
            error_code: ErrorCode::OperationSequenceExhausted,
            continuity: None,
            last_accepted: last(kernel),
            sent: None,
        };
    };
    let outcome =
        apply(kernel, links, SequencedInput { operation_sequence, operation: Operation::ObservationBatch(batch) }, x);
    if let (Some(a), Some(rc)) = (links.alarms.as_mut(), core_result(&outcome)) {
        a.after_apply(kernel, &rc, Origin::Batch, counters);
    }
    match outcome {
        Ok(TransitionOutcome::Applied { operation_sequence, kind }) => {
            Counters::inc(&counters.batches_applied);
            let continuity = match kind {
                AppliedKind::Batch { continuity, .. } => Some(convert::continuity(continuity)),
                _ => None,
            };
            BatchReply {
                status: AckStatus::AppliedVolatile,
                operation_sequence: Some(operation_sequence),
                error_code: ErrorCode::Unspecified,
                continuity,
                last_accepted: last(kernel),
                sent: None,
            }
        }
        Ok(TransitionOutcome::Duplicate { .. }) => {
            Counters::inc(&counters.batches_duplicate);
            BatchReply {
                status: AckStatus::Duplicate,
                operation_sequence: None,
                error_code: ErrorCode::Unspecified,
                continuity: None,
                last_accepted: last(kernel),
                sent: None,
            }
        }
        Ok(TransitionOutcome::Ignored(_)) => {
            Counters::inc(&counters.batches_rejected);
            BatchReply {
                status: AckStatus::Rejected,
                operation_sequence: None,
                error_code: ErrorCode::Internal,
                continuity: None,
                last_accepted: last(kernel),
                sent: None,
            }
        }
        Err(e) => {
            Counters::inc(&counters.batches_rejected);
            BatchReply {
                status: AckStatus::Rejected,
                operation_sequence: None,
                error_code: match e {
                    Refused::Core(e) => convert::error_code(&e),
                    Refused::Journal(j) => j.driver_code(),
                },
                continuity: None,
                last_accepted: last(kernel),
                sent: None,
            }
        }
    }
}

fn introspect(
    kernel: &Kernel,
    include_tags: bool,
    ctx: &SequencerContext,
    alarms: Option<&AlarmRuntime>,
    persist: Option<&Persist>,
) -> ipb::IntrospectResponse {
    let snap = kernel.logical_snapshot();
    let c = &ctx.counters;
    let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
    ipb::IntrospectResponse {
        kernel_instance_id: ctx.kernel_instance_id.to_vec(),
        state_digest: kernel.state_digest().to_vec(),
        state_digest_version: u32::from(kernel.state_digest_version()),
        last_operation_sequence: snap.last_operation_sequence,
        config_fingerprint: snap.config_fingerprint.to_vec(),
        tags: if include_tags {
            snap.tags
                .iter()
                .map(|t| ipb::TagState {
                    tag_id: t.tag_id.0,
                    value: t.value.map(|v| match v {
                        Value::Bool(b) => ipb::tag_state::Value::BoolValue(b),
                        Value::I64(i) => ipb::tag_state::Value::I64Value(i),
                    }),
                    quality: match t.quality {
                        Quality::Unknown => ipb::Quality::Unknown,
                        Quality::Good => ipb::Quality::Good,
                        Quality::Uncertain(UncertainReason::SourceUnavailable) => {
                            ipb::Quality::UncertainSourceUnavailable
                        }
                        Quality::Uncertain(UncertainReason::SourceSilent) => ipb::Quality::UncertainSourceSilent,
                        Quality::Uncertain(UncertainReason::StaleData) => ipb::Quality::UncertainStaleData,
                        Quality::Uncertain(UncertainReason::RecoveredUnverified) => {
                            ipb::Quality::UncertainRecoveredUnverified
                        }
                    } as i32,
                    revision: t.revision,
                    last_observed_operation: t.last_observed_operation,
                })
                .collect()
        } else {
            Vec::new()
        },
        sources: snap
            .sources
            .iter()
            .map(|s| {
                let (state, generation) = match s.link {
                    LinkState::NeverConnected => (ipb::LinkState::NeverConnected, 0),
                    LinkState::Connected { generation } => (ipb::LinkState::Connected, generation),
                    LinkState::Disconnected { generation } => (ipb::LinkState::Disconnected, generation),
                };
                let liveness = match (s.supervision.liveness, s.link, s.silent) {
                    (false, _, _) => ipb::Liveness::NotSupervised,
                    (true, LinkState::Connected { .. }, true) => ipb::Liveness::Silent,
                    (true, LinkState::Connected { .. }, false) => ipb::Liveness::Alive,
                    (true, _, _) => ipb::Liveness::NoLink,
                };
                ipb::SourceState {
                    source_id: s.source_id.0,
                    link_state: state as i32,
                    link_generation: generation,
                    source_epoch: s.epoch,
                    last_accepted_source_sequence: s.last_accepted_source_sequence,
                    liveness: liveness as i32,
                    last_batch_operation: s.last_batch_operation,
                    freshness_supervised: s.supervision.freshness,
                    unverified: s.unverified,
                }
            })
            .collect(),
        counters: Some(ipb::RuntimeCounters {
            connections_accepted: ld(&c.connections_accepted),
            connections_refused_limit: ld(&c.connections_refused_limit),
            handshakes_rejected: ld(&c.handshakes_rejected),
            protocol_errors: ld(&c.protocol_errors),
            batches_applied: ld(&c.batches_applied),
            batches_duplicate: ld(&c.batches_duplicate),
            batches_rejected: ld(&c.batches_rejected),
            input_queue_full_waits: ld(&c.input_queue_full_waits),
            input_queue_capacity: ctx.input_queue_capacity,
            active_connections: u32::try_from(ld(&c.active_connections)).unwrap_or(u32::MAX),
            ack_write_timeouts: ld(&c.ack_write_timeouts),
            links_superseded: ld(&c.links_superseded),
            heartbeats_valid: ld(&c.heartbeats_valid),
            heartbeats_invalid: ld(&c.heartbeats_invalid),
            heartbeats_coalesced: ld(&c.heartbeats_coalesced),
            liveness_expiries: ld(&c.liveness_expiries),
            liveness_recoveries: ld(&c.liveness_recoveries),
            freshness_expiries: ld(&c.freshness_expiries),
            freshness_tags_expired: ld(&c.freshness_tags_expired),
            watchdog_ticks: ld(&c.watchdog_ticks),
            watchdog_ticks_coalesced: ld(&c.watchdog_ticks_coalesced),
            watchdog_ticks_late: ld(&c.watchdog_ticks_late),
            watchdog_max_late_ns: ld(&c.watchdog_max_late_ns),
            liveness_recoveries_by_batch: ld(&c.liveness_recoveries_by_batch),
        }),
        stages: ctx
            .diag
            .as_deref()
            .map(|d| {
                d.summaries()
                    .map(|(s, m)| ipb::StageStats {
                        name: s.name().to_owned(),
                        count: m.count,
                        sum_ns: m.sum_ns,
                        p50_ns: m.p50_ns,
                        p99_ns: m.p99_ns,
                        p999_ns: m.p999_ns,
                        max_ns: m.max_ns,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        scheduler: ctx.scheduler.clone(),
        alarms: match (kernel.outbox_status(), alarms) {
            (Some(o), Some(a)) => Some(ipb::AlarmSummary {
                rules: kernel.config().alarm_table().map_or(0, |t| t.len() as u32),
                active_or_unacked: o.active_or_unacked,
                outbox_capacity: o.capacity,
                outbox_used: o.used,
                next_event_seq: o.next_event_seq,
                durable_watermark: o.durable_watermark,
                outbox_high_water: o.high_water,
                overload_state: a.overload(&o) as u32,
                outbox_rejections: ld(&c.outbox_rejected_batches)
                    + ld(&c.outbox_rejected_link)
                    + ld(&c.outbox_rejected_supervision)
                    + ld(&c.outbox_rejected_acks),
                pending_link_transitions: a.pending_disconnects.len() as u32,
                acknowledgements_applied: ld(&c.alarm_acks_applied),
                events_digest: if include_tags {
                    kernel.alarm_events_digest().map_or_else(Vec::new, |d| d.to_vec())
                } else {
                    Vec::new()
                },
            }),
            _ => None,
        },
        persistence: persist.map(|p| {
            let sh = &p.shared;
            ipb::PersistenceSummary {
                timeline_id: p.timeline_id.to_vec(),
                boot_generation: p.boot_generation,
                durable_operation_sequence: sh.durable_op.load(Ordering::Acquire),
                durable_event_sequence: sh.durable_event.load(Ordering::Acquire),
                backlog_operations: sh.reserved_ops.load(Ordering::Relaxed),
                journal_state: sh.state() as u32,
                refused_queue_full: ld(&sh.refused_queue_full),
                refused_disk_full: ld(&sh.refused_disk_full),
                refused_fault: ld(&sh.refused_fault),
                recovered_operation_sequence: p.recovered_op,
                checkpoints_written: ld(&sh.checkpoints_written),
            }
        }),
    }
}
