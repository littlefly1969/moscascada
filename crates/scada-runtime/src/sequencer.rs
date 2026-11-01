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

use crate::alarms::{AlarmCall, AlarmReply, AlarmRuntime, CapacityReport, Origin};
use crate::capture::CaptureWriter;
use crate::convert;
use crate::diag::{self, Diag, Stage};
use crate::supervision::{HeartbeatCounter, Supervisor, WatchdogShared};

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
) -> std::io::Result<(thread::JoinHandle<Kernel>, oneshot::Receiver<()>)> {
    let (alive, stopped) = oneshot::channel::<()>();
    let handle = thread::Builder::new().name("scada-sequencer".into()).spawn(move || {
        let _alive = alive;
        run(kernel, rx, ctx, capture, supervisor, alarms)
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
}

fn run(
    mut kernel: Kernel,
    mut rx: Inbox,
    ctx: SequencerContext,
    capture: Option<CaptureWriter>,
    supervisor: Option<Supervisor>,
    alarms: Option<AlarmRuntime>,
) -> Kernel {
    let mut links = Links { next_generation: 0, capture, handled: 0, active: Vec::new(), supervisor, alarms };
    links.active.resize_with(kernel.config().sources().len(), || None);
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
    }
    kernel
}

fn retry_pending_disconnects(kernel: &mut Kernel, links: &mut Links, ctx: &SequencerContext) {
    let Some(a) = links.alarms.as_mut() else { return };
    let mut i = 0;
    while let Some(&(source_id, link_generation)) = a.pending_disconnects.get(i) {
        let Some(operation_sequence) = kernel.next_operation_sequence() else { return };
        let op = Operation::SourceDisconnected { source_id, link_generation };
        let r = apply(kernel, &mut links.capture, None, SequencedInput { operation_sequence, operation: op });
        if matches!(r, Err(scada_core::ApplyError::OutboxCapacityExceeded { .. })) {
            i += 1;
        } else {
            a.pending_disconnects.remove(i);
        }
        a.after_apply(kernel, &r, Origin::Link, &ctx.counters);
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
    let Some(sup) = links.supervisor.as_mut() else { return };
    let capture = &mut links.capture;
    let alarms = &mut links.alarms;
    let counters = &ctx.counters;
    let mut apply_one = |k: &mut Kernel, operation: Operation| -> Option<TransitionOutcome> {
        let operation_sequence = k.next_operation_sequence()?;
        let r = apply(k, capture, None, SequencedInput { operation_sequence, operation });
        if let Some(a) = alarms.as_mut() {
            a.after_apply(k, &r, Origin::Supervision, counters);
        }
        match r {
            Ok(o) => Some(o),
            // VS-005: refused for lack of outbox slots: nothing applied, the next tick
            // decides again (the expiry stays pending; never invented). Counted, and the
            // overload state is reported once on stderr.
            Err(scada_core::ApplyError::OutboxCapacityExceeded { .. }) => None,
            Err(e) => {
                eprintln!("sequencer: supervision operation rejected: {e}");
                None
            }
        }
    };
    sup.tick(now, kernel, &mut apply_one, &ctx.counters);
}

/// The only place where the sequencer calls `Kernel::apply`. (`_origin` is only a
/// readability hint at the call sites; the VS-005 bookkeeping is done by the callers.)
fn apply(
    kernel: &mut Kernel,
    capture: &mut Option<CaptureWriter>,
    _origin: Option<Origin>,
    input: SequencedInput,
) -> Result<TransitionOutcome, scada_core::ApplyError> {
    if let Some(c) = capture {
        c.record(&input);
    }
    kernel.apply(input)
}

fn handle(kernel: &mut Kernel, links: &mut Links, req: Request, ctx: &SequencerContext) {
    links.handled = links.handled.saturating_add(1);
    if ctx.fail_after.is_some_and(|n| links.handled > n) {
        panic!("--test-fail-sequencer-after reached: simulated sequencer failure");
    }
    match req {
        Request::Connect { source_id, source_epoch, reply, superseded, heartbeats } => {
            let r = connect(
                kernel,
                &mut links.capture,
                &mut links.next_generation,
                source_id,
                source_epoch,
                links.alarms.as_mut(),
                &ctx.counters,
            );
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
            let r = apply(kernel, &mut links.capture, None, SequencedInput { operation_sequence, operation: op });
            match &r {
                Err(scada_core::ApplyError::OutboxCapacityExceeded { required, free }) => {
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
                Err(e) => eprintln!("sequencer: disconnect of source {source_id} gen {link_generation} rejected: {e}"),
                Ok(_) => {}
            }
            if let Some(a) = links.alarms.as_mut() {
                a.after_apply(kernel, &r, Origin::Link, &ctx.counters);
            }
        }
        Request::Batch { batch, reply, submitted, permits } => {
            let d = ctx.diag.as_deref();
            let probe = d.is_some_and(|d| d.is_probe(batch.source_id.0));
            if let (Some(d), Some(t)) = (d, submitted) {
                d.since(Stage::QueueWait, t);
                if probe {
                    d.since(Stage::ProbeQueueWait, t);
                }
            }
            let t0 = diag::now(d);
            let mut r = apply_batch(kernel, &mut links.capture, batch, &ctx.counters, links.alarms.as_mut());
            if let (Some(d), Some(t0)) = (d, t0) {
                d.since(Stage::Apply, t0);
                r.sent = Some(Instant::now());
            }
            // ACK only after `apply` returned; if the session is gone the ACK is moot.
            let _ = reply.send(r);
            drop(permits); // VS-003: the batch is applied, its admission slots are free
        }
        Request::Introspect { include_tags, reset_diagnostics, reply } => {
            // Everything applied before this answer is on disk for a test that replays it.
            if let Some(c) = &mut links.capture {
                c.flush();
            }
            let _ = reply.send(introspect(kernel, include_tags, ctx, links.alarms.as_ref()));
            if reset_diagnostics && let Some(d) = &ctx.diag {
                d.reset();
            }
        }
        Request::Tick => {
            if let Some(w) = &ctx.watchdog {
                w.woken();
            }
        }
        Request::Alarm { call, reply } => {
            let r = alarm_call(kernel, links, call, ctx);
            let _ = reply.send(r);
        }
    }
    if !ctx.throttle.is_zero() {
        thread::sleep(ctx.throttle);
    }
}

/// VS-005: one control-plane request, at the current position of the sequence.
fn alarm_call(kernel: &mut Kernel, links: &mut Links, call: AlarmCall, ctx: &SequencerContext) -> AlarmReply {
    let Some(a) = links.alarms.as_mut() else { return AlarmReply::Unsupported("alarms are not configured") };
    let page = |limit: u32| {
        let max = ctx.alarm_page_max as usize;
        if limit == 0 { max } else { (limit as usize).min(max) }
    };
    match call {
        AlarmCall::Snapshot { after, limit, expected_change, mut buf } => {
            let current = kernel.outbox_status().map_or(0, |s| s.last_alarm_change_operation);
            let operation_sequence = kernel.last_operation_sequence();
            if expected_change != 0 && expected_change != current {
                buf.clear();
                return AlarmReply::Snapshot { page: Err(current), buf, operation_sequence };
            }
            match kernel.alarm_snapshot_page(after, page(limit), &mut buf) {
                Some(p) => AlarmReply::Snapshot { page: Ok(p), buf, operation_sequence },
                None => AlarmReply::Unsupported("alarms are not configured"),
            }
        }
        AlarmCall::Events { after, limit, mut buf } => match kernel.read_alarm_events(after, page(limit), &mut buf) {
            Some(p) => AlarmReply::Events { page: p, buf },
            None => AlarmReply::Unsupported("alarms are not configured"),
        },
        AlarmCall::Ack { alarm_id, token, actor_uid } => {
            let Some(operation_sequence) = kernel.next_operation_sequence() else {
                return AlarmReply::Ack(crate::alarms::AckOutcome::SequenceExhausted);
            };
            let op = Operation::AlarmAcknowledge { alarm_id, expected_activation_token: token, actor_uid };
            let r = apply(kernel, &mut links.capture, None, SequencedInput { operation_sequence, operation: op });
            a.after_apply(kernel, &r, Origin::Ack, &ctx.counters);
            let o = crate::alarms::ack_outcome(&r);
            if matches!(o, crate::alarms::AckOutcome::Applied { .. }) {
                Counters::inc(&ctx.counters.alarm_acks_applied);
            }
            AlarmReply::Ack(o)
        }
        AlarmCall::Capacity => {
            let Some(outbox) = kernel.outbox_status() else {
                return AlarmReply::Unsupported("alarms are not configured");
            };
            let c = &ctx.counters;
            let ld = |x: &AtomicU64| x.load(Ordering::Relaxed);
            let t = kernel.config().alarm_table();
            AlarmReply::Capacity(CapacityReport {
                outbox,
                overload: a.overload(&outbox),
                simulated_sink: a.simulated_sink,
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
            if !a.simulated_sink {
                return AlarmReply::Unsupported(
                    "no durable sink in VS-005; the simulated sink exists only with --test-simulated-durable-sink",
                );
            }
            let Some(operation_sequence) = kernel.next_operation_sequence() else {
                return AlarmReply::Watermark(Err("operation sequence exhausted".into()));
            };
            let op = Operation::AdvanceDurableWatermark { up_to_event_seq: up_to };
            let r = apply(kernel, &mut links.capture, None, SequencedInput { operation_sequence, operation: op });
            a.after_apply(kernel, &r, Origin::Other, &ctx.counters);
            let wm = kernel.outbox_status().map_or(0, |s| s.durable_watermark);
            match r {
                Ok(TransitionOutcome::Applied {
                    operation_sequence,
                    kind: AppliedKind::DurableWatermarkAdvanced { released, .. },
                }) => AlarmReply::Watermark(Ok((wm, released, Some(operation_sequence)))),
                Ok(_) => AlarmReply::Watermark(Ok((wm, 0, None))),
                Err(e) => AlarmReply::Watermark(Err(e.to_string())),
            }
        }
    }
}

fn connect(
    kernel: &mut Kernel,
    capture: &mut Option<CaptureWriter>,
    next_generation: &mut u64,
    source_id: SourceId,
    source_epoch: u64,
    alarms: Option<&mut AlarmRuntime>,
    counters: &Counters,
) -> ConnectReply {
    let Some(operation_sequence) = kernel.next_operation_sequence() else {
        return ConnectReply::Rejected(ErrorCode::OperationSequenceExhausted);
    };
    let Some(link_generation) = next_generation.checked_add(1) else {
        return ConnectReply::Rejected(ErrorCode::Internal);
    };
    let op = Operation::SourceConnected { source_id, link_generation, source_epoch };
    let r = apply(kernel, capture, None, SequencedInput { operation_sequence, operation: op });
    if let Some(a) = alarms {
        a.after_apply(kernel, &r, Origin::Link, counters);
    }
    match r {
        Ok(TransitionOutcome::Applied {
            kind: AppliedKind::Connected { full_state_required, last_accepted_source_sequence, .. },
            ..
        }) => {
            *next_generation = link_generation;
            ConnectReply::Welcome { link_generation, full_state_required, last_accepted: last_accepted_source_sequence }
        }
        Ok(other) => {
            eprintln!("sequencer: unexpected connect outcome {other:?}");
            ConnectReply::Rejected(ErrorCode::Internal)
        }
        Err(e) => ConnectReply::Rejected(convert::error_code(&e)),
    }
}

fn apply_batch(
    kernel: &mut Kernel,
    capture: &mut Option<CaptureWriter>,
    batch: ObservationBatch,
    counters: &Counters,
    alarms: Option<&mut AlarmRuntime>,
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
    let outcome = apply(
        kernel,
        capture,
        None,
        SequencedInput { operation_sequence, operation: Operation::ObservationBatch(batch) },
    );
    if let Some(a) = alarms {
        a.after_apply(kernel, &outcome, Origin::Batch, counters);
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
                error_code: convert::error_code(&e),
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
    }
}
