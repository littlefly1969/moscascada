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
) -> std::io::Result<(thread::JoinHandle<Kernel>, oneshot::Receiver<()>)> {
    let (alive, stopped) = oneshot::channel::<()>();
    let handle = thread::Builder::new().name("scada-sequencer".into()).spawn(move || {
        let _alive = alive;
        run(kernel, rx, ctx, capture, supervisor)
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
}

fn run(
    mut kernel: Kernel,
    mut rx: Inbox,
    ctx: SequencerContext,
    capture: Option<CaptureWriter>,
    supervisor: Option<Supervisor>,
) -> Kernel {
    let mut links = Links { next_generation: 0, capture, handled: 0, active: Vec::new(), supervisor };
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
    }
    kernel
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
    let mut apply_one = |k: &mut Kernel, operation: Operation| -> Option<TransitionOutcome> {
        let operation_sequence = k.next_operation_sequence()?;
        match apply(k, capture, SequencedInput { operation_sequence, operation }) {
            Ok(o) => Some(o),
            Err(e) => {
                eprintln!("sequencer: supervision operation rejected: {e}");
                None
            }
        }
    };
    sup.tick(now, kernel, &mut apply_one, &ctx.counters);
}

/// The only place where the sequencer calls `Kernel::apply`.
fn apply(
    kernel: &mut Kernel,
    capture: &mut Option<CaptureWriter>,
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
            let r = connect(kernel, &mut links.capture, &mut links.next_generation, source_id, source_epoch);
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
            if let Err(e) = apply(kernel, &mut links.capture, SequencedInput { operation_sequence, operation: op }) {
                eprintln!("sequencer: disconnect of source {source_id} gen {link_generation} rejected: {e}");
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
            let mut r = apply_batch(kernel, &mut links.capture, batch, &ctx.counters);
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
            let _ = reply.send(introspect(kernel, include_tags, ctx));
            if reset_diagnostics && let Some(d) = &ctx.diag {
                d.reset();
            }
        }
        Request::Tick => {
            if let Some(w) = &ctx.watchdog {
                w.woken();
            }
        }
    }
    if !ctx.throttle.is_zero() {
        thread::sleep(ctx.throttle);
    }
}

fn connect(
    kernel: &mut Kernel,
    capture: &mut Option<CaptureWriter>,
    next_generation: &mut u64,
    source_id: SourceId,
    source_epoch: u64,
) -> ConnectReply {
    let Some(operation_sequence) = kernel.next_operation_sequence() else {
        return ConnectReply::Rejected(ErrorCode::OperationSequenceExhausted);
    };
    let Some(link_generation) = next_generation.checked_add(1) else {
        return ConnectReply::Rejected(ErrorCode::Internal);
    };
    let op = Operation::SourceConnected { source_id, link_generation, source_epoch };
    match apply(kernel, capture, SequencedInput { operation_sequence, operation: op }) {
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
        apply(kernel, capture, SequencedInput { operation_sequence, operation: Operation::ObservationBatch(batch) });
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

fn introspect(kernel: &Kernel, include_tags: bool, ctx: &SequencerContext) -> ipb::IntrospectResponse {
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
    }
}
