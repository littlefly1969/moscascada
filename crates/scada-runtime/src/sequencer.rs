//! The single writer: one OS thread owns the [`Kernel`] and applies admitted requests one
//! at a time, in the order they are taken from the bounded input queue (ingress
//! admission order). It assigns `operation_sequence` and `link_generation`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use scada_core::{
    AppliedKind, Kernel, LinkState, ObservationBatch, Operation, Quality, SequencedInput, SourceId, TransitionOutcome,
    Value,
};
use scada_protocol::driver_v1::{AckStatus, ErrorCode};
use scada_protocol::introspect_v1 as ipb;
use tokio::sync::{mpsc, oneshot};

use crate::convert;
use crate::diag::{self, Diag, Stage};

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
    Connect {
        source_id: SourceId,
        source_epoch: u64,
        reply: oneshot::Sender<ConnectReply>,
        superseded: oneshot::Sender<()>,
    },
    Disconnect {
        source_id: SourceId,
        link_generation: u64,
    },
    /// `submitted`: when the session started to submit it (diagnostics only).
    Batch {
        batch: ObservationBatch,
        reply: oneshot::Sender<BatchReply>,
        submitted: Option<Instant>,
    },
    Introspect {
        include_tags: bool,
        reset_diagnostics: bool,
        reply: oneshot::Sender<ipb::IntrospectResponse>,
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
    /// Requests taken from the queue per wake-up (>= 1).
    pub drain_max: u32,
}

/// Start the sequencer thread. It ends when every sender of `rx` is dropped. `stopped`
/// resolves when the thread ends for any reason (including a panic, which unwinds and drops
/// it), so the host can stop instead of running without its single writer.
pub fn spawn(
    kernel: Kernel,
    rx: mpsc::Receiver<Request>,
    ctx: SequencerContext,
) -> std::io::Result<(thread::JoinHandle<Kernel>, oneshot::Receiver<()>)> {
    let (alive, stopped) = oneshot::channel::<()>();
    let handle = thread::Builder::new().name("scada-sequencer".into()).spawn(move || {
        let _alive = alive;
        run(kernel, rx, ctx)
    })?;
    Ok((handle, stopped))
}

/// Runtime-only state of the sequencer (never part of the kernel state or digest).
struct Links {
    next_generation: u64,
    /// Per configured source (config order): active link generation and its takeover signal.
    active: Vec<Option<(u64, oneshot::Sender<()>)>>,
}

fn run(mut kernel: Kernel, mut rx: mpsc::Receiver<Request>, ctx: SequencerContext) -> Kernel {
    let mut links = Links { next_generation: 0, active: Vec::new() };
    links.active.resize_with(kernel.config().sources().len(), || None);
    let drain = ctx.drain_max.max(1) as usize;
    if drain == 1 {
        // VS-001 path: one request per wake-up.
        while let Some(req) = rx.blocking_recv() {
            if let Some(d) = &ctx.diag {
                d.record_ns(Stage::DrainSize, 1);
            }
            handle(&mut kernel, &mut links, req, &ctx);
        }
        return kernel;
    }
    // Take what is already queued, up to `drain`, without waiting for more; then apply
    // each request in order. Preallocated once; `recv_many` never grows it (len 0 <= cap).
    let mut taken: Vec<Request> = Vec::with_capacity(drain);
    loop {
        taken.clear();
        let n = rx.blocking_recv_many(&mut taken, drain);
        if n == 0 {
            break; // every sender dropped and the queue is empty
        }
        if let Some(d) = &ctx.diag {
            d.record_ns(Stage::DrainSize, n as u64);
        }
        for req in taken.drain(..) {
            handle(&mut kernel, &mut links, req, &ctx);
        }
    }
    kernel
}

fn handle(kernel: &mut Kernel, links: &mut Links, req: Request, ctx: &SequencerContext) {
    match req {
        Request::Connect { source_id, source_epoch, reply, superseded } => {
            let r = connect(kernel, &mut links.next_generation, source_id, source_epoch);
            if let ConnectReply::Welcome { link_generation, .. } = r
                && let Some(slot) = kernel.config().source_index(source_id).and_then(|i| links.active.get_mut(i))
                && let Some((_, old)) = slot.replace((link_generation, superseded))
            {
                // D3: tell the previous link of this source to close. Its own
                // Disconnect stays queued after its batches and is ignored as stale.
                let _ = old.send(());
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
            if let Err(e) = kernel.apply(SequencedInput { operation_sequence, operation: op }) {
                eprintln!("sequencer: disconnect of source {source_id} gen {link_generation} rejected: {e}");
            }
        }
        Request::Batch { batch, reply, submitted } => {
            let d = ctx.diag.as_deref();
            let probe = d.is_some_and(|d| d.is_probe(batch.source_id.0));
            if let (Some(d), Some(t)) = (d, submitted) {
                d.since(Stage::QueueWait, t);
                if probe {
                    d.since(Stage::ProbeQueueWait, t);
                }
            }
            let t0 = diag::now(d);
            let mut r = apply_batch(kernel, batch, &ctx.counters);
            if let (Some(d), Some(t0)) = (d, t0) {
                d.since(Stage::Apply, t0);
                r.sent = Some(Instant::now());
            }
            // ACK only after `apply` returned; if the session is gone the ACK is moot.
            let _ = reply.send(r);
        }
        Request::Introspect { include_tags, reset_diagnostics, reply } => {
            let _ = reply.send(introspect(kernel, include_tags, ctx));
            if reset_diagnostics && let Some(d) = &ctx.diag {
                d.reset();
            }
        }
    }
    if !ctx.throttle.is_zero() {
        thread::sleep(ctx.throttle);
    }
}

fn connect(kernel: &mut Kernel, next_generation: &mut u64, source_id: SourceId, source_epoch: u64) -> ConnectReply {
    let Some(operation_sequence) = kernel.next_operation_sequence() else {
        return ConnectReply::Rejected(ErrorCode::OperationSequenceExhausted);
    };
    let Some(link_generation) = next_generation.checked_add(1) else {
        return ConnectReply::Rejected(ErrorCode::Internal);
    };
    let op = Operation::SourceConnected { source_id, link_generation, source_epoch };
    match kernel.apply(SequencedInput { operation_sequence, operation: op }) {
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

fn apply_batch(kernel: &mut Kernel, batch: ObservationBatch, counters: &Counters) -> BatchReply {
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
    let outcome = kernel.apply(SequencedInput { operation_sequence, operation: Operation::ObservationBatch(batch) });
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
                        Quality::Uncertain(_) => ipb::Quality::UncertainSourceUnavailable,
                    } as i32,
                    revision: t.revision,
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
                ipb::SourceState {
                    source_id: s.source_id.0,
                    link_state: state as i32,
                    link_generation: generation,
                    source_epoch: s.epoch,
                    last_accepted_source_sequence: s.last_accepted_source_sequence,
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
