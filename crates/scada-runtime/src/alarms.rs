//! VS-005 (ADR 0012): runtime side of the alarm engine.
//!
//! * [`AlarmCall`] / [`AlarmReply`]: requests of the control plane, served by the single
//!   writer in sequence order (an acknowledgement is an `Operation`; reads are answered at
//!   a precise operation sequence). Page buffers travel with the request and come back with
//!   the reply, so the sequencer never allocates for a read.
//! * [`AlarmRuntime`]: sequencer-owned bookkeeping: link transitions refused for lack of
//!   outbox slots (retried after slots are released), overload state, test-only simulated
//!   durable sink.
//! * [`serve_control`]: the `scada.alarm.v1` socket (peer uid allowlist, bounded
//!   connections, bounded frames, idle and write timeouts).
//!
//! Nothing here is durable: the outbox is RAM, VS-006 adds the journal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use scada_core::{
    AlarmEvent, AlarmEventKind, AlarmId, AlarmView, AppliedKind, ApplyError, EpisodeState, EventPage, IgnoredReason,
    Kernel, OutboxStatus, Severity, SnapshotPage, SourceId, TransitionOutcome,
};
use scada_protocol::alarm_v1 as apb;
use scada_protocol::frame;
use scada_protocol::{KERNEL_INSTANCE_ID_LEN, Message};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, oneshot};
use tokio::time::timeout;

use crate::config::AlarmSettings;
use crate::sequencer::{Counters, Request};
use crate::server::Server;

/// Largest accepted control request frame (requests are a few dozen bytes).
pub const MAX_CONTROL_REQUEST_BYTES: u32 = 1024;
/// Idle control session timeout.
pub const CONTROL_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Upper bound of one encoded `AlarmEventRecord` / `AlarmEntry` inside a page (field keys
/// + worst-case varints), used for the response buffer bound.
pub const MAX_RECORD_WIRE_BYTES: u128 = 80;
/// Fixed per-session estimate (task, split socket, futures), as for driver sessions.
pub const CONTROL_SESSION_ESTIMATE_BYTES: u128 = 16 * 1024;

/// A control-plane request handed to the sequencer.
#[derive(Debug)]
pub enum AlarmCall {
    Snapshot { after: AlarmId, limit: u32, expected_change: u64, buf: Vec<AlarmView> },
    Events { after: u64, limit: u32, buf: Vec<AlarmEvent> },
    Ack { alarm_id: AlarmId, token: u64, actor_uid: u32 },
    Capacity,
    SimulatedDurableCommit { up_to: u64 },
}

/// Outcome of an acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Applied { operation_sequence: u64, event_seq: u64 },
    AlreadyAcked,
    Stale { current: u64 },
    OutboxFull { required: u64, free: u64 },
    UnknownAlarm,
    NotAcknowledgeable,
    SequenceExhausted,
    Internal,
}

/// Capacity report built on the sequencer thread.
#[derive(Debug, Clone, Copy)]
pub struct CapacityReport {
    pub outbox: OutboxStatus,
    pub overload: Overload,
    pub simulated_sink: bool,
    pub rejected_batches: u64,
    pub rejected_link: u64,
    pub rejected_supervision: u64,
    pub rejected_acks: u64,
    pub pending_link_transitions: u32,
    pub rules: u32,
    pub worst_fanout: u32,
    pub memory_bound: u64,
    pub high_percent: u32,
    pub operation_sequence: u64,
}

/// Answer of the sequencer.
#[derive(Debug)]
pub enum AlarmReply {
    Snapshot { page: Result<SnapshotPage, u64>, buf: Vec<AlarmView>, operation_sequence: u64 },
    Events { page: EventPage, buf: Vec<AlarmEvent> },
    Ack(AckOutcome),
    Capacity(CapacityReport),
    Watermark(Result<(u64, u64, Option<u64>), String>),
    Unsupported(&'static str),
}

/// Overload classification (ADR 0012 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overload {
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Sequencer-owned alarm bookkeeping (runtime only, never in the digest).
#[derive(Debug)]
pub struct AlarmRuntime {
    /// Disconnections refused for lack of slots: retried after slots are released.
    /// One entry per source at most (the newest generation).
    pub pending_disconnects: Vec<(SourceId, u64)>,
    /// A transition was refused since the last slot release (sticky CRITICAL).
    pub refused_since_release: bool,
    /// Slots were released by the last applied operation.
    pub released: bool,
    pub simulated_sink: bool,
    pub high_percent: u32,
    pub memory_bound: u64,
    last_overload: Overload,
}

impl AlarmRuntime {
    pub fn new(sources: usize, simulated_sink: bool, high_percent: u32, memory_bound: u64) -> AlarmRuntime {
        AlarmRuntime {
            pending_disconnects: Vec::with_capacity(sources),
            refused_since_release: false,
            released: false,
            simulated_sink,
            high_percent,
            memory_bound,
            last_overload: Overload::Normal,
        }
    }

    pub fn overload(&self, s: &OutboxStatus) -> Overload {
        if self.refused_since_release || s.free == 0 || !self.pending_disconnects.is_empty() {
            Overload::Critical
        } else if u128::from(s.used) * 100 >= u128::from(s.capacity) * u128::from(self.high_percent) {
            Overload::High
        } else {
            Overload::Normal
        }
    }

    /// Bookkeeping after every `apply` of a kernel with alarms. Diagnostics go to stderr
    /// (independent of the outbox: a full outbox cannot hold its own alarm).
    pub fn after_apply(&mut self, k: &Kernel, r: &Result<TransitionOutcome, ApplyError>, origin: Origin, c: &Counters) {
        match r {
            Err(ApplyError::OutboxCapacityExceeded { .. }) => {
                self.refused_since_release = true;
                Counters::inc(match origin {
                    Origin::Batch => &c.outbox_rejected_batches,
                    Origin::Link => &c.outbox_rejected_link,
                    Origin::Supervision => &c.outbox_rejected_supervision,
                    Origin::Ack => &c.outbox_rejected_acks,
                    Origin::Other => &c.outbox_rejected_link,
                });
            }
            Ok(TransitionOutcome::Applied { kind: AppliedKind::DurableWatermarkAdvanced { .. }, .. }) => {
                self.refused_since_release = false;
                self.released = true;
            }
            _ => {}
        }
        if let Some(s) = k.outbox_status() {
            let o = self.overload(&s);
            if o != self.last_overload {
                eprintln!(
                    "alarms: outbox {} (used {}/{} events, free {}, durable_watermark {}, pending link transitions {}){}",
                    match o {
                        Overload::Normal => "NORMAL",
                        Overload::High => "HIGH",
                        Overload::Critical => "CRITICAL: transitions that produce alarm events are refused",
                    },
                    s.used,
                    s.capacity,
                    s.free,
                    s.durable_watermark,
                    self.pending_disconnects.len(),
                    if self.simulated_sink { "" } else { " [no durable sink in VS-005: slots are never released]" }
                );
                self.last_overload = o;
            }
            c.outbox_overload.store(o as u64, Ordering::Relaxed);
        }
        c.pending_link_transitions.store(self.pending_disconnects.len() as u64, Ordering::Relaxed);
    }

    /// Remember a refused disconnection (keep only the newest generation per source).
    pub fn defer_disconnect(&mut self, source_id: SourceId, generation: u64) {
        self.pending_disconnects.retain(|&(s, g)| s != source_id || g > generation);
        if !self.pending_disconnects.iter().any(|&(s, _)| s == source_id) {
            self.pending_disconnects.push((source_id, generation));
        }
    }
}

/// Which kind of input an `apply` came from (rejection counters).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Batch,
    Link,
    Supervision,
    Ack,
    Other,
}

/// Ack outcome of an `apply` of an `AlarmAcknowledge`.
pub fn ack_outcome(r: &Result<TransitionOutcome, ApplyError>) -> AckOutcome {
    match r {
        Ok(TransitionOutcome::Applied {
            operation_sequence,
            kind: AppliedKind::AlarmAcknowledged { event_seq, .. },
        }) => AckOutcome::Applied { operation_sequence: *operation_sequence, event_seq: *event_seq },
        Ok(TransitionOutcome::Ignored(IgnoredReason::AlreadyAcknowledged)) => AckOutcome::AlreadyAcked,
        Err(ApplyError::StaleActivationToken { current, .. }) => AckOutcome::Stale { current: *current },
        Err(ApplyError::OutboxCapacityExceeded { required, free }) => {
            AckOutcome::OutboxFull { required: *required, free: *free }
        }
        Err(ApplyError::UnknownAlarm(_)) => AckOutcome::UnknownAlarm,
        Err(ApplyError::NotAcknowledgeable(_)) => AckOutcome::NotAcknowledgeable,
        Err(ApplyError::OperationSequenceExhausted) => AckOutcome::SequenceExhausted,
        _ => AckOutcome::Internal,
    }
}

// ------------------------------------------------------------------ protobuf mapping

fn severity(s: Severity) -> apb::Severity {
    match s {
        Severity::Info => apb::Severity::Info,
        Severity::Warning => apb::Severity::Warning,
        Severity::Major => apb::Severity::Major,
        Severity::Critical => apb::Severity::Critical,
    }
}

fn episode(e: EpisodeState) -> apb::EpisodeState {
    match e {
        EpisodeState::Normal => apb::EpisodeState::Normal,
        EpisodeState::ActiveUnacked => apb::EpisodeState::ActiveUnacked,
        EpisodeState::ActiveAcked => apb::EpisodeState::ActiveAcked,
        EpisodeState::ReturnedUnacked => apb::EpisodeState::ReturnedUnacked,
    }
}

pub fn event_record(e: &AlarmEvent) -> apb::AlarmEventRecord {
    apb::AlarmEventRecord {
        event_seq: e.event_seq,
        operation_seq: e.operation_seq,
        ordinal_in_operation: e.ordinal_in_operation,
        alarm_id: e.alarm_id.0,
        occurrence_id: e.occurrence_id,
        activation_token: e.activation_token,
        kind: match e.kind {
            AlarmEventKind::Raised => apb::EventKind::Raised,
            AlarmEventKind::Returned => apb::EventKind::Returned,
            AlarmEventKind::Retriggered => apb::EventKind::Retriggered,
            AlarmEventKind::Acknowledged => apb::EventKind::Acknowledged,
        } as i32,
        state_after: episode(e.status_after.episode()) as i32,
        input_valid_after: e.status_after.input_valid(),
        actor_uid: e.actor_uid,
    }
}

pub fn alarm_entry(v: &AlarmView) -> apb::AlarmEntry {
    apb::AlarmEntry {
        alarm_id: v.alarm_id.0,
        severity: severity(v.severity) as i32,
        state: episode(v.flags.episode()) as i32,
        input_valid: v.flags.input_valid(),
        occurrence_id: v.occurrence_id,
        activation_token: v.activation_token,
        last_event_seq: v.last_event_seq,
    }
}

pub fn capacity_status(r: &CapacityReport) -> apb::CapacityStatus {
    let o = &r.outbox;
    let sizes = scada_core::layout_sizes();
    let size = |n: &str| sizes.iter().find(|(k, _)| *k == n).map_or(0, |(_, v)| *v as u32);
    apb::CapacityStatus {
        capacity_events: o.capacity,
        used: o.used,
        free: o.free,
        high_water: o.high_water,
        next_event_seq: o.next_event_seq,
        durable_watermark: o.durable_watermark,
        oldest_visible: o.oldest_visible,
        active_or_unacked: o.active_or_unacked,
        durability: if r.simulated_sink { apb::Durability::SimulatedTestOnly } else { apb::Durability::NoneVolatile }
            as i32,
        overload: match r.overload {
            Overload::Normal => apb::OverloadState::Normal,
            Overload::High => apb::OverloadState::High,
            Overload::Critical => apb::OverloadState::Critical,
        } as i32,
        rejected_batches: r.rejected_batches,
        rejected_link_operations: r.rejected_link,
        rejected_supervision_operations: r.rejected_supervision,
        rejected_acknowledgements: r.rejected_acks,
        pending_link_transitions: r.pending_link_transitions,
        rules: r.rules,
        worst_fanout_per_operation: r.worst_fanout,
        event_record_bytes: size("AlarmEvent"),
        alarm_state_bytes: size("AlarmState"),
        alarm_memory_bound_bytes: r.memory_bound,
        operation_sequence: r.operation_sequence,
        last_alarm_change_operation: o.last_alarm_change_operation,
        high_percent: r.high_percent,
    }
}

fn ack_result(a: AckOutcome) -> apb::AckResult {
    let mut r = apb::AckResult::default();
    let st = match a {
        AckOutcome::Applied { operation_sequence, event_seq } => {
            r.operation_sequence = Some(operation_sequence);
            r.event_seq = Some(event_seq);
            apb::AckStatus::AppliedVolatile
        }
        AckOutcome::AlreadyAcked => apb::AckStatus::AlreadyAcked,
        AckOutcome::Stale { current } => {
            r.current_activation_token = current;
            apb::AckStatus::StaleOccurrence
        }
        AckOutcome::OutboxFull { required, free } => {
            r.required = required;
            r.free = free;
            apb::AckStatus::OutboxCapacityExceeded
        }
        AckOutcome::UnknownAlarm => apb::AckStatus::UnknownAlarm,
        AckOutcome::NotAcknowledgeable => apb::AckStatus::NotAcknowledgeable,
        AckOutcome::SequenceExhausted => apb::AckStatus::OperationSequenceExhausted,
        AckOutcome::Internal => apb::AckStatus::Internal,
    };
    r.status = st as i32;
    r
}

// ------------------------------------------------------------------ control socket

/// Memory bound of the control plane for `settings` (application allocations).
pub fn control_bound_bytes(s: &AlarmSettings) -> u128 {
    if s.control_socket_path.is_none() {
        return 0;
    }
    let page = u128::from(s.control_page_max);
    let per = page
        .saturating_mul(size_of::<AlarmEvent>() as u128) // reusable event page
        .saturating_add(page.saturating_mul(size_of::<AlarmView>() as u128)) // reusable alarm page
        // Response message (records) + its encoding, transient, one at a time per session.
        .saturating_add(page.saturating_mul(size_of::<apb::AlarmEventRecord>() as u128))
        .saturating_add(page.saturating_mul(MAX_RECORD_WIRE_BYTES).saturating_add(256))
        .saturating_add(u128::from(MAX_CONTROL_REQUEST_BYTES))
        .saturating_add(CONTROL_SESSION_ESTIMATE_BYTES);
    per.saturating_mul(u128::from(s.max_control_connections))
}

fn response(id: &[u8], body: apb::alarm_response::Body) -> apb::AlarmResponse {
    apb::AlarmResponse { kernel_instance_id: id.to_vec(), body: Some(body) }
}

fn error(id: &[u8], code: apb::ErrorCode, detail: &str) -> apb::AlarmResponse {
    response(id, apb::alarm_response::Body::Error(apb::ErrorResponse { code: code as i32, detail: detail.to_owned() }))
}

/// Accept loop of the alarm control socket. Bounded to `max_control_connections`.
pub async fn serve_control(
    server: Arc<Server>,
    listener: UnixListener,
    settings: AlarmSettings,
    refused: Arc<AtomicU64>,
) {
    let permits = Arc::new(Semaphore::new(settings.max_control_connections as usize));
    let settings = Arc::new(settings);
    let mut scratch = Vec::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        match permits.clone().try_acquire_owned() {
            Ok(permit) => {
                let (server, settings, refused) = (server.clone(), settings.clone(), refused.clone());
                tokio::spawn(async move {
                    control_session(&server, &settings, stream, &refused).await;
                    drop(permit);
                });
            }
            Err(_) => {
                refused.fetch_add(1, Ordering::Relaxed);
                let msg = error(
                    &server.kernel_instance_id,
                    apb::ErrorCode::TooManyConnections,
                    "control connection limit reached",
                );
                if frame::encode_frame(&msg, frame::MAX_FRAME_BYTES_CEILING, &mut scratch).is_ok() {
                    let _ = stream.try_write(&scratch);
                }
            }
        }
    }
}

async fn write(
    wr: &mut tokio::net::unix::OwnedWriteHalf,
    msg: &apb::AlarmResponse,
    scratch: &mut Vec<u8>,
    t: Duration,
) -> bool {
    use tokio::io::AsyncWriteExt;
    if frame::encode_frame(msg, frame::MAX_FRAME_BYTES_CEILING, scratch).is_err() {
        return false;
    }
    matches!(timeout(t, wr.write_all(scratch)).await, Ok(Ok(())))
}

async fn control_session(server: &Server, settings: &AlarmSettings, stream: UnixStream, refused: &AtomicU64) {
    let id = server.kernel_instance_id;
    let wt = Duration::from_millis(server.config.limits.ack_write_timeout_ms);
    let ft = Duration::from_millis(server.config.limits.frame_timeout_ms);
    let peer_uid = stream.peer_cred().ok().map(|c| c.uid());
    let (mut rd, mut wr) = stream.into_split();
    let mut scratch = Vec::new();
    let Some(uid) = peer_uid.filter(|u| settings.control_authorized_uids.binary_search(u).is_ok()) else {
        refused.fetch_add(1, Ordering::Relaxed);
        let _ = write(
            &mut wr,
            &error(&id, apb::ErrorCode::Unauthorized, "peer uid not in control_authorized_uids"),
            &mut scratch,
            wt,
        )
        .await;
        return;
    };
    let page = settings.control_page_max as usize;
    let mut events = Vec::with_capacity(page);
    let mut views = Vec::with_capacity(page);
    let mut buf = Vec::new();
    loop {
        let r = timeout(
            CONTROL_IDLE_TIMEOUT,
            crate::server::read_frame_pub(&mut rd, MAX_CONTROL_REQUEST_BYTES, &mut buf, ft),
        )
        .await;
        let ok = match r {
            Ok(Ok(true)) => true,
            Ok(Err(Some(code))) => {
                let c = if code == scada_protocol::driver_v1::ErrorCode::FrameTooLarge {
                    apb::ErrorCode::FrameTooLarge
                } else {
                    apb::ErrorCode::InvalidRequest
                };
                let _ = write(&mut wr, &error(&id, c, "invalid frame"), &mut scratch, wt).await;
                false
            }
            _ => false,
        };
        if !ok {
            return;
        }
        let Ok(req) = apb::AlarmRequest::decode(buf.as_slice()) else {
            let _ =
                write(&mut wr, &error(&id, apb::ErrorCode::InvalidRequest, "malformed AlarmRequest"), &mut scratch, wt)
                    .await;
            return;
        };
        if req.alarm_api_version != 1 {
            let _ = write(
                &mut wr,
                &error(&id, apb::ErrorCode::InvalidRequest, "alarm_api_version must be 1"),
                &mut scratch,
                wt,
            )
            .await;
            return;
        }
        let instance_ok = req.kernel_instance_id.is_empty() || req.kernel_instance_id.as_slice() == id.as_slice();
        if !instance_ok || req.kernel_instance_id.len() > KERNEL_INSTANCE_ID_LEN {
            if !write(
                &mut wr,
                &error(
                    &id,
                    apb::ErrorCode::InstanceMismatch,
                    "kernel restarted: cursors, tokens and volatile episodes of the old instance are gone",
                ),
                &mut scratch,
                wt,
            )
            .await
            {
                return;
            }
            continue;
        }
        use apb::alarm_request::Body;
        let call = match req.body {
            Some(Body::Snapshot(s)) => AlarmCall::Snapshot {
                after: AlarmId(s.after_alarm_id),
                limit: s.limit,
                expected_change: s.expected_change_operation,
                buf: std::mem::take(&mut views),
            },
            Some(Body::ReadEvents(e)) => {
                AlarmCall::Events { after: e.after_event_seq, limit: e.limit, buf: std::mem::take(&mut events) }
            }
            Some(Body::Acknowledge(a)) => {
                if req.kernel_instance_id.is_empty() {
                    if !write(
                        &mut wr,
                        &error(&id, apb::ErrorCode::InvalidRequest, "AcknowledgeAlarm requires kernel_instance_id"),
                        &mut scratch,
                        wt,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                }
                // The actor is the authenticated peer, never a client-declared value.
                AlarmCall::Ack { alarm_id: AlarmId(a.alarm_id), token: a.activation_token, actor_uid: uid }
            }
            Some(Body::Capacity(_)) => AlarmCall::Capacity,
            Some(Body::SimulatedDurableCommit(c)) => AlarmCall::SimulatedDurableCommit { up_to: c.up_to_event_seq },
            None => {
                let _ =
                    write(&mut wr, &error(&id, apb::ErrorCode::InvalidRequest, "empty request body"), &mut scratch, wt)
                        .await;
                return;
            }
        };
        let (tx, rx) = oneshot::channel();
        if server.requests.submit(Request::Alarm { call, reply: tx }, || {}).await.is_err() {
            let _ = write(&mut wr, &error(&id, apb::ErrorCode::ShuttingDown, "kernel shutting down"), &mut scratch, wt)
                .await;
            return;
        }
        let Ok(reply) = rx.await else { return };
        use apb::alarm_response::Body as R;
        let msg = match reply {
            AlarmReply::Snapshot { page, buf, operation_sequence } => {
                let m = match page {
                    Ok(p) => response(
                        &id,
                        R::Snapshot(apb::AlarmSnapshotPage {
                            alarms: buf.iter().map(alarm_entry).collect(),
                            next_after_alarm_id: p.next_after.0,
                            has_more: p.has_more,
                            total_listed: p.total_listed,
                            change_operation: p.as_of_change_operation,
                            operation_sequence,
                        }),
                    ),
                    Err(current) => error(
                        &id,
                        apb::ErrorCode::SnapshotChanged,
                        &format!("alarm state changed (change_operation {current}): restart from the first page"),
                    ),
                };
                views = buf;
                m
            }
            AlarmReply::Events { page, buf } => {
                let m = response(
                    &id,
                    R::Events(apb::EventPage {
                        events: buf.iter().map(event_record).collect(),
                        next_cursor: page.next_cursor,
                        oldest_visible: page.oldest_visible,
                        newest: page.newest,
                        has_more: page.has_more,
                        gap_detected: page.gap_detected,
                        cursor_ahead: page.cursor_ahead,
                        durable_watermark: page.durable_watermark,
                    }),
                );
                events = buf;
                m
            }
            AlarmReply::Ack(a) => response(&id, R::Ack(ack_result(a))),
            AlarmReply::Capacity(c) => response(&id, R::Capacity(capacity_status(&c))),
            AlarmReply::Watermark(Ok((wm, released, op))) => response(
                &id,
                R::Watermark(apb::WatermarkResult {
                    applied: true,
                    durable_watermark: wm,
                    released,
                    operation_sequence: op,
                }),
            ),
            AlarmReply::Watermark(Err(e)) => error(&id, apb::ErrorCode::WatermarkRefused, &e),
            AlarmReply::Unsupported(why) => error(&id, apb::ErrorCode::Unsupported, why),
        };
        if !write(&mut wr, &msg, &mut scratch, wt).await {
            return;
        }
    }
}
