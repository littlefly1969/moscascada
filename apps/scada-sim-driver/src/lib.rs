//! Minimal blocking driver client for `scada.driver.v1`. Depends only on the protocol
//! crate: the kernel and the driver share no code besides the wire schema.
#![forbid(unsafe_code)]

use std::io::{self, BufReader, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use scada_protocol::alarm_v1;
use scada_protocol::driver_v1::{
    AckStatus, BatchMode, Continuity, DriverMessage, ErrorCode, Heartbeat, Hello, InputAck, KernelMessage, Observation,
    ObservationBatch, Welcome, driver_message, kernel_message, observation,
};
use scada_protocol::frame::{self, FrameError};
use scada_protocol::{MINOR_HEARTBEAT, PROTOCOL_MAJOR, PROTOCOL_MINOR};

/// Frame limit used for reading kernel messages before the Welcome tells us the real one.
pub const CLIENT_MAX_FRAME: u32 = 1 << 20;

#[derive(Debug)]
pub enum ClientError {
    Frame(FrameError),
    Closed,
    Rejected { code: ErrorCode, detail: String },
    Protocol { code: ErrorCode, detail: String },
    Unexpected(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Frame(e) => write!(f, "frame: {e}"),
            ClientError::Closed => f.write_str("connection closed by kernel"),
            ClientError::Rejected { code, detail } => {
                write!(f, "handshake rejected: {} ({detail})", code.as_str_name())
            }
            ClientError::Protocol { code, detail } => write!(f, "protocol error: {} ({detail})", code.as_str_name()),
            ClientError::Unexpected(s) => write!(f, "unexpected: {s}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<FrameError> for ClientError {
    fn from(e: FrameError) -> Self {
        ClientError::Frame(e)
    }
}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        ClientError::Frame(FrameError::Io(e))
    }
}

/// A connected, handshaken session for one source.
#[derive(Debug)]
pub struct Session {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
    pub welcome: Welcome,
    rbuf: Vec<u8>,
    wbuf: Vec<u8>,
    /// VS-004: sequence of the last heartbeat sent on this link (0 = none).
    heartbeat_sequence: u64,
}

/// `Hello` announcing the highest minor this driver implements (1.1, VS-004).
pub fn hello(driver_id: &str, source_id: u32, source_epoch: u64) -> DriverMessage {
    hello_minor(driver_id, source_id, source_epoch, PROTOCOL_MINOR)
}

/// `Hello` of a 1.0 driver (VS-001..VS-003): byte-identical to what those drivers send
/// (the `protocol_minor` field is absent on the wire).
pub fn hello_v10(driver_id: &str, source_id: u32, source_epoch: u64) -> DriverMessage {
    hello_minor(driver_id, source_id, source_epoch, 0)
}

pub fn hello_minor(driver_id: &str, source_id: u32, source_epoch: u64, protocol_minor: u32) -> DriverMessage {
    DriverMessage {
        body: Some(driver_message::Body::Hello(Hello {
            driver_id: driver_id.to_owned(),
            source_id,
            source_epoch,
            supported_protocol_majors: vec![PROTOCOL_MAJOR],
            protocol_minor,
            request_durable_receipts: false,
        })),
    }
}

/// VS-006: `Hello` of a 1.2 driver that asks for durable receipts.
pub fn hello_durable(driver_id: &str, source_id: u32, source_epoch: u64) -> DriverMessage {
    let mut m = hello_minor(driver_id, source_id, source_epoch, PROTOCOL_MINOR);
    if let Some(driver_message::Body::Hello(h)) = &mut m.body {
        h.request_durable_receipts = true;
    }
    m
}

/// VS-004 heartbeat message (minor 1.1).
pub fn heartbeat(sequence: u64) -> DriverMessage {
    DriverMessage { body: Some(driver_message::Body::Heartbeat(Heartbeat { sequence })) }
}

pub fn batch(mode: BatchMode, source_sequence: u64, obs: &[(u32, Val)]) -> DriverMessage {
    DriverMessage {
        body: Some(driver_message::Body::Batch(ObservationBatch {
            mode: mode as i32,
            source_sequence,
            observations: obs
                .iter()
                .map(|&(channel_id, v)| Observation {
                    channel_id,
                    value: Some(match v {
                        Val::B(b) => observation::Value::BoolValue(b),
                        Val::I(i) => observation::Value::I64Value(i),
                    }),
                })
                .collect(),
            ..Default::default()
        })),
    }
}

/// VS-006 (minor 1.2): a batch whose observations carry source timestamps (`None` = not
/// available for that observation) and an optional driver acquisition time.
pub fn batch_with_times(
    mode: BatchMode,
    source_sequence: u64,
    obs: &[(u32, Val, Option<i64>)],
    acquired_at: Option<i64>,
    clock_quality: scada_protocol::driver_v1::ClockQuality,
) -> DriverMessage {
    DriverMessage {
        body: Some(driver_message::Body::Batch(ObservationBatch {
            mode: mode as i32,
            source_sequence,
            observations: obs
                .iter()
                .map(|&(channel_id, v, _)| Observation {
                    channel_id,
                    value: Some(match v {
                        Val::B(b) => observation::Value::BoolValue(b),
                        Val::I(i) => observation::Value::I64Value(i),
                    }),
                })
                .collect(),
            // Aligned with the observations; INT64_MIN = not available.
            source_timestamps_utc_ns: if obs.iter().any(|o| o.2.is_some()) {
                obs.iter().map(|o| o.2.unwrap_or(i64::MIN)).collect()
            } else {
                Vec::new()
            },
            acquired_at_utc_ns: acquired_at,
            source_clock_quality: clock_quality as i32,
            source_clock_id: "sim-clock".into(),
        })),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Val {
    B(bool),
    I(i64),
}

/// Connect and send an arbitrary first message; returns the raw kernel answer.
pub fn connect_raw(socket: &Path, first: &DriverMessage) -> Result<(UnixStream, KernelMessage), ClientError> {
    let mut s = UnixStream::connect(socket)?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut w = Vec::new();
    frame::write_message(&mut s, first, CLIENT_MAX_FRAME, &mut w)?;
    let mut r = Vec::new();
    let msg: KernelMessage = frame::read_message(&mut s, CLIENT_MAX_FRAME, &mut r)?.ok_or(ClientError::Closed)?;
    Ok((s, msg))
}

impl Session {
    pub fn connect(socket: &Path, driver_id: &str, source_id: u32, source_epoch: u64) -> Result<Session, ClientError> {
        Session::connect_with(socket, &hello(driver_id, source_id, source_epoch))
    }

    /// Handshake with an explicit `Hello` (e.g. [`hello_v10`] for a legacy driver).
    pub fn connect_with(socket: &Path, hello: &DriverMessage) -> Result<Session, ClientError> {
        let (s, msg) = connect_raw(socket, hello)?;
        let welcome = match msg.body {
            Some(kernel_message::Body::Welcome(w)) => w,
            Some(kernel_message::Body::HandshakeRejected(r)) => {
                return Err(ClientError::Rejected { code: r.error_code(), detail: r.detail });
            }
            other => return Err(ClientError::Unexpected(format!("{other:?}"))),
        };
        Ok(Session {
            reader: BufReader::new(s.try_clone()?),
            writer: BufWriter::new(s),
            welcome,
            rbuf: Vec::new(),
            wbuf: Vec::new(),
            heartbeat_sequence: 0,
        })
    }

    /// VS-004: whether the negotiated minor allows heartbeats on this link.
    pub fn heartbeats_negotiated(&self) -> bool {
        self.welcome.negotiated_protocol_minor >= MINOR_HEARTBEAT
    }

    /// VS-004: send one heartbeat (next sequence) and flush, only if the kernel negotiated
    /// minor >= 1. Returns whether it was sent: against a 1.0 kernel nothing is written.
    /// No ACK is expected.
    pub fn heartbeat(&mut self) -> Result<bool, ClientError> {
        if !self.heartbeats_negotiated() {
            return Ok(false);
        }
        self.heartbeat_sequence += 1;
        self.send(&heartbeat(self.heartbeat_sequence))?;
        self.flush()?;
        Ok(true)
    }

    /// Sequence of the last heartbeat sent (0 = none).
    pub fn heartbeat_sequence(&self) -> u64 {
        self.heartbeat_sequence
    }

    pub fn max_frame(&self) -> u32 {
        self.welcome.limits.map_or(65_536, |l| l.max_frame_bytes)
    }

    /// Queue a message (buffered; call [`Session::flush`] or [`Session::recv_ack`]).
    pub fn send(&mut self, msg: &DriverMessage) -> Result<(), ClientError> {
        frame::encode_frame(msg, self.max_frame(), &mut self.wbuf)?;
        self.writer.write_all(&self.wbuf)?;
        Ok(())
    }

    /// Write raw bytes (for malformed-input tests).
    pub fn send_raw(&mut self, bytes: &[u8]) -> Result<(), ClientError> {
        self.writer.write_all(bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), ClientError> {
        self.writer.flush()?;
        Ok(())
    }

    pub fn recv(&mut self) -> Result<KernelMessage, ClientError> {
        self.writer.flush()?;
        frame::read_message(&mut self.reader, CLIENT_MAX_FRAME, &mut self.rbuf)?.ok_or(ClientError::Closed)
    }

    pub fn recv_ack(&mut self) -> Result<InputAck, ClientError> {
        match self.recv()?.body {
            Some(kernel_message::Body::Ack(a)) => Ok(a),
            Some(kernel_message::Body::ProtocolError(p)) => {
                Err(ClientError::Protocol { code: p.error_code(), detail: p.detail })
            }
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }

    /// Send one batch and wait for its ACK (stop-and-wait).
    pub fn request(&mut self, msg: &DriverMessage) -> Result<InputAck, ClientError> {
        self.send(msg)?;
        self.recv_ack()
    }

    pub fn into_stream(self) -> UnixStream {
        self.writer.into_parts().0
    }
}

/// Outcome of [`resume`].
#[derive(Debug)]
pub struct ResumeReport {
    /// ACKs of the replayed batches, in order.
    pub replayed: Vec<InputAck>,
    /// ACK of the reasserting FullState.
    pub snapshot: InputAck,
    /// Sequence to use for the next batch.
    pub next_sequence: u64,
}

/// VS-002 (D1) reconnection procedure for a driver that reconnects WITHOUT restarting
/// (same `source_epoch`), with no change to the wire protocol. Call it right after
/// [`Session::connect`]:
///
/// 1. `unacked` holds the batches sent on the previous link whose ACK was not received,
///    with their sequences, in order. Those at or below the kernel's
///    `last_accepted_source_sequence` were applied (only their ACK was lost) and are not
///    resent; the others are replayed in order and must come back `APPLIED_VOLATILE`.
/// 2. A `FullState` with the device's current value of every channel is sent at the next
///    sequence. Its continuity is `CONTINUOUS` (no gap) and it turns every channel of the
///    source back to `Good`, including channels that did not change while the link was down
///    and would otherwise stay `Uncertain` indefinitely.
///
/// Replayed duplicates never refresh quality (the kernel answers `DUPLICATE` without any
/// state change), so old data is not mistaken for fresh evidence. Events that the device
/// did not buffer between the last replayed batch and the snapshot are lost; the snapshot
/// replaces the state, it cannot reconstruct them. A device that cannot produce a snapshot
/// skips step 2; its unobserved channels then correctly stay `Uncertain`.
pub fn resume(
    s: &mut Session,
    unacked: &[(u64, DriverMessage)],
    snapshot: &[(u32, Val)],
) -> Result<ResumeReport, ClientError> {
    let mut replayed = Vec::new();
    let last = if s.welcome.full_state_required { None } else { s.welcome.last_accepted_source_sequence };
    let mut next = last.map_or(1, |l| l + 1);
    if let Some(l) = last {
        for (seq, msg) in unacked.iter().filter(|(q, _)| *q > l) {
            let ack = s.request(msg)?;
            next = seq + 1;
            replayed.push(ack);
        }
    } else if let Some((seq, _)) = unacked.last() {
        // No baseline (e.g. kernel restarted): deltas cannot be replayed; the snapshot
        // re-establishes the baseline after the highest sequence ever used.
        next = next.max(seq + 1);
    }
    let snapshot = s.request(&batch(BatchMode::FullState, next, snapshot))?;
    Ok(ResumeReport { replayed, snapshot, next_sequence: next + 1 })
}

/// One-line, stable, human and machine readable rendering of an ACK.
pub fn ack_line(a: &InputAck) -> String {
    let status = AckStatus::try_from(a.status).map_or("?", |s| s.as_str_name());
    let err = ErrorCode::try_from(a.error_code).map_or("?", |e| e.as_str_name());
    let cont = Continuity::try_from(a.continuity).map_or("?", |c| c.as_str_name());
    format!(
        "ACK seq={} status={} op={} error={} continuity={} last_accepted={}",
        a.source_sequence,
        status,
        a.operation_sequence.map_or("-".into(), |o| o.to_string()),
        err,
        cont,
        a.last_accepted_source_sequence.map_or("-".into(), |o| o.to_string()),
    )
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

pub fn welcome_line(w: &Welcome) -> String {
    format!(
        "WELCOME instance={} version={}.{} link_generation={} full_state_required={} last_accepted={}",
        hex(&w.kernel_instance_id),
        w.negotiated_protocol_major,
        w.negotiated_protocol_minor,
        w.link_generation,
        w.full_state_required,
        w.last_accepted_source_sequence.map_or("-".into(), |o| o.to_string()),
    )
}

// ------------------------------------------------------------------ VS-005 alarm client

/// Blocking client of the alarm control socket (`scada.alarm.v1`, ADR 0012). Used by the
/// tests, the benchmark harness and the `alarms` subcommand of the simulator. One request,
/// one response; the kernel identifies the actor of an ACK by the peer credentials of this
/// socket, never by a field of the request.
#[derive(Debug)]
pub struct AlarmClient {
    stream: UnixStream,
    wbuf: Vec<u8>,
    rbuf: Vec<u8>,
    /// Instance id sent with every request (empty = any; ACK requires it).
    pub instance: Vec<u8>,
}

impl AlarmClient {
    pub fn connect(socket: &Path) -> Result<AlarmClient, ClientError> {
        let stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        Ok(AlarmClient { stream, wbuf: Vec::new(), rbuf: Vec::new(), instance: Vec::new() })
    }

    /// Send any request (with its own instance id) and read the response.
    pub fn call_raw(&mut self, req: &alarm_v1::AlarmRequest) -> Result<alarm_v1::AlarmResponse, ClientError> {
        frame::encode_frame(req, frame::MAX_FRAME_BYTES_CEILING, &mut self.wbuf)?;
        self.stream.write_all(&self.wbuf)?;
        self.read()
    }

    pub fn read(&mut self) -> Result<alarm_v1::AlarmResponse, ClientError> {
        frame::read_message(&mut self.stream, frame::MAX_FRAME_BYTES_CEILING, &mut self.rbuf)?
            .ok_or(ClientError::Closed)
    }

    pub fn send_raw(&mut self, bytes: &[u8]) -> Result<(), ClientError> {
        self.stream.write_all(bytes)?;
        Ok(())
    }

    pub fn call(&mut self, body: alarm_v1::alarm_request::Body) -> Result<alarm_v1::AlarmResponse, ClientError> {
        let req = alarm_v1::AlarmRequest {
            alarm_api_version: 1,
            kernel_instance_id: self.instance.clone(),
            body: Some(body),
        };
        let r = self.call_raw(&req)?;
        if self.instance.is_empty() {
            self.instance = r.kernel_instance_id.clone();
        }
        Ok(r)
    }

    pub fn snapshot(
        &mut self,
        after: u32,
        limit: u32,
        expected_change: u64,
    ) -> Result<alarm_v1::AlarmResponse, ClientError> {
        self.call(alarm_v1::alarm_request::Body::Snapshot(alarm_v1::GetAlarmSnapshot {
            after_alarm_id: after,
            limit,
            expected_change_operation: expected_change,
        }))
    }

    pub fn events(&mut self, after: u64, limit: u32) -> Result<alarm_v1::AlarmResponse, ClientError> {
        self.call(alarm_v1::alarm_request::Body::ReadEvents(alarm_v1::ReadEvents { after_event_seq: after, limit }))
    }

    pub fn ack(&mut self, alarm_id: u32, activation_token: u64) -> Result<alarm_v1::AlarmResponse, ClientError> {
        self.call(alarm_v1::alarm_request::Body::Acknowledge(alarm_v1::AcknowledgeAlarm {
            alarm_id,
            activation_token,
            ..Default::default()
        }))
    }

    /// VS-006: acknowledgement with an idempotency key, an operator note and, optionally,
    /// a durable answer (sent only after the journal commit).
    pub fn ack_action(
        &mut self,
        alarm_id: u32,
        activation_token: u64,
        action_id: [u8; 16],
        note: &str,
        require_durable: bool,
    ) -> Result<alarm_v1::AlarmResponse, ClientError> {
        self.call(alarm_v1::alarm_request::Body::Acknowledge(alarm_v1::AcknowledgeAlarm {
            alarm_id,
            activation_token,
            action_id: action_id.to_vec(),
            note: note.to_owned(),
            require_durable,
        }))
    }

    pub fn capacity(&mut self) -> Result<alarm_v1::CapacityStatus, ClientError> {
        match self.call(alarm_v1::alarm_request::Body::Capacity(alarm_v1::GetCapacityStatus {}))?.body {
            Some(alarm_v1::alarm_response::Body::Capacity(c)) => Ok(c),
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }

    /// TEST ONLY (`--test-simulated-durable-sink`): release events up to `up_to`.
    pub fn simulated_commit(&mut self, up_to: u64) -> Result<alarm_v1::AlarmResponse, ClientError> {
        self.call(alarm_v1::alarm_request::Body::SimulatedDurableCommit(alarm_v1::SimulatedDurableCommit {
            up_to_event_seq: up_to,
        }))
    }
}

// ------------------------------------------------------------------ VS-006 history client

/// Blocking client of the historian socket (`scada.history.v1`, ADR 0013 D12).
#[derive(Debug)]
pub struct HistoryClient {
    stream: UnixStream,
    wbuf: Vec<u8>,
    rbuf: Vec<u8>,
}

impl HistoryClient {
    pub fn connect(socket: &Path) -> Result<HistoryClient, ClientError> {
        let stream = UnixStream::connect(socket)?;
        stream.set_read_timeout(Some(Duration::from_secs(60)))?;
        Ok(HistoryClient { stream, wbuf: Vec::new(), rbuf: Vec::new() })
    }

    pub fn call(
        &mut self,
        body: scada_protocol::history_v1::history_request::Body,
    ) -> Result<scada_protocol::history_v1::HistoryResponse, ClientError> {
        let req = scada_protocol::history_v1::HistoryRequest { history_api_version: 1, body: Some(body) };
        frame::encode_frame(&req, frame::MAX_FRAME_BYTES_CEILING, &mut self.wbuf)?;
        self.stream.write_all(&self.wbuf)?;
        frame::read_message(&mut self.stream, frame::MAX_FRAME_BYTES_CEILING, &mut self.rbuf)?
            .ok_or(ClientError::Closed)
    }

    /// One page of alarm history after `after` (keyset), optionally for one alarm.
    pub fn history(
        &mut self,
        after: u64,
        limit: u32,
        alarm_id: u32,
    ) -> Result<scada_protocol::history_v1::HistoryPage, ClientError> {
        use scada_protocol::history_v1 as h;
        let q = h::QueryAlarmHistory { after_event_seq: after, limit, alarm_id, ..Default::default() };
        match self.call(h::history_request::Body::Query(q))?.body {
            Some(h::history_response::Body::History(p)) => Ok(p),
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }

    pub fn journal(&mut self) -> Result<scada_protocol::history_v1::JournalStatus, ClientError> {
        use scada_protocol::history_v1 as h;
        match self.call(h::history_request::Body::Journal(h::GetJournalStatus {}))?.body {
            Some(h::history_response::Body::Journal(j)) => Ok(j),
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }

    pub fn recovery(&mut self) -> Result<scada_protocol::history_v1::RecoveryStatus, ClientError> {
        use scada_protocol::history_v1 as h;
        match self.call(h::history_request::Body::Recovery(h::GetRecoveryStatus {}))?.body {
            Some(h::history_response::Body::Recovery(r)) => Ok(r),
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }

    pub fn time(&mut self) -> Result<scada_protocol::history_v1::TimeStatus, ClientError> {
        use scada_protocol::history_v1 as h;
        match self.call(h::history_request::Body::Time(h::GetTimeStatus {}))?.body {
            Some(h::history_response::Body::Time(t)) => Ok(t),
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }

    pub fn projection(
        &mut self,
        after: u32,
        limit: u32,
    ) -> Result<scada_protocol::history_v1::ActiveAlarmProjection, ClientError> {
        use scada_protocol::history_v1 as h;
        let q = h::GetActiveAlarmProjection { after_alarm_id: after, limit };
        match self.call(h::history_request::Body::Projection(q))?.body {
            Some(h::history_response::Body::Projection(p)) => Ok(p),
            other => Err(ClientError::Unexpected(format!("{other:?}"))),
        }
    }
}
