//! Minimal blocking driver client for `scada.driver.v1`. Depends only on the protocol
//! crate: the kernel and the driver share no code besides the wire schema.
#![forbid(unsafe_code)]

use std::io::{self, BufReader, BufWriter, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use scada_protocol::PROTOCOL_MAJOR;
use scada_protocol::driver_v1::{
    AckStatus, BatchMode, Continuity, DriverMessage, ErrorCode, Hello, InputAck, KernelMessage, Observation,
    ObservationBatch, Welcome, driver_message, kernel_message, observation,
};
use scada_protocol::frame::{self, FrameError};

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
}

pub fn hello(driver_id: &str, source_id: u32, source_epoch: u64) -> DriverMessage {
    DriverMessage {
        body: Some(driver_message::Body::Hello(Hello {
            driver_id: driver_id.to_owned(),
            source_id,
            source_epoch,
            supported_protocol_majors: vec![PROTOCOL_MAJOR],
        })),
    }
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
        let (s, msg) = connect_raw(socket, &hello(driver_id, source_id, source_epoch))?;
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
        })
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
