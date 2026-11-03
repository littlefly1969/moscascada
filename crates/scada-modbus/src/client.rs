//! Blocking Modbus TCP client: exactly one outstanding request per connection.
//!
//! Every response is matched against the request it answers (transaction id, protocol id,
//! unit id, function code, byte count, quantity). Any mismatch, timeout, end of stream or
//! framing error poisons the connection: the caller must drop it and reconnect, so bytes of
//! an old or partial frame are never combined with a later answer. An exception response is
//! the only failure after which the connection stays usable.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use crate::mbap::{self, AduDecoder};
use crate::pdu::{self, ReadData, ReadRequest, ResponseError};

/// A byte stream with a settable read deadline (implemented for `TcpStream`).
pub trait Transport: Read + Write {
    fn set_read_timeout(&mut self, t: Option<Duration>) -> io::Result<()>;
}

impl Transport for TcpStream {
    fn set_read_timeout(&mut self, t: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, t)
    }
}

/// Failure of one request. Distinct and observable (VS-007 §3).
#[derive(Debug)]
pub enum ModbusError {
    /// No response (or only part of one) before the response timeout.
    Timeout { partial_bytes: usize },
    /// The peer closed the connection (possibly inside a frame).
    Closed { partial_bytes: usize },
    /// Socket error.
    Io(io::Error),
    /// Invalid MBAP header (protocol id, length).
    Frame(mbap::FrameError),
    /// A response with another transaction id: stale or foreign; never used.
    TransactionMismatch { expected: u16, got: u16 },
    /// A response from another unit id (gateway / wrong device); never used.
    UnitMismatch { expected: u8, got: u8 },
    /// Exception response, or a response that does not match the request.
    Response(ResponseError),
    /// The connection is poisoned by an earlier failure (reconnect first).
    Poisoned,
}

impl ModbusError {
    /// Whether the connection must be dropped. Only an exception response leaves it usable.
    pub fn is_fatal_for_connection(&self) -> bool {
        !matches!(self, ModbusError::Response(ResponseError::Exception(_)))
    }

    /// Short stable name for counters and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            ModbusError::Timeout { partial_bytes: 0 } => "timeout",
            ModbusError::Timeout { .. } => "timeout_partial_frame",
            ModbusError::Closed { partial_bytes: 0 } => "closed",
            ModbusError::Closed { .. } => "closed_partial_frame",
            ModbusError::Io(_) => "io",
            ModbusError::Frame(_) => "bad_header",
            ModbusError::TransactionMismatch { .. } => "transaction_mismatch",
            ModbusError::UnitMismatch { .. } => "unit_mismatch",
            ModbusError::Response(ResponseError::Exception(_)) => "exception",
            ModbusError::Response(_) => "incoherent_response",
            ModbusError::Poisoned => "poisoned",
        }
    }
}

impl fmt::Display for ModbusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModbusError::Timeout { partial_bytes } => write!(f, "response timeout ({partial_bytes} bytes of a frame)"),
            ModbusError::Closed { partial_bytes } => write!(f, "connection closed ({partial_bytes} bytes of a frame)"),
            ModbusError::Io(e) => write!(f, "i/o: {e}"),
            ModbusError::Frame(e) => write!(f, "{e}"),
            ModbusError::TransactionMismatch { expected, got } => {
                write!(f, "transaction id {got}, expected {expected}")
            }
            ModbusError::UnitMismatch { expected, got } => write!(f, "unit id {got}, expected {expected}"),
            ModbusError::Response(e) => write!(f, "{e}"),
            ModbusError::Poisoned => f.write_str("connection poisoned by an earlier failure"),
        }
    }
}

impl std::error::Error for ModbusError {}

/// Counters of one client (monotonic for the life of the client object).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ClientCounters {
    pub requests: u64,
    pub responses_ok: u64,
    pub exceptions: u64,
    pub timeouts: u64,
    pub closed: u64,
    pub framing_errors: u64,
    pub transaction_mismatches: u64,
    pub unit_mismatches: u64,
    pub incoherent_responses: u64,
    pub io_errors: u64,
}

/// One Modbus TCP connection to one unit id.
#[derive(Debug)]
pub struct Client<S: Transport> {
    stream: S,
    unit_id: u8,
    response_timeout: Duration,
    next_tid: u16,
    decoder: AduDecoder,
    wbuf: Vec<u8>,
    poisoned: bool,
    pub counters: ClientCounters,
}

/// Connect to the first reachable address of `host:port` (IPv4, IPv6 or a name) within
/// `timeout` per address. Name resolution uses the system resolver (blocking).
pub fn connect_tcp(host: &str, port: u16, timeout: Duration) -> io::Result<TcpStream> {
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.take(8).collect();
    let mut last = io::Error::new(io::ErrorKind::NotFound, "no address");
    for a in addrs {
        match TcpStream::connect_timeout(&a, timeout) {
            Ok(s) => {
                s.set_nodelay(true)?;
                s.set_write_timeout(Some(timeout))?;
                return Ok(s);
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

impl<S: Transport> Client<S> {
    pub fn new(stream: S, unit_id: u8, response_timeout: Duration) -> Client<S> {
        Client {
            stream,
            unit_id,
            response_timeout,
            next_tid: 1,
            decoder: AduDecoder::new(),
            wbuf: Vec::with_capacity(mbap::MAX_ADU_BYTES),
            poisoned: false,
            counters: ClientCounters::default(),
        }
    }

    pub fn unit_id(&self) -> u8 {
        self.unit_id
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Transaction id of the next request.
    pub fn next_transaction_id(&self) -> u16 {
        self.next_tid
    }

    /// Send one read request and wait for its validated response.
    pub fn read(&mut self, req: &ReadRequest) -> Result<ReadData, ModbusError> {
        if self.poisoned {
            return Err(ModbusError::Poisoned);
        }
        let r = self.read_inner(req);
        match &r {
            Ok(_) => self.counters.responses_ok += 1,
            Err(e) => {
                match e {
                    ModbusError::Timeout { .. } => self.counters.timeouts += 1,
                    ModbusError::Closed { .. } => self.counters.closed += 1,
                    ModbusError::Io(_) => self.counters.io_errors += 1,
                    ModbusError::Frame(_) => self.counters.framing_errors += 1,
                    ModbusError::TransactionMismatch { .. } => self.counters.transaction_mismatches += 1,
                    ModbusError::UnitMismatch { .. } => self.counters.unit_mismatches += 1,
                    ModbusError::Response(ResponseError::Exception(_)) => self.counters.exceptions += 1,
                    ModbusError::Response(_) => self.counters.incoherent_responses += 1,
                    ModbusError::Poisoned => {}
                }
                if e.is_fatal_for_connection() {
                    self.poisoned = true;
                    self.decoder.reset();
                }
            }
        }
        r
    }

    fn read_inner(&mut self, req: &ReadRequest) -> Result<ReadData, ModbusError> {
        let tid = self.next_tid;
        self.next_tid = self.next_tid.wrapping_add(1);
        self.counters.requests += 1;
        // One request at a time: nothing may be buffered from before.
        self.decoder.reset();
        mbap::encode_adu(tid, self.unit_id, &req.encode(), &mut self.wbuf);
        self.stream.write_all(&self.wbuf).map_err(ModbusError::Io)?;
        self.stream.flush().map_err(ModbusError::Io)?;
        let deadline = Instant::now() + self.response_timeout;
        let adu = loop {
            if let Some(f) = self.decoder.next_frame().map_err(ModbusError::Frame)? {
                break f;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(ModbusError::Timeout { partial_bytes: self.decoder.buffered() });
            }
            self.stream.set_read_timeout(Some(left)).map_err(ModbusError::Io)?;
            let spare = self.decoder.spare();
            match self.stream.read(spare) {
                Ok(0) => return Err(ModbusError::Closed { partial_bytes: self.decoder.buffered() }),
                Ok(n) => self.decoder.advance(n),
                Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                    return Err(ModbusError::Timeout { partial_bytes: self.decoder.buffered() });
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(ModbusError::Io(e)),
            }
        };
        if adu.header.transaction_id != tid {
            return Err(ModbusError::TransactionMismatch { expected: tid, got: adu.header.transaction_id });
        }
        if adu.header.unit_id != self.unit_id {
            return Err(ModbusError::UnitMismatch { expected: self.unit_id, got: adu.header.unit_id });
        }
        pdu::decode_response(req, &adu.pdu).map_err(ModbusError::Response)
    }
}
