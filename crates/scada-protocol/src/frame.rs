//! Bounded length-prefixed framing (blocking `std::io` flavour; the async kernel reuses
//! [`check_length`]).

use std::fmt;
use std::io::{self, Read, Write};

use prost::Message;

/// Size of the length prefix.
pub const HEADER_LEN: usize = 4;
/// Absolute ceiling for any configured `max_frame_bytes` (16 MiB).
pub const MAX_FRAME_BYTES_CEILING: u32 = 16 * 1024 * 1024;

/// Framing or decoding failure.
#[derive(Debug)]
pub enum FrameError {
    /// Declared length 0.
    ZeroLength,
    /// Declared length above the limit; nothing was allocated for it.
    TooLarge {
        len: u32,
        max: u32,
    },
    /// The stream ended inside a frame (header or payload).
    Truncated,
    /// The payload is not a valid message.
    Decode(prost::DecodeError),
    /// An outgoing message does not fit in `max`.
    EncodeTooLarge {
        len: usize,
        max: u32,
    },
    Io(io::Error),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::ZeroLength => f.write_str("frame length 0 is invalid"),
            FrameError::TooLarge { len, max } => write!(f, "frame length {len} exceeds {max}"),
            FrameError::Truncated => f.write_str("stream ended inside a frame"),
            FrameError::Decode(e) => write!(f, "malformed message: {e}"),
            FrameError::EncodeTooLarge { len, max } => write!(f, "message of {len} bytes exceeds {max}"),
            FrameError::Io(e) => write!(f, "i/o: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof { FrameError::Truncated } else { FrameError::Io(e) }
    }
}

/// Validate a length prefix against `max` (must happen before allocating).
pub fn check_length(header: [u8; HEADER_LEN], max: u32) -> Result<usize, FrameError> {
    let len = u32::from_be_bytes(header);
    if len == 0 {
        return Err(FrameError::ZeroLength);
    }
    if len > max {
        return Err(FrameError::TooLarge { len, max });
    }
    Ok(len as usize)
}

/// Encode `msg` as one frame into `out` (cleared first). `out` is reused by callers.
pub fn encode_frame<M: Message>(msg: &M, max: u32, out: &mut Vec<u8>) -> Result<(), FrameError> {
    let len = msg.encoded_len();
    let len32 = u32::try_from(len).map_err(|_| FrameError::EncodeTooLarge { len, max })?;
    if len32 == 0 || len32 > max {
        return Err(FrameError::EncodeTooLarge { len, max });
    }
    out.clear();
    out.reserve(len.saturating_add(HEADER_LEN));
    out.extend_from_slice(&len32.to_be_bytes());
    msg.encode(out).map_err(|_| FrameError::EncodeTooLarge { len, max })?;
    Ok(())
}

/// Read one frame. Returns `Ok(None)` on a clean end of stream at a frame boundary.
/// `buf` is resized to the payload length (never above `max`) and reused across calls.
pub fn read_frame<'a, R: Read>(r: &mut R, max: u32, buf: &'a mut Vec<u8>) -> Result<Option<&'a [u8]>, FrameError> {
    let mut header = [0u8; HEADER_LEN];
    let mut got = 0;
    while got < HEADER_LEN {
        let Some(rest) = header.get_mut(got..) else { break };
        match r.read(rest) {
            Ok(0) if got == 0 => return Ok(None),
            Ok(0) => return Err(FrameError::Truncated),
            Ok(n) => got = got.saturating_add(n),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    let len = check_length(header, max)?;
    buf.clear();
    buf.resize(len, 0);
    r.read_exact(buf)?;
    Ok(Some(buf.as_slice()))
}

/// Read and decode one message frame.
pub fn read_message<M: Message + Default, R: Read>(
    r: &mut R,
    max: u32,
    buf: &mut Vec<u8>,
) -> Result<Option<M>, FrameError> {
    match read_frame(r, max, buf)? {
        None => Ok(None),
        Some(payload) => M::decode(payload).map(Some).map_err(FrameError::Decode),
    }
}

/// Encode and write one message frame.
pub fn write_message<M: Message, W: Write>(
    w: &mut W,
    msg: &M,
    max: u32,
    scratch: &mut Vec<u8>,
) -> Result<(), FrameError> {
    encode_frame(msg, max, scratch)?;
    w.write_all(scratch)?;
    Ok(())
}
