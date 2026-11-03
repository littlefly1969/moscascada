//! Incremental, bounded reader of `scada.driver.v1` frames (u32 BE length + protobuf).
//!
//! Unlike the blocking helpers of `scada_protocol::frame`, a read timeout never loses the
//! bytes of a partial frame: they stay buffered until the rest arrives. The buffer never
//! holds more than one maximal frame plus one read chunk.

use std::io::{self, Read};

use scada_protocol::Message;
use scada_protocol::frame::{FrameError, HEADER_LEN, check_length};

const CHUNK: usize = 8192;

#[derive(Debug)]
pub struct FrameReader {
    buf: Vec<u8>,
    max_frame: u32,
}

/// Result of one [`FrameReader::fill`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fill {
    /// `n > 0` bytes appended.
    Data(usize),
    /// The read timed out (nothing appended).
    Timeout,
    /// Clean end of stream.
    Eof,
}

impl FrameReader {
    pub fn new(max_frame: u32) -> FrameReader {
        FrameReader { buf: Vec::with_capacity(CHUNK), max_frame }
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    pub fn clear(&mut self) {
        self.buf.clear();
    }

    /// One `read` call (the caller sets the stream's read timeout).
    pub fn fill<R: Read>(&mut self, r: &mut R) -> io::Result<Fill> {
        let mut chunk = [0u8; CHUNK];
        loop {
            return match r.read(&mut chunk) {
                Ok(0) => Ok(Fill::Eof),
                Ok(n) => {
                    self.buf.extend_from_slice(chunk.get(..n).unwrap_or_default());
                    Ok(Fill::Data(n))
                }
                Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => Ok(Fill::Timeout),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => Err(e),
            };
        }
    }

    /// Decode the next complete frame, if any. A length above the limit is rejected as soon
    /// as the 4 header bytes are present.
    pub fn next_message<M: Message + Default>(&mut self) -> Result<Option<M>, FrameError> {
        let Some(h) = self.buf.get(..HEADER_LEN) else { return Ok(None) };
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(h);
        let len = check_length(header, self.max_frame)?;
        let total = HEADER_LEN.saturating_add(len);
        let Some(payload) = self.buf.get(HEADER_LEN..total) else { return Ok(None) };
        let msg = M::decode(payload).map_err(FrameError::Decode)?;
        self.buf.drain(..total);
        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scada_protocol::driver_v1::{InputAck, KernelMessage, kernel_message};
    use scada_protocol::frame::encode_frame;

    #[test]
    fn partial_frames_survive_timeouts() {
        let msg = KernelMessage {
            body: Some(kernel_message::Body::Ack(InputAck { source_sequence: 7, ..Default::default() })),
        };
        let mut bytes = Vec::new();
        encode_frame(&msg, 1024, &mut bytes).unwrap();
        let mut two = bytes.clone();
        two.extend_from_slice(&bytes);
        let mut r = FrameReader::new(1024);
        for (i, b) in two.iter().enumerate() {
            let mut one: &[u8] = std::slice::from_ref(b);
            assert_eq!(r.fill(&mut one).unwrap(), Fill::Data(1));
            let got: Option<KernelMessage> = r.next_message().unwrap();
            if i + 1 == bytes.len() || i + 1 == two.len() {
                assert_eq!(got.as_ref(), Some(&msg));
            } else {
                assert!(got.is_none());
            }
        }
        assert_eq!(r.buffered(), 0);
    }

    #[test]
    fn oversized_and_zero_lengths_rejected() {
        let mut r = FrameReader::new(16);
        let mut data: &[u8] = &[0, 0, 0, 17];
        r.fill(&mut data).unwrap();
        assert!(matches!(r.next_message::<KernelMessage>(), Err(FrameError::TooLarge { len: 17, max: 16 })));
        let mut r = FrameReader::new(16);
        let mut data: &[u8] = &[0, 0, 0, 0];
        r.fill(&mut data).unwrap();
        assert!(matches!(r.next_message::<KernelMessage>(), Err(FrameError::ZeroLength)));
    }
}
