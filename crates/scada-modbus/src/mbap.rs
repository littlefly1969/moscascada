//! Modbus TCP framing (Messaging Implementation Guide V1.0b §3.1.3).
//!
//! ADU = MBAP header (7 bytes) + PDU. MBAP = transaction id (u16), protocol id (u16, 0 for
//! Modbus), length (u16, number of following bytes = unit id + PDU), unit id (u8). There is
//! no CRC on TCP. The decoder below is incremental: bytes may arrive split over several TCP
//! segments or several frames may arrive in one segment; it never buffers more than one
//! maximal ADU and rejects a header before any allocation.

use std::fmt;

use crate::MAX_PDU_BYTES;

/// MBAP header length.
pub const MBAP_LEN: usize = 7;
/// Largest `length` field: unit id + maximal PDU.
pub const MAX_LENGTH_FIELD: u16 = 1 + MAX_PDU_BYTES as u16;
/// Largest ADU on TCP (260 bytes).
pub const MAX_ADU_BYTES: usize = MBAP_LEN - 1 + MAX_LENGTH_FIELD as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MbapHeader {
    pub transaction_id: u16,
    pub protocol_id: u16,
    /// Bytes following the length field (unit id + PDU).
    pub length: u16,
    pub unit_id: u8,
}

impl MbapHeader {
    pub fn parse(b: [u8; MBAP_LEN]) -> MbapHeader {
        MbapHeader {
            transaction_id: u16::from_be_bytes([b[0], b[1]]),
            protocol_id: u16::from_be_bytes([b[2], b[3]]),
            length: u16::from_be_bytes([b[4], b[5]]),
            unit_id: b[6],
        }
    }

    /// Length of the PDU announced by this header.
    pub fn pdu_len(&self) -> usize {
        usize::from(self.length).saturating_sub(1)
    }
}

/// A header that cannot start a valid Modbus TCP frame. After such an error the stream is
/// desynchronised: the connection must be closed (never resynchronised by guessing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    /// Protocol identifier is not 0 (Modbus).
    ProtocolId(u16),
    /// Length field 0 or 1 (no PDU) or above 254.
    Length(u16),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::ProtocolId(p) => write!(f, "MBAP protocol id {p} (expected 0)"),
            FrameError::Length(l) => write!(f, "MBAP length {l} outside 2..={MAX_LENGTH_FIELD}"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Validate a header (protocol id and length) before reading its PDU.
pub fn check_header(h: &MbapHeader) -> Result<(), FrameError> {
    if h.protocol_id != 0 {
        return Err(FrameError::ProtocolId(h.protocol_id));
    }
    if h.length < 2 || h.length > MAX_LENGTH_FIELD {
        return Err(FrameError::Length(h.length));
    }
    Ok(())
}

/// Encode one ADU into `out` (cleared first). `pdu` must be 1..=253 bytes.
pub fn encode_adu(transaction_id: u16, unit_id: u8, pdu: &[u8], out: &mut Vec<u8>) {
    out.clear();
    let len = u16::try_from(pdu.len().saturating_add(1)).unwrap_or(u16::MAX);
    out.extend_from_slice(&transaction_id.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out.push(unit_id);
    out.extend_from_slice(pdu);
}

/// Incremental ADU decoder with a fixed buffer of [`MAX_ADU_BYTES`].
#[derive(Debug)]
pub struct AduDecoder {
    buf: [u8; MAX_ADU_BYTES],
    len: usize,
}

impl Default for AduDecoder {
    fn default() -> Self {
        AduDecoder { buf: [0; MAX_ADU_BYTES], len: 0 }
    }
}

/// One complete frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Adu {
    pub header: MbapHeader,
    pub pdu: Vec<u8>,
}

impl AduDecoder {
    pub fn new() -> AduDecoder {
        AduDecoder::default()
    }

    /// Bytes buffered (part of a frame not yet complete).
    pub fn buffered(&self) -> usize {
        self.len
    }

    /// Drop any buffered partial frame (after a timeout or a reconnection: old bytes must
    /// never be combined with a new stream).
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Free space for the next read. Never larger than what the current frame still needs,
    /// so a read never consumes bytes of a following frame beyond one ADU.
    pub fn spare(&mut self) -> &mut [u8] {
        let need = self.needed();
        let end = self.len.saturating_add(need).min(MAX_ADU_BYTES);
        self.buf.get_mut(self.len..end).unwrap_or_default()
    }

    /// Account for `n` bytes written into [`AduDecoder::spare`].
    pub fn advance(&mut self, n: usize) {
        self.len = self.len.saturating_add(n).min(MAX_ADU_BYTES);
    }

    /// Bytes still missing for the current frame (header first).
    fn needed(&self) -> usize {
        if self.len < MBAP_LEN {
            return MBAP_LEN - self.len;
        }
        let h = self.header();
        let total = (MBAP_LEN - 1).saturating_add(usize::from(h.length)).min(MAX_ADU_BYTES);
        total.saturating_sub(self.len)
    }

    fn header(&self) -> MbapHeader {
        let mut h = [0u8; MBAP_LEN];
        if let Some(src) = self.buf.get(..MBAP_LEN) {
            h.copy_from_slice(src);
        }
        MbapHeader::parse(h)
    }

    /// Feed bytes; returns how many were consumed (stops at the end of one frame).
    pub fn push(&mut self, data: &[u8]) -> usize {
        let mut used = 0;
        // Twice at most: the header, then the rest of the frame it announces.
        for _ in 0..2 {
            let rest = data.get(used..).unwrap_or_default();
            let spare = self.spare();
            let n = spare.len().min(rest.len());
            if let (Some(dst), Some(src)) = (spare.get_mut(..n), rest.get(..n)) {
                dst.copy_from_slice(src);
            }
            self.advance(n);
            used += n;
            if n == 0 || self.len < MBAP_LEN || check_header(&self.header()).is_err() {
                break;
            }
        }
        used
    }

    /// Returns a complete frame if one is buffered. A bad header is reported as soon as the
    /// 7 header bytes are present, before waiting for (or allocating) its PDU.
    pub fn next_frame(&mut self) -> Result<Option<Adu>, FrameError> {
        if self.len < MBAP_LEN {
            return Ok(None);
        }
        let header = self.header();
        check_header(&header)?;
        let total = MBAP_LEN - 1 + usize::from(header.length);
        if self.len < total {
            return Ok(None);
        }
        let pdu = self.buf.get(MBAP_LEN..total).map(<[u8]>::to_vec).unwrap_or_default();
        self.len = 0;
        Ok(Some(Adu { header, pdu }))
    }
}

/// Decode as many frames as possible from a byte string (fuzzing / tests). Stops at the
/// first framing error.
pub fn decode_all(mut data: &[u8], max_frames: usize) -> (Vec<Adu>, Option<FrameError>) {
    let mut d = AduDecoder::new();
    let mut out = Vec::new();
    while !data.is_empty() && out.len() < max_frames {
        let n = d.push(data);
        data = data.get(n..).unwrap_or_default();
        match d.next_frame() {
            Ok(Some(f)) => out.push(f),
            Ok(None) => {
                if n == 0 {
                    break;
                }
            }
            Err(e) => return (out, Some(e)),
        }
    }
    (out, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adu(tid: u16, unit: u8, pdu: &[u8]) -> Vec<u8> {
        let mut v = Vec::new();
        encode_adu(tid, unit, pdu, &mut v);
        v
    }

    #[test]
    fn header_layout() {
        let v = adu(0x1234, 0x11, &[0x03, 0x00, 0x6B, 0x00, 0x03]);
        assert_eq!(v, [0x12, 0x34, 0, 0, 0, 6, 0x11, 0x03, 0x00, 0x6B, 0x00, 0x03]);
    }

    #[test]
    fn byte_by_byte_and_coalesced() {
        let mut stream = adu(1, 7, &[0x04, 0x02, 0x00, 0x2A]);
        stream.extend(adu(2, 7, &[0x84, 0x02]));
        // One byte at a time (worst TCP segmentation).
        let mut d = AduDecoder::new();
        let mut frames = Vec::new();
        for b in &stream {
            assert_eq!(d.push(std::slice::from_ref(b)), 1);
            if let Some(f) = d.next_frame().unwrap() {
                frames.push(f);
            }
        }
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].header.transaction_id, 1);
        assert_eq!(frames[1].pdu, vec![0x84, 0x02]);
        // All in one segment: push stops at the end of the first frame.
        let (all, err) = decode_all(&stream, 16);
        assert!(err.is_none());
        assert_eq!(all, frames);
    }

    #[test]
    fn bad_headers_rejected_before_payload() {
        let mut d = AduDecoder::new();
        d.push(&[0, 1, 0, 1, 0, 6, 1]);
        assert_eq!(d.next_frame(), Err(FrameError::ProtocolId(1)));
        for len in [0u16, 1, 255, u16::MAX] {
            let mut d = AduDecoder::new();
            let l = len.to_be_bytes();
            d.push(&[0, 1, 0, 0, l[0], l[1], 1]);
            assert_eq!(d.next_frame(), Err(FrameError::Length(len)));
        }
    }

    #[test]
    fn reset_discards_partial_frame() {
        let mut d = AduDecoder::new();
        let v = adu(9, 1, &[0x03, 0x02, 0x12, 0x34]);
        d.push(&v[..9]);
        assert_eq!(d.next_frame(), Ok(None));
        d.reset();
        assert_eq!(d.buffered(), 0);
        let w = adu(10, 1, &[0x03, 0x02, 0xAB, 0xCD]);
        d.push(&w);
        let f = d.next_frame().unwrap().unwrap();
        assert_eq!(f.header.transaction_id, 10);
        assert_eq!(f.pdu, vec![0x03, 0x02, 0xAB, 0xCD]);
    }

    #[test]
    fn maximal_frame_fits() {
        let pdu = vec![0x03; MAX_PDU_BYTES];
        let v = adu(1, 1, &pdu);
        assert_eq!(v.len(), MAX_ADU_BYTES);
        let (f, e) = decode_all(&v, 2);
        assert!(e.is_none());
        assert_eq!(f[0].pdu.len(), MAX_PDU_BYTES);
    }
}
