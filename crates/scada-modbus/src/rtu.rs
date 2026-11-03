//! VS-008: pure Modbus RTU wire framing and serial timing (Serial Line V1.02).
//! This module does not access a serial device. One bus arbiter per RS-485 port must
//! enforce t3.5 silence and serialize read requests across all slaves. Userspace
//! read() timing on USB/serial is NOT sufficient to prove t1.5 inter-character timing.
//! Read-only FC01..FC04 reuses the existing PDU implementation.

use std::fmt;
use std::time::Duration;
use crate::pdu::{self, PduDecodeError, ReadData, ReadRequest, ResponseError};

/// Slave + PDU + CRC, in bytes.
pub const MAX_RTU_ADU_BYTES: usize = 256;
pub const MAX_SLAVE_ADDRESS: u8 = 247;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtuError {
    InvalidAddress(u8),
    AddressMismatch { expected: u8, got: u8 },
    FrameLength(usize),
    CrcMismatch { expected: u16, got: u16 },
    Request(PduDecodeError),
    Response(ResponseError),
}
impl fmt::Display for RtuError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAddress(a) => write!(f, "invalid RTU unicast slave address {a}"),
            Self::AddressMismatch { expected, got } => write!(f, "RTU slave address {got}, expected {expected}"),
            Self::FrameLength(n) => write!(f, "invalid RTU frame length {n}"),
            Self::CrcMismatch { expected, got } => write!(f, "RTU CRC 0x{got:04x}, expected 0x{expected:04x}"),
            Self::Request(e) => write!(f, "RTU request: {e:?}"),
            Self::Response(e) => write!(f, "RTU response: {e}"),
        }
    }
}
impl std::error::Error for RtuError {}

/// CRC16, polynomial 0xA001, register initialized to 0xffff; CRC low octet first.
pub fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0xffffu16;
    for &byte in bytes {
        crc ^= u16::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 { (crc >> 1) ^ 0xa001 } else { crc >> 1 };
        }
    }
    crc
}

fn valid_slave(address: u8) -> Result<(), RtuError> {
    if (1..=MAX_SLAVE_ADDRESS).contains(&address) { Ok(()) } else { Err(RtuError::InvalidAddress(address)) }
}

/// Assemble a read-only request. Reject broadcast address 0 and reserved 248..255.
pub fn encode_read_request(slave: u8, request: &ReadRequest, out: &mut Vec<u8>) -> Result<(), RtuError> {
    valid_slave(slave)?;
    out.clear();
    out.extend_from_slice(&[slave]);
    out.extend_from_slice(&request.encode());
    let check = crc16(out);
    out.extend_from_slice(&check.to_le_bytes());
    Ok(())
}

/// Validated ADU. Caller must first delimit an entire serial frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtuAdu<'a> {
    pub slave: u8,
    pub pdu: &'a [u8],
}

/// Check frame bounds, CRC and slave address, without allocating from untrusted lengths.
/// Does not guess frame boundaries or retry after a broken frame.
pub fn decode_adu(frame: &[u8], expected_slave: u8) -> Result<RtuAdu<'_>, RtuError> {
    valid_slave(expected_slave)?;
    if !(4..=MAX_RTU_ADU_BYTES).contains(&frame.len()) {
        return Err(RtuError::FrameLength(frame.len()));
    }
    let (data, received) = frame.split_at(frame.len() - 2);
    let actual_crc = u16::from_le_bytes([received[0], received[1]]);
    let expected_crc = crc16(data);
    if actual_crc != expected_crc {
        return Err(RtuError::CrcMismatch { expected: expected_crc, got: actual_crc });
    }
    let address = data[0];
    if address != expected_slave {
        return Err(RtuError::AddressMismatch { expected: expected_slave, got: address });
    }
    Ok(RtuAdu { slave: address, pdu: &data[1..] })
}

/// Strict RTU response decoding using the existing read-only PDU implementation.
pub fn decode_read_response(frame: &[u8], slave: u8, req: &ReadRequest) -> Result<ReadData, RtuError> {
    let adu = decode_adu(frame, slave)?;
    pdu::decode_response(req, adu.pdu).map_err(RtuError::Response)
}

/// Helper for serial-line simulators: reads only FC01..FC04.
pub fn decode_read_request(frame: &[u8]) -> Result<(u8, ReadRequest), RtuError> {
    let slave = frame.first().copied().ok_or(RtuError::FrameLength(0))?;
    let adu = decode_adu(frame, slave)?;
    let req = ReadRequest::decode(adu.pdu).map_err(RtuError::Request)?;
    Ok((slave, req))
}

/// 11 bits/character for 8E1, 8O1 or 8N2; optional 10 bits for 8N1.
/// At >19200 baud use the fixed t1.5=750us and t3.5=1750us from the spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RtuTiming {
    pub baud: u32,
    pub bits_per_character: u8,
    pub t1_5: Duration,
    pub t3_5: Duration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimingError {
    ZeroBaud,
    InvalidCharacterBits(u8),
}
impl fmt::Display for TimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for TimingError {}
impl RtuTiming {
    pub fn new(baud: u32, bits_per_character: u8) -> Result<Self, TimingError> {
        if baud == 0 { return Err(TimingError::ZeroBaud); }
        if !(10..=12).contains(&bits_per_character) {
            return Err(TimingError::InvalidCharacterBits(bits_per_character));
        }
        let (t1_5, t3_5) = if baud > 19_200 {
            (Duration::from_micros(750), Duration::from_micros(1_750))
        } else {
            let denominator = u64::from(baud) * 2;
            let period = |half_chars: u64| -> Duration {
                let nanos = u64::from(bits_per_character) * half_chars * 1_000_000_000;
                Duration::from_nanos(nanos / denominator + u64::from(nanos % denominator != 0))
            };
            (period(3), period(7))
        };
        Ok(Self { baud, bits_per_character, t1_5, t3_5 })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Area;
    fn holding() -> ReadRequest { ReadRequest::new(Area::HoldingRegister, 0, 10).unwrap() }

    #[test]
    fn reference_crc_and_wire_order() {
        let mut frame = Vec::new();
        encode_read_request(1, &holding(), &mut frame).unwrap();
        assert_eq!(frame, [1, 3, 0, 0, 0, 10, 0xc5, 0xcd]);
        assert_eq!(crc16(&frame), 0);
        assert_eq!(decode_read_request(&frame).unwrap(), (1, holding()));
    }

    #[test]
    fn reject_broadcast_reserved_mismatch_and_corruption() {
        let mut frame = Vec::new();
        for slave in [0, 248, 255] {
            assert_eq!(encode_read_request(slave, &holding(), &mut frame), Err(RtuError::InvalidAddress(slave)));
        }
        encode_read_request(7, &holding(), &mut frame).unwrap();
        assert!(matches!(decode_adu(&frame, 8), Err(RtuError::AddressMismatch { .. })));
        assert_eq!(decode_adu(&frame, 0), Err(RtuError::InvalidAddress(0)));
        frame[2] ^= 1;
        assert!(matches!(decode_adu(&frame, 7), Err(RtuError::CrcMismatch { .. })));
        assert_eq!(decode_adu(&[7, 3, 0], 7), Err(RtuError::FrameLength(3)));
        assert_eq!(decode_adu(&vec![0; 257], 7), Err(RtuError::FrameLength(257)));
    }

    #[test]
    fn response_reuses_strict_pdu_validation() {
        let req = ReadRequest::new(Area::InputRegister, 0, 2).unwrap();
        let mut frame = vec![7u8, 4, 4, 0x12, 0x34, 0xab, 0xcd];
        frame.extend_from_slice(&crc16(&frame).to_le_bytes());
        assert_eq!(decode_read_response(&frame, 7, &req).unwrap(), ReadData::Registers(vec![0x1234, 0xabcd]));
        frame[2] = 3;
        frame.truncate(7);
        frame.extend_from_slice(&crc16(&frame).to_le_bytes());
        assert!(matches!(decode_read_response(&frame, 7, &req),
            Err(RtuError::Response(pdu::ResponseError::ByteCount { .. }))));
    }

    #[test]
    fn serial_timing() {
        let slow = RtuTiming::new(9_600, 11).unwrap();
        assert_eq!(slow.t1_5, Duration::from_nanos(1_718_750));
        assert_eq!(slow.t3_5, Duration::from_nanos(4_010_417));
        let fast = RtuTiming::new(38_400, 11).unwrap();
        assert_eq!(fast.t1_5, Duration::from_micros(750));
        assert_eq!(fast.t3_5, Duration::from_micros(1_750));
        assert_eq!(RtuTiming::new(0, 11), Err(TimingError::ZeroBaud));
        assert_eq!(RtuTiming::new(9_600, 9), Err(TimingError::InvalidCharacterBits(9)));
    }
}
