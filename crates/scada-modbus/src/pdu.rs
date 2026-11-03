//! Read requests and responses of FC01..FC04 (Modbus Application Protocol V1.1b3 §6.1–6.4).

use std::fmt;

use crate::{Area, MAX_PDU_BYTES};

/// One read request. Constructed only through [`ReadRequest::new`], which checks the
/// quantity and the address range **before** any I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadRequest {
    area: Area,
    address: u16,
    quantity: u16,
}

/// Invalid request parameters (never sent).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestError {
    /// Quantity 0 or above the limit of the area (2000 bits, 125 registers).
    Quantity { area: Area, quantity: u16 },
    /// `address + quantity` exceeds the 16-bit address space (65536 items).
    AddressOverflow { address: u16, quantity: u16 },
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestError::Quantity { area, quantity } => {
                write!(f, "quantity {quantity} invalid for {area} (1..={})", area.max_quantity())
            }
            RequestError::AddressOverflow { address, quantity } => {
                write!(f, "address {address} + quantity {quantity} exceeds 65536")
            }
        }
    }
}

impl std::error::Error for RequestError {}

impl ReadRequest {
    pub fn new(area: Area, address: u16, quantity: u16) -> Result<ReadRequest, RequestError> {
        if quantity == 0 || quantity > area.max_quantity() {
            return Err(RequestError::Quantity { area, quantity });
        }
        if u32::from(address) + u32::from(quantity) > 65_536 {
            return Err(RequestError::AddressOverflow { address, quantity });
        }
        Ok(ReadRequest { area, address, quantity })
    }

    pub fn area(&self) -> Area {
        self.area
    }

    pub fn address(&self) -> u16 {
        self.address
    }

    pub fn quantity(&self) -> u16 {
        self.quantity
    }

    /// Exact data byte count of a correct response.
    pub fn expected_byte_count(&self) -> usize {
        let q = usize::from(self.quantity);
        if self.area.is_bit() { q.div_ceil(8) } else { q * 2 }
    }

    /// PDU bytes: function code, starting address, quantity (big endian).
    pub fn encode(&self) -> [u8; 5] {
        let a = self.address.to_be_bytes();
        let q = self.quantity.to_be_bytes();
        [self.area.function_code(), a[0], a[1], q[0], q[1]]
    }

    /// Parse a request PDU (used by the simulator and the fuzz target).
    pub fn decode(pdu: &[u8]) -> Result<ReadRequest, PduDecodeError> {
        let [fc, a0, a1, q0, q1] = <[u8; 5]>::try_from(pdu).map_err(|_| PduDecodeError::Length(pdu.len()))?;
        let area = Area::from_function_code(fc).ok_or(PduDecodeError::Function(fc))?;
        ReadRequest::new(area, u16::from_be_bytes([a0, a1]), u16::from_be_bytes([q0, q1]))
            .map_err(PduDecodeError::Request)
    }
}

/// A request PDU that cannot be parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PduDecodeError {
    Length(usize),
    Function(u8),
    Request(RequestError),
}

/// Validated response data, exactly `quantity` items.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadData {
    Bits(Vec<bool>),
    Registers(Vec<u16>),
}

impl ReadData {
    pub fn len(&self) -> usize {
        match self {
            ReadData::Bits(b) => b.len(),
            ReadData::Registers(r) => r.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Exception codes (Application Protocol §7). Unknown codes are kept as `Other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceptionCode {
    IllegalFunction,
    IllegalDataAddress,
    IllegalDataValue,
    ServerDeviceFailure,
    Acknowledge,
    ServerDeviceBusy,
    MemoryParityError,
    GatewayPathUnavailable,
    GatewayTargetFailedToRespond,
    Other(u8),
}

impl ExceptionCode {
    pub fn from_u8(c: u8) -> ExceptionCode {
        match c {
            0x01 => ExceptionCode::IllegalFunction,
            0x02 => ExceptionCode::IllegalDataAddress,
            0x03 => ExceptionCode::IllegalDataValue,
            0x04 => ExceptionCode::ServerDeviceFailure,
            0x05 => ExceptionCode::Acknowledge,
            0x06 => ExceptionCode::ServerDeviceBusy,
            0x08 => ExceptionCode::MemoryParityError,
            0x0A => ExceptionCode::GatewayPathUnavailable,
            0x0B => ExceptionCode::GatewayTargetFailedToRespond,
            other => ExceptionCode::Other(other),
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            ExceptionCode::IllegalFunction => 0x01,
            ExceptionCode::IllegalDataAddress => 0x02,
            ExceptionCode::IllegalDataValue => 0x03,
            ExceptionCode::ServerDeviceFailure => 0x04,
            ExceptionCode::Acknowledge => 0x05,
            ExceptionCode::ServerDeviceBusy => 0x06,
            ExceptionCode::MemoryParityError => 0x08,
            ExceptionCode::GatewayPathUnavailable => 0x0A,
            ExceptionCode::GatewayTargetFailedToRespond => 0x0B,
            ExceptionCode::Other(c) => c,
        }
    }
}

/// A response PDU that does not answer the request it is matched with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseError {
    /// Exception response (function code | 0x80, code). The device answered; the data is
    /// unavailable.
    Exception(ExceptionCode),
    /// Empty PDU.
    Empty,
    /// The function code is neither the request's nor its exception.
    FunctionMismatch { expected: u8, got: u8 },
    /// The byte count field differs from the one implied by the requested quantity.
    ByteCount { expected: usize, got: usize },
    /// The PDU length disagrees with its byte count (or an exception PDU is not 2 bytes).
    Length { expected: usize, got: usize },
    /// FC01/FC02: padding bits of the last byte are not zero (Application Protocol §6.1:
    /// "padded with zeros"). Treated as an incoherent response.
    NonZeroPadding,
}

impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseError::Exception(c) => write!(f, "exception 0x{:02x} ({c:?})", c.as_u8()),
            ResponseError::Empty => f.write_str("empty PDU"),
            ResponseError::FunctionMismatch { expected, got } => {
                write!(f, "function 0x{got:02x} does not answer 0x{expected:02x}")
            }
            ResponseError::ByteCount { expected, got } => write!(f, "byte count {got}, expected {expected}"),
            ResponseError::Length { expected, got } => write!(f, "PDU length {got}, expected {expected}"),
            ResponseError::NonZeroPadding => f.write_str("non-zero padding bits"),
        }
    }
}

impl std::error::Error for ResponseError {}

/// Validate a response PDU against `req` and return exactly `req.quantity()` items.
pub fn decode_response(req: &ReadRequest, pdu: &[u8]) -> Result<ReadData, ResponseError> {
    let fc = req.area().function_code();
    let (&got_fc, rest) = pdu.split_first().ok_or(ResponseError::Empty)?;
    if got_fc == fc | 0x80 {
        return match rest {
            [code] => Err(ResponseError::Exception(ExceptionCode::from_u8(*code))),
            _ => Err(ResponseError::Length { expected: 2, got: pdu.len() }),
        };
    }
    if got_fc != fc {
        return Err(ResponseError::FunctionMismatch { expected: fc, got: got_fc });
    }
    let (&count, data) = rest.split_first().ok_or(ResponseError::Length { expected: 2, got: pdu.len() })?;
    let expected = req.expected_byte_count();
    if usize::from(count) != expected {
        return Err(ResponseError::ByteCount { expected, got: usize::from(count) });
    }
    if data.len() != expected {
        return Err(ResponseError::Length { expected: expected + 2, got: pdu.len() });
    }
    let q = usize::from(req.quantity());
    if req.area().is_bit() {
        let mut bits = Vec::with_capacity(q);
        for i in 0..q {
            let byte = data.get(i / 8).copied().unwrap_or(0);
            bits.push(byte >> (i % 8) & 1 == 1);
        }
        let used = q % 8;
        if used != 0 {
            let last = data.last().copied().unwrap_or(0);
            if last >> used != 0 {
                return Err(ResponseError::NonZeroPadding);
            }
        }
        Ok(ReadData::Bits(bits))
    } else {
        Ok(ReadData::Registers(
            data.chunks_exact(2).map(|c| <[u8; 2]>::try_from(c).map_or(0, u16::from_be_bytes)).collect(),
        ))
    }
}

/// Encode a correct response PDU for `req` (simulator / tests). `data` must have
/// `req.quantity()` items of the request's kind; extra items are ignored, missing ones are 0.
pub fn encode_response(req: &ReadRequest, data: &ReadData, out: &mut Vec<u8>) {
    out.clear();
    out.push(req.area().function_code());
    let n = req.expected_byte_count();
    out.push(u8::try_from(n).unwrap_or(u8::MAX));
    let q = usize::from(req.quantity());
    match data {
        ReadData::Bits(bits) => {
            let start = out.len();
            out.resize(start + n, 0);
            for (i, b) in bits.iter().take(q).enumerate() {
                if *b && let Some(byte) = out.get_mut(start + i / 8) {
                    *byte |= 1 << (i % 8);
                }
            }
        }
        ReadData::Registers(regs) => {
            for i in 0..q {
                out.extend_from_slice(&regs.get(i).copied().unwrap_or(0).to_be_bytes());
            }
        }
    }
    debug_assert!(out.len() <= MAX_PDU_BYTES);
}

/// Exception response PDU.
pub fn encode_exception(function_code: u8, code: ExceptionCode) -> [u8; 2] {
    [function_code | 0x80, code.as_u8()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_and_address_limits_checked_before_io() {
        assert!(ReadRequest::new(Area::Coil, 0, 2000).is_ok());
        assert_eq!(
            ReadRequest::new(Area::Coil, 0, 2001),
            Err(RequestError::Quantity { area: Area::Coil, quantity: 2001 })
        );
        assert!(ReadRequest::new(Area::HoldingRegister, 0, 125).is_ok());
        assert!(ReadRequest::new(Area::InputRegister, 0, 126).is_err());
        assert!(ReadRequest::new(Area::DiscreteInput, 0, 0).is_err());
        assert!(ReadRequest::new(Area::HoldingRegister, 65_535, 1).is_ok());
        assert_eq!(
            ReadRequest::new(Area::HoldingRegister, 65_535, 2),
            Err(RequestError::AddressOverflow { address: 65_535, quantity: 2 })
        );
    }

    #[test]
    fn encodes_zero_based_address() {
        // Register "40001" of vendor documentation is PDU address 0: nothing is added here.
        let r = ReadRequest::new(Area::HoldingRegister, 0, 2).unwrap();
        assert_eq!(r.encode(), [0x03, 0x00, 0x00, 0x00, 0x02]);
        let r = ReadRequest::new(Area::Coil, 0x0013, 0x0013).unwrap();
        // Application Protocol §6.1 example: read coils 20-38.
        assert_eq!(r.encode(), [0x01, 0x00, 0x13, 0x00, 0x13]);
    }

    #[test]
    fn spec_example_fc01() {
        let r = ReadRequest::new(Area::Coil, 19, 19).unwrap();
        // §6.1: response 01 03 CD 6B 05.
        let d = decode_response(&r, &[0x01, 0x03, 0xCD, 0x6B, 0x05]).unwrap();
        let ReadData::Bits(b) = d else { panic!() };
        assert_eq!(b.len(), 19);
        // Coils 27..20 = CD = 1100 1101 (LSB = coil 20).
        assert_eq!(&b[0..8], &[true, false, true, true, false, false, true, true]);
        // 3 padding bits of 0x05 are zero.
        assert_eq!(&b[16..19], &[true, false, true]);
    }

    #[test]
    fn spec_example_fc03() {
        let r = ReadRequest::new(Area::HoldingRegister, 107, 3).unwrap();
        // §6.3: 03 06 02 2B 00 00 00 64.
        let d = decode_response(&r, &[0x03, 0x06, 0x02, 0x2B, 0x00, 0x00, 0x00, 0x64]).unwrap();
        assert_eq!(d, ReadData::Registers(vec![555, 0, 100]));
    }

    #[test]
    fn rejects_incoherent_responses() {
        let r = ReadRequest::new(Area::InputRegister, 8, 1).unwrap();
        assert_eq!(decode_response(&r, &[]), Err(ResponseError::Empty));
        assert_eq!(
            decode_response(&r, &[0x84, 0x02]),
            Err(ResponseError::Exception(ExceptionCode::IllegalDataAddress))
        );
        assert_eq!(decode_response(&r, &[0x84]), Err(ResponseError::Length { expected: 2, got: 1 }));
        assert_eq!(decode_response(&r, &[0x84, 0x02, 0x00]), Err(ResponseError::Length { expected: 2, got: 3 }));
        assert_eq!(
            decode_response(&r, &[0x03, 0x02, 0, 1]),
            Err(ResponseError::FunctionMismatch { expected: 4, got: 3 })
        );
        assert_eq!(
            decode_response(&r, &[0x04, 0x04, 0, 1, 0, 2]),
            Err(ResponseError::ByteCount { expected: 2, got: 4 })
        );
        assert_eq!(decode_response(&r, &[0x04, 0x02, 0]), Err(ResponseError::Length { expected: 4, got: 3 }));
        assert_eq!(decode_response(&r, &[0x04, 0x02, 0, 1, 9]), Err(ResponseError::Length { expected: 4, got: 5 }));
        assert_eq!(decode_response(&r, &[0x04]), Err(ResponseError::Length { expected: 2, got: 1 }));
        let b = ReadRequest::new(Area::DiscreteInput, 0, 3).unwrap();
        assert_eq!(decode_response(&b, &[0x02, 0x01, 0x08]), Err(ResponseError::NonZeroPadding));
        assert!(decode_response(&b, &[0x02, 0x01, 0x07]).is_ok());
    }

    #[test]
    fn roundtrip_request_and_response() {
        for area in Area::ALL {
            for q in [1u16, 7, 8, 9, area.max_quantity()] {
                let r = ReadRequest::new(area, 100, q).unwrap();
                assert_eq!(ReadRequest::decode(&r.encode()).unwrap(), r);
                let data = if area.is_bit() {
                    ReadData::Bits((0..q).map(|i| i % 3 == 0).collect())
                } else {
                    ReadData::Registers((0..q).map(|i| i.wrapping_mul(977)).collect())
                };
                let mut pdu = Vec::new();
                encode_response(&r, &data, &mut pdu);
                assert!(pdu.len() <= MAX_PDU_BYTES);
                assert_eq!(decode_response(&r, &pdu).unwrap(), data);
            }
        }
    }
}
