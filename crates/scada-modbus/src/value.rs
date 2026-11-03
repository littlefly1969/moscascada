//! Deterministic decoding of Modbus data to the two value types the MoScada RTDB accepts
//! today (`bool`, `i64`). Every conversion is exact or explicitly rounded, overflow is an
//! error, NaN/Inf are rejected; nothing depends on the platform (x86_64 and aarch64 give the
//! same results: IEEE-754 binary64 arithmetic without fused operations).

use std::fmt;

/// Value type of a point as stored in the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RawType {
    /// One coil or discrete input.
    Bit,
    /// Bit `n` (0 = least significant) of one register.
    RegisterBit(u8),
    U16,
    I16,
    /// Two registers.
    U32,
    /// Two registers, two's complement.
    I32,
    /// Two registers, IEEE-754 binary32. Accepted only with an explicit fixed-point scale.
    F32,
}

impl RawType {
    /// Number of items (bits or registers) the type occupies.
    pub fn width(self) -> u16 {
        match self {
            RawType::U32 | RawType::I32 | RawType::F32 => 2,
            _ => 1,
        }
    }

    /// Whether the decoded MoScada value is a `bool`.
    pub fn is_bool(self) -> bool {
        matches!(self, RawType::Bit | RawType::RegisterBit(_))
    }

    /// Whether the type lives in a bit area (coils / discrete inputs).
    pub fn needs_bit_area(self) -> bool {
        matches!(self, RawType::Bit)
    }

    /// Integer range of the raw value (`None` for bools and floats).
    pub fn integer_range(self) -> Option<(i64, i64)> {
        match self {
            RawType::U16 => Some((0, i64::from(u16::MAX))),
            RawType::I16 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
            RawType::U32 => Some((0, i64::from(u32::MAX))),
            RawType::I32 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
            _ => None,
        }
    }
}

/// Order of the two registers of a 32-bit value. Bytes inside a register are always big
/// endian (Modbus data model); `byte_swap` swaps them explicitly for devices that do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WordOrder {
    /// First register = most significant word ("ABCD").
    HighFirst,
    /// First register = least significant word ("CDAB").
    LowFirst,
}

/// Rounding of a scaled value that is not an integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rounding {
    /// Not an integer => decode error.
    Exact,
    /// Round half to even (banker's rounding).
    HalfEven,
    /// Toward zero.
    Truncate,
}

/// Linear fixed-point scale: `value = round(raw * numerator / denominator) + offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Scale {
    pub numerator: i64,
    /// > 0.
    pub denominator: i64,
    pub offset: i64,
    pub rounding: Rounding,
}

impl Scale {
    pub const IDENTITY: Scale = Scale { numerator: 1, denominator: 1, offset: 0, rounding: Rounding::Exact };

    pub fn is_identity(&self) -> bool {
        self.numerator == 1 && self.denominator == 1 && self.offset == 0
    }
}

/// Full description of how to decode one point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decoding {
    pub raw: RawType,
    pub word_order: WordOrder,
    pub byte_swap: bool,
    pub scale: Scale,
    /// Optional plausibility range of the final value (inclusive); outside => decode error.
    pub valid_range: Option<(i64, i64)>,
}

/// Decoded value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Value {
    Bool(bool),
    I64(i64),
}

/// A configuration that can never decode (found statically, before any I/O).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodingConfigError {
    Denominator(i64),
    Numerator,
    BitIndex(u8),
    ScaleOnBool,
    FloatWithoutScale,
    /// Some raw value of the type overflows `i64` after scaling.
    ScaleOverflow,
    RangeEmpty,
    RangeOnBool,
}

impl fmt::Display for DecodingConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodingConfigError::Denominator(d) => write!(f, "scale denominator {d} must be > 0"),
            DecodingConfigError::Numerator => f.write_str("scale numerator must not be 0"),
            DecodingConfigError::BitIndex(b) => write!(f, "register bit {b} outside 0..=15"),
            DecodingConfigError::ScaleOnBool => f.write_str("scale on a boolean point"),
            DecodingConfigError::FloatWithoutScale => {
                f.write_str("f32 needs an explicit fixed-point scale and rounding (half_even or truncate)")
            }
            DecodingConfigError::ScaleOverflow => f.write_str("scaled value can overflow i64"),
            DecodingConfigError::RangeEmpty => f.write_str("valid range min > max"),
            DecodingConfigError::RangeOnBool => f.write_str("valid range on a boolean point"),
        }
    }
}

impl std::error::Error for DecodingConfigError {}

/// A value that cannot be accepted (the read is then a failure, never a `Good` sample).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The image does not hold the point (planner bug or short read).
    Missing,
    NotFinite,
    /// Float magnitude above 2^53 or scaled value outside `i64`.
    Overflow,
    /// `Rounding::Exact` and the scaled value is not an integer.
    Inexact,
    /// Outside the configured plausibility range.
    OutOfRange(i64),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Missing => f.write_str("point not in the scan image"),
            DecodeError::NotFinite => f.write_str("NaN or infinite float"),
            DecodeError::Overflow => f.write_str("value overflows the fixed-point range"),
            DecodeError::Inexact => f.write_str("scaled value is not an integer (rounding = exact)"),
            DecodeError::OutOfRange(v) => write!(f, "value {v} outside the valid range"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Largest float magnitude converted (every integer up to it is exact in binary64).
const MAX_EXACT_F64: f64 = 9_007_199_254_740_992.0; // 2^53

impl Decoding {
    /// Static validation: every raw value of an integer type must scale without overflow.
    pub fn validate(&self) -> Result<(), DecodingConfigError> {
        if let RawType::RegisterBit(b) = self.raw
            && b > 15
        {
            return Err(DecodingConfigError::BitIndex(b));
        }
        if self.raw.is_bool() {
            if !self.scale.is_identity() {
                return Err(DecodingConfigError::ScaleOnBool);
            }
            if self.valid_range.is_some() {
                return Err(DecodingConfigError::RangeOnBool);
            }
            return Ok(());
        }
        if self.scale.denominator <= 0 {
            return Err(DecodingConfigError::Denominator(self.scale.denominator));
        }
        if self.scale.numerator == 0 {
            return Err(DecodingConfigError::Numerator);
        }
        if let Some((lo, hi)) = self.valid_range
            && lo > hi
        {
            return Err(DecodingConfigError::RangeEmpty);
        }
        match self.raw.integer_range() {
            Some((lo, hi)) => {
                // Linear and monotonic: checking both ends of the raw range is sufficient.
                // Truncate rounding for the check: the magnitude bound is the same.
                for raw in [lo, hi] {
                    scale_integer(raw, &Scale { rounding: Rounding::Truncate, ..self.scale })
                        .map_err(|_| DecodingConfigError::ScaleOverflow)?;
                }
                Ok(())
            }
            None => {
                // F32: rounding must be explicit (exact is meaningless for binary floats).
                if self.scale.rounding == Rounding::Exact {
                    return Err(DecodingConfigError::FloatWithoutScale);
                }
                Ok(())
            }
        }
    }

    /// Decode from bits (bit areas) or registers (register areas). `items` holds exactly
    /// the `raw.width()` items of the point (from the scan image).
    pub fn decode_bits(&self, bit: bool) -> Result<Value, DecodeError> {
        match self.raw {
            RawType::Bit => Ok(Value::Bool(bit)),
            _ => Err(DecodeError::Missing),
        }
    }

    pub fn decode_registers(&self, regs: &[u16]) -> Result<Value, DecodeError> {
        let reg = |i: usize| -> Result<u16, DecodeError> {
            let r = *regs.get(i).ok_or(DecodeError::Missing)?;
            Ok(if self.byte_swap { r.swap_bytes() } else { r })
        };
        let word32 = || -> Result<u32, DecodeError> {
            let (a, b) = (reg(0)?, reg(1)?);
            let (hi, lo) = match self.word_order {
                WordOrder::HighFirst => (a, b),
                WordOrder::LowFirst => (b, a),
            };
            Ok(u32::from(hi) << 16 | u32::from(lo))
        };
        let v = match self.raw {
            RawType::Bit => return Err(DecodeError::Missing),
            RawType::RegisterBit(b) => return Ok(Value::Bool(reg(0)? >> u32::from(b & 15) & 1 == 1)),
            RawType::U16 => scale_integer(i64::from(reg(0)?), &self.scale)?,
            RawType::I16 => scale_integer(i64::from(reg(0)? as i16), &self.scale)?,
            RawType::U32 => scale_integer(i64::from(word32()?), &self.scale)?,
            RawType::I32 => scale_integer(i64::from(word32()? as i32), &self.scale)?,
            RawType::F32 => scale_float(f32::from_bits(word32()?), &self.scale)?,
        };
        if let Some((lo, hi)) = self.valid_range
            && (v < lo || v > hi)
        {
            return Err(DecodeError::OutOfRange(v));
        }
        Ok(Value::I64(v))
    }
}

/// `round(raw * num / den) + offset` in 128-bit integers, overflow checked.
pub fn scale_integer(raw: i64, s: &Scale) -> Result<i64, DecodeError> {
    let n = i128::from(raw).checked_mul(i128::from(s.numerator)).ok_or(DecodeError::Overflow)?;
    let d = i128::from(s.denominator);
    if d <= 0 {
        return Err(DecodeError::Overflow);
    }
    let q = n.div_euclid(d);
    let r = n.rem_euclid(d); // 0 <= r < d, n = q*d + r
    let rounded = if r == 0 {
        q
    } else {
        match s.rounding {
            Rounding::Exact => return Err(DecodeError::Inexact),
            Rounding::Truncate => {
                if n < 0 {
                    q + 1
                } else {
                    q
                }
            }
            Rounding::HalfEven => {
                let twice = 2 * r;
                if twice > d || (twice == d && q % 2 != 0) { q + 1 } else { q }
            }
        }
    };
    let v = rounded.checked_add(i128::from(s.offset)).ok_or(DecodeError::Overflow)?;
    i64::try_from(v).map_err(|_| DecodeError::Overflow)
}

/// `round(f * num / den) + offset` for a binary32 value: finite only, magnitude after
/// scaling at most 2^53 (exactly representable), rounding explicit.
pub fn scale_float(f: f32, s: &Scale) -> Result<i64, DecodeError> {
    if !f.is_finite() {
        return Err(DecodeError::NotFinite);
    }
    if s.denominator <= 0 {
        return Err(DecodeError::Overflow);
    }
    // f32 -> f64 is exact; numerator and denominator are converted exactly when |x| <= 2^53.
    let (num, den) = (s.numerator as f64, s.denominator as f64);
    if num.abs() > MAX_EXACT_F64 || den > MAX_EXACT_F64 {
        return Err(DecodeError::Overflow);
    }
    let x = f64::from(f) * num / den;
    if !x.is_finite() || x.abs() > MAX_EXACT_F64 {
        return Err(DecodeError::Overflow);
    }
    let r = match s.rounding {
        Rounding::HalfEven => x.round_ties_even(),
        Rounding::Truncate => x.trunc(),
        Rounding::Exact => {
            if x.fract() != 0.0 {
                return Err(DecodeError::Inexact);
            }
            x
        }
    };
    // |r| <= 2^53: the cast is exact.
    (r as i64).checked_add(s.offset).ok_or(DecodeError::Overflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(raw: RawType) -> Decoding {
        Decoding { raw, word_order: WordOrder::HighFirst, byte_swap: false, scale: Scale::IDENTITY, valid_range: None }
    }

    #[test]
    fn integers_and_word_orders() {
        assert_eq!(dec(RawType::U16).decode_registers(&[0xFFFF]), Ok(Value::I64(65_535)));
        assert_eq!(dec(RawType::I16).decode_registers(&[0xFFFF]), Ok(Value::I64(-1)));
        assert_eq!(dec(RawType::U32).decode_registers(&[0x0001, 0x0002]), Ok(Value::I64(0x0001_0002)));
        let lo = Decoding { word_order: WordOrder::LowFirst, ..dec(RawType::U32) };
        assert_eq!(lo.decode_registers(&[0x0001, 0x0002]), Ok(Value::I64(0x0002_0001)));
        assert_eq!(dec(RawType::I32).decode_registers(&[0xFFFF, 0xFFFE]), Ok(Value::I64(-2)));
        let sw = Decoding { byte_swap: true, ..dec(RawType::U16) };
        assert_eq!(sw.decode_registers(&[0x1234]), Ok(Value::I64(0x3412)));
        assert_eq!(dec(RawType::RegisterBit(15)).decode_registers(&[0x8000]), Ok(Value::Bool(true)));
        assert_eq!(dec(RawType::RegisterBit(0)).decode_registers(&[0x8000]), Ok(Value::Bool(false)));
        assert_eq!(dec(RawType::U32).decode_registers(&[1]), Err(DecodeError::Missing));
    }

    #[test]
    fn integer_scale_rounding() {
        let s = |num, den, off, rounding| Scale { numerator: num, denominator: den, offset: off, rounding };
        assert_eq!(scale_integer(1234, &s(10, 1, 0, Rounding::Exact)), Ok(12_340));
        assert_eq!(scale_integer(5, &s(1, 2, 0, Rounding::Exact)), Err(DecodeError::Inexact));
        assert_eq!(scale_integer(5, &s(1, 2, 0, Rounding::HalfEven)), Ok(2));
        assert_eq!(scale_integer(7, &s(1, 2, 0, Rounding::HalfEven)), Ok(4));
        assert_eq!(scale_integer(-5, &s(1, 2, 0, Rounding::HalfEven)), Ok(-2));
        assert_eq!(scale_integer(-7, &s(1, 2, 0, Rounding::HalfEven)), Ok(-4));
        assert_eq!(scale_integer(-7, &s(1, 2, 0, Rounding::Truncate)), Ok(-3));
        assert_eq!(scale_integer(7, &s(1, 2, 0, Rounding::Truncate)), Ok(3));
        assert_eq!(scale_integer(-27315, &s(1, 1, 27315, Rounding::Exact)), Ok(0));
        assert_eq!(scale_integer(i64::MAX, &s(2, 1, 0, Rounding::Exact)), Err(DecodeError::Overflow));
    }

    #[test]
    fn float_fixed_point() {
        let centi = Scale { numerator: 100, denominator: 1, offset: 0, rounding: Rounding::HalfEven };
        let f = |x: f32| scale_float(x, &centi);
        assert_eq!(f(21.5), Ok(2150));
        assert_eq!(f(-0.125), Ok(-12)); // -12.5 -> -12 (ties to even)
        assert_eq!(f(0.375), Ok(38)); // 37.5 -> 38
        assert_eq!(f(f32::NAN), Err(DecodeError::NotFinite));
        assert_eq!(f(f32::INFINITY), Err(DecodeError::NotFinite));
        assert_eq!(f(f32::MAX), Err(DecodeError::Overflow));
        let d = Decoding { scale: centi, ..dec(RawType::F32) };
        // 21.5f32 = 0x41AC0000.
        assert_eq!(d.decode_registers(&[0x41AC, 0x0000]), Ok(Value::I64(2150)));
        let nan = 0x7FC0_0000u32;
        assert_eq!(d.decode_registers(&[(nan >> 16) as u16, 0]), Err(DecodeError::NotFinite));
    }

    #[test]
    fn static_validation() {
        assert!(dec(RawType::U32).validate().is_ok());
        assert_eq!(dec(RawType::F32).validate(), Err(DecodingConfigError::FloatWithoutScale));
        assert_eq!(dec(RawType::RegisterBit(16)).validate(), Err(DecodingConfigError::BitIndex(16)));
        let big = Scale { numerator: i64::MAX, denominator: 1, offset: 0, rounding: Rounding::Exact };
        assert_eq!(Decoding { scale: big, ..dec(RawType::U32) }.validate(), Err(DecodingConfigError::ScaleOverflow));
        assert_eq!(Decoding { scale: big, ..dec(RawType::Bit) }.validate(), Err(DecodingConfigError::ScaleOnBool));
        let zero = Scale { denominator: 0, ..Scale::IDENTITY };
        assert_eq!(Decoding { scale: zero, ..dec(RawType::U16) }.validate(), Err(DecodingConfigError::Denominator(0)));
        assert_eq!(
            Decoding { valid_range: Some((5, 1)), ..dec(RawType::U16) }.validate(),
            Err(DecodingConfigError::RangeEmpty)
        );
        let r = Decoding { valid_range: Some((0, 100)), ..dec(RawType::U16) };
        assert_eq!(r.decode_registers(&[101]), Err(DecodeError::OutOfRange(101)));
    }
}
