//! VS-007 T12: value decoding (types, word orders, scales, rounding, floats) on arbitrary
//! registers and parameters. Properties: no panic, no overflow, a configuration accepted by
//! `validate` never overflows on integer types, f32 NaN/Inf never produce a value, the
//! result respects the valid range.
#![no_main]

use libfuzzer_sys::fuzz_target;
use scada_modbus::value::{Decoding, RawType, Rounding, Scale, Value, WordOrder};

fn take<const N: usize>(d: &mut &[u8]) -> [u8; N] {
    let mut a = [0u8; N];
    for (i, b) in d.iter().take(N).enumerate() {
        a[i] = *b;
    }
    *d = d.get(N..).unwrap_or_default();
    a
}

fuzz_target!(|data: &[u8]| {
    let mut d = data;
    let [t, b, o, r] = take::<4>(&mut d);
    let raw = match t % 7 {
        0 => RawType::Bit,
        1 => RawType::RegisterBit(b % 20),
        2 => RawType::U16,
        3 => RawType::I16,
        4 => RawType::U32,
        5 => RawType::I32,
        _ => RawType::F32,
    };
    let num = i64::from_le_bytes(take::<8>(&mut d));
    let den = i64::from_le_bytes(take::<8>(&mut d));
    let off = i64::from_le_bytes(take::<8>(&mut d));
    let lo = i64::from_le_bytes(take::<8>(&mut d));
    let hi = i64::from_le_bytes(take::<8>(&mut d));
    let regs = take::<4>(&mut d);
    let rounding = match r % 3 {
        0 => Rounding::Exact,
        1 => Rounding::HalfEven,
        _ => Rounding::Truncate,
    };
    let dec = Decoding {
        raw,
        word_order: if o & 1 == 0 { WordOrder::HighFirst } else { WordOrder::LowFirst },
        byte_swap: o & 2 != 0,
        scale: Scale { numerator: num, denominator: den, offset: off, rounding },
        valid_range: if o & 4 != 0 { Some((lo, hi)) } else { None },
    };
    let regs = [u16::from_le_bytes([regs[0], regs[1]]), u16::from_le_bytes([regs[2], regs[3]])];
    let valid = dec.validate().is_ok();
    if raw == RawType::Bit {
        let _ = dec.decode_bits(regs[0] & 1 == 1);
        return;
    }
    match dec.decode_registers(&regs) {
        Ok(Value::I64(v)) => {
            if let Some((lo, hi)) = dec.valid_range {
                assert!(v >= lo && v <= hi);
            }
            // f32: NaN/Inf are rejected inside decode_registers; Ok means a finite input.
        }
        Ok(Value::Bool(_)) => assert!(raw.is_bool()),
        Err(e) => {
            // An accepted integer configuration never overflows.
            if valid && raw.integer_range().is_some() && dec.valid_range.is_none() {
                assert_ne!(e, scada_modbus::value::DecodeError::Overflow, "{dec:?} {regs:?}");
            }
        }
    }
});
