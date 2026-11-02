//! VS-006 (ADR 0013 D3/D6): the journal payload and checkpoint decoders read bytes from
//! disk, which can be corrupted. Properties: no panic, no unbounded allocation; whatever
//! decodes re-encodes to exactly the same bytes (strict, canonical encoding), so a
//! corrupted row is either rejected (fail-closed recovery) or is a valid record.
#![no_main]

use libfuzzer_sys::fuzz_target;
use scada_journal::codec;

fuzz_target!(|data: &[u8]| {
    let Some((&sel, rest)) = data.split_first() else { return };
    match sel % 3 {
        0 => {
            if let Ok(op) = codec::decode_op(rest) {
                let mut out = Vec::new();
                codec::encode_op(&op, &mut out);
                assert_eq!(out, rest, "operation payload must be canonical");
            }
        }
        1 => {
            if let Ok(cp) = codec::decode_checkpoint(rest) {
                assert_eq!(codec::encode_checkpoint(&cp), rest, "checkpoint must be canonical");
            }
        }
        _ => {
            if let Ok(t) = codec::decode_source_times(rest) {
                assert_eq!(codec::encode_source_times(&t), rest, "source times must be canonical");
            }
        }
    }
});
