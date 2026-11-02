//! EXP-001 (feature `exp001`): the open-wal record envelope decoder reads bytes from the
//! log, which can be corrupted or forged (the outer open-wal CRC does not stop a hostile
//! writer). Properties: no panic, no unbounded allocation; whatever decodes re-encodes to
//! exactly the same bytes (strict, canonical encoding).
#![no_main]

use libfuzzer_sys::fuzz_target;
use scada_journal::owal::envelope;

fuzz_target!(|data: &[u8]| {
    // Half of the inputs get a valid header + CRC so the body parser is reached.
    let forged;
    let input = if data.first().is_some_and(|b| b & 1 == 1) {
        let body = &data[1..];
        let mut b = Vec::with_capacity(body.len() + envelope::HEADER_BYTES);
        b.extend_from_slice(envelope::MAGIC);
        b.extend_from_slice(&envelope::VERSION.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        b.extend_from_slice(&(body.len() as u32).to_le_bytes());
        b.extend_from_slice(&scada_journal::owal::open_wal::crc32c(body).to_le_bytes());
        b.extend_from_slice(body);
        forged = b;
        &forged[..]
    } else {
        data
    };
    if let Ok(d) = envelope::decode(input) {
        let (mut out, mut scratch) = (Vec::new(), Vec::new());
        envelope::encode(&d.record, &d.timeline_id, d.boot_generation, &mut out, &mut scratch);
        assert_eq!(out, input, "envelope must be canonical");
    }
});
