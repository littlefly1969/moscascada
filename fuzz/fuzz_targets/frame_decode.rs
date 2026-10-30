//! Framing + protobuf decode of driver messages from an arbitrary byte stream.
//! Properties: no panic, no buffer larger than the limit, termination.
#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use scada_protocol::driver_v1::DriverMessage;
use scada_protocol::frame;

fuzz_target!(|data: &[u8]| {
    // First byte picks the limit so short and long limits are both explored.
    let (max, stream) = match data.split_first() {
        Some((m, rest)) => (u32::from(*m) * 256 + 1, rest),
        None => return,
    };
    let mut cur = Cursor::new(stream);
    let mut buf = Vec::new();
    loop {
        match frame::read_message::<DriverMessage, _>(&mut cur, max, &mut buf) {
            Ok(Some(_)) => assert!(buf.len() <= max as usize),
            Ok(None) | Err(_) => break,
        }
    }
});
