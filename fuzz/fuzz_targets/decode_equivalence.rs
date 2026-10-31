//! C3: the fast-path decoder (`convert::decode_driver_message`, reused scratch batch) must
//! accept exactly the frames the generic `DriverMessage::decode` accepts and yield the same
//! message, whatever the scratch held before (it is reused across frames, so the target
//! keeps one dirty scratch for the whole run).
#![no_main]

use std::cell::RefCell;

use libfuzzer_sys::fuzz_target;
use scada_protocol::Message;
use scada_protocol::driver_v1::driver_message::Body;
use scada_protocol::driver_v1::{DriverMessage, ObservationBatch};
use scada_runtime::convert::{Decoded, decode_driver_message};

thread_local! {
    static SCRATCH: RefCell<ObservationBatch> = RefCell::new(ObservationBatch::default());
}

fuzz_target!(|data: &[u8]| {
    let generic = DriverMessage::decode(data);
    SCRATCH.with(|s| {
        let mut s = s.borrow_mut();
        let fast = decode_driver_message(data, &mut s).map(|d| match d {
            Decoded::Batch(b) => DriverMessage { body: Some(Body::Batch(b.clone())) },
            Decoded::Other(m) => m,
        });
        match (&generic, &fast) {
            (Ok(g), Ok(f)) => assert_eq!(g, f),
            (Err(_), Err(_)) => {}
            _ => panic!("decoders disagree: generic={generic:?} fast={fast:?}"),
        }
    });
});
