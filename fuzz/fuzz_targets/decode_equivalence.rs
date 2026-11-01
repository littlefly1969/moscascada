//! C3 + review P1: the data-phase decoder (`convert::decode_driver_message`, reused scratch
//! batch, observation limit enforced before materialization) must agree with the generic
//! `DriverMessage::decode` on every input, whatever the scratch held before (it is reused
//! across frames, so the target keeps one dirty scratch for the whole run):
//! - it fails exactly when the generic decoder fails;
//! - `Batch`/`Other` equal the generic result, with at most `LIMIT` observations;
//! - `Oversized` is a generic batch with more than `LIMIT` observations and the same
//!   header; `NotBatch` is a generic `hello` body; VS-004 `Heartbeat` is a generic
//!   heartbeat body with the same sequence;
//! - the scratch never retains capacity for more than `LIMIT` observations.
//!
//! The first input byte selects the limit (0..=15) so that oversized frames are common;
//! each limit has its own scratch (as each connection has one fixed limit).
#![no_main]

use std::cell::RefCell;

use libfuzzer_sys::fuzz_target;
use scada_protocol::Message;
use scada_protocol::driver_v1::driver_message::Body;
use scada_protocol::driver_v1::{DriverMessage, ObservationBatch};
use scada_runtime::convert::{Decoded, decode_driver_message};

thread_local! {
    // One dirty scratch per limit: in the server the limit is fixed for the life of a
    // connection's scratch, so capacity retained under a larger limit must not be charged
    // to a smaller one (a shared scratch made the capacity check fail spuriously).
    static SCRATCH: [RefCell<ObservationBatch>; 16] = std::array::from_fn(|_| RefCell::new(ObservationBatch::default()));
}

fuzz_target!(|input: &[u8]| {
    let Some((&sel, data)) = input.split_first() else { return };
    let limit = u32::from(sel & 0x0f);
    let generic = DriverMessage::decode(data);
    SCRATCH.with(|all| {
        let mut s = all[limit as usize].borrow_mut();
        let fast = decode_driver_message(data, &mut s, limit);
        match (&generic, fast) {
            (Err(_), Err(_)) => {}
            (Ok(g), Ok(Decoded::Batch(b))) => {
                assert!(b.observations.len() <= limit as usize, "{} > {limit}", b.observations.len());
                assert_eq!(g, &DriverMessage { body: Some(Body::Batch(b.clone())) });
            }
            (Ok(g), Ok(Decoded::Other(m))) => {
                if let Some(Body::Batch(b)) = &m.body {
                    assert!(b.observations.len() <= limit as usize, "{} > {limit}", b.observations.len());
                }
                assert_eq!(g, &m);
            }
            (Ok(g), Ok(Decoded::Oversized { mode, source_sequence, observations })) => match &g.body {
                Some(Body::Batch(b)) => {
                    assert!(observations > u64::from(limit));
                    assert_eq!((b.mode, b.source_sequence, b.observations.len() as u64), (mode, source_sequence, observations));
                }
                other => panic!("Oversized but generic body is {other:?}"),
            },
            (Ok(g), Ok(Decoded::NotBatch)) => assert!(matches!(g.body, Some(Body::Hello(_))), "{g:?}"),
            // VS-004: a heartbeat body after oversized batch fields, without materializing them.
            (Ok(g), Ok(Decoded::Heartbeat { sequence })) => {
                assert_eq!(g.body, Some(Body::Heartbeat(scada_protocol::driver_v1::Heartbeat { sequence })), "{g:?}")
            }
            (g, f) => panic!("decoders disagree: generic={g:?} fast={f:?}"),
        }
        // Retained capacity bounded by the limit (the bound used by the memory budget).
        let cap = s.observations.capacity();
        assert!(cap <= scada_runtime::convert::retained_capacity_bound(limit), "capacity {cap} for limit {limit}");
    });
});
