//! DoD 12 (codec level): invalid lengths, corruption and truncation never panic and never
//! allocate according to an untrusted length.

use std::io::Cursor;

use proptest::prelude::*;
use scada_protocol::Message;
use scada_protocol::driver_v1::{
    BatchMode, DriverMessage, Hello, Observation, ObservationBatch, driver_message, observation,
};
use scada_protocol::frame::{self, FrameError};

const MAX: u32 = 65_536;

fn hello() -> DriverMessage {
    DriverMessage {
        body: Some(driver_message::Body::Hello(Hello {
            driver_id: "sim".into(),
            source_id: 1001,
            source_epoch: 0xDEAD_BEEF,
            supported_protocol_majors: vec![1],
        })),
    }
}

#[test]
fn roundtrip() {
    let mut out = Vec::new();
    frame::encode_frame(&hello(), MAX, &mut out).unwrap();
    let mut buf = Vec::new();
    let got: DriverMessage = frame::read_message(&mut Cursor::new(&out), MAX, &mut buf).unwrap().unwrap();
    assert_eq!(got, hello());
}

#[test]
fn zero_length_is_invalid() {
    let mut buf = Vec::new();
    let r = frame::read_frame(&mut Cursor::new([0u8, 0, 0, 0]), MAX, &mut buf);
    assert!(matches!(r, Err(FrameError::ZeroLength)));
}

#[test]
fn oversize_length_rejected_before_allocation() {
    let mut buf = Vec::new();
    let r = frame::read_frame(&mut Cursor::new([0xFFu8, 0xFF, 0xFF, 0xFF]), MAX, &mut buf);
    assert!(matches!(r, Err(FrameError::TooLarge { len: u32::MAX, max: MAX })));
    assert_eq!(buf.capacity(), 0, "no buffer reserved for an untrusted length");
    let r = frame::read_frame(&mut Cursor::new((MAX + 1).to_be_bytes()), MAX, &mut buf);
    assert!(matches!(r, Err(FrameError::TooLarge { .. })));
    assert_eq!(buf.capacity(), 0);
}

#[test]
fn truncated_header_and_payload() {
    let mut buf = Vec::new();
    assert!(matches!(frame::read_frame(&mut Cursor::new([0u8, 0]), MAX, &mut buf), Err(FrameError::Truncated)));
    let mut out = Vec::new();
    frame::encode_frame(&hello(), MAX, &mut out).unwrap();
    out.truncate(out.len() - 1);
    assert!(matches!(frame::read_frame(&mut Cursor::new(&out), MAX, &mut buf), Err(FrameError::Truncated)));
    // Clean EOF at a boundary is not an error.
    assert!(matches!(frame::read_frame(&mut Cursor::new([]), MAX, &mut buf), Ok(None)));
}

#[test]
fn corrupt_payload_is_a_decode_error() {
    // Field 1 (hello), wire type 2, length 100 but only 2 bytes follow.
    let payload = [0x0Au8, 100, 1, 2];
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&payload);
    let mut buf = Vec::new();
    let r: Result<Option<DriverMessage>, _> = frame::read_message(&mut Cursor::new(&framed), MAX, &mut buf);
    assert!(matches!(r, Err(FrameError::Decode(_))));
}

#[test]
fn encode_refuses_messages_above_limit() {
    let big = DriverMessage {
        body: Some(driver_message::Body::Batch(ObservationBatch {
            mode: BatchMode::Delta as i32,
            source_sequence: 1,
            observations: (0..1000)
                .map(|c| Observation { channel_id: c, value: Some(observation::Value::I64Value(i64::MAX)) })
                .collect(),
        })),
    };
    let mut out = Vec::new();
    assert!(matches!(frame::encode_frame(&big, 1024, &mut out), Err(FrameError::EncodeTooLarge { .. })));
}

/// Wire-format anchor: if field numbers or types change, this encoding changes.
#[test]
fn wire_format_anchor() {
    let m = DriverMessage {
        body: Some(driver_message::Body::Batch(ObservationBatch {
            mode: BatchMode::FullState as i32,
            source_sequence: 7,
            observations: vec![
                Observation { channel_id: 1, value: Some(observation::Value::BoolValue(true)) },
                Observation { channel_id: 3, value: Some(observation::Value::I64Value(-2)) },
            ],
        })),
    };
    let hex: String = m.encode_to_vec().iter().map(|b| format!("{b:02x}")).collect();
    // 12 = field 2 (batch) LEN; 08 01 mode; 10 07 seq; 1a.. observations (sint64 -2 = zigzag 3).
    assert_eq!(hex, "1210080110071a0408011001 1a0408031803".replace(' ', ""));
}

proptest! {
    #[test]
    fn arbitrary_bytes_never_panic(data in proptest::collection::vec(any::<u8>(), 0..2048), max in 1u32..4096) {
        let mut cur = Cursor::new(&data);
        let mut buf = Vec::new();
        for _ in 0..64 {
            match frame::read_message::<DriverMessage, _>(&mut cur, max, &mut buf) {
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => break,
            }
            prop_assert!(buf.capacity() <= (max as usize).max(8) * 2);
        }
        prop_assert!(buf.len() <= max as usize);
    }

    #[test]
    fn roundtrip_arbitrary_batches(seq in any::<u64>(), vals in proptest::collection::vec((any::<u32>(), any::<i64>(), any::<bool>()), 0..128)) {
        let m = DriverMessage {
            body: Some(driver_message::Body::Batch(ObservationBatch {
                mode: BatchMode::Delta as i32,
                source_sequence: seq,
                observations: vals.iter().map(|&(c, v, is_bool)| Observation {
                    channel_id: c,
                    value: Some(if is_bool { observation::Value::BoolValue(v % 2 == 0) } else { observation::Value::I64Value(v) }),
                }).collect(),
            })),
        };
        let mut out = Vec::new();
        frame::encode_frame(&m, MAX, &mut out).unwrap();
        let mut buf = Vec::new();
        let back: DriverMessage = frame::read_message(&mut Cursor::new(&out), MAX, &mut buf).unwrap().unwrap();
        prop_assert_eq!(back, m);
    }
}
