//! VS-004 (brief §4, §7.9): minor-version negotiation seen from the driver side.
//!
//! A fake kernel that speaks only 1.0 (it answers `negotiated_protocol_minor = 0`, as every
//! VS-001..VS-003 kernel does, whatever the `Hello` contains) must never receive a
//! heartbeat from an updated driver; a legacy `Hello` is byte-identical to the VS-003 one.

use std::os::unix::net::UnixListener;
use std::sync::mpsc;
use std::thread;

use scada_protocol::Message;
use scada_protocol::driver_v1::{
    AckStatus, BatchMode, DriverMessage, InputAck, KernelMessage, SessionLimits, Welcome, driver_message,
    kernel_message,
};
use scada_protocol::frame;
use scada_sim_driver::{Session, Val, batch, hello, hello_v10};

/// What the fake kernel saw on its single connection.
#[derive(Debug, Default)]
struct Seen {
    hello_minor: u32,
    batches: usize,
    heartbeats: usize,
}

/// Accept one connection, answer the Hello with minor `minor`, ACK every batch, record
/// everything until EOF.
fn fake_kernel(minor: u32) -> (std::path::PathBuf, tempfile::TempDir, mpsc::Receiver<Seen>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut seen = Seen::default();
        let (mut r, mut w) = (Vec::new(), Vec::new());
        while let Ok(Some(m)) = frame::read_message::<DriverMessage, _>(&mut s, 1 << 20, &mut r) {
            match m.body {
                Some(driver_message::Body::Hello(h)) => {
                    seen.hello_minor = h.protocol_minor;
                    let welcome = KernelMessage {
                        body: Some(kernel_message::Body::Welcome(Welcome {
                            kernel_instance_id: vec![1; 16],
                            negotiated_protocol_major: 1,
                            negotiated_protocol_minor: minor,
                            link_generation: 1,
                            limits: Some(SessionLimits {
                                max_frame_bytes: 65_536,
                                max_observations_per_batch: 128,
                                max_inflight_batches: 32,
                            }),
                            full_state_required: true,
                            last_accepted_source_sequence: None,
                        })),
                    };
                    frame::write_message(&mut s, &welcome, 65_536, &mut w).unwrap();
                }
                Some(driver_message::Body::Batch(b)) => {
                    seen.batches += 1;
                    let ack = KernelMessage {
                        body: Some(kernel_message::Body::Ack(InputAck {
                            source_sequence: b.source_sequence,
                            status: AckStatus::AppliedVolatile as i32,
                            operation_sequence: Some(seen.batches as u64),
                            ..Default::default()
                        })),
                    };
                    frame::write_message(&mut s, &ack, 65_536, &mut w).unwrap();
                }
                Some(driver_message::Body::Heartbeat(_)) => seen.heartbeats += 1,
                None => {}
            }
        }
        tx.send(seen).unwrap();
    });
    (path, dir, rx)
}

fn exchange(path: &std::path::Path, first: &DriverMessage) -> (bool, u32) {
    let mut s = Session::connect_with(path, first).unwrap();
    let negotiated = s.heartbeats_negotiated();
    let mut sent = 0;
    for i in 1..=5u64 {
        if s.heartbeat().unwrap() {
            sent += 1;
        }
        let ack = s.request(&batch(BatchMode::FullState, i, &[(1, Val::B(true))])).unwrap();
        assert_eq!(ack.status, AckStatus::AppliedVolatile as i32);
    }
    (negotiated, sent)
}

#[test]
fn updated_driver_sends_no_heartbeat_to_a_1_0_kernel() {
    let (path, _dir, rx) = fake_kernel(0);
    let (negotiated, sent) = exchange(&path, &hello("d", 1, 7));
    assert!(!negotiated);
    assert_eq!(sent, 0);
    let seen = rx.recv().unwrap();
    assert_eq!(seen.hello_minor, 1, "the updated driver announces 1.1");
    assert_eq!((seen.batches, seen.heartbeats), (5, 0), "{seen:?}");
}

#[test]
fn updated_driver_sends_heartbeats_when_1_1_is_negotiated() {
    let (path, _dir, rx) = fake_kernel(1);
    let (negotiated, sent) = exchange(&path, &hello("d", 1, 7));
    assert!(negotiated);
    assert_eq!(sent, 5);
    let seen = rx.recv().unwrap();
    assert_eq!((seen.batches, seen.heartbeats), (5, 5), "{seen:?}");
}

/// A legacy Hello carries no `protocol_minor` field at all on the wire: byte-identical to
/// what a VS-003 driver sends (proto3 omits a zero scalar).
#[test]
fn legacy_hello_is_byte_identical_to_v1_0() {
    let legacy = hello_v10("scada-sim-driver/0.1", 1001, 0x0102_0304_0506_0708).encode_to_vec();
    // Hand-built VS-003 encoding: field 1 (string), 2 (varint), 3 (fixed64), 4 (packed).
    let mut inner = vec![0x0a, 20];
    inner.extend_from_slice(b"scada-sim-driver/0.1");
    inner.extend_from_slice(&[0x10, 0xe9, 0x07]); // 1001
    inner.push(0x19);
    inner.extend_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
    inner.extend_from_slice(&[0x22, 0x01, 0x01]); // packed [1]
    let mut expected = vec![0x0a, inner.len() as u8];
    expected.extend_from_slice(&inner);
    assert_eq!(legacy, expected);
    let new = hello("scada-sim-driver/0.1", 1001, 0x0102_0304_0506_0708).encode_to_vec();
    assert_eq!(new.len(), legacy.len() + 2, "1.1 adds exactly field 5 = 1");
}

/// A legacy driver against the fake 1.0 kernel: nothing changes.
#[test]
fn legacy_driver_against_1_0_kernel() {
    let (path, _dir, rx) = fake_kernel(0);
    let (negotiated, sent) = exchange(&path, &hello_v10("d", 1, 7));
    assert!(!negotiated);
    assert_eq!(sent, 0);
    let seen = rx.recv().unwrap();
    assert_eq!((seen.hello_minor, seen.batches, seen.heartbeats), (0, 5, 0));
}
