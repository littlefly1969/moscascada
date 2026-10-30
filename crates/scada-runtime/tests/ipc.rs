//! DoD 11-16 and the brief §6 demo, with real separate processes over Unix sockets.

mod support;

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use scada_protocol::driver_v1::{
    AckStatus, BatchMode, Continuity, DriverMessage, ErrorCode, KernelMessage, kernel_message,
};
use scada_protocol::frame;
use scada_protocol::introspect_v1::{LinkState, Quality};
use scada_sim_driver::{ClientError, Session, Val, batch, connect_raw, hello};
use support::*;

fn full(seq: u64, door: bool, tamper: bool, temp: i64) -> DriverMessage {
    batch(BatchMode::FullState, seq, &[(1, Val::B(door)), (2, Val::B(tamper)), (3, Val::I(temp))])
}

fn status(a: &scada_protocol::driver_v1::InputAck) -> AckStatus {
    AckStatus::try_from(a.status).unwrap()
}

fn rejected_code(m: &KernelMessage) -> ErrorCode {
    match &m.body {
        Some(kernel_message::Body::HandshakeRejected(r)) => r.error_code(),
        Some(kernel_message::Body::ProtocolError(p)) => p.error_code(),
        other => panic!("expected rejection, got {other:?}"),
    }
}

fn source_link(r: &scada_protocol::introspect_v1::IntrospectResponse, id: u32) -> (LinkState, u64) {
    let s = r.sources.iter().find(|s| s.source_id == id).unwrap();
    (LinkState::try_from(s.link_state).unwrap(), s.link_generation)
}

// Brief §6, run as a separate simulator process.
#[test]
fn demo_scenario_with_separate_driver_process() {
    let k = KernelProc::start(&Limits::default());
    let out = Command::new(sim_bin()).arg("--socket").arg(&k.socket).arg("demo").output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{text}");
    let acks: Vec<&str> = text.lines().filter(|l| l.contains(": ACK")).collect();
    let expect = [
        ("full_state", "ACK_STATUS_APPLIED_VOLATILE", "CONTINUITY_BASELINE_ESTABLISHED"),
        ("delta", "ACK_STATUS_APPLIED_VOLATILE", "CONTINUITY_CONTINUOUS"),
        ("delta", "ACK_STATUS_APPLIED_VOLATILE", "CONTINUITY_CONTINUOUS"),
        ("duplicate", "ACK_STATUS_DUPLICATE", ""),
        ("gap", "ERROR_CODE_SEQUENCE_GAP", ""),
        ("invalid", "ERROR_CODE_UNKNOWN_CHANNEL", ""),
        ("delta", "ACK_STATUS_APPLIED_VOLATILE", "CONTINUITY_CONTINUOUS"),
        ("retransmit", "ACK_STATUS_DUPLICATE", ""),
        ("delta", "ACK_STATUS_APPLIED_VOLATILE", "CONTINUITY_CONTINUOUS"),
    ];
    assert_eq!(acks.len(), expect.len(), "{text}");
    for (line, (label, a, b)) in acks.iter().zip(expect) {
        assert!(line.starts_with(label) && line.contains(a) && line.contains(b), "{line} vs {label} {a} {b}");
    }
    let welcomes: Vec<&str> = text.lines().filter(|l| l.starts_with("WELCOME")).collect();
    assert!(welcomes[0].contains("full_state_required=true"));
    assert!(welcomes[1].contains("full_state_required=false last_accepted=4"));
    assert!(welcomes.iter().all(|w| w.contains(&k.instance)));

    let r = k.wait_for("source disconnected", |r| source_link(r, 1001).0 == LinkState::Disconnected);
    // Last values preserved, qualities uncertain after the driver left.
    let door = tag(&r, 101);
    assert_eq!(door.value, Some(scada_protocol::introspect_v1::tag_state::Value::BoolValue(true)));
    assert_eq!(quality(&door), Quality::UncertainSourceUnavailable);
    let temp = tag(&r, 103);
    assert_eq!(temp.value, Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(2200)));
    assert_eq!(quality(&temp), Quality::UncertainSourceUnavailable);
    k.stop();
}

// DoD 11: the kernel survives SIGKILL of the driver and represents the loss of reliability.
#[test]
fn kernel_survives_driver_kill() {
    let mut k = KernelProc::start(&Limits::default());
    let mut drv =
        Command::new(sim_bin()).arg("--socket").arg(&k.socket).arg("hold").stdout(Stdio::piped()).spawn().unwrap();
    let mut line = String::new();
    let mut so = drv.stdout.take().unwrap();
    let start = Instant::now();
    while !line.contains("HOLDING") {
        let mut b = [0u8; 256];
        let n = so.read(&mut b).unwrap();
        assert!(n > 0 && start.elapsed() < Duration::from_secs(10), "{line}");
        line.push_str(&String::from_utf8_lossy(&b[..n]));
    }
    let r = k.introspect();
    assert_eq!(source_link(&r, 1001).0, LinkState::Connected);
    assert_eq!(quality(&tag(&r, 103)), Quality::Good);
    drv.kill().unwrap(); // SIGKILL: no goodbye from the driver
    drv.wait().unwrap();
    let r = k.wait_for("disconnect after kill", |r| source_link(r, 1001).0 == LinkState::Disconnected);
    let t = tag(&r, 103);
    assert_eq!(t.value, Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(1999)));
    assert_eq!(quality(&t), Quality::UncertainSourceUnavailable);
    assert!(k.alive());
    // Independent restart of the driver: a new process, new epoch, FullState required.
    let mut s = Session::connect(&k.socket, "restarted", 1001, 77).unwrap();
    assert!(s.welcome.full_state_required);
    assert_eq!(status(&s.request(&full(1, false, false, 2000)).unwrap()), AckStatus::AppliedVolatile);
    drop(s);
    k.stop();
}

fn data_session(k: &KernelProc) -> Session {
    let mut s = Session::connect(&k.socket, "t", 1001, 5).unwrap();
    // Same epoch each time: the first FullState is applied, later ones are duplicates.
    let st = status(&s.request(&full(1, false, false, 1)).unwrap());
    assert!(matches!(st, AckStatus::AppliedVolatile | AckStatus::Duplicate), "{st:?}");
    s
}

fn expect_protocol_error(s: &mut Session) -> ErrorCode {
    match s.recv() {
        Ok(m) => rejected_code(&m),
        Err(e) => panic!("expected ProtocolError, got {e}"),
    }
}

// DoD 12: invalid lengths, corrupt input, truncated frames.
#[test]
fn malformed_frames_do_not_crash_or_allocate() {
    let limits = Limits { frame_timeout_ms: 300, ..Limits::default() };
    let mut k = KernelProc::start(&limits);
    let rss_before = k.rss_kib();

    let mut s = data_session(&k);
    s.send_raw(&u32::MAX.to_be_bytes()).unwrap();
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::FrameTooLarge);

    let mut s = data_session(&k);
    s.send_raw(&65_537u32.to_be_bytes()).unwrap();
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::FrameTooLarge);

    let mut s = data_session(&k);
    s.send_raw(&0u32.to_be_bytes()).unwrap();
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::InvalidFrameLength);

    let mut s = data_session(&k);
    s.send_raw(&[0, 0, 0, 4, 0x12, 0x7F, 0x01, 0x02]).unwrap(); // LEN field claiming 127 bytes
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::MalformedMessage);

    let mut s = data_session(&k);
    s.send_raw(&[0, 0, 0, 100, 1, 2, 3]).unwrap(); // truncated: 3 of 100 bytes, then silence
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::FrameTimeout);

    let mut s = data_session(&k);
    s.send_raw(&[0, 0]).unwrap(); // truncated header, then silence
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::FrameTimeout);

    // Garbage instead of a Hello, and a huge length before the handshake.
    for bytes in [&[0u8, 0, 0, 3, 0xFF, 0xFF, 0xFF][..], &[0x7F, 0xFF, 0xFF, 0xFF][..], &[0, 0, 0, 0][..]] {
        let mut raw = UnixStream::connect(&k.socket).unwrap();
        raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        raw.write_all(bytes).unwrap();
        let mut buf = Vec::new();
        let m: KernelMessage = frame::read_message(&mut raw, 1 << 20, &mut buf).unwrap().unwrap();
        let code = rejected_code(&m);
        assert!(
            matches!(code, ErrorCode::InvalidHello | ErrorCode::FrameTooLarge | ErrorCode::InvalidFrameLength),
            "{code:?}"
        );
    }

    // A storm of 2000 connections sending random bytes, sequentially.
    let mut x: u64 = 0x1234_5678;
    for _ in 0..2000 {
        let mut raw = UnixStream::connect(&k.socket).unwrap();
        let mut junk = [0u8; 64];
        for b in &mut junk {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let _ = raw.write_all(&junk);
    }
    assert!(k.alive());
    // Bad peers hold a slot only until their handshake/frame timeout expires.
    let r = k.wait_for("bad connections accepted and reaped", |r| {
        let c = r.counters.as_ref().unwrap();
        c.connections_accepted >= 6 + 3 + 2000 && c.active_connections == 0
    });
    // The data path still works and state was only touched by the valid FullStates.
    let mut s = Session::connect(&k.socket, "after", 1001, 5).unwrap();
    assert!(!s.welcome.full_state_required);
    let ack = s.request(&batch(BatchMode::Delta, 2, &[(3, Val::I(7))])).unwrap();
    assert_eq!(status(&ack), AckStatus::AppliedVolatile);
    let rss_after = k.rss_kib();
    // Memory must not track the declared (up to 4 GiB) lengths nor the number of bad peers.
    assert!(rss_after < rss_before + 16 * 1024, "rss grew from {rss_before} KiB to {rss_after} KiB");
    assert!(r.counters.unwrap().protocol_errors >= 6);
    drop(s);
    k.stop();
}

// DoD 13: version, identity and local authorization.
#[test]
fn handshake_rejections_are_explicit() {
    let uid = current_uid();
    let limits = Limits { handshake_timeout_ms: 300, ..Limits::default() };
    let k = KernelProc::start_with(&limits, Some(&[uid]), &[]);

    let mut h = hello("t", 1001, 9);
    if let Some(scada_protocol::driver_v1::driver_message::Body::Hello(x)) = &mut h.body {
        x.supported_protocol_majors = vec![2, 3];
    }
    assert_eq!(rejected_code(&connect_raw(&k.socket, &h).unwrap().1), ErrorCode::UnsupportedVersion);
    assert_eq!(rejected_code(&connect_raw(&k.socket, &hello("t", 4242, 9)).unwrap().1), ErrorCode::UnknownSource);
    // Source 2001 only accepts uid 4294967294: our credentials (SO_PEERCRED) do not match.
    assert_eq!(rejected_code(&connect_raw(&k.socket, &hello("t", 2001, 9)).unwrap().1), ErrorCode::Unauthorized);
    assert_eq!(rejected_code(&connect_raw(&k.socket, &hello("t", 1001, 0)).unwrap().1), ErrorCode::InvalidHello);
    let long = "x".repeat(65);
    assert_eq!(rejected_code(&connect_raw(&k.socket, &hello(&long, 1001, 9)).unwrap().1), ErrorCode::InvalidHello);
    assert_eq!(
        rejected_code(&connect_raw(&k.socket, &full(1, true, true, 1)).unwrap().1),
        ErrorCode::UnexpectedMessage
    );
    // Silent peer: handshake timeout.
    let mut raw = UnixStream::connect(&k.socket).unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut buf = Vec::new();
    let m: KernelMessage = frame::read_message(&mut raw, 1 << 20, &mut buf).unwrap().unwrap();
    assert_eq!(rejected_code(&m), ErrorCode::HandshakeTimeout);
    // Authorized uid for 1001 works.
    let s = Session::connect(&k.socket, "ok", 1001, 9).unwrap();
    assert_eq!(s.welcome.negotiated_protocol_major, 1);
    // Every rejection left the RTDB untouched (only the accepted connect was applied).
    let r = k.introspect();
    assert_eq!(r.last_operation_sequence, 1);
    assert!(r.tags.iter().all(|t| quality(t) == Quality::Unknown));
    // Hello after the handshake is a protocol error.
    let mut s = s;
    s.send(&hello("again", 1001, 9)).unwrap();
    assert_eq!(expect_protocol_error(&mut s), ErrorCode::UnexpectedMessage);
    k.stop();
}

#[test]
fn connection_limit_is_enforced_with_explicit_refusal() {
    let limits = Limits { max_driver_connections: 2, ..Limits::default() };
    let k = KernelProc::start(&limits);
    let a = UnixStream::connect(&k.socket).unwrap();
    let b = UnixStream::connect(&k.socket).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    // The refusal is written immediately after accept. We read it without sending first: if
    // the peer had already sent data, Linux may report ECONNRESET instead (documented).
    let mut third = UnixStream::connect(&k.socket).unwrap();
    third.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut buf = Vec::new();
    let m: KernelMessage = frame::read_message(&mut third, 1 << 20, &mut buf).unwrap().unwrap();
    assert_eq!(rejected_code(&m), ErrorCode::TooManyConnections);
    drop((a, b));
    let r = k.wait_for("slots released", |r| r.counters.as_ref().unwrap().active_connections == 0);
    assert_eq!(r.counters.unwrap().connections_refused_limit, 1);
    Session::connect(&k.socket, "fourth", 1001, 1).unwrap();
    k.stop();
}

// DoD 14: a producer faster than the consumer is slowed down; nothing is dropped, nothing
// is acknowledged early, memory stays flat.
#[test]
fn backpressure_when_queue_is_full() {
    let limits = Limits { input_queue_capacity: 2, max_inflight_per_connection: 32, ..Limits::default() };
    // 1 ms of artificial work per request in the sequencer thread (test-only flag).
    let k = KernelProc::start_with(&limits, None, &["--throttle-sequencer-us", "1000"]);
    const CLIENTS: u32 = 4;
    const PER_CLIENT: u64 = 150;
    let rss_start = k.rss_kib();
    let started = Instant::now();
    let handles: Vec<_> = (1..=CLIENTS)
        .map(|c| {
            let socket = k.socket.clone();
            std::thread::spawn(move || {
                let mut s = Session::connect(&socket, "bp", 3000 + c, 100 + u64::from(c)).unwrap();
                assert_eq!(
                    status(&s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap()),
                    AckStatus::AppliedVolatile
                );
                // Pipeline up to the advertised in-flight limit; the kernel must slow us down.
                let window = u64::from(s.welcome.limits.unwrap().max_inflight_batches);
                let (mut sent, mut acks) = (0u64, Vec::new());
                while (acks.len() as u64) < PER_CLIENT {
                    while sent < PER_CLIENT && sent - acks.len() as u64 <= window {
                        sent += 1;
                        s.send(&batch(BatchMode::Delta, 1 + sent, &[(1, Val::I(sent as i64))])).unwrap();
                    }
                    acks.push(s.recv_ack().unwrap());
                }
                acks
            })
        })
        .collect();
    let mut rss_peak = 0;
    while handles.iter().any(|h| !h.is_finished()) {
        rss_peak = rss_peak.max(k.rss_kib());
        let c = k.introspect().counters.unwrap();
        assert!(c.input_queue_capacity == 2);
        std::thread::sleep(Duration::from_millis(20));
    }
    let elapsed = started.elapsed();
    let mut all_ops = Vec::new();
    for h in handles {
        let acks = h.join().unwrap();
        // No silent drop: exactly one ACK per batch, in send order, all applied.
        assert_eq!(acks.len() as u64, PER_CLIENT);
        for (n, a) in acks.iter().enumerate() {
            assert_eq!(a.source_sequence, n as u64 + 2);
            assert_eq!(status(a), AckStatus::AppliedVolatile, "{a:?}");
            all_ops.push(a.operation_sequence.unwrap());
        }
    }
    all_ops.sort_unstable();
    all_ops.dedup();
    assert_eq!(all_ops.len() as u64, u64::from(CLIENTS) * PER_CLIENT, "operation sequences are unique");
    let r = k.wait_for("all load sources disconnected", |r| {
        (1..=CLIENTS).all(|c| source_link(r, 3000 + c).0 == LinkState::Disconnected)
    });
    let c = r.counters.unwrap();
    assert!(c.input_queue_full_waits > 0, "producers never had to wait: {c:?}");
    // Per client: connect + full state + deltas + disconnect; nothing else consumed a sequence.
    assert_eq!(r.last_operation_sequence, u64::from(CLIENTS) * (PER_CLIENT + 3));
    // Throughput is bounded by the 1 ms consumer, so producers were blocked, not buffered.
    assert!(elapsed >= Duration::from_millis(600), "{elapsed:?}");
    assert!(rss_peak < rss_start + 8 * 1024, "rss {rss_start} -> peak {rss_peak} KiB");
    eprintln!(
        "backpressure: {} batches in {elapsed:?}, queue_full_waits={}, rss start={rss_start} KiB peak={rss_peak} KiB",
        u64::from(CLIENTS) * PER_CLIENT,
        c.input_queue_full_waits
    );
    k.stop();
}

// DoD 15: a new kernel process has a new identity, no state, and requires a FullState.
#[test]
fn kernel_restart_loses_volatile_state_explicitly() {
    let dir = tempfile::Builder::new().prefix("scada-restart").tempdir().unwrap();
    let path = dir.path().to_owned();
    let k1 = KernelProc::start_in(dir, &Limits::default(), None, &[]);
    let id1 = k1.instance.clone();
    let mut s = Session::connect(&k1.socket, "t", 1001, 42).unwrap();
    let ack = s.request(&full(1, true, true, 3000)).unwrap();
    assert_eq!(status(&ack), AckStatus::AppliedVolatile);
    assert_eq!(scada_sim_driver::hex(&ack.kernel_instance_id), id1);
    // Crash the kernel (SIGKILL): the stale socket file stays behind.
    let tmp = k1.dir.path().to_owned();
    let k1_dir = {
        let KernelProc { mut child, dir, .. } = k1;
        child.kill().unwrap();
        child.wait().unwrap();
        dir
    };
    assert_eq!(tmp, path);
    // The driver notices the loss as EOF / error on its connection.
    assert!(s.request(&batch(BatchMode::Delta, 2, &[(3, Val::I(1))])).is_err());
    let k2 = KernelProc::start_in(k1_dir, &Limits::default(), None, &[]);
    assert_ne!(k2.instance, id1, "new kernel_instance_id");
    let r = k2.introspect();
    assert_eq!(r.last_operation_sequence, 0);
    assert!(r.tags.iter().all(|t| quality(t) == Quality::Unknown && t.value.is_none() && t.revision == 0));
    // Same producer epoch reconnects: no baseline exists any more, no false recovery.
    let mut s = Session::connect(&k2.socket, "t", 1001, 42).unwrap();
    assert!(s.welcome.full_state_required);
    assert_eq!(s.welcome.last_accepted_source_sequence, None);
    let ack = s.request(&batch(BatchMode::Delta, 2, &[(3, Val::I(1))])).unwrap();
    assert_eq!(ErrorCode::try_from(ack.error_code).unwrap(), ErrorCode::ResyncRequired);
    let ack = s.request(&full(2, true, true, 3000)).unwrap();
    assert_eq!(Continuity::try_from(ack.continuity).unwrap(), Continuity::BaselineEstablished);
    assert_eq!(scada_sim_driver::hex(&ack.kernel_instance_id), k2.instance);
    drop(s);
    k2.stop();
}

// DoD 16 (deterministic part): the old link cannot overwrite the new generation.
#[test]
fn superseded_link_cannot_overwrite_new_generation() {
    let k = KernelProc::start(&Limits::default());
    let mut old = Session::connect(&k.socket, "old", 1001, 7).unwrap();
    assert_eq!(status(&old.request(&full(1, false, false, 100)).unwrap()), AckStatus::AppliedVolatile);
    let mut new = Session::connect(&k.socket, "new", 1001, 7).unwrap();
    assert!(new.welcome.link_generation > old.welcome.link_generation);
    assert_eq!(new.welcome.last_accepted_source_sequence, Some(1));
    assert_eq!(
        status(&new.request(&batch(BatchMode::Delta, 2, &[(3, Val::I(200))])).unwrap()),
        AckStatus::AppliedVolatile
    );
    // Old link writes: refused, then closed by the kernel.
    let ack = old.request(&batch(BatchMode::Delta, 3, &[(3, Val::I(999))])).unwrap();
    assert_eq!(ErrorCode::try_from(ack.error_code).unwrap(), ErrorCode::LinkSuperseded);
    assert!(matches!(old.recv(), Err(ClientError::Closed) | Err(ClientError::Frame(_))));
    drop(old);
    std::thread::sleep(Duration::from_millis(100));
    let r = k.introspect();
    // The late disconnect of the old generation did not degrade the new one.
    assert_eq!(source_link(&r, 1001), (LinkState::Connected, new.welcome.link_generation));
    let t = tag(&r, 103);
    assert_eq!(t.value, Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(200)));
    assert_eq!(quality(&t), Quality::Good);
    assert_eq!(
        status(&new.request(&batch(BatchMode::Delta, 3, &[(3, Val::I(201))])).unwrap()),
        AckStatus::AppliedVolatile
    );
    k.stop();
}

// DoD 16 (stress): concurrent connect/disconnect/takeover races keep the state coherent.
#[test]
fn concurrent_reconnects_do_not_corrupt_state() {
    let k = KernelProc::start(&Limits::default());
    const THREADS: u64 = 8;
    const ROUNDS: u64 = 40;
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let socket = k.socket.clone();
            std::thread::spawn(move || {
                let mut applied = Vec::new(); // (operation_sequence, temp value)
                for r in 0..ROUNDS {
                    // Distinct epoch per (thread, round): every connect needs a FullState.
                    let Ok(mut s) = Session::connect(&socket, "race", 1001, 1 + t * 1000 + r) else { continue };
                    let value = (t * 1000 + r) as i64;
                    for (seq, msg) in [
                        (1, full(1, t % 2 == 0, r % 2 == 0, value)),
                        (2, batch(BatchMode::Delta, 2, &[(3, Val::I(value + 1))])),
                    ] {
                        match s.request(&msg) {
                            Ok(a) if status(&a) == AckStatus::AppliedVolatile => {
                                applied.push((a.operation_sequence.unwrap(), if seq == 1 { value } else { value + 1 }));
                            }
                            Ok(a) => {
                                let code = ErrorCode::try_from(a.error_code).unwrap();
                                assert!(
                                    matches!(code, ErrorCode::LinkSuperseded | ErrorCode::ResyncRequired),
                                    "{code:?}"
                                );
                                break;
                            }
                            Err(_) => break, // closed after supersession
                        }
                    }
                }
                applied
            })
        })
        .collect();
    let mut all: Vec<(u64, i64)> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_unstable();
    let ops: Vec<u64> = all.iter().map(|x| x.0).collect();
    let mut dedup = ops.clone();
    dedup.dedup();
    assert_eq!(ops, dedup, "an operation sequence was acknowledged twice");
    let r = k.wait_for("all links closed", |r| source_link(r, 1001).0 == LinkState::Disconnected);
    // The temperature equals the value of the last applied write in the global order.
    let (_, last_value) = *all.last().expect("some writes applied");
    let t = tag(&r, 103);
    assert_eq!(t.value, Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(last_value)));
    assert_eq!(quality(&t), Quality::UncertainSourceUnavailable);
    assert!(r.last_operation_sequence >= ops.len() as u64);
    eprintln!("concurrent reconnects: {} applied writes, last op {}", ops.len(), r.last_operation_sequence);
    k.stop();
}
