//! VS-002 gates (brief §8): ingestion modes, per-source ordering under multicore ingestion,
//! takeover, reconnection with fresh snapshot, peer that does not read, replay determinism,
//! sequencer failure. Real kernel and driver processes / sockets, as in `ipc.rs`.

mod support;

#[path = "../../scada-core/tests/reference/vs001_kernel.rs"]
mod reference;

use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scada_core::Kernel;
use scada_protocol::driver_v1::{AckStatus, BatchMode, Continuity, DriverMessage, ErrorCode, kernel_message};
use scada_protocol::frame;
use scada_protocol::introspect_v1::{IntrospectResponse, LinkState, Quality, tag_state};
use scada_sim_driver::{ClientError, Session, Val, batch, resume};
use support::*;

fn full1001(seq: u64, door: bool, tamper: bool, temp: i64) -> DriverMessage {
    batch(BatchMode::FullState, seq, &[(1, Val::B(door)), (2, Val::B(tamper)), (3, Val::I(temp))])
}

fn st(a: &scada_protocol::driver_v1::InputAck) -> AckStatus {
    AckStatus::try_from(a.status).unwrap()
}

fn code(a: &scada_protocol::driver_v1::InputAck) -> ErrorCode {
    ErrorCode::try_from(a.error_code).unwrap()
}

fn i64_of(r: &IntrospectResponse, tag_id: u32) -> Option<i64> {
    match tag(r, tag_id).value {
        Some(tag_state::Value::I64Value(v)) => Some(v),
        _ => None,
    }
}

fn link(r: &IntrospectResponse, id: u32) -> (LinkState, u64) {
    let s = r.sources.iter().find(|s| s.source_id == id).unwrap();
    (LinkState::try_from(s.link_state).unwrap(), s.link_generation)
}

/// Error code of the next kernel message, if it is a ProtocolError (None if closed).
fn next_protocol_error(s: &mut Session) -> Option<ErrorCode> {
    match s.recv() {
        Ok(m) => match m.body {
            Some(kernel_message::Body::ProtocolError(p)) => Some(p.error_code()),
            other => panic!("expected ProtocolError, got {other:?}"),
        },
        Err(ClientError::Closed) | Err(ClientError::Frame(_)) => None,
        Err(e) => panic!("{e}"),
    }
}

fn modes() -> [Option<u32>; 4] {
    [None, Some(1), Some(2), Some(4)]
}

fn start_mode(limits: &Limits, workers: Option<u32>, more: &[&str]) -> KernelProc {
    let mut args = scheduler_args(workers);
    args.extend(more.iter().map(|s| s.to_string()));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    KernelProc::start_with(limits, None, &refs)
}

/// One pipelined load client: FullState then `n` Deltas with `pipeline` in flight. Checks
/// that ACKs arrive in batch order with strictly increasing operation sequences, and
/// returns every operation sequence it was given.
fn load_client(socket: std::path::PathBuf, source: u32, epoch: u64, n: u64, pipeline: u64) -> Vec<u64> {
    let mut s = Session::connect(&socket, "load", source, epoch).unwrap();
    let a = s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    let mut ops = vec![a.operation_sequence.unwrap()];
    let (mut sent, mut acked) = (0u64, 0u64);
    while acked < n {
        while sent < n && sent - acked < pipeline {
            sent += 1;
            s.send(&batch(BatchMode::Delta, 1 + sent, &[(1, Val::I(sent as i64))])).unwrap();
        }
        s.flush().unwrap();
        let a = s.recv_ack().unwrap();
        acked += 1;
        assert_eq!(a.source_sequence, 1 + acked, "source {source}: ACK out of order");
        assert_eq!(st(&a), AckStatus::AppliedVolatile, "source {source}: {a:?}");
        let op = a.operation_sequence.unwrap();
        assert!(op > *ops.last().unwrap(), "source {source}: operation sequence not increasing");
        ops.push(op);
    }
    ops
}

/// Normalized result of a run: what must not depend on the scheduler (tag values,
/// qualities and revisions of the load sources, their last accepted sequences).
fn normalized(r: &IntrospectResponse, sources: u32) -> BTreeMap<u32, (Option<i64>, i32, u64, Option<u64>)> {
    (1..=sources)
        .map(|n| {
            let t = tag(r, 300 + n);
            let s = r.sources.iter().find(|s| s.source_id == 3000 + n).unwrap();
            (n, (i64_of(r, 300 + n), t.quality, t.revision, s.last_accepted_source_sequence))
        })
        .collect()
}

// §8.4 + §8.3 + D4: the same scenario under current_thread and multi_thread(1/2/4), with
// 16 concurrent pipelined sources and repeated takeovers of another source: no panic, no
// deadlock, per-source ACK order, globally unique operation sequences, same normalized
// final state.
#[test]
fn ingestion_modes_preserve_order_and_give_the_same_normalized_state() {
    const SOURCES: u32 = 16;
    const N: u64 = 400;
    let limits = Limits { max_driver_connections: 40, ..Limits::default() };
    let mut results = Vec::new();
    for workers in modes() {
        let k = start_mode(&limits, workers, &[]);
        let handles: Vec<_> = (1..=SOURCES)
            .map(|n| {
                let sock = k.socket.clone();
                std::thread::spawn(move || load_client(sock, 3000 + n, 77, N, 16))
            })
            .collect();
        // Concurrent takeovers of source 1001: every new link must be welcomed, every old
        // link must be closed by the kernel (D3), the state stays coherent.
        let sock = k.socket.clone();
        let takeovers = std::thread::spawn(move || {
            let mut seq = 1u64;
            let mut prev: Option<Session> = None;
            for i in 0..20u64 {
                let mut s = Session::connect(&sock, "takeover", 1001, 5).unwrap();
                let a = s.request(&full1001(seq, i % 2 == 0, false, i as i64)).unwrap();
                assert_eq!(st(&a), AckStatus::AppliedVolatile, "{a:?}");
                seq += 1;
                if let Some(mut old) = prev.take() {
                    // The old link is closed with LINK_SUPERSEDED (or already gone).
                    let c = next_protocol_error(&mut old);
                    assert!(matches!(c, None | Some(ErrorCode::LinkSuperseded)), "{c:?}");
                }
                prev = Some(s);
            }
            seq - 1
        });
        let mut all_ops: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
        let last_1001 = takeovers.join().unwrap();
        let total = all_ops.len();
        all_ops.sort_unstable();
        all_ops.dedup();
        assert_eq!(all_ops.len(), total, "{workers:?}: an operation sequence was given twice");
        // Also wait until every load client's close has been sequenced: its
        // SourceDisconnected changes quality and revision, and the clients close
        // concurrently with this read (reading earlier made the comparison racy).
        let r = k.wait_for("all applied and load links closed", |r| {
            r.sources.iter().find(|s| s.source_id == 1001).and_then(|s| s.last_accepted_source_sequence)
                == Some(last_1001)
                && (1..=SOURCES).all(|n| link(r, 3000 + n).0 == LinkState::Disconnected)
        });
        assert_eq!(r.counters.unwrap().batches_rejected, 0, "{workers:?}");
        assert!(r.counters.unwrap().links_superseded >= 19, "{workers:?}: {:?}", r.counters);
        for n in 1..=SOURCES {
            assert_eq!(i64_of(&r, 300 + n), Some(N as i64), "{workers:?} source {n}");
        }
        assert!(r.scheduler.starts_with(if workers.is_some() { "multi_thread" } else { "current_thread" }));
        results.push((workers, normalized(&r, SOURCES)));
        k.stop();
    }
    for (w, n) in &results[1..] {
        assert_eq!(n, &results[0].1, "normalized state differs between {:?} and {w:?}", results[0].0);
    }
}

// D3: an idle old link no longer holds a connection slot after takeover. With 2 slots and
// 30 takeovers whose old sockets the client never closes, VS-001 would refuse the third
// connection with TOO_MANY_CONNECTIONS.
#[test]
fn repeated_takeovers_do_not_exhaust_connection_slots() {
    let limits = Limits { max_driver_connections: 2, ..Limits::default() };
    let k = KernelProc::start(&limits);
    let mut kept = Vec::new(); // old sockets, never closed by the client until the end
    let mut current: Option<Session> = None;
    let mut seq = 1;
    for i in 0..30 {
        let mut s = Session::connect(&k.socket, "t", 1001, 9).unwrap_or_else(|e| panic!("takeover {i}: {e}"));
        let a = s.request(&full1001(seq, false, false, i)).unwrap();
        assert_eq!(st(&a), AckStatus::AppliedVolatile);
        seq += 1;
        if let Some(old) = current.replace(s) {
            kept.push(old.into_stream());
        }
        // The kernel closes the superseded session; wait until its slot is released.
        k.wait_for("old session released", |r| r.counters.unwrap().active_connections <= 1);
    }
    let r = k.introspect();
    assert_eq!(r.counters.unwrap().connections_refused_limit, 0);
    assert_eq!(r.counters.unwrap().links_superseded, 29);
    // Late closes of all the old sockets: the active link stays Connected and Good.
    let active = link(&r, 1001);
    assert_eq!(active.0, LinkState::Connected);
    drop(kept);
    std::thread::sleep(Duration::from_millis(200));
    let r = k.introspect();
    assert_eq!(link(&r, 1001), active);
    for t in [101, 102, 103] {
        assert_eq!(quality(&tag(&r, t)), Quality::Good);
    }
    let mut s = current.unwrap();
    assert_eq!(st(&s.request(&full1001(seq, true, true, 1)).unwrap()), AckStatus::AppliedVolatile);
    k.stop();
}

// §8.5 / D2: a peer that writes but never reads its ACKs is closed within the declared
// write deadline; its resources are released, nothing is invented, and the inputs that
// were applied before the close stay applied. The driver reconciles by reconnecting.
#[test]
fn non_reading_peer_is_closed_and_reconciles_by_reconnect() {
    let limits = Limits { ack_write_timeout_ms: 300, ..Limits::default() };
    let k = KernelProc::start(&limits);
    let rss0 = k.rss_kib();
    let mut s = Session::connect(&k.socket, "mute", 3001, 42).unwrap();
    let a = s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    let mut raw: UnixStream = s.into_stream();
    raw.set_write_timeout(Some(Duration::from_secs(20))).unwrap();
    // Write Deltas as fast as possible and never read. Each ACK is ~30 bytes, so the
    // kernel's ACK writes block once the socket buffer toward us is full.
    let t0 = Instant::now();
    let writer = std::thread::spawn(move || {
        let mut w = Vec::new();
        let mut sent = 0u64;
        for seq in 2..2_000_000u64 {
            frame::encode_frame(&batch(BatchMode::Delta, seq, &[(1, Val::I(seq as i64))]), 65_536, &mut w).unwrap();
            if raw.write_all(&w).is_err() {
                break;
            }
            sent += 1;
        }
        (sent, raw)
    });
    let r = k.wait_for("write timeout", |r| r.counters.unwrap().ack_write_timeouts == 1);
    let closed_after = t0.elapsed();
    let (sent, _raw) = writer.join().unwrap();
    // The kernel stops reading once max_inflight ACKs are queued, so the peer's writes
    // stall too; what it managed to send before the close is just the socket buffers.
    assert!(sent > 32, "the peer could not even fill the in-flight window ({sent})");
    let r2 = k.wait_for("link released", |r| link(r, 3001).0 == LinkState::Disconnected);
    assert_eq!(r2.counters.unwrap().active_connections, 0);
    assert!(closed_after < Duration::from_secs(10), "{closed_after:?}");
    assert!(k.rss_kib() < rss0 + 8 * 1024, "RSS grew from {rss0} to {} KiB", k.rss_kib());
    let _ = r;
    // Reconcile: same epoch. The kernel reports what it applied; the tag holds exactly
    // that batch's value; the next sequence continues without a gap.
    let mut s = Session::connect(&k.socket, "mute", 3001, 42).unwrap();
    let last = s.welcome.last_accepted_source_sequence.expect("baseline kept");
    assert!(last >= 2, "nothing applied?");
    assert_eq!(i64_of(&k.introspect(), 301), Some(last as i64));
    let a = s.request(&batch(BatchMode::Delta, last + 1, &[(1, Val::I(-1))])).unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    assert_eq!(Continuity::try_from(a.continuity).unwrap(), Continuity::Continuous);
    k.stop();
}

// §8.7 / D1: after a reconnection with partial deltas, untouched tags stay Uncertain;
// replayed duplicates do not refresh them; a fresh FullState makes them Good again.
#[test]
fn reconnection_with_partial_deltas_then_fresh_full_state() {
    let k = KernelProc::start(&Limits::default());
    let mut s = Session::connect(&k.socket, "d1", 1001, 11).unwrap();
    assert_eq!(st(&s.request(&full1001(1, false, false, 2000)).unwrap()), AckStatus::AppliedVolatile);
    let lost = batch(BatchMode::Delta, 2, &[(3, Val::I(2010))]);
    assert_eq!(st(&s.request(&lost).unwrap()), AckStatus::AppliedVolatile);
    drop(s);
    let r = k.wait_for("disconnected", |r| link(r, 1001).0 == LinkState::Disconnected);
    for t in [101, 102, 103] {
        assert_eq!(quality(&tag(&r, t)), Quality::UncertainSourceUnavailable);
    }
    let mut s = Session::connect(&k.socket, "d1", 1001, 11).unwrap();
    assert_eq!(s.welcome.last_accepted_source_sequence, Some(2));
    // Replaying the already applied batch (its ACK "lost"): DUPLICATE, no freshness.
    assert_eq!(st(&s.request(&lost).unwrap()), AckStatus::Duplicate);
    let r = k.introspect();
    assert_eq!(quality(&tag(&r, 103)), Quality::UncertainSourceUnavailable);
    // Partial delta: only the observed tag becomes Good.
    assert_eq!(st(&s.request(&batch(BatchMode::Delta, 3, &[(3, Val::I(2020))])).unwrap()), AckStatus::AppliedVolatile);
    let r = k.introspect();
    assert_eq!(quality(&tag(&r, 103)), Quality::Good);
    assert_eq!(quality(&tag(&r, 101)), Quality::UncertainSourceUnavailable);
    assert_eq!(quality(&tag(&r, 102)), Quality::UncertainSourceUnavailable);
    // Fresh full picture with the next sequence: everything Good, continuity Continuous.
    let a = s.request(&full1001(4, false, false, 2020)).unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    assert_eq!(Continuity::try_from(a.continuity).unwrap(), Continuity::Continuous);
    let r = k.introspect();
    for t in [101, 102, 103] {
        assert_eq!(quality(&tag(&r, t)), Quality::Good, "tag {t}");
    }
    k.stop();
}

// D1 with the simulator's `resume` procedure: unacknowledged batches are replayed in order
// (already applied ones skipped), then the snapshot reasserts every channel.
#[test]
fn simulator_resume_procedure_reasserts_all_channels() {
    let k = KernelProc::start(&Limits::default());
    let mut s = Session::connect(&k.socket, "d1", 1001, 12).unwrap();
    assert_eq!(st(&s.request(&full1001(1, true, false, 100)).unwrap()), AckStatus::AppliedVolatile);
    // Batches 2..=4 sent; pretend only 2 was applied before the link broke: send 2,
    // read its ACK, then queue 3 and 4 locally as "unacknowledged".
    let b2 = batch(BatchMode::Delta, 2, &[(3, Val::I(101))]);
    assert_eq!(st(&s.request(&b2).unwrap()), AckStatus::AppliedVolatile);
    drop(s);
    k.wait_for("disconnected", |r| link(r, 1001).0 == LinkState::Disconnected);
    let unacked = vec![
        (2, b2),
        (3, batch(BatchMode::Delta, 3, &[(3, Val::I(102))])),
        (4, batch(BatchMode::Delta, 4, &[(1, Val::B(false))])),
    ];
    let mut s = Session::connect(&k.socket, "d1", 1001, 12).unwrap();
    let rep = resume(&mut s, &unacked, &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(102))]).unwrap();
    assert_eq!(rep.replayed.len(), 2, "batch 2 was applied and must not be resent");
    assert!(rep.replayed.iter().all(|a| st(a) == AckStatus::AppliedVolatile));
    assert_eq!(rep.replayed.iter().map(|a| a.source_sequence).collect::<Vec<_>>(), [3, 4]);
    assert_eq!(rep.snapshot.source_sequence, 5);
    assert_eq!(Continuity::try_from(rep.snapshot.continuity).unwrap(), Continuity::Continuous);
    let r = k.introspect();
    for t in [101, 102, 103] {
        assert_eq!(quality(&tag(&r, t)), Quality::Good, "tag {t}");
    }
    assert_eq!(rep.next_sequence, 6);
    k.stop();
}

// §8.8: a continuous replay is distinguished from a loss of sequence. Stale and gapped
// inputs change nothing; a FullState after a gap is RESYNCHRONIZED, not CONTINUOUS.
#[test]
fn continuous_replay_vs_sequence_loss() {
    let k = KernelProc::start(&Limits::default());
    let mut s = Session::connect(&k.socket, "d8", 1001, 13).unwrap();
    assert_eq!(st(&s.request(&full1001(10, false, false, 1)).unwrap()), AckStatus::AppliedVolatile);
    let a = s.request(&batch(BatchMode::Delta, 11, &[(3, Val::I(2))])).unwrap();
    assert_eq!(Continuity::try_from(a.continuity).unwrap(), Continuity::Continuous);
    let before = k.introspect();
    let stale = s.request(&batch(BatchMode::Delta, 9, &[(3, Val::I(99))])).unwrap();
    assert_eq!(code(&stale), ErrorCode::StaleSequence);
    let gap = s.request(&batch(BatchMode::Delta, 13, &[(3, Val::I(99))])).unwrap();
    assert_eq!(code(&gap), ErrorCode::SequenceGap);
    assert_eq!(gap.last_accepted_source_sequence, Some(11));
    let after = k.introspect();
    assert_eq!(before.state_digest, after.state_digest, "stale/gap changed the state");
    let resync = s.request(&full1001(13, false, false, 3)).unwrap();
    assert_eq!(st(&resync), AckStatus::AppliedVolatile);
    assert_eq!(Continuity::try_from(resync.continuity).unwrap(), Continuity::Resynchronized);
    k.stop();
}

/// Replay a capture through a fresh kernel and through the frozen VS-001 reference.
fn replay(capture: &std::path::Path, cfg_path: &std::path::Path) -> ([u8; 32], scada_core::LogicalSnapshot) {
    let cfg = scada_runtime::config::load(cfg_path).unwrap();
    let inputs = scada_runtime::capture::read(capture).unwrap();
    assert!(!inputs.is_empty());
    let mut k = Kernel::new(Arc::new(cfg.core.clone()));
    let mut r = reference::RefKernel::new(Arc::new(cfg.core.clone()));
    for i in inputs {
        let a = k.apply(i.clone());
        let b = r.apply(i);
        assert_eq!(a, b, "optimized and VS-001 kernels disagree on a captured input");
    }
    assert_eq!(k.logical_snapshot(), r.logical_snapshot());
    (k.state_digest(), k.logical_snapshot())
}

// §8.1 / D5: the stream of sequenced inputs actually admitted by a running kernel, under
// every scheduler, replays to exactly the live digest — through the optimized kernel and
// through the frozen VS-001 kernel. (Independent runs are NOT compared with each other:
// their global admission order legitimately differs.)
#[test]
fn captured_streams_replay_to_the_live_digest_in_every_mode() {
    let limits = Limits { max_driver_connections: 40, ..Limits::default() };
    for workers in modes() {
        let dir = tempfile::Builder::new().prefix("scada-cap").tempdir().unwrap();
        let cap = dir.path().join("inputs.txt");
        let cap_s = cap.display().to_string();
        let mut args = scheduler_args(workers);
        args.extend(["--capture-inputs".to_string(), cap_s]);
        let refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let k = KernelProc::start_with(&limits, None, &refs);
        let hs: Vec<_> = (1..=8u32)
            .map(|n| {
                let sock = k.socket.clone();
                std::thread::spawn(move || load_client(sock, 3000 + n, 5, 150, 8))
            })
            .collect();
        // Some invalid and duplicate inputs too, so that rejections are part of the stream.
        let mut s = Session::connect(&k.socket, "mix", 1001, 3).unwrap();
        s.request(&full1001(1, true, true, 1)).unwrap();
        s.request(&full1001(1, true, true, 1)).unwrap(); // duplicate
        s.request(&batch(BatchMode::Delta, 5, &[(3, Val::I(2))])).unwrap(); // gap
        s.request(&batch(BatchMode::Delta, 2, &[(9, Val::I(2))])).unwrap(); // unknown channel
        for h in hs {
            h.join().unwrap();
        }
        drop(s);
        let live = k.wait_for("disconnect applied", |r| link(r, 1001).0 == LinkState::Disconnected);
        let (digest, snap) = replay(&cap, &k.dir.path().join("kernel.toml"));
        assert_eq!(live.state_digest, digest.to_vec(), "{workers:?}: replay digest differs from live");
        assert_eq!(live.last_operation_sequence, snap.last_operation_sequence);
        k.stop();
    }
}

// §8.11: if the sequencer thread dies, the host stops (non-zero exit) instead of accepting
// inputs that nobody can apply.
#[test]
fn sequencer_failure_stops_the_host() {
    let mut k = KernelProc::start_with(&Limits::default(), None, &["--test-fail-sequencer-after", "3"]);
    let mut s = Session::connect(&k.socket, "x", 1001, 1).unwrap(); // request 1
    let _ = s.request(&full1001(1, false, false, 0)); // request 2
    let _ = s.request(&full1001(2, false, false, 0)); // request 3
    let r = s.request(&full1001(3, false, false, 0)); // request 4: sequencer panics
    assert!(r.is_err(), "no ACK may be produced without the sequencer: {r:?}");
    let t0 = Instant::now();
    while k.alive() {
        assert!(t0.elapsed() < Duration::from_secs(5), "host still running without its sequencer");
        std::thread::sleep(Duration::from_millis(20));
    }
    let code = k.child.wait().unwrap().code();
    assert_ne!(code, Some(0));
    assert!(UnixStream::connect(&k.socket).is_err(), "socket still accepting");
    let err = k.kill();
    assert!(err.contains("sequencer thread stopped"), "{err}");
}

// §8.6 with the C5 read buffer on: incomplete, oversize, malformed frames and EOF in the
// middle of a frame are handled exactly as without the buffer (no crash, no allocation
// beyond max_frame_bytes, explicit errors).
#[test]
fn malformed_input_with_read_buffer() {
    malformed_input(8192);
}

#[test]
fn malformed_input_unbuffered() {
    malformed_input(0);
}

fn malformed_input(read_buffer_bytes: u32) {
    let limits = Limits { frame_timeout_ms: 300, read_buffer_bytes, ..Limits::default() };
    let k = KernelProc::start(&limits);
    let epoch = std::cell::Cell::new(0u64);
    let open = || {
        // A new epoch each time: sequence 1 is then fresh, not a duplicate.
        epoch.set(epoch.get() + 1);
        let mut s = Session::connect(&k.socket, "m", 3002, epoch.get()).unwrap();
        assert_eq!(
            st(&s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap()),
            AckStatus::AppliedVolatile
        );
        s
    };
    let perr = next_protocol_error;
    // Oversize length prefix: refused before allocation.
    let mut s = open();
    s.send_raw(&(u32::MAX).to_be_bytes()).unwrap();
    assert_eq!(perr(&mut s), Some(ErrorCode::FrameTooLarge));
    // Zero length.
    let mut s = open();
    s.send_raw(&0u32.to_be_bytes()).unwrap();
    assert_eq!(perr(&mut s), Some(ErrorCode::InvalidFrameLength));
    // Garbage payload.
    let mut s = open();
    s.send_raw(&[0, 0, 0, 3, 0xff, 0xff, 0xff]).unwrap();
    assert_eq!(perr(&mut s), Some(ErrorCode::MalformedMessage));
    // Incomplete frame: header promises 100 bytes, 10 arrive, then silence.
    let mut s = open();
    s.send_raw(&[0, 0, 0, 100, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
    assert_eq!(perr(&mut s), Some(ErrorCode::FrameTimeout));
    // Valid batch and a truncated one in the same write, then EOF.
    let mut s = open();
    let mut w = Vec::new();
    frame::encode_frame(&batch(BatchMode::Delta, 2, &[(1, Val::I(5))]), 65_536, &mut w).unwrap();
    let mut both = w.clone();
    both.extend_from_slice(&w[..w.len() - 2]);
    s.send_raw(&both).unwrap();
    let a = s.recv_ack().unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    let stream = s.into_stream();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let r = k.wait_for("eof handled", |r| link(r, 3002).0 == LinkState::Disconnected);
    assert_eq!(i64_of(&r, 302), Some(5));
    assert!(k.rss_kib() < 64 * 1024);
    k.stop();
}

// Review P1: valid but adversarial frames — a 64 KiB frame of ~32 000 empty observations
// (2 bytes each), far above max_observations_per_batch = 128 — are rejected with the same
// ACK as any oversized batch (REJECTED / BATCH_TOO_LARGE), the connection stays usable,
// nothing changes in the RTDB and no connection retains decoder capacity for them. Before
// the fix each connection kept ~768 KiB of observation capacity after one such frame.
fn oversized_empty_observations_frame(n: usize, seq: u64) -> Vec<u8> {
    fn varint(mut v: u64, out: &mut Vec<u8>) {
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
    }
    let mut inner = vec![0x08, 0x02, 0x10]; // mode = DELTA, source_sequence = ...
    varint(seq, &mut inner);
    for _ in 0..n {
        inner.extend_from_slice(&[0x1a, 0x00]); // observations: empty Observation
    }
    let mut payload = vec![0x12]; // DriverMessage.batch
    varint(inner.len() as u64, &mut payload);
    payload.extend_from_slice(&inner);
    let mut framed = (payload.len() as u32).to_be_bytes().to_vec();
    framed.extend_from_slice(&payload);
    framed
}

#[test]
fn adversarial_many_small_observations_are_rejected_without_retained_memory() {
    for workers in [None, Some(2)] {
        let limits = Limits { max_frame_bytes: 65_536, ..Limits::default() };
        let k = start_mode(&limits, workers, &[]);
        let conns = 16usize;
        let mut sessions: Vec<Session> = (0..conns)
            .map(|i| {
                let src = 3001 + i as u32;
                let mut s = Session::connect(&k.socket, "adv", src, 1).unwrap();
                let a = s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(i as i64))])).unwrap();
                assert_eq!(st(&a), AckStatus::AppliedVolatile);
                s
            })
            .collect();
        let before = k.introspect();
        let rss0 = k.rss_kib();
        let frame = oversized_empty_observations_frame(32_000, 2);
        assert!(frame.len() - 4 <= 65_536, "{}", frame.len());
        for round in 0..20 {
            for s in &mut sessions {
                s.send_raw(&frame).unwrap();
                let a = s.recv_ack().unwrap();
                assert_eq!(
                    (st(&a), a.error_code(), a.source_sequence),
                    (AckStatus::Rejected, ErrorCode::BatchTooLarge, 2),
                    "round {round}"
                );
            }
        }
        let rss1 = k.rss_kib();
        // Old behaviour: >= 16 x 768 KiB retained. Allow allocator noise only.
        assert!(rss1 < rss0 + 4 * 1024, "{workers:?}: RSS grew from {rss0} to {rss1} KiB");
        // Nothing applied, every connection still usable, sequence continues at 2.
        let after = k.introspect();
        assert_eq!(after.state_digest, before.state_digest);
        assert_eq!(after.last_operation_sequence, before.last_operation_sequence);
        assert_eq!(
            after.counters.unwrap().batches_rejected,
            before.counters.unwrap().batches_rejected + 20 * conns as u64
        );
        for (i, s) in sessions.iter_mut().enumerate() {
            let a = s.request(&batch(BatchMode::Delta, 2, &[(1, Val::I(100 + i as i64))])).unwrap();
            assert_eq!(st(&a), AckStatus::AppliedVolatile);
        }
        eprintln!("{workers:?} adversarial frames: RSS {rss0} -> {rss1} KiB over {} rejected frames", 20 * conns);
        k.stop();
    }
}

// Review P2: takeover while the old session's reader is NOT waiting on the socket but on
// backpressure. The old client writes Deltas as fast as it can and never reads its ACKs:
// the kernel's ACK writes fill the socket toward the client, the per-connection ACK queue
// fills (max_inflight), the reader blocks on reserving a slot, and the client's own
// writes block. Then the same source reconnects with only 2 connection slots. The old
// session must be released promptly (well before the 20 s ACK write deadline, so it is
// not the deadline that frees it), without inventing ACKs or losing applied state, and
// its late close must not degrade the new link. Run in current_thread and multi_thread(2).
#[test]
fn takeover_of_a_backpressured_non_reading_link_releases_its_slot_promptly() {
    for workers in [None, Some(2)] {
        let limits = Limits { max_driver_connections: 2, ack_write_timeout_ms: 20_000, ..Limits::default() };
        let k = start_mode(&limits, workers, &[]);
        let mut current = Session::connect(&k.socket, "p2", 3001, 5).unwrap();
        let a = current.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(1))])).unwrap();
        assert_eq!(st(&a), AckStatus::AppliedVolatile);
        let mut next_seq = 2u64;
        let mut stale = Vec::new();
        for round in 0..3 {
            // Old link: flood without reading until our own writes block.
            let mut raw: UnixStream = current.into_stream();
            raw.set_write_timeout(Some(Duration::from_millis(300))).unwrap();
            let first = next_seq;
            let flooder = std::thread::spawn(move || {
                let mut w = Vec::new();
                let mut seq = first;
                loop {
                    frame::encode_frame(&batch(BatchMode::Delta, seq, &[(1, Val::I(seq as i64))]), 65_536, &mut w)
                        .unwrap();
                    if raw.write_all(&w).is_err() {
                        break; // blocked (timeout) or closed
                    }
                    seq += 1;
                }
                (seq, raw)
            });
            let (written_up_to, raw) = flooder.join().unwrap();
            assert!(written_up_to > first + 32, "{workers:?} round {round}: flood too short");
            let busy = k.introspect().counters.unwrap();
            assert_eq!(busy.active_connections, 1);
            // Takeover by the same source while the old reader sits on backpressure.
            let t0 = Instant::now();
            let mut s = Session::connect(&k.socket, "p2", 3001, 5)
                .unwrap_or_else(|e| panic!("{workers:?} round {round}: takeover refused: {e}"));
            let r = k.wait_for("old link released", |r| {
                let c = r.counters.unwrap();
                c.active_connections == 1 && c.links_superseded == round + 1
            });
            let released = t0.elapsed();
            eprintln!("{workers:?} round {round}: old link released {released:?} after the takeover Hello");
            assert!(released < Duration::from_secs(2), "{workers:?}: release took {released:?}");
            assert_eq!(r.counters.unwrap().ack_write_timeouts, 0, "released by the deadline, not the takeover");
            assert_eq!(r.counters.unwrap().connections_refused_limit, 0);
            // No invented ACK, no lost state: the new Welcome reports what was applied, and
            // the tag holds exactly that batch's value (each Delta carries its sequence).
            let last = s.welcome.last_accepted_source_sequence.expect("baseline");
            assert!(last >= first - 1 && last < written_up_to, "{workers:?}: last={last}");
            assert_eq!(i64_of(&r, 301), Some(if last == 1 { 1 } else { last as i64 }));
            // The new link continues without a gap.
            let a = s.request(&batch(BatchMode::Delta, last + 1, &[(1, Val::I((last + 1) as i64))])).unwrap();
            assert_eq!(st(&a), AckStatus::AppliedVolatile, "{workers:?}: {a:?}");
            assert_eq!(a.continuity(), Continuity::Continuous);
            next_seq = last + 2;
            stale.push(raw); // the old socket closes only later
            current = s;
        }
        // Late closes of every old socket: the active link stays Connected and Good.
        let before = link(&k.introspect(), 3001);
        drop(stale);
        std::thread::sleep(Duration::from_millis(200));
        let r = k.introspect();
        assert_eq!(link(&r, 3001), before);
        assert_eq!(before.0, LinkState::Connected);
        assert_eq!(quality(&tag(&r, 301)), Quality::Good);
        let a = current.request(&batch(BatchMode::Delta, next_seq, &[(1, Val::I(0))])).unwrap();
        assert_eq!(st(&a), AckStatus::AppliedVolatile);
        k.stop();
    }
}
