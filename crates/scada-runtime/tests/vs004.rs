//! VS-004 gates (brief §7, ADR 0011) with real kernel and driver processes: liveness,
//! heartbeat, per-tag freshness, negotiation, takeover, capture/replay, hostile input.
//!
//! The exact semantics (boundaries T-ε / T / T+ε, late ticks, stale tokens) are proven
//! with controlled time in `src/supervision.rs` and `scada-core/tests/supervision.rs`.
//! Here wall-clock is used only with wide margins, to prove the integration: the states
//! that must eventually hold, and the ones that must never appear. The suite runs under
//! every scheduler and ingress policy of `scripts/check.sh` (`SCADA_TEST_SCHEDULER`,
//! `SCADA_TEST_INGRESS`).

mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use scada_core::Kernel;
use scada_protocol::driver_v1::{AckStatus, BatchMode, ErrorCode, KernelMessage, kernel_message};
use scada_protocol::introspect_v1::{IntrospectResponse, LinkState, Liveness, Quality};
use scada_sim_driver::{Session, Val, batch, heartbeat, hello, hello_v10};
use support::*;

const SRC: u32 = 1001;
const POLL_MS: u64 = 20;
const LIVE_MS: u64 = 300;
const FRESH_MS: u64 = 500;

fn supervised(live: Option<u64>, fresh: Option<u64>) -> Limits {
    let mut src = String::new();
    if let Some(l) = live {
        src.push_str(&format!("liveness_timeout_ms = {l}\n"));
    }
    if let Some(f) = fresh {
        src.push_str(&format!("freshness_timeout_ms = {f}\n"));
    }
    // VS-004 digest versions (1/2) are asserted here: alarms stay off (VS-005 coupling is
    // tested in vs005.rs).
    Limits { runtime: format!("watchdog_poll_ms = {POLL_MS}\n"), source_1001: src, alarms: None, ..Limits::default() }
}

fn unsupervised() -> Limits {
    Limits { runtime: String::new(), source_1001: String::new(), alarms: None, ..Limits::default() }
}

fn liveness(r: &IntrospectResponse, id: u32) -> Liveness {
    Liveness::try_from(r.sources.iter().find(|s| s.source_id == id).unwrap().liveness).unwrap()
}

fn link(r: &IntrospectResponse, id: u32) -> LinkState {
    LinkState::try_from(r.sources.iter().find(|s| s.source_id == id).unwrap().link_state).unwrap()
}

fn q(r: &IntrospectResponse, t: u32) -> Quality {
    quality(&tag(r, t))
}

fn full(s: &mut Session, seq: u64) {
    let a = s
        .request(&batch(BatchMode::FullState, seq, &[(1, Val::B(true)), (2, Val::B(false)), (3, Val::I(2150))]))
        .unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
}

fn protocol_error(m: KernelMessage) -> ErrorCode {
    match m.body {
        Some(kernel_message::Body::ProtocolError(p)) => p.error_code(),
        other => panic!("expected ProtocolError, got {other:?}"),
    }
}

/// Brief §7.1 + §7.9: supervision absent → VS-003 contract (digest v1, no watchdog, no
/// marker); 1.0 drivers get 1.0, updated drivers get 1.1; a heartbeat on a 1.0 link is an
/// unexpected message exactly as on a VS-003 kernel.
#[test]
fn default_off_is_the_vs003_contract() {
    let k = KernelProc::start(&unsupervised());
    let mut legacy = Session::connect_with(&k.socket, &hello_v10("legacy", SRC, 7)).unwrap();
    assert_eq!((legacy.welcome.negotiated_protocol_major, legacy.welcome.negotiated_protocol_minor), (1, 0));
    full(&mut legacy, 1);
    assert!(!legacy.heartbeat().unwrap(), "no heartbeat on a 1.0 link");
    std::thread::sleep(Duration::from_millis(300));
    let r = k.introspect();
    assert_eq!(r.state_digest_version, 1);
    assert_eq!(liveness(&r, SRC), Liveness::NotSupervised);
    assert!(r.tags.iter().all(|t| t.last_observed_operation == 0));
    let c = r.counters.unwrap();
    assert_eq!((c.watchdog_ticks, c.liveness_expiries, c.freshness_expiries), (0, 0, 0));
    assert_eq!(q(&r, 101), Quality::Good, "no supervision: stays Good indefinitely");
    // A heartbeat forced onto the 1.0 link: fatal, as for any unknown body in 1.0.
    legacy.send(&heartbeat(1)).unwrap();
    assert_eq!(protocol_error(legacy.recv().unwrap()), ErrorCode::UnexpectedMessage);
    // Updated driver: 1.1 negotiated, heartbeats accepted and harmless without supervision.
    let mut new = Session::connect_with(&k.socket, &hello("new", SRC, 7)).unwrap();
    assert_eq!(new.welcome.negotiated_protocol_minor, 1);
    for _ in 0..10 {
        assert!(new.heartbeat().unwrap());
    }
    let a = new.request(&batch(BatchMode::Delta, 2, &[(3, Val::I(1))])).unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "the ACK stream is not disturbed: {a:?}");
    let r = k.wait_for("heartbeats counted", |r| r.counters.as_ref().unwrap().heartbeats_valid == 10);
    assert_eq!(r.counters.unwrap().heartbeats_invalid, 1);
    k.stop();
}

/// Brief §7.2: a live socket with no message → Silent; the link stays Connected; values
/// kept; a new Delta refreshes only its own tag.
#[test]
fn silent_open_socket_is_silent_not_disconnected() {
    let k = KernelProc::start(&supervised(Some(LIVE_MS), None));
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    let r = k.wait_for("silent", |r| liveness(r, SRC) == Liveness::Silent);
    assert_eq!(link(&r, SRC), LinkState::Connected);
    for t in [101, 102, 103] {
        assert_eq!(q(&r, t), Quality::UncertainSourceSilent, "tag {t}");
        assert!(tag(&r, t).value.is_some());
    }
    assert_eq!(r.state_digest_version, 2);
    let a = s.request(&batch(BatchMode::Delta, 2, &[(3, Val::I(2200))])).unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32);
    let r = k.introspect();
    assert_eq!(liveness(&r, SRC), Liveness::Alive, "an applied batch is liveness evidence");
    assert_eq!((q(&r, 101), q(&r, 103)), (Quality::UncertainSourceSilent, Quality::Good));
    let c = r.counters.unwrap();
    assert_eq!(c.liveness_expiries, 1, "one expiry, no repetition while silent");
    k.stop();
}

/// Brief §7.3 and §5.1: heartbeats without data keep the source alive; the tags go stale
/// and are never Good again through heartbeats alone; no operation per heartbeat.
#[test]
fn heartbeats_keep_the_source_alive_but_never_refresh_data() {
    let k = KernelProc::start(&supervised(Some(LIVE_MS), Some(FRESH_MS)));
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    let start = Instant::now();
    let mut saw_stale = false;
    while start.elapsed() < Duration::from_millis(1_200) {
        assert!(s.heartbeat().unwrap());
        std::thread::sleep(Duration::from_millis(25));
        let r = k.introspect();
        assert_eq!(liveness(&r, SRC), Liveness::Alive, "silent despite heartbeats: {r:?}");
        if q(&r, 101) == Quality::UncertainStaleData {
            saw_stale = true;
        }
        if saw_stale {
            assert!(
                r.tags.iter().filter(|t| (101..=103).contains(&t.tag_id)).all(|t| t.quality != Quality::Good as i32)
            );
        }
    }
    assert!(saw_stale);
    let r = k.introspect();
    let c = r.counters.unwrap();
    assert_eq!(c.liveness_expiries, 0);
    assert!(c.heartbeats_valid >= 30);
    // connect + full state + one freshness expiry: heartbeats consumed no operation.
    assert_eq!(r.last_operation_sequence, 3, "{c:?}");
    k.stop();
}

/// Brief §7.3: silence, then one heartbeat → Alive again, tags still uncertain.
#[test]
fn heartbeat_after_silence_restores_communication_only() {
    let k = KernelProc::start(&supervised(Some(LIVE_MS), None));
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    k.wait_for("silent", |r| liveness(r, SRC) == Liveness::Silent);
    assert!(s.heartbeat().unwrap());
    let r = k.wait_for("restored", |r| liveness(r, SRC) == Liveness::Alive);
    assert_eq!(q(&r, 101), Quality::UncertainSourceSilent, "a heartbeat is not a measurement");
    assert_eq!(r.counters.unwrap().liveness_recoveries, 1);
    k.stop();
}

/// Brief §7.4: FullState + partial Deltas → only the observed tag stays Good.
#[test]
fn partial_deltas_refresh_only_their_tags() {
    let k = KernelProc::start(&supervised(None, Some(FRESH_MS)));
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    let mut seq = 2;
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(900) {
        s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))])).unwrap();
        seq += 1;
        std::thread::sleep(Duration::from_millis(40));
    }
    let r = k.introspect();
    assert_eq!(
        (q(&r, 101), q(&r, 102), q(&r, 103)),
        (Quality::UncertainStaleData, Quality::UncertainStaleData, Quality::Good)
    );
    assert_eq!(liveness(&r, SRC), Liveness::NotSupervised);
    assert_eq!(q(&r, 201), Quality::Unknown, "never observed: Unknown");
    k.stop();
}

/// Brief §7.5: identical values with new sequences keep a tag fresh (no revision change);
/// duplicates do not.
#[test]
fn identical_values_refresh_duplicates_do_not() {
    let k = KernelProc::start(&supervised(None, Some(FRESH_MS)));
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    let rev0 = tag(&k.introspect(), 103).revision;
    let mut seq = 2;
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(900) {
        // Same value of channel 3, new sequence; plus a duplicate of it (same sequence).
        let m = batch(BatchMode::Delta, seq, &[(3, Val::I(2150))]);
        assert_eq!(s.request(&m).unwrap().status, AckStatus::AppliedVolatile as i32);
        assert_eq!(s.request(&m).unwrap().status, AckStatus::Duplicate as i32);
        seq += 1;
        std::thread::sleep(Duration::from_millis(40));
    }
    let r = k.introspect();
    assert_eq!(q(&r, 103), Quality::Good);
    assert_eq!(tag(&r, 103).revision, rev0, "same value: same revision, still refreshed");
    assert_eq!(q(&r, 101), Quality::UncertainStaleData);
    // Only duplicates now: tag 103 expires.
    let m = batch(BatchMode::Delta, seq - 1, &[(3, Val::I(2150))]);
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(700) {
        assert_eq!(s.request(&m).unwrap().status, AckStatus::Duplicate as i32);
        std::thread::sleep(Duration::from_millis(40));
    }
    assert_eq!(q(&k.introspect(), 103), Quality::UncertainStaleData);
    k.stop();
}

/// Brief §7.7: takeover. The new link starts alive at its own Welcome, a superseded link
/// cannot keep it alive, no ACK is invented, and the old link's expiries never degrade it.
#[test]
fn takeover_old_link_cannot_keep_or_degrade_the_new_one() {
    let k = KernelProc::start(&supervised(Some(LIVE_MS), Some(FRESH_MS)));
    let mut old = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut old, 1);
    let mut new = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    assert!(new.welcome.link_generation > old.welcome.link_generation);
    // The old link is closed by the kernel (LINK_SUPERSEDED); a heartbeat on it is lost.
    let _ = old.heartbeat();
    // (Err: closed before the reason could be written.)
    if let Ok(m) = old.recv() {
        assert_eq!(protocol_error(m), ErrorCode::LinkSuperseded);
    }
    // The new link observes channel 1 at once and keeps beating.
    let a = new.request(&batch(BatchMode::Delta, 2, &[(1, Val::B(false))])).unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32);
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(250) {
        new.heartbeat().unwrap();
        std::thread::sleep(Duration::from_millis(25));
        let r = k.introspect();
        assert_eq!(liveness(&r, SRC), Liveness::Alive);
        assert_eq!(q(&r, 101), Quality::Good, "old-generation freshness never degrades the new link");
        assert_eq!(q(&r, 102), Quality::UncertainSourceUnavailable, "no elevation on mere connection");
    }
    // Stop beating: the new link goes silent by itself.
    let r = k.wait_for("new link silent", |r| liveness(r, SRC) == Liveness::Silent);
    assert_eq!(q(&r, 101), Quality::UncertainSourceSilent);
    assert_eq!(q(&r, 102), Quality::UncertainSourceUnavailable, "higher precedence kept");
    k.stop();
}

/// Brief §7.8: capture/replay with supervision: the captured stream (with the
/// watchdog's operations) replays to the same digest v2, without any clock.
#[test]
fn capture_replay_reproduces_supervised_state() {
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let cap = dir.path().join("capture.txt");
    let capture = cap.to_str().unwrap().to_owned();
    let k =
        KernelProc::start_in(dir, &supervised(Some(LIVE_MS), Some(FRESH_MS)), None, &["--capture-inputs", &capture]);
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    k.wait_for("silent", |r| liveness(r, SRC) == Liveness::Silent);
    s.heartbeat().unwrap();
    k.wait_for("alive", |r| liveness(r, SRC) == Liveness::Alive);
    for seq in 2..6 {
        s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))])).unwrap();
    }
    // Alive (heartbeats) long enough for the Deltas to go stale: X operations.
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(FRESH_MS + 300) {
        s.heartbeat().unwrap();
        std::thread::sleep(Duration::from_millis(25));
    }
    drop(s);
    let r = k.wait_for("disconnected", |r| link(r, SRC) == LinkState::Disconnected);
    let inputs = scada_runtime::capture::read(&cap).unwrap();
    let kinds: String = std::fs::read_to_string(&cap).unwrap().lines().map(|l| &l[..1]).collect();
    assert!(kinds.contains('S') && kinds.contains('R') && kinds.contains('X'), "{kinds}");
    let cfg = scada_runtime::config::load(&k.dir.path().join("kernel.toml")).unwrap();
    let mut replay = Kernel::new(Arc::new(cfg.core.clone()));
    for i in inputs {
        let _ = replay.apply(i);
    }
    assert_eq!(replay.state_digest().to_vec(), r.state_digest);
    assert_eq!(replay.last_operation_sequence(), r.last_operation_sequence);
    k.stop();
}

/// Heartbeat with a non-increasing sequence: fatal, typed; the link is disconnected.
#[test]
fn invalid_heartbeat_closes_the_link() {
    let k = KernelProc::start(&supervised(Some(LIVE_MS), None));
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    full(&mut s, 1);
    s.send(&heartbeat(5)).unwrap();
    s.send(&heartbeat(5)).unwrap();
    assert_eq!(protocol_error(s.recv().unwrap()), ErrorCode::InvalidHeartbeat);
    let r = k.wait_for("disconnected", |r| link(r, SRC) == LinkState::Disconnected);
    assert_eq!(q(&r, 101), Quality::UncertainSourceUnavailable);
    assert_eq!(r.counters.unwrap().heartbeats_invalid, 1);
    let mut s = Session::connect(&k.socket, "d", SRC, 7).unwrap();
    s.send(&heartbeat(0)).unwrap();
    assert_eq!(protocol_error(s.recv().unwrap()), ErrorCode::InvalidHeartbeat);
    k.stop();
}

/// Brief §7.11: a heartbeat flood grows no queue or memory, consumes no operation
/// sequence and does not starve another source.
#[test]
fn heartbeat_flood_is_bounded_and_does_not_starve() {
    let k = KernelProc::start(&supervised(Some(LIVE_MS), Some(FRESH_MS)));
    let mut s = Session::connect(&k.socket, "flood", SRC, 7).unwrap();
    full(&mut s, 1);
    let op0 = k.introspect().last_operation_sequence;
    let rss0 = k.rss_kib();
    let socket = k.socket.clone();
    let other = std::thread::spawn(move || {
        let mut o = Session::connect(&socket, "other", 3001, 9).unwrap();
        let start = Instant::now();
        o.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap();
        for seq in 2..=300u64 {
            let a = o.request(&batch(BatchMode::Delta, seq, &[(1, Val::I(seq as i64))])).unwrap();
            assert_eq!(a.status, AckStatus::AppliedVolatile as i32);
        }
        start.elapsed()
    });
    const N: u64 = 200_000;
    for seq in 1..=N {
        // Buffered writes, flushed in chunks: the kernel reads them as fast as it can.
        s.send(&heartbeat(seq)).unwrap();
        if seq % 1_000 == 0 {
            s.flush().unwrap();
        }
    }
    s.flush().unwrap();
    let other = other.join().unwrap();
    let r = k.wait_for("flood read", |r| r.counters.as_ref().unwrap().heartbeats_valid == N);
    let c = r.counters.unwrap();
    assert!(c.heartbeats_coalesced > N / 2, "{c:?}");
    assert_eq!(c.heartbeats_invalid, 0);
    // The other source: connect + full + 299 deltas; the flooding source: no operation
    // (at most one freshness expiry while the test runs).
    let ops = r.last_operation_sequence - op0;
    assert!((301..=303).contains(&ops), "{ops} operations for a heartbeat flood");
    assert!(other < Duration::from_secs(20), "other source starved: {other:?}");
    let rss1 = k.rss_kib();
    assert!(rss1 < rss0 + 8 * 1024, "RSS grew {rss0} -> {rss1} KiB");
    k.stop();
}
