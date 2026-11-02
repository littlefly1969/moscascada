//! VS-006 gates (brief §6/§10, ADR 0013, ADR 0014) with real kernel and driver processes:
//! default-off (J10), durable receipts, restart and recovery (J2, J5, J6), reconciliation
//! and history continuity (J8), crash at every fault point (T05), torn WAL (T06),
//! slow/stalled/full/failing disk (T07), ring release by the journal (T08), durable ACK
//! deduplication (T21), clock anomalies (T15–T19), driver compatibility (T20), no command
//! on replay (T27), store permissions and single writer (T28).
//!
//! The storage-level properties (coherent prefix, fail-closed corruption, checkpoint
//! fallback, ENOSPC without publication) are proven in `scada-journal/tests/journal.rs`.

mod support;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use scada_protocol::alarm_v1 as apb;
use scada_protocol::driver_v1::{AckStatus, BatchMode, ErrorCode, InputAck};
use scada_protocol::history_v1 as hpb;
use scada_protocol::introspect_v1::{IntrospectResponse, Quality};
use scada_sim_driver::{AlarmClient, HistoryClient, Session, Val, batch, hello_durable, hello_minor, hello_v10};
use support::*;

const SRC: u32 = 1001;

fn journal_limits(extra: &str, alarms: Option<Alarms>) -> Limits {
    Limits { persistence: Some(extra.to_owned()), alarms, ..Limits::default() }
}

fn alarms(capacity: u64) -> Alarms {
    Alarms::with(capacity, demo_rules())
}

fn history_sock(k: &KernelProc) -> PathBuf {
    k.dir.path().join("history.sock")
}

fn db_path(dir: &Path) -> PathBuf {
    dir.join("journal").join("kernel.db")
}

fn hist(k: &KernelProc) -> HistoryClient {
    HistoryClient::connect(&history_sock(k)).expect("history socket")
}

fn wait<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(start.elapsed() < Duration::from_secs(20), "timeout waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Every event of the historian (durable prefix), in event order.
fn all_events(h: &mut HistoryClient) -> Vec<hpb::HistoryEvent> {
    let mut out = Vec::new();
    let mut after = 0;
    loop {
        let p = h.history(after, 0, 0).unwrap();
        out.extend(p.events);
        if !p.has_more {
            return out;
        }
        after = p.next_after_event_seq;
    }
}

fn wait_durable_op(k: &KernelProc, op: u64) -> hpb::JournalStatus {
    let mut h = hist(k);
    wait("durable operation", || {
        let j = h.journal().unwrap();
        (j.durable_operation_seq >= op).then_some(j)
    })
}

fn full(s: &mut Session, seq: u64, door: bool, temp: i64) -> InputAck {
    s.request(&batch(BatchMode::FullState, seq, &[(1, Val::B(door)), (2, Val::B(false)), (3, Val::I(temp))])).unwrap()
}

fn delta_door(s: &mut Session, seq: u64, door: bool) -> InputAck {
    s.request(&batch(BatchMode::Delta, seq, &[(1, Val::B(door))])).unwrap()
}

fn ready_u64(ready: &str, key: &str) -> u64 {
    let k = format!(" {key}=");
    let i = ready.find(&k).unwrap_or_else(|| panic!("{key} not in {ready}")) + k.len();
    ready[i..].split(' ').next().unwrap().parse().unwrap()
}

fn tag_q(r: &IntrospectResponse, id: u32) -> Quality {
    quality(&tag(r, id))
}

fn restart(dir: tempfile::TempDir, limits: &Limits, extra: &[&str]) -> KernelProc {
    KernelProc::start_in(dir, limits, None, extra)
}

fn ack_result(r: apb::AlarmResponse) -> apb::AckResult {
    match r.body {
        Some(apb::alarm_response::Body::Ack(a)) => a,
        other => panic!("expected an ACK result, got {other:?}"),
    }
}

fn snapshot_entry(c: &mut AlarmClient, id: u32) -> apb::AlarmEntry {
    match c.snapshot(0, 0, 0).unwrap().body {
        Some(apb::alarm_response::Body::Snapshot(p)) => p.alarms.into_iter().find(|a| a.alarm_id == id).unwrap(),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------- T01 / J10

/// T01/J10: without `[persistence]` nothing changes: no journal, no durable receipts
/// even when asked, VS-005 READY line, APPLIED_VOLATILE.
#[test]
fn default_off_is_the_vs005_contract() {
    let k = KernelProc::start(&Limits { persistence: None, ..Limits::default() });
    assert!(!k.ready.contains("persistence="), "{}", k.ready);
    assert!(!k.dir.path().join("journal").exists());
    assert!(!history_sock(&k).exists());
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    assert_eq!(s.welcome.negotiated_protocol_minor, 2);
    assert!(!s.welcome.durable_receipts, "no journal: no durable receipts");
    assert!(s.welcome.timeline_id.is_empty());
    let a = full(&mut s, 1, false, 2000);
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
    let r = k.introspect();
    assert!(r.persistence.is_none());
    k.stop();
}

/// `--check-config` reports the RAM of the persistence path and the 4 GiB Micro share.
#[test]
fn check_config_reports_persistence_memory() {
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let cfg = dir.path().join("k.toml");
    std::fs::write(&cfg, demo_config(dir.path(), &journal_limits("", Some(alarms(256))), None)).unwrap();
    let out =
        std::process::Command::new(kernel_bin()).arg("--config").arg(&cfg).arg("--check-config").output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{text}");
    for item in [
        "event_ring",
        "staging",
        "journal_queue",
        "checkpoint",
        "history_sessions",
        "sqlite_cache",
        "persistence_total",
    ] {
        assert!(text.contains(&format!("persistence memory: {item}")), "{item} missing:\n{text}");
    }
    assert!(text.contains("% of 4 GiB Micro"), "{text}");
    assert!(text.contains("durability=full"), "{text}");
}

// --------------------------------------------------------------- durable receipts (J1)

/// J1: a 1.2 driver that asks for durable receipts gets APPLIED_DURABLE only after the
/// commit; the operation is in the store when the ACK arrives.
#[test]
fn durable_receipts_follow_the_commit() {
    let k = KernelProc::start(&journal_limits("", None));
    assert!(k.ready.contains("persistence=full"), "{}", k.ready);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    assert_eq!(s.welcome.negotiated_protocol_minor, 2);
    assert!(s.welcome.durable_receipts);
    assert_eq!(s.welcome.timeline_id.len(), 16);
    let a = full(&mut s, 1, false, 2000);
    assert_eq!(a.status, AckStatus::AppliedDurable as i32, "{a:?}");
    let op = a.operation_sequence.expect("operation sequence on a durable ACK");
    // Already durable when the ACK was written: no waiting needed.
    let j = hist(&k).journal().unwrap();
    assert!(j.durable_operation_seq >= op, "{j:?} < {op}");
    assert_eq!(j.durability, hpb::Durability::PowerLossDurable as i32);
    for seq in 2..50 {
        let a = delta_door(&mut s, seq, seq % 2 == 0);
        assert_eq!(a.status, AckStatus::AppliedDurable as i32, "{a:?}");
        let op = a.operation_sequence.unwrap();
        assert!(hist(&k).journal().unwrap().durable_operation_seq >= op);
    }
    // A duplicate is answered as such (and only after the original is durable).
    let d = delta_door(&mut s, 49, false);
    assert_eq!(d.status, AckStatus::Duplicate as i32, "{d:?}");
    k.stop();
}

/// T20: 1.0, 1.1 and 1.2 drivers against a journaling kernel. Only a 1.2 driver that asks
/// gets durable receipts; the others keep their VS-005 contract.
#[test]
fn driver_versions_against_a_journaling_kernel() {
    let k = KernelProc::start(&journal_limits("", None));
    let mut v10 = Session::connect_with(&k.socket, &hello_v10("v10", SRC, 1)).unwrap();
    assert_eq!(v10.welcome.negotiated_protocol_minor, 0);
    assert!(!v10.welcome.durable_receipts);
    assert_eq!(full(&mut v10, 1, false, 1).status, AckStatus::AppliedVolatile as i32);
    drop(v10);
    let mut v11 = Session::connect_with(&k.socket, &hello_minor("v11", SRC, 2, 1)).unwrap();
    assert_eq!(v11.welcome.negotiated_protocol_minor, 1);
    assert!(!v11.welcome.durable_receipts);
    assert_eq!(full(&mut v11, 1, false, 1).status, AckStatus::AppliedVolatile as i32);
    drop(v11);
    // 1.2 without the request: volatile ACKs (and its timestamps are still journaled).
    let mut v12 = Session::connect_with(&k.socket, &hello_minor("v12", SRC, 3, 2)).unwrap();
    assert_eq!(v12.welcome.negotiated_protocol_minor, 2);
    assert!(!v12.welcome.durable_receipts);
    assert_eq!(full(&mut v12, 1, false, 1).status, AckStatus::AppliedVolatile as i32);
    drop(v12);
    let mut v12d = Session::connect_with(&k.socket, &hello_durable("v12d", SRC, 4)).unwrap();
    assert!(v12d.welcome.durable_receipts);
    assert_eq!(full(&mut v12d, 1, false, 1).status, AckStatus::AppliedDurable as i32);
    k.stop();
}

/// Normal (non-durable) journaling exists only for benchmarks and never grants durable
/// receipts; it is refused without the explicit test flag.
#[test]
fn normal_durability_is_benchmark_only() {
    let limits = journal_limits("durability = \"normal\"\n", None);
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let cfg = dir.path().join("k.toml");
    std::fs::write(&cfg, demo_config(dir.path(), &limits, None)).unwrap();
    let out = std::process::Command::new(kernel_bin()).arg("--config").arg(&cfg).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("--test-allow-non-durable"));
    let k = KernelProc::start_with(&limits, None, &["--test-allow-non-durable"]);
    assert!(k.ready.contains("persistence=normal_NOT_DURABLE"), "{}", k.ready);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    assert!(!s.welcome.durable_receipts, "never durable receipts with synchronous=NORMAL");
    assert_eq!(full(&mut s, 1, false, 1).status, AckStatus::AppliedVolatile as i32);
    assert_eq!(hist(&k).journal().unwrap().durability, hpb::Durability::NotDurableBenchmarkOnly as i32);
    k.stop();
}

// ------------------------------------------------------------- restart / recovery (J5)

/// T11/T13/T26/J5/J6/J8: restart with Good values and a latched alarm. After recovery the
/// values are RecoveredUnverified (not Good), the alarm keeps its state, no event is
/// produced by the restart itself; the first FullState verifies the source, marks the
/// reconciliation events, and the history continuity of a snapshot-only driver is
/// Unknown (never Complete).
#[test]
fn restart_recovers_unverified_then_reconciles() {
    let limits = journal_limits("", Some(alarms(256)));
    let k = KernelProc::start(&limits);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 7)).unwrap();
    assert_eq!(full(&mut s, 1, false, 2000).status, AckStatus::AppliedDurable as i32);
    // Door open: alarm 1 raised (ActiveUnacked), then closed: ReturnedUnacked (latched).
    assert_eq!(delta_door(&mut s, 2, true).status, AckStatus::AppliedDurable as i32);
    let last = delta_door(&mut s, 3, false);
    let r = k.introspect();
    assert_eq!(tag_q(&r, 101), Quality::Good);
    let digest_before = r.state_digest.clone();
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let before = snapshot_entry(&mut c, 1);
    assert_eq!(before.state, apb::EpisodeState::ReturnedUnacked as i32);
    let events_before = all_events(&mut hist(&k));
    assert!(events_before.iter().any(|e| e.alarm_id == 1 && e.kind == hpb::EventKind::Raised as i32));
    assert!(events_before.iter().all(|e| !e.reconciliation));
    drop(c);
    drop(s);
    let (_err, dir) = k.kill_keep();

    let k = restart(dir, &limits, &[]);
    assert!(k.ready.contains("boot=2"), "{}", k.ready);
    // Everything acknowledged durably is recovered (plus the disconnection, also journaled).
    assert!(ready_u64(&k.ready, "durable_op") > last.operation_sequence.unwrap(), "{}", k.ready);
    let r = k.introspect();
    assert_ne!(r.state_digest, Vec::<u8>::new());
    let _ = digest_before;
    assert_eq!(tag_q(&r, 101), Quality::UncertainRecoveredUnverified, "J5: last known, not live");
    assert_eq!(tag_q(&r, 103), Quality::UncertainRecoveredUnverified);
    let src = r.sources.iter().find(|x| x.source_id == SRC).unwrap();
    assert!(src.unverified);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let after = snapshot_entry(&mut c, 1);
    assert_eq!(after.state, before.state, "latched state survives");
    assert_eq!(after.activation_token, before.activation_token);
    // J6: the restart produced no event (in particular no false Raised/Returned).
    let mut h = hist(&k);
    let events_mid = all_events(&mut h);
    // The disconnection (drop(s) above) is journaled too and may or may not be durable
    // before the kill, so its events (input invalid) may follow `events_before`. What J6
    // forbids is an event from the recovery: every event belongs to an operation of the
    // first boot (<= the recovered durable operation) and the old events are unchanged.
    let recovered_op = ready_u64(&k.ready, "durable_op");
    assert!(events_mid.len() >= events_before.len());
    // durable_at is written by the *next* commit (ADR 0014 §2): it may be filled in only
    // after the restart. Everything else must be identical.
    let strip = |v: &[hpb::HistoryEvent]| -> Vec<hpb::HistoryEvent> {
        v.iter()
            .cloned()
            .map(|mut e| {
                e.durable_at_utc_ns = None;
                e
            })
            .collect()
    };
    assert_eq!(strip(&events_mid[..events_before.len()]), strip(&events_before), "recovered history is unchanged");
    for e in &events_mid {
        assert!(e.operation_seq <= recovered_op, "no event from the recovery itself: {e:?}");
        assert!(!e.reconciliation, "{e:?}");
    }
    let rec = h.recovery().unwrap();
    assert_eq!(rec.boot_generation, 2);
    let sr = rec.sources.iter().find(|x| x.source_id == SRC).unwrap();
    assert_eq!(sr.live, hpb::LiveVerification::UnverifiedRecovered as i32);
    assert_eq!(sr.continuity, hpb::HistoryContinuity::Unknown as i32);
    let p = h.projection(0, 0).unwrap();
    assert!(p.recovery_in_progress, "{p:?}");

    // Same epoch, contiguous resume: FullState with the door OPEN (changed while down).
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 7)).unwrap();
    let ack = full(&mut s, 4, true, 2000);
    assert_eq!(ack.status, AckStatus::AppliedDurable as i32, "{ack:?}");
    let r = k.introspect();
    assert_eq!(tag_q(&r, 101), Quality::Good);
    assert!(!r.sources.iter().find(|x| x.source_id == SRC).unwrap().unverified);
    let events = all_events(&mut h);
    let new: Vec<_> = events.iter().filter(|e| e.event_seq > events_mid.last().map_or(0, |e| e.event_seq)).collect();
    assert!(!new.is_empty(), "the reconciliation raised the alarm again");
    for e in &new {
        assert!(e.reconciliation, "J6: reconciliation events are marked: {e:?}");
        assert!(e.actual_field_time_unknown, "{e:?}");
    }
    let rec = h.recovery().unwrap();
    let sr = rec.sources.iter().find(|x| x.source_id == SRC).unwrap();
    assert_eq!(sr.live, hpb::LiveVerification::LiveVerified as i32);
    assert_eq!(sr.continuity, hpb::HistoryContinuity::Unknown as i32, "J8: a snapshot driver never proves Complete");
    assert!(sr.gap_records >= 1, "the Unknown interval is persisted");
    k.stop();
}

/// T12: partial reconnection: only the source that sent a FullState is verified.
#[test]
fn partial_reconnect_verifies_only_that_source() {
    let limits = journal_limits("", None);
    let k = KernelProc::start(&limits);
    let mut a = Session::connect_with(&k.socket, &hello_durable("a", SRC, 1)).unwrap();
    full(&mut a, 1, false, 1);
    let mut b = Session::connect_with(&k.socket, &hello_durable("b", 3001, 1)).unwrap();
    b.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(5))])).unwrap();
    drop((a, b));
    let (_e, dir) = k.kill_keep();
    let k = restart(dir, &limits, &[]);
    let mut a = Session::connect_with(&k.socket, &hello_durable("a", SRC, 1)).unwrap();
    full(&mut a, 2, false, 1);
    let r = k.introspect();
    assert_eq!(tag_q(&r, 101), Quality::Good);
    assert_eq!(tag_q(&r, 301), Quality::UncertainRecoveredUnverified, "not reconnected: still unverified");
    let rec = hist(&k).recovery().unwrap();
    let live = |id: u32| rec.sources.iter().find(|x| x.source_id == id).map(|x| x.live).unwrap();
    assert_eq!(live(SRC), hpb::LiveVerification::LiveVerified as i32);
    assert_eq!(live(3001), hpb::LiveVerification::UnverifiedRecovered as i32);
    k.stop();
}

/// J8: a new epoch after the restart is Unknown with unknown bounds; a sequence hole in
/// the same epoch is a Gap.
#[test]
fn continuity_gap_and_new_epoch() {
    let limits = journal_limits("", None);
    let k = KernelProc::start(&limits);
    let mut a = Session::connect_with(&k.socket, &hello_durable("a", SRC, 1)).unwrap();
    full(&mut a, 1, false, 1);
    let mut b = Session::connect_with(&k.socket, &hello_durable("b", 3001, 1)).unwrap();
    b.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(5))])).unwrap();
    drop((a, b));
    let (_e, dir) = k.kill_keep();
    let k = restart(dir, &limits, &[]);
    // Same epoch, sequence jumps 1 -> 10: Gap.
    let mut a = Session::connect_with(&k.socket, &hello_durable("a", SRC, 1)).unwrap();
    assert_eq!(full(&mut a, 10, false, 1).status, AckStatus::AppliedDurable as i32);
    // New epoch: Unknown.
    let mut b = Session::connect_with(&k.socket, &hello_durable("b", 3001, 2)).unwrap();
    assert_eq!(
        b.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(5))])).unwrap().status,
        AckStatus::AppliedDurable as i32
    );
    let rec = hist(&k).recovery().unwrap();
    let cont = |id: u32| rec.sources.iter().find(|x| x.source_id == id).map(|x| x.continuity).unwrap();
    assert_eq!(cont(SRC), hpb::HistoryContinuity::Gap as i32);
    assert_eq!(cont(3001), hpb::HistoryContinuity::Unknown as i32);
    k.stop();
    // The gaps are queryable offline.
}

/// Graceful stop drains the journal: everything applied before SIGTERM is durable.
#[test]
fn sigterm_drains_the_journal() {
    let limits = journal_limits("", None);
    let k = KernelProc::start(&limits);
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    let mut last = 0;
    for seq in 2..500 {
        last =
            s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))])).unwrap().operation_sequence.unwrap();
    }
    drop(s);
    let (err, dir) = k.stop_keep();
    assert!(err.contains("persistence: shutdown"), "{err}");
    let k = restart(dir, &limits, &[]);
    // The disconnection of the session is the last journaled operation.
    assert_eq!(ready_u64(&k.ready, "durable_op"), last + 1, "{}", k.ready);
    k.stop();
}

// ------------------------------------------------------------ T05: crash at fault points

/// Outcome of a workload cut by a crash: highest durably acknowledged (source sequence,
/// operation sequence).
struct Cut {
    last_durable_seq: u64,
    last_durable_op: u64,
    sent_seq: u64,
}

/// FullState then deltas (tag 103 = source sequence) with durable receipts, until the
/// kernel dies or `n` batches are sent. Alarm 3 (high > 3000) toggles every 4 batches.
fn workload_until_crash(k: &KernelProc, epoch: u64, n: u64) -> Cut {
    let mut cut = Cut { last_durable_seq: 0, last_durable_op: 0, sent_seq: 0 };
    let Ok(mut s) = Session::connect_with(&k.socket, &hello_durable("d", SRC, epoch)) else { return cut };
    for seq in 1..=n {
        cut.sent_seq = seq;
        let temp = if (seq / 4) % 2 == 0 { seq as i64 } else { 5000 + seq as i64 };
        let msg = if seq == 1 {
            batch(BatchMode::FullState, 1, &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(temp))])
        } else {
            batch(BatchMode::Delta, seq, &[(3, Val::I(temp))])
        };
        match s.request(&msg) {
            Ok(a) if a.status == AckStatus::AppliedDurable as i32 => {
                cut.last_durable_seq = seq;
                cut.last_durable_op = a.operation_sequence.unwrap();
            }
            Ok(a) => panic!("unexpected ACK before the crash: {a:?}"),
            Err(_) => return cut,
        }
    }
    cut
}

/// T05/J1/J2/J3: abort at each fault point; the restarted kernel recovers a coherent
/// prefix that contains every durably acknowledged operation, the event sequence has no
/// hole, the recovered value is the one of the last recovered batch, and the source can
/// resume (a batch the driver never saw acknowledged is either a Duplicate or applied).
#[test]
fn crash_at_every_fault_point_recovers_a_coherent_prefix() {
    for point in scada_journal::fault::POINTS {
        for nth in [1u64, 7, 31] {
            let (extra, n) = match *point {
                "during_checkpoint" => ("checkpoint_every_operations = 10\n", nth.min(7)),
                _ => ("checkpoint_every_operations = 25\n", nth),
            };
            let limits = journal_limits(extra, Some(alarms(64)));
            let spec = format!("{point}:{n}");
            let k = KernelProc::start_with(&limits, None, &["--test-fault", &spec]);
            let cut = workload_until_crash(&k, 9, 400);
            let (status, err, dir) = k.wait_exit(Duration::from_secs(10));
            assert!(!status.success(), "{spec}: the kernel must have aborted ({status:?})\n{err}");
            assert!(err.contains(&format!("TEST FAULT: abort at {point}")), "{spec}: {err}");
            assert!(cut.sent_seq < 400, "{spec}: workload not cut");

            let k = restart(dir, &limits, &[]);
            let durable_op = ready_u64(&k.ready, "durable_op");
            assert!(
                durable_op >= cut.last_durable_op,
                "{spec}: J1 lost a durable ACK: {} < {}",
                durable_op,
                cut.last_durable_op
            );
            let r = k.introspect();
            let src = r.sources.iter().find(|x| x.source_id == SRC).unwrap();
            let recovered_seq = src.last_accepted_source_sequence.unwrap_or(0);
            assert!(recovered_seq >= cut.last_durable_seq, "{spec}: {recovered_seq} < {}", cut.last_durable_seq);
            assert!(recovered_seq <= cut.sent_seq, "{spec}: invented a batch");
            if recovered_seq > 0 {
                // Coherent prefix: tag 103 holds exactly the value of that batch.
                let temp =
                    if (recovered_seq / 4) % 2 == 0 { recovered_seq as i64 } else { 5000 + recovered_seq as i64 };
                let t = tag(&r, 103);
                assert_eq!(t.value, Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(temp)), "{spec}");
                assert_eq!(quality(&t), Quality::UncertainRecoveredUnverified, "{spec}");
            }
            // J3: event sequence without holes, all within the durable watermark.
            let mut h = hist(&k);
            let ev = all_events(&mut h);
            let dj = h.journal().unwrap();
            for (i, e) in ev.iter().enumerate() {
                assert_eq!(e.event_seq, i as u64 + 1, "{spec}: hole in the event sequence");
                assert!(e.event_seq <= dj.durable_event_seq);
                assert!(!e.reconciliation, "{spec}");
            }
            // Resume: the batch after the recovered one is applied; the one the driver
            // never saw acknowledged (if recovered) is a Duplicate.
            let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 9)).unwrap();
            if recovered_seq > cut.last_durable_seq {
                // Resent delta: an unverified source accepts only a FullState, so the
                // answer is RESYNC_REQUIRED whose hint says the batch is already in.
                let d = s.request(&batch(BatchMode::Delta, recovered_seq, &[(3, Val::I(1))])).unwrap();
                assert!(
                    d.status == AckStatus::Duplicate as i32
                        || (d.status == AckStatus::Rejected as i32 && d.error_code == ErrorCode::ResyncRequired as i32),
                    "{spec}: {d:?}"
                );
                assert_eq!(d.last_accepted_source_sequence, Some(recovered_seq), "{spec}: resync hint");
            }
            let a = s
                .request(&batch(
                    BatchMode::FullState,
                    recovered_seq + 1,
                    &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(0))],
                ))
                .unwrap();
            assert_eq!(a.status, AckStatus::AppliedDurable as i32, "{spec}: {a:?}");
            assert_eq!(tag_q(&k.introspect(), 103), Quality::Good, "{spec}");
            k.stop();
        }
    }
}

// --------------------------------------------------------------- T06: torn WAL tail

/// T06 (power-cut SIMULATION, not a physical power cut): after a kill -9 the WAL tail is
/// truncated at arbitrary offsets (frames never fsynced are what a power cut loses) or its
/// last bytes are corrupted. SQLite ignores frames after the first invalid one: the
/// recovered state is a coherent prefix (J2) or the startup is refused (fail-closed),
/// never an inconsistent state.
#[test]
fn torn_wal_tail_gives_a_prefix_or_a_refusal() {
    let limits = journal_limits("checkpoint_every_operations = 1000000\nwal_max_bytes = 1073741824\n", None);
    for (case, cut_frac) in [(0, 0.95), (1, 0.6), (2, 0.3), (3, 0.0)] {
        let k = KernelProc::start(&limits);
        let cut = workload_until_crash(&k, 3, 120);
        assert_eq!(cut.sent_seq, 120);
        let (_e, dir) = k.kill_keep();
        let wal = db_path(dir.path()).with_extension("db-wal");
        let len = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
        assert!(len > 0, "WAL expected");
        let f = std::fs::OpenOptions::new().write(true).open(&wal).unwrap();
        if case == 3 {
            // Corrupt the last 100 bytes instead of truncating.
            use std::os::unix::fs::FileExt;
            f.write_at(&[0xA5; 100], len - 100).unwrap();
        } else {
            f.set_len((len as f64 * cut_frac) as u64).unwrap();
        }
        drop(f);
        let cfg = dir.path().join("kernel.toml");
        let mut child = std::process::Command::new(kernel_bin())
            .arg("--config")
            .arg(&cfg)
            .arg("--introspection-socket")
            .arg(dir.path().join("intro.sock"))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        // Either READY (prefix) or a refusal; never a crash with a panic.
        let mut out = String::new();
        use std::io::BufRead;
        let mut rd = std::io::BufReader::new(child.stdout.take().unwrap());
        let _ = rd.read_line(&mut out);
        if out.starts_with("READY") {
            let op = ready_u64(&out, "durable_op");
            assert!(op <= cut.last_durable_op + 1, "case {case}: {out}");
            // Coherent: the introspection value matches the recovered source sequence.
            let intro = dir.path().join("intro.sock");
            let r = support::introspect_at(&intro);
            let src = r.sources.iter().find(|x| x.source_id == SRC).unwrap();
            let seq = src.last_accepted_source_sequence.unwrap_or(0);
            if seq > 0 {
                let temp = if (seq / 4) % 2 == 0 { seq as i64 } else { 5000 + seq as i64 };
                assert_eq!(
                    tag(&r, 103).value,
                    Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(temp)),
                    "case {case}"
                );
            }
            let _ = child.kill();
            let _ = child.wait();
        } else {
            let st = child.wait().unwrap();
            let mut err = String::new();
            use std::io::Read;
            child.stderr.take().unwrap().read_to_string(&mut err).unwrap();
            assert!(!st.success(), "case {case}");
            assert!(err.contains("persistence"), "case {case}: {err}");
            assert!(!err.contains("panicked"), "case {case}: {err}");
        }
    }
}

// --------------------------------------------------------- T07: slow / stalled / full disk

/// T07: a slow disk only adds latency; a stalled disk fills the bounded journal queue and
/// the kernel refuses new operations with JOURNAL_QUEUE_FULL before applying them (J4),
/// then recovers when the disk resumes. No ACK is ever durable before its commit.
#[test]
fn stalled_disk_backpressure_then_recovery() {
    let limits = journal_limits("journal_queue_operations = 16\n", None);
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let stall = dir.path().join("stall");
    let stall_s = stall.display().to_string();
    let k = KernelProc::start_in(dir, &limits, None, &["--test-journal-stall-file", &stall_s]);
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    let before = wait_durable_op(&k, 2).durable_operation_seq;
    std::fs::write(&stall, b"").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut refused = None;
    for seq in 2..200 {
        let a = s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))])).unwrap();
        if a.status == AckStatus::Rejected as i32 {
            assert_eq!(a.error_code, ErrorCode::JournalQueueFull as i32, "{a:?}");
            refused = Some(seq);
            break;
        }
        assert_eq!(a.status, AckStatus::AppliedVolatile as i32);
    }
    let refused = refused.expect("the bounded queue must refuse while the disk is stalled");
    let r = k.introspect();
    let p = r.persistence.clone().unwrap();
    assert!(p.refused_queue_full >= 1);
    assert_eq!(p.durable_operation_sequence, before, "nothing committed while stalled");
    // The refused batch changed nothing: J4.
    assert_eq!(r.sources.iter().find(|x| x.source_id == SRC).unwrap().last_accepted_source_sequence, Some(refused - 1));
    std::fs::remove_file(&stall).unwrap();
    let a = wait("queue drains", || {
        let a = s.request(&batch(BatchMode::Delta, refused, &[(3, Val::I(1))])).unwrap();
        (a.status != AckStatus::Rejected as i32).then_some(a)
    });
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32);
    wait_durable_op(&k, a.operation_sequence.unwrap());
    k.stop();
}

/// T07: slow disk (2 ms per commit): durable receipts are delayed, never early.
#[test]
fn slow_disk_delays_durable_receipts() {
    let limits = journal_limits("", None);
    let k = KernelProc::start_with(&limits, None, &["--test-journal-delay-us", "2000"]);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    let t = Instant::now();
    full(&mut s, 1, false, 0);
    for seq in 2..=50 {
        let a = s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))])).unwrap();
        assert_eq!(a.status, AckStatus::AppliedDurable as i32);
        assert!(hist(&k).journal().unwrap().durable_operation_seq >= a.operation_sequence.unwrap());
    }
    assert!(t.elapsed() >= Duration::from_millis(50 * 2), "each receipt waited for its commit");
    k.stop();
}

/// T07/T23: ENOSPC (simulated with a page cap): the failing commit publishes nothing
/// (no watermark, no durable receipt: a durable batch caught in it waits, it is never
/// answered APPLIED_DURABLE), the journal enters DISK_FULL and admission is refused with
/// DISK_FULL before any state change; the kernel stays up and recovers a coherent prefix.
#[test]
fn disk_full_refuses_and_publishes_nothing() {
    let limits = journal_limits("checkpoint_every_operations = 1000000\n", Some(alarms(256)));
    let k = KernelProc::start_with(&limits, None, &["--test-journal-max-pages", "40"]);
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    let mut refused = None;
    for seq in 2..20_000u64 {
        let a = s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I((seq % 7000) as i64))])).unwrap();
        if a.status == AckStatus::Rejected as i32 {
            assert_eq!(a.error_code, ErrorCode::DiskFull as i32, "{a:?}");
            refused = Some(seq);
            break;
        }
        assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
    }
    let refused = refused.expect("the page cap must be reached");
    let mut h = hist(&k);
    let j = h.journal().unwrap();
    assert_eq!(j.state, hpb::JournalState::DiskFull as i32, "{j:?}");
    assert!(j.refused_disk_full >= 1);
    let r = k.introspect();
    assert_eq!(
        r.sources.iter().find(|x| x.source_id == SRC).unwrap().last_accepted_source_sequence,
        Some(refused - 1),
        "J4: the refused batch changed nothing"
    );
    let applied = r.persistence.unwrap();
    assert!(applied.durable_operation_sequence < r.last_operation_sequence, "the tail is not durable and not claimed");
    // A new link is an operation too: refused at the handshake (nothing applied).
    match Session::connect_with(&k.socket, &hello_durable("d2", 3001, 1)) {
        Err(scada_sim_driver::ClientError::Rejected { code, .. }) => assert_eq!(code, ErrorCode::DiskFull),
        other => panic!("expected a DISK_FULL handshake rejection, got {other:?}"),
    }
    let durable_before = j.durable_operation_seq;
    drop(s);
    let (_e, dir) = k.kill_keep();
    let k = restart(dir, &journal_limits("checkpoint_every_operations = 1000000\n", Some(alarms(256))), &[]);
    assert_eq!(ready_u64(&k.ready, "durable_op"), durable_before, "exactly the published prefix: {}", k.ready);
    k.stop();
}

/// T07: EIO on a commit is an unrecoverable fault: the operations of that transaction get
/// APPLIED_NOT_DURABLE, admission stops (JOURNAL_FAULT) until restart.
#[test]
fn eio_is_a_fault_until_restart() {
    let limits = journal_limits("", None);
    let k = KernelProc::start_with(&limits, None, &["--test-journal-eio-at-commit", "10"]);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 1, false, 0);
    let mut not_durable = None;
    for seq in 2..40 {
        let a = s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(seq as i64))])).unwrap();
        if a.status == AckStatus::AppliedNotDurable as i32 {
            not_durable = Some(seq);
            break;
        }
        assert_eq!(a.status, AckStatus::AppliedDurable as i32, "{a:?}");
    }
    let nd = not_durable.expect("the 10th commit fails");
    let a = s.request(&batch(BatchMode::Delta, nd + 1, &[(3, Val::I(1))])).unwrap();
    assert_eq!(a.status, AckStatus::Rejected as i32);
    assert_eq!(a.error_code, ErrorCode::JournalFault as i32);
    assert_eq!(hist(&k).journal().unwrap().state, hpb::JournalState::Fault as i32);
    drop(s);
    let (_e, dir) = k.kill_keep();
    // After restart the not-durable batch is not there; the driver resends it.
    let k = restart(dir, &limits, &[]);
    let r = k.introspect();
    let seq = r.sources.iter().find(|x| x.source_id == SRC).unwrap().last_accepted_source_sequence.unwrap();
    assert_eq!(seq, nd - 1, "the failed transaction is absent");
    k.stop();
}

// ----------------------------------------------------------- T08: ring released by commit

/// T08: with the journal the outbox slots are released by the commit (no consumer
/// needed): a storm of 20x the ring capacity is accepted; without the journal (VS-005)
/// the same storm is refused once the ring is full.
#[test]
fn journal_releases_the_outbox_ring() {
    let k = KernelProc::start(&journal_limits("", Some(alarms(64))));
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    let mut last_op = 0;
    for seq in 2..=1300u64 {
        // alarm 3 toggles each batch: one event per batch, 20x the ring.
        let v = if seq % 2 == 0 { 5000 } else { 0 };
        let a = s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(v))])).unwrap();
        if a.status == AckStatus::Rejected as i32 {
            // Transient: the ring is full until the next commit publishes the watermark.
            assert_eq!(a.error_code, ErrorCode::OutboxCapacityExceeded as i32, "{a:?}");
            // Nothing was applied: resend the same batch until a commit has released slots.
            let a = wait("ring released by a commit", || {
                std::thread::sleep(Duration::from_millis(1));
                let a = s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(v))])).unwrap();
                (a.status != AckStatus::Rejected as i32).then_some(a)
            });
            assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
            last_op = a.operation_sequence.unwrap();
        } else {
            last_op = a.operation_sequence.unwrap();
        }
    }
    // (The durable watermark advances are operations too: last_op > 1300.)
    let j = wait_durable_op(&k, last_op);
    assert!(j.durable_event_seq >= 1299, "one event per batch, 20x the ring: {j:?}");
    let mut h = hist(&k);
    assert_eq!(all_events(&mut h).len() as u64, j.durable_event_seq);
    k.stop();
}

// --------------------------------------------------------- T21: durable ACK + dedup

/// T21/T04: an operator ACK with an action_id and require_durable is answered after the
/// commit; a retry (same id) returns the same outcome without a new event, also after a
/// restart; the same id for another alarm is a conflict. The ACK keeps the episode
/// (ActiveAcked / Returned handling) across the restart.
#[test]
fn durable_operator_ack_is_idempotent_across_restart() {
    let limits = journal_limits("", Some(alarms(256)));
    let k = KernelProc::start(&limits);
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    delta_door(&mut s, 2, true);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let e = snapshot_entry(&mut c, 1);
    assert_eq!(e.state, apb::EpisodeState::ActiveUnacked as i32);
    let id = [7u8; 16];
    let a = ack_result(c.ack_action(1, e.activation_token, id, "checked on site", true).unwrap());
    assert_eq!(a.status, apb::AckStatus::AppliedDurable as i32, "{a:?}");
    assert!(!a.deduplicated);
    let ev = a.event_seq.unwrap();
    assert!(hist(&k).journal().unwrap().durable_event_seq >= ev);
    let again = ack_result(c.ack_action(1, e.activation_token, id, "checked on site", true).unwrap());
    assert!(again.deduplicated, "{again:?}");
    assert_eq!(again.event_seq, Some(ev));
    let conflict = ack_result(c.ack_action(2, e.activation_token, id, "", true).unwrap());
    assert_eq!(conflict.status, apb::AckStatus::ActionIdConflict as i32);
    // The note and the actor are in the history (actor = process uid, J11).
    let row = all_events(&mut hist(&k)).into_iter().find(|x| x.event_seq == ev).unwrap();
    assert_eq!(row.kind, hpb::EventKind::Acknowledged as i32);
    assert_eq!(row.note, "checked on site");
    assert_eq!(row.action_id, id.to_vec());
    assert_eq!(row.actor_uid, current_uid());
    drop((c, s));
    let (_e, dir) = k.kill_keep();
    let k = restart(dir, &limits, &[]);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    assert_eq!(snapshot_entry(&mut c, 1).state, apb::EpisodeState::ActiveAcked as i32);
    let after = ack_result(c.ack_action(1, e.activation_token, id, "checked on site", true).unwrap());
    assert!(after.deduplicated, "dedup survives the restart: {after:?}");
    assert_eq!(after.event_seq, Some(ev));
    // require_durable without action_id is refused (nothing to deduplicate on).
    let bad = ack_result(
        c.call(apb::alarm_request::Body::Acknowledge(apb::AcknowledgeAlarm {
            alarm_id: 1,
            activation_token: e.activation_token,
            require_durable: true,
            ..Default::default()
        }))
        .unwrap(),
    );
    assert_eq!(bad.status, apb::AckStatus::DurabilityUnavailable as i32);
    k.stop();
}

/// J12: require_durable without a journal is refused (never a false durable answer).
#[test]
fn require_durable_without_journal_is_refused() {
    let k = KernelProc::start(&Limits { persistence: None, alarms: Some(alarms(256)), ..Limits::default() });
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    delta_door(&mut s, 2, true);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let e = snapshot_entry(&mut c, 1);
    let a = ack_result(c.ack_action(1, e.activation_token, [1; 16], "", true).unwrap());
    assert_eq!(a.status, apb::AckStatus::DurabilityUnavailable as i32);
    assert_eq!(snapshot_entry(&mut c, 1).state, apb::EpisodeState::ActiveUnacked as i32, "nothing applied");
    k.stop();
}

// ---------------------------------------------------------------- T28: store security

/// T28: one writer per store (a second kernel is refused), private directory required.
#[test]
fn single_writer_and_private_store() {
    use std::os::unix::fs::PermissionsExt;
    let limits = journal_limits("", None);
    let k = KernelProc::start(&limits);
    let cfg = k.dir.path().join("kernel.toml");
    let text = std::fs::read_to_string(&cfg).unwrap();
    let other = k.dir.path().join("other.toml");
    std::fs::write(&other, text.replace("driver.sock", "driver2.sock").replace("history.sock", "history2.sock"))
        .unwrap();
    let out = std::process::Command::new(kernel_bin()).arg("--config").arg(&other).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("lock") || err.contains("Locked") || err.contains("another"), "{err}");
    let meta = std::fs::metadata(db_path(k.dir.path())).unwrap();
    assert_eq!(meta.permissions().mode() & 0o077, 0, "store file is private");
    let (_e, dir) = k.kill_keep();
    std::fs::set_permissions(dir.path().join("journal"), std::fs::Permissions::from_mode(0o777)).unwrap();
    let out = std::process::Command::new(kernel_bin()).arg("--config").arg(&cfg).output().unwrap();
    assert!(!out.status.success(), "a group/world-writable store directory is refused");
    // The historian refuses an unauthorized uid: covered by the allowlist (peer uid).
}

/// A different configuration (fingerprint) is refused fail-closed until an explicit new
/// timeline is created with the offline tool; the old history stays readable.
#[test]
fn configuration_change_needs_an_explicit_new_timeline() {
    let limits = journal_limits("", None);
    let k = KernelProc::start(&limits);
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, false, 0);
    drop(s);
    let old_timeline = k.ready.split(" timeline=").nth(1).unwrap().split(' ').next().unwrap().to_owned();
    let (_e, dir) = k.stop_keep();
    let cfg = dir.path().join("kernel.toml");
    let changed = std::fs::read_to_string(&cfg).unwrap().replace("name = \"aux_counter\"", "name = \"aux_counter_2\"");
    std::fs::write(&cfg, &changed).unwrap();
    let out = std::process::Command::new(kernel_bin()).arg("--config").arg(&cfg).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("new-timeline"), "{err}");
    // Fingerprint of the new configuration from --check-config.
    let chk =
        std::process::Command::new(kernel_bin()).arg("--config").arg(&cfg).arg("--check-config").output().unwrap();
    let text = String::from_utf8_lossy(&chk.stdout);
    let fp = text.split("fingerprint=").nth(1).unwrap().split(' ').next().unwrap().to_owned();
    let tool = kernel_bin().with_file_name("scada-journal");
    assert!(tool.exists(), "{} missing: build it (`cargo build -p scada-journal`)", tool.display());
    let out = std::process::Command::new(&tool)
        .arg("new-timeline")
        .arg(db_path(dir.path()))
        .args(["--fingerprint", &fp, "--reason", "test config change", "--confirm"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    // The kernel test helper rewrites the configuration: start the changed one by hand.
    std::fs::write(dir.path().join("changed.toml"), &changed).unwrap();
    let mut child = std::process::Command::new(kernel_bin())
        .arg("--config")
        .arg(dir.path().join("changed.toml"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    use std::io::BufRead;
    std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
    assert!(line.starts_with("READY"), "{line}");
    assert!(!line.contains(&format!("timeline={old_timeline}")), "a new timeline: {line}");
    assert!(line.contains("durable_op=0"), "nothing recovered on the new timeline: {line}");
    unsafe_free_sigterm(child.id());
    assert!(child.wait().unwrap().success());
    // The old history stays readable offline.
    let info = std::process::Command::new(&tool).arg("info").arg(db_path(dir.path())).output().unwrap();
    assert!(
        String::from_utf8_lossy(&info.stdout).contains("timeline_no=2"),
        "{}",
        String::from_utf8_lossy(&info.stdout)
    );
    let c =
        rusqlite::Connection::open_with_flags(db_path(dir.path()), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let old_rows: i64 =
        c.query_row("SELECT count(*) FROM journal_operations WHERE timeline_no = 1", [], |r| r.get(0)).unwrap();
    assert!(old_rows >= 2, "the old timeline's operations are kept");
    let old_id: Vec<u8> =
        c.query_row("SELECT timeline_id FROM timelines WHERE timeline_no = 1", [], |r| r.get(0)).unwrap();
    assert_eq!(scada_journal::hex(&old_id), old_timeline);
}

// ------------------------------------------------------------- T15–T19: time authority

fn set_offset(path: &Path, ns: i64) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, ns.to_string()).unwrap();
    std::fs::rename(&tmp, path).unwrap();
    // The authority re-reads the file at most every 5 ms.
    std::thread::sleep(Duration::from_millis(20));
}

/// T15/J7: steps of the host clock of −1 s, −1 h and +24 h (injected offset, ADR 0014 §9):
/// sequencing goes on, the raw `sequenced_at` is kept (it regresses), the regression is
/// flagged and counted, the logical time never decreases, the order stays the event
/// sequence.
#[test]
fn clock_steps_are_diagnosed_not_causal() {
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let off = dir.path().join("clock-offset");
    std::fs::write(&off, "0").unwrap();
    let off_s = off.display().to_string();
    let limits =
        journal_limits("\n[time]\nclock_regression_alert_ms = 500\nclock_jump_alert_ms = 60000\n", Some(alarms(1024)));
    let k = KernelProc::start_in(dir, &limits, None, &["--test-clock-offset-file", &off_s]);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 1, false, 0);
    let mut seq = 2;
    let mut toggle = |s: &mut Session, n: u64| {
        for _ in 0..n {
            let v = if seq % 2 == 0 { 5000 } else { 0 };
            assert_eq!(
                s.request(&batch(BatchMode::Delta, seq, &[(3, Val::I(v))])).unwrap().status,
                AckStatus::AppliedDurable as i32
            );
            seq += 1;
        }
    };
    toggle(&mut s, 4);
    let mut h = hist(&k);
    let t0 = h.time().unwrap();
    assert_eq!(t0.clock_regressions, 0);
    set_offset(&off, -1_000_000_000);
    toggle(&mut s, 4);
    let t1 = h.time().unwrap();
    assert_eq!(t1.clock_regressions, 1, "{t1:?}");
    set_offset(&off, -3_600_000_000_000);
    toggle(&mut s, 4);
    let t2 = h.time().unwrap();
    assert_eq!(t2.clock_regressions, 2, "{t2:?}");
    assert!(t2.last_anomaly_delta_ns <= -3_598_000_000_000, "{t2:?}");
    set_offset(&off, 86_400_000_000_000 - 3_600_000_000_000);
    toggle(&mut s, 4);
    let t3 = h.time().unwrap();
    assert_eq!(t3.clock_jumps_forward, 1, "{t3:?}");
    let ev = all_events(&mut h);
    assert!(ev.len() >= 16);
    let mut prev_logical = i64::MIN;
    let mut regressed = 0;
    for w in ev.windows(2) {
        assert!(w[1].event_seq > w[0].event_seq);
        if w[1].sequenced_at_utc_ns < w[0].sequenced_at_utc_ns {
            regressed += 1;
            // REGRESSED is per operation, against the previous *operation*: when operations
            // without alarm events (watermark advances, deltas that change no alarm) sit
            // between the two events, the flagged operation may be one of those.
            assert!(w[1].clock_regressed || w[1].operation_seq > w[0].operation_seq + 1, "{:?} after {:?}", w[1], w[0]);
        }
    }
    for e in &ev {
        assert!(e.logical_time_ns >= prev_logical, "logical time never decreases");
        prev_logical = e.logical_time_ns;
        assert!(e.logical_time_ns >= e.sequenced_at_utc_ns);
    }
    assert_eq!(regressed, 2, "the raw time is stored as observed (two regressions)");
    k.stop();
}

/// T17: the host clock is behind the last persisted logical time at the next boot (RTC
/// moved back / monotonic reset): a cross-boot regression is detected and the logical
/// time continues from the persisted value.
#[test]
fn cross_boot_regression_is_detected() {
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let off = dir.path().join("clock-offset");
    std::fs::write(&off, "0").unwrap();
    let off_s = off.display().to_string();
    let limits = journal_limits("", None);
    let k = KernelProc::start_in(dir, &limits, None, &["--test-clock-offset-file", &off_s]);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 1, false, 0);
    let logical_before = hist(&k).time().unwrap().last_logical_time_ns;
    drop(s);
    let (_e, dir) = k.stop_keep();
    std::fs::write(&off, (-3_600_000_000_000i64).to_string()).unwrap();
    let k = KernelProc::start_in(dir, &limits, None, &["--test-clock-offset-file", &off_s]);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 2, false, 0);
    let t = hist(&k).time().unwrap();
    assert!(t.clock_regressions >= 1, "{t:?}");
    assert!(t.last_logical_time_ns >= logical_before, "{t:?} < {logical_before}");
    assert!(t.last_sequenced_at_utc_ns < logical_before, "raw time kept: {t:?}");
    assert_eq!(t.boot_generation, 2);
    k.stop();
}

/// T16: the store is UTC only; the process time zone (and therefore DST) changes nothing.
#[test]
fn time_zone_does_not_change_stored_times() {
    let limits = journal_limits("", Some(alarms(256)));
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let cfg = dir.path().join("kernel.toml");
    std::fs::write(&cfg, demo_config(dir.path(), &limits, None)).unwrap();
    let mut child = std::process::Command::new(kernel_bin())
        .arg("--config")
        .arg(&cfg)
        .env("TZ", "Europe/Rome")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    use std::io::BufRead;
    std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
    assert!(line.starts_with("READY"), "{line}");
    let mut s = Session::connect_with(&dir.path().join("driver.sock"), &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 1, true, 0);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
    let mut h = HistoryClient::connect(&dir.path().join("history.sock")).unwrap();
    let ev = all_events(&mut h);
    assert!(!ev.is_empty());
    for e in ev {
        assert!((now - e.sequenced_at_utc_ns).abs() < 10_000_000_000, "UTC, not local: {e:?}");
    }
    unsafe_free_sigterm(child.id());
    assert!(child.wait().unwrap().success());
}

/// T19: supervision deadlines use CLOCK_MONOTONIC/BOOTTIME only, so a step of the wall
/// clock (NTP) cannot expire or extend them. Verified on the source (no wall-clock read in
/// the supervisor or the watchdog) and at run time with an injected +24 h / −1 h step.
#[test]
fn watchdog_is_independent_of_the_wall_clock() {
    for (name, src) in
        [("supervision.rs", include_str!("../src/supervision.rs")), ("ingress.rs", include_str!("../src/ingress.rs"))]
    {
        assert!(!src.contains("SystemTime"), "{name} must not read the wall clock");
        assert!(!src.contains("realtime_ns"), "{name} must not read the wall clock");
    }
    let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
    let off = dir.path().join("clock-offset");
    std::fs::write(&off, "0").unwrap();
    let off_s = off.display().to_string();
    let limits = Limits {
        persistence: Some(String::new()),
        runtime: "watchdog_poll_ms = 20\n".into(),
        source_1001: "liveness_timeout_ms = 400\n".into(),
        ..Limits::default()
    };
    let k = KernelProc::start_in(dir, &limits, None, &["--test-clock-offset-file", &off_s]);
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 1, false, 0);
    let t = Instant::now();
    let mut step = 0;
    while t.elapsed() < Duration::from_millis(1500) {
        assert!(s.heartbeat().unwrap());
        std::thread::sleep(Duration::from_millis(50));
        if t.elapsed() > Duration::from_millis(400) && step == 0 {
            set_offset(&off, 86_400_000_000_000);
            step = 1;
        }
        if t.elapsed() > Duration::from_millis(900) && step == 1 {
            set_offset(&off, -3_600_000_000_000);
            step = 2;
        }
    }
    let r = k.introspect();
    assert_eq!(r.counters.unwrap().liveness_expiries, 0, "a wall-clock step never expires a deadline");
    assert_eq!(tag_q(&r, 101), Quality::Good);
    k.stop();
}

// ----------------------------------------------------------- T18: source timestamps

/// T18: device and driver timestamps of a 1.2 link are journaled unchanged next to the
/// sequencer time; a 1.1 link (or a batch without them) has none (NULL, never invented);
/// invalid ones reject the batch without effect.
#[test]
fn source_timestamps_are_kept_unchanged() {
    use scada_sim_driver::batch_with_times;
    let k = KernelProc::start(&journal_limits("", Some(alarms(256))));
    let mut s = Session::connect_with(&k.socket, &hello_durable("d", SRC, 1)).unwrap();
    full(&mut s, 1, false, 0);
    let dev_ts = 1_700_000_000_123_456_789i64; // earlier than now: late data is legal
    let drv_ts = 1_700_000_001_000_000_000i64;
    let a = s
        .request(&batch_with_times(
            BatchMode::Delta,
            2,
            &[(1, Val::B(true), Some(dev_ts)), (2, Val::B(false), None)],
            Some(drv_ts),
            scada_protocol::driver_v1::ClockQuality::Synchronized,
        ))
        .unwrap();
    assert_eq!(a.status, AckStatus::AppliedDurable as i32, "{a:?}");
    // Temperature without a device time: the driver time applies.
    let a = s
        .request(&batch_with_times(
            BatchMode::Delta,
            3,
            &[(3, Val::I(5000), None)],
            Some(drv_ts),
            scada_protocol::driver_v1::ClockQuality::Holdover,
        ))
        .unwrap();
    assert_eq!(a.status, AckStatus::AppliedDurable as i32);
    let mut h = hist(&k);
    let ev = all_events(&mut h);
    let door = ev.iter().find(|e| e.alarm_id == 1 && e.kind == hpb::EventKind::Raised as i32).unwrap();
    assert_eq!(door.origin_utc_ns, Some(dev_ts));
    assert_eq!(door.origin_source, hpb::TimeSource::Device as i32);
    assert_eq!(door.origin_clock, hpb::ClockState::Synchronized as i32);
    assert!(door.sequenced_at_utc_ns > dev_ts, "the sequencer time is separate");
    let high = ev.iter().find(|e| e.alarm_id == 3 && e.kind == hpb::EventKind::Raised as i32).unwrap();
    assert_eq!(high.origin_utc_ns, Some(drv_ts));
    assert_eq!(high.origin_source, hpb::TimeSource::Driver as i32);
    assert_eq!(high.origin_clock, hpb::ClockState::Holdover as i32);
    // Invalid: negative timestamp, or a length mismatch: rejected, nothing applied.
    let bad = s
        .request(&batch_with_times(
            BatchMode::Delta,
            4,
            &[(1, Val::B(false), Some(-5))],
            None,
            scada_protocol::driver_v1::ClockQuality::Unknown,
        ))
        .unwrap();
    assert_eq!(bad.status, AckStatus::Rejected as i32);
    assert_eq!(bad.error_code, ErrorCode::InvalidTimestamp as i32);
    let mut m = batch(BatchMode::Delta, 4, &[(1, Val::B(false)), (2, Val::B(false))]);
    if let Some(scada_protocol::driver_v1::driver_message::Body::Batch(b)) = &mut m.body {
        b.source_timestamps_utc_ns = vec![dev_ts];
    }
    let bad = s.request(&m).unwrap();
    assert_eq!(bad.error_code, ErrorCode::InvalidTimestamp as i32);
    assert_eq!(tag(&k.introspect(), 101).value, Some(scada_protocol::introspect_v1::tag_state::Value::BoolValue(true)));
    drop(s);
    // A 1.1 driver sending the same fields: ignored (unknown to 1.1), no origin time.
    let mut s = Session::connect_with(&k.socket, &hello_minor("old", SRC, 2, 1)).unwrap();
    full(&mut s, 1, false, 0);
    let a = s
        .request(&batch_with_times(
            BatchMode::Delta,
            2,
            &[(1, Val::B(true), Some(dev_ts))],
            Some(drv_ts),
            scada_protocol::driver_v1::ClockQuality::Synchronized,
        ))
        .unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32);
    wait_durable_op(&k, a.operation_sequence.unwrap());
    let ev = all_events(&mut h);
    // (Still unacknowledged: the door opening again is a Retriggered event.)
    let last_raise = ev.iter().find(|e| e.alarm_id == 1 && e.operation_seq == a.operation_sequence.unwrap()).unwrap();
    assert_eq!(last_raise.origin_utc_ns, None, "never invented: {last_raise:?}");
    assert_eq!(last_raise.origin_source, hpb::TimeSource::Unspecified as i32);
    k.stop();
}

// ------------------------------------------------------------- T27: no command on replay

/// T27/J6: recovery replays the journal into the kernel only: no driver is contacted, no
/// event is created, and the operation sequence continues after the recovered one.
#[test]
fn replay_emits_nothing() {
    let limits = journal_limits("checkpoint_every_operations = 20\n", Some(alarms(256)));
    let k = KernelProc::start(&limits);
    let cut = workload_until_crash(&k, 5, 90);
    assert_eq!(cut.sent_seq, 90);
    // The disconnection (and its bad-quality alarms) must be durable before the snapshot.
    std::thread::sleep(Duration::from_millis(100));
    let applied = k.introspect().last_operation_sequence;
    wait_durable_op(&k, applied);
    let events_before = all_events(&mut hist(&k));
    let (_e, dir) = k.kill_keep();
    let k = restart(dir, &limits, &[]);
    assert!(ready_u64(&k.ready, "replayed_ops") > 0, "{}", k.ready);
    std::thread::sleep(Duration::from_millis(200));
    let mut h = hist(&k);
    assert_eq!(all_events(&mut h), events_before, "the replay produced no event");
    let r = k.introspect();
    assert_eq!(r.counters.unwrap().batches_applied, 0, "no batch applied in this process");
    k.stop();
}

// ------------------------------------------------- T24: kill -9 during a storm, repeated

/// T24 (crash-recovery part; the 30-60 min soak is `results/vs006/soak.txt`): five cycles
/// of a durable-receipt storm (alarm 3 toggles every 4 batches) killed with SIGKILL at an
/// arbitrary moment. After every kill: `scada-journal verify` passes on the cold store; on
/// restart every durably acknowledged operation is there, the event sequence is gap-free
/// and ends at the durable watermark, and the active projection of alarm 3 is the state of
/// its last historical event (RTDB <-> alarms <-> historian coherent). The storm then
/// resumes from the recovered source sequence.
#[test]
fn kill9_during_a_storm_recovers_coherently_every_time() {
    let limits = journal_limits("checkpoint_every_operations = 50\n", Some(alarms(65536)));
    let tool = kernel_bin().with_file_name("scada-journal");
    let mut k = KernelProc::start_with(&limits, None, &[]);
    let mut recovered_seq = 0u64;
    for cycle in 0..5u64 {
        let pid = k.pid();
        let delay = Duration::from_millis(150 + cycle * 97);
        let killer = std::thread::spawn(move || {
            std::thread::sleep(delay);
            let _ = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status();
        });
        let mut last_durable_op = 0u64;
        let mut sent = 0u64;
        if let Ok(mut s) = Session::connect_with(&k.socket, &hello_durable("d", SRC, 77)) {
            let mut seq = recovered_seq + 1;
            loop {
                let temp = if (seq / 4).is_multiple_of(2) { seq as i64 } else { 5000 + seq as i64 };
                let msg = if seq == recovered_seq + 1 {
                    batch(BatchMode::FullState, seq, &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(temp))])
                } else {
                    batch(BatchMode::Delta, seq, &[(3, Val::I(temp))])
                };
                match s.request(&msg) {
                    Ok(a) if a.status == AckStatus::AppliedDurable as i32 => {
                        last_durable_op = a.operation_sequence.unwrap();
                        sent = seq; // the last durably acknowledged batch
                    }
                    Ok(a) => panic!("cycle {cycle}: unexpected ACK {a:?}"),
                    Err(_) => break,
                }
                seq += 1;
            }
        }
        killer.join().unwrap();
        let (_status, _err, dir) = k.wait_exit(Duration::from_secs(10));
        assert!(sent > recovered_seq, "cycle {cycle}: the storm made no progress before the kill");
        let v = std::process::Command::new(&tool).args(["verify"]).arg(db_path(dir.path())).output().unwrap();
        assert!(v.status.success(), "cycle {cycle}: verify failed: {}", String::from_utf8_lossy(&v.stderr));
        assert!(String::from_utf8_lossy(&v.stdout).contains("integrity_check: ok"));

        k = restart(dir, &limits, &[]);
        let durable_op = ready_u64(&k.ready, "durable_op");
        assert!(
            durable_op >= last_durable_op,
            "cycle {cycle}: J1 lost a durable ACK ({durable_op} < {last_durable_op})"
        );
        let r = k.introspect();
        let src = r.sources.iter().find(|x| x.source_id == SRC).unwrap();
        recovered_seq = src.last_accepted_source_sequence.unwrap_or(0);
        assert!(recovered_seq >= sent, "cycle {cycle}: durably acknowledged batch {sent} missing ({recovered_seq})");
        let mut h = hist(&k);
        let ev = all_events(&mut h);
        let dj = h.journal().unwrap();
        assert_eq!(ev.len() as u64, dj.durable_event_seq, "cycle {cycle}: historian != durable watermark");
        for (i, e) in ev.iter().enumerate() {
            assert_eq!(e.event_seq, i as u64 + 1, "cycle {cycle}: hole in the event sequence");
        }
        if let Some(last) = ev.iter().rev().find(|e| e.alarm_id == 3) {
            let mut c = AlarmClient::connect(&k.alarm).unwrap();
            let entry = snapshot_entry(&mut c, 3);
            assert_eq!(entry.state, last.state_after, "cycle {cycle}: projection != history");
            assert_eq!(entry.occurrence_id, last.occurrence_id, "cycle {cycle}");
            assert_eq!(entry.last_event_seq, last.event_seq, "cycle {cycle}");
        }
    }
    k.stop();
}
