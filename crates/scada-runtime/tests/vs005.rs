//! VS-005 gates (brief §10, ADR 0012) with real kernel and driver processes: default-off,
//! alarm episodes and ACK over the control socket, control-plane security (T09),
//! outbox exhaustion and recovery (T05/T07), deferred disconnections, supervision
//! coupling (T10), restart semantics (T14) and capture/replay with alarms.
//!
//! Exact semantics (purity, total order, atomicity, no overwrite) are proven in
//! `scada-core/tests/alarms.rs` (model-based). Here: the integration. The suite runs under
//! every scheduler and ingress policy of `scripts/check.sh`.

mod support;

use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scada_core::Kernel;
use scada_protocol::alarm_v1 as apb;
use scada_protocol::driver_v1::{AckStatus, BatchMode, ErrorCode};
use scada_protocol::introspect_v1::{IntrospectResponse, LinkState, Liveness, Quality};
use scada_sim_driver::{AlarmClient, Session, Val, batch};
use support::*;

const SRC: u32 = 1001;

/// VS-005 contract (volatile outbox, simulated sink, `durability=none`): the VS-006 journal
/// stays off here; alarms with the journal are tested in vs006.rs.
fn with_alarms(a: Alarms) -> Limits {
    Limits { alarms: Some(a), persistence: None, ..Limits::default() }
}

fn door(s: &mut Session, seq: u64, v: bool) -> scada_protocol::driver_v1::InputAck {
    s.request(&batch(BatchMode::Delta, seq, &[(1, Val::B(v))])).unwrap()
}

fn full(s: &mut Session, seq: u64, door: bool) {
    let a = s
        .request(&batch(BatchMode::FullState, seq, &[(1, Val::B(door)), (2, Val::B(false)), (3, Val::I(2150))]))
        .unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
}

fn err_code(r: &apb::AlarmResponse) -> apb::ErrorCode {
    match &r.body {
        Some(apb::alarm_response::Body::Error(e)) => e.code(),
        other => panic!("expected an error, got {other:?}"),
    }
}

fn page(r: apb::AlarmResponse) -> apb::AlarmSnapshotPage {
    match r.body {
        Some(apb::alarm_response::Body::Snapshot(p)) => p,
        other => panic!("expected a snapshot page, got {other:?}"),
    }
}

fn events(r: apb::AlarmResponse) -> apb::EventPage {
    match r.body {
        Some(apb::alarm_response::Body::Events(p)) => p,
        other => panic!("expected an event page, got {other:?}"),
    }
}

fn ack(r: apb::AlarmResponse) -> apb::AckResult {
    match r.body {
        Some(apb::alarm_response::Body::Ack(a)) => a,
        other => panic!("expected an ACK result, got {other:?}"),
    }
}

fn entry(c: &mut AlarmClient, id: u32) -> Option<apb::AlarmEntry> {
    page(c.snapshot(0, 0, 0).unwrap()).alarms.into_iter().find(|a| a.alarm_id == id)
}

fn link(r: &IntrospectResponse, id: u32) -> LinkState {
    LinkState::try_from(r.sources.iter().find(|s| s.source_id == id).unwrap().link_state).unwrap()
}

fn wait<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(start.elapsed() < Duration::from_secs(10), "timeout waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Two bool rules on the door/tamper tags of source 1001 (worst fan-out 2).
fn two_bool_rules() -> String {
    let mut r = rule(1, 101, "bool_equals", "expected = true\n", "major");
    r.push_str(&rule(2, 102, "bool_equals", "expected = true\n", "critical"));
    r
}

/// T01 (runtime part): without `[alarms]` nothing of VS-005 exists: no control socket, no
/// introspection summary, READY line and digest versions of VS-004, protocol 1.1 unchanged.
#[test]
fn default_off_has_no_alarm_surface() {
    let k = KernelProc::start(&Limits {
        alarms: None,
        runtime: String::new(),
        source_1001: String::new(),
        persistence: None,
        ..Limits::default()
    });
    assert!(!k.ready.contains("alarms="), "{}", k.ready);
    assert!(!k.alarm.exists(), "no control socket without [alarms]");
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    assert_eq!(
        (s.welcome.negotiated_protocol_major, s.welcome.negotiated_protocol_minor),
        (1, scada_protocol::PROTOCOL_MINOR),
        "VS-006 driver library: 1.2"
    );
    full(&mut s, 1, true);
    let r = k.introspect();
    assert_eq!(r.state_digest_version, 1, "VS-003/VS-004 digest v1 unchanged");
    assert!(r.alarms.is_none());
    k.stop();
    // --check-config says so explicitly.
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("k.toml");
    std::fs::write(&cfg, demo_config(dir.path(), &Limits { alarms: None, ..Limits::default() }, None)).unwrap();
    let out = Command::new(kernel_bin()).arg("--config").arg(&cfg).arg("--check-config").output().unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("alarms: disabled"));
}

/// `--check-config` with alarms: capacity report, no durability claim.
#[test]
fn check_config_reports_alarm_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("k.toml");
    std::fs::write(&cfg, demo_config(dir.path(), &with_alarms(Alarms::with(4096, demo_rules())), None)).unwrap();
    let out = Command::new(kernel_bin()).arg("--config").arg(&cfg).arg("--check-config").output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{text}");
    assert!(text.contains("alarms: rules=22 "), "{text}");
    assert!(text.contains("outbox_capacity_events=4096"), "{text}");
    assert!(text.contains("durability=NONE"), "{text}");
    assert!(text.contains("MiB application allocations (not RSS)"), "{text}");
    // Outbox smaller than the worst fan-out of one operation: refused at startup.
    std::fs::write(&cfg, demo_config(dir.path(), &with_alarms(Alarms::with(3, demo_rules())), None)).unwrap();
    let out = Command::new(kernel_bin()).arg("--config").arg(&cfg).arg("--check-config").output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot hold"), "{}", String::from_utf8_lossy(&out.stderr));
}

/// T02/T03/T06 end to end: raise, ACK (stale token refused, duplicate ignored, actor =
/// peer uid), return; events contiguous; the captured stream replays to the live digest.
#[test]
fn episode_and_ack_over_ipc_and_replay() {
    let dir = tempfile::Builder::new().prefix("scada-a").tempdir().unwrap();
    let cap = dir.path().join("inputs.txt");
    let cap_s = cap.display().to_string();
    let k =
        KernelProc::start_in(dir, &with_alarms(Alarms::with(1024, demo_rules())), None, &["--capture-inputs", &cap_s]);
    assert!(k.ready.contains(" alarms=22 outbox=1024 durability=none"), "{}", k.ready);
    let mode = std::fs::metadata(&k.alarm).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o660);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let cap0 = c.capacity().unwrap();
    assert_eq!(cap0.durability(), apb::Durability::NoneVolatile);
    assert_eq!((cap0.used, cap0.free, cap0.next_event_seq), (0, 1024, 1));
    assert_eq!(c.instance, hex_to_bytes(&k.instance));

    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, true);
    let a1 = entry(&mut c, 1).unwrap();
    assert_eq!(a1.state(), apb::EpisodeState::ActiveUnacked);
    assert_eq!(a1.occurrence_id, 1);
    let token = a1.activation_token;
    assert!(token > 0);
    // Stale token: refused, nothing changes.
    let r = ack(c.ack(1, token + 1).unwrap());
    assert_eq!(r.status(), apb::AckStatus::StaleOccurrence);
    assert_eq!(r.current_activation_token, token);
    // ACK without the instance id: refused (a token is meaningful only for its instance).
    let mut anon = AlarmClient::connect(&k.alarm).unwrap();
    let r = anon
        .call_raw(&apb::AlarmRequest {
            alarm_api_version: 1,
            kernel_instance_id: Vec::new(),
            body: Some(apb::alarm_request::Body::Acknowledge(apb::AcknowledgeAlarm {
                alarm_id: 1,
                activation_token: token,
                ..Default::default()
            })),
        })
        .unwrap();
    assert_eq!(err_code(&r), apb::ErrorCode::InvalidRequest);
    let r = ack(c.ack(1, token).unwrap());
    assert_eq!(r.status(), apb::AckStatus::AppliedVolatile, "{r:?}");
    let ack_seq = r.event_seq.unwrap();
    assert_eq!(ack(c.ack(1, token).unwrap()).status(), apb::AckStatus::AlreadyAcked);
    assert_eq!(ack(c.ack(999, 1).unwrap()).status(), apb::AckStatus::UnknownAlarm);
    assert_eq!(ack(c.ack(2, 1).unwrap()).status(), apb::AckStatus::NotAcknowledgeable);
    assert_eq!(entry(&mut c, 1).unwrap().state(), apb::EpisodeState::ActiveAcked);
    // Return: acknowledged episode closes (Normal, no longer listed).
    assert_eq!(door(&mut s, 2, false).status, AckStatus::AppliedVolatile as i32);
    assert!(entry(&mut c, 1).is_none());
    let p = events(c.events(0, 0).unwrap());
    let kinds: Vec<_> = p.events.iter().map(|e| (e.alarm_id, e.kind())).collect();
    assert_eq!(kinds, [(1, apb::EventKind::Raised), (1, apb::EventKind::Acknowledged), (1, apb::EventKind::Returned)]);
    for (i, e) in p.events.iter().enumerate() {
        assert_eq!(e.event_seq, i as u64 + 1, "contiguous event sequence");
    }
    let ackev = p.events.iter().find(|e| e.event_seq == ack_seq).unwrap();
    assert_eq!(ackev.actor_uid, current_uid(), "actor = authenticated peer uid");
    assert!(!p.gap_detected && !p.has_more);
    assert_eq!(p.durable_watermark, 0, "nothing is durable in VS-005");
    // Cursor ahead of the newest event is reported, not silently empty.
    assert!(events(c.events(1_000, 0).unwrap()).cursor_ahead);
    let r = k.wait_for("summary", |r| r.alarms.as_ref().is_some_and(|a| a.acknowledgements_applied == 1));
    let sum = r.alarms.clone().unwrap();
    assert_eq!((sum.rules, sum.outbox_used, sum.next_event_seq, sum.outbox_capacity), (22, 3, 4, 1024));
    assert_eq!(r.state_digest_version, 3);
    // Replay of the captured inputs (batches + ACK) reproduces the live digests.
    drop(s);
    let live = k.wait_for("disconnect applied", |r| link(r, SRC) == LinkState::Disconnected);
    let cfg = scada_runtime::config::load(&k.dir.path().join("kernel.toml")).unwrap();
    let mut rk = Kernel::new(Arc::new(cfg.core.clone()));
    for i in scada_runtime::capture::read(&cap).unwrap() {
        let _ = rk.apply(i);
    }
    assert_eq!(live.state_digest, rk.state_digest().to_vec(), "replay != live");
    assert_eq!(live.alarms.unwrap().events_digest, rk.alarm_events_digest().unwrap().to_vec());
    k.stop();
}

fn hex_to_bytes(h: &str) -> Vec<u8> {
    (0..h.len()).step_by(2).map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap()).collect()
}

/// Brief T09: control plane security and robustness.
#[test]
fn control_plane_security() {
    // 1. Peer uid not in the allowlist: UNAUTHORIZED, connection closed, nothing applied.
    let k = KernelProc::start(&with_alarms(Alarms {
        authorized_uids: Some(vec![current_uid().wrapping_add(4242)]),
        ..Alarms::with(64, two_bool_rules())
    }));
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let r = c.read().unwrap();
    assert_eq!(err_code(&r), apb::ErrorCode::Unauthorized);
    assert!(c.read().is_err(), "closed after UNAUTHORIZED");
    k.stop();

    let k = KernelProc::start(&with_alarms(Alarms {
        extra: "max_control_connections = 2\ncontrol_page_max = 1\n".into(),
        ..Alarms::with(64, two_bool_rules())
    }));
    // 2. Oversized frame: FRAME_TOO_LARGE, closed.
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    c.send_raw(&(1u32 << 20).to_be_bytes()).unwrap();
    assert_eq!(err_code(&c.read().unwrap()), apb::ErrorCode::FrameTooLarge);
    // 3. Malformed protobuf: INVALID_REQUEST, closed.
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    c.send_raw(&[0, 0, 0, 3, 0xff, 0xff, 0xff]).unwrap();
    assert_eq!(err_code(&c.read().unwrap()), apb::ErrorCode::InvalidRequest);
    // 4. Wrong API version.
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let r = c.call_raw(&apb::AlarmRequest { alarm_api_version: 2, ..Default::default() }).unwrap();
    assert_eq!(err_code(&r), apb::ErrorCode::InvalidRequest);
    // 5. Wrong instance: INSTANCE_MISMATCH, the session stays usable.
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    c.instance = vec![7; 16];
    assert_eq!(err_code(&c.ack(1, 1).unwrap()), apb::ErrorCode::InstanceMismatch);
    c.instance = Vec::new();
    assert!(c.capacity().is_ok());
    // 6. Simulated durable commit without the test flag: refused.
    assert_eq!(err_code(&c.simulated_commit(1).unwrap()), apb::ErrorCode::Unsupported);
    // 7. Page limit enforced (control_page_max = 1).
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    assert_eq!(
        s.request(&batch(BatchMode::FullState, 1, &[(1, Val::B(true)), (2, Val::B(true)), (3, Val::I(1))]))
            .unwrap()
            .status,
        AckStatus::AppliedVolatile as i32
    );
    let p = page(c.snapshot(0, 100, 0).unwrap());
    assert_eq!((p.alarms.len(), p.has_more, p.total_listed), (1, true, 2));
    // A page changed under the reader is reported, not mixed.
    let change = p.change_operation;
    assert_eq!(door(&mut s, 2, false).status, AckStatus::AppliedVolatile as i32);
    assert_eq!(err_code(&c.snapshot(p.next_after_alarm_id, 1, change).unwrap()), apb::ErrorCode::SnapshotChanged);
    let e = events(c.events(0, 100).unwrap());
    assert_eq!((e.events.len(), e.has_more), (1, true));
    // 8. Connection limit (2: `c` + one more; the third is refused).
    // The sessions dropped above release their permit asynchronously (when the server sees
    // EOF): retry until `c2` is really served, so it provably holds the second permit.
    let t0 = std::time::Instant::now();
    let _c2 = loop {
        let mut c2 = AlarmClient::connect(&k.alarm).unwrap();
        if c2.capacity().is_ok() {
            break c2;
        }
        assert!(t0.elapsed() < Duration::from_secs(10), "dropped control sessions never released");
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut c3 = AlarmClient::connect(&k.alarm).unwrap();
    assert_eq!(err_code(&c3.read().unwrap()), apb::ErrorCode::TooManyConnections);
    // The driver socket is unaffected by any of this.
    assert_eq!(door(&mut s, 3, true).status, AckStatus::AppliedVolatile as i32);
    k.stop();
}

/// T05/T07: an outbox without free slots refuses the WHOLE batch (RTDB unchanged, ACK
/// rejected with OUTBOX_CAPACITY_EXCEEDED, CRITICAL); releasing slots (test-only sink)
/// lets the same batch through. Never an overwrite.
#[test]
fn full_outbox_refuses_whole_batch_then_recovers() {
    let k = KernelProc::start_with(
        &with_alarms(Alarms::with(2, two_bool_rules())),
        None,
        &["--test-simulated-durable-sink"],
    );
    assert!(k.ready.contains("simulated_sink=test_only"), "{}", k.ready);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, true); // Raised (1)
    assert_eq!(door(&mut s, 2, false).status, AckStatus::AppliedVolatile as i32); // Returned (2)
    let st = c.capacity().unwrap();
    assert_eq!((st.used, st.free), (2, 0));
    assert_eq!(st.overload(), apb::OverloadState::Critical);
    // Full: a batch that would raise is refused as a whole (tag 103 not written either).
    let a = s.request(&batch(BatchMode::Delta, 3, &[(1, Val::B(true)), (3, Val::I(4242))])).unwrap();
    assert_eq!(a.status, AckStatus::Rejected as i32, "{a:?}");
    assert_eq!(a.error_code(), ErrorCode::OutboxCapacityExceeded);
    let r = k.introspect();
    assert_eq!(quality(&tag(&r, 103)), Quality::Good);
    assert_ne!(tag(&r, 103).value, Some(scada_protocol::introspect_v1::tag_state::Value::I64Value(4242)));
    // A batch that raises nothing still passes (only transitions need slots).
    assert_eq!(
        s.request(&batch(BatchMode::Delta, 3, &[(3, Val::I(7))])).unwrap().status,
        AckStatus::AppliedVolatile as i32
    );
    let st = c.capacity().unwrap();
    assert_eq!(st.rejected_batches, 1);
    // An ACK needs a slot too: refused with required/free, the episode unchanged.
    let a1 = entry(&mut c, 1).unwrap();
    assert_eq!(a1.state(), apb::EpisodeState::ReturnedUnacked);
    let r = ack(c.ack(1, a1.activation_token).unwrap());
    assert_eq!(r.status(), apb::AckStatus::OutboxCapacityExceeded);
    assert_eq!((r.required, r.free), (1, 0));
    assert_eq!(entry(&mut c, 1).unwrap().state(), apb::EpisodeState::ReturnedUnacked);
    assert_eq!(c.capacity().unwrap().rejected_acknowledgements, 1);
    // The oldest events are still readable: nothing was overwritten.
    let e = events(c.events(0, 0).unwrap());
    assert_eq!(e.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(), [1, 2]);
    // Release (TEST ONLY): watermark 2 frees both slots.
    let w = match c.simulated_commit(2).unwrap().body {
        Some(apb::alarm_response::Body::Watermark(w)) => w,
        other => panic!("{other:?}"),
    };
    assert!(w.applied);
    assert_eq!((w.durable_watermark, w.released), (2, 2));
    // Regression and beyond-committed are refused.
    assert_eq!(err_code(&c.simulated_commit(1).unwrap()), apb::ErrorCode::WatermarkRefused);
    assert_eq!(err_code(&c.simulated_commit(99).unwrap()), apb::ErrorCode::WatermarkRefused);
    let a = s.request(&batch(BatchMode::Delta, 4, &[(1, Val::B(true)), (3, Val::I(4242))])).unwrap();
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
    let st = c.capacity().unwrap();
    assert_eq!((st.used, st.durable_watermark, st.next_event_seq), (1, 2, 4));
    assert_eq!(st.durability(), apb::Durability::SimulatedTestOnly);
    // Released events stay readable until their slot is reused: event 1 (slot reused by
    // event 3) is gone, event 2 (released, slot still intact) is still visible.
    let e = events(c.events(0, 0).unwrap());
    assert_eq!(e.oldest_visible, 2);
    assert_eq!(e.events.iter().map(|e| e.event_seq).collect::<Vec<_>>(), [2, 3]);
    // A reader whose cursor fell behind the reused prefix sees the gap explicitly.
    assert!(e.gap_detected);
    assert!(!events(c.events(1, 0).unwrap()).gap_detected);
    k.stop();
}

/// T07 (links): a disconnection that would raise more events than free slots is deferred
/// (link kept Connected in the kernel, CRITICAL, pending counted) and applied as soon as
/// slots are released, in order. Never dropped, never applied partially.
#[test]
fn disconnection_is_deferred_until_slots_are_released() {
    let mut rules = rule(1, 101, "bool_equals", "expected = true\n", "major");
    rules.push_str(&rule(5, 103, "bad_quality", "", "info"));
    let k = KernelProc::start_with(&with_alarms(Alarms::with(2, rules)), None, &["--test-simulated-durable-sink"]);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, true); // Raised 1; bad quality armed
    assert_eq!(door(&mut s, 2, false).status, AckStatus::AppliedVolatile as i32); // Returned 1
    drop(s);
    let r = k.wait_for("pending disconnection", |r| r.alarms.as_ref().is_some_and(|a| a.pending_link_transitions == 1));
    assert_eq!(link(&r, SRC), LinkState::Connected, "not applied while the outbox is full");
    assert_eq!(quality(&tag(&r, 103)), Quality::Good);
    assert_eq!(c.capacity().unwrap().overload(), apb::OverloadState::Critical);
    // Release: the deferred disconnection is applied (bad quality raised).
    c.simulated_commit(2).unwrap();
    let r = k.wait_for("disconnection applied", |r| link(r, SRC) == LinkState::Disconnected);
    assert_eq!(r.alarms.as_ref().unwrap().pending_link_transitions, 0);
    let a5 = wait("bad quality raised", || entry(&mut c, 5));
    assert_eq!(a5.state(), apb::EpisodeState::ActiveUnacked);
    assert!(!a5.input_valid);
    k.stop();
}

/// T10: supervision coupling. Silence (Uncertain) raises BadQuality and never returns a
/// process rule to Normal; fresh data returns it. Runs under the ingress policy and
/// scheduler of the environment (check.sh: FIFO, source_fair:1/4, both schedulers).
#[test]
fn supervision_silence_drives_bad_quality_but_never_auto_normal() {
    let mut rules = rule(1, 101, "bool_equals", "expected = true\n", "major");
    rules.push_str(&rule(5, 103, "bad_quality", "", "info"));
    let limits = Limits {
        runtime: "watchdog_poll_ms = 20\n".into(),
        source_1001: "liveness_timeout_ms = 300\n".into(),
        ..with_alarms(Alarms::with(1024, rules))
    };
    let k = KernelProc::start(&limits);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, true);
    assert_eq!(entry(&mut c, 1).unwrap().state(), apb::EpisodeState::ActiveUnacked);
    let r = k.wait_for("silent", |r| {
        r.sources.iter().find(|x| x.source_id == SRC).is_some_and(|x| x.liveness == Liveness::Silent as i32)
    });
    assert_eq!(quality(&tag(&r, 103)), Quality::UncertainSourceSilent);
    let a5 = wait("bad quality on silence", || entry(&mut c, 5));
    assert_eq!(a5.state(), apb::EpisodeState::ActiveUnacked);
    // The process alarm is indeterminate: still active, input not valid, NOT Normal.
    let a1 = entry(&mut c, 1).unwrap();
    assert_eq!(a1.state(), apb::EpisodeState::ActiveUnacked);
    assert!(!a1.input_valid);
    // Fresh data: bad quality returns; the door clears its alarm only by its own value.
    full(&mut s, 2, false);
    let a5 = entry(&mut c, 5).unwrap();
    assert_eq!(a5.state(), apb::EpisodeState::ReturnedUnacked);
    assert_eq!(entry(&mut c, 1).unwrap().state(), apb::EpisodeState::ReturnedUnacked);
    k.stop();
}

/// T14: a restart is a new instance. Old cursors, tokens and volatile episodes are gone and
/// are refused explicitly, never applied to the new instance.
#[test]
fn restart_is_a_new_instance() {
    let limits = with_alarms(Alarms::with(64, two_bool_rules()));
    let k = KernelProc::start(&limits);
    let mut c = AlarmClient::connect(&k.alarm).unwrap();
    let mut s = Session::connect(&k.socket, "d", SRC, 1).unwrap();
    full(&mut s, 1, true);
    let token = entry(&mut c, 1).unwrap().activation_token;
    let old = c.instance.clone();
    drop(s);
    drop(c);
    let _ = k.kill();
    let k2 = KernelProc::start(&limits);
    let mut c = AlarmClient::connect(&k2.alarm).unwrap();
    c.instance = old;
    assert_eq!(err_code(&c.ack(1, token).unwrap()), apb::ErrorCode::InstanceMismatch);
    assert_eq!(err_code(&c.events(0, 0).unwrap()), apb::ErrorCode::InstanceMismatch);
    c.instance = Vec::new();
    assert!(page(c.snapshot(0, 0, 0).unwrap()).alarms.is_empty(), "volatile episodes do not survive");
    assert_eq!(c.capacity().unwrap().next_event_seq, 1);
    k2.stop();
}
