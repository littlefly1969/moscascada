//! VS-007 end-to-end: Modbus simulator -> driver process -> kernel process (RTDB, alarm
//! engine, SQLite journal, recovery). T01, T04..T11, T13 of the specification.

mod support;

use std::process::Command;
use std::time::{Duration, Instant};

use scada_modbus::Area;
use scada_modbus::pdu::ExceptionCode;
use scada_modbus_sim::{Fault, Trigger};
use scada_protocol::alarm_v1;
use scada_protocol::history_v1;
use scada_protocol::introspect_v1::{LinkState, Quality};
use scada_sim_driver::{AlarmClient, HistoryClient};
use support::*;

fn tmp() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("scada-vs007").tempdir().unwrap()
}

fn source(
    r: &scada_protocol::introspect_v1::IntrospectResponse,
    id: u32,
) -> scada_protocol::introspect_v1::SourceState {
    r.sources.iter().find(|s| s.source_id == id).cloned().expect("source")
}

// ------------------------------------------------------------------ T01

#[test]
fn t01_check_config_is_fail_closed_and_opens_no_socket() {
    let d = tmp();
    let s = sim();
    // A listener on the kernel socket path: nobody may connect to it.
    let ksock = d.path().join("driver.sock");
    let listener = std::os::unix::net::UnixListener::bind(&ksock).unwrap();
    listener.set_nonblocking(true).unwrap();
    let good = driver_config(&ksock, s.port(), &DriverOpts::default());
    let kernel_cfg = d.path().join("kernel.toml");
    std::fs::write(&kernel_cfg, kernel_config(d.path(), &KernelOpts::default())).unwrap();
    let run = |cfg: &str, kernel: bool| {
        let p = d.path().join("check.toml");
        std::fs::write(&p, cfg).unwrap();
        let mut c = Command::new(driver_bin());
        c.arg("--config").arg(&p).arg("--check-config");
        if kernel {
            c.arg("--kernel-config").arg(&kernel_cfg);
        }
        c.output().unwrap()
    };
    let ok = run(&good, true);
    assert!(ok.status.success(), "{}", String::from_utf8_lossy(&ok.stderr));
    let out = String::from_utf8_lossy(&ok.stdout);
    assert!(out.contains("CONFIG OK devices=1"), "{out}");
    assert!(out.contains("READ fc=01 coil address=0 quantity=1"), "{out}");
    let cases: Vec<(String, bool, &str)> = vec![
        (good.replace("schema = \"moscada.driver.modbus-tcp/1\"", "schema = \"x/2\""), false, "schema"),
        (good.replace("unit_id = 1\n", "unit_id = 1\nbogus = 3\n"), false, "unknown field"),
        (good.replace("{ channel = 2, point = \"di_1\" }", "{ channel = 1, point = \"di_1\" }"), false, "mapped twice"),
        (good.replace("{ channel = 2, point = \"di_1\" }", "{ channel = 2, point = \"di_0\" }"), false, "two channels"),
        (good.replace("point = \"di_1\"", "point = \"nope\""), false, "not in profile"),
        (good.replace("profile_version = 1", "profile_version = 2"), false, "not in the catalog"),
        (good.replace("scan_interval_ms = 50", "scan_interval_ms = 1"), false, "scan_interval_ms"),
        (good.replace("max_pending_batches = 64", "max_pending_batches = 0"), false, "max_pending"),
        (good.replace("require_durable_receipts = false", "require_durable_receipts = true"), false, "needs request"),
        // Against the kernel: a missing channel makes a FullState impossible.
        (good.replace("  { channel = 9, point = \"signed_i32\" },\n", ""), true, "FullState"),
        // Type mismatch: channel 4 is i64 in the kernel, di_0 decodes to bool.
        (
            good.replace("{ channel = 4, point = \"temperature_centi\" }", "{ channel = 4, point = \"coil_0\" }")
                .replace("{ channel = 3, point = \"coil_0\" }", "{ channel = 3, point = \"temperature_centi\" }"),
            true,
            "decodes to",
        ),
        (good.replace("source_id = 1001", "source_id = 4242"), true, "not configured in the kernel"),
    ];
    for (cfg, kernel, want) in &cases {
        let o = run(cfg, *kernel);
        let err = String::from_utf8_lossy(&o.stderr);
        assert_eq!(o.status.code(), Some(2), "accepted an invalid configuration ({want}): {err}");
        assert!(err.contains("CONFIG INVALID") && err.contains(want), "expected `{want}`: {err}");
    }
    // Zero connections: neither to the device nor to the kernel.
    assert_eq!(s.counters().connections_accepted, 0);
    assert!(listener.accept().is_err(), "check-config connected to the kernel socket");
}

// ------------------------------------------------------------------ T04, T05, T11

#[test]
fn t04_t05_full_state_exact_channels_then_minimal_deltas() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts::default());
    let s = sim();
    let mut img = Image::default();
    let drv = Driver::start(d.path(), &driver_config(&k.socket, s.port(), &DriverOpts::default()));
    let r = k.wait_for("full state", T, |r| matches_image(r, &img));
    let applied0 = r.counters.as_ref().unwrap().batches_applied;
    let st = drv.wait_status("dev-a", "streaming", T, |s| s["link"] == "streaming");
    assert_eq!(num(&st, "full_states"), 1);
    assert_eq!(st["continuity"], "CONTINUITY_BASELINE_ESTABLISHED");
    // Several no-change scans: no batch at all.
    let scans = num(&st, "scans_ok");
    drv.wait_status("dev-a", "more scans", T, |s| num(s, "scans_ok") >= scans + 6);
    let r = k.introspect();
    assert_eq!(r.counters.as_ref().unwrap().batches_applied, applied0, "superfluous batch");
    // One register changes: one Delta with exactly that channel.
    img.temperature_centi = 2275;
    set_image(&s, &img);
    k.wait_for("delta", T, |r| matches_image(r, &img));
    let st = drv.wait_status("dev-a", "delta", T, |s| num(s, "deltas") == 1);
    assert_eq!(num(&st, "full_states"), 1);
    let r = k.introspect();
    assert_eq!(r.counters.as_ref().unwrap().batches_applied, applied0 + 1);
    assert_eq!(source(&r, 1001).last_accepted_source_sequence, Some(2));
    // A 32-bit counter and a float change in the same scan: still one delta.
    img.counter = 70_001;
    img.level = -2.5;
    set_image(&s, &img);
    k.wait_for("delta 2", T, |r| matches_image(r, &img));
    assert_eq!(source(&k.introspect(), 1001).last_accepted_source_sequence, Some(3));
    let st = drv.status("dev-a").unwrap();
    assert_eq!(num(&st, "requests_per_scan"), 4);
    assert_eq!(st["scan_errors"], "-");
    // T11: the sequence of transitions is explicit.
    let ts = drv.transitions().join("\n");
    assert!(
        ts.contains("to=handshaking") && ts.contains("to=awaiting_full_state") && ts.contains("to=streaming"),
        "{ts}"
    );
}

#[test]
fn t11_only_driver_acquisition_time_is_recorded() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts { persistence: Some(String::new()), ..Default::default() });
    let s = sim();
    let mut img = Image::default();
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
    let drv = Driver::start(
        d.path(),
        &driver_config(&k.socket, s.port(), &DriverOpts { durable: true, ..Default::default() }),
    );
    k.wait_for("full state", T, |r| matches_image(r, &img));
    img.di_0 = true;
    set_image(&s, &img);
    k.wait_for("delta", T, |r| matches_image(r, &img));
    drv.wait_status("dev-a", "durable", T, |s| s["durable_seq"] == "2");
    let after = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
    let db = rusqlite::Connection::open_with_flags(
        d.path().join("journal").join("kernel.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut st = db
        .prepare("SELECT source_times FROM journal_operations WHERE source_times IS NOT NULL ORDER BY operation_seq")
        .unwrap();
    let rows: Vec<Vec<u8>> = st.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect();
    assert_eq!(rows.len(), 2, "FullState + Delta carry an acquisition time");
    for b in rows {
        let t = scada_journal::codec::decode_source_times(&b).unwrap();
        let acq = t.acquired_at_utc_ns.expect("acquired_at");
        assert!(acq >= before && acq <= after, "{acq} not in [{before}, {after}]");
        assert_eq!(t.clock_quality, 1, "CLOCK_QUALITY_UNKNOWN: the driver does not certify the host clock");
        assert!(t.clock_id.is_empty());
        assert!(t.observations.is_empty(), "no invented device timestamp");
    }
}

// ------------------------------------------------------------------ T06

#[test]
fn t06_device_failure_mid_scan_degrades_and_never_publishes_a_partial_scan() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts::default());
    let s = sim();
    let mut img = Image::default();
    let drv = Driver::start(d.path(), &driver_config(&k.socket, s.port(), &DriverOpts::default()));
    k.wait_for("full state", T, |r| matches_image(r, &img));
    // The coil is read by the FIRST request of a scan; the holding registers fail later in
    // the same scan (connection closed by the device). The coil change must not appear.
    s.inject(Fault::Close, Trigger::Always, Some(Area::HoldingRegister));
    img.coil_0 = true;
    set_image(&s, &img);
    let r = k.wait_for("degraded", T, |r| all_quality(r, Quality::UncertainSourceUnavailable, 101));
    assert_eq!(tag_value(&tag(&r, 103)), Some(Expect::B(false)), "value of a partial scan leaked");
    assert_eq!(LinkState::try_from(source(&r, 1001).link_state), Ok(LinkState::Disconnected));
    let st = drv.wait_status("dev-a", "down", T, |s| s["device_state"] == "down" && num(s, "scans_failed") >= 2);
    assert!(st["scan_errors"].contains("closed"), "{st:?}");
    // Still degraded after several retries; nothing Good meanwhile.
    std::thread::sleep(Duration::from_millis(300));
    let r = k.introspect();
    assert!(all_quality(&r, Quality::UncertainSourceUnavailable, 101), "{}", summary(&r));
    // Exceptions and timeouts degrade the same way.
    s.clear_faults();
    s.inject(Fault::Exception(ExceptionCode::ServerDeviceBusy), Trigger::Always, Some(Area::InputRegister));
    drv.wait_status("dev-a", "exception", T, |s| s["scan_errors"].contains("exception"));
    assert!(all_quality(&k.introspect(), Quality::UncertainSourceUnavailable, 101));
    // Recovery: a complete scan, a FullState, every channel Good with the current values.
    s.clear_faults();
    let r = k.wait_for("recovered", T, |r| matches_image(r, &img));
    assert!(source(&r, 1001).last_accepted_source_sequence.unwrap() >= 2);
    let st = drv.wait_status("dev-a", "streaming again", T, |s| s["link"] == "streaming");
    assert!(num(&st, "full_states") >= 2);
    assert!(st["scan_errors"].contains("exception"), "{st:?}");
    // Same epoch across the device outage: a reconnect, not a restart.
    assert_eq!(st["continuity"], "CONTINUITY_CONTINUOUS");
}

#[test]
fn t06_mutation_during_scan_and_truncated_frames_are_handled() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts::default());
    let s = sim();
    let img = Image::default();
    let drv = Driver::start(d.path(), &driver_config(&k.socket, s.port(), &DriverOpts::default()));
    k.wait_for("full state", T, |r| matches_image(r, &img));
    // Truncated frame: timeout with a partial frame, connection dropped, reconnected.
    s.inject(Fault::Truncate { keep: 8 }, Trigger::Next, Some(Area::InputRegister));
    let st = drv.wait_status("dev-a", "partial frame", T, |s| s["scan_errors"].contains("timeout_partial_frame"));
    assert!(num(&st, "modbus_connects") >= 1);
    k.wait_for("recovered", T, |r| matches_image(r, &img));
    // Wrong transaction id and wrong unit id: never accepted.
    s.inject(Fault::WrongTransactionId, Trigger::Next, None);
    drv.wait_status("dev-a", "tid", T, |s| s["scan_errors"].contains("transaction_mismatch"));
    s.inject(Fault::WrongUnitId, Trigger::Next, None);
    drv.wait_status("dev-a", "unit", T, |s| s["scan_errors"].contains("unit_mismatch"));
    k.wait_for("recovered 2", T, |r| matches_image(r, &img));
    // Value changing between the requests of a scan (not an atomic snapshot): the scan
    // reports its spread; the next scan converges to the device state.
    s.set_mutation(Some(Box::new(|m, n, _| m.input[0] = (2000 + n % 50) as u16)));
    std::thread::sleep(Duration::from_millis(300));
    s.set_mutation(None);
    let final_temp = s.with_memory(|m| m.input[0] as i16);
    let want = Image { temperature_centi: final_temp, ..img };
    k.wait_for("converged", T, |r| matches_image(r, &want));
    let st = drv.status("dev-a").unwrap();
    assert!(num(&st, "max_spread_us") > 0);
}

// ------------------------------------------------------------------ T07, T09

#[test]
fn t07_kernel_crash_with_journal_recovered_unverified_until_driver_full_state() {
    let d = tmp();
    let opts = KernelOpts { persistence: Some(String::new()), ..Default::default() };
    let k = Kernel::start(d.path(), &opts);
    let s = sim();
    let img = Image::default();
    let drv = Driver::start(
        d.path(),
        &driver_config(&k.socket, s.port(), &DriverOpts { durable: true, ..Default::default() }),
    );
    k.wait_for("full state", T, |r| matches_image(r, &img));
    drv.wait_status("dev-a", "durable", T, |s| s["durable"] == "true" && s["durable_seq"] == "1");
    // The device goes away first, so that the restarted kernel is observed before the driver
    // can verify the source again.
    s.set_accepting(false);
    s.drop_connections();
    drv.wait_status("dev-a", "device down", T, |s| s["device_state"] == "down");
    let _ = k.kill();
    let k = Kernel::start(d.path(), &opts);
    let r = k.wait_for("recovered", T, |r| r.persistence.as_ref().is_some_and(|p| p.recovered_operation_sequence > 0));
    assert!(all_quality(&r, Quality::UncertainRecoveredUnverified, 101), "{}", summary(&r));
    assert!(source(&r, 1001).unverified);
    std::thread::sleep(Duration::from_millis(300));
    assert!(all_quality(&k.introspect(), Quality::UncertainRecoveredUnverified, 101));
    // Device back: complete scan, FullState demanded by the Welcome, Live verified.
    s.set_accepting(true);
    let r = k.wait_for("verified", T, |r| matches_image(r, &img));
    assert!(!source(&r, 1001).unverified);
    let mut h = HistoryClient::connect(&k.history).unwrap();
    let rec = h.recovery().unwrap();
    let src = rec.sources.iter().find(|x| x.source_id == 1001).unwrap();
    assert_eq!(src.live(), history_v1::LiveVerification::LiveVerified);
    assert_ne!(src.continuity(), history_v1::HistoryContinuity::Complete, "a poller never proves completeness");
    let st = drv.wait_status("dev-a", "streaming", T, |s| s["link"] == "streaming");
    // Recovered sequence 1 was durable: nothing declared lost.
    assert_eq!(num(&st, "lost_kernel_restart"), 0);
}

#[test]
fn t09_driver_restart_is_a_new_epoch_with_baseline_and_unknown_history() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts { persistence: Some(String::new()), ..Default::default() });
    let s = sim();
    let img = Image::default();
    let cfg = driver_config(&k.socket, s.port(), &DriverOpts { durable: true, ..Default::default() });
    let mut drv = Driver::start(d.path(), &cfg);
    k.wait_for("full state", T, |r| matches_image(r, &img));
    let e1 = source(&k.introspect(), 1001).source_epoch.unwrap();
    drv.kill();
    k.wait_for("disconnected", T, |r| all_quality(r, Quality::UncertainSourceUnavailable, 101));
    let drv2 = Driver::start(d.path(), &cfg);
    let r = k.wait_for("full state 2", T, |r| matches_image(r, &img));
    let e2 = source(&r, 1001).source_epoch.unwrap();
    assert_ne!(e1, e2, "a new process is a new epoch");
    assert_eq!(source(&r, 1001).last_accepted_source_sequence, Some(1), "sequence restarts in the new epoch");
    let st = drv2.wait_status("dev-a", "baseline", T, |s| s["link"] == "streaming");
    assert_eq!(st["continuity"], "CONTINUITY_BASELINE_ESTABLISHED");
    let mut h = HistoryClient::connect(&k.history).unwrap();
    let rec = h.recovery().unwrap();
    let src = rec.sources.iter().find(|x| x.source_id == 1001).unwrap();
    assert_ne!(src.continuity(), history_v1::HistoryContinuity::Complete);
}

#[test]
fn t09_volatile_kernel_restart_reconnects_with_full_state() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts::default());
    let s = sim();
    let mut img = Image::default();
    let drv = Driver::start(d.path(), &driver_config(&k.socket, s.port(), &DriverOpts::default()));
    k.wait_for("full state", T, |r| matches_image(r, &img));
    let epoch = source(&k.introspect(), 1001).source_epoch;
    let _ = k.kill();
    img.temperature_centi = -120;
    set_image(&s, &img);
    let k = Kernel::start(d.path(), &KernelOpts::default());
    let r = k.wait_for("full state after restart", T, |r| matches_image(r, &img));
    assert_eq!(source(&r, 1001).source_epoch, epoch, "same driver process, same epoch");
    let st = drv.wait_status("dev-a", "streaming", T, |s| s["link"] == "streaming");
    assert_eq!(st["continuity"], "CONTINUITY_BASELINE_ESTABLISHED");
    assert!(num(&st, "connects") >= 2);
}

// ------------------------------------------------------------------ T08, T13

#[test]
fn t08_stalled_journal_no_premature_durable_receipt_bounded_pending() {
    let d = tmp();
    let stall = d.path().join("stall");
    let opts = KernelOpts {
        persistence: Some(String::new()),
        args: vec!["--test-journal-stall-file".into(), stall.display().to_string()],
        ..Default::default()
    };
    let k = Kernel::start(d.path(), &opts);
    let s = sim();
    let mut img = Image::default();
    let drv = Driver::start(
        d.path(),
        &driver_config(
            &k.socket,
            s.port(),
            &DriverOpts { durable: true, require_durable: true, max_pending: 4, ..Default::default() },
        ),
    );
    k.wait_for("full state", T, |r| matches_image(r, &img));
    drv.wait_status("dev-a", "durable 1", T, |s| s["durable_seq"] == "1");
    std::fs::write(&stall, b"").unwrap();
    // Every scan changes something: batches pile up behind the stalled commit.
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(800) {
        img.counter += 1;
        set_image(&s, &img);
        std::thread::sleep(Duration::from_millis(30));
    }
    let st = drv.wait_status("dev-a", "backpressured", T, |s| num(s, "deferred") > 0);
    assert!(num(&st, "pending") <= 4 && num(&st, "pending_high_water") <= 4, "{st:?}");
    assert_eq!(st["durable_seq"], "1", "no durable receipt while the journal is stalled");
    assert_eq!(num(&st, "acks_volatile"), 0);
    std::fs::remove_file(&stall).unwrap();
    // Released: every pending batch gets its durable receipt; the latest state arrives.
    k.wait_for("caught up", T, |r| matches_image(r, &img));
    let st = drv.wait_status("dev-a", "drained", T, |s| num(s, "pending") == 0 && s["link"] == "streaming");
    let next = num(&st, "next_seq");
    assert_eq!(st["durable_seq"], (next - 1).to_string());
    assert_eq!(num(&st, "lost_kernel_restart"), 0);
}

#[test]
fn t13_journal_queue_full_is_a_typed_refusal_retried_with_the_same_sequence() {
    let d = tmp();
    let stall = d.path().join("stall");
    let opts = KernelOpts {
        persistence: Some("journal_queue_operations = 16\n".into()),
        args: vec!["--test-journal-stall-file".into(), stall.display().to_string()],
        ..Default::default()
    };
    let k = Kernel::start(d.path(), &opts);
    let s = sim();
    let mut img = Image::default();
    // Volatile receipts: ACKs come right after apply, so the journal queue (16) fills.
    let drv = Driver::start(
        d.path(),
        &driver_config(&k.socket, s.port(), &DriverOpts { max_pending: 32, ..Default::default() }),
    );
    k.wait_for("full state", T, |r| matches_image(r, &img));
    std::fs::write(&stall, b"").unwrap();
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_millis(2000) {
        img.counter += 1;
        set_image(&s, &img);
        std::thread::sleep(Duration::from_millis(30));
        if drv.status("dev-a").is_some_and(|s| num(&s, "rejected_retryable") > 0) {
            break;
        }
    }
    let st = drv.wait_status("dev-a", "refused", T, |s| num(s, "rejected_retryable") > 0);
    assert!(st["kernel_error"].contains("JOURNAL_QUEUE_FULL"), "{st:?}");
    let r = k.introspect();
    assert!(r.persistence.as_ref().unwrap().refused_queue_full > 0);
    let seq_before = source(&r, 1001).last_accepted_source_sequence.unwrap();
    std::fs::remove_file(&stall).unwrap();
    let r = k.wait_for("caught up", T, |r| matches_image(r, &img));
    // No gap: the refused sequence was resent, not skipped.
    let st = drv.wait_status("dev-a", "drained", T, |s| num(s, "pending") == 0);
    assert!(source(&r, 1001).last_accepted_source_sequence.unwrap() > seq_before);
    assert_eq!(num(&st, "rejected_fatal"), 0);
    assert_eq!(st["continuity"], "CONTINUITY_CONTINUOUS");
}

#[test]
fn t13_full_outbox_is_a_typed_refusal_and_nothing_is_silently_dropped() {
    let d = tmp();
    let rules = rule(1, 101, "bool_equals", "expected = true\n", "major");
    let k = Kernel::start(d.path(), &KernelOpts { alarm_rules: Some(rules), outbox_capacity: 4, ..Default::default() });
    let s = sim();
    let mut img = Image::default();
    let drv = Driver::start(
        d.path(),
        &driver_config(&k.socket, s.port(), &DriverOpts { max_pending: 8, ..Default::default() }),
    );
    k.wait_for("full state", T, |r| matches_image(r, &img));
    // Without a journal nothing frees outbox slots: after 4 events every alarm transition
    // is refused (OUTBOX_CAPACITY_EXCEEDED), batches without transitions still apply.
    for _ in 0..6 {
        img.di_0 = !img.di_0;
        set_image(&s, &img);
        std::thread::sleep(Duration::from_millis(150));
    }
    let st = drv.wait_status("dev-a", "outbox refusal", T, |s| num(s, "rejected_retryable") > 0);
    assert!(st["kernel_error"].contains("OUTBOX_CAPACITY_EXCEEDED"), "{st:?}");
    assert!(num(&st, "pending") <= 8);
    let r = k.introspect();
    assert!(r.alarms.as_ref().unwrap().outbox_rejections > 0);
    // The refused batch is still pending in the driver, retried with the same sequence.
    std::thread::sleep(Duration::from_millis(300));
    let st2 = drv.status("dev-a").unwrap();
    assert!(num(&st2, "pending") >= 1 && num(&st2, "rejected_retryable") > num(&st, "rejected_retryable"), "{st2:?}");
    assert_eq!(num(&st2, "rejected_fatal"), 0);
    assert!(num(&st2, "deferred") > 0);
}

// ------------------------------------------------------------------ T10

#[test]
fn t10_alarm_on_a_modbus_point_survives_disconnection_without_false_return() {
    let d = tmp();
    let rules = format!(
        "{}{}",
        rule(1, 101, "bool_equals", "expected = true\n", "major"),
        rule(3, 104, "bad_quality", "", "info")
    );
    let opts = KernelOpts {
        alarm_rules: Some(rules),
        outbox_capacity: 1024,
        persistence: Some(String::new()),
        ..Default::default()
    };
    let k = Kernel::start(d.path(), &opts);
    let s = sim();
    let mut img = Image::default();
    let drv = Driver::start(
        d.path(),
        &driver_config(&k.socket, s.port(), &DriverOpts { durable: true, ..Default::default() }),
    );
    k.wait_for("full state", T, |r| matches_image(r, &img));
    img.di_0 = true;
    set_image(&s, &img);
    k.wait_for("trip", T, |r| matches_image(r, &img));
    let mut a = AlarmClient::connect(&k.alarm).unwrap();
    let alarm = |a: &mut AlarmClient, id: u32| -> alarm_v1::AlarmEntry {
        match a.snapshot(0, 64, 0).unwrap().body {
            Some(alarm_v1::alarm_response::Body::Snapshot(sn)) => {
                sn.alarms.into_iter().find(|x| x.alarm_id == id).unwrap()
            }
            other => panic!("{other:?}"),
        }
    };
    let a1 = alarm(&mut a, 1);
    assert_eq!(a1.state(), alarm_v1::EpisodeState::ActiveUnacked);
    let occ = a1.occurrence_id;
    let events_before = match a.events(0, 1000).unwrap().body {
        Some(alarm_v1::alarm_response::Body::Events(e)) => e.events.len(),
        other => panic!("{other:?}"),
    };
    // Device lost; while it is away the door closes (di_0 false): unobservable.
    s.set_accepting(false);
    s.drop_connections();
    k.wait_for("degraded", T, |r| all_quality(r, Quality::UncertainSourceUnavailable, 101));
    img.di_0 = false;
    set_image(&s, &img);
    std::thread::sleep(Duration::from_millis(300));
    let a1 = alarm(&mut a, 1);
    assert_eq!(a1.state(), alarm_v1::EpisodeState::ActiveUnacked, "no return while the input is not Good");
    assert!(!a1.input_valid);
    assert_eq!(alarm(&mut a, 3).state(), alarm_v1::EpisodeState::ActiveUnacked, "signal-lost alarm active");
    let evs = match a.events(0, 1000).unwrap().body {
        Some(alarm_v1::alarm_response::Body::Events(e)) => e.events,
        other => panic!("{other:?}"),
    };
    assert!(!evs.iter().skip(events_before).any(|e| e.alarm_id == 1), "no event of alarm 1 during the outage: {evs:?}");
    // Device back: FullState with di_0 = false -> Returned (same occurrence), signal back.
    s.set_accepting(true);
    k.wait_for("recovered", T, |r| matches_image(r, &img));
    let a1 = alarm(&mut a, 1);
    assert_eq!(a1.state(), alarm_v1::EpisodeState::ReturnedUnacked);
    assert_eq!(a1.occurrence_id, occ, "same episode");
    assert_eq!(alarm(&mut a, 3).state(), alarm_v1::EpisodeState::ReturnedUnacked);
    // Durable operator ACK, then a kernel crash: the acknowledged state survives.
    let r = a.ack_action(1, a1.activation_token, [7; 16], "e2e", true).unwrap();
    assert!(
        matches!(&r.body, Some(alarm_v1::alarm_response::Body::Ack(x)) if x.status() == alarm_v1::AckStatus::AppliedDurable),
        "{r:?}"
    );
    drv.wait_status("dev-a", "streaming", T, |s| s["link"] == "streaming");
    drop(a);
    let _ = k.kill();
    let k = Kernel::start(d.path(), &opts);
    k.wait_for("verified after crash", T, |r| matches_image(r, &img));
    let mut h = HistoryClient::connect(&k.history).unwrap();
    let p = h.projection(0, 64).unwrap();
    assert!(!p.alarms.iter().any(|x| x.alarm_id == 1), "alarm 1 acknowledged and returned: not active: {p:?}");
    let page = h.history(0, 1000, 1).unwrap();
    let kinds: Vec<i32> = page.events.iter().map(|e| e.kind).collect();
    assert!(kinds.len() >= 3, "raised, returned, acknowledged: {kinds:?}");
    assert!(page.events.iter().all(|e| e.occurrence_id == occ));
}

// ------------------------------------------------------------------ isolation

#[test]
fn one_failing_device_does_not_disturb_another() {
    let d = tmp();
    let k = Kernel::start(d.path(), &KernelOpts { extra_sources: vec![(1002, 201)], ..Default::default() });
    let s1 = sim();
    let s2 = sim();
    let mut img2 = Image { temperature_centi: 1111, ..Image::default() };
    set_image(&s2, &img2);
    let o = DriverOpts { extra_devices: vec![("dev-b".into(), 1002, s2.port())], ..Default::default() };
    let drv = Driver::start(d.path(), &driver_config(&k.socket, s1.port(), &o));
    k.wait_for("both", T, |r| matches_image(r, &Image::default()) && matches_image_at(r, &img2, 201));
    // Device A stops answering (silent: timeouts), device B keeps changing.
    s1.inject(Fault::NoResponse, Trigger::Always, None);
    k.wait_for("a degraded", T, |r| all_quality(r, Quality::UncertainSourceUnavailable, 101));
    for i in 0..5 {
        img2.counter = 10 + i;
        set_image(&s2, &img2);
        k.wait_for("b follows", T, |r| matches_image_at(r, &img2, 201));
    }
    let a = drv.status("dev-a").unwrap();
    let b = drv.status("dev-b").unwrap();
    assert_eq!(a["device_state"], "down");
    assert!(a["scan_errors"].contains("timeout"));
    assert_eq!(b["device_state"], "up");
    assert_eq!(b["scan_errors"], "-");
}
