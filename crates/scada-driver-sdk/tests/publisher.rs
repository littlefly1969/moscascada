//! SDK behaviour against a scripted fake kernel (exact control over ACKs, losses and
//! refusals that the real kernel produces only under rare timing). The real kernel is
//! exercised by the end-to-end tests of `scada-driver-modbus-tcp`.

use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use scada_driver_sdk::{ChannelType, LinkState, Publish, PublisherConfig, SourcePublisher, Value};
use scada_protocol::driver_v1::{
    AckStatus, BatchMode, ClockQuality, Continuity, DriverMessage, ErrorCode, InputAck, KernelMessage,
    ObservationBatch, SessionLimits, Welcome, driver_message, kernel_message,
};
use scada_protocol::frame;

const INSTANCE_A: [u8; 16] = [0xA; 16];
const INSTANCE_B: [u8; 16] = [0xB; 16];

struct Conn {
    s: UnixStream,
    r: Vec<u8>,
    w: Vec<u8>,
    instance: Vec<u8>,
}

impl Conn {
    fn recv(&mut self) -> Option<DriverMessage> {
        frame::read_message(&mut self.s, 1 << 20, &mut self.r).ok().flatten()
    }

    fn recv_batch(&mut self) -> ObservationBatch {
        loop {
            match self.recv().expect("driver message").body {
                Some(driver_message::Body::Batch(b)) => return b,
                Some(driver_message::Body::Heartbeat(_)) => continue,
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    fn send(&mut self, m: KernelMessage) {
        frame::write_message(&mut self.s, &m, 1 << 20, &mut self.w).unwrap();
    }

    fn ack(&mut self, seq: u64, status: AckStatus, code: ErrorCode, cont: Continuity, last: Option<u64>) {
        let op =
            matches!(status, AckStatus::AppliedVolatile | AckStatus::AppliedDurable | AckStatus::AppliedNotDurable)
                .then_some(seq + 100);
        self.send(KernelMessage {
            body: Some(kernel_message::Body::Ack(InputAck {
                source_sequence: seq,
                status: status as i32,
                operation_sequence: op,
                error_code: code as i32,
                kernel_instance_id: self.instance.clone(),
                continuity: cont as i32,
                last_accepted_source_sequence: last,
            })),
        });
    }

    fn applied(&mut self, seq: u64, durable: bool) {
        let st = if durable { AckStatus::AppliedDurable } else { AckStatus::AppliedVolatile };
        self.ack(seq, st, ErrorCode::Unspecified, Continuity::Continuous, Some(seq));
    }
}

struct FakeKernel {
    path: PathBuf,
    listener: UnixListener,
    _dir: tempfile::TempDir,
}

struct WelcomeSpec {
    instance: [u8; 16],
    minor: u32,
    full_state_required: bool,
    last: Option<u64>,
    durable: bool,
    inflight: u32,
}

impl Default for WelcomeSpec {
    fn default() -> Self {
        WelcomeSpec {
            instance: INSTANCE_A,
            minor: 2,
            full_state_required: true,
            last: None,
            durable: false,
            inflight: 32,
        }
    }
}

impl FakeKernel {
    fn new() -> FakeKernel {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.sock");
        let listener = UnixListener::bind(&path).unwrap();
        FakeKernel { path, listener, _dir: dir }
    }

    /// Accept one connection on a thread, answer the Hello, then run `script`.
    fn serve<T: Send + 'static>(
        &self,
        w: WelcomeSpec,
        script: impl FnOnce(&mut Conn, scada_protocol::driver_v1::Hello) -> T + Send + 'static,
    ) -> std::thread::JoinHandle<T> {
        let l = self.listener.try_clone().unwrap();
        std::thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let mut c = Conn { s, r: Vec::new(), w: Vec::new(), instance: w.instance.to_vec() };
            let Some(driver_message::Body::Hello(h)) = c.recv().unwrap().body else { panic!("hello") };
            c.send(KernelMessage {
                body: Some(kernel_message::Body::Welcome(Welcome {
                    kernel_instance_id: w.instance.to_vec(),
                    negotiated_protocol_major: 1,
                    negotiated_protocol_minor: w.minor.min(h.protocol_minor),
                    link_generation: 1,
                    limits: Some(SessionLimits {
                        max_frame_bytes: 65_536,
                        max_observations_per_batch: 128,
                        max_inflight_batches: w.inflight,
                    }),
                    full_state_required: w.full_state_required,
                    last_accepted_source_sequence: w.last,
                    durable_receipts: w.durable && h.request_durable_receipts,
                    timeline_id: if w.durable { vec![1; 16] } else { Vec::new() },
                })),
            });
            script(&mut c, h)
        })
    }
}

fn cfg(k: &FakeKernel) -> PublisherConfig {
    let mut c = PublisherConfig::new(
        k.path.clone(),
        "sdk-test",
        1001,
        vec![(1, ChannelType::Bool), (2, ChannelType::I64), (3, ChannelType::I64)],
    );
    c.heartbeat_interval = None;
    c.reconnect_backoff.min = Duration::from_millis(1);
    c.retry_backoff.min = Duration::from_millis(5);
    c
}

fn vals(a: bool, b: i64, c: i64) -> Vec<Value> {
    vec![Value::Bool(a), Value::I64(b), Value::I64(c)]
}

fn connect(p: &mut SourcePublisher) {
    for _ in 0..200 {
        if p.ensure_connected().unwrap() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("not connected: {:?}", p.last_error());
}

#[test]
fn t04_t05_full_state_then_minimal_deltas_and_no_superfluous_batch() {
    let k = FakeKernel::new();
    let (tx, rx) = mpsc::channel();
    let h = k.serve(WelcomeSpec::default(), move |c, hello| {
        assert_eq!(hello.protocol_minor, 2);
        assert!(!hello.request_durable_receipts);
        assert_ne!(hello.source_epoch, 0);
        for _ in 0..2 {
            let b = c.recv_batch();
            tx.send(b.clone()).unwrap();
            c.applied(b.source_sequence, false);
        }
    });
    let mut p = SourcePublisher::new(cfg(&k), 77).unwrap();
    connect(&mut p);
    assert_eq!(p.state(), LinkState::AwaitingFullState);
    assert_eq!(
        p.publish_scan(&vals(true, 5, 7), Some(1_700_000_000_000_000_000)).unwrap(),
        Publish::FullState { seq: 1 }
    );
    let full = rx.recv().unwrap();
    assert_eq!(full.mode(), BatchMode::FullState);
    assert_eq!(full.observations.len(), 3);
    // T11: acquisition time only, clock quality UNKNOWN, no source timestamp, no clock id.
    assert_eq!(full.acquired_at_utc_ns, Some(1_700_000_000_000_000_000));
    assert_eq!(full.source_clock_quality(), ClockQuality::Unknown);
    assert!(full.source_timestamps_utc_ns.is_empty());
    assert!(full.source_clock_id.is_empty());
    p.drain(Duration::from_secs(5));
    assert_eq!(p.state(), LinkState::Streaming);
    assert_eq!(p.publish_scan(&vals(true, 5, 7), None).unwrap(), Publish::NoChange);
    assert_eq!(p.publish_scan(&vals(true, 6, 7), None).unwrap(), Publish::Delta { seq: 2, changed: 1 });
    let d = rx.recv().unwrap();
    assert_eq!(
        (d.mode(), d.source_sequence, d.observations.len(), d.observations[0].channel_id),
        (BatchMode::Delta, 2, 1, 2)
    );
    assert!(d.acquired_at_utc_ns.is_none());
    p.drain(Duration::from_secs(5));
    assert_eq!(p.pending_len(), 0);
    assert_eq!(p.counters().scans_no_change, 1);
    h.join().unwrap();
}

#[test]
fn t08_retryable_refusal_resends_the_same_sequence_in_order() {
    let k = FakeKernel::new();
    let (tx, rx) = mpsc::channel();
    let h = k.serve(WelcomeSpec::default(), move |c, _| {
        let b = c.recv_batch();
        c.applied(b.source_sequence, false); // FullState 1
        let b2 = c.recv_batch();
        let b3 = c.recv_batch(); // pipelined
        c.ack(2, AckStatus::Rejected, ErrorCode::JournalQueueFull, Continuity::Unspecified, Some(1));
        c.ack(3, AckStatus::Rejected, ErrorCode::SequenceGap, Continuity::Unspecified, Some(1));
        let r2 = c.recv_batch();
        let r3 = c.recv_batch();
        c.applied(2, false);
        c.applied(3, false);
        tx.send((b2, b3, r2, r3)).unwrap();
    });
    let mut cf = cfg(&k);
    cf.retry_backoff.min = Duration::from_millis(200);
    let mut p = SourcePublisher::new(cf, 9).unwrap();
    connect(&mut p);
    p.publish_scan(&vals(false, 0, 0), None).unwrap();
    p.drain(Duration::from_secs(5));
    assert_eq!(p.publish_scan(&vals(true, 0, 0), None).unwrap(), Publish::Delta { seq: 2, changed: 1 });
    assert_eq!(p.publish_scan(&vals(true, 1, 0), None).unwrap(), Publish::Delta { seq: 3, changed: 1 });
    while p.counters().rejections_collateral == 0 {
        p.pump(Duration::from_millis(1), false);
    }
    assert_eq!(p.state(), LinkState::Backpressured);
    // While refused: no new batch is built (bounded, explicit).
    assert_eq!(p.publish_scan(&vals(true, 2, 0), None).unwrap(), Publish::Deferred);
    p.drain(Duration::from_secs(5));
    let (b2, b3, r2, r3) = rx.recv().unwrap();
    assert_eq!(b2, r2, "the refused batch is resent unchanged");
    assert_eq!(b3, r3);
    let c = p.counters();
    assert_eq!((c.rejections_retryable, c.rejections_collateral, c.batches_resent, c.scans_deferred), (1, 1, 2, 1));
    assert_eq!(p.pending_len(), 0);
    assert_eq!(p.state(), LinkState::Streaming);
    h.join().unwrap();
}

#[test]
fn t08_lost_ack_on_durable_link_replays_and_proves_durability_by_duplicate() {
    let k = FakeKernel::new();
    let (tx, rx) = mpsc::channel();
    let first = k.serve(WelcomeSpec { durable: true, ..Default::default() }, move |c, h| {
        assert!(h.request_durable_receipts);
        let b = c.recv_batch();
        c.applied(b.source_sequence, true);
        let _ = c.recv_batch(); // 2
        let _ = c.recv_batch(); // 3: applied by the kernel, receipts lost with the link
    });
    let mut cf = cfg(&k);
    cf.request_durable_receipts = true;
    cf.require_durable_receipts = true;
    let mut p = SourcePublisher::new(cf, 5).unwrap();
    connect(&mut p);
    assert!(p.durable_receipts_active());
    p.publish_scan(&vals(false, 0, 0), None).unwrap();
    p.drain(Duration::from_secs(5));
    assert_eq!(p.durable_confirmed_sequence(), Some(1));
    p.publish_scan(&vals(true, 0, 0), None).unwrap();
    p.publish_scan(&vals(true, 1, 0), None).unwrap();
    first.join().unwrap();
    p.pump(Duration::from_millis(200), false);
    assert_eq!(p.state(), LinkState::Disconnected);
    assert_eq!(p.pending_len(), 2, "unconfirmed batches are kept for the same epoch");
    let second = k.serve(
        WelcomeSpec { durable: true, full_state_required: false, last: Some(3), ..Default::default() },
        move |c, h| {
            assert_eq!(h.source_epoch, 5, "same process, same epoch");
            let dup = c.recv_batch();
            tx.send(dup.clone()).unwrap();
            c.ack(3, AckStatus::Duplicate, ErrorCode::Unspecified, Continuity::Unspecified, Some(3));
            let full = c.recv_batch();
            c.applied(full.source_sequence, true);
            full
        },
    );
    connect(&mut p);
    let dup = rx.recv().unwrap();
    assert_eq!(dup.source_sequence, 3, "only the batch at last_accepted is resent (durable proof)");
    p.drain(Duration::from_secs(5));
    assert_eq!(p.durable_confirmed_sequence(), Some(3));
    assert_eq!(p.pending_len(), 0);
    assert_eq!(p.publish_scan(&vals(true, 1, 0), None).unwrap(), Publish::FullState { seq: 4 });
    p.drain(Duration::from_secs(5));
    let full = second.join().unwrap();
    assert_eq!(full.mode(), BatchMode::FullState);
    assert_eq!(p.state(), LinkState::Streaming);
}

#[test]
fn t08_no_premature_durable_confirmation_and_bounded_pending() {
    let k = FakeKernel::new();
    let (tx, rx) = mpsc::channel::<()>();
    let h = k.serve(WelcomeSpec { durable: true, ..Default::default() }, move |c, _| {
        let b = c.recv_batch();
        c.applied(b.source_sequence, true);
        // Stalled journal: read the deltas, answer nothing until told.
        let mut got = Vec::new();
        while got.len() < 3 {
            got.push(c.recv_batch().source_sequence);
        }
        rx.recv().unwrap();
        for s in &got {
            c.applied(*s, true);
        }
        got
    });
    let mut cf = cfg(&k);
    cf.request_durable_receipts = true;
    cf.max_pending = 3;
    let mut p = SourcePublisher::new(cf, 5).unwrap();
    connect(&mut p);
    p.publish_scan(&vals(false, 0, 0), None).unwrap();
    p.drain(Duration::from_secs(5));
    assert_eq!(p.durable_confirmed_sequence(), Some(1));
    for i in 1..=3 {
        p.publish_scan(&vals(false, i, 0), None).unwrap();
    }
    assert_eq!(p.publish_scan(&vals(false, 9, 0), None).unwrap(), Publish::Deferred);
    assert_eq!(p.state(), LinkState::Backpressured);
    p.pump(Duration::from_millis(100), false);
    assert_eq!(p.pending_len(), 3);
    assert_eq!(p.durable_confirmed_sequence(), Some(1), "nothing durable before its receipt");
    tx.send(()).unwrap();
    assert!(p.drain(Duration::from_secs(5)));
    assert_eq!(p.durable_confirmed_sequence(), Some(4));
    assert_eq!(p.counters().pending_high_water, 3);
    assert_eq!(p.counters().scans_deferred, 1);
    assert_eq!(h.join().unwrap(), vec![2, 3, 4]);
}

#[test]
fn t09_kernel_restart_without_baseline_reports_lost_batches_and_resends_full_state() {
    let k = FakeKernel::new();
    let first = k.serve(WelcomeSpec::default(), move |c, _| {
        let b = c.recv_batch();
        c.applied(b.source_sequence, false);
        let _ = c.recv_batch();
        let _ = c.recv_batch(); // kernel dies before answering
    });
    let mut p = SourcePublisher::new(cfg(&k), 5).unwrap();
    connect(&mut p);
    p.publish_scan(&vals(false, 0, 0), None).unwrap();
    p.drain(Duration::from_secs(5));
    p.publish_scan(&vals(true, 0, 0), None).unwrap();
    p.publish_scan(&vals(true, 1, 0), None).unwrap();
    first.join().unwrap();
    p.pump(Duration::from_millis(200), false);
    // Restarted journaling kernel recovered sequence 2 (durable), not 3.
    let second = k.serve(
        WelcomeSpec { instance: INSTANCE_B, full_state_required: true, last: Some(2), ..Default::default() },
        |c, _| {
            let b = c.recv_batch();
            c.ack(
                b.source_sequence,
                AckStatus::AppliedVolatile,
                ErrorCode::Unspecified,
                Continuity::Resynchronized,
                Some(4),
            );
            b
        },
    );
    connect(&mut p);
    assert_eq!(p.counters().batches_lost_kernel_restart, 1);
    assert!(p.last_error().unwrap().contains("3..=3"), "{:?}", p.last_error());
    assert_eq!(p.pending_len(), 0);
    // Never a Delta to a kernel that wants a FullState; the sequence never goes back.
    assert_eq!(p.publish_scan(&vals(true, 1, 0), None).unwrap(), Publish::FullState { seq: 4 });
    p.drain(Duration::from_secs(5));
    let b = second.join().unwrap();
    assert_eq!((b.mode(), b.source_sequence), (BatchMode::FullState, 4));
    assert_eq!(p.last_continuity(), Some(Continuity::Resynchronized));
}

#[test]
fn applied_not_durable_is_kept_and_blocks_new_batches_until_kernel_restart() {
    let k = FakeKernel::new();
    let first = k.serve(WelcomeSpec { durable: true, ..Default::default() }, |c, _| {
        let b = c.recv_batch();
        c.ack(
            b.source_sequence,
            AckStatus::AppliedNotDurable,
            ErrorCode::JournalFault,
            Continuity::BaselineEstablished,
            Some(1),
        );
        std::thread::sleep(Duration::from_millis(200));
    });
    let mut cf = cfg(&k);
    cf.request_durable_receipts = true;
    let mut p = SourcePublisher::new(cf, 5).unwrap();
    connect(&mut p);
    p.publish_scan(&vals(false, 0, 0), None).unwrap();
    p.pump(Duration::from_millis(100), false);
    assert_eq!(p.state(), LinkState::Recovering);
    assert_eq!(p.pending_len(), 1, "never dropped silently");
    assert_eq!(p.publish_scan(&vals(true, 0, 0), None).unwrap(), Publish::Deferred);
    assert_eq!(p.durable_confirmed_sequence(), None);
    first.join().unwrap();
    p.pump(Duration::from_millis(200), false);
    // Restart: nothing recovered for this epoch -> counted as lost, FullState again.
    let second = k.serve(WelcomeSpec { instance: INSTANCE_B, durable: true, ..Default::default() }, |c, _| {
        let b = c.recv_batch();
        c.applied(b.source_sequence, true);
        b.source_sequence
    });
    connect(&mut p);
    assert_eq!(p.counters().batches_lost_kernel_restart, 1);
    assert_eq!(p.publish_scan(&vals(true, 0, 0), None).unwrap(), Publish::FullState { seq: 2 });
    p.drain(Duration::from_secs(5));
    assert_eq!(second.join().unwrap(), 2);
    assert_eq!(p.state(), LinkState::Streaming);
}

#[test]
fn fatal_rejection_voids_later_batches_and_forces_full_state() {
    let k = FakeKernel::new();
    let h = k.serve(WelcomeSpec::default(), |c, _| {
        let b = c.recv_batch();
        c.applied(b.source_sequence, false);
        let _ = c.recv_batch(); // 2
        let _ = c.recv_batch(); // 3
        c.ack(2, AckStatus::Rejected, ErrorCode::TypeMismatch, Continuity::Unspecified, Some(1));
        c.ack(3, AckStatus::Rejected, ErrorCode::SequenceGap, Continuity::Unspecified, Some(1));
        let b = c.recv_batch();
        c.ack(
            b.source_sequence,
            AckStatus::AppliedVolatile,
            ErrorCode::Unspecified,
            Continuity::Resynchronized,
            Some(4),
        );
        b
    });
    let mut cf = cfg(&k);
    cf.retry_backoff.min = Duration::from_millis(1);
    let mut p = SourcePublisher::new(cf, 5).unwrap();
    connect(&mut p);
    p.publish_scan(&vals(false, 0, 0), None).unwrap();
    p.drain(Duration::from_secs(5));
    p.publish_scan(&vals(true, 0, 0), None).unwrap();
    p.publish_scan(&vals(true, 1, 0), None).unwrap();
    p.drain(Duration::from_secs(5));
    assert_eq!(p.state(), LinkState::Recovering);
    assert_eq!(p.pending_len(), 0);
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(p.publish_scan(&vals(true, 1, 0), None).unwrap(), Publish::FullState { seq: 4 });
    p.drain(Duration::from_secs(5));
    let b = h.join().unwrap();
    assert_eq!(b.mode(), BatchMode::FullState);
    assert_eq!(p.counters().rejections_fatal, 1);
    assert_eq!(p.counters().rejections_collateral, 1);
}

#[test]
fn heartbeats_only_when_negotiated() {
    for minor in [0u32, 1] {
        let k = FakeKernel::new();
        let h = k.serve(WelcomeSpec { minor, ..Default::default() }, |c, _| {
            c.s.set_read_timeout(Some(Duration::from_millis(300))).unwrap();
            let mut hb = 0;
            while let Some(m) = c.recv() {
                if let Some(driver_message::Body::Heartbeat(h)) = m.body {
                    hb += 1;
                    assert_eq!(h.sequence, hb);
                }
            }
            hb
        });
        let mut cf = cfg(&k);
        cf.heartbeat_interval = Some(Duration::from_millis(30));
        let mut p = SourcePublisher::new(cf, 5).unwrap();
        connect(&mut p);
        p.pump(Duration::from_millis(200), false);
        p.disconnect("test end");
        let n = h.join().unwrap();
        if minor == 0 { assert_eq!(n, 0) } else { assert!(n >= 3, "{n}") }
    }
}

#[test]
fn incomplete_or_mistyped_scans_are_refused_before_any_io() {
    let k = FakeKernel::new();
    let mut p = SourcePublisher::new(cfg(&k), 5).unwrap();
    assert!(p.publish_scan(&vals(true, 1, 2)[..2], None).is_err());
    assert!(p.publish_scan(&[Value::I64(1), Value::I64(2), Value::I64(3)], None).is_err());
    assert!(matches!(p.publish_scan(&vals(true, 1, 2), None), Err(scada_driver_sdk::SdkError::NotConnected)));
    assert!(SourcePublisher::new(cfg(&k), 0).is_err());
    let mut bad = cfg(&k);
    bad.channels = vec![(2, ChannelType::Bool), (1, ChannelType::Bool)];
    assert!(SourcePublisher::new(bad, 1).is_err());
}
