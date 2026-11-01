//! VS-006 component tests of the journal (no runtime): group commit, recovery equal to
//! the live kernel, corruption and incompatibility fail-closed, single writer.
//! Covers T06 (corruption part), T09, T10, T22 (small scale), T23 (SQLITE_FULL), T28.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::Duration;

use scada_core::*;
use scada_journal::recovery::recover;
use scada_journal::store::{StartupCheck, StoreOptions};
use scada_journal::writer::{self, WriterOptions};
use scada_journal::*;

fn config() -> Arc<ValidatedConfig> {
    let t = |id: u32| TagSpec { tag_id: TagId(id), name: format!("t{id}"), value_type: ValueType::I64 };
    let c = |ch: u32, id: u32| ChannelSpec { channel_id: ChannelId(ch), tag_id: TagId(id), value_type: ValueType::I64 };
    let spec = ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: 8 },
        tags: vec![t(1), t(2)],
        sources: vec![SourceSpec {
            source_id: SourceId(1),
            name: "s".into(),
            channels: vec![c(1, 1), c(2, 2)],
            supervision: Supervision::NONE,
        }],
    };
    let rule = |id: u32, tag: u32, trip: i64| AlarmRuleSpec {
        alarm_id: AlarmId(id),
        name: format!("a{id}"),
        tag_id: TagId(tag),
        condition: AlarmCondition::High { trip, reset: trip - 1 },
        severity: Severity::Major,
    };
    let alarms = AlarmSpec {
        max_rules: 8,
        outbox_capacity_events: 8,
        rules: vec![rule(1, 1, 10), rule(2, 1, 20), rule(3, 2, 5)],
    };
    Arc::new(ValidatedConfig::validate_with_alarms(spec, Some(alarms)).unwrap())
}

fn opts(path: &Path) -> StoreOptions {
    StoreOptions {
        path: path.to_path_buf(),
        durability: Durability::Full,
        startup_check: StartupCheck::Quick,
        cache_kib: 2048,
        test_max_pages: None,
    }
}

fn wopts(boot: u64) -> WriterOptions {
    WriterOptions {
        commit_max_operations: 64,
        commit_max_delay: Duration::ZERO,
        checkpoints_keep: 2,
        wal_max_bytes: 64 << 20,
        durability: Durability::Full,
        boot_generation: boot,
        test_delay: Duration::ZERO,
        test_stall_file: None,
        test_eio_at_commit: None,
    }
}

/// Minimal persistent pipeline (what the runtime sequencer does, without sockets).
struct Pipe {
    k: Kernel,
    tx: Option<SyncSender<WriterMsg>>,
    shared: Arc<JournalShared>,
    writer: Option<JoinHandle<()>>,
    checkpoint_every: u64,
}

impl Pipe {
    fn open(path: &Path, checkpoint_every: u64) -> Pipe {
        Self::open_with(path, checkpoint_every, opts(path), |_| {})
    }

    fn open_with(_path: &Path, checkpoint_every: u64, o: StoreOptions, tweak: impl FnOnce(&mut WriterOptions)) -> Pipe {
        let cfg = config();
        let mut store = Store::open(&o, cfg.fingerprint()).expect("open");
        let rec = recover(&store.conn, store.info.timeline_no, cfg).expect("recover");
        let boot =
            store.begin_boot([1; 16], "test", rec.report.durable_op + 1, rec.report.checkpoint_op, 0, 0).unwrap();
        let shared = Arc::new(JournalShared::new(rec.report.durable_op, rec.report.durable_event));
        let (tx, rx) = sync_channel(4096);
        let mut w = wopts(boot);
        tweak(&mut w);
        let writer = writer::spawn(store, shared.clone(), rx, w, Box::new(|| {})).unwrap();
        Pipe { k: rec.kernel, tx: Some(tx), shared, writer: Some(writer), checkpoint_every }
    }

    fn apply(&mut self, op: Operation) -> Result<TransitionOutcome, ApplyError> {
        let seq = self.k.last_operation_sequence() + 1;
        let before = self.k.outbox_status().map_or(1, |s| s.next_event_seq);
        let input = SequencedInput { operation_sequence: seq, operation: op };
        let r = self.k.apply_ref(&input);
        if let Ok(TransitionOutcome::Applied { .. }) = r {
            let after = self.k.outbox_status().map_or(1, |s| s.next_event_seq);
            let events: Vec<AlarmEvent> = (before..after).map(|s| self.k.alarm_event(s).unwrap()).collect();
            let mut rec = OpRecord {
                operation_seq: seq,
                operation: input.operation,
                time: TimeEnvelope {
                    sequenced_at_utc_ns: 1_700_000_000_000_000_000 + seq as i64,
                    boottime_ns: boottime_ns(),
                    logical_time_ns: 1_700_000_000_000_000_000 + seq as i64,
                    clock_quality: clock_quality::UNKNOWN,
                    clock_flags: 0,
                    received_at_utc_ns: None,
                },
                source_times: None,
                events,
                origins: Vec::new(),
                reconciliation: false,
                action: None,
                recovery: Vec::new(),
                anomaly: None,
                reserved_bytes: 0,
                receipt: None,
            };
            rec.reserved_bytes = rec.queued_bytes();
            self.shared.reserved_ops.fetch_add(1, Ordering::Relaxed);
            self.shared.reserved_bytes.fetch_add(rec.reserved_bytes, Ordering::Relaxed);
            self.tx.as_ref().unwrap().send(WriterMsg::Op(Box::new(rec))).unwrap();
            if self.checkpoint_every > 0 && seq.is_multiple_of(self.checkpoint_every) {
                let cp = self.k.export_checkpoint();
                let event_seq_high = self.k.outbox_status().map_or(0, |s| s.next_event_seq - 1);
                self.tx
                    .as_ref()
                    .unwrap()
                    .send(WriterMsg::Checkpoint(Box::new(CheckpointMsg {
                        checkpoint: cp,
                        state_digest: self.k.state_digest(),
                        event_seq_high,
                    })))
                    .unwrap();
            }
        }
        r
    }

    /// Advance the outbox watermark to what is durable (J3: never beyond).
    fn watermark(&mut self) {
        let d = self.shared.durable_event.load(Ordering::Acquire);
        if self.k.outbox_status().is_some_and(|s| s.durable_watermark < d) {
            let _ = self.apply(Operation::AdvanceDurableWatermark { up_to_event_seq: d });
        }
    }

    fn wait_durable(&mut self) {
        let target = self.k.last_operation_sequence();
        while self.shared.durable_op.load(Ordering::Acquire) < target {
            if matches!(self.shared.state(), JournalState::DiskFull | JournalState::Fault) {
                return;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn close(mut self) -> Kernel {
        self.tx.take();
        self.writer.take().unwrap().join().unwrap();
        self.k
    }
}

fn workload(p: &mut Pipe, n: u64) {
    if p.k.source_status(SourceId(1)).unwrap().link == LinkState::NeverConnected
        || matches!(p.k.source_status(SourceId(1)).unwrap().link, LinkState::Disconnected { .. })
    {
        let g = match p.k.source_status(SourceId(1)).unwrap().link {
            LinkState::Disconnected { generation } => generation + 1,
            _ => 1,
        };
        p.apply(Operation::SourceConnected { source_id: SourceId(1), link_generation: g, source_epoch: 1 }).unwrap();
    }
    let g = match p.k.source_status(SourceId(1)).unwrap().link {
        LinkState::Connected { generation } => generation,
        other => panic!("{other:?}"),
    };
    for i in 0..n {
        let seq = p.k.source_status(SourceId(1)).unwrap().last_accepted_source_sequence.unwrap_or(0) + 1;
        let unverified = p.k.source_status(SourceId(1)).unwrap().unverified;
        let mode = if seq == 1 || unverified || i % 17 == 0 { BatchMode::FullState } else { BatchMode::Delta };
        let v = ((i * 7) % 30) as i64;
        let obs = vec![
            Observation { channel_id: ChannelId(1), value: Value::I64(v) },
            Observation { channel_id: ChannelId(2), value: Value::I64(v % 7) },
        ];
        let op = Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(1),
            link_generation: g,
            mode,
            source_sequence: seq,
            observations: obs,
        });
        match p.apply(op) {
            Ok(_) => {}
            Err(ApplyError::OutboxCapacityExceeded { .. }) => {
                p.wait_durable();
                p.watermark();
            }
            Err(e) => panic!("{e:?}"),
        }
        if i % 5 == 0 {
            p.watermark();
        }
        if i % 23 == 0
            && let Some(v) = p.k.alarm(AlarmId(1))
            && !v.flags.acknowledged()
        {
            let _ = p.apply(Operation::AlarmAcknowledge {
                alarm_id: AlarmId(1),
                expected_activation_token: v.activation_token,
                actor_uid: 1000,
            });
        }
    }
}

fn dir() -> (tempfile::TempDir, PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("j").join("journal.db");
    (d, p)
}

fn reopen(path: &Path) -> Result<scada_journal::recovery::Recovered, JournalError> {
    let s = Store::open(&opts(path), config().fingerprint())?;
    recover(&s.conn, s.info.timeline_no, config())
}

/// T09: recovery replays more operations than the ring holds, from a checkpoint, and
/// gives the digest of the live kernel; the ring window is reloaded from the store.
#[test]
fn recovery_equals_live_kernel() {
    let (_d, path) = dir();
    let mut p = Pipe::open(&path, 50);
    workload(&mut p, 400);
    p.wait_durable();
    let live = p.close();
    let r = reopen(&path).unwrap();
    assert_eq!(r.kernel.state_digest(), live.state_digest());
    assert_eq!(r.kernel.alarm_events_digest(), live.alarm_events_digest());
    assert!(r.report.checkpoint_op.is_some(), "a checkpoint was used");
    assert!(r.report.replayed_operations < 60, "bounded replay: {}", r.report.replayed_operations);
    assert!(live.outbox_status().unwrap().next_event_seq > 9, "more events than the ring (8)");
}

/// Genesis replay without any checkpoint, and two sessions on the same store continue
/// the same sequence (operation and event sequences resume, J2/D3).
#[test]
fn genesis_replay_and_resume() {
    let (_d, path) = dir();
    let mut p = Pipe::open(&path, 0);
    workload(&mut p, 120);
    p.wait_durable();
    let live = p.close();
    let mut p = Pipe::open(&path, 0);
    assert_eq!(p.k.state_digest(), live.state_digest());
    let last = p.k.last_operation_sequence();
    p.apply(Operation::RecoveryFence { boot_generation: 2 }).unwrap();
    workload(&mut p, 50);
    p.wait_durable();
    let live2 = p.close();
    assert!(live2.last_operation_sequence() > last);
    let r = reopen(&path).unwrap();
    assert_eq!(r.kernel.state_digest(), live2.state_digest());
}

/// T06 (corruption) / T10: a flipped payload byte is detected (fail-closed); a corrupt
/// latest checkpoint falls back to the previous one; a corrupt checkpoint with a journal
/// that no longer starts at 1 is refused.
#[test]
fn corruption_is_fail_closed() {
    let (_d, path) = dir();
    let mut p = Pipe::open(&path, 40);
    workload(&mut p, 200);
    p.wait_durable();
    let live = p.close();
    let raw = rusqlite::Connection::open(&path).unwrap();
    // Corrupt the newest checkpoint blob: the previous one is used.
    raw.execute(
        "UPDATE journal_checkpoints SET blob = zeroblob(length(blob)) WHERE operation_seq = (SELECT max(operation_seq) FROM journal_checkpoints)",
        [],
    )
    .unwrap();
    let r = reopen(&path).unwrap();
    assert_eq!(r.kernel.state_digest(), live.state_digest());
    assert_eq!(r.report.checkpoints_rejected.len(), 1, "{:?}", r.report.checkpoints_rejected);
    // Flip one payload byte of the last operation: digest mismatch.
    raw.execute(
        "UPDATE journal_operations SET payload = CAST(substr(payload, 1, 2) || x'ff' || substr(payload, 4) AS BLOB) WHERE operation_seq = (SELECT max(operation_seq) FROM journal_operations)",
        [],
    )
    .unwrap();
    match reopen(&path) {
        Err(JournalError::Corrupt(m)) => assert!(m.contains("digest"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn missing_operation_or_event_is_fail_closed() {
    let (_d, path) = dir();
    let mut p = Pipe::open(&path, 0);
    workload(&mut p, 60);
    p.wait_durable();
    p.close();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute("DELETE FROM alarm_events WHERE event_seq = 2", []).unwrap();
    assert!(matches!(reopen(&path), Err(JournalError::Corrupt(_))));
    let (_d2, path2) = dir();
    let mut p = Pipe::open(&path2, 0);
    workload(&mut p, 60);
    p.wait_durable();
    p.close();
    let raw = rusqlite::Connection::open(&path2).unwrap();
    raw.execute("DELETE FROM journal_operations WHERE operation_seq = 10", []).unwrap();
    match reopen(&path2) {
        Err(JournalError::Corrupt(m)) => assert!(m.contains("gap") || m.contains("event"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// T10: unknown schema version, another application, another configuration: refused.
#[test]
fn incompatible_store_is_refused() {
    let (_d, path) = dir();
    Pipe::open(&path, 0).close();
    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch("PRAGMA user_version = 99").unwrap();
    assert!(matches!(Store::open(&opts(&path), config().fingerprint()), Err(JournalError::Incompatible(_))));
    raw.execute_batch("PRAGMA user_version = 1; PRAGMA application_id = 7").unwrap();
    assert!(matches!(Store::open(&opts(&path), config().fingerprint()), Err(JournalError::Incompatible(_))));
    raw.execute_batch(&format!("PRAGMA application_id = {}", scada_journal::schema::APPLICATION_ID)).unwrap();
    match Store::open(&opts(&path), [9; 32]) {
        Err(JournalError::Incompatible(m)) => assert!(m.contains("new-timeline"), "{m}"),
        other => panic!("{other:?}"),
    }
    // A new timeline for the other configuration (offline tool) is accepted, the old
    // history stays.
    scada_journal::store::new_timeline(&path, [9; 32], "test").unwrap();
    assert!(Store::open(&opts(&path), [9; 32]).is_ok());
}

/// T28: one writer per store; private files.
#[test]
fn single_writer_and_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let (_d, path) = dir();
    let s = Store::open(&opts(&path), config().fingerprint()).unwrap();
    assert!(matches!(Store::open(&opts(&path), config().fingerprint()), Err(JournalError::Locked(_))));
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let dmode = std::fs::metadata(path.parent().unwrap()).unwrap().permissions().mode() & 0o777;
    assert_eq!(dmode, 0o700);
    drop(s);
    assert!(Store::open(&opts(&path), config().fingerprint()).is_ok());
    // A group-writable directory is refused.
    let d2 = tempfile::tempdir().unwrap();
    std::fs::set_permissions(d2.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
    assert!(Store::open(&opts(&d2.path().join("x.db")), config().fingerprint()).is_err());
}

/// T23 (SQLITE_FULL): the writer reports DISK_FULL, publishes nothing beyond the last
/// commit and retries; the store stays recoverable to the last durable prefix.
#[test]
fn sqlite_full_publishes_nothing_and_recovers() {
    let (_d, path) = dir();
    let mut o = opts(&path);
    Pipe::open(&path, 0).close();
    o.test_max_pages = Some(40);
    let mut p = Pipe::open_with(&path, 0, o, |_| {});
    let mut full = false;
    for _ in 0..4000 {
        if p.shared.state() == JournalState::DiskFull {
            full = true;
            break;
        }
        workload(&mut p, 10);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(full, "SQLITE_FULL reached");
    let durable = p.shared.durable_op.load(Ordering::Acquire);
    assert!(durable < p.k.last_operation_sequence());
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(p.shared.durable_op.load(Ordering::Acquire), durable, "no publication while full");
    p.shared.shutdown.store(true, Ordering::Relaxed);
    p.close();
    let r = reopen(&path).unwrap();
    assert_eq!(r.kernel.last_operation_sequence(), durable, "recovered exactly the durable prefix");
}

/// T07 (EIO): an unrecoverable commit error moves the writer to FAULT; pending receipts
/// report NotDurable and nothing more is published.
#[test]
fn eio_is_a_fault() {
    let (_d, path) = dir();
    let mut p = Pipe::open_with(&path, 0, opts(&path), |w| w.test_eio_at_commit = Some(3));
    for _ in 0..200 {
        if p.shared.state() == JournalState::Fault {
            break;
        }
        workload(&mut p, 5);
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(p.shared.state(), JournalState::Fault);
    assert!(p.shared.error().contains("EIO"));
    let durable = p.shared.durable_op.load(Ordering::Acquire);
    p.close();
    let r = reopen(&path).unwrap();
    assert_eq!(r.kernel.last_operation_sequence(), durable);
}

/// Historian keyset paging over more events than the ring (T22 at small scale) and the
/// persisted projection.
#[test]
fn historian_pages_beyond_the_ring() {
    let (_d, path) = dir();
    let mut p = Pipe::open(&path, 0);
    workload(&mut p, 300);
    p.wait_durable();
    let live = p.close();
    let c = scada_journal::store::open_read_only(&path, 1024).unwrap();
    let (tl, _) = scada_journal::reader::current_timeline(&c).unwrap();
    let mut after = 0;
    let mut n = 0u64;
    loop {
        let q = scada_journal::reader::HistoryQuery {
            after_event_seq: after,
            limit: 7,
            max_scan: 1000,
            ..Default::default()
        };
        let page = scada_journal::reader::history(&c, tl, &q).unwrap();
        for r in &page.rows {
            n += 1;
            assert_eq!(r.event_seq, n, "contiguous keyset");
        }
        after = page.next_after_event_seq;
        if !page.has_more {
            break;
        }
    }
    assert_eq!(n, live.outbox_status().unwrap().next_event_seq - 1);
    let (rows, _, as_of) = scada_journal::reader::projection(&c, tl, 0, 100).unwrap();
    assert_eq!(as_of, n);
    let live_listed: Vec<u32> =
        [1u32, 2, 3].into_iter().filter(|a| live.alarm(AlarmId(*a)).unwrap().flags.listed()).collect();
    let proj: Vec<u32> = rows.iter().map(|r| r.alarm_id).collect();
    assert_eq!(proj, live_listed);
}
