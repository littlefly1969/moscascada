//! EXP-001 (docs/experiments/exp-001-open-wal-backend.md §4): correctness of the
//! experimental open-wal journal against the VS-006 SQLite journal, **before** any
//! performance claim. Only built with `--features experimental-open-wal`.
//!
//! - T01 equivalence: the same canonical stream (events, ACK with operator action,
//!   event-less status changes after a RecoveryFence, source recovery with gaps, source
//!   timestamps, clock anomalies with -1 s / -1 h / +24 h steps) through A (SQLite) and
//!   C (open-wal + derived SQL projection) gives identical state digest, watermarks,
//!   historian pages, projections and tables; recovery of both gives the live digest.
//! - T02/T03 crash at every fault point and random SIGKILL (child process): no
//!   acknowledged operation lost, recovered state = the state the child had applied at
//!   that operation, no event duplicated.
//! - T03 corruption: torn tail, bit flips mid-log, truncated / deleted segments,
//!   damaged checkpoints (fallback), damaged MANIFEST, other configuration, second writer.
//! - T04 simulated EIO and a **real** I/O failure (`RLIMIT_FSIZE`, segment preallocation
//!   refused at a roll): FAULT, poisoned handle, durable prefix only, recovery.
//! - T05 fan-out 0..10k events in one record; an operation over `max_record_size` is
//!   refused before apply by `worst_case_len`, and a writer that meets one faults without
//!   writing any part of it.
//! - T08 projection rebuilt from the log keeps `status_after` changed without event.
//! - T10 a log that no longer starts at LSN 1 is refused (retention disabled).
//! - T11 random byte corruption of segments: recovery never panics, either fails closed
//!   or yields a prefix equal to the reference state at that operation.
#![cfg(feature = "experimental-open-wal")]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;
use std::time::Duration;

use scada_core::*;
use scada_journal::owal::projector::Projector;
use scada_journal::owal::writer::{self as owriter, OwalWriterOptions};
use scada_journal::owal::{self, OwalOptions, OwalStore, envelope};
use scada_journal::reader::{self, HistoryQuery};
use scada_journal::recovery::recover;
use scada_journal::store::{StartupCheck, StoreOptions};
use scada_journal::writer::{self, WriterOptions};
use scada_journal::*;

const ALARMS: u32 = 3;

type Trace = Box<dyn FnMut(u64, &Kernel)>;

fn config_with(ring: u64) -> Arc<ValidatedConfig> {
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
        outbox_capacity_events: ring,
        rules: vec![rule(1, 1, 10), rule(2, 1, 20), rule(3, 2, 5)],
    };
    Arc::new(ValidatedConfig::validate_with_alarms(spec, Some(alarms)).unwrap())
}

fn sqlite_opts(path: &Path) -> StoreOptions {
    StoreOptions {
        path: path.to_path_buf(),
        durability: Durability::Full,
        startup_check: StartupCheck::Quick,
        cache_kib: 2048,
        test_max_pages: None,
    }
}

fn small_wal() -> OwalOptions {
    // Small segments force rolls (and commit splits) in every test.
    OwalOptions { segment_size: 64 * 1024, max_record_size: 16 * 1024 }
}

enum Backend {
    Sqlite(PathBuf),
    /// open-wal dir, optional projection file.
    Owal(PathBuf, Option<PathBuf>, OwalOptions),
}

/// Minimal persistent pipeline (what the runtime sequencer does, without sockets),
/// generic over the journal backend. Annotations are synthetic and deterministic so the
/// two backends receive byte-identical records.
struct Pipe {
    k: Kernel,
    tx: Option<SyncSender<WriterMsg>>,
    shared: Arc<JournalShared>,
    writer: Option<JoinHandle<()>>,
    projector: Option<JoinHandle<Projector>>,
    checkpoint_every: u64,
    boot: u64,
    clock: i64,
    /// Every applied operation's digest (for crash children).
    trace: Option<Trace>,
    /// Advance the outbox watermark only after a full wait (timing-independent stream).
    det: bool,
}

fn recover_owal(
    dir: &Path,
    o: OwalOptions,
    cfg: Arc<ValidatedConfig>,
) -> Result<(OwalStore, open_wal::Wal, owal::Recovered), JournalError> {
    let (st, wal) = OwalStore::open(dir, o, cfg.fingerprint())?;
    let r = owal::recover(&st, &wal, cfg)?;
    Ok((st, wal, r))
}

impl Pipe {
    fn open(b: &Backend, cfg: Arc<ValidatedConfig>, checkpoint_every: u64) -> Pipe {
        Self::open_with(b, cfg, checkpoint_every, 64, |_| {})
    }

    fn open_with(
        b: &Backend,
        cfg: Arc<ValidatedConfig>,
        checkpoint_every: u64,
        commit_max: usize,
        tweak: impl FnOnce(&mut OwalWriterOptions),
    ) -> Pipe {
        let (tx, rx) = sync_channel(4096);
        match b {
            Backend::Sqlite(path) => {
                let mut store = Store::open(&sqlite_opts(path), cfg.fingerprint()).expect("open");
                let rec = recover(&store.conn, store.info.timeline_no, cfg.clone()).expect("recover");
                let boot = store
                    .begin_boot([1; 16], "test", rec.report.durable_op + 1, rec.report.checkpoint_op, 0, 0)
                    .unwrap();
                let shared = Arc::new(JournalShared::new(rec.report.durable_op, rec.report.durable_event));
                let w = WriterOptions {
                    commit_max_operations: commit_max,
                    commit_max_delay: Duration::ZERO,
                    checkpoints_keep: 2,
                    compact_replay_payloads: false,
                    wal_max_bytes: 64 << 20,
                    durability: Durability::Full,
                    boot_generation: boot,
                    test_delay: Duration::ZERO,
                    test_stall_file: None,
                    test_eio_at_commit: None,
                };
                let writer = writer::spawn(store, shared.clone(), rx, w, Box::new(|| {})).unwrap();
                Pipe::new(rec.kernel, tx, shared, writer, None, checkpoint_every, boot)
            }
            Backend::Owal(dir, proj, o) => {
                let (st, wal, rec) = recover_owal(dir, *o, cfg.clone()).expect("recover owal");
                let boot = rec.position.last_boot_generation + 1;
                let shared = Arc::new(JournalShared::new(rec.position.durable_op, rec.position.durable_event));
                let (ptx, projector) = match proj {
                    Some(p) => {
                        let mut pr = match Projector::open(
                            p,
                            Durability::NormalBenchmarkOnly,
                            cfg.fingerprint(),
                            st.manifest.timeline_id,
                        ) {
                            Ok(pr) => pr,
                            Err(JournalError::Incompatible(_)) => Projector::rebuild(
                                p,
                                Durability::NormalBenchmarkOnly,
                                cfg.fingerprint(),
                                st.manifest.timeline_id,
                            )
                            .unwrap(),
                            Err(e) => panic!("{e}"),
                        };
                        pr.catch_up(&wal, 512).expect("catch up");
                        let (ptx, prx) = sync_channel(64);
                        (Some(ptx), Some(pr.spawn(prx, 4096).unwrap()))
                    }
                    None => (None, None),
                };
                let mut w = OwalWriterOptions {
                    commit_max_operations: commit_max,
                    commit_max_delay: Duration::ZERO,
                    checkpoints_keep: 2,
                    boot_generation: boot,
                    last_logical_time_ns: rec.position.last_logical_time_ns,
                    config: cfg.clone(),
                    test_eio_at_commit: None,
                    test_delay: Duration::ZERO,
                };
                tweak(&mut w);
                let seed = owriter::ring_seed(&rec.kernel);
                let writer = owriter::spawn(st, wal, shared.clone(), rx, w, seed, ptx, Box::new(|| {})).unwrap();
                Pipe::new(rec.kernel, tx, shared, writer, projector, checkpoint_every, boot)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        k: Kernel,
        tx: SyncSender<WriterMsg>,
        shared: Arc<JournalShared>,
        writer: JoinHandle<()>,
        projector: Option<JoinHandle<Projector>>,
        checkpoint_every: u64,
        boot: u64,
    ) -> Pipe {
        let clock = 1_700_000_000_000_000_000 + k.last_operation_sequence() as i64 * 1_000_000;
        Pipe {
            k,
            tx: Some(tx),
            shared,
            writer: Some(writer),
            projector,
            checkpoint_every,
            boot,
            clock,
            trace: None,
            det: true,
        }
    }

    fn flags(&self) -> Vec<u8> {
        (1..=ALARMS).map(|a| self.k.alarm(AlarmId(a)).map_or(0, |v| v.flags.0)).collect()
    }

    /// Apply + journal one operation with full metadata (what the runtime attaches).
    fn apply(&mut self, op: Operation) -> Result<TransitionOutcome, ApplyError> {
        let seq = self.k.last_operation_sequence() + 1;
        let before = self.k.outbox_status().map_or(1, |s| s.next_event_seq);
        let flags_before = self.flags();
        let input = SequencedInput { operation_sequence: seq, operation: op };
        let r = self.k.apply_ref(&input);
        if let Ok(TransitionOutcome::Applied { .. }) = r {
            let after = self.k.outbox_status().map_or(1, |s| s.next_event_seq);
            let events: Vec<AlarmEvent> = (before..after).map(|s| self.k.alarm_event(s).unwrap()).collect();
            let flags_after = self.flags();
            let alarm_updates: Vec<(u32, u8)> = (1..=ALARMS)
                .filter(|a| {
                    let i = (*a - 1) as usize;
                    flags_before[i] != flags_after[i] && !events.iter().any(|e| e.alarm_id.0 == *a)
                })
                .map(|a| (a, flags_after[(a - 1) as usize]))
                .collect();
            // Synthetic clock with steps: -1 s, -1 h, +24 h (T09 annotations only).
            let (step, anomaly) = match seq % 97 {
                13 => (-1_000_000_000, Some(TimeAnomaly { kind: 1, delta_ns: -1_000_000_000 })),
                41 => (-3_600_000_000_000, Some(TimeAnomaly { kind: 1, delta_ns: -3_600_000_000_000 })),
                77 => (86_400_000_000_000, Some(TimeAnomaly { kind: 2, delta_ns: 86_400_000_000_000 })),
                _ => (1_000_000, None),
            };
            self.clock += step;
            let logical = self.clock.max(1_700_000_000_000_000_000);
            let action = if let Operation::AlarmAcknowledge { alarm_id, expected_activation_token, actor_uid } =
                &input.operation
            {
                let mut id = [0u8; 16];
                id[..8].copy_from_slice(&seq.to_le_bytes());
                Some(OperatorAction {
                    action_id: id,
                    alarm_id: alarm_id.0,
                    requested_token: *expected_activation_token,
                    actor_uid: *actor_uid,
                    note: format!("ack op {seq} — nota"),
                    outcome: if events.is_empty() { 2 } else { 1 },
                    event_seq: events.first().map(|e| e.event_seq),
                    occurrence_id: self.k.alarm(*alarm_id).map_or(0, |v| v.occurrence_id),
                })
            } else {
                None
            };
            let (source_times, recovery, origins) = match &input.operation {
                Operation::ObservationBatch(b) => {
                    let st = seq.is_multiple_of(3).then(|| {
                        Box::new(SourceTimes {
                            acquired_at_utc_ns: Some(self.clock - 5_000),
                            clock_quality: clock_quality::SYNCHRONIZED,
                            clock_id: "ptp-grandmaster".into(),
                            observations: b.observations.iter().map(|o| (o.channel_id.0, self.clock - 7_000)).collect(),
                        })
                    });
                    let rec = if b.mode == BatchMode::FullState {
                        vec![SourceRecoveryUpdate {
                            source_id: b.source_id.0,
                            source_epoch: Some(1),
                            live: live::LIVE_VERIFIED,
                            history: if seq.is_multiple_of(2) { continuity::COMPLETE } else { continuity::GAP },
                            last_source_sequence: Some(b.source_sequence),
                            verified_at_operation_seq: seq,
                            gap: (seq % 2 == 1).then_some(GapRecord {
                                source_epoch: Some(1),
                                from_seq: Some(b.source_sequence.saturating_sub(3)),
                                to_seq: None,
                                bounds_unknown: seq % 4 == 1,
                                reason: gap_reason::SEQUENCE_GAP,
                            }),
                        }]
                    } else {
                        Vec::new()
                    };
                    let origins = events
                        .iter()
                        .map(|_| EventOrigin {
                            utc_ns: Some(self.clock - 7_000),
                            source: time_source::DEVICE,
                            clock_quality: 2,
                        })
                        .collect();
                    (st, rec, origins)
                }
                _ => (None, Vec::new(), Vec::new()),
            };
            let mut rec = OpRecord {
                operation_seq: seq,
                operation: input.operation,
                time: TimeEnvelope {
                    sequenced_at_utc_ns: self.clock,
                    boottime_ns: seq * 1_000,
                    logical_time_ns: logical,
                    clock_quality: clock_quality::HOLDOVER,
                    clock_flags: if anomaly.is_some() { clock_flags::REGRESSED } else { 0 },
                    received_at_utc_ns: Some(self.clock - 1_000),
                },
                source_times,
                events,
                origins,
                reconciliation: false,
                action,
                recovery,
                alarm_updates,
                anomaly,
                reserved_bytes: 0,
                receipt: None,
            };
            rec.reserved_bytes = rec.queued_bytes();
            self.shared.reserved_ops.fetch_add(1, Ordering::Relaxed);
            self.shared.reserved_bytes.fetch_add(rec.reserved_bytes, Ordering::Relaxed);
            // Traced before the writer can see it (a crash child prints the applied
            // state before the operation can become durable).
            if let Some(t) = self.trace.as_mut() {
                t(seq, &self.k);
            }
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

    fn watermark(&mut self) {
        if self.det {
            // Deterministic stream: the watermark value must not depend on timing.
            self.wait_durable();
        }
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

    fn fence(&mut self) {
        let b = self.boot;
        self.apply(Operation::RecoveryFence { boot_generation: b }).unwrap();
    }

    /// Close the writer (and the projector) and return the kernel.
    fn close(mut self) -> (Kernel, Option<Projector>) {
        self.tx.take();
        self.writer.take().unwrap().join().unwrap();
        let p = self.projector.take().map(|h| h.join().unwrap());
        (self.k, p)
    }
}

fn workload(p: &mut Pipe, n: u64) {
    let link = p.k.source_status(SourceId(1)).unwrap().link;
    if matches!(link, LinkState::NeverConnected | LinkState::Disconnected { .. }) {
        let g = match link {
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
        let st = p.k.source_status(SourceId(1)).unwrap();
        let seq = st.last_accepted_source_sequence.unwrap_or(0) + 1;
        let mode = if seq == 1 || st.unverified || i % 17 == 0 { BatchMode::FullState } else { BatchMode::Delta };
        let v = ((i * 7) % 30) as i64;
        let op = Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(1),
            link_generation: g,
            mode,
            source_sequence: seq,
            observations: vec![
                Observation { channel_id: ChannelId(1), value: Value::I64(v) },
                Observation { channel_id: ChannelId(2), value: Value::I64(v % 7) },
            ],
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

fn err_of<T>(r: Result<T, JournalError>) -> JournalError {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    }
}

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn private(d: &Path, name: &str) -> PathBuf {
    use std::os::unix::fs::DirBuilderExt;
    let p = d.join(name);
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&p).unwrap();
    p
}

/// Deterministic workload with no timing dependency (ring large enough: the watermark
/// is only advanced at fixed points after a full wait).
fn det_workload(p: &mut Pipe, n: u64) {
    workload(p, n);
    p.wait_durable();
    p.watermark();
    p.wait_durable();
}

// ---- table comparison helpers ----

fn dump(c: &rusqlite::Connection, tl: i64, sql: &str) -> Vec<String> {
    let mut st = c.prepare(sql).unwrap();
    let n = st.column_count();
    let rows = st
        .query_map([tl], |r| {
            let mut s = String::new();
            for i in 0..n {
                let v: rusqlite::types::Value = r.get(i)?;
                s.push_str(&format!("{v:?}|"));
            }
            Ok(s)
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

fn tables(c: &rusqlite::Connection, tl: i64) -> BTreeMap<&'static str, Vec<String>> {
    let q: &[(&str, &str)] = &[
        (
            "journal_operations",
            "SELECT operation_seq, kind, payload, payload_digest, first_event_seq, event_count, boot_generation, sequenced_at_utc_ns, boottime_ns, logical_time_ns, clock_quality, clock_flags, received_at_utc_ns, source_times, reconciliation FROM journal_operations WHERE timeline_no = ?1 ORDER BY operation_seq",
        ),
        (
            "alarm_events",
            "SELECT event_seq, operation_seq, ordinal, alarm_id, occurrence_id, activation_token, kind, status_after, actor_uid, reconciliation, origin_utc_ns, origin_source, origin_clock FROM alarm_events WHERE timeline_no = ?1 ORDER BY event_seq",
        ),
        (
            "alarm_active_projection",
            "SELECT alarm_id, status_after, occurrence_id, activation_token, last_event_seq, last_kind FROM alarm_active_projection WHERE timeline_no = ?1 ORDER BY alarm_id",
        ),
        ("projection_meta", "SELECT as_of_event_seq, as_of_operation_seq FROM projection_meta WHERE timeline_no = ?1"),
        (
            "operator_actions",
            "SELECT action_id, operation_seq, event_seq, alarm_id, occurrence_id, requested_token, actor_uid, note, outcome FROM operator_actions WHERE timeline_no = ?1 ORDER BY action_id",
        ),
        (
            "source_recovery",
            "SELECT source_id, source_epoch, boot_generation, live_verification, history_continuity, last_verified_operation_seq, last_source_sequence, updated_operation_seq FROM source_recovery WHERE timeline_no = ?1 ORDER BY source_id",
        ),
        (
            "source_history_gaps",
            "SELECT source_id, source_epoch, from_seq, to_seq, recorded_operation_seq, bounds_unknown, reason, boot_generation FROM source_history_gaps WHERE timeline_no = ?1 ORDER BY gap_id",
        ),
        (
            "time_observations",
            "SELECT operation_seq, kind, delta_ns, realtime_ns, boottime_ns, boot_generation FROM time_observations WHERE timeline_no = ?1 ORDER BY operation_seq, kind",
        ),
    ];
    q.iter().map(|(n, s)| (*n, dump(c, tl, s))).collect()
}

fn history_all(c: &rusqlite::Connection, tl: i64) -> Vec<reader::HistoryRow> {
    let mut out = Vec::new();
    let mut after = 0;
    loop {
        let p = reader::history(
            c,
            tl,
            &HistoryQuery { after_event_seq: after, limit: 7, max_scan: 1000, ..Default::default() },
        )
        .unwrap();
        for mut r in p.rows {
            r.durable_at_utc_ns = None; // wall clock of the commit: backend specific
            out.push(r);
        }
        if !p.has_more {
            break;
        }
        after = p.next_after_event_seq;
    }
    out
}

fn sqlite_tl(c: &rusqlite::Connection) -> i64 {
    reader::current_timeline(c).unwrap().0
}

// ---- T01 ----

#[test]
fn t01_sqlite_and_owal_projection_are_equivalent() {
    let d = tmp();
    let a_path = private(d.path(), "a").join("journal.db");
    let c_dir = private(d.path(), "c").join("owal");
    let c_proj = private(d.path(), "cp").join("projection.db");
    let a = Backend::Sqlite(a_path.clone());
    let c = Backend::Owal(c_dir.clone(), Some(c_proj.clone()), small_wal());
    let cfg = config_with(64);
    let mut digests = Vec::new();
    // Two boots: the second starts with a RecoveryFence (input invalid without event).
    for boot in 0..2 {
        let mut pa = Pipe::open(&a, cfg.clone(), 50);
        let mut pc = Pipe::open(&c, cfg.clone(), 50);
        assert_eq!(pa.k.state_digest(), pc.k.state_digest(), "recovered state equal at boot {boot}");
        if boot == 1 {
            pa.fence();
            pc.fence();
        }
        det_workload(&mut pa, 400);
        det_workload(&mut pc, 400);
        assert_eq!(pa.k.state_digest(), pc.k.state_digest());
        assert_eq!(pa.shared.durable_op.load(Ordering::Acquire), pc.shared.durable_op.load(Ordering::Acquire));
        assert_eq!(pa.shared.durable_event.load(Ordering::Acquire), pc.shared.durable_event.load(Ordering::Acquire));
        let (ka, _) = pa.close();
        let (kc, pr) = pc.close();
        let pr = pr.unwrap();
        assert_eq!(pr.as_of_op, kc.last_operation_sequence(), "projection caught up");
        digests.push((ka.state_digest(), kc.state_digest(), kc.last_operation_sequence()));
    }
    // Event-less status changes exist in the stream (RecoveryFence), else T08 is vacuous.
    let ca = rusqlite::Connection::open(&a_path).unwrap();
    let cc = rusqlite::Connection::open(&c_proj).unwrap();
    let (ta, tc) = (sqlite_tl(&ca), sqlite_tl(&cc));
    let (xa, xc) = (tables(&ca, ta), tables(&cc, tc));
    for (name, rows) in &xa {
        assert!(!rows.is_empty() || *name == "time_observations", "{name} empty: workload too weak");
        assert_eq!(rows, &xc[name], "table {name} differs between A and C");
    }
    assert_eq!(history_all(&ca, ta), history_all(&cc, tc), "historian pages differ");
    assert_eq!(reader::projection(&ca, ta, 0, 100).unwrap(), reader::projection(&cc, tc, 0, 100).unwrap());
    assert_eq!(reader::as_of(&ca, ta).unwrap(), reader::as_of(&cc, tc).unwrap());
    drop((ca, cc));
    // Recovery from each authority equals the live kernel.
    let (_s, _w, rc) = recover_owal(&c_dir, small_wal(), cfg.clone()).unwrap();
    let sa = Store::open(&sqlite_opts(&a_path), cfg.fingerprint()).unwrap();
    let ra = recover(&sa.conn, sa.info.timeline_no, cfg.clone()).unwrap();
    let (da, dc, last) = digests[1];
    assert_eq!(da, dc);
    assert_eq!(rc.kernel.state_digest(), dc);
    assert_eq!(ra.kernel.state_digest(), da);
    assert_eq!(rc.position.durable_op, last);
    assert!(rc.report.checkpoint_op.is_some(), "recovered from a checkpoint");
    assert_eq!(rc.position.last_boot_generation, 2);
    drop(_w);
    // T08: lose the projection and rebuild it from the log only.
    std::fs::remove_file(&c_proj).unwrap();
    let (st, wal) = OwalStore::open(&c_dir, small_wal(), cfg.fingerprint()).unwrap();
    let mut pr =
        Projector::rebuild(&c_proj, Durability::NormalBenchmarkOnly, cfg.fingerprint(), st.manifest.timeline_id)
            .unwrap();
    assert_eq!(pr.catch_up(&wal, 100).unwrap(), last);
    drop(pr);
    let ca = rusqlite::Connection::open(&a_path).unwrap();
    let cc = rusqlite::Connection::open(&c_proj).unwrap();
    let (ta, tc) = (sqlite_tl(&ca), sqlite_tl(&cc));
    let (xa, xc) = (tables(&ca, ta), tables(&cc, tc));
    for (name, rows) in &xa {
        assert_eq!(rows, &xc[name], "table {name} differs after rebuild from the log");
    }
    assert_eq!(history_all(&ca, ta), history_all(&cc, tc));
}

#[test]
fn t08_projection_follows_status_change_without_event() {
    let d = tmp();
    let dir = private(d.path(), "c").join("owal");
    let proj = private(d.path(), "cp").join("p.db");
    let b = Backend::Owal(dir.clone(), Some(proj.clone()), small_wal());
    let cfg = config_with(64);
    let mut p = Pipe::open(&b, cfg.clone(), 0);
    // Raise alarm 1 and 3 (values above trip), durable.
    p.apply(Operation::SourceConnected { source_id: SourceId(1), link_generation: 1, source_epoch: 1 }).unwrap();
    p.apply(Operation::ObservationBatch(ObservationBatch {
        source_id: SourceId(1),
        link_generation: 1,
        mode: BatchMode::FullState,
        source_sequence: 1,
        observations: vec![
            Observation { channel_id: ChannelId(1), value: Value::I64(15) },
            Observation { channel_id: ChannelId(2), value: Value::I64(6) },
        ],
    }))
    .unwrap();
    p.wait_durable();
    let _ = p.close();
    let mut p = Pipe::open(&b, cfg.clone(), 0);
    let before: Vec<u8> = p.flags();
    p.fence();
    let after: Vec<u8> = p.flags();
    assert_ne!(before, after, "fence changes input_valid");
    p.wait_durable();
    let (k, _) = p.close();
    let c = rusqlite::Connection::open(&proj).unwrap();
    let tl = sqlite_tl(&c);
    let rows = reader::projection(&c, tl, 0, 100).unwrap().0;
    for r in &rows {
        assert_eq!(
            r.status_after,
            k.alarm(AlarmId(r.alarm_id)).unwrap().flags.0,
            "projection = kernel for alarm {}",
            r.alarm_id
        );
    }
    assert!(!rows.is_empty());
}

// ---- T02/T03: crash children ----

const CHILD_ENV: &str = "EXP001_OWAL_CHILD";

/// Child role: run the deterministic workload on `dir`, printing `A <seq> <digest>` for
/// every applied operation and `D <op>` whenever the published durable watermark moves
/// (what a client would have been acknowledged). Optionally armed at a fault point.
fn child_main(spec: &str) {
    if let Some(rest) = spec.strip_prefix("recover:") {
        // Recovery inside the same (mount) namespace as the crashed child.
        let mut it = rest.split(',');
        let dir = PathBuf::from(it.next().unwrap());
        let seg: u64 = it.next().unwrap().parse().unwrap();
        let o = OwalOptions { segment_size: seg, max_record_size: 16 * 1024 };
        match recover_owal(&dir, o, config_with(64)) {
            Ok((_s, _w, r)) => println!(
                "\nR {} {} {}",
                r.position.durable_op,
                hex(&r.kernel.state_digest()),
                r.kernel.outbox_status().unwrap().next_event_seq - 1 == r.position.durable_event
            ),
            Err(e) => println!("\nR-ERR {e}"),
        }
        return;
    }
    let mut it = spec.split(',');
    let dir = PathBuf::from(it.next().unwrap());
    let fault_spec = it.next().unwrap_or("");
    let seg: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(64 * 1024);
    let ops: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(3000);
    if !fault_spec.is_empty() {
        scada_journal::fault::arm(fault_spec).unwrap();
    }
    let o = OwalOptions { segment_size: seg, max_record_size: 16 * 1024 };
    let b = Backend::Owal(dir, None, o);
    let mut p = Pipe::open_with(&b, config_with(64), 37, 16, |_| {});
    p.det = false;
    use std::io::Write;
    let out = std::sync::Arc::new(std::sync::Mutex::new(std::io::stdout()));
    let o2 = out.clone();
    p.trace = Some(Box::new(move |seq, k| {
        let mut o = o2.lock().unwrap();
        let _ = writeln!(o, "A {seq} {}", hex(&k.state_digest()));
        let _ = o.flush();
    }));
    {
        // The test harness prints "test child_entry ... " without a newline first.
        let mut o = out.lock().unwrap();
        let _ = writeln!(o);
    }
    let shared = p.shared.clone();
    let o3 = out.clone();
    std::thread::spawn(move || {
        let mut last = shared.durable_op.load(Ordering::Acquire);
        loop {
            let d = shared.durable_op.load(Ordering::Acquire);
            if d != last {
                let mut o = o3.lock().unwrap();
                let _ = writeln!(o, "D {d}");
                let _ = o.flush();
                last = d;
            }
            if shared.state() == JournalState::Fault {
                let mut o = o3.lock().unwrap();
                let _ = writeln!(o, "F {}", shared.error());
                let _ = o.flush();
                std::process::exit(3);
            }
            std::thread::sleep(Duration::from_micros(50));
        }
    });
    for _ in 0..(ops / 100) {
        workload(&mut p, 100);
        p.wait_durable();
        p.watermark();
    }
    p.wait_durable();
    std::thread::sleep(Duration::from_millis(5));
    let _ = p.close();
    println!("END");
}

#[test]
fn child_entry() {
    // Not a test by itself: the crash tests re-execute this binary with CHILD_ENV set.
    if let Ok(spec) = std::env::var(CHILD_ENV) {
        child_main(&spec);
    }
}

struct ChildOut {
    applied: BTreeMap<u64, String>,
    durable: u64,
    ended: bool,
    fault: Option<String>,
    recovered: Option<String>,
    status: std::process::ExitStatus,
}

fn run_child(spec: &str, kill_after: Option<Duration>, wrap: Option<&str>) -> ChildOut {
    let exe = std::env::current_exe().unwrap();
    let mut cmd = match wrap {
        // `sh -c '<prefix>; exec "$0" ...'`: shell-level setup inherited across exec.
        Some(prefix) => {
            let mut c = Command::new("sh");
            c.arg("-c")
                .arg(format!("{prefix}; exec \"$0\" child_entry --exact --nocapture --test-threads=1"))
                .arg(&exe);
            c
        }
        None => {
            let mut c = Command::new(&exe);
            c.args(["child_entry", "--exact", "--nocapture", "--test-threads=1"]);
            c
        }
    };
    cmd.env(CHILD_ENV, spec).stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = cmd.spawn().unwrap();
    let stdout = child.stdout.take().unwrap();
    let killer = kill_after.map(|d| {
        let pid = child.id();
        std::thread::spawn(move || {
            std::thread::sleep(d);
            let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
        })
    });
    let mut out = ChildOut {
        applied: BTreeMap::new(),
        durable: 0,
        ended: false,
        fault: None,
        recovered: None,
        status: Default::default(),
    };
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        let mut w = line.splitn(3, ' ');
        match (w.next(), w.next(), w.next()) {
            (Some("A"), Some(s), Some(dg)) => {
                out.applied.insert(s.parse().unwrap(), dg.to_owned());
            }
            (Some("D"), Some(s), _) => out.durable = out.durable.max(s.parse().unwrap()),
            (Some("F"), _, _) => out.fault = Some(line.clone()),
            (Some("END"), _, _) => out.ended = true,
            (Some("R" | "R-ERR"), _, _) => out.recovered = Some(line.clone()),
            _ => {}
        }
    }
    out.status = child.wait().unwrap();
    if let Some(k) = killer {
        let _ = k.join();
    }
    out
}

/// After a crash: recovery succeeds, nothing acknowledged is lost, the recovered state
/// is exactly what the child had at that operation, and events are not duplicated.
fn check_after_crash(dir: &Path, o: OwalOptions, out: &ChildOut, what: &str) -> u64 {
    check_after_crash_from(dir, o, out, what, None)
}

fn check_after_crash_from(dir: &Path, o: OwalOptions, out: &ChildOut, what: &str, base: Option<(u64, String)>) -> u64 {
    let (_s, _w, r) = recover_owal(dir, o, config_with(64)).unwrap_or_else(|e| panic!("{what}: recovery failed: {e}"));
    let got = r.position.durable_op;
    assert!(got >= out.durable, "{what}: acknowledged {} but recovered only {got}", out.durable);
    if got > 0 {
        let base_dg = base.as_ref().filter(|(op, _)| *op == got).map(|(_, d)| d);
        let want = out
            .applied
            .get(&got)
            .or(base_dg)
            .unwrap_or_else(|| panic!("{what}: recovered op {got} was never applied by the child"));
        assert_eq!(&hex(&r.kernel.state_digest()), want, "{what}: recovered state differs from the child's at {got}");
    }
    let next = r.kernel.outbox_status().unwrap().next_event_seq;
    assert_eq!(next - 1, r.position.durable_event, "{what}: event sequence");
    got
}

#[test]
fn t02_t03_crash_at_every_fault_point() {
    let points = ["before_transaction", "during_transaction", "after_commit_before_watermark", "during_checkpoint"];
    for point in points {
        for n in [1u64, 7, 40] {
            let d = tmp();
            let dir = private(d.path(), "w").join("owal");
            let out = run_child(&format!("{},{point}:{n},65536,3000", dir.display()), None, None);
            assert!(!out.ended, "{point}:{n} did not crash");
            assert!(!out.status.success());
            let got = check_after_crash(&dir, small_wal(), &out, &format!("{point}:{n}"));
            // Resume after recovery: the journal continues on the same timeline.
            let mut p = Pipe::open(&Backend::Owal(dir.clone(), None, small_wal()), config_with(64), 37);
            assert_eq!(p.k.last_operation_sequence(), got);
            det_workload(&mut p, 50);
            let (k, _) = p.close();
            let (_s, _w, r) = recover_owal(&dir, small_wal(), config_with(64)).unwrap();
            assert_eq!(r.kernel.state_digest(), k.state_digest(), "{point}:{n}: resume");
        }
    }
}

#[test]
fn t03_random_sigkill() {
    let mut rng = 0x2545_F491_4F6C_DD1Du64;
    for round in 0..12 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let ms = 20 + rng % 400;
        let d = tmp();
        let dir = private(d.path(), "w").join("owal");
        let out = run_child(&format!("{},,65536,200000", dir.display()), Some(Duration::from_millis(ms)), None);
        assert!(!out.ended, "round {round}: child finished before the kill");
        let got = check_after_crash(&dir, small_wal(), &out, &format!("sigkill round {round} at {ms} ms"));
        eprintln!(
            "sigkill round {round}: {ms} ms, acknowledged {}, applied {}, recovered {got}",
            out.durable,
            out.applied.len()
        );
    }
}

// ---- T03 corruption ----

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    v.sort();
    v
}

/// Byte offset of the end of the records of a segment (first all-zero record header).
fn used_bytes(seg: &Path) -> u64 {
    let b = std::fs::read(seg).unwrap();
    let mut off = 64usize;
    while off + 20 <= b.len() {
        let len = u32::from_le_bytes(b[off + 4..off + 8].try_into().unwrap()) as usize;
        if len == 0 && b[off..off + 20].iter().all(|x| *x == 0) {
            break;
        }
        off += (20 + len).div_ceil(8) * 8;
    }
    off as u64
}

fn flip(path: &Path, at: u64) {
    let f = std::fs::OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut b = [0u8; 1];
    f.read_exact_at(&mut b, at).unwrap();
    b[0] ^= 0x40;
    f.write_all_at(&b, at).unwrap();
    f.sync_all().unwrap();
}

fn build(dir: &Path, n: u64, checkpoint_every: u64) -> (Kernel, u64) {
    let mut p = Pipe::open(&Backend::Owal(dir.to_path_buf(), None, small_wal()), config_with(64), checkpoint_every);
    det_workload(&mut p, n);
    let (k, _) = p.close();
    let last = k.last_operation_sequence();
    (k, last)
}

#[test]
fn t03_torn_tail_is_truncated_and_mid_log_corruption_is_fatal() {
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    let (_k, last) = build(&dir, 1500, 0);
    let segs = segments(&dir);
    assert!(segs.len() >= 3, "test needs several segments");
    // Torn tail: damage inside the last record of the active segment. open-wal cannot
    // tell a torn write from a corrupted committed record at the tail: it truncates it.
    let active = segs.last().unwrap();
    let end = used_bytes(active);
    flip(active, end - 3);
    let (_s, _w, r) = recover_owal(&dir, small_wal(), config_with(64)).unwrap();
    assert_eq!(r.position.durable_op, last - 1, "torn tail dropped exactly one record");
    drop(_w);
    // Mid-log corruption in a sealed segment: fatal (fail-closed).
    let d2 = tmp();
    let dir2 = private(d2.path(), "w").join("owal");
    build(&dir2, 1500, 0);
    let segs = segments(&dir2);
    flip(&segs[1], 64 + 30);
    let e = err_of(recover_owal(&dir2, small_wal(), config_with(64)));
    assert!(matches!(e, JournalError::Corrupt(_)), "{e}");
    // Mid-record corruption in the active segment with valid records after it: fatal.
    let d3 = tmp();
    let dir3 = private(d3.path(), "w").join("owal");
    build(&dir3, 1500, 0);
    let segs = segments(&dir3);
    let act = segs.last().unwrap();
    flip(act, 64 + 30);
    let e = err_of(recover_owal(&dir3, small_wal(), config_with(64)));
    assert!(matches!(e, JournalError::Corrupt(_)), "{e}");
}

#[test]
fn t03_projection_ahead_of_a_truncated_log_is_detected() {
    // The torn-tail truncation of a *committed* record is silent in open-wal; a derived
    // projection that already holds that record detects the regression (fail-closed).
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    let proj = private(d.path(), "p").join("p.db");
    let b = Backend::Owal(dir.clone(), Some(proj.clone()), small_wal());
    let mut p = Pipe::open(&b, config_with(64), 0);
    det_workload(&mut p, 300);
    let _ = p.close();
    let active = segments(&dir).last().unwrap().clone();
    flip(&active, used_bytes(&active) - 3);
    let (st, wal) = OwalStore::open(&dir, small_wal(), config_with(64).fingerprint()).unwrap();
    let mut pr =
        Projector::open(&proj, Durability::NormalBenchmarkOnly, config_with(64).fingerprint(), st.manifest.timeline_id)
            .unwrap();
    let e = pr.catch_up(&wal, 100).unwrap_err();
    assert!(e.to_string().contains("ahead of the durable log"), "{e}");
}

#[test]
fn t03_damaged_checkpoints_fall_back_then_genesis() {
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    let (k, last) = build(&dir, 1200, 100);
    let cdir = dir.join("checkpoints");
    let mut cps: Vec<PathBuf> = std::fs::read_dir(&cdir).unwrap().flatten().map(|e| e.path()).collect();
    cps.sort();
    assert_eq!(cps.len(), 2, "checkpoints_keep = 2");
    flip(&cps[1], 200);
    let (_s, _w, r) = recover_owal(&dir, small_wal(), config_with(64)).unwrap();
    assert_eq!(r.report.checkpoints_rejected.len(), 1);
    assert_eq!(r.kernel.state_digest(), k.state_digest());
    let fallback = r.report.checkpoint_op.unwrap();
    drop(_w);
    std::fs::write(&cps[0], b"garbage").unwrap();
    let (_s, _w, r) = recover_owal(&dir, small_wal(), config_with(64)).unwrap();
    assert_eq!(r.report.checkpoint_op, None, "genesis replay");
    assert_eq!(r.report.replayed_operations, last);
    assert_eq!(r.kernel.state_digest(), k.state_digest());
    assert!(fallback < last);
}

#[test]
fn t03_identity_configuration_and_single_writer() {
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    build(&dir, 50, 0);
    // Second writer.
    let (_s, _w) = OwalStore::open(&dir, small_wal(), config_with(64).fingerprint()).unwrap();
    let e = err_of(OwalStore::open(&dir, small_wal(), config_with(64).fingerprint()));
    assert!(matches!(e, JournalError::Locked(_)), "{e}");
    drop((_s, _w));
    // Other configuration.
    let e = err_of(OwalStore::open(&dir, small_wal(), fanout_config(1).fingerprint()));
    assert!(matches!(e, JournalError::Incompatible(_)), "{e}");
    // Damaged manifest.
    flip(&dir.join("MANIFEST"), 30);
    let e = err_of(OwalStore::open(&dir, small_wal(), config_with(64).fingerprint()));
    assert!(matches!(e, JournalError::Corrupt(_)), "{e}");
    // Lost manifest with segments present.
    std::fs::remove_file(dir.join("MANIFEST")).unwrap();
    let e = err_of(OwalStore::open(&dir, small_wal(), config_with(64).fingerprint()));
    assert!(matches!(e, JournalError::Corrupt(_)), "{e}");
}

#[test]
fn t10_log_not_starting_at_one_is_refused_and_a_missing_middle_segment_too() {
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    build(&dir, 1500, 100);
    let segs = segments(&dir);
    assert!(segs.len() >= 3);
    std::fs::remove_file(&segs[1]).unwrap();
    let e = err_of(recover_owal(&dir, small_wal(), config_with(64)));
    assert!(matches!(e, JournalError::Corrupt(_)), "middle segment: {e}");
    std::fs::remove_file(&segs[0]).unwrap();
    for s in &segs[2..segs.len() - 1] {
        std::fs::remove_file(s).unwrap();
    }
    let e = err_of(recover_owal(&dir, small_wal(), config_with(64)));
    assert!(matches!(e, JournalError::Corrupt(_)), "prefix deleted: {e}");
}

// ---- T04 ----

#[test]
fn t04_simulated_eio_faults_and_recovers() {
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    let b = Backend::Owal(dir.clone(), None, small_wal());
    let mut p = Pipe::open_with(&b, config_with(64), 0, 16, |w| w.test_eio_at_commit = Some(5));
    workload(&mut p, 400);
    p.wait_durable();
    assert_eq!(p.shared.state(), JournalState::Fault);
    let durable = p.shared.durable_op.load(Ordering::Acquire);
    assert!(durable < p.k.last_operation_sequence(), "nothing past the fault is durable");
    drop(p.tx.take());
    p.writer.take().unwrap().join().unwrap();
    let (_s, _w, r) = recover_owal(&dir, small_wal(), config_with(64)).unwrap();
    assert_eq!(r.position.durable_op, durable, "recovery = published watermark");
}

/// Real write failure: `ulimit -f` (SIGXFSZ ignored) makes `write` return EFBIG in the
/// middle of the pre-allocated active segment. open-wal poisons the handle; the writer
/// faults; recovery gives a whole-record durable prefix covering every acknowledgement.
#[test]
fn t04_real_write_failure_poisons_and_faults() {
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    let o = OwalOptions { segment_size: 1 << 20, max_record_size: 16 * 1024 };
    let mut p = Pipe::open(&Backend::Owal(dir.clone(), None, o), config_with(64), 0);
    det_workload(&mut p, 50);
    let (k, _) = p.close();
    let base = (k.last_operation_sequence(), hex(&k.state_digest()));
    // 256 blocks (512 B or 1 KiB depending on the shell): far below the 1 MiB segment,
    // far above the bytes already written.
    let out = run_child(&format!("{},,{},200000", dir.display(), 1 << 20), None, Some("trap '' XFSZ; ulimit -f 256"));
    assert!(!out.ended, "child should fault");
    let f = out.fault.clone().expect("FAULT reported");
    assert!(f.contains("commit"), "{f}");
    let got = check_after_crash_from(&dir, o, &out, "efbig", Some(base.clone()));
    assert!(got > base.0, "some operations were durable before the failure");
}

/// Real ENOSPC at a segment roll: a 448 KiB tmpfs in a private mount namespace holds three
/// 128 KiB segments; pre-allocating the fourth fails in the middle of a commit group
/// (the commit is split at the roll: a durable prefix, then the error). Skipped when
/// unprivileged user/mount namespaces are not available.
#[test]
fn t04_real_enospc_at_segment_preallocation() {
    if !Command::new("unshare").args(["-rm", "true"]).status().is_ok_and(|s| s.success()) {
        eprintln!("SKIPPED: unshare -rm not available");
        return;
    }
    let d = tmp();
    let mnt = private(d.path(), "mnt");
    let dir = mnt.join("owal");
    let seg = 131_072u64;
    let exe = std::env::current_exe().unwrap();
    let prefix = format!(
        "mount -t tmpfs -o size=448k,mode=0700 tmpfs {m} || exit 9; \
         {CHILD_ENV}={d},,{seg},200000 \"$0\" child_entry --exact --nocapture --test-threads=1; \
         {CHILD_ENV}=recover:{d},{seg} \"$0\" child_entry --exact --nocapture --test-threads=1; exit 0",
        m = mnt.display(),
        d = dir.display()
    );
    let mut child = Command::new("unshare")
        .args(["-rm", "sh", "-c", &prefix])
        .arg(&exe)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut out = ChildOut {
        applied: BTreeMap::new(),
        durable: 0,
        ended: false,
        fault: None,
        recovered: None,
        status: Default::default(),
    };
    for line in BufReader::new(stdout).lines().map_while(Result::ok) {
        let mut w = line.splitn(3, ' ');
        match (w.next(), w.next(), w.next()) {
            (Some("A"), Some(s), Some(dg)) => {
                out.applied.insert(s.parse().unwrap(), dg.to_owned());
            }
            (Some("D"), Some(s), _) => out.durable = out.durable.max(s.parse().unwrap()),
            (Some("F"), _, _) => out.fault = Some(line.clone()),
            (Some("END"), _, _) => out.ended = true,
            (Some("R" | "R-ERR"), _, _) => out.recovered = Some(line.clone()),
            _ => {}
        }
    }
    assert!(child.wait().unwrap().success());
    assert!(!out.ended, "child should fault");
    let f = out.fault.expect("FAULT reported");
    assert!(f.contains("commit") && f.contains("No space"), "{f}");
    let r = out.recovered.expect("recovery ran");
    let mut w = r.split(' ');
    assert_eq!(w.next(), Some("R"), "{r}");
    let op: u64 = w.next().unwrap().parse().unwrap();
    let dg = w.next().unwrap();
    assert_eq!(w.next(), Some("true"), "event sequence consistent");
    assert!(op >= out.durable, "acknowledged {} but recovered {op}", out.durable);
    assert_eq!(out.applied.get(&op).map(String::as_str), Some(dg), "recovered state = applied state at {op}");
}

// ---- T05 ----

fn fanout_config(n: u32) -> Arc<ValidatedConfig> {
    let spec = ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: 8 },
        tags: vec![TagSpec { tag_id: TagId(1), name: "t1".into(), value_type: ValueType::I64 }],
        sources: vec![SourceSpec {
            source_id: SourceId(1),
            name: "s".into(),
            channels: vec![ChannelSpec { channel_id: ChannelId(1), tag_id: TagId(1), value_type: ValueType::I64 }],
            supervision: Supervision::NONE,
        }],
    };
    let rules = (1..=n.max(1))
        .map(|i| AlarmRuleSpec {
            alarm_id: AlarmId(i),
            name: format!("a{i}"),
            tag_id: TagId(1),
            condition: AlarmCondition::High { trip: 1, reset: 0 },
            severity: Severity::Major,
        })
        .collect();
    let alarms = AlarmSpec { max_rules: n.max(1), outbox_capacity_events: u64::from(n.max(1)) * 4, rules };
    Arc::new(ValidatedConfig::validate_with_alarms(spec, Some(alarms)).unwrap())
}

#[test]
fn t05_fanout_bursts_fit_one_record_and_oversize_is_refused_before_apply() {
    let default_max = OwalOptions::default().max_record_size as usize;
    for n in [1u32, 10, 100, 1000, 10_000] {
        let d = tmp();
        let dir = private(d.path(), "w").join("owal");
        let cfg = fanout_config(n);
        let o = OwalOptions::default();
        // Admission: the worst case must fit before apply.
        assert!(envelope::worst_case_len(1, n as usize, 0, 0, 0) <= default_max, "{n} events fit 1 MiB");
        let b = Backend::Owal(dir.clone(), None, o);
        let mut p = Pipe::open(&b, cfg.clone(), 0);
        p.apply(Operation::SourceConnected { source_id: SourceId(1), link_generation: 1, source_epoch: 1 }).unwrap();
        for (i, v) in [1i64, 0, 1].iter().enumerate() {
            p.apply(Operation::ObservationBatch(ObservationBatch {
                source_id: SourceId(1),
                link_generation: 1,
                mode: if i == 0 { BatchMode::FullState } else { BatchMode::Delta },
                source_sequence: i as u64 + 1,
                observations: vec![Observation { channel_id: ChannelId(1), value: Value::I64(*v) }],
            }))
            .unwrap();
            p.wait_durable();
            p.watermark();
        }
        p.wait_durable();
        let (k, _) = p.close();
        let (_s, _w, r) = recover_owal(&dir, o, cfg).unwrap();
        assert_eq!(r.kernel.state_digest(), k.state_digest(), "fan-out {n}");
        assert_eq!(r.position.durable_event, 3 * u64::from(n), "fan-out {n}: every event in the log");
    }
    // 20 000 events exceed the default 1 MiB record: refused by the admission bound.
    assert!(envelope::worst_case_len(1, 20_000, 0, 0, 0) > default_max);
    // A writer that nevertheless meets an oversize record faults and writes none of it.
    let d = tmp();
    let dir = private(d.path(), "w").join("owal");
    let o = OwalOptions { segment_size: 1 << 20, max_record_size: 4096 };
    let cfg = fanout_config(200);
    let mut p = Pipe::open(&Backend::Owal(dir.clone(), None, o), cfg.clone(), 0);
    p.apply(Operation::SourceConnected { source_id: SourceId(1), link_generation: 1, source_epoch: 1 }).unwrap();
    p.wait_durable();
    p.apply(Operation::ObservationBatch(ObservationBatch {
        source_id: SourceId(1),
        link_generation: 1,
        mode: BatchMode::FullState,
        source_sequence: 1,
        observations: vec![Observation { channel_id: ChannelId(1), value: Value::I64(1) }],
    }))
    .unwrap();
    p.wait_durable();
    assert_eq!(p.shared.state(), JournalState::Fault);
    assert!(p.shared.error().contains("RecordTooLarge"), "{}", p.shared.error());
    drop(p.tx.take());
    p.writer.take().unwrap().join().unwrap();
    let (_s, _w, r) = recover_owal(&dir, o, cfg).unwrap();
    assert_eq!(r.position.durable_op, 1, "only the connect is durable");
    assert_eq!(r.position.durable_event, 0);
}

// ---- T11 ----

#[test]
fn t11_random_segment_corruption_never_panics_and_never_lies() {
    let d = tmp();
    let base = private(d.path(), "base").join("owal");
    // Reference digests per operation (deterministic workload, single writer).
    let mut p = Pipe::open(&Backend::Owal(base.clone(), None, small_wal()), config_with(64), 0);
    let digests = std::sync::Arc::new(std::sync::Mutex::new(BTreeMap::new()));
    let dg = digests.clone();
    p.trace = Some(Box::new(move |s, k| {
        dg.lock().unwrap().insert(s, k.state_digest());
    }));
    det_workload(&mut p, 800);
    let (_k, _) = p.close();
    let digests = digests.lock().unwrap().clone();
    let segs = segments(&base);
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut outcomes = BTreeMap::new();
    for round in 0..60 {
        let work = private(d.path(), &format!("r{round}")).join("owal");
        std::fs::create_dir_all(work.join("wal")).unwrap();
        std::fs::create_dir_all(work.join("checkpoints")).unwrap();
        std::fs::copy(base.join("MANIFEST"), work.join("MANIFEST")).unwrap();
        for s in &segs {
            std::fs::copy(s, work.join("wal").join(s.file_name().unwrap())).unwrap();
        }
        let wsegs = segments(&work);
        for _ in 0..(1 + round % 3) {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            if round % 2 == 0 {
                // Tail region of the active segment (torn-tail class).
                let s = wsegs.last().unwrap();
                let used = used_bytes(s).max(65);
                flip(s, used - 1 - (rng >> 8) % 40.min(used - 64));
            } else {
                let s = &wsegs[(rng % wsegs.len() as u64) as usize];
                let used = used_bytes(s).max(65);
                flip(s, 64 + (rng >> 8) % (used - 64));
            }
        }
        match recover_owal(&work, small_wal(), config_with(64)) {
            Ok((_s, _w, r)) => {
                let op = r.position.durable_op;
                if op > 0 {
                    assert_eq!(
                        Some(&r.kernel.state_digest()),
                        digests.get(&op),
                        "round {round}: recovered prefix {op} lies"
                    );
                }
                *outcomes.entry("prefix").or_insert(0) += 1;
            }
            Err(JournalError::Corrupt(_)) => *outcomes.entry("corrupt").or_insert(0) += 1,
            Err(e) => panic!("round {round}: unexpected error class {e}"),
        }
    }
    eprintln!("t11 outcomes: {outcomes:?}");
}
