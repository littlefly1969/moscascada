//! The journal writer thread (ADR 0013 D5, D13). Sole owner of the write connection.
//!
//! Group commit: it blocks for the first message, then takes whatever is already queued
//! (up to `commit_max_operations`, optionally waiting `commit_max_delay` for more) and
//! writes it in **one** `BEGIN IMMEDIATE … COMMIT`. Only after `COMMIT` returned does it
//! publish `durable_op` / `durable_event`, release the reservations, call the durable
//! receipts and wake the sequencer (J1, J3). Checkpoints go in their own transaction after
//! the commit that made their prefix durable (D7).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::{Connection, ErrorCode, params};

use crate::codec::{encode_checkpoint, encode_op, encode_source_times, op_kind, payload_digest};
use crate::progress::{JournalShared, JournalState};
use crate::{
    CheckpointMsg, CommitResult, Durability, JournalError, OpRecord, Store, WriterMsg, fault, realtime_ns, sq,
};

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub commit_max_operations: usize,
    /// Optional extra wait for more records after the first one (0 = natural batching).
    pub commit_max_delay: Duration,
    pub checkpoints_keep: u32,
    pub wal_max_bytes: u64,
    pub durability: Durability,
    pub boot_generation: u64,
    /// TEST-ONLY: sleep before each commit (slow disk).
    pub test_delay: Duration,
    /// TEST-ONLY: stall while this file exists (stalled disk).
    pub test_stall_file: Option<PathBuf>,
    /// TEST-ONLY: the n-th commit fails as an unrecoverable I/O error (simulated EIO).
    pub test_eio_at_commit: Option<u64>,
}

/// Start the writer. It ends when every sender is dropped and the queue is drained.
pub fn spawn(
    store: Store,
    shared: Arc<JournalShared>,
    rx: Receiver<WriterMsg>,
    opts: WriterOptions,
    wake: Box<dyn Fn() + Send>,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new().name("scada-journal".into()).spawn(move || {
        let mut w = Writer::new(store, shared, opts, wake);
        w.run(rx);
    })
}

struct Writer {
    store: Store,
    shared: Arc<JournalShared>,
    opts: WriterOptions,
    wake: Box<dyn Fn() + Send>,
    payload: Vec<u8>,
    /// First op of the previous commit and its measured durable time (written with the
    /// next commit, ADR 0013 D5).
    prev_commit: Option<(u64, u64, i64)>,
    commits: u64,
    last_stats: Option<Instant>,
}

/// Result of one group transaction.
struct Written {
    first_op: u64,
    last_op: u64,
    ops: u64,
    events: u64,
    max_event: u64,
    bytes: u64,
}

fn is_full(e: &JournalError) -> bool {
    matches!(e, JournalError::Sqlite(rusqlite::Error::SqliteFailure(f, _)) if f.code == ErrorCode::DiskFull)
}

fn ops_in(group: &[WriterMsg]) -> usize {
    group.iter().filter(|m| matches!(m, WriterMsg::Op(_))).count()
}

impl Writer {
    fn new(store: Store, shared: Arc<JournalShared>, opts: WriterOptions, wake: Box<dyn Fn() + Send>) -> Writer {
        store.conn.set_prepared_statement_cache_capacity(32);
        Writer {
            store,
            shared,
            opts,
            wake,
            payload: Vec::with_capacity(4096),
            prev_commit: None,
            commits: 0,
            last_stats: None,
        }
    }

    fn run(&mut self, rx: Receiver<WriterMsg>) {
        self.stats();
        loop {
            let first = match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(m) => m,
                Err(RecvTimeoutError::Timeout) => {
                    self.stats();
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };
            let mut group = vec![first];
            let max = self.opts.commit_max_operations.max(1);
            let deadline = Instant::now() + self.opts.commit_max_delay;
            while ops_in(&group) < max {
                match rx.try_recv() {
                    Ok(m) => group.push(m),
                    Err(TryRecvError::Empty) => {
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        match rx.recv_timeout(deadline - now) {
                            Ok(m) => group.push(m),
                            Err(_) => break,
                        }
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            self.process(group);
        }
    }

    fn test_hooks(&self) {
        if let Some(f) = &self.opts.test_stall_file {
            let mut stalled = false;
            while f.exists() {
                if !stalled {
                    self.shared.state.store(JournalState::Backpressure as u8, Relaxed);
                    stalled = true;
                }
                thread::sleep(Duration::from_millis(5));
            }
            if stalled {
                self.shared.state.store(JournalState::Ok as u8, Relaxed);
            }
        }
        if !self.opts.test_delay.is_zero() {
            thread::sleep(self.opts.test_delay);
        }
    }

    fn process(&mut self, mut group: Vec<WriterMsg>) {
        if self.shared.state() == JournalState::Fault {
            self.fail(group);
            return;
        }
        self.test_hooks();
        let mut backoff = Duration::from_millis(20);
        let written = loop {
            let t0 = Instant::now();
            match self.write_group(&group) {
                Ok(w) => {
                    self.shared.commit_latency.record_ns(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
                    break w;
                }
                Err(e) if is_full(&e) && !self.shared.shutdown.load(Relaxed) => {
                    // D13: SQLITE_FULL rolled the transaction back; retry the same group.
                    self.shared.state.store(JournalState::DiskFull as u8, Relaxed);
                    self.shared.commit_retries.fetch_add(1, Relaxed);
                    self.shared.set_error(format!("disk full: {e}"));
                    self.stats();
                    thread::sleep(backoff);
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                }
                Err(e) => {
                    eprintln!("journal: FAULT, no further commit: {e}");
                    self.shared.set_error(e.to_string());
                    self.shared.state.store(JournalState::Fault as u8, Release);
                    self.fail(group);
                    (self.wake)();
                    return;
                }
            }
        };
        if self.shared.state() == JournalState::DiskFull {
            self.shared.state.store(JournalState::Ok as u8, Relaxed);
        }
        fault::hit("after_commit_before_watermark");
        // Publication strictly after COMMIT returned (J1).
        if written.ops > 0 {
            self.shared.durable_op.fetch_max(written.last_op, Release);
        }
        if written.events > 0 {
            self.shared.durable_event.fetch_max(written.max_event, Release);
        }
        self.shared.reserved_ops.fetch_sub(written.ops, Relaxed);
        self.shared.reserved_bytes.fetch_sub(written.bytes, Relaxed);
        self.shared.commits.fetch_add(1, Relaxed);
        self.shared.committed_ops.fetch_add(written.ops, Relaxed);
        self.shared.committed_events.fetch_add(written.events, Relaxed);
        self.shared.committed_bytes.fetch_add(written.bytes, Relaxed);
        self.shared.group_size.record_ns(written.ops);
        let now_boot = crate::boottime_ns();
        let durable_at = realtime_ns();
        self.prev_commit = Some((written.first_op, 0, durable_at));
        let mut checkpoints = Vec::new();
        for m in group.drain(..) {
            match m {
                WriterMsg::Op(mut r) => {
                    self.shared.durable_lag.record_ns(now_boot.saturating_sub(r.time.boottime_ns));
                    if let Some(rc) = r.receipt.take() {
                        rc(match self.opts.durability {
                            Durability::Full => CommitResult::Durable,
                            Durability::NormalBenchmarkOnly => CommitResult::NotDurable,
                        });
                    }
                }
                WriterMsg::Notify { receipt, .. } => {
                    self.shared.reserved_ops.fetch_sub(1, Relaxed);
                    self.shared.reserved_bytes.fetch_sub(crate::NOTIFY_BYTES, Relaxed);
                    receipt(CommitResult::Durable);
                }
                WriterMsg::Checkpoint(c) => checkpoints.push(c),
            }
        }
        (self.wake)();
        for c in checkpoints {
            let r = self.write_checkpoint(&c);
            self.shared.checkpoints_done.fetch_add(1, Relaxed);
            if let Err(e) = r {
                if is_full(&e) {
                    // A checkpoint is an optimisation of recovery time: skipped, retried
                    // at the next interval. Never blocks operations.
                    self.shared.set_error(format!("checkpoint skipped (disk full): {e}"));
                } else {
                    eprintln!("journal: checkpoint failed, FAULT: {e}");
                    self.shared.set_error(e.to_string());
                    self.shared.state.store(JournalState::Fault as u8, Release);
                }
            }
        }
        self.maybe_stats();
    }

    /// The group could not be made durable: receipts report it, reservations are released
    /// (admission is closed by the FAULT state anyway).
    fn fail(&mut self, group: Vec<WriterMsg>) {
        for m in group {
            match m {
                WriterMsg::Op(mut r) => {
                    self.shared.reserved_ops.fetch_sub(1, Relaxed);
                    self.shared.reserved_bytes.fetch_sub(r.reserved_bytes, Relaxed);
                    if let Some(rc) = r.receipt.take() {
                        rc(CommitResult::NotDurable);
                    }
                }
                WriterMsg::Notify { receipt, .. } => {
                    self.shared.reserved_ops.fetch_sub(1, Relaxed);
                    self.shared.reserved_bytes.fetch_sub(crate::NOTIFY_BYTES, Relaxed);
                    receipt(CommitResult::NotDurable);
                }
                WriterMsg::Checkpoint(_) => {
                    self.shared.checkpoints_done.fetch_add(1, Relaxed);
                }
            }
        }
    }

    fn write_group(&mut self, group: &[WriterMsg]) -> Result<Written, JournalError> {
        fault::hit("before_transaction");
        self.commits += 1;
        if self.opts.test_eio_at_commit == Some(self.commits) {
            return Err(JournalError::Io("TEST: simulated EIO at commit".into()));
        }
        let c = &self.store.conn;
        c.execute_batch("BEGIN IMMEDIATE")?;
        match write_rows(
            c,
            self.store.info.timeline_no,
            self.opts.boot_generation,
            group,
            &mut self.payload,
            self.prev_commit,
        ) {
            Ok(w) => match c.execute_batch("COMMIT") {
                Ok(()) => Ok(w),
                Err(e) => {
                    let _ = c.execute_batch("ROLLBACK");
                    Err(e.into())
                }
            },
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn write_checkpoint(&mut self, m: &CheckpointMsg) -> Result<(), JournalError> {
        let t0 = Instant::now();
        let blob = encode_checkpoint(&m.checkpoint);
        let digest = blake3::hash(&blob);
        let tl = self.store.info.timeline_no;
        let c = &self.store.conn;
        c.execute_batch("BEGIN IMMEDIATE")?;
        let r = (|| -> Result<(), JournalError> {
            c.execute(
                "INSERT OR REPLACE INTO journal_checkpoints (timeline_no, operation_seq, event_seq_high, config_fingerprint, \
                 state_digest, schema_version, blob, blob_digest, complete, created_at_utc_ns) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1, ?9)",
                params![
                    tl,
                    sq(m.checkpoint.last_operation_sequence),
                    sq(m.event_seq_high),
                    m.checkpoint.config_fingerprint.as_slice(),
                    m.state_digest.as_slice(),
                    crate::schema::SCHEMA_VERSION,
                    blob,
                    digest.as_bytes().as_slice(),
                    realtime_ns()
                ],
            )?;
            fault::hit("during_checkpoint");
            c.execute(
                "DELETE FROM journal_checkpoints WHERE timeline_no = ?1 AND checkpoint_id NOT IN \
                 (SELECT checkpoint_id FROM journal_checkpoints WHERE timeline_no = ?1 ORDER BY operation_seq DESC LIMIT ?2)",
                params![tl, i64::from(self.opts.checkpoints_keep.max(1))],
            )?;
            Ok(())
        })();
        match r {
            Ok(()) => c.execute_batch("COMMIT")?,
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                return Err(e);
            }
        }
        self.shared.checkpoints_written.fetch_add(1, Relaxed);
        self.shared.last_checkpoint_op.store(m.checkpoint.last_operation_sequence, Relaxed);
        let _ = t0;
        Ok(())
    }

    fn maybe_stats(&mut self) {
        if self.last_stats.is_none_or(|t| t.elapsed() >= Duration::from_millis(100)) {
            self.stats();
        }
    }

    /// Publish file sizes and free space; truncate the WAL when it exceeds its cap.
    fn stats(&mut self) {
        self.last_stats = Some(Instant::now());
        let path = &self.store.info.path;
        let db = std::fs::metadata(path).map_or(0, |m| m.len());
        let mut walp = path.as_os_str().to_owned();
        walp.push("-wal");
        let wal = std::fs::metadata(PathBuf::from(&walp)).map_or(0, |m| m.len());
        self.shared.db_bytes.store(db, Relaxed);
        self.shared.wal_bytes.store(wal, Relaxed);
        let dir =
            path.parent().filter(|d| !d.as_os_str().is_empty()).map_or_else(|| PathBuf::from("."), |d| d.to_path_buf());
        if let Ok(s) = rustix::fs::statvfs(&dir) {
            self.shared.disk_free.store(s.f_bavail.saturating_mul(s.f_frsize), Relaxed);
        }
        if wal > self.opts.wal_max_bytes && self.shared.state() != JournalState::Fault {
            let _ = self.store.conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
            let wal = std::fs::metadata(PathBuf::from(&walp)).map_or(0, |m| m.len());
            self.shared.wal_bytes.store(wal, Relaxed);
        }
    }
}

fn write_rows(
    c: &Connection,
    tl: i64,
    boot: u64,
    group: &[WriterMsg],
    payload: &mut Vec<u8>,
    prev_commit: Option<(u64, u64, i64)>,
) -> Result<Written, JournalError> {
    let mut w = Written { first_op: 0, last_op: 0, ops: 0, events: 0, max_event: 0, bytes: 0 };
    let mut max_logical = i64::MIN;
    if let Some((first, _, durable_at)) = prev_commit {
        c.prepare_cached("UPDATE journal_commits SET durable_at_utc_ns = ?3 WHERE timeline_no = ?1 AND first_op = ?2")?
            .execute(params![tl, sq(first), durable_at])?;
    }
    let mut ins_op = c.prepare_cached(
        "INSERT INTO journal_operations VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
    )?;
    let mut ins_ev = c.prepare_cached(
        "INSERT INTO alarm_events VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
    )?;
    let mut proj = c.prepare_cached(
        "INSERT INTO alarm_active_projection VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (timeline_no, alarm_id) DO UPDATE SET \
         status_after = excluded.status_after, occurrence_id = excluded.occurrence_id, activation_token = excluded.activation_token, \
         last_event_seq = excluded.last_event_seq, last_kind = excluded.last_kind",
    )?;
    for m in group {
        let WriterMsg::Op(r) = m else { continue };
        write_op(c, tl, boot, r, payload, &mut ins_op, &mut ins_ev, &mut proj)?;
        if w.ops == 0 {
            w.first_op = r.operation_seq;
            fault::hit("during_transaction");
        }
        w.last_op = r.operation_seq;
        w.ops += 1;
        w.events += r.events.len() as u64;
        if let Some(e) = r.events.last() {
            w.max_event = w.max_event.max(e.event_seq);
        }
        w.bytes += r.reserved_bytes;
        max_logical = max_logical.max(r.time.logical_time_ns);
    }
    if w.ops > 0 {
        c.prepare_cached(
            "UPDATE projection_meta SET as_of_operation_seq = ?2, as_of_event_seq = max(as_of_event_seq, ?3) WHERE timeline_no = ?1",
        )?
        .execute(params![tl, sq(w.last_op), sq(w.max_event)])?;
        c.prepare_cached("UPDATE schema_meta SET last_logical_time_ns = max(last_logical_time_ns, ?1) WHERE id = 1")?
            .execute(params![max_logical])?;
        c.prepare_cached("INSERT OR REPLACE INTO journal_commits VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL)")?
            .execute(params![tl, sq(w.first_op), sq(w.last_op), sq(w.ops), sq(w.events), sq(w.bytes)])?;
    }
    Ok(w)
}

#[allow(clippy::too_many_arguments)]
fn write_op(
    c: &Connection,
    tl: i64,
    boot: u64,
    r: &OpRecord,
    payload: &mut Vec<u8>,
    ins_op: &mut rusqlite::CachedStatement<'_>,
    ins_ev: &mut rusqlite::CachedStatement<'_>,
    proj: &mut rusqlite::CachedStatement<'_>,
) -> Result<(), JournalError> {
    encode_op(&r.operation, payload);
    let st = r.source_times.as_deref().map(encode_source_times);
    let first_ev = r.events.first().map_or(0, |e| e.event_seq);
    let t = &r.time;
    ins_op.execute(params![
        tl,
        sq(r.operation_seq),
        i64::from(op_kind(&r.operation)),
        payload.as_slice(),
        payload_digest(payload),
        sq(first_ev),
        r.events.len() as i64,
        sq(boot),
        t.sequenced_at_utc_ns,
        sq(t.boottime_ns),
        t.logical_time_ns,
        i64::from(t.clock_quality),
        i64::from(t.clock_flags),
        t.received_at_utc_ns,
        st,
        i64::from(r.reconciliation)
    ])?;
    for (i, e) in r.events.iter().enumerate() {
        let o = r.origins.get(i).copied().unwrap_or_default();
        ins_ev.execute(params![
            tl,
            sq(e.event_seq),
            sq(e.operation_seq),
            i64::from(e.ordinal_in_operation),
            i64::from(e.alarm_id.0),
            sq(e.occurrence_id),
            sq(e.activation_token),
            i64::from(e.kind.code()),
            i64::from(e.status_after.0),
            i64::from(e.actor_uid),
            i64::from(r.reconciliation),
            o.utc_ns,
            i64::from(o.source),
            i64::from(o.clock_quality)
        ])?;
        proj.execute(params![
            tl,
            i64::from(e.alarm_id.0),
            i64::from(e.status_after.0),
            sq(e.occurrence_id),
            sq(e.activation_token),
            sq(e.event_seq),
            i64::from(e.kind.code())
        ])?;
    }
    if let Some(a) = &r.action {
        c.prepare_cached(
            "INSERT OR IGNORE INTO operator_actions VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10)",
        )?
        .execute(params![
            a.action_id.as_slice(),
            tl,
            sq(r.operation_seq),
            a.event_seq.map(sq),
            i64::from(a.alarm_id),
            sq(a.occurrence_id),
            sq(a.requested_token),
            i64::from(a.actor_uid),
            a.note,
            i64::from(a.outcome)
        ])?;
    }
    for u in &r.recovery {
        c.prepare_cached(
            "INSERT INTO source_recovery VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) ON CONFLICT (timeline_no, source_id) DO UPDATE SET \
             source_epoch = excluded.source_epoch, boot_generation = excluded.boot_generation, live_verification = excluded.live_verification, \
             history_continuity = excluded.history_continuity, last_verified_operation_seq = excluded.last_verified_operation_seq, \
             last_source_sequence = excluded.last_source_sequence, updated_operation_seq = excluded.updated_operation_seq",
        )?
        .execute(params![
            tl,
            i64::from(u.source_id),
            u.source_epoch.map(sq),
            sq(boot),
            i64::from(u.live),
            i64::from(u.history),
            sq(u.verified_at_operation_seq),
            u.last_source_sequence.map(sq),
            sq(r.operation_seq)
        ])?;
        if let Some(g) = &u.gap {
            c.prepare_cached(
                "INSERT INTO source_history_gaps (timeline_no, source_id, source_epoch, from_seq, to_seq, recorded_operation_seq, \
                 bounds_unknown, reason, boot_generation) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?
            .execute(params![
                tl,
                i64::from(u.source_id),
                g.source_epoch.map(sq),
                g.from_seq.map(sq),
                g.to_seq.map(sq),
                sq(r.operation_seq),
                i64::from(g.bounds_unknown),
                i64::from(g.reason),
                sq(boot)
            ])?;
        }
    }
    if let Some(a) = &r.anomaly {
        c.prepare_cached("INSERT OR IGNORE INTO time_observations VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")?.execute(
            params![
                tl,
                sq(r.operation_seq),
                i64::from(a.kind),
                a.delta_ns,
                t.sequenced_at_utc_ns,
                sq(t.boottime_ns),
                sq(boot)
            ],
        )?;
    }
    Ok(())
}
