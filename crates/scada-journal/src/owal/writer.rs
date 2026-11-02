//! EXP-001 writer thread on open-wal: same message interface ([`WriterMsg`]), same shared
//! progress ([`JournalShared`]) and the same group-commit loop as the SQLite writer
//! (`crate::writer`), so a harness can swap one for the other without touching the
//! sequencer side.
//!
//! Per group: every operation is encoded into **one** record and appended (memory only),
//! then one `Wal::commit` (write + fdatasync). Durable progress (`durable_op`,
//! `durable_event`), reservations and receipts are published only after `commit`
//! returned an LSN covering the operation (J1). A commit split at a segment roll can make
//! only a prefix durable before an error: the prefix is reported durable (it is, and it
//! will be replayed), the rest `NotDurable`, and the writer enters FAULT. open-wal
//! poisons its handle on any write/fdatasync/segment-creation failure, so there is no
//! retry: FAULT until restart and recovery (EXP-001 §2.5). This includes `ENOSPC`, which
//! the SQLite writer retries (D13) — a deliberate, documented difference.
//!
//! Optional derived projection: after the receipts, the group's records are handed to the
//! projector over a **bounded** channel. A full channel blocks the writer (backpressure);
//! it never drops a record, so the projection can lag but never skip.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use open_wal::{Wal, WalError};
use scada_core::{AlarmEvent, Kernel, ValidatedConfig};

use super::{CheckpointHeader, Manifest, OwalStore, envelope, write_checkpoint};
use crate::progress::{JournalShared, JournalState};
use crate::{CommitResult, OpRecord, WriterMsg, fault};

#[derive(Debug, Clone)]
pub struct OwalWriterOptions {
    pub commit_max_operations: usize,
    pub commit_max_delay: Duration,
    pub checkpoints_keep: u32,
    pub boot_generation: u64,
    /// Highest logical time already in the log (from recovery), carried into checkpoints.
    pub last_logical_time_ns: i64,
    /// Ring window events needed by checkpoints are tracked by the writer (the checkpoint
    /// file is self-contained: open-wal has no event index to reload them from).
    pub config: Arc<ValidatedConfig>,
    /// TEST-ONLY: the n-th commit is not attempted and the writer enters FAULT as after an
    /// EIO (open-wal itself has no injection hook; real I/O failures are tested with
    /// `RLIMIT_FSIZE` in `tests/owal.rs`).
    pub test_eio_at_commit: Option<u64>,
    /// TEST-ONLY: sleep before each commit (slow disk).
    pub test_delay: Duration,
}

/// A group of durable records for the projector, with the boot that wrote them.
#[derive(Debug)]
pub struct ProjBatch {
    pub boot_generation: u64,
    pub records: Vec<Box<OpRecord>>,
}

/// Ring events the first checkpoints after a recovery need (taken from the recovered
/// kernel, at most the outbox capacity).
pub fn ring_seed(k: &Kernel) -> Vec<AlarmEvent> {
    let Some(s) = k.outbox_status() else { return Vec::new() };
    let newest = s.next_event_seq.saturating_sub(1);
    if newest == 0 {
        return Vec::new();
    }
    let oldest = if newest >= s.capacity { newest - s.capacity + 1 } else { 1 };
    (oldest..=newest).filter_map(|e| k.alarm_event(e)).collect()
}

#[allow(clippy::too_many_arguments)]
pub fn spawn(
    store: OwalStore,
    wal: Wal,
    shared: Arc<JournalShared>,
    rx: Receiver<WriterMsg>,
    opts: OwalWriterOptions,
    ring_seed: Vec<AlarmEvent>,
    projector: Option<SyncSender<ProjBatch>>,
    wake: Box<dyn Fn() + Send>,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new().name("scada-journal-owal".into()).spawn(move || {
        let cap = opts.config.alarm_table().map_or(0, |t| t.outbox_capacity()) as usize;
        let opts_last_logical = opts.last_logical_time_ns;
        let mut ring = VecDeque::with_capacity(cap);
        for e in ring_seed.into_iter().rev().take(cap).collect::<Vec<_>>().into_iter().rev() {
            ring.push_back(e);
        }
        let mut w = Writer {
            manifest: store.manifest,
            store,
            wal,
            shared,
            opts,
            wake,
            projector,
            buf: Vec::with_capacity(4096),
            scratch: Vec::with_capacity(256),
            commits: 0,
            ring,
            cap,
            last_logical: opts_last_logical,
        };
        w.run(rx);
    })
}

struct Writer {
    store: OwalStore,
    manifest: Manifest,
    wal: Wal,
    shared: Arc<JournalShared>,
    opts: OwalWriterOptions,
    wake: Box<dyn Fn() + Send>,
    projector: Option<SyncSender<ProjBatch>>,
    buf: Vec<u8>,
    scratch: Vec<u8>,
    commits: u64,
    ring: VecDeque<AlarmEvent>,
    cap: usize,
    last_logical: i64,
}

struct PendingCheckpoint {
    msg: Box<crate::CheckpointMsg>,
    ring: Vec<AlarmEvent>,
    last_logical_time_ns: i64,
}

fn ops_in(group: &[WriterMsg]) -> usize {
    group.iter().filter(|m| matches!(m, WriterMsg::Op(_))).count()
}

impl Writer {
    fn run(&mut self, rx: Receiver<WriterMsg>) {
        loop {
            let first = match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(m) => m,
                Err(RecvTimeoutError::Timeout) => continue,
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

    fn fault(&mut self, why: String) {
        eprintln!("journal(open-wal): FAULT, no further commit: {why}");
        self.shared.set_error(why);
        self.shared.state.store(JournalState::Fault as u8, Release);
    }

    fn process(&mut self, group: Vec<WriterMsg>) {
        if self.shared.state() == JournalState::Fault {
            self.finish(group, 0);
            (self.wake)();
            return;
        }
        if !self.opts.test_delay.is_zero() {
            thread::sleep(self.opts.test_delay);
        }
        let t0 = Instant::now();
        fault::hit("before_transaction");
        self.commits += 1;
        let mut checkpoints = Vec::new();
        let mut appended_any = false;
        let mut err: Option<String> = None;
        for m in &group {
            match m {
                WriterMsg::Op(r) => {
                    envelope::encode(
                        r,
                        &self.manifest.timeline_id,
                        self.opts.boot_generation,
                        &mut self.buf,
                        &mut self.scratch,
                    );
                    match self.wal.append(&self.buf) {
                        Ok(lsn) if lsn.0 == r.operation_seq => {}
                        Ok(lsn) => {
                            err = Some(format!("LSN {lsn} assigned to operation {} (mapping broken)", r.operation_seq));
                            break;
                        }
                        Err(WalError::RecordTooLarge) => {
                            // Admission must have refused it before apply (worst_case_len);
                            // reaching here is a bug: nothing of the operation is written.
                            err = Some(format!(
                                "operation {} encodes to {} B > max_record_size {} (RecordTooLarge)",
                                r.operation_seq,
                                self.buf.len(),
                                self.store.options.max_record_size
                            ));
                            break;
                        }
                        Err(e) => {
                            err = Some(format!("append: {e}"));
                            break;
                        }
                    }
                    if !appended_any {
                        appended_any = true;
                        fault::hit("during_transaction");
                    }
                    self.last_logical = self.last_logical.max(r.time.logical_time_ns);
                    for e in &r.events {
                        if self.ring.len() == self.cap && self.cap > 0 {
                            self.ring.pop_front();
                        }
                        if self.cap > 0 {
                            self.ring.push_back(*e);
                        }
                    }
                }
                WriterMsg::Checkpoint(c) => {
                    let ring = match Kernel::checkpoint_ring_window(&self.opts.config, &c.checkpoint) {
                        Some((a, b)) => {
                            self.ring.iter().filter(|e| e.event_seq >= a && e.event_seq <= b).copied().collect()
                        }
                        None => Vec::new(),
                    };
                    checkpoints.push((c.checkpoint.last_operation_sequence, ring, self.last_logical));
                }
                WriterMsg::Notify { .. } => {}
            }
        }
        let durable = if let Some(e) = err {
            // Records already staged are not committed by us; the handle is abandoned
            // (FAULT) so they never become durable from this process.
            self.fault(e);
            self.wal.durable_lsn().0
        } else if self.opts.test_eio_at_commit == Some(self.commits) {
            self.fault("TEST: simulated EIO at commit".into());
            self.wal.durable_lsn().0
        } else {
            match self.wal.commit() {
                Ok(l) => l.0,
                Err(e) => {
                    let d = self.wal.durable_lsn().0;
                    self.fault(format!("commit: {e} (durable prefix up to LSN {d})"));
                    d
                }
            }
        };
        if appended_any {
            self.shared.commit_latency.record_ns(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
        }
        fault::hit("after_commit_before_watermark");
        // Checkpoints leave the group here with their ring snapshot (same order); only
        // those whose operation is durable, and never in FAULT, are written.
        let mut pending = Vec::new();
        let mut snaps = checkpoints.into_iter();
        let mut rest = Vec::with_capacity(group.len());
        for m in group {
            match m {
                WriterMsg::Checkpoint(c) => {
                    self.shared.checkpoints_done.fetch_add(1, Relaxed);
                    if let Some((op, ring, logical)) = snaps.next()
                        && op <= durable
                        && self.shared.state() != JournalState::Fault
                    {
                        pending.push(PendingCheckpoint { msg: c, ring, last_logical_time_ns: logical });
                    }
                }
                other => rest.push(other),
            }
        }
        self.finish(rest, durable);
        (self.wake)();
        for c in pending {
            let t = Instant::now();
            let h = CheckpointHeader {
                operation_seq: c.msg.checkpoint.last_operation_sequence,
                event_seq_high: c.msg.event_seq_high,
                boot_generation: self.opts.boot_generation,
                last_logical_time_ns: c.last_logical_time_ns,
                state_digest: c.msg.state_digest,
            };
            let r = write_checkpoint(
                &self.store.dir,
                &self.manifest,
                &h,
                &c.msg.checkpoint,
                &c.ring,
                self.opts.checkpoints_keep,
            );
            match r {
                Ok(()) => {
                    self.shared.checkpoints_written.fetch_add(1, Relaxed);
                    self.shared.last_checkpoint_op.store(h.operation_seq, Relaxed);
                    self.shared.checkpoint_write.record_ns(u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX));
                }
                // A checkpoint only shortens recovery: a failure is reported, never FAULT
                // (the log keeps every record; retention is disabled).
                Err(e) => self.shared.set_error(format!("checkpoint skipped: {e}")),
            }
        }
    }

    /// Publish what is durable (`<= durable`), report the rest `NotDurable`, release
    /// reservations, hand durable records to the projector.
    fn finish(&mut self, group: Vec<WriterMsg>, durable: u64) {
        let mut ops = 0u64;
        let mut events = 0u64;
        let mut bytes = 0u64;
        let mut max_event = 0u64;
        let mut last_op = 0u64;
        let now_boot = crate::boottime_ns();
        let mut released_bytes = 0u64;
        let mut released_ops = 0u64;
        let mut proj = Vec::new();
        let mut receipts: Vec<(crate::Receipt, CommitResult)> = Vec::new();
        for m in group {
            match m {
                WriterMsg::Op(mut r) => {
                    released_ops += 1;
                    released_bytes += r.reserved_bytes;
                    let ok = r.operation_seq <= durable;
                    if ok {
                        ops += 1;
                        events += r.events.len() as u64;
                        bytes += r.reserved_bytes;
                        last_op = last_op.max(r.operation_seq);
                        if let Some(e) = r.events.last() {
                            max_event = max_event.max(e.event_seq);
                        }
                        self.shared.durable_lag.record_ns(now_boot.saturating_sub(r.time.boottime_ns));
                    }
                    if let Some(rc) = r.receipt.take() {
                        receipts.push((rc, if ok { CommitResult::Durable } else { CommitResult::NotDurable }));
                    }
                    if ok && self.projector.is_some() {
                        proj.push(r);
                    }
                }
                WriterMsg::Notify { operation_seq, receipt } => {
                    released_ops += 1;
                    released_bytes += crate::NOTIFY_BYTES;
                    let ok = operation_seq <= durable;
                    receipts.push((receipt, if ok { CommitResult::Durable } else { CommitResult::NotDurable }));
                }
                WriterMsg::Checkpoint(_) => {
                    // FAULT path: not written.
                    self.shared.checkpoints_done.fetch_add(1, Relaxed);
                }
            }
        }
        // Publication strictly after commit returned (J1).
        if ops > 0 {
            self.shared.durable_op.fetch_max(last_op, Release);
        }
        if max_event > 0 {
            self.shared.durable_event.fetch_max(max_event, Release);
        }
        self.shared.reserved_ops.fetch_sub(released_ops, Relaxed);
        self.shared.reserved_bytes.fetch_sub(released_bytes, Relaxed);
        if ops > 0 {
            self.shared.commits.fetch_add(1, Relaxed);
            self.shared.committed_ops.fetch_add(ops, Relaxed);
            self.shared.committed_events.fetch_add(events, Relaxed);
            self.shared.committed_bytes.fetch_add(bytes, Relaxed);
            self.shared.group_size.record_ns(ops);
        }
        for (rc, res) in receipts {
            rc(res);
        }
        if let Some(tx) = &self.projector
            && !proj.is_empty()
        {
            // Bounded: blocks when the projector lags (backpressure, never a drop).
            if tx.send(ProjBatch { boot_generation: self.opts.boot_generation, records: proj }).is_err() {
                self.shared.set_error("projector gone: the derived projection stops (WAL unaffected)");
                self.projector = None;
            }
        }
    }
}
