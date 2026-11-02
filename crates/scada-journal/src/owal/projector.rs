//! EXP-001 variant C: SQL projection **derived** from the open-wal log.
//!
//! The log is the only authority. The projector materializes the VS-006 SQLite schema
//! (same DDL, same `write_rows` code as the SQLite writer, so the historian API in
//! [`crate::reader`] works unchanged) in its own file, with its own watermark:
//! `projection_meta.as_of_operation_seq`, committed in the same SQL transaction as the
//! rows of the batch. It is idempotent on `(timeline, operation_seq)` (records at or below
//! the watermark are skipped) and refuses a gap. After a crash or the loss of the file it
//! is rebuilt from the log ([`Projector::catch_up`]). A read states the operation it is
//! up to date with (`reader::as_of`); it is never presented as the live state.
//!
//! No SQL runs on the WAL acknowledgement path: the writer hands durable records over a
//! bounded channel after the receipts.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::thread;
use std::time::Instant;

use open_wal::{Lsn, Wal};
use rusqlite::params;

use super::envelope;
use super::writer::ProjBatch;
use crate::progress::Hist;
use crate::store::{StartupCheck, Store, StoreOptions};
use crate::{Durability, JournalError, OpRecord, WriterMsg};

/// Progress of the projector, readable from other threads.
#[derive(Debug, Default)]
pub struct ProjShared {
    pub projected_op: AtomicU64,
    pub projected_event: AtomicU64,
    pub batches: AtomicU64,
    pub batch_latency: Hist,
    pub failed: AtomicU64,
}

#[derive(Debug)]
pub struct Projector {
    pub store: Store,
    wal_timeline: [u8; 16],
    pub as_of_op: u64,
    payload: Vec<u8>,
    pub shared: Arc<ProjShared>,
}

fn reason_for(t: &[u8; 16]) -> String {
    format!("owal-projection:{}", crate::hex(t))
}

impl Projector {
    /// Open (or create) the projection file for the log timeline `wal_timeline`. A file
    /// bound to another log is refused (the caller rebuilds it).
    pub fn open(
        path: &Path,
        durability: Durability,
        fingerprint: [u8; 32],
        wal_timeline: [u8; 16],
    ) -> Result<Projector, JournalError> {
        let o = StoreOptions {
            path: path.to_path_buf(),
            durability,
            startup_check: StartupCheck::Quick,
            cache_kib: 8192,
            test_max_pages: None,
        };
        let store = Store::open(&o, fingerprint)?;
        let tl = store.info.timeline_no;
        let want = reason_for(&wal_timeline);
        if store.info.created {
            store.conn.execute("UPDATE timelines SET reason = ?2 WHERE timeline_no = ?1", params![tl, want])?;
        } else {
            let got: String =
                store
                    .conn
                    .query_row("SELECT reason FROM timelines WHERE timeline_no = ?1", params![tl], |r| r.get(0))?;
            if got != want {
                return Err(JournalError::Incompatible(format!(
                    "{} is not the projection of this log ({got}); rebuild it",
                    path.display()
                )));
            }
        }
        let (as_of_event, as_of_op) = crate::reader::as_of(&store.conn, tl)?;
        let shared = Arc::new(ProjShared::default());
        shared.projected_op.store(as_of_op, Ordering::Release);
        shared.projected_event.store(as_of_event, Ordering::Release);
        Ok(Projector { store, wal_timeline, as_of_op, payload: Vec::with_capacity(4096), shared })
    }

    /// Delete the projection file (and its SQLite side files) and create it empty.
    pub fn rebuild(
        path: &Path,
        durability: Durability,
        fingerprint: [u8; 32],
        wal_timeline: [u8; 16],
    ) -> Result<Projector, JournalError> {
        for suffix in ["", "-wal", "-shm", ".lock"] {
            let mut p = path.as_os_str().to_owned();
            p.push(suffix);
            match std::fs::remove_file(PathBuf::from(p)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Self::open(path, durability, fingerprint, wal_timeline)
    }

    /// Materialize `records` (ascending) in one SQL transaction. Records at or below the
    /// watermark are skipped (idempotent); a gap is corruption.
    pub fn apply(&mut self, boot_generation: u64, records: Vec<Box<OpRecord>>) -> Result<(), JournalError> {
        let msgs: Vec<WriterMsg> =
            records.into_iter().filter(|r| r.operation_seq > self.as_of_op).map(WriterMsg::Op).collect();
        let Some(WriterMsg::Op(first)) = msgs.first() else { return Ok(()) };
        if first.operation_seq != self.as_of_op + 1 {
            return Err(JournalError::Corrupt(format!(
                "projection at operation {} received operation {} (gap)",
                self.as_of_op, first.operation_seq
            )));
        }
        let mut prev = self.as_of_op;
        for m in &msgs {
            if let WriterMsg::Op(r) = m {
                if r.operation_seq != prev + 1 {
                    return Err(JournalError::Corrupt(format!("projection batch gap after operation {prev}")));
                }
                prev = r.operation_seq;
            }
        }
        let t0 = Instant::now();
        let c = &self.store.conn;
        let tl = self.store.info.timeline_no;
        c.execute_batch("BEGIN IMMEDIATE")?;
        match crate::writer::write_rows(c, tl, boot_generation, &msgs, &mut self.payload, None) {
            Ok(_) => {
                if let Err(e) = c.execute_batch("COMMIT") {
                    let _ = c.execute_batch("ROLLBACK");
                    return Err(e.into());
                }
            }
            Err(e) => {
                let _ = c.execute_batch("ROLLBACK");
                return Err(e);
            }
        }
        self.as_of_op = prev;
        let max_event = msgs
            .iter()
            .filter_map(|m| if let WriterMsg::Op(r) = m { r.events.last().map(|e| e.event_seq) } else { None })
            .max();
        self.shared.projected_op.store(prev, Ordering::Release);
        if let Some(e) = max_event {
            self.shared.projected_event.fetch_max(e, Ordering::Release);
        }
        self.shared.batches.fetch_add(1, Ordering::Relaxed);
        self.shared.batch_latency.record_ns(u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX));
        Ok(())
    }

    /// Materialize every durable record after the watermark, read back from the log
    /// (startup, after a crash of the projector, after a rebuild).
    pub fn catch_up(&mut self, wal: &Wal, batch: usize) -> Result<u64, JournalError> {
        let durable = wal.durable_lsn().0;
        let mut done = 0u64;
        if self.as_of_op >= durable {
            if self.as_of_op > durable {
                return Err(JournalError::Corrupt(format!(
                    "projection at operation {} is ahead of the durable log ({durable})",
                    self.as_of_op
                )));
            }
            return Ok(0);
        }
        let mut rd = wal.reader_from(Lsn(self.as_of_op + 1)).map_err(super::map_wal)?;
        let mut cur: Vec<Box<OpRecord>> = Vec::with_capacity(batch);
        let mut cur_boot = 0u64;
        while let Some(item) = rd.next() {
            let (lsn, payload) = item.map_err(super::map_wal)?;
            if lsn.0 > durable {
                break;
            }
            let d = envelope::decode(payload).map_err(|e| JournalError::Corrupt(format!("LSN {lsn}: {e}")))?;
            if d.timeline_id != self.wal_timeline || d.record.operation_seq != lsn.0 {
                return Err(JournalError::Corrupt(format!("LSN {lsn}: wrong timeline or operation")));
            }
            if (!cur.is_empty() && d.boot_generation != cur_boot) || cur.len() >= batch.max(1) {
                let n = cur.len() as u64;
                self.apply(cur_boot, std::mem::take(&mut cur))?;
                done += n;
            }
            cur_boot = d.boot_generation;
            cur.push(Box::new(d.record));
        }
        if !cur.is_empty() {
            let n = cur.len() as u64;
            self.apply(cur_boot, cur)?;
            done += n;
        }
        Ok(done)
    }

    #[allow(clippy::vec_box)] // the writer hands over its boxes without copying
    fn apply_or_stop(&mut self, boot: u64, recs: Vec<Box<OpRecord>>) -> bool {
        match self.apply(boot, recs) {
            Ok(()) => true,
            Err(e) => {
                eprintln!("owal projector: stopped: {e}");
                self.shared.failed.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// Live projector thread: drains the writer's bounded channel, coalescing up to
    /// `max_records` per SQL transaction. A failure stops the projection (the log is
    /// unaffected; a later catch-up or rebuild recovers it).
    pub fn spawn(
        mut self,
        rx: Receiver<ProjBatch>,
        max_records: usize,
    ) -> std::io::Result<thread::JoinHandle<Projector>> {
        thread::Builder::new().name("scada-owal-projector".into()).spawn(move || {
            'outer: while let Ok(b) = rx.recv() {
                let mut boot = b.boot_generation;
                let mut recs = b.records;
                while recs.len() < max_records {
                    match rx.try_recv() {
                        Ok(more) if more.boot_generation == boot => recs.extend(more.records),
                        Ok(more) => {
                            if !self.apply_or_stop(boot, std::mem::take(&mut recs)) {
                                break 'outer;
                            }
                            boot = more.boot_generation;
                            recs = more.records;
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
                if !self.apply_or_stop(boot, recs) {
                    break 'outer;
                }
            }
            self
        })
    }
}
