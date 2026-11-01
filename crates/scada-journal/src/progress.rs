//! Shared journal progress: published by the writer after `COMMIT`, read by the sequencer
//! (one relaxed load per admission) and by the historian/diagnostics. Metrics only, never
//! part of the kernel state.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering::Relaxed};

const LINEAR: u64 = 64;
const SUB_BITS: u32 = 5;
const SUB: u64 = 1 << SUB_BITS;
const OCTAVES: u64 = 35;
const BUCKETS: usize = (LINEAR + OCTAVES * SUB) as usize;

/// Fixed log-linear histogram of nanosecond values (same layout as `scada-runtime::diag`).
#[derive(Debug)]
pub struct Hist {
    buckets: Box<[AtomicU64]>,
    count: AtomicU64,
    max: AtomicU64,
}

impl Default for Hist {
    fn default() -> Self {
        Hist {
            buckets: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

fn bucket_of(v: u64) -> usize {
    if v < LINEAR {
        return v as usize;
    }
    let e = 63 - u64::from(v.leading_zeros());
    let octave = e - 6;
    if octave >= OCTAVES {
        return BUCKETS - 1;
    }
    let sub = (v >> (e - u64::from(SUB_BITS))) & (SUB - 1);
    (LINEAR + octave * SUB + sub) as usize
}

fn bucket_low(i: usize) -> u64 {
    let i = i as u64;
    if i < LINEAR {
        return i;
    }
    let octave = (i - LINEAR) / SUB;
    let sub = (i - LINEAR) % SUB;
    let e = octave + 6;
    (1u64 << e) + (sub << (e - u64::from(SUB_BITS)))
}

/// Percentiles (lower bound of the bucket, relative resolution ≈ 3 %).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatSummary {
    pub count: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

impl Hist {
    pub fn record_ns(&self, ns: u64) {
        if let Some(b) = self.buckets.get(bucket_of(ns)) {
            b.fetch_add(1, Relaxed);
        }
        self.count.fetch_add(1, Relaxed);
        self.max.fetch_max(ns, Relaxed);
    }

    pub fn reset(&self) {
        for b in self.buckets.iter() {
            b.store(0, Relaxed);
        }
        self.count.store(0, Relaxed);
        self.max.store(0, Relaxed);
    }

    pub fn summary(&self) -> LatSummary {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let total: u64 = counts.iter().sum();
        let pct = |q: f64| -> u64 {
            if total == 0 {
                return 0;
            }
            let rank = ((q * total as f64).ceil() as u64).clamp(1, total);
            let mut acc = 0u64;
            for (i, c) in counts.iter().enumerate() {
                acc = acc.saturating_add(*c);
                if acc >= rank {
                    return bucket_low(i);
                }
            }
            bucket_low(BUCKETS - 1)
        };
        LatSummary {
            count: total,
            p50_ns: pct(0.50),
            p99_ns: pct(0.99),
            p999_ns: pct(0.999),
            max_ns: self.max.load(Relaxed),
        }
    }
}

/// Writer state (`scada.history.v1.JournalState` numbers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JournalState {
    Ok = 1,
    Backpressure = 2,
    DiskFull = 3,
    Fault = 4,
}

impl JournalState {
    pub fn from_u8(v: u8) -> JournalState {
        match v {
            2 => JournalState::Backpressure,
            3 => JournalState::DiskFull,
            4 => JournalState::Fault,
            _ => JournalState::Ok,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            JournalState::Ok => "OK",
            JournalState::Backpressure => "BACKPRESSURE",
            JournalState::DiskFull => "DISK_FULL",
            JournalState::Fault => "FAULT",
        }
    }
}

/// Everything the writer publishes. Sequences only move forward.
#[derive(Debug)]
pub struct JournalShared {
    /// Highest operation sequence contained in a returned `COMMIT` (J1).
    pub durable_op: AtomicU64,
    /// Highest event sequence contained in a returned `COMMIT`.
    pub durable_event: AtomicU64,
    /// Admission reservations (J4): incremented by the sequencer before `apply`,
    /// decremented by the sequencer (not applied) or by the writer (committed).
    pub reserved_ops: AtomicU64,
    pub reserved_bytes: AtomicU64,
    pub reserved_ops_high: AtomicU64,
    pub reserved_bytes_high: AtomicU64,
    /// [`JournalState`] as `u8` (set by the writer).
    pub state: AtomicU8,
    pub commits: AtomicU64,
    pub committed_ops: AtomicU64,
    pub committed_events: AtomicU64,
    pub committed_bytes: AtomicU64,
    pub checkpoints_written: AtomicU64,
    /// ADR 0013 D13: highest operation whose replay payload was compacted (0 = none).
    pub compacted_operations: AtomicU64,
    pub last_checkpoint_op: AtomicU64,
    /// Checkpoint messages processed (written, skipped or failed).
    pub checkpoints_done: AtomicU64,
    pub wal_bytes: AtomicU64,
    pub db_bytes: AtomicU64,
    pub disk_free: AtomicU64,
    pub refused_queue_full: AtomicU64,
    pub refused_disk_full: AtomicU64,
    pub refused_fault: AtomicU64,
    pub commit_retries: AtomicU64,
    /// Last persisted logical time (written with every commit).
    pub last_logical: AtomicI64,
    /// `COMMIT` duration (write + fsync) per transaction.
    pub commit_latency: Hist,
    /// Boottime at `COMMIT` return minus boottime when the operation was sequenced.
    pub durable_lag: Hist,
    /// Checkpoint export pause on the sequencer.
    pub checkpoint_pause: Hist,
    /// Writer time to store one checkpoint (commits wait behind it).
    pub checkpoint_write: Hist,
    /// Operations per committed transaction.
    pub group_size: Hist,
    pub last_error: Mutex<String>,
    /// Set by the host at shutdown: a writer stuck retrying `SQLITE_FULL` gives up (the
    /// pending receipts report `NotDurable`).
    pub shutdown: AtomicBool,
}

impl JournalShared {
    pub fn new(durable_op: u64, durable_event: u64) -> JournalShared {
        JournalShared {
            durable_op: AtomicU64::new(durable_op),
            durable_event: AtomicU64::new(durable_event),
            reserved_ops: AtomicU64::new(0),
            reserved_bytes: AtomicU64::new(0),
            reserved_ops_high: AtomicU64::new(0),
            reserved_bytes_high: AtomicU64::new(0),
            state: AtomicU8::new(JournalState::Ok as u8),
            commits: AtomicU64::new(0),
            committed_ops: AtomicU64::new(0),
            committed_events: AtomicU64::new(0),
            committed_bytes: AtomicU64::new(0),
            checkpoints_written: AtomicU64::new(0),
            compacted_operations: AtomicU64::new(0),
            last_checkpoint_op: AtomicU64::new(0),
            checkpoints_done: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
            db_bytes: AtomicU64::new(0),
            disk_free: AtomicU64::new(u64::MAX),
            refused_queue_full: AtomicU64::new(0),
            refused_disk_full: AtomicU64::new(0),
            refused_fault: AtomicU64::new(0),
            commit_retries: AtomicU64::new(0),
            last_logical: AtomicI64::new(i64::MIN),
            commit_latency: Hist::default(),
            durable_lag: Hist::default(),
            checkpoint_pause: Hist::default(),
            checkpoint_write: Hist::default(),
            group_size: Hist::default(),
            last_error: Mutex::new(String::new()),
            shutdown: AtomicBool::new(false),
        }
    }

    pub fn state(&self) -> JournalState {
        JournalState::from_u8(self.state.load(Relaxed))
    }

    pub fn set_error(&self, e: impl Into<String>) {
        if let Ok(mut g) = self.last_error.lock() {
            *g = e.into();
        }
    }

    pub fn error(&self) -> String {
        self.last_error.lock().map(|g| g.clone()).unwrap_or_default()
    }
}
