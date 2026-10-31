//! Optional, bounded stage timing of the ingestion path (brief §3 A2).
//!
//! Disabled unless the kernel is started with `--diag`. When disabled nothing is
//! allocated and the hot path pays one branch on an `Option` per probe point. When
//! enabled every probe is one `Instant::now()` plus two relaxed atomic increments into a
//! fixed log-linear histogram: no allocation, no string, no event per request.
//!
//! Wall-clock values never reach the kernel, its state or its digest: they live in this
//! module only and are exported through the read-only introspection socket.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

/// Linear buckets for values below this many nanoseconds.
const LINEAR: u64 = 64;
/// Sub-buckets per power of two above `LINEAR` (relative resolution 1/32 ≈ 3 %).
const SUB_BITS: u32 = 5;
const SUB: u64 = 1 << SUB_BITS;
/// Powers of two covered above `LINEAR` (2^6 ns .. 2^41 ns ≈ 36 min).
const OCTAVES: u64 = 35;
const BUCKETS: usize = (LINEAR + OCTAVES * SUB) as usize;

/// Fixed-size log-linear histogram of nanosecond values.
#[derive(Debug)]
pub struct Histogram {
    buckets: Box<[AtomicU64]>,
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

fn bucket_of(v: u64) -> usize {
    if v < LINEAR {
        return v as usize;
    }
    let e = 63 - u64::from(v.leading_zeros()); // >= 6
    let octave = e - 6;
    if octave >= OCTAVES {
        return BUCKETS - 1;
    }
    let sub = (v >> (e - u64::from(SUB_BITS))) & (SUB - 1);
    (LINEAR + octave * SUB + sub) as usize
}

/// Lower bound (inclusive) of a bucket, in nanoseconds.
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

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            buckets: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    pub fn record_ns(&self, ns: u64) {
        if let Some(b) = self.buckets.get(bucket_of(ns)) {
            b.fetch_add(1, Relaxed);
        }
        self.count.fetch_add(1, Relaxed);
        self.sum.fetch_add(ns, Relaxed);
        self.max.fetch_max(ns, Relaxed);
    }

    pub fn record(&self, d: Duration) {
        self.record_ns(u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    }

    pub fn since(&self, t0: Instant) {
        self.record(t0.elapsed());
    }

    /// Racy reset (diagnostics only; concurrent records may survive it).
    pub fn reset(&self) {
        for b in self.buckets.iter() {
            b.store(0, Relaxed);
        }
        self.count.store(0, Relaxed);
        self.sum.store(0, Relaxed);
        self.max.store(0, Relaxed);
    }

    /// Nearest-rank percentile, reported as the lower bound of its bucket.
    fn percentile(&self, counts: &[u64], total: u64, q: f64) -> u64 {
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
    }

    pub fn summary(&self) -> Summary {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let total: u64 = counts.iter().sum();
        Summary {
            count: total,
            sum_ns: self.sum.load(Relaxed),
            p50_ns: self.percentile(&counts, total, 0.50),
            p99_ns: self.percentile(&counts, total, 0.99),
            p999_ns: self.percentile(&counts, total, 0.999),
            max_ns: self.max.load(Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub count: u64,
    pub sum_ns: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

/// Probe points. Elapsed (wall) time unless noted; waits are not CPU time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// First header byte available → payload complete (socket read of one frame).
    FrameRead,
    /// `DriverMessage::decode` (CPU).
    Decode,
    /// Protocol → core conversion (CPU).
    Convert,
    /// Wait for a free ACK slot of the connection (`max_inflight_per_connection`).
    SlotWait,
    /// Wait to enter the bounded sequencer queue (0 unless it is full).
    SubmitWait,
    /// Submit started → request taken by the sequencer (includes `SubmitWait`).
    QueueWait,
    /// `Kernel::apply` plus building the reply, on the sequencer thread (CPU).
    Apply,
    /// Reply sent by the sequencer → received by the session writer (wake-up latency
    /// plus head-of-line waiting behind earlier ACKs of the same connection).
    ReplyWait,
    /// Encoding and writing one ACK (socket write).
    AckWrite,
    /// Frame complete → its ACK written: time the batch spent inside the kernel process.
    InKernel,
    /// `QueueWait` of the probe source only (see `--diag-probe-source`).
    ProbeQueueWait,
    /// `InKernel` of the probe source only.
    ProbeInKernel,
}

pub const STAGES: [Stage; 12] = [
    Stage::FrameRead,
    Stage::Decode,
    Stage::Convert,
    Stage::SlotWait,
    Stage::SubmitWait,
    Stage::QueueWait,
    Stage::Apply,
    Stage::ReplyWait,
    Stage::AckWrite,
    Stage::InKernel,
    Stage::ProbeQueueWait,
    Stage::ProbeInKernel,
];

impl Stage {
    pub fn name(self) -> &'static str {
        match self {
            Stage::FrameRead => "frame_read",
            Stage::Decode => "decode",
            Stage::Convert => "convert",
            Stage::SlotWait => "slot_wait",
            Stage::SubmitWait => "submit_wait",
            Stage::QueueWait => "queue_wait",
            Stage::Apply => "apply",
            Stage::ReplyWait => "reply_wait",
            Stage::AckWrite => "ack_write",
            Stage::InKernel => "in_kernel",
            Stage::ProbeQueueWait => "probe_queue_wait",
            Stage::ProbeInKernel => "probe_in_kernel",
        }
    }
}

/// All histograms. Allocated once at startup when diagnostics are enabled (~12 × 9 KiB).
#[derive(Debug)]
pub struct Diag {
    hist: Box<[Histogram]>,
    /// Source whose queue wait and in-kernel time are also recorded separately.
    pub probe_source: Option<u32>,
}

impl Diag {
    pub fn new(probe_source: Option<u32>) -> Diag {
        Diag { hist: STAGES.iter().map(|_| Histogram::default()).collect(), probe_source }
    }

    pub fn h(&self, s: Stage) -> Option<&Histogram> {
        // STAGES lists the variants in declaration order, so the discriminant is the index.
        self.hist.get(s as usize)
    }

    pub fn since(&self, s: Stage, t0: Instant) {
        if let Some(h) = self.h(s) {
            h.since(t0);
        }
    }

    pub fn record_ns(&self, s: Stage, v: u64) {
        if let Some(h) = self.h(s) {
            h.record_ns(v);
        }
    }

    pub fn is_probe(&self, source: u32) -> bool {
        self.probe_source == Some(source)
    }

    pub fn reset(&self) {
        for h in self.hist.iter() {
            h.reset();
        }
    }

    pub fn summaries(&self) -> impl Iterator<Item = (Stage, Summary)> + '_ {
        STAGES.iter().zip(self.hist.iter()).map(|(s, h)| (*s, h.summary()))
    }
}

/// `Some(Instant::now())` only when diagnostics are on.
#[inline]
pub fn now(d: Option<&Diag>) -> Option<Instant> {
    d.map(|_| Instant::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_cover_their_values() {
        let mut prev = 0;
        for v in (0..5_000u64).chain([1 << 20, (1 << 20) + 12_345, 1 << 39, u64::MAX]) {
            let b = bucket_of(v);
            assert!(b >= prev || v > 5_000, "non-monotonic at {v}");
            if v < 5_000 {
                prev = b;
            }
            assert!(b < BUCKETS);
            if b < BUCKETS - 1 {
                assert!(bucket_low(b) <= v && v < bucket_low(b + 1), "{v} not in bucket {b}");
            }
        }
    }

    #[test]
    fn stage_discriminants_match_table() {
        for (i, s) in STAGES.iter().enumerate() {
            assert_eq!(*s as usize, i);
        }
    }

    #[test]
    fn percentiles_are_within_resolution() {
        let h = Histogram::default();
        for v in 1..=100_000u64 {
            h.record_ns(v * 10);
        }
        let s = h.summary();
        assert_eq!(s.count, 100_000);
        let close = |got: u64, want: u64| (got as f64 - want as f64).abs() / want as f64 <= 1.0 / 32.0 + 1e-9;
        assert!(close(s.p50_ns, 500_000), "{s:?}");
        assert!(close(s.p99_ns, 990_000), "{s:?}");
        assert!(close(s.p999_ns, 999_000), "{s:?}");
        assert_eq!(s.max_ns, 1_000_000);
        h.reset();
        assert_eq!(h.summary().count, 0);
    }
}
