//! Bounded latency histogram (microseconds) for driver diagnostics and benchmarks.
//!
//! Log-linear buckets: exact below 8 µs, then 8 sub-buckets per power of two up to 2^40 µs
//! (≈ 12.7 days; larger values land in the last bucket). Fixed size (320 counters), no
//! allocation after creation. A quantile is reported as the **upper bound** of its bucket:
//! it overestimates by at most 12.5 %, never underestimates.

const SUB: usize = 8;
const MAX_EXP: u32 = 40;
const BUCKETS: usize = SUB + (MAX_EXP as usize - 3) * SUB;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencyHistogram {
    counts: Vec<u64>,
    n: u64,
    max_us: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        LatencyHistogram { counts: vec![0; BUCKETS], n: 0, max_us: 0 }
    }
}

fn index(us: u64) -> usize {
    if us < SUB as u64 {
        return us as usize;
    }
    let e = 63 - us.leading_zeros(); // >= 3
    if e >= MAX_EXP {
        return BUCKETS - 1;
    }
    let sub = ((us >> (e - 3)) & 7) as usize;
    (e as usize - 2) * SUB + sub
}

fn upper(idx: usize) -> u64 {
    if idx < SUB {
        return idx as u64;
    }
    let e = (idx / SUB + 2) as u32;
    let sub = (idx % SUB) as u64;
    let low = (8 + sub) << (e - 3);
    low + (1u64 << (e - 3)) - 1
}

impl LatencyHistogram {
    pub fn record_us(&mut self, us: u64) {
        let i = index(us).min(BUCKETS - 1);
        if let Some(c) = self.counts.get_mut(i) {
            *c = c.saturating_add(1);
        }
        self.n = self.n.saturating_add(1);
        self.max_us = self.max_us.max(us);
    }

    pub fn count(&self) -> u64 {
        self.n
    }

    pub fn max_us(&self) -> u64 {
        self.max_us
    }

    /// Upper bound of the bucket holding quantile `q` (0 < q <= 1); `None` when empty.
    pub fn quantile_us(&self, q: f64) -> Option<u64> {
        if self.n == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * self.n as f64).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return Some(upper(i).min(self.max_us));
            }
        }
        Some(self.max_us)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotonic_and_bounded() {
        let mut last = 0;
        for us in [0u64, 1, 7, 8, 9, 15, 16, 17, 100, 1000, 12_345, 1 << 30, (1 << 40) - 1, 1 << 41, u64::MAX] {
            let i = index(us);
            assert!(i < BUCKETS);
            assert!(i >= last, "{us}");
            last = i;
            if us < (1 << 40) {
                assert!(upper(i) >= us, "{us}");
                assert!(upper(i) as f64 <= us as f64 * 1.125 + 1.0, "{us}");
            }
        }
    }

    #[test]
    fn quantiles() {
        let mut h = LatencyHistogram::default();
        assert_eq!(h.quantile_us(0.5), None);
        for us in 1..=1000 {
            h.record_us(us);
        }
        let p50 = h.quantile_us(0.5).unwrap();
        assert!((500..=563).contains(&p50), "{p50}");
        assert_eq!(h.quantile_us(1.0), Some(1000));
        assert_eq!(h.count(), 1000);
    }
}
