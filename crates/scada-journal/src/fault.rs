//! TEST-ONLY crash points (brief §6 "fault points"). Armed only by the
//! `--test-fault <point>:<n>` flag of the kernel: the `n`-th time `point` is reached the
//! process calls `abort()` (no destructor, no flush: equivalent to `kill -9` at that
//! instruction). Unarmed, every [`hit`] is one relaxed load.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Every fault point name, in pipeline order.
pub const POINTS: &[&str] = &[
    "before_apply",
    "after_apply_before_enqueue",
    "after_enqueue",
    "before_transaction",
    "during_transaction",
    "after_commit_before_watermark",
    "during_checkpoint",
    "after_durable_ack_before_socket_write",
];

static ARMED: AtomicBool = AtomicBool::new(false);
static TARGET: OnceLock<(&'static str, u64)> = OnceLock::new();
static HITS: AtomicU64 = AtomicU64::new(0);

/// Arm `point` to abort on its `n`-th hit (`n >= 1`). Errors on an unknown point.
pub fn arm(spec: &str) -> Result<(), String> {
    let (p, n) = spec.split_once(':').ok_or_else(|| format!("--test-fault expects <point>:<n>, got {spec:?}"))?;
    let n: u64 = n.parse().map_err(|_| format!("--test-fault: invalid count {n:?}"))?;
    let point = POINTS
        .iter()
        .find(|x| **x == p)
        .ok_or_else(|| format!("--test-fault: unknown point {p:?} (known: {POINTS:?})"))?;
    if n == 0 {
        return Err("--test-fault: count must be >= 1".into());
    }
    TARGET.set((point, n)).map_err(|_| "--test-fault given twice".to_string())?;
    ARMED.store(true, Ordering::Release);
    Ok(())
}

#[inline]
pub fn hit(point: &'static str) {
    if !ARMED.load(Ordering::Relaxed) {
        return;
    }
    if let Some((p, n)) = TARGET.get()
        && *p == point
        && HITS.fetch_add(1, Ordering::AcqRel) + 1 == *n
    {
        eprintln!("TEST FAULT: abort at {point} (hit {n})");
        std::process::abort();
    }
}
