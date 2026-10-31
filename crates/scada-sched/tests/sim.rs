//! The policy model is deterministic and reproduces the logical bounds of ADR 0010 §3.1.

use scada_sched::sim::{Outcome, Policy, Workload, run};

fn workload() -> Workload {
    Workload {
        hv: 16,
        hv_obs: 128,
        window: 32,
        rare: 1,
        rare_period_ns: 1_000_000,
        fixed_ns: 1_000,
        per_obs_ns: 30,
        duration_ns: 200_000_000,
        seed: 7,
    }
}

#[test]
fn model_is_deterministic() {
    let w = workload();
    for p in [Policy::Fifo { capacity: 64 }, Policy::RoundRobin { per_source: 4, quantum: 1 }] {
        let (a, b) = (run(p, &w), run(p, &w));
        assert_eq!(a.rare_latency_ns, b.rare_latency_ns);
        assert_eq!(a.hv_batches_per_s, b.hv_batches_per_s);
    }
}

#[test]
fn requests_ahead_of_the_rare_source_follow_the_policy_bounds() {
    let w = workload();
    let max_ahead = |p| run(p, &w).rare_ahead.last().copied().unwrap_or(0);
    // A: the queue plus one waiting batch per blocked session.
    assert!(max_ahead(Policy::Fifo { capacity: 64 }) <= 64 + 16);
    // B: at most `per_source` per noisy source ahead (+ waiting sessions bounded by it).
    for c in [1u32, 2, 4] {
        assert!(max_ahead(Policy::FifoCapped { capacity: 64, per_source: c }) <= u64::from(16 * c));
    }
    // C: independent of the lane depth: at most one turn per other active lane.
    for c in [1u32, 4, 8] {
        assert!(max_ahead(Policy::RoundRobin { per_source: c, quantum: 1 }) <= 16);
    }
    // Quantum q multiplies the bound.
    assert!(max_ahead(Policy::RoundRobin { per_source: 4, quantum: 4 }) <= 64);
}

#[test]
fn every_rare_request_is_served() {
    let w = workload();
    for p in [
        Policy::Fifo { capacity: 16 },
        Policy::FifoCapped { capacity: 64, per_source: 1 },
        Policy::RoundRobin { per_source: 1, quantum: 1 },
        Policy::Deficit { per_source: 4, quantum: 128 },
    ] {
        let o = run(p, &w);
        // ~0.75 ms mean period over 90 % of 200 ms.
        assert!(o.rare_n > 150, "{}: {}", o.label, o.rare_n);
        assert!(Outcome::pct(&o.rare_latency_ns, 1.0) < 10_000_000, "{}: starvation", o.label);
    }
}
