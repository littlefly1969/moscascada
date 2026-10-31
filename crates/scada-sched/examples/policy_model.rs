//! Print the VS-003 policy model table (a MODEL, not a measurement; see `sim` docs).
//!
//! `cargo run --release -p scada-sched --example policy_model [-- --fixed-ns F --per-obs-ns P]`

use scada_sched::sim::{Outcome, Policy, Workload, run};

fn arg(name: &str, default: u64) -> u64 {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let base = Workload {
        hv: arg("--hv", 16) as u32,
        hv_obs: arg("--hv-obs", 128) as u32,
        window: arg("--window", 32) as u32,
        rare: arg("--rare", 1) as u32,
        rare_period_ns: arg("--rare-period-ns", 1_000_000),
        fixed_ns: arg("--fixed-ns", 1_000),
        per_obs_ns: arg("--per-obs-ns", 30),
        duration_ns: arg("--duration-ns", 2_000_000_000),
        seed: arg("--seed", 1),
    };
    let policies = [
        Policy::Fifo { capacity: 64 },
        Policy::Fifo { capacity: 16 },
        Policy::FifoCapped { capacity: 64, per_source: 1 },
        Policy::FifoCapped { capacity: 64, per_source: 2 },
        Policy::FifoCapped { capacity: 64, per_source: 4 },
        Policy::FifoCapped { capacity: 64, per_source: 8 },
        Policy::RoundRobin { per_source: 1, quantum: 1 },
        Policy::RoundRobin { per_source: 4, quantum: 1 },
        Policy::RoundRobin { per_source: 8, quantum: 1 },
        Policy::RoundRobin { per_source: 4, quantum: 4 },
        Policy::Deficit { per_source: 4, quantum: 128 },
    ];
    println!("model workload: {base:?}");
    println!(
        "| policy | hv batch/s | rare n | rare p50 µs | rare p99 µs | rare max µs | ahead p99 | ahead max | queue hw | busy |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for p in policies {
        let o = run(p, &base);
        let us = |v: u64| v as f64 / 1000.0;
        println!(
            "| {} | {:.0} | {} | {:.1} | {:.1} | {:.1} | {} | {} | {} | {:.2} |",
            o.label,
            o.hv_batches_per_s,
            o.rare_n,
            us(Outcome::pct(&o.rare_latency_ns, 0.50)),
            us(Outcome::pct(&o.rare_latency_ns, 0.99)),
            us(o.rare_latency_ns.last().copied().unwrap_or(0)),
            Outcome::pct(&o.rare_ahead, 0.99),
            o.rare_ahead.last().copied().unwrap_or(0),
            o.queue_high_watermark,
            o.sequencer_busy
        );
    }
}
