//! VS-004 review P1: cost of one watchdog tick on an entirely stale plant, in process
//! (controlled time, no IPC, no other thread).
//!
//! ```text
//! cargo run --release -p scada-runtime --example stale_tick -- [SOURCES] [CHANNELS] [TICKS]
//! ```
//!
//! `SOURCES` sources of `CHANNELS` i64 channels (freshness 300 ms, poll 10 ms), each
//! connected with a FullState; the clock then advances past the timeout so that every tag
//! expires, and `TICKS` further ticks (nothing left to expire) are timed one by one.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use scada_core::{
    BatchMode, ChannelId, Kernel, Observation, ObservationBatch, Operation, SequencedInput, SourceId, Value,
};
use scada_runtime::config::parse;
use scada_runtime::sequencer::Counters;
use scada_runtime::supervision::Supervisor;

fn main() {
    let a: Vec<u64> = std::env::args().skip(1).map(|x| x.parse().expect("number")).collect();
    let sources = a.first().copied().unwrap_or(255) as u32;
    let channels = a.get(1).copied().unwrap_or(4096) as u32;
    let ticks = a.get(2).copied().unwrap_or(1000);
    let mut c = format!(
        "[ipc]\nsocket_path = \"/run/x/d.sock\"\nmax_frame_bytes = 131072\n[runtime]\nwatchdog_poll_ms = 10\n\
         [core]\nmax_observations_per_batch = {channels}\n"
    );
    for s in 1..=sources {
        for ch in 1..=channels {
            c.push_str(&format!("[[tags]]\nid = {}\nname = \"s{s}c{ch}\"\ntype = \"i64\"\n", s * 10_000 + ch));
        }
    }
    for s in 1..=sources {
        c.push_str(&format!("[[sources]]\nid = {s}\nname = \"s{s}\"\nfreshness_timeout_ms = 300\nchannels = [\n"));
        for ch in 1..=channels {
            c.push_str(&format!("{{ channel = {ch}, tag = {}, type = \"i64\" }},\n", s * 10_000 + ch));
        }
        c.push_str("]\n");
    }
    let cfg = parse(&c).expect("config");
    let mut k = Kernel::new(Arc::new(cfg.core.clone()));
    let mut sup = Supervisor::new(&cfg).expect("supervised");
    let counters = Counters::default();
    let mut apply = |k: &mut Kernel, operation: Operation| {
        let operation_sequence = k.last_operation_sequence() + 1;
        k.apply(SequencedInput { operation_sequence, operation }).ok()
    };
    let ms = Duration::from_millis;
    for s in 1..=sources {
        let g = u64::from(s);
        apply(&mut k, Operation::SourceConnected { source_id: SourceId(s), link_generation: g, source_epoch: 1 });
        sup.on_connected((s - 1) as usize, g, Some(Arc::new(AtomicU64::new(0))), &k);
        let obs = (1..=channels).map(|ch| Observation { channel_id: ChannelId(ch), value: Value::I64(1) }).collect();
        let b = ObservationBatch {
            source_id: SourceId(s),
            link_generation: g,
            mode: BatchMode::FullState,
            source_sequence: 1,
            observations: obs,
        };
        apply(&mut k, Operation::ObservationBatch(b)).expect("full state");
    }
    // Ticks every 10 ms up to 400 ms: every tag expires.
    let mut now = 0;
    let t0 = Instant::now();
    while now <= 400 {
        sup.tick(ms(now), &mut k, &mut apply, &counters);
        now += 10;
    }
    let expiring = t0.elapsed();
    let expired = counters.freshness_tags_expired.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(expired, u64::from(sources) * u64::from(channels), "all tags stale");
    // Entirely stale: time each tick.
    let mut v = Vec::with_capacity(ticks as usize);
    for _ in 0..ticks {
        let t = Instant::now();
        sup.tick(ms(now), &mut k, &mut apply, &counters);
        v.push(t.elapsed().as_nanos() as u64);
        now += 10;
    }
    v.sort_unstable();
    let at = |q: f64| v[((q * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1];
    println!(
        "kind=stale_tick sources={sources} channels={channels} tags={} expiry_phase_ms={:.1} ticks={ticks} tick_p50_ns={} tick_p99_ns={} tick_max_ns={} ops={}",
        expired,
        expiring.as_secs_f64() * 1e3,
        at(0.5),
        at(0.99),
        v[v.len() - 1],
        k.last_operation_sequence()
    );
}
