//! VS-003 gates (brief §7, ADR 0010): ingress policies `fifo` / `source_capped` /
//! `source_fair` with real kernel and driver processes. The whole VS-001/VS-002 suite also
//! runs under every policy with `SCADA_TEST_INGRESS=source_fair:4` (CI).
//!
//! Wall-clock is used only with a throttled sequencer and wide margins: the gates are
//! invariants (order, barrier, no loss, no permit leak, liveness), not latency targets.

mod support;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use scada_core::Operation;
use scada_protocol::driver_v1::{AckStatus, BatchMode, ErrorCode};
use scada_protocol::introspect_v1::{IntrospectResponse, LinkState};
use scada_sim_driver::{Session, Val, batch};
use support::*;

/// Per source: last accepted sequence and link (state, generation).
type Normalized = BTreeMap<u32, (Option<u64>, (LinkState, u64))>;

const POLICIES: [&str; 4] = ["fifo", "source_capped:2", "source_fair:1", "source_fair:4"];

fn limits(policy: &str) -> Limits {
    Limits {
        max_driver_connections: 40,
        ingress: if policy == "fifo" { String::new() } else { ingress_lines(policy) },
        ..Limits::default()
    }
}

fn start(policy: &str, workers: Option<u32>, more: &[&str]) -> KernelProc {
    let mut args = scheduler_args(workers);
    args.extend(more.iter().map(|s| s.to_string()));
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    KernelProc::start_with(&limits(policy), None, &refs)
}

fn st(a: &scada_protocol::driver_v1::InputAck) -> AckStatus {
    AckStatus::try_from(a.status).unwrap()
}

fn link(r: &IntrospectResponse, id: u32) -> (LinkState, u64) {
    let s = r.sources.iter().find(|s| s.source_id == id).unwrap();
    (LinkState::try_from(s.link_state).unwrap(), s.link_generation)
}

fn last_accepted(r: &IntrospectResponse, id: u32) -> Option<u64> {
    r.sources.iter().find(|s| s.source_id == id).and_then(|s| s.last_accepted_source_sequence)
}

/// Pipelined load client (FullState + `n` Deltas, `pipeline` in flight). Asserts per-source
/// ACK order and increasing operation sequences; returns the operation sequences.
fn load_client(socket: PathBuf, source: u32, epoch: u64, n: u64, pipeline: u64) -> Vec<u64> {
    let mut s = Session::connect(&socket, "load", source, epoch).unwrap();
    let a = s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    let mut ops = vec![a.operation_sequence.unwrap()];
    let (mut sent, mut acked) = (0u64, 0u64);
    while acked < n {
        while sent < n && sent - acked < pipeline {
            sent += 1;
            s.send(&batch(BatchMode::Delta, 1 + sent, &[(1, Val::I(sent as i64))])).unwrap();
        }
        s.flush().unwrap();
        let a = s.recv_ack().unwrap();
        acked += 1;
        assert_eq!(a.source_sequence, 1 + acked, "source {source}: ACK out of order");
        assert_eq!(st(&a), AckStatus::AppliedVolatile, "source {source}: {a:?}");
        let op = a.operation_sequence.unwrap();
        assert!(op > *ops.last().unwrap(), "source {source}: operation sequence not increasing");
        ops.push(op);
    }
    ops
}

/// Load client that never stops until `stop` (always backlogged), single observation.
fn noisy_client(socket: PathBuf, source: u32, pipeline: u64, until: Instant) -> u64 {
    let mut s = Session::connect(&socket, "noisy", source, 11).unwrap();
    let a = s.request(&batch(BatchMode::FullState, 1, &[(1, Val::I(0))])).unwrap();
    assert_eq!(st(&a), AckStatus::AppliedVolatile);
    let (mut sent, mut acked) = (0u64, 0u64);
    loop {
        let done = Instant::now() >= until;
        while !done && sent - acked < pipeline {
            sent += 1;
            s.send(&batch(BatchMode::Delta, 1 + sent, &[(1, Val::I(sent as i64))])).unwrap();
        }
        s.flush().unwrap();
        if acked == sent {
            return acked;
        }
        let a = s.recv_ack().unwrap();
        acked += 1;
        assert_eq!(a.source_sequence, 1 + acked);
        assert_eq!(st(&a), AckStatus::AppliedVolatile);
    }
}

#[test]
fn policy_is_reported_and_check_config_counts_it() {
    for p in POLICIES {
        let k = start(p, None, &[]);
        k.stop();
    }
    // --check-config prints the policy and a larger bound for source_fair.
    let dir = tempfile::tempdir().unwrap();
    let bound = |policy: &str| {
        let cfg = dir.path().join(format!("{}.toml", policy.replace(':', "_")));
        std::fs::write(&cfg, demo_config(dir.path(), &limits(policy), None)).unwrap();
        let out =
            std::process::Command::new(kernel_bin()).arg("--config").arg(&cfg).arg("--check-config").output().unwrap();
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        let want = match policy {
            "fifo" => "ingress=fifo".to_string(),
            p => {
                let (name, l) = p.split_once(':').unwrap();
                format!("ingress={name}({l})")
            }
        };
        assert!(text.contains(&want), "{text}");
        let n: u128 =
            text.split_whitespace().find_map(|w| w.strip_prefix("memory_bound_bytes=")).unwrap().parse().unwrap();
        n
    };
    let (fifo, capped, fair) = (bound("fifo"), bound("source_capped:2"), bound("source_fair:4"));
    assert!(fifo < capped && capped < fair, "{fifo} {capped} {fair}");
}

// §7.2 + §7.3 + §7.1: 16 pipelined sources + one with dedup/gap/rejections, under every
// policy and both schedulers: per-source ACK order, unique operation sequences, the same
// normalized final state, and the captured stream replays to the live digest. Live
// digests of different runs are NOT compared (inter-source order legitimately differs).
#[test]
fn every_policy_keeps_per_source_order_and_replays_to_the_live_digest() {
    const SOURCES: u32 = 16;
    const N: u64 = 300;
    let mut normalized: Vec<(String, Normalized)> = Vec::new();
    for workers in [None, Some(2)] {
        for p in POLICIES {
            let dir = tempfile::Builder::new().prefix("scada-cap3").tempdir().unwrap();
            let cap = dir.path().join("inputs.txt");
            let k = start(p, workers, &["--capture-inputs", &cap.display().to_string()]);
            let hs: Vec<_> = (1..=SOURCES)
                .map(|n| {
                    let sock = k.socket.clone();
                    std::thread::spawn(move || load_client(sock, 3000 + n, 5, N, 16))
                })
                .collect();
            let mut s = Session::connect(&k.socket, "mix", 1001, 3).unwrap();
            let full = |seq| batch(BatchMode::FullState, seq, &[(1, Val::B(true)), (2, Val::B(false)), (3, Val::I(7))]);
            assert_eq!(st(&s.request(&full(1)).unwrap()), AckStatus::AppliedVolatile);
            assert_eq!(st(&s.request(&full(1)).unwrap()), AckStatus::Duplicate);
            let gap = s.request(&batch(BatchMode::Delta, 5, &[(3, Val::I(2))])).unwrap();
            assert_eq!(st(&gap), AckStatus::Rejected);
            assert_eq!(st(&s.request(&full(2)).unwrap()), AckStatus::AppliedVolatile);
            let mut ops: Vec<u64> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
            let total = ops.len();
            ops.sort_unstable();
            ops.dedup();
            assert_eq!(ops.len(), total, "{p} {workers:?}: operation sequence given twice");
            drop(s);
            let live = k.wait_for("all links closed", |r| {
                link(r, 1001).0 == LinkState::Disconnected
                    && (1..=SOURCES).all(|n| link(r, 3000 + n).0 == LinkState::Disconnected)
            });
            let inputs = scada_runtime::capture::read(&cap).unwrap();
            let cfg = scada_runtime::config::load(&k.dir.path().join("kernel.toml")).unwrap();
            let mut kernel = scada_core::Kernel::new(std::sync::Arc::new(cfg.core.clone()));
            for i in inputs {
                let _ = kernel.apply(i);
            }
            assert_eq!(live.state_digest, kernel.state_digest().to_vec(), "{p} {workers:?}: replay != live");
            let norm = (1..=SOURCES).map(|n| (n, (last_accepted(&live, 3000 + n), link(&live, 3000 + n)))).collect();
            normalized.push((format!("{p} {workers:?}"), norm));
            k.stop();
        }
    }
    for (name, n) in &normalized[1..] {
        // Same last accepted sequence for every source (generations are per-run, equal
        // here because every run connects each source exactly once).
        let strip = |m: &Normalized| -> Vec<(u32, Option<u64>, LinkState)> {
            m.iter().map(|(k, (l, (s, _)))| (*k, *l, *s)).collect()
        };
        assert_eq!(strip(n), strip(&normalized[0].1), "normalized state differs: {name} vs {}", normalized[0].0);
    }
}

/// Rare stop-and-wait latencies (ms) under a throttled sequencer with 16 always-backlogged
/// sources (pipeline 32 each, 512 outstanding against a queue of 64).
fn rare_latencies_ms(policy: &str, workers: Option<u32>, samples: usize) -> Vec<u64> {
    let k = start(policy, workers, &["--throttle-sequencer-us", "2000"]);
    let until = Instant::now() + Duration::from_secs(30);
    let noisy: Vec<_> = (1..=16u32)
        .map(|n| {
            let sock = k.socket.clone();
            std::thread::spawn(move || noisy_client(sock, 3000 + n, 32, until))
        })
        .collect();
    std::thread::sleep(Duration::from_millis(500)); // queues saturated
    let mut s = Session::connect(&k.socket, "rare", 1001, 1).unwrap();
    let mut lat = Vec::new();
    for i in 0..samples as u64 {
        let t0 = Instant::now();
        let a = s
            .request(&batch(BatchMode::FullState, i + 1, &[(1, Val::B(true)), (2, Val::B(false)), (3, Val::I(1))]))
            .unwrap();
        assert_eq!(st(&a), AckStatus::AppliedVolatile);
        lat.push(t0.elapsed().as_millis() as u64);
    }
    drop(s);
    // Stop the noisy clients early: kill the kernel (their sessions error out).
    let _ = k.kill();
    for h in noisy {
        let _ = h.join(); // they may panic on the killed socket: that is fine here
    }
    lat.sort_unstable();
    lat
}

// §7.4 fair progress: with every other source always backlogged, a rare request under
// `source_fair` is served within ~(active lanes) turns of the throttled sequencer
// (17 × 2 ms + ACK path), independently of the noisy sources' depth; under `fifo` it waits
// behind the whole queue (≥ 64 × 2 ms). Bounds are in turns; the wall-clock margin is wide.
#[test]
fn fair_policy_bounds_the_rare_source_wait_in_turns() {
    for workers in [None, Some(2)] {
        let fair = rare_latencies_ms("source_fair:2", workers, 15);
        let fifo = rare_latencies_ms("fifo", workers, 15);
        let med = |v: &[u64]| v[v.len() / 2];
        eprintln!("{workers:?}: rare stop-and-wait latency ms, fair {fair:?} fifo {fifo:?}");
        // 16 noisy lanes + the rare + the introspection lane: ≤ 17 other turns of 2 ms.
        assert!(*fair.last().unwrap() < 150, "{workers:?}: fair rare max {fair:?} ms");
        assert!(med(&fifo) >= 120, "{workers:?}: fifo rare median {fifo:?} ms (queue of 64 × 2 ms expected)");
        assert!(med(&fair) * 2 < med(&fifo), "{workers:?}: fair {fair:?} vs fifo {fifo:?}");
    }
}

// §7.5 / §4.5 barrier: under source_fair, links closed abruptly while their batches are
// queued. In the captured authoritative stream, no batch of a generation is ever applied
// after that generation's SourceDisconnected.
#[test]
fn disconnect_is_never_applied_before_the_same_links_batches() {
    for workers in [None, Some(2)] {
        let dir = tempfile::Builder::new().prefix("scada-bar").tempdir().unwrap();
        let cap = dir.path().join("inputs.txt");
        let k = start(
            "source_fair:4",
            workers,
            &["--capture-inputs", &cap.display().to_string(), "--throttle-sequencer-us", "200"],
        );
        let hs: Vec<_> = (1..=8u32)
            .map(|n| {
                let sock = k.socket.clone();
                std::thread::spawn(move || {
                    for round in 0..5u64 {
                        let mut s = Session::connect(&sock, "abrupt", 3000 + n, 100 + round).unwrap();
                        let base = s.welcome.last_accepted_source_sequence.unwrap_or(0);
                        s.send(&batch(BatchMode::FullState, base + 1, &[(1, Val::I(0))])).unwrap();
                        for i in 2..=20u64 {
                            s.send(&batch(BatchMode::Delta, base + i, &[(1, Val::I(i as i64))])).unwrap();
                        }
                        s.flush().unwrap();
                        drop(s); // close without reading the ACKs
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        k.wait_for("all links closed", |r| (1..=8).all(|n| link(r, 3000 + n).0 == LinkState::Disconnected));
        let inputs = scada_runtime::capture::read(&cap).unwrap();
        let mut disconnected = std::collections::HashSet::new();
        let mut batches = 0;
        for i in &inputs {
            match &i.operation {
                Operation::SourceDisconnected { source_id, link_generation } => {
                    disconnected.insert((source_id.0, *link_generation));
                }
                Operation::ObservationBatch(b) => {
                    batches += 1;
                    assert!(
                        !disconnected.contains(&(b.source_id.0, b.link_generation)),
                        "{workers:?}: batch of {:?} gen {} applied after its disconnect",
                        b.source_id,
                        b.link_generation
                    );
                }
                _ => {}
            }
        }
        assert!(batches > 0);
        assert_eq!(disconnected.len(), 40, "{workers:?}: every link gets exactly one disconnect");
        k.stop();
    }
}

// §7.6 cancellation + §7.7 takeover: sessions are destroyed or superseded while waiting
// for a source/global permit under a throttled sequencer. Afterwards no permit has leaked:
// a fresh link of the same sources can keep `max_inflight` batches in flight and every
// one is acknowledged in order, and the stale links never degrade the new ones.
#[test]
fn cancelled_and_superseded_waiters_leak_no_permit() {
    for workers in [None, Some(2)] {
        let k = start("source_fair:2", workers, &["--throttle-sequencer-us", "300"]);
        for round in 0..6u64 {
            let mut olds = Vec::new();
            for n in 1..=8u32 {
                let mut s = Session::connect(&k.socket, "victim", 3000 + n, 10 + round).unwrap();
                let base = s.welcome.last_accepted_source_sequence.unwrap_or(0);
                for i in 1..=32u64 {
                    let mode = if i == 1 { BatchMode::FullState } else { BatchMode::Delta };
                    s.send(&batch(mode, base + i, &[(1, Val::I(i as i64))])).unwrap();
                }
                s.flush().unwrap();
                olds.push(s);
            }
            std::thread::sleep(Duration::from_millis(5));
            if round % 2 == 0 {
                drop(olds); // destroyed while blocked on permits
            } else {
                // Superseded while blocked: the takeover Hello must be welcomed promptly.
                for n in 1..=8u32 {
                    let t0 = Instant::now();
                    let s = Session::connect(&k.socket, "taker", 3000 + n, 50 + round).unwrap();
                    assert!(
                        t0.elapsed() < Duration::from_secs(5),
                        "{workers:?}: takeover Welcome took {:?}",
                        t0.elapsed()
                    );
                    drop(s);
                }
                drop(olds);
            }
        }
        k.wait_for("all closed", |r| (1..=8).all(|n| link(r, 3000 + n).0 == LinkState::Disconnected));
        // No leak: each source can again have its whole window acknowledged.
        let hs: Vec<_> = (1..=8u32)
            .map(|n| {
                let sock = k.socket.clone();
                std::thread::spawn(move || {
                    let mut s = Session::connect(&sock, "after", 3000 + n, 999).unwrap();
                    assert!(s.welcome.full_state_required || s.welcome.last_accepted_source_sequence.is_some());
                    let base = s.welcome.last_accepted_source_sequence.unwrap_or(0);
                    s.send(&batch(BatchMode::FullState, base + 1, &[(1, Val::I(0))])).unwrap();
                    for i in 2..=64u64 {
                        s.send(&batch(BatchMode::Delta, base + i, &[(1, Val::I(i as i64))])).unwrap();
                    }
                    s.flush().unwrap();
                    for i in 1..=64u64 {
                        let a = s.recv_ack().unwrap();
                        assert_eq!(a.source_sequence, base + i);
                        assert_eq!(st(&a), AckStatus::AppliedVolatile, "{a:?}");
                    }
                })
            })
            .collect();
        let t0 = Instant::now();
        for h in hs {
            h.join().unwrap();
        }
        assert!(t0.elapsed() < Duration::from_secs(20), "{workers:?}: permits leaked?");
        let r = k.introspect();
        assert_eq!(r.counters.unwrap().active_connections, 0);
        k.stop();
    }
}

// §7.7: a superseded link whose lane is full and whose peer never reads is closed; the
// new link is Connected, its quality is not degraded by the old link's late close, and the
// old link's late batches are refused with a typed outcome or never applied.
#[test]
fn takeover_with_a_full_lane_under_fair_policy() {
    for workers in [None, Some(2)] {
        let k = start("source_fair:2", workers, &["--throttle-sequencer-us", "500"]);
        let mut old = Session::connect(&k.socket, "old", 1001, 1).unwrap();
        old.send(&batch(BatchMode::FullState, 1, &[(1, Val::B(true)), (2, Val::B(true)), (3, Val::I(1))])).unwrap();
        for i in 2..=40u64 {
            old.send(&batch(BatchMode::Delta, i, &[(3, Val::I(i as i64))])).unwrap();
        }
        old.flush().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let t0 = Instant::now();
        let mut new = Session::connect(&k.socket, "new", 1001, 2).unwrap();
        assert!(t0.elapsed() < Duration::from_secs(2), "{workers:?}: Welcome after {:?}", t0.elapsed());
        let gen_new = new.welcome.link_generation;
        let a = new
            .request(&batch(BatchMode::FullState, 1, &[(1, Val::B(false)), (2, Val::B(false)), (3, Val::I(-1))]))
            .unwrap();
        assert_eq!(st(&a), AckStatus::AppliedVolatile, "{a:?}");
        drop(old);
        std::thread::sleep(Duration::from_millis(200));
        let r = k.introspect();
        assert_eq!(link(&r, 1001), (LinkState::Connected, gen_new), "{workers:?}");
        let t = tag(&r, 103);
        assert_eq!(quality(&t), scada_protocol::introspect_v1::Quality::Good, "{workers:?}");
        assert!(r.counters.unwrap().links_superseded >= 1);
        let _ = ErrorCode::LinkSuperseded;
        k.stop();
    }
}

// §7.10: SIGTERM while sessions are blocked on permits of a saturated, throttled
// sequencer: clean exit, no panic.
#[test]
fn shutdown_with_sessions_waiting_for_permits() {
    for p in ["source_fair:1", "source_capped:1"] {
        let k = start(p, Some(2), &["--throttle-sequencer-us", "5000"]);
        let mut ss = Vec::new();
        for n in 1..=8u32 {
            let mut s = Session::connect(&k.socket, "w", 3000 + n, 1).unwrap();
            for i in 1..=16u64 {
                let mode = if i == 1 { BatchMode::FullState } else { BatchMode::Delta };
                s.send(&batch(mode, i, &[(1, Val::I(0))])).unwrap();
            }
            s.flush().unwrap();
            ss.push(s);
        }
        std::thread::sleep(Duration::from_millis(50));
        let t0 = Instant::now();
        k.stop();
        assert!(t0.elapsed() < Duration::from_secs(5), "{p}: shutdown took {:?}", t0.elapsed());
    }
}
