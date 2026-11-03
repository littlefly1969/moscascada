//! VS-005 alarm benchmarks (brief §11, ADR 0012).
//!
//! * `alarms-core`: in-process `Kernel::apply` with alarm rules: normal operation without
//!   trips (0 / 100 / 1 000 / 100 000 rules), trip/clear/retrigger storms (1 / 10 / 100 /
//!   1 000 events per operation), bad-quality fan-out on disconnection, refusal on a full
//!   outbox (capacity C and C+1), ACK cost, cold start (validation, table build, kernel
//!   allocation) and allocations inside `apply` (counting allocator).
//! * `alarm-storm`: real kernel process with `[alarms]` (rules with trip 1 / reset 0 on
//!   every tag of the driven sources): N stop-and-wait drivers alternate 0/1 on every
//!   channel for S seconds; a sink either drains the outbox through the control socket
//!   (read pages + TEST-ONLY simulated durable commit) or is stopped (controlled
//!   saturation). One machine-readable line with PASS/FAIL.
//!
//! Every number is what this machine measures. Nothing here is durable: the simulated
//! sink exists only with `--test-simulated-durable-sink` and is NOT a journal.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::{Duration, Instant};

use scada_core::{
    AlarmCondition, AlarmId, AlarmRuleSpec, AlarmSpec, BatchMode, ChannelId, ChannelSpec, ConfigSpec, CoreLimits,
    Kernel, Observation, ObservationBatch, Operation, SequencedInput, Severity, SourceId, SourceSpec, Supervision,
    TagId, TagSpec, TransitionOutcome, ValidatedConfig, Value, ValueType,
};
use scada_protocol::alarm_v1 as apb;
use scada_protocol::driver_v1::{BatchMode as WireMode, ErrorCode};
use scada_sim_driver::{AlarmClient, Session, Val, batch};

use crate::{CHANNELS, KernelProc, KernelSpec, alloc, arg, arg_str, measure_window, proc_status, stats, us};

// ---------------------------------------------------------------- in-process

fn tags_and_sources(sources: u32, supervision: Supervision) -> (Vec<TagSpec>, Vec<SourceSpec>) {
    let mut tags = Vec::new();
    let mut srcs = Vec::new();
    for s in 0..sources {
        let mut channels = Vec::new();
        for c in 0..CHANNELS {
            let tag_id = TagId(s * 1000 + c + 1);
            tags.push(TagSpec { tag_id, name: format!("s{s}_c{c}"), value_type: ValueType::I64 });
            channels.push(ChannelSpec { channel_id: ChannelId(c + 1), tag_id, value_type: ValueType::I64 });
        }
        srcs.push(SourceSpec { source_id: SourceId(s + 1), name: format!("load-{s}"), channels, supervision });
    }
    (tags, srcs)
}

/// `rules` rules assigned round-robin over the tags in id order (rule i on tag i mod n).
fn config(sources: u32, rules: u32, cond: AlarmCondition, capacity: u64) -> (Arc<ValidatedConfig>, Duration) {
    config_sup(sources, rules, cond, capacity, Supervision::NONE)
}

fn config_sup(
    sources: u32,
    rules: u32,
    cond: AlarmCondition,
    capacity: u64,
    supervision: Supervision,
) -> (Arc<ValidatedConfig>, Duration) {
    let (tags, srcs) = tags_and_sources(sources, supervision);
    let ids: Vec<TagId> = tags.iter().map(|t| t.tag_id).collect();
    let spec = ConfigSpec { limits: CoreLimits { max_observations_per_batch: CHANNELS }, tags, sources: srcs };
    let alarms = (rules > 0).then(|| AlarmSpec {
        max_rules: rules,
        outbox_capacity_events: capacity,
        rules: (0..rules)
            .map(|i| AlarmRuleSpec {
                alarm_id: AlarmId(i + 1),
                name: format!("a{}", i + 1),
                tag_id: ids[i as usize % ids.len()],
                condition: cond,
                severity: Severity::Warning,
            })
            .collect(),
    });
    let t0 = Instant::now();
    let c = ValidatedConfig::validate_with_alarms(spec, alarms).expect("config");
    (Arc::new(c), t0.elapsed())
}

struct Run {
    k: Kernel,
    gens: Vec<u64>,
    seqs: Vec<u64>,
}

impl Run {
    fn new(cfg: Arc<ValidatedConfig>) -> Run {
        let n = cfg.sources().len();
        let mut r = Run { k: Kernel::new(cfg), gens: vec![0; n], seqs: vec![0; n] };
        for s in 1..=n as u32 {
            r.connect(s);
            r.full(s, 0);
        }
        r
    }
    fn op(&mut self, operation: Operation) -> Result<TransitionOutcome, scada_core::ApplyError> {
        let operation_sequence = self.k.last_operation_sequence() + 1;
        self.k.apply(SequencedInput { operation_sequence, operation })
    }
    fn connect(&mut self, s: u32) {
        let i = (s - 1) as usize;
        self.gens[i] += 1;
        let r = self.op(Operation::SourceConnected {
            source_id: SourceId(s),
            link_generation: self.gens[i],
            source_epoch: 1,
        });
        assert!(r.is_ok(), "{r:?}");
    }
    fn batch_op(&mut self, s: u32, mode: BatchMode, obs: Vec<Observation>) -> Operation {
        let i = (s - 1) as usize;
        Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(s),
            link_generation: self.gens[i],
            mode,
            source_sequence: self.seqs[i] + 1,
            observations: obs,
        })
    }
    fn full(&mut self, s: u32, v: i64) {
        let op = self.batch_op(s, BatchMode::FullState, all(v));
        let r = self.op(op);
        assert!(r.is_ok(), "{r:?}");
        self.seqs[(s - 1) as usize] += 1;
    }
    fn release_all(&mut self) {
        if let Some(st) = self.k.outbox_status() {
            let up = st.next_event_seq - 1;
            if up > st.durable_watermark {
                let r = self.op(Operation::AdvanceDurableWatermark { up_to_event_seq: up });
                assert!(r.is_ok(), "{r:?}");
            }
        }
    }
}

fn all(v: i64) -> Vec<Observation> {
    (1..=CHANNELS).map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(v) }).collect()
}

struct Timed {
    samples: Vec<u64>,
    allocs: u64,
    busy: Duration,
    events: u64,
}

impl Timed {
    fn new(n: usize) -> Timed {
        Timed { samples: Vec::with_capacity(n), allocs: 0, busy: Duration::ZERO, events: 0 }
    }
    fn time(&mut self, k: &mut Kernel, input: SequencedInput) -> Result<TransitionOutcome, scada_core::ApplyError> {
        let e0 = k.outbox_status().map_or(0, |s| s.next_event_seq);
        let a0 = alloc::ALLOCS.load(Relaxed);
        let t0 = Instant::now();
        let r = k.apply(input);
        let dt = t0.elapsed();
        self.allocs += alloc::ALLOCS.load(Relaxed) - a0;
        self.busy += dt;
        self.samples.push(dt.as_nanos() as u64);
        self.events += k.outbox_status().map_or(0, |s| s.next_event_seq) - e0;
        r
    }
    /// Markdown row + raw line.
    fn report(self, label: &str, extra: &str) {
        let n = self.samples.len();
        let ops_s = n as f64 / self.busy.as_secs_f64();
        let ev_s = self.events as f64 / self.busy.as_secs_f64();
        let allocs = self.allocs as f64 / n as f64;
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let p95 = sorted[(n * 95 / 100).min(n - 1)];
        let s = stats(self.samples);
        println!(
            "| {label} | {n} | {ops_s:.0} | {ev_s:.0} | {} | {} | {} | {} | {} | {allocs:.3} | {extra} |",
            us(s.p50),
            us(p95),
            us(s.p99),
            us(s.p999),
            us(s.max)
        );
        eprintln!(
            "kind=alarms-core case={label} n={n} ops_s={ops_s:.0} events_s={ev_s:.0} p50_ns={} p95_ns={p95} p99_ns={} p999_ns={} max_ns={} allocs_per_op={allocs:.3} {}",
            s.p50,
            s.p99,
            s.p999,
            s.max,
            extra.replace(' ', "_")
        );
    }
}

const HEADER: &str = "| case | ops | ops/s | events/s | p50 µs | p95 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | notes |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|";

/// Sequencer hot path of the process benchmark, in-process: `sources` x 128 tags, every
/// tag carries `rules_per_tag` quiet `high` rules (0 = alarms disabled), Delta x 128 with
/// every value changing, rotating over the sources. No per-op timer: wall time of chunks of
/// 10 000 applies, `reps` repetitions; prints the median and min ns/op (A/B of variants).
pub fn alarms_hot(sources: u32, rules_per_tag: u32, ops: u64, reps: u32) {
    let quiet = AlarmCondition::High { trip: 1 << 60, reset: (1 << 60) - 1 };
    let rules = sources * CHANNELS * rules_per_tag;
    let (cfg, _) = config(sources, rules, quiet, 1 << 16);
    let mut r = Run::new(cfg);
    let mut inputs: Vec<Operation> = Vec::with_capacity(10_000);
    let mut per_rep = Vec::new();
    let mut v: i64 = 1;
    for rep in 0..=reps {
        let mut spent = Duration::ZERO;
        let mut done = 0u64;
        while done < ops {
            inputs.clear();
            let n = 10_000.min(ops - done);
            for j in 0..n {
                let s = ((done + j) % u64::from(sources)) as u32 + 1;
                let idx = (s - 1) as usize;
                r.seqs[idx] += 1;
                v += 1;
                inputs.push(Operation::ObservationBatch(ObservationBatch {
                    source_id: SourceId(s),
                    link_generation: r.gens[idx],
                    mode: BatchMode::Delta,
                    source_sequence: r.seqs[idx],
                    observations: all(v),
                }));
            }
            let t0 = Instant::now();
            for op in inputs.drain(..) {
                let seq = r.k.last_operation_sequence() + 1;
                let out = r.k.apply(SequencedInput { operation_sequence: seq, operation: op });
                debug_assert!(out.is_ok());
                std::hint::black_box(&out);
            }
            spent += t0.elapsed();
            done += n;
        }
        if rep > 0 {
            per_rep.push(spent.as_nanos() as f64 / ops as f64); // rep 0 = warm-up
        }
    }
    per_rep.sort_by(f64::total_cmp);
    let med = per_rep[per_rep.len() / 2];
    println!(
        "kind=alarms-hot sources={sources} rules_per_tag={rules_per_tag} rules={rules} ops={ops} reps={reps} median_ns_per_op={med:.1} min_ns_per_op={:.1} all={:?}",
        per_rep[0],
        per_rep.iter().map(|x| (x * 10.0).round() / 10.0).collect::<Vec<_>>()
    );
}

pub fn alarms_core(ops: u64) {
    println!("## VS-005 alarm engine in-memory (`scada-bench alarms-core --ops {ops}`)\n");
    println!("Timing: `Instant::now()` around each single `apply` (inputs built outside the timed region).");
    println!(
        "ops/s and events/s = count / sum of measured `apply` durations. allocs/op = counting allocator inside `apply`.\n"
    );
    let quiet = AlarmCondition::High { trip: 1 << 60, reset: (1 << 60) - 1 };

    // A. Normal operation, no trip: 800 sources x 128 = 102 400 tags; batches rotate
    // over every source (Delta, 128 observations, every value changes).
    println!("### A. Normal operation without trips (102 400 tags, Delta x 128 rotating over 800 sources)\n");
    println!("{HEADER}");
    for rules in [0u32, 100, 1_000, 100_000] {
        let (cfg, validate) = config(800, rules, quiet, 1 << 16);
        let mut r = Run::new(cfg.clone());
        let mut t = Timed::new(ops as usize);
        let mut inputs = Vec::with_capacity(10_000);
        let mut i = 0u64;
        let warm = ops / 10;
        while i < warm + ops {
            inputs.clear();
            for j in 0..10_000.min(warm + ops - i) {
                let s = ((i + j) % 800) as u32 + 1;
                let v = (i + j) as i64 + 1;
                let idx = (s - 1) as usize;
                r.seqs[idx] += 1;
                let op = Operation::ObservationBatch(ObservationBatch {
                    source_id: SourceId(s),
                    link_generation: r.gens[idx],
                    mode: BatchMode::Delta,
                    source_sequence: r.seqs[idx],
                    observations: all(v),
                });
                inputs.push(op);
            }
            for op in inputs.drain(..) {
                let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
                if i < warm {
                    r.k.apply(input).expect("warm");
                } else {
                    assert!(t.time(&mut r.k, input).is_ok());
                }
                i += 1;
            }
        }
        assert_eq!(t.events, 0);
        let mem = Kernel::alarm_storage_bytes(&cfg);
        t.report(
            &format!("quiet rules={rules}"),
            &format!(
                "alarm storage {mem} B, table {} B, validate {:.1} ms",
                cfg.alarm_table().map_or(0, |a| a.heap_bytes()),
                validate.as_secs_f64() * 1e3
            ),
        );
    }

    // B. Storms: 1 source x 128 tags; K rules (trip 1 / reset 0) round-robin over the
    // tags; FullState alternating 1/0 -> K events per operation (Raised, Returned,
    // Retriggered, ...). The outbox is released after every op (untimed) = ideal sink.
    println!("\n### B. Trip/clear/retrigger storms (FullState x 128 alternating 1/0, ideal sink)\n");
    println!("{HEADER}");
    for k_rules in [1u32, 10, 100, 1_000] {
        let (cfg, _) = config(1, k_rules, AlarmCondition::High { trip: 1, reset: 0 }, u64::from(k_rules) * 4);
        let mut r = Run::new(cfg);
        let n = (ops / u64::from(k_rules).max(1)).clamp(2_000, 200_000);
        let mut t = Timed::new(n as usize);
        for i in 0..(n + n / 10) {
            let op = r.batch_op(1, BatchMode::FullState, all(((i + 1) % 2) as i64));
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            let res = if i < n / 10 { r.k.apply(input) } else { t.time(&mut r.k, input) };
            assert!(res.is_ok(), "{res:?}");
            r.seqs[0] += 1;
            r.release_all();
        }
        assert_eq!(t.events, n * u64::from(k_rules));
        t.report(&format!("storm events/op={k_rules}"), "trip+clear alternate");
    }

    // C. Bad-quality fan-out: K bad_quality rules on 128 tags; disconnection raises K,
    // the reconnect FullState returns K. Both operations timed.
    println!("\n### C. Bad-quality fan-out on disconnection / reconnection\n");
    println!("{HEADER}");
    for k_rules in [128u32, 1_024, 8_192] {
        let (cfg, _) = config(1, k_rules, AlarmCondition::BadQuality, u64::from(k_rules) * 4);
        let mut r = Run::new(cfg);
        let n = (ops / u64::from(k_rules)).clamp(500, 20_000);
        let (mut d, mut f) = (Timed::new(n as usize), Timed::new(n as usize));
        for _ in 0..n {
            let op = Operation::SourceDisconnected { source_id: SourceId(1), link_generation: r.gens[0] };
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            assert!(d.time(&mut r.k, input).is_ok());
            r.release_all();
            r.connect(1);
            let op = r.batch_op(1, BatchMode::FullState, all(5));
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            assert!(f.time(&mut r.k, input).is_ok());
            r.seqs[0] += 1;
            r.release_all();
        }
        d.report(&format!("disconnect fanout={k_rules}"), "raises fanout events");
        f.report(&format!("reconnect FullState fanout={k_rules}"), "returns fanout events");
    }

    // D. Full outbox: capacity C = 2 x fan-out (128). Fill with a sink stopped; time the
    // refused operations (C+1) and the accepted one that fits exactly (free == required).
    println!("\n### D. Outbox capacity C / C+1 (128 rules, C = 256, stopped sink)\n");
    println!("{HEADER}");
    {
        let (cfg, _) = config(1, 128, AlarmCondition::High { trip: 1, reset: 0 }, 256);
        let mut r = Run::new(cfg);
        // Two storms fill it exactly (256 = C).
        r.full(1, 1);
        let mut exact = Timed::new(1);
        let op = r.batch_op(1, BatchMode::FullState, all(0));
        let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
        assert!(exact.time(&mut r.k, input).is_ok(), "free == required must be accepted");
        r.seqs[0] += 1;
        assert_eq!(r.k.outbox_status().unwrap().free, 0);
        let digest = r.k.state_digest();
        let mut refused = Timed::new(ops as usize / 10);
        for _ in 0..(ops / 10).max(1_000) {
            let op = r.batch_op(1, BatchMode::FullState, all(1));
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            assert!(matches!(
                refused.time(&mut r.k, input),
                Err(scada_core::ApplyError::OutboxCapacityExceeded { required: 128, free: 0 })
            ));
        }
        assert_eq!(digest, r.k.state_digest(), "a refused operation changed the state");
        exact.report("accept at free == required (C)", "128 events into the last 128 slots");
        refused.report("refuse at C+1 (outbox full)", "OutboxCapacityExceeded, state unchanged");
        // Cost of a no-transition batch while full (values inside the hysteresis band).
    }

    // E. ACK cost: 1 000 active alarms, ACK each (1 event per op).
    println!("\n### E. Acknowledgement (1 event per operation)\n");
    println!("{HEADER}");
    {
        let (cfg, _) = config(8, 1_000, AlarmCondition::High { trip: 1, reset: 0 }, 1 << 20);
        let mut r = Run::new(cfg);
        let mut t = Timed::new(10_000);
        for round in 0..10 {
            for s in 1..=8 {
                r.full(s, 1);
            }
            for id in 1..=1_000u32 {
                let token = r.k.alarm(AlarmId(id)).unwrap().activation_token;
                let op = Operation::AlarmAcknowledge {
                    alarm_id: AlarmId(id),
                    expected_activation_token: token,
                    actor_uid: 1000,
                };
                let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
                assert!(t.time(&mut r.k, input).is_ok());
            }
            for s in 1..=8 {
                r.full(s, 0);
            }
            r.release_all();
            let _ = round;
        }
        t.report("ack", "1 000 alarms x 10 rounds");
    }

    // F. Cold start and memory per rule count.
    println!("\n### F. Cold start and alarm memory\n");
    println!(
        "| rules | outbox events | validate+build ms | Kernel::new ms | alarm storage bytes | table heap bytes | bytes/rule (storage+table, outbox excluded) |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for (rules, cap) in [(100u32, 1u64 << 16), (1_000, 1 << 16), (100_000, 1 << 16), (1_000_000, 1 << 20)] {
        let sources = rules.div_ceil(CHANNELS).clamp(1, 4_096);
        let (cfg, validate) = config(sources, rules, quiet, cap);
        let t0 = Instant::now();
        let k = Kernel::new(cfg.clone());
        let new = t0.elapsed();
        let storage = Kernel::alarm_storage_bytes(&cfg);
        let table = cfg.alarm_table().map_or(0, |a| a.heap_bytes());
        let outbox = u128::from(cap) * std::mem::size_of::<scada_core::AlarmEvent>() as u128;
        let per_rule = (storage + table - outbox) as f64 / f64::from(rules);
        println!(
            "| {rules} | {cap} | {:.1} | {:.2} | {storage} | {table} | {per_rule:.1} |",
            validate.as_secs_f64() * 1e3,
            new.as_secs_f64() * 1e3
        );
        eprintln!(
            "kind=alarms-cold rules={rules} cap={cap} validate_ms={:.2} new_ms={:.3} storage_b={storage} table_b={table} per_rule_b={per_rule:.1}",
            validate.as_secs_f64() * 1e3,
            new.as_secs_f64() * 1e3
        );
        drop(k);
    }

    // G. VS-004 coupling: silence and freshness expiry raise K bad-quality alarms in one
    // operation; then an ENTIRELY stale plant: repeated expiries find no candidate and must
    // cost the same whatever the number of rules (no O(all rules) scan per tick).
    println!("\n### G. Supervision coupling: silence / freshness fan-out, entirely stale plant\n");
    println!("{HEADER}");
    for k_rules in [128u32, 8_192] {
        let sup = Supervision { liveness: true, freshness: true };
        let (cfg, _) = config_sup(1, k_rules, AlarmCondition::BadQuality, u64::from(k_rules) * 4, sup);
        let mut r = Run::new(cfg);
        let n = (ops / u64::from(k_rules)).clamp(200, 5_000);
        let (mut silence, mut fresh, mut idle) =
            (Timed::new(n as usize), Timed::new(n as usize), Timed::new(n as usize));
        for _ in 0..n {
            let op = Operation::SourceSilenceExpired { source_id: SourceId(1), link_generation: r.gens[0] };
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            assert!(silence.time(&mut r.k, input).is_ok());
            r.release_all();
            r.full(1, 7); // returns them all, refreshes every tag
            r.release_all();
            let up_to = r.k.last_operation_sequence();
            let op = Operation::FreshnessExpired {
                source_id: SourceId(1),
                link_generation: r.gens[0],
                observed_up_to: up_to,
            };
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            assert!(fresh.time(&mut r.k, input).is_ok());
            r.release_all();
            // Entirely stale: nothing left to expire (Ignored, no candidate).
            let op = Operation::FreshnessExpired {
                source_id: SourceId(1),
                link_generation: r.gens[0],
                observed_up_to: up_to,
            };
            let input = SequencedInput { operation_sequence: r.k.last_operation_sequence() + 1, operation: op };
            assert!(matches!(idle.time(&mut r.k, input), Ok(TransitionOutcome::Ignored(_))));
            r.full(1, 7);
            r.release_all();
        }
        silence.report(&format!("SourceSilenceExpired fanout={k_rules}"), "raises fanout bad-quality events");
        fresh.report(&format!("FreshnessExpired fanout={k_rules}"), "raises fanout events");
        idle.report(&format!("FreshnessExpired stale plant rules={k_rules}"), "no candidate: Ignored, O(1)");
    }

    println!("\nLayouts: {:?}", scada_core::layout_sizes());
}

// ---------------------------------------------------------------- process storm

static STOP: AtomicBool = AtomicBool::new(false);

struct DriverOut {
    lat: Vec<u64>,
    applied: u64,
    refused: u64,
    first_refusal_ns: Option<u64>,
}

fn storm_driver(
    socket: PathBuf,
    source: u32,
    size: u32,
    start: Instant,
    warmup: Duration,
    interval: Option<Duration>,
) -> DriverOut {
    let mut s = if crate::DURABLE_RECEIPTS.load(Relaxed) {
        Session::connect_with(&socket, &scada_sim_driver::hello_durable("storm", source, 1)).expect("connect")
    } else {
        Session::connect(&socket, "storm", source, 1).expect("connect")
    };
    let obs0: Vec<(u32, Val)> = (1..=CHANNELS).map(|c| (c, Val::I(0))).collect();
    let mut seq = s.welcome.last_accepted_source_sequence.map_or(1, |l| l + 1);
    let a = s.request(&batch(WireMode::FullState, seq, &obs0)).expect("full");
    assert!(crate::applied_ok(a.status), "{a:?}");
    seq += 1;
    let mut out = DriverOut { lat: Vec::with_capacity(1 << 20), applied: 0, refused: 0, first_refusal_ns: None };
    let mut v = 1i64;
    let msgs: [Vec<(u32, Val)>; 2] =
        [(1..=size).map(|c| (c, Val::I(0))).collect(), (1..=size).map(|c| (c, Val::I(1))).collect()];
    while !STOP.load(Relaxed) {
        let msg = batch(WireMode::Delta, seq, &msgs[v as usize]);
        let t0 = Instant::now();
        let a = s.request(&msg).expect("request");
        let dt = t0.elapsed().as_nanos() as u64;
        let measuring = start.elapsed() >= warmup;
        if crate::applied_ok(a.status) {
            seq += 1;
            v ^= 1;
            if measuring {
                out.lat.push(dt);
                out.applied += 1;
            }
            if let Some(i) = interval {
                std::thread::sleep(i);
            }
        } else {
            // VS-005 outbox full, or VS-006 journal backpressure (queue / disk): typed
            // refusals before apply, retried with the same sequence.
            assert!(
                matches!(
                    a.error_code(),
                    ErrorCode::OutboxCapacityExceeded | ErrorCode::JournalQueueFull | ErrorCode::DiskFull
                ),
                "{a:?}"
            );
            if measuring {
                out.refused += 1;
                out.first_refusal_ns.get_or_insert(start.elapsed().as_nanos() as u64);
            }
            // Retry the same sequence later (nothing was applied).
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    out
}

/// Drain: read pages after the cursor, then release them (TEST-ONLY watermark).
fn sink(path: PathBuf, page: u32, released: Arc<AtomicU64>) {
    let mut c = AlarmClient::connect(&path).expect("alarm socket");
    let mut cursor = 0u64;
    while !STOP.load(Relaxed) {
        let r = c.events(cursor, page).expect("events");
        let p = match r.body {
            Some(apb::alarm_response::Body::Events(p)) => p,
            other => panic!("{other:?}"),
        };
        assert!(!p.gap_detected, "sink lost events: cursor {cursor} oldest {}", p.oldest_visible);
        if p.events.is_empty() {
            std::thread::sleep(Duration::from_micros(200));
            continue;
        }
        cursor = p.next_cursor;
        match c.simulated_commit(cursor).expect("commit").body {
            Some(apb::alarm_response::Body::Watermark(w)) => {
                released.fetch_add(w.released, Relaxed);
            }
            other => panic!("{other:?}"),
        }
    }
}

pub fn alarm_storm(spec: &KernelSpec) {
    let clients = arg("--clients", 4) as u32;
    let size = arg("--size", u64::from(CHANNELS)) as u32;
    let secs = arg("--secs", 60);
    let warmup = Duration::from_secs(arg("--warmup-secs", 2));
    let page = arg("--sink-page", 1024) as u32;
    // Throttled storm (plant-like event rate): each driver sleeps this long after each
    // applied batch; 0 = as fast as the kernel answers.
    let interval_us = arg("--interval-us", 0);
    let interval = (interval_us > 0).then(|| Duration::from_micros(interval_us));
    let sink_mode = arg_str("--sink").unwrap_or_else(|| "drain".into());
    let label = arg_str("--label").unwrap_or_default();
    let a = spec.alarms.expect("--alarms R:CAP:storm required");
    assert!(a.storm, "--alarms R:CAP:storm");
    // VS-006: with --persistence the journal is the durable consumer (sink=journal); the
    // TEST-ONLY simulated sink is refused by the kernel in that case.
    let journal = spec.persistence.is_some();
    let stall_file = std::env::temp_dir().join(format!("scada-bench-stall-{}", std::process::id()));
    let _ = std::fs::remove_file(&stall_file);
    let stall_s = stall_file.display().to_string();
    let stall_at = arg("--stall-at", 0);
    let stall_for = arg("--stall-for", 0);
    let extra: Vec<&str> = if !journal {
        vec!["--test-simulated-durable-sink"]
    } else if stall_for > 0 {
        vec!["--test-journal-stall-file", &stall_s]
    } else {
        vec![]
    };
    let sink_mode = if journal { "journal".to_owned() } else { sink_mode };
    let k = KernelProc::start(spec, "storm", &extra);
    let alarm_sock = k.dir.join("alarm.sock");
    std::thread::sleep(Duration::from_millis(200));
    let rss_idle = proc_status(k.pid(), "VmRSS:");
    STOP.store(false, Relaxed);
    let start = Instant::now();
    let released = Arc::new(AtomicU64::new(0));
    let sink_h = (sink_mode == "drain" && !journal).then(|| {
        let (p, r) = (alarm_sock.clone(), released.clone());
        std::thread::spawn(move || sink(p, page, r))
    });
    let hs: Vec<_> = (1..=clients)
        .map(|c| {
            let sock = k.socket();
            std::thread::spawn(move || storm_driver(sock, c, size, start, warmup, interval))
        })
        .collect();
    std::thread::sleep(warmup);
    let mut probe = AlarmClient::connect(&alarm_sock).expect("probe");
    let c0 = probe.capacity().expect("capacity");
    let released0 = released.load(Relaxed);
    let t0 = Instant::now();
    // Sample the capacity status every second (also a responsiveness probe of the control plane).
    let mut max_used = 0;
    let mut probe_lat = Vec::new();
    let mut time_to_full: Option<f64> = None;
    let mut hist =
        journal.then(|| scada_sim_driver::HistoryClient::connect(&k.dir.join("history.sock")).expect("history"));
    let mut max_backlog = 0u64;
    let mut drained_at: Option<f64> = None;
    let mut stall_events: Vec<String> = Vec::new();
    let usage = {
        let pid = k.pid();
        let h = std::thread::spawn(move || measure_window(pid, Duration::from_secs(secs), rss_idle));
        let mut stalled = false;
        while t0.elapsed() < Duration::from_secs(secs) {
            std::thread::sleep(Duration::from_millis(250));
            // VS-006 stalled-disk scenario: the writer stops between stall_at and
            // stall_at + stall_for (seconds into the window), then catches up.
            let el = t0.elapsed().as_secs();
            if journal && stall_for > 0 {
                if !stalled && el >= stall_at && el < stall_at + stall_for {
                    let _ = std::fs::write(&stall_file, b"");
                    stalled = true;
                    stall_events.push(format!("stall_on@{:.2}", t0.elapsed().as_secs_f64()));
                } else if stalled && el >= stall_at + stall_for {
                    let _ = std::fs::remove_file(&stall_file);
                    stalled = false;
                    stall_events.push(format!("stall_off@{:.2}", t0.elapsed().as_secs_f64()));
                }
            }
            if journal && let Ok(j) = hist.as_mut().expect("history").journal() {
                max_backlog = max_backlog.max(j.backlog_operations);
                if !stalled && stall_events.len() == 2 && j.backlog_operations == 0 && drained_at.is_none() {
                    drained_at = Some(t0.elapsed().as_secs_f64());
                }
            }
            let q0 = Instant::now();
            let c = probe.capacity().expect("capacity");
            probe_lat.push(q0.elapsed().as_nanos() as u64);
            max_used = max_used.max(c.used);
            if c.free == 0 && time_to_full.is_none() {
                time_to_full = Some(t0.elapsed().as_secs_f64());
            }
        }
        h.join().expect("usage")
    };
    let c1 = probe.capacity().expect("capacity");
    let _ = std::fs::remove_file(&stall_file);
    STOP.store(true, Relaxed);
    let outs: Vec<DriverOut> = hs.into_iter().map(|h| h.join().expect("driver")).collect();
    if let Some(h) = sink_h {
        h.join().expect("sink");
    }
    let wall = t0.elapsed().as_secs_f64();
    let applied: u64 = outs.iter().map(|o| o.applied).sum();
    let refused: u64 = outs.iter().map(|o| o.refused).sum();
    let events = c1.next_event_seq - c0.next_event_seq;
    let mut lat: Vec<u64> = outs.into_iter().flat_map(|o| o.lat).collect();
    lat.sort_unstable();
    let p95 = lat.get(lat.len() * 95 / 100).copied().unwrap_or(0);
    let s = (!lat.is_empty()).then(|| stats(lat));
    let probe_max = probe_lat.iter().copied().max().unwrap_or(0);
    let (p50, p99, p999) = s.as_ref().map_or((0, 0, 0), |s| (s.p50, s.p99, s.p999));
    // storm: the outbox absorbed the whole measured window with zero refusals.
    let storm = if refused == 0 { "PASS" } else { "FAIL" };
    // safety: never more than C in use, accounting exact, and (sink stopped) the very first
    // event still readable at the end: nothing committed was overwritten.
    let first = match probe.events(0, 1).expect("events").body {
        Some(apb::alarm_response::Body::Events(p)) => p,
        other => panic!("{other:?}"),
    };
    let no_overwrite = c1.durable_watermark > 0 || (first.oldest_visible <= 1 && !first.gap_detected);
    // VS-006: with the journal as sink a refusal may be journal backpressure
    // (JOURNAL_QUEUE_FULL / DISK_FULL, counted by the journal) instead of a full outbox.
    let journal_refused = if journal {
        hist.as_mut()
            .and_then(|h| h.journal().ok())
            .map_or(0, |j| j.refused_queue_full + j.refused_disk_full + j.refused_fault)
    } else {
        0
    };
    let safety = if max_used <= a.capacity
        && c1.used == c1.next_event_seq - 1 - c1.durable_watermark
        && no_overwrite
        && (refused == 0
            || c1.overload() == apb::OverloadState::Critical
            || c1.rejected_batches > 0
            || journal_refused > 0)
    {
        "PASS"
    } else {
        "FAIL"
    };
    let verdict = format!("storm60={storm} safety={safety}");
    // VS-006: released = durable watermark advance (journal commits), not a simulated sink.
    let released_total =
        if journal { c1.durable_watermark - c0.durable_watermark } else { released.load(Relaxed) - released0 };
    let jkv = if journal {
        format!(
            " max_backlog_ops={max_backlog} stall={} drained_at_s={}{}",
            if stall_events.is_empty() { "none".to_owned() } else { stall_events.join(",") },
            drained_at.map_or("n/a".to_owned(), |t| format!("{t:.2}")),
            crate::journal_kv(&k.dir)
        )
    } else {
        String::new()
    };
    println!(
        "kind=alarm-storm label={label} scheduler={} sink={sink_mode} clients={clients} size={size} rules_per_tag={} capacity={} secs={secs} interval_us={interval_us} batches_s={:.0} events_s={:.0} released_s={:.0} refused={refused} refused_s={:.0} rejected_batches_total={} p50_ns={p50} p95_ns={p95} p99_ns={p99} p999_ns={p999} max_used={max_used} used_end={} next_event_seq={} time_to_full_s={} ctl_probe_max_us={} cpu_pct={:.1} cpu_threads={} rss_idle_kib={} rss_steady_kib={} rss_peak_kib={} {verdict}",
        crate::scheduler_of(&k.ready),
        a.rules_per_tag,
        a.capacity,
        applied as f64 / wall,
        events as f64 / wall,
        released_total as f64 / wall,
        refused as f64 / wall,
        c1.rejected_batches,
        c1.used,
        c1.next_event_seq,
        time_to_full.map_or("never".to_owned(), |t| format!("{t:.2}")),
        probe_max / 1000,
        usage.cpu_pct,
        usage.cpu_by_thread,
        usage.rss_idle,
        usage.rss_steady,
        usage.peak
    );
    if !jkv.is_empty() {
        println!("kind=alarm-storm-journal label={label}{jkv}");
    }
}
