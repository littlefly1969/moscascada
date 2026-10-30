//! `scada-bench`: reproducible measurements for docs/benchmarks/vs001.md.
//!
//! ```text
//! scada-bench core       [--ops N]                      in-memory Kernel::apply
//! scada-bench e2e        [--secs S] [--warmup-secs W]   kernel process + client threads over UDS
//! scada-bench saturation [--secs S]                     producers faster than the sequencer
//! ```
//! Output is Markdown on stdout. Numbers are whatever this machine measures; nothing is
//! a target or threshold.

mod alloc;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use scada_core::{
    BatchMode, ChannelId, ChannelSpec, ConfigSpec, CoreLimits, Kernel, Observation, ObservationBatch, Operation,
    SequencedInput, SourceId, SourceSpec, TagId, TagSpec, ValidatedConfig, Value, ValueType,
};
use scada_protocol::driver_v1::{AckStatus, BatchMode as WireMode};
use scada_protocol::frame;
use scada_protocol::introspect_v1::{IntrospectRequest, IntrospectResponse};
use scada_sim_driver::{Session, Val, batch};

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

const CHANNELS: u32 = 128;
const SOURCES: u32 = 8;

fn arg(name: &str, default: u64) -> u64 {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    match mode.as_str() {
        "core" => core_bench(arg("--ops", 1_000_000)),
        "e2e" => e2e_bench(arg("--secs", 10), arg("--warmup-secs", 2)),
        "saturation" => saturation(arg("--secs", 60)),
        _ => {
            eprintln!("usage: scada-bench core|e2e|saturation [--ops N] [--secs S] [--warmup-secs W]");
            std::process::exit(2);
        }
    }
}

// ---------------------------------------------------------------- statistics

struct Stats {
    n: usize,
    p50: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

/// Exact nearest-rank percentiles over all samples (nanoseconds).
fn stats(mut v: Vec<u64>) -> Stats {
    v.sort_unstable();
    let n = v.len();
    let at = |q: f64| v[((q * n as f64).ceil() as usize).clamp(1, n) - 1];
    Stats { n, p50: at(0.50), p99: at(0.99), p999: at(0.999), max: v[n - 1] }
}

fn us(ns: u64) -> String {
    format!("{:.2}", ns as f64 / 1000.0)
}

fn proc_status(pid: u32, key: &str) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// utime + stime in clock ticks.
fn proc_cpu_ticks(pid: u32) -> u64 {
    let s = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    // Fields after the parenthesised command name; utime/stime are fields 14/15 overall.
    let rest = s.rsplit_once(')').map_or("", |x| x.1);
    let f: Vec<&str> = rest.split_whitespace().collect();
    f.get(11).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0)
        + f.get(12).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0)
}

fn clk_tck() -> u64 {
    Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(100)
}

// ---------------------------------------------------------------- core benchmark

fn bench_config(sources: u32) -> ConfigSpec {
    let mut tags = Vec::new();
    let mut srcs = Vec::new();
    for s in 0..sources {
        let mut channels = Vec::new();
        for c in 0..CHANNELS {
            let tag_id = TagId(s * 1000 + c + 1);
            tags.push(TagSpec { tag_id, name: format!("s{s}_c{c}"), value_type: ValueType::I64 });
            channels.push(ChannelSpec { channel_id: ChannelId(c + 1), tag_id, value_type: ValueType::I64 });
        }
        srcs.push(SourceSpec { source_id: SourceId(s + 1), name: format!("load-{s}"), channels });
    }
    ConfigSpec { limits: CoreLimits { max_observations_per_batch: CHANNELS }, tags, sources: srcs }
}

fn core_bench(ops: u64) {
    println!("## Kernel in-memory (`scada-bench core --ops {ops}`)\n");
    println!("Config: 1 source x {CHANNELS} i64 channels. Every Delta changes every carried value (revision +1 each).");
    println!("Timing: `Instant::now()` around each single `apply` call (timer overhead included, measured below).");
    println!("Inputs are pre-built in chunks of 10 000 outside the timed region.\n");
    // Timer overhead.
    let mut t = Vec::with_capacity(100_000);
    for _ in 0..100_000 {
        let a = Instant::now();
        let b = Instant::now();
        t.push((b - a).as_nanos() as u64);
    }
    let to = stats(t);
    println!("Timer overhead (empty `Instant` pair): p50 {} ns, p99 {} ns\n", to.p50, to.p99);
    println!(
        "| batch size | ops | transitions/s | p50 µs | p99 µs | p99.9 µs | max µs | allocs/op in apply | live heap Δ (bytes) | RSS start→end KiB |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for size in [1u32, 32, 128] {
        let cfg = Arc::new(ValidatedConfig::validate(bench_config(1)).expect("config"));
        let mut k = Kernel::new(cfg);
        let src = SourceId(1);
        k.apply(SequencedInput {
            operation_sequence: 1,
            operation: Operation::SourceConnected { source_id: src, link_generation: 1, source_epoch: 1 },
        })
        .expect("connect");
        let full: Vec<Observation> =
            (1..=CHANNELS).map(|c| Observation { channel_id: ChannelId(c), value: Value::I64(0) }).collect();
        k.apply(SequencedInput {
            operation_sequence: 2,
            operation: Operation::ObservationBatch(ObservationBatch {
                source_id: src,
                link_generation: 1,
                mode: BatchMode::FullState,
                source_sequence: 1,
                observations: full,
            }),
        })
        .expect("full");
        let mk = |i: u64| SequencedInput {
            operation_sequence: 2 + i,
            operation: Operation::ObservationBatch(ObservationBatch {
                source_id: src,
                link_generation: 1,
                mode: BatchMode::Delta,
                source_sequence: 1 + i,
                observations: (0..size)
                    .map(|j| Observation {
                        channel_id: ChannelId(1 + ((i as u32 * size + j) % CHANNELS)),
                        value: Value::I64(i as i64),
                    })
                    .collect(),
            }),
        };
        // Warm-up: 10% of ops (not recorded).
        let warm = ops / 10;
        let mut i = 1u64;
        while i <= warm {
            k.apply(mk(i)).expect("warm-up apply");
            i += 1;
        }
        // Measurement buffers are allocated before the heap baseline is taken.
        let mut samples = Vec::with_capacity(ops as usize);
        let (mut allocs, mut chunk) = (0u64, Vec::with_capacity(10_000));
        let rss0 = proc_status(std::process::id(), "VmRSS:");
        let live0 = alloc::LIVE_BYTES.load(Relaxed);
        let end = warm + ops;
        let mut busy = Duration::ZERO;
        while i <= end {
            chunk.clear();
            let upto = (i + 10_000).min(end + 1);
            chunk.extend((i..upto).map(mk));
            for input in chunk.drain(..) {
                let a0 = alloc::ALLOCS.load(Relaxed);
                let t0 = Instant::now();
                let r = k.apply(input);
                let dt = t0.elapsed();
                allocs += alloc::ALLOCS.load(Relaxed) - a0;
                busy += dt;
                samples.push(dt.as_nanos() as u64);
                assert!(r.is_ok(), "{r:?}");
            }
            i = upto;
        }
        let live1 = alloc::LIVE_BYTES.load(Relaxed);
        drop(chunk);
        let rss1 = proc_status(std::process::id(), "VmRSS:");
        let s = stats(samples);
        let tps = s.n as f64 / busy.as_secs_f64();
        // Keep the kernel alive until after the measurement.
        assert_eq!(k.last_operation_sequence(), 2 + end);
        println!(
            "| {size} | {} | {:.0} | {} | {} | {} | {} | {:.3} | {} | {rss0}→{rss1} |",
            s.n,
            tps,
            us(s.p50),
            us(s.p99),
            us(s.p999),
            us(s.max),
            allocs as f64 / s.n as f64,
            live1 - live0
        );
    }
    println!("\ntransitions/s = ops / sum of measured `apply` durations (excludes input construction).");
    println!(
        "live heap Δ = allocator live bytes after the run minus before (inputs already consumed): ~0 means no growth.\n"
    );
    // Digest on demand, for reference.
    let cfg = Arc::new(ValidatedConfig::validate(bench_config(SOURCES)).expect("config"));
    let k = Kernel::new(cfg);
    let mut d = Vec::new();
    for _ in 0..10_000 {
        let t0 = Instant::now();
        std::hint::black_box(k.state_digest());
        d.push(t0.elapsed().as_nanos() as u64);
    }
    let s = stats(d);
    println!(
        "`state_digest()` on demand, {} tags: p50 {} µs, p99 {} µs (not on the hot path).\n",
        SOURCES * CHANNELS,
        us(s.p50),
        us(s.p99)
    );
}

// ---------------------------------------------------------------- process helpers

fn kernel_bin() -> PathBuf {
    std::env::current_exe().expect("exe").with_file_name("scada-kernel")
}

fn write_config(dir: &Path, queue: u32) -> PathBuf {
    let mut s = format!(
        "[ipc]\nsocket_path = \"{}\"\nmax_frame_bytes = 65536\nmax_driver_connections = 16\ninput_queue_capacity = {queue}\nmax_inflight_per_connection = 32\nhandshake_timeout_ms = 2000\nframe_timeout_ms = 2000\n\n[core]\nmax_observations_per_batch = {CHANNELS}\n",
        dir.join("driver.sock").display()
    );
    for src in 1..=SOURCES {
        for c in 1..=CHANNELS {
            s.push_str(&format!("\n[[tags]]\nid = {}\nname = \"s{src}_c{c}\"\ntype = \"i64\"\n", src * 1000 + c));
        }
    }
    for src in 1..=SOURCES {
        s.push_str(&format!("\n[[sources]]\nid = {src}\nname = \"load-{src}\"\nchannels = [\n"));
        for c in 1..=CHANNELS {
            s.push_str(&format!("  {{ channel = {c}, tag = {}, type = \"i64\" }},\n", src * 1000 + c));
        }
        s.push_str("]\n");
    }
    let p = dir.join("bench.toml");
    std::fs::write(&p, s).expect("write config");
    p
}

struct KernelProc {
    child: Child,
    dir: PathBuf,
}

impl KernelProc {
    fn start(tag: &str, queue: u32, extra: &[&str]) -> KernelProc {
        let dir = std::env::temp_dir().join(format!("scada-bench-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let cfg = write_config(&dir, queue);
        let mut child = Command::new(kernel_bin())
            .arg("--config")
            .arg(&cfg)
            .arg("--introspection-socket")
            .arg(dir.join("intro.sock"))
            .args(extra)
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn kernel (build with --release first)");
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("stdout")).read_line(&mut line).expect("READY");
        assert!(line.starts_with("READY"), "{line}");
        KernelProc { child, dir }
    }
    fn socket(&self) -> PathBuf {
        self.dir.join("driver.sock")
    }
    fn pid(&self) -> u32 {
        self.child.id()
    }
    fn introspect(&self) -> IntrospectResponse {
        let mut s = UnixStream::connect(self.dir.join("intro.sock")).expect("intro");
        let mut w = Vec::new();
        frame::write_message(&mut s, &IntrospectRequest { include_tags: false, introspect_version: 1 }, 1024, &mut w)
            .expect("req");
        let mut r = Vec::new();
        frame::read_message(&mut s, frame::MAX_FRAME_BYTES_CEILING, &mut r).expect("resp").expect("resp")
    }
}

impl Drop for KernelProc {
    fn drop(&mut self) {
        let _ = Command::new("kill").arg("-TERM").arg(self.child.id().to_string()).status();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn delta(seq: u64, size: u32, v: i64) -> scada_protocol::driver_v1::DriverMessage {
    let obs: Vec<(u32, Val)> = (1..=size).map(|c| (c, Val::I(v))).collect();
    batch(WireMode::Delta, seq, &obs)
}

fn open(socket: &Path, source: u32, epoch: u64) -> (Session, u64) {
    let mut s = Session::connect(socket, "bench", source, epoch).expect("connect");
    let obs: Vec<(u32, Val)> = (1..=CHANNELS).map(|c| (c, Val::I(0))).collect();
    let next = s.welcome.last_accepted_source_sequence.map_or(1, |l| l + 1);
    let a = s.request(&batch(WireMode::FullState, next, &obs)).expect("full state");
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
    (s, next + 1)
}

// ---------------------------------------------------------------- end-to-end

struct ClientResult {
    lat: Vec<u64>,
    batches: u64,
}

/// Stop-and-wait client: every batch waits for its ACK; latency = send -> AppliedVolatile ACK.
fn run_client(socket: PathBuf, source: u32, size: u32, warmup: Duration, dur: Duration, pipeline: u64) -> ClientResult {
    let epoch = 1 + u64::from(source) + (Instant::now().elapsed().as_nanos() as u64);
    let (mut s, mut seq) = open(&socket, source, epoch * 7919);
    let mut lat = Vec::new();
    let mut batches = 0u64;
    let start = Instant::now();
    let mut v = 1i64;
    if pipeline <= 1 {
        loop {
            let el = start.elapsed();
            if el >= warmup + dur {
                break;
            }
            let t0 = Instant::now();
            let a = s.request(&delta(seq, size, v)).expect("request");
            let dt = t0.elapsed().as_nanos() as u64;
            assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
            if el >= warmup {
                lat.push(dt);
                batches += 1;
            }
            seq += 1;
            v += 1;
        }
    } else {
        // Pipelined: keep `pipeline` batches in flight; measures throughput only.
        let (mut sent, mut acked, mut counted) = (0u64, 0u64, 0u64);
        let mut measuring = false;
        loop {
            let el = start.elapsed();
            if !measuring && el >= warmup {
                measuring = true;
                counted = 0;
            }
            let done = el >= warmup + dur;
            while !done && sent - acked < pipeline {
                s.send(&delta(seq + sent, size, v)).expect("send");
                sent += 1;
                v += 1;
            }
            if acked == sent {
                break;
            }
            let a = s.recv_ack().expect("ack");
            assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
            acked += 1;
            if measuring && !done {
                counted += 1;
            }
        }
        batches = counted;
    }
    ClientResult { lat, batches }
}

fn e2e_bench(secs: u64, warmup_secs: u64) {
    let tck = clk_tck();
    println!("## End-to-end IPC (`scada-bench e2e --secs {secs} --warmup-secs {warmup_secs}`)\n");
    println!(
        "Kernel: release `scada-kernel` process, {SOURCES} sources x {CHANNELS} i64 channels ({} tags), queue 64.",
        SOURCES * CHANNELS
    );
    println!(
        "Clients: threads of the bench process, one connection and one source each, every observation changes value."
    );
    println!("Each cell: fresh kernel, {warmup_secs} s warm-up (discarded), {secs} s measured.\n");
    println!("### Stop-and-wait (latency send → `AppliedVolatile` ACK)\n");
    println!(
        "| clients | obs/batch | batches/s | obs/s | p50 µs | p99 µs | p99.9 µs | max µs | kernel CPU % | kernel RSS KiB (idle→steady, peak) | kernel threads |"
    );
    println!("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|");
    for clients in [1u32, 8] {
        for size in [1u32, 32, 128] {
            cell(clients, size, secs, warmup_secs, 1, tck);
        }
    }
    println!("\n### Pipelined throughput (up to 32 batches in flight per connection)\n");
    println!(
        "| clients | obs/batch | batches/s | obs/s | kernel CPU % | kernel RSS KiB (idle→steady, peak) | kernel threads |"
    );
    println!("|---:|---:|---:|---:|---:|---|---:|");
    for clients in [1u32, 8] {
        for size in [1u32, 32, 128] {
            cell(clients, size, secs, warmup_secs, 32, tck);
        }
    }
    println!();
}

fn cell(clients: u32, size: u32, secs: u64, warmup_secs: u64, pipeline: u64, tck: u64) {
    let k = KernelProc::start(&format!("e2e-{clients}-{size}-{pipeline}"), 64, &[]);
    std::thread::sleep(Duration::from_millis(200));
    let rss_idle = proc_status(k.pid(), "VmRSS:");
    let warm = Duration::from_secs(warmup_secs);
    let dur = Duration::from_secs(secs);
    let handles: Vec<_> = (1..=clients)
        .map(|c| {
            let sock = k.socket();
            std::thread::spawn(move || run_client(sock, c, size, warm, dur, pipeline))
        })
        .collect();
    std::thread::sleep(warm);
    let cpu0 = proc_cpu_ticks(k.pid());
    let t0 = Instant::now();
    let mut rss_steady = 0;
    let mut threads = 0;
    while t0.elapsed() < dur {
        std::thread::sleep(Duration::from_millis(250));
        rss_steady = rss_steady.max(proc_status(k.pid(), "VmRSS:"));
        threads = threads.max(proc_status(k.pid(), "Threads:"));
    }
    let cpu1 = proc_cpu_ticks(k.pid());
    let wall = t0.elapsed().as_secs_f64();
    let results: Vec<ClientResult> = handles.into_iter().map(|h| h.join().expect("client")).collect();
    let peak = proc_status(k.pid(), "VmHWM:");
    let batches: u64 = results.iter().map(|r| r.batches).sum();
    let bps = batches as f64 / secs as f64;
    let cpu = (cpu1 - cpu0) as f64 / tck as f64 / wall * 100.0;
    let applied = k.introspect().counters.map_or(0, |c| c.batches_applied);
    assert!(applied >= batches);
    if pipeline <= 1 {
        let lat: Vec<u64> = results.into_iter().flat_map(|r| r.lat).collect();
        let s = stats(lat);
        println!(
            "| {clients} | {size} | {bps:.0} | {:.0} | {} | {} | {} | {} | {cpu:.0} | {rss_idle}→{rss_steady}, {peak} | {threads} |",
            bps * f64::from(size),
            us(s.p50),
            us(s.p99),
            us(s.p999),
            us(s.max)
        );
    } else {
        println!(
            "| {clients} | {size} | {bps:.0} | {:.0} | {cpu:.0} | {rss_idle}→{rss_steady}, {peak} | {threads} |",
            bps * f64::from(size)
        );
    }
    let _ = std::io::stdout().flush();
}

// ---------------------------------------------------------------- saturation

fn saturation(secs: u64) {
    let throttle_us = 200u64;
    println!("## Saturation (`scada-bench saturation --secs {secs}`)\n");
    println!(
        "Kernel started with `--throttle-sequencer-us {throttle_us}` (artificial 200 µs per request in the sequencer"
    );
    println!(
        "thread) so that 8 pipelined producers (32 in flight each, 128-observation batches) are faster than the consumer."
    );
    println!("Queue capacity 64. RSS sampled every second.\n");
    let k = KernelProc::start("sat", 64, &["--throttle-sequencer-us", &throttle_us.to_string()]);
    let rss_idle = proc_status(k.pid(), "VmRSS:");
    let dur = Duration::from_secs(secs);
    let handles: Vec<_> = (1..=SOURCES)
        .map(|c| {
            let sock = k.socket();
            std::thread::spawn(move || run_client(sock, c, CHANNELS, Duration::ZERO, dur, 32))
        })
        .collect();
    let t0 = Instant::now();
    let mut series = Vec::new();
    while t0.elapsed() < dur {
        std::thread::sleep(Duration::from_secs(1));
        let c = k.introspect().counters.expect("counters");
        series.push((
            t0.elapsed().as_secs(),
            proc_status(k.pid(), "VmRSS:"),
            c.batches_applied,
            c.input_queue_full_waits,
        ));
    }
    let results: Vec<ClientResult> = handles.into_iter().map(|h| h.join().expect("client")).collect();
    let acked: u64 = results.iter().map(|r| r.batches).sum();
    let c = k.introspect().counters.expect("counters");
    let peak = proc_status(k.pid(), "VmHWM:");
    println!("| t (s) | kernel RSS KiB | batches applied (cum.) | queue-full waits (cum.) |");
    println!("|---:|---:|---:|---:|");
    for (t, rss, applied, waits) in &series {
        if *t <= 5 || t % 10 == 0 || *t + 1 >= secs {
            println!("| {t} | {rss} | {applied} | {waits} |");
        }
    }
    let rss_vals: Vec<u64> = series.iter().map(|x| x.1).collect();
    let (mn, mx) = (rss_vals.iter().min().copied().unwrap_or(0), rss_vals.iter().max().copied().unwrap_or(0));
    let second_half: Vec<u64> = rss_vals[rss_vals.len() / 2..].to_vec();
    println!();
    println!("- RSS idle {rss_idle} KiB; under load min {mn} KiB, max {mx} KiB; VmHWM {peak} KiB.");
    println!(
        "- RSS second half of the run: min {} KiB, max {} KiB.",
        second_half.iter().min().copied().unwrap_or(0),
        second_half.iter().max().copied().unwrap_or(0)
    );
    println!(
        "- Batches applied {}, duplicates {}, rejected {}; ACKs counted by producers inside the window {acked}.",
        c.batches_applied, c.batches_duplicate, c.batches_rejected
    );
    println!("- Queue-full waits (producer blocked on the full input queue): {}.", c.input_queue_full_waits);
    println!(
        "- Theoretical consumer ceiling with the throttle: {:.0} requests/s; observed {:.0} batches/s.\n",
        1e6 / throttle_us as f64,
        c.batches_applied as f64 / secs as f64
    );
}
