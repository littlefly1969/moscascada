//! `scada-bench`: reproducible measurements for docs/benchmarks/vs001.md and vs002.md.
//!
//! ```text
//! scada-bench core       [--ops N]                      in-memory Kernel::apply
//! scada-bench e2e        [--secs S] [--warmup-secs W]   VS-001 table (stop-and-wait + pipelined)
//! scada-bench saturation [--secs S] [--clients N]       producers faster than the sequencer
//! scada-bench cell  --clients N --size S --pipeline P [--secs S] [--warmup-secs W]
//! scada-bench mixed --hv N [--rare-interval-us U] [--secs S] [--warmup-secs W]
//! ```
//! Common options for `cell`, `mixed`, `saturation`:
//! `--kernel PATH` (default: `scada-kernel` next to this binary), `--queue Q`,
//! `--kernel-arg ARG` (repeatable, passed verbatim to the kernel), `--label TEXT`.
//! `cell` and `mixed` print ONE machine-readable line `key=value ...` for campaign scripts
//! (scripts/bench-vs002.sh); the others print Markdown. Numbers are whatever this machine
//! measures; nothing is a target or threshold.

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
/// High-volume sources 1..=LOAD_SOURCES (128 channels each).
const LOAD_SOURCES: u32 = 16;
/// Low-traffic "critical in the benchmark" source: 1 channel. No alarm semantics in the core.
const RARE_SOURCE: u32 = 900;
/// VS-001 table used 8 sources; kept for `core`.
const SOURCES: u32 = 8;

fn arg(name: &str, default: u64) -> u64 {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1)).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn arg_str(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1)).cloned()
}

fn args_multi(name: &str) -> Vec<String> {
    let a: Vec<String> = std::env::args().collect();
    a.windows(2).filter(|w| w[0] == name).map(|w| w[1].clone()).collect()
}

/// Kernel binary and arguments shared by the process benchmarks.
#[derive(Clone)]
struct KernelSpec {
    bin: PathBuf,
    queue: u32,
    args: Vec<String>,
}

impl KernelSpec {
    fn from_args() -> KernelSpec {
        KernelSpec {
            bin: arg_str("--kernel").map_or_else(kernel_bin, PathBuf::from),
            queue: arg("--queue", 64) as u32,
            args: args_multi("--kernel-arg"),
        }
    }
}

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    match mode.as_str() {
        "core" => core_bench(arg("--ops", 1_000_000)),
        "e2e" => e2e_bench(&KernelSpec::from_args(), arg("--secs", 10), arg("--warmup-secs", 2)),
        "saturation" => saturation(&KernelSpec::from_args(), arg("--secs", 60), arg("--clients", 8) as u32),
        "cell" => {
            let r = cell(
                &KernelSpec::from_args(),
                arg("--clients", 1) as u32,
                arg("--size", 128) as u32,
                arg("--secs", 5),
                arg("--warmup-secs", 1),
                arg("--pipeline", 1),
            );
            println!("{}", r.line(&arg_str("--label").unwrap_or_default()));
        }
        "mixed" => mixed(
            &KernelSpec::from_args(),
            arg("--hv", 8) as u32,
            arg("--rare-interval-us", 1000),
            arg("--secs", 10),
            arg("--warmup-secs", 1),
            &arg_str("--label").unwrap_or_default(),
        ),
        _ => {
            eprintln!("usage: scada-bench core|e2e|saturation|cell|mixed [options] (see module docs)");
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
    if n == 0 {
        // e.g. `mixed --hv 0` (idle cell): no high-volume samples.
        return Stats { n: 0, p50: 0, p99: 0, p999: 0, max: 0 };
    }
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

/// 16 load sources x 128 i64 channels (2048 tags) + the rare source (1 i64 channel).
fn write_config(dir: &Path, queue: u32, max_conn: u32) -> PathBuf {
    let mut s = format!(
        "[ipc]\nsocket_path = \"{}\"\nmax_frame_bytes = 65536\nmax_driver_connections = {max_conn}\ninput_queue_capacity = {queue}\nmax_inflight_per_connection = 32\nhandshake_timeout_ms = 2000\nframe_timeout_ms = 2000\n\n[core]\nmax_observations_per_batch = {CHANNELS}\n",
        dir.join("driver.sock").display()
    );
    for src in 1..=LOAD_SOURCES {
        for c in 1..=CHANNELS {
            s.push_str(&format!("\n[[tags]]\nid = {}\nname = \"s{src}_c{c}\"\ntype = \"i64\"\n", src * 1000 + c));
        }
    }
    s.push_str(&format!("\n[[tags]]\nid = {}\nname = \"rare\"\ntype = \"i64\"\n", RARE_SOURCE * 1000 + 1));
    for src in 1..=LOAD_SOURCES {
        s.push_str(&format!("\n[[sources]]\nid = {src}\nname = \"load-{src}\"\nchannels = [\n"));
        for c in 1..=CHANNELS {
            s.push_str(&format!("  {{ channel = {c}, tag = {}, type = \"i64\" }},\n", src * 1000 + c));
        }
        s.push_str("]\n");
    }
    s.push_str(&format!(
        "\n[[sources]]\nid = {RARE_SOURCE}\nname = \"rare\"\nchannels = [ {{ channel = 1, tag = {}, type = \"i64\" }} ]\n",
        RARE_SOURCE * 1000 + 1
    ));
    let p = dir.join("bench.toml");
    std::fs::write(&p, s).expect("write config");
    p
}

struct KernelProc {
    child: Child,
    dir: PathBuf,
    ready: String,
}

impl KernelProc {
    fn start(spec: &KernelSpec, tag: &str, extra: &[&str]) -> KernelProc {
        let dir = std::env::temp_dir().join(format!("scada-bench-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        // The kernel refuses group/world-writable socket directories (VS-002 E2).
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        let cfg = write_config(&dir, spec.queue, LOAD_SOURCES + 8);
        let mut child = Command::new(&spec.bin)
            .arg("--config")
            .arg(&cfg)
            .arg("--introspection-socket")
            .arg(dir.join("intro.sock"))
            .args(&spec.args)
            .args(extra)
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn kernel (build with --release first)");
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("stdout")).read_line(&mut line).expect("READY");
        assert!(line.starts_with("READY"), "{line}");
        KernelProc { child, dir, ready: line.trim().to_owned() }
    }
    fn socket(&self) -> PathBuf {
        self.dir.join("driver.sock")
    }
    fn pid(&self) -> u32 {
        self.child.id()
    }
    fn introspect_with(&self, reset: bool) -> IntrospectResponse {
        let mut s = UnixStream::connect(self.dir.join("intro.sock")).expect("intro");
        let mut w = Vec::new();
        frame::write_message(
            &mut s,
            &IntrospectRequest { include_tags: false, introspect_version: 1, reset_diagnostics: reset },
            1024,
            &mut w,
        )
        .expect("req");
        let mut r = Vec::new();
        frame::read_message(&mut s, frame::MAX_FRAME_BYTES_CEILING, &mut r).expect("resp").expect("resp")
    }
    fn introspect(&self) -> IntrospectResponse {
        self.introspect_with(false)
    }
}

impl Drop for KernelProc {
    fn drop(&mut self) {
        let _ = Command::new("kill").arg("-TERM").arg(self.child.id().to_string()).status();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Per-thread CPU ticks and context switches of a process, grouped by thread name.
#[derive(Default, Clone)]
struct ThreadSample {
    /// (thread name group, cpu ticks)
    cpu: Vec<(String, u64)>,
    voluntary: u64,
    involuntary: u64,
}

fn thread_group(comm: &str) -> String {
    if comm.starts_with("scada-io") {
        "io".into()
    } else if comm.starts_with("scada-sequencer") {
        "seq".into()
    } else {
        "main".into()
    }
}

fn sample_threads(pid: u32) -> ThreadSample {
    let mut out = ThreadSample::default();
    let Ok(rd) = std::fs::read_dir(format!("/proc/{pid}/task")) else { return out };
    for e in rd.flatten() {
        let p = e.path();
        let stat = std::fs::read_to_string(p.join("stat")).unwrap_or_default();
        let comm = stat.split_once('(').and_then(|x| x.1.rsplit_once(')')).map_or("", |x| x.0).to_owned();
        let rest = stat.rsplit_once(')').map_or("", |x| x.1);
        let f: Vec<&str> = rest.split_whitespace().collect();
        let ticks = f.get(11).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0)
            + f.get(12).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0);
        let g = thread_group(&comm);
        match out.cpu.iter_mut().find(|(n, _)| *n == g) {
            Some((_, t)) => *t += ticks,
            None => out.cpu.push((g, ticks)),
        }
        let status = std::fs::read_to_string(p.join("status")).unwrap_or_default();
        for l in status.lines() {
            let v = || l.split_whitespace().nth(1).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            if l.starts_with("voluntary_ctxt_switches:") {
                out.voluntary += v();
            } else if l.starts_with("nonvoluntary_ctxt_switches:") {
                out.involuntary += v();
            }
        }
    }
    out.cpu.sort();
    out
}

fn delta(seq: u64, size: u32, v: i64) -> scada_protocol::driver_v1::DriverMessage {
    let obs: Vec<(u32, Val)> = (1..=size).map(|c| (c, Val::I(v))).collect();
    batch(WireMode::Delta, seq, &obs)
}

fn open(socket: &Path, source: u32, epoch: u64, channels: u32) -> (Session, u64) {
    let mut s = Session::connect(socket, "bench", source, epoch).expect("connect");
    let obs: Vec<(u32, Val)> = (1..=channels).map(|c| (c, Val::I(0))).collect();
    let next = s.welcome.last_accepted_source_sequence.map_or(1, |l| l + 1);
    let a = s.request(&batch(WireMode::FullState, next, &obs)).expect("full state");
    assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
    (s, next + 1)
}

fn epoch_for(source: u32) -> u64 {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(1, |d| d.as_nanos() as u64);
    (t ^ (u64::from(source) << 48)).max(1)
}

// ---------------------------------------------------------------- end-to-end

struct ClientResult {
    lat: Vec<u64>,
    batches: u64,
}

/// One client: `pipeline` = 1 is stop-and-wait, > 1 keeps that many batches in flight.
/// Latency = send of a batch -> its `AppliedVolatile` ACK, in both modes (pipelined
/// latency therefore includes waiting behind the client's own earlier batches).
/// `interval`: stop-and-wait only, sleep between batches (low-traffic source).
#[allow(clippy::too_many_arguments)]
fn run_client(
    socket: PathBuf,
    source: u32,
    size: u32,
    channels: u32,
    warmup: Duration,
    dur: Duration,
    pipeline: u64,
    interval: Option<Duration>,
) -> ClientResult {
    let (mut s, mut seq) = open(&socket, source, epoch_for(source), channels);
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
            let msg = delta(seq, size, v);
            let t0 = Instant::now();
            let a = s.request(&msg).expect("request");
            let dt = t0.elapsed().as_nanos() as u64;
            assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
            assert_eq!(a.source_sequence, seq, "ACK out of order");
            if el >= warmup {
                lat.push(dt);
                batches += 1;
            }
            seq += 1;
            v += 1;
            if let Some(i) = interval {
                std::thread::sleep(i);
            }
        }
    } else {
        let (mut sent, mut acked, mut counted) = (0u64, 0u64, 0u64);
        let mut measuring = false;
        let mut sent_at: std::collections::VecDeque<Instant> = std::collections::VecDeque::with_capacity(64);
        loop {
            let el = start.elapsed();
            if !measuring && el >= warmup {
                measuring = true;
                counted = 0;
            }
            let done = el >= warmup + dur;
            let mut queued = false;
            while !done && sent - acked < pipeline {
                s.send(&delta(seq + sent, size, v)).expect("send");
                sent_at.push_back(Instant::now());
                sent += 1;
                v += 1;
                queued = true;
            }
            if queued {
                s.flush().expect("flush");
            }
            if acked == sent {
                break;
            }
            let a = s.recv_ack().expect("ack");
            let t = sent_at.pop_front().expect("in flight");
            assert_eq!(a.status, AckStatus::AppliedVolatile as i32, "{a:?}");
            assert_eq!(a.source_sequence, seq + acked, "ACK out of order");
            acked += 1;
            if measuring && !done {
                counted += 1;
                lat.push(t.elapsed().as_nanos() as u64);
            }
        }
        batches = counted;
    }
    ClientResult { lat, batches }
}

/// Kernel-side resource usage over a measured window.
struct Usage {
    cpu_pct: f64,
    cpu_by_thread: String,
    ctx_vol_per_s: f64,
    ctx_invol_per_s: f64,
    rss_idle: u64,
    rss_steady: u64,
    peak: u64,
    threads: u64,
}

fn measure_window(pid: u32, dur: Duration, rss_idle: u64) -> Usage {
    let tck = clk_tck() as f64;
    let s0 = sample_threads(pid);
    let cpu0 = proc_cpu_ticks(pid);
    let t0 = Instant::now();
    let (mut rss_steady, mut threads) = (0, 0);
    while t0.elapsed() < dur {
        std::thread::sleep(Duration::from_millis(250).min(dur.saturating_sub(t0.elapsed())));
        rss_steady = rss_steady.max(proc_status(pid, "VmRSS:"));
        threads = threads.max(proc_status(pid, "Threads:"));
    }
    let cpu1 = proc_cpu_ticks(pid);
    let s1 = sample_threads(pid);
    let wall = t0.elapsed().as_secs_f64();
    let cpu_by_thread = s1
        .cpu
        .iter()
        .map(|(n, t)| {
            let before = s0.cpu.iter().find(|(m, _)| m == n).map_or(0, |x| x.1);
            format!("{n}:{:.0}", (t.saturating_sub(before)) as f64 / tck / wall * 100.0)
        })
        .collect::<Vec<_>>()
        .join(",");
    Usage {
        cpu_pct: (cpu1 - cpu0) as f64 / tck / wall * 100.0,
        cpu_by_thread,
        ctx_vol_per_s: (s1.voluntary.saturating_sub(s0.voluntary)) as f64 / wall,
        ctx_invol_per_s: (s1.involuntary.saturating_sub(s0.involuntary)) as f64 / wall,
        rss_idle,
        rss_steady,
        peak: proc_status(pid, "VmHWM:"),
        threads,
    }
}

fn stages_kv(r: &IntrospectResponse) -> String {
    r.stages
        .iter()
        .filter(|s| s.count > 0)
        .map(|s| {
            format!(
                " d_{n}_n={} d_{n}_mean={:.0} d_{n}_p50={} d_{n}_p99={} d_{n}_p999={} d_{n}_max={}",
                s.count,
                s.sum_ns as f64 / s.count as f64,
                s.p50_ns,
                s.p99_ns,
                s.p999_ns,
                s.max_ns,
                n = s.name
            )
        })
        .collect()
}

struct CellResult {
    clients: u32,
    size: u32,
    pipeline: u64,
    bps: f64,
    lat: Option<Stats>,
    usage: Usage,
    scheduler: String,
    diag: String,
}

impl CellResult {
    fn line(&self, label: &str) -> String {
        let u = &self.usage;
        let (p50, p99, p999, max) = self.lat.as_ref().map_or((0, 0, 0, 0), |s| (s.p50, s.p99, s.p999, s.max));
        format!(
            "kind=cell label={label} scheduler={} clients={} size={} pipeline={} batches_s={:.0} obs_s={:.0} p50_ns={p50} p99_ns={p99} p999_ns={p999} max_ns={max} cpu_pct={:.1} cpu_threads={} ctx_vol_s={:.0} ctx_invol_s={:.0} rss_idle_kib={} rss_steady_kib={} rss_peak_kib={} threads={}{}",
            self.scheduler,
            self.clients,
            self.size,
            self.pipeline,
            self.bps,
            self.bps * f64::from(self.size),
            u.cpu_pct,
            u.cpu_by_thread,
            u.ctx_vol_per_s,
            u.ctx_invol_per_s,
            u.rss_idle,
            u.rss_steady,
            u.peak,
            u.threads,
            self.diag
        )
    }
}

fn scheduler_of(ready: &str) -> String {
    ready.split_whitespace().find_map(|w| w.strip_prefix("scheduler=")).unwrap_or("current_thread").to_owned()
}

fn cell(spec: &KernelSpec, clients: u32, size: u32, secs: u64, warmup_secs: u64, pipeline: u64) -> CellResult {
    assert!(clients <= LOAD_SOURCES);
    let k = KernelProc::start(spec, &format!("cell-{clients}-{size}-{pipeline}"), &[]);
    std::thread::sleep(Duration::from_millis(200));
    let rss_idle = proc_status(k.pid(), "VmRSS:");
    let warm = Duration::from_secs(warmup_secs);
    let dur = Duration::from_secs(secs);
    let handles: Vec<_> = (1..=clients)
        .map(|c| {
            let sock = k.socket();
            std::thread::spawn(move || run_client(sock, c, size, CHANNELS, warm, dur, pipeline, None))
        })
        .collect();
    std::thread::sleep(warm);
    let _ = k.introspect_with(true); // reset diagnostics after warm-up
    let usage = measure_window(k.pid(), dur, rss_idle);
    let intro = k.introspect();
    let results: Vec<ClientResult> = handles.into_iter().map(|h| h.join().expect("client")).collect();
    let batches: u64 = results.iter().map(|r| r.batches).sum();
    let applied = intro.counters.as_ref().map_or(0, |c| c.batches_applied);
    assert!(applied >= batches);
    let lat: Vec<u64> = results.into_iter().flat_map(|r| r.lat).collect();
    CellResult {
        clients,
        size,
        pipeline,
        bps: batches as f64 / secs as f64,
        lat: (!lat.is_empty()).then(|| stats(lat)),
        usage,
        scheduler: scheduler_of(&k.ready),
        diag: stages_kv(&intro),
    }
}

fn e2e_bench(spec: &KernelSpec, secs: u64, warmup_secs: u64) {
    println!("## End-to-end IPC (`scada-bench e2e --secs {secs} --warmup-secs {warmup_secs}`)\n");
    println!(
        "Kernel: `{}` {:?}, {LOAD_SOURCES}+1 sources ({} tags), queue {}.",
        spec.bin.display(),
        spec.args,
        LOAD_SOURCES * CHANNELS + 1,
        spec.queue
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
            let r = cell(spec, clients, size, secs, warmup_secs, 1);
            let s = r.lat.as_ref().expect("latency");
            let u = &r.usage;
            println!(
                "| {clients} | {size} | {:.0} | {:.0} | {} | {} | {} | {} | {:.0} | {}→{}, {} | {} |",
                r.bps,
                r.bps * f64::from(size),
                us(s.p50),
                us(s.p99),
                us(s.p999),
                us(s.max),
                u.cpu_pct,
                u.rss_idle,
                u.rss_steady,
                u.peak,
                u.threads
            );
            let _ = std::io::stdout().flush();
        }
    }
    println!("\n### Pipelined throughput (up to 32 batches in flight per connection)\n");
    println!(
        "| clients | obs/batch | batches/s | obs/s | kernel CPU % | kernel RSS KiB (idle→steady, peak) | kernel threads |"
    );
    println!("|---:|---:|---:|---:|---:|---|---:|");
    for clients in [1u32, 8] {
        for size in [1u32, 32, 128] {
            let r = cell(spec, clients, size, secs, warmup_secs, 32);
            let u = &r.usage;
            println!(
                "| {clients} | {size} | {:.0} | {:.0} | {:.0} | {}→{}, {} | {} |",
                r.bps,
                r.bps * f64::from(size),
                u.cpu_pct,
                u.rss_idle,
                u.rss_steady,
                u.peak,
                u.threads
            );
            let _ = std::io::stdout().flush();
        }
    }
    println!();
}

// ---------------------------------------------------------------- mixed: rare vs high volume

/// `hv` high-volume sources (128-observation Deltas, 32 in flight each) saturate the kernel
/// while the rare source sends one 1-observation Delta every `interval_us`, stop-and-wait.
/// Reports the rare source's send→ACK latency and (with `--diag`) its queue wait.
fn mixed(spec: &KernelSpec, hv: u32, interval_us: u64, secs: u64, warmup_secs: u64, label: &str) {
    assert!(hv <= LOAD_SOURCES);
    let probe = RARE_SOURCE.to_string();
    let extra: Vec<&str> =
        if spec.args.iter().any(|a| a == "--diag") { vec!["--diag-probe-source", &probe] } else { vec![] };
    let k = KernelProc::start(spec, &format!("mixed-{hv}"), &extra);
    std::thread::sleep(Duration::from_millis(200));
    let rss_idle = proc_status(k.pid(), "VmRSS:");
    let warm = Duration::from_secs(warmup_secs);
    let dur = Duration::from_secs(secs);
    let hv_handles: Vec<_> = (1..=hv)
        .map(|c| {
            let sock = k.socket();
            std::thread::spawn(move || run_client(sock, c, CHANNELS, CHANNELS, warm, dur, 32, None))
        })
        .collect();
    let sock = k.socket();
    let interval = Duration::from_micros(interval_us);
    let rare = std::thread::spawn(move || run_client(sock, RARE_SOURCE, 1, 1, warm, dur, 1, Some(interval)));
    std::thread::sleep(warm);
    let _ = k.introspect_with(true);
    let usage = measure_window(k.pid(), dur, rss_idle);
    let intro = k.introspect();
    let rare = rare.join().expect("rare");
    let hv_res: Vec<ClientResult> = hv_handles.into_iter().map(|h| h.join().expect("hv")).collect();
    let hv_batches: u64 = hv_res.iter().map(|r| r.batches).sum();
    let hv_lat = stats(hv_res.into_iter().flat_map(|r| r.lat).collect());
    let c = intro.counters.unwrap_or_default();
    let rs = stats(rare.lat);
    println!(
        "kind=mixed label={label} scheduler={} hv={hv} rare_interval_us={interval_us} queue={} rare_n={} rare_p50_ns={} rare_p99_ns={} rare_p999_ns={} rare_max_ns={} hv_batches_s={:.0} hv_p50_ns={} hv_p99_ns={} hv_p999_ns={} cpu_pct={:.1} cpu_threads={} rss_steady_kib={} rss_peak_kib={} threads={} queue_full_waits={} rejected={}{}",
        scheduler_of(&k.ready),
        spec.queue,
        rs.n,
        rs.p50,
        rs.p99,
        rs.p999,
        rs.max,
        hv_batches as f64 / secs as f64,
        hv_lat.p50,
        hv_lat.p99,
        hv_lat.p999,
        usage.cpu_pct,
        usage.cpu_by_thread,
        usage.rss_steady,
        usage.peak,
        usage.threads,
        c.input_queue_full_waits,
        c.batches_rejected,
        stages_kv(&intro)
    );
}

// ---------------------------------------------------------------- saturation

fn saturation(spec: &KernelSpec, secs: u64, clients: u32) {
    let throttle_us = 200u64;
    println!("## Saturation (`scada-bench saturation --secs {secs} --clients {clients}`)\n");
    println!(
        "Kernel `{}` {:?} started with `--throttle-sequencer-us {throttle_us}` (artificial 200 µs per request in the sequencer",
        spec.bin.display(),
        spec.args
    );
    println!(
        "thread) so that {clients} pipelined producers (32 in flight each, 128-observation batches) are faster than the consumer."
    );
    println!("Queue capacity {}. RSS sampled every second.\n", spec.queue);
    let k = KernelProc::start(spec, "sat", &["--throttle-sequencer-us", &throttle_us.to_string()]);
    let rss_idle = proc_status(k.pid(), "VmRSS:");
    let dur = Duration::from_secs(secs);
    let handles: Vec<_> = (1..=clients)
        .map(|c| {
            let sock = k.socket();
            std::thread::spawn(move || run_client(sock, c, CHANNELS, CHANNELS, Duration::ZERO, dur, 32, None))
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
