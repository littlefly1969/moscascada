//! `scada-kernel`: the kernel host process.
//!
//! ```text
//! scada-kernel --config FILE [--socket PATH] [--introspection-socket PATH]
//!              [--scheduler current_thread|multi_thread] [--worker-threads N]
//!              [--read-buffer-bytes N]
//!              [--diag [--diag-probe-source ID]] [--capture-inputs FILE] [--throttle-sequencer-us N] [--check-config]
//! ```
//! Prints `READY instance=<hex> socket=<path>` on stdout once it accepts connections.
//! State lives only in RAM: nothing survives the process. VS-005: alarm states and the
//! event outbox are volatile too (durability arrives with the VS-006 journal);
//! `--test-simulated-durable-sink` enables a TEST-ONLY watermark command on the alarm
//! control socket.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use scada_core::Kernel;
use scada_runtime::alarms::{self, AlarmRuntime};
use scada_runtime::capture::CaptureWriter;
use scada_runtime::diag::Diag;
use scada_runtime::sequencer::{self, Counters, SequencerContext};
use scada_runtime::server::{self, Server};
use scada_runtime::supervision::{self, Supervisor, WatchdogShared};
use scada_runtime::{budget, config};

struct Args {
    config: PathBuf,
    socket: Option<PathBuf>,
    introspection: Option<PathBuf>,
    throttle_us: u64,
    check_only: bool,
    scheduler: Option<String>,
    worker_threads: Option<u32>,
    diag: bool,
    probe_source: Option<u32>,
    read_buffer: Option<u32>,
    coalesce: Option<u32>,
    capture: Option<PathBuf>,
    fail_after: Option<u64>,
    simulated_sink: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        config: PathBuf::new(),
        socket: None,
        introspection: None,
        throttle_us: 0,
        check_only: false,
        scheduler: None,
        worker_threads: None,
        diag: false,
        probe_source: None,
        read_buffer: None,
        coalesce: None,
        capture: None,
        fail_after: None,
        simulated_sink: false,
    };
    let mut have_config = false;
    while let Some(arg) = it.next() {
        let mut val = |name: &str| it.next().ok_or_else(|| format!("{name} needs a value"));
        match arg.as_str() {
            "--config" => {
                a.config = val("--config")?.into();
                have_config = true;
            }
            "--socket" => a.socket = Some(val("--socket")?.into()),
            "--introspection-socket" => a.introspection = Some(val("--introspection-socket")?.into()),
            "--throttle-sequencer-us" => {
                a.throttle_us =
                    val("--throttle-sequencer-us")?.parse().map_err(|e| format!("--throttle-sequencer-us: {e}"))?
            }
            "--check-config" => a.check_only = true,
            "--test-simulated-durable-sink" => a.simulated_sink = true,
            "--diag" => a.diag = true,
            "--capture-inputs" => a.capture = Some(val("--capture-inputs")?.into()),
            "--test-fail-sequencer-after" => {
                a.fail_after = Some(
                    val("--test-fail-sequencer-after")?
                        .parse()
                        .map_err(|e| format!("--test-fail-sequencer-after: {e}"))?,
                )
            }
            "--read-buffer-bytes" => {
                a.read_buffer =
                    Some(val("--read-buffer-bytes")?.parse().map_err(|e| format!("--read-buffer-bytes: {e}"))?)
            }
            "--ack-coalesce-max" => {
                a.coalesce = Some(val("--ack-coalesce-max")?.parse().map_err(|e| format!("--ack-coalesce-max: {e}"))?)
            }
            "--diag-probe-source" => {
                a.probe_source =
                    Some(val("--diag-probe-source")?.parse().map_err(|e| format!("--diag-probe-source: {e}"))?)
            }
            "--scheduler" => a.scheduler = Some(val("--scheduler")?),
            "--worker-threads" => {
                a.worker_threads = Some(val("--worker-threads")?.parse().map_err(|e| format!("--worker-threads: {e}"))?)
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if !have_config {
        return Err("--config is required".into());
    }
    Ok(a)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("error: {e}");
            }
            eprintln!(
                "usage: scada-kernel --config FILE [--socket PATH] [--introspection-socket PATH] \
                 [--scheduler current_thread|multi_thread] [--worker-threads N] \
                 [--read-buffer-bytes N] [--capture-inputs FILE] [--diag [--diag-probe-source ID]] [--throttle-sequencer-us N] [--test-simulated-durable-sink] [--check-config]"
            );
            return ExitCode::from(2);
        }
    };
    let mut cfg = match config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(3);
        }
    };
    if let Some(s) = args.socket {
        cfg.socket_path = s;
    }
    // Command-line override of [runtime] (benchmarks). Same validation as the file.
    if args.scheduler.is_some() || args.worker_threads.is_some() {
        let name = args.scheduler.as_deref().unwrap_or(match cfg.runtime.scheduler {
            config::Scheduler::CurrentThread => "current_thread",
            config::Scheduler::MultiThread { .. } => "multi_thread",
        });
        match config::Scheduler::parse(name, args.worker_threads) {
            Ok(s) => cfg.runtime.scheduler = s,
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::from(2);
            }
        }
    }
    if let Some(n) = args.read_buffer {
        if n > 1 << 20 {
            eprintln!("error: --read-buffer-bytes {n} outside 0..=1048576");
            return ExitCode::from(2);
        }
        cfg.limits.read_buffer_bytes = n;
    }
    if let Some(n) = args.coalesce {
        if !(1..=1024).contains(&n) {
            eprintln!("error: --ack-coalesce-max {n} outside 1..=1024");
            return ExitCode::from(2);
        }
        cfg.limits.ack_coalesce_max = n;
    }
    // E1: configured worst-case bound of the kernel's own allocations.
    let bound = budget::compute(&cfg, args.introspection.is_some(), args.diag);
    if let Err(e) = budget::check(&cfg, &bound) {
        eprintln!("error: {e}");
        return ExitCode::from(3);
    }
    if args.check_only {
        println!(
            "config ok: fingerprint={} scheduler={} ingress={} memory_bound_bytes={}",
            hex(&cfg.core.fingerprint()),
            cfg.runtime.scheduler,
            cfg.limits.ingress,
            bound.total
        );
        println!("memory bound: {bound}");
        // VS-004: supervision summary (diagnostic output only; nothing is started).
        match &cfg.supervision {
            Some(s) => println!("supervision: {s} memory_bound_bytes={}", bound.supervision),
            None => println!("supervision: disabled (no source sets liveness_timeout_ms / freshness_timeout_ms)"),
        }
        // VS-005: alarm capacity summary (ADR 0012 §capacity).
        match cfg.core.alarm_table() {
            Some(t) => {
                let l = scada_core::layout_sizes().map(|(n, b)| format!("{n}={b}")).join(" ");
                let (fanout, worst) = t.worst_fanout();
                println!(
                    "alarms: rules={} max_rules={} worst_fanout={} (source {}) outbox_capacity_events={} \
                     alarm_core_bytes={} control_plane_bytes={} sizes({}) durability=NONE (volatile until VS-006)",
                    t.len(),
                    t.max_rules(),
                    fanout,
                    worst.map_or(0, |s| s.0),
                    t.outbox_capacity(),
                    bound.alarm_core,
                    bound.alarms,
                    l
                );
                if let Some(a) = &cfg.alarms {
                    println!(
                        "alarms control: socket={} authorized_uids={:?} max_connections={} page_max={} high_percent={}",
                        a.control_socket_path.as_ref().map_or("off".to_owned(), |p| p.display().to_string()),
                        a.control_authorized_uids,
                        a.max_control_connections,
                        a.control_page_max,
                        a.outbox_high_percent
                    );
                }
            }
            None => println!("alarms: disabled (no [alarms] section)"),
        }
        return ExitCode::SUCCESS;
    }
    eprintln!("memory bound (application allocations, not RSS): {bound}");
    if cfg.core.alarm_table().is_some() {
        eprintln!(
            "WARNING: alarms enabled; alarm states and events are VOLATILE (not durable before VS-006). \
             Without a durable consumer the outbox slots are never released: NOT PRODUCTION-READY FOR LONG-RUN ALARM RETENTION."
        );
    }
    if args.simulated_sink {
        if cfg.alarms.as_ref().and_then(|a| a.control_socket_path.as_ref()).is_none() {
            eprintln!("error: --test-simulated-durable-sink requires [alarms] control_socket_path");
            return ExitCode::from(2);
        }
        eprintln!("WARNING: --test-simulated-durable-sink is a TEST-ONLY watermark: released events are NOT durable");
    }
    let memory_bound = u64::try_from(bound.total).unwrap_or(u64::MAX);
    let built = match cfg.runtime.scheduler {
        config::Scheduler::CurrentThread => tokio::runtime::Builder::new_current_thread(),
        config::Scheduler::MultiThread { worker_threads } => {
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(worker_threads as usize);
            b
        }
    }
    .enable_io()
    .enable_time()
    .thread_name("scada-io")
    // Nothing in the kernel uses spawn_blocking; keep the pool from growing anyway.
    .max_blocking_threads(1)
    .build();
    let rt = match built {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };
    let diag = args.diag.then(|| Arc::new(Diag::new(args.probe_source)));
    let capture = match &args.capture {
        Some(p) => match CaptureWriter::create(p) {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("error: --capture-inputs {}: {e}", p.display());
                return ExitCode::from(2);
            }
        },
        None => None,
    };
    let opts = Options {
        introspection: args.introspection,
        throttle: Duration::from_micros(args.throttle_us),
        diag,
        capture,
        fail_after: args.fail_after,
        simulated_sink: args.simulated_sink,
        memory_bound,
    };
    match rt.block_on(run(cfg, opts)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}

/// Diagnostic and test options that are not part of the configuration file.
struct Options {
    introspection: Option<PathBuf>,
    throttle: Duration,
    diag: Option<Arc<Diag>>,
    capture: Option<CaptureWriter>,
    fail_after: Option<u64>,
    simulated_sink: bool,
    memory_bound: u64,
}

async fn run(cfg: config::RuntimeConfig, opts: Options) -> std::io::Result<()> {
    let Options { introspection, throttle, diag, capture, fail_after, simulated_sink, memory_bound } = opts;
    let cfg = Arc::new(cfg);
    let kernel_instance_id = server::new_instance_id()?;
    let counters = Arc::new(Counters::default());
    let kernel = Kernel::new(Arc::new(cfg.core.clone()));
    let (tx, rx) = scada_runtime::ingress::channel(&cfg);
    // VS-004: supervisor and watchdog exist only when a source is supervised.
    let supervisor = Supervisor::new(&cfg);
    let watchdog = supervisor.as_ref().map(|s| Arc::new(WatchdogShared::new(s.poll())));
    let waker = watchdog.as_ref().map(|_| tx.tick_waker());
    // VS-005: alarm runtime only when [alarms] is configured (default off: nothing exists).
    let alarm_rt = cfg.core.alarm_table().map(|_| {
        let high = cfg.alarms.as_ref().map_or(80, |a| a.outbox_high_percent);
        AlarmRuntime::new(cfg.core.sources().len(), simulated_sink, high, memory_bound)
    });
    let (seq_thread, mut seq_stopped) = sequencer::spawn(
        kernel,
        rx,
        SequencerContext {
            kernel_instance_id,
            counters: counters.clone(),
            input_queue_capacity: cfg.limits.input_queue_capacity,
            throttle,
            diag: diag.clone(),
            scheduler: cfg.runtime.scheduler.to_string(),
            fail_after,
            watchdog: watchdog.clone(),
            alarm_page_max: cfg.alarms.as_ref().map_or(0, |a| a.control_page_max),
        },
        capture,
        supervisor,
        alarm_rt,
    )?;
    if let (Some(w), Some(waker)) = (watchdog, waker) {
        // Detached: it stops by itself once the ingress is closed.
        drop(supervision::spawn_watchdog(w, waker, counters.clone())?);
    }
    let (listener, _guard) = server::bind_socket(&cfg.socket_path, 0o660, cfg.socket_group_gid)?;
    let srv = Arc::new(Server { config: cfg.clone(), kernel_instance_id, counters, requests: tx, diag });
    let intro = match &introspection {
        Some(p) => Some(server::bind_socket(p, 0o600, None)?),
        None => None,
    };
    // VS-005: alarm control socket (peer-uid allowlist on top of the file mode).
    let control = match cfg.alarms.as_ref().and_then(|a| a.control_socket_path.as_ref().map(|p| (a, p))) {
        Some((a, p)) => Some((server::bind_socket(p, 0o660, cfg.socket_group_gid)?, a.clone())),
        None => None,
    };
    println!(
        "READY instance={} socket={} scheduler={} ingress={} supervision={}{}",
        hex(&kernel_instance_id),
        cfg.socket_path.display(),
        cfg.runtime.scheduler,
        cfg.limits.ingress,
        cfg.supervision.as_ref().map_or("off".to_owned(), |s| format!("{}", s.sources.len())),
        // VS-005: appended only when alarms are configured (READY line unchanged otherwise).
        cfg.core.alarm_table().map_or(String::new(), |t| format!(
            " alarms={} outbox={} durability=none{}",
            t.len(),
            t.outbox_capacity(),
            if simulated_sink { " simulated_sink=test_only" } else { "" }
        ))
    );
    let _ = std::io::stdout().flush();

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let intro_task = async {
        match intro {
            Some((l, _g)) => server::serve_introspection(srv.clone(), l).await,
            None => std::future::pending().await,
        }
    };
    let control_task = async {
        match control {
            Some(((l, _g), settings)) => {
                let refused = Arc::new(std::sync::atomic::AtomicU64::new(0));
                alarms::serve_control(srv.clone(), l, settings, refused).await
            }
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        _ = server::serve(srv.clone(), listener) => {}
        _ = intro_task => {}
        _ = control_task => {}
        _ = term.recv() => eprintln!("SIGTERM: shutting down (volatile state is discarded)"),
        _ = int.recv() => eprintln!("SIGINT: shutting down (volatile state is discarded)"),
        _ = &mut seq_stopped => {
            return Err(std::io::Error::other("sequencer thread stopped unexpectedly; exiting"));
        }
    }
    drop(srv);
    // The sequencer stops when the last request sender is dropped; sessions still running
    // on the runtime are dropped with it. We do not wait for them: nothing is durable.
    drop(seq_thread);
    Ok(())
}
