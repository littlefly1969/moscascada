//! `scada-kernel`: the kernel host process.
//!
//! ```text
//! scada-kernel --config FILE [--socket PATH] [--introspection-socket PATH]
//!              [--scheduler current_thread|multi_thread] [--worker-threads N]
//!              [--read-buffer-bytes N]
//!              [--diag [--diag-probe-source ID]] [--capture-inputs FILE] [--throttle-sequencer-us N] [--check-config]
//! ```
//! Prints `READY instance=<hex> socket=<path>` on stdout once it accepts connections.
//! State lives only in RAM: nothing survives the process.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use scada_core::Kernel;
use scada_runtime::capture::CaptureWriter;
use scada_runtime::diag::Diag;
use scada_runtime::sequencer::{self, Counters, SequencerContext};
use scada_runtime::server::{self, Server};
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
                 [--read-buffer-bytes N] [--capture-inputs FILE] [--diag [--diag-probe-source ID]] [--throttle-sequencer-us N] [--check-config]"
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
        return ExitCode::SUCCESS;
    }
    eprintln!("memory bound (application allocations, not RSS): {bound}");
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
}

async fn run(cfg: config::RuntimeConfig, opts: Options) -> std::io::Result<()> {
    let Options { introspection, throttle, diag, capture, fail_after } = opts;
    let cfg = Arc::new(cfg);
    let kernel_instance_id = server::new_instance_id()?;
    let counters = Arc::new(Counters::default());
    let kernel = Kernel::new(Arc::new(cfg.core.clone()));
    let (tx, rx) = scada_runtime::ingress::channel(&cfg);
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
        },
        capture,
    )?;
    let (listener, _guard) = server::bind_socket(&cfg.socket_path, 0o660, cfg.socket_group_gid)?;
    let srv = Arc::new(Server { config: cfg.clone(), kernel_instance_id, counters, requests: tx, diag });
    let intro = match &introspection {
        Some(p) => Some(server::bind_socket(p, 0o600, None)?),
        None => None,
    };
    println!(
        "READY instance={} socket={} scheduler={} ingress={}",
        hex(&kernel_instance_id),
        cfg.socket_path.display(),
        cfg.runtime.scheduler,
        cfg.limits.ingress
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
    tokio::select! {
        _ = server::serve(srv.clone(), listener) => {}
        _ = intro_task => {}
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
