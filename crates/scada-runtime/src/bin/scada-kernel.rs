//! `scada-kernel`: the kernel host process.
//!
//! ```text
//! scada-kernel --config FILE [--socket PATH] [--introspection-socket PATH]
//!              [--throttle-sequencer-us N] [--check-config]
//! ```
//! Prints `READY instance=<hex> socket=<path>` on stdout once it accepts connections.
//! State lives only in RAM: nothing survives the process.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use scada_core::Kernel;
use scada_runtime::config;
use scada_runtime::sequencer::{self, Counters, SequencerContext};
use scada_runtime::server::{self, Server, SocketGuard};
use tokio::sync::mpsc;

struct Args {
    config: PathBuf,
    socket: Option<PathBuf>,
    introspection: Option<PathBuf>,
    throttle_us: u64,
    check_only: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut a = Args { config: PathBuf::new(), socket: None, introspection: None, throttle_us: 0, check_only: false };
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
                 [--throttle-sequencer-us N] [--check-config]"
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
    if args.check_only {
        println!("config ok: fingerprint={}", hex(&cfg.core.fingerprint()));
        return ExitCode::SUCCESS;
    }
    let rt = match tokio::runtime::Builder::new_current_thread().enable_io().enable_time().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: tokio runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match rt.block_on(run(cfg, args.introspection, Duration::from_micros(args.throttle_us))) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}

async fn run(cfg: config::RuntimeConfig, introspection: Option<PathBuf>, throttle: Duration) -> std::io::Result<()> {
    let cfg = Arc::new(cfg);
    let kernel_instance_id = server::new_instance_id()?;
    let counters = Arc::new(Counters::default());
    let kernel = Kernel::new(Arc::new(cfg.core.clone()));
    let (tx, rx) = mpsc::channel(cfg.limits.input_queue_capacity as usize);
    let (seq_thread, mut seq_stopped) = sequencer::spawn(
        kernel,
        rx,
        SequencerContext {
            kernel_instance_id,
            counters: counters.clone(),
            input_queue_capacity: cfg.limits.input_queue_capacity,
            throttle,
        },
    )?;
    let listener = server::bind_socket(&cfg.socket_path, 0o660, cfg.socket_group_gid)?;
    let _guard = SocketGuard(cfg.socket_path.clone());
    let srv = Arc::new(Server { config: cfg.clone(), kernel_instance_id, counters, requests: tx });
    let intro = match &introspection {
        Some(p) => Some((server::bind_socket(p, 0o600, None)?, SocketGuard(p.clone()))),
        None => None,
    };
    println!("READY instance={} socket={}", hex(&kernel_instance_id), cfg.socket_path.display());
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
