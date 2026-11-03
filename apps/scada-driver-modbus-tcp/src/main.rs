//! `scada-driver-modbus-tcp` — read-only Modbus TCP driver process (VS-007, ADR 0015).
//!
//! ```text
//! scada-driver-modbus-tcp --config FILE [--check-config] [--kernel-config KERNEL.toml]
//!                         [--status-interval-ms N] [--run-for-ms N]
//! ```
//!
//! `--check-config` validates the configuration and the catalog (and, with
//! `--kernel-config`, the mapping against the kernel's sources) **without opening any
//! socket**, prints the read plan of every device and exits (0 = valid, 2 = invalid).
//!
//! Output: `READY ...` once the workers run, then periodically one `STATUS ...` line per
//! device and `TRANSITION ...` lines for the session state changes (stdout); errors on
//! stderr. SIGTERM/SIGINT terminate the process immediately: the kernel sees the links
//! close (`SourceDisconnected`, tags `Uncertain(SourceUnavailable)`); the next start is a
//! new epoch and begins with a FullState.
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use scada_driver_modbus_tcp::config::{self, DriverConfig};
use scada_driver_modbus_tcp::device::Worker;
use scada_driver_modbus_tcp::status;

struct Args {
    config: PathBuf,
    check_only: bool,
    kernel_config: Option<PathBuf>,
    status_interval_ms: Option<u64>,
    run_for_ms: Option<u64>,
}

const USAGE: &str = "usage: scada-driver-modbus-tcp --config FILE [--check-config] [--kernel-config KERNEL.toml] \
                     [--status-interval-ms N] [--run-for-ms N]";

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        config: PathBuf::new(),
        check_only: false,
        kernel_config: None,
        status_interval_ms: None,
        run_for_ms: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = |n: &str| it.next().ok_or_else(|| format!("{n} needs a value"));
        match arg.as_str() {
            "--config" => a.config = val("--config")?.into(),
            "--check-config" => a.check_only = true,
            "--kernel-config" => a.kernel_config = Some(val("--kernel-config")?.into()),
            "--status-interval-ms" => {
                a.status_interval_ms =
                    Some(val("--status-interval-ms")?.parse().map_err(|e| format!("--status-interval-ms: {e}"))?)
            }
            "--run-for-ms" => {
                a.run_for_ms = Some(val("--run-for-ms")?.parse().map_err(|e| format!("--run-for-ms: {e}"))?)
            }
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if a.config.as_os_str().is_empty() {
        return Err("--config is required".into());
    }
    Ok(a)
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("scada-driver-modbus-tcp: {e}");
            }
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let cfg = match config::load(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("CONFIG INVALID: {e}");
            std::process::exit(2);
        }
    };
    if let Some(k) = &args.kernel_config {
        let text = match std::fs::read_to_string(k) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("CONFIG INVALID: {}: {e}", k.display());
                std::process::exit(2);
            }
        };
        match config::check_against_kernel(&cfg, &text) {
            Ok(w) => w.iter().for_each(|w| eprintln!("WARNING: {w}")),
            Err(e) => {
                eprintln!("CONFIG INVALID: {e}");
                std::process::exit(2);
            }
        }
    }
    if args.check_only {
        print_plan(&cfg);
        println!("CONFIG OK devices={} profiles={}", cfg.devices.len(), cfg.catalog.profiles.len());
        return;
    }
    run(cfg, &args);
}

fn print_plan(cfg: &DriverConfig) {
    for d in &cfg.devices {
        println!(
            "DEVICE {} source={} endpoint={}:{} unit={} profile={} v{} channels={} requests={} items={} scan_ms={}",
            d.name,
            d.source_id,
            d.host,
            d.port,
            d.unit_id,
            d.profile_id,
            d.profile_version,
            d.channels.len(),
            d.plan.requests.len(),
            d.plan.items(),
            d.scan_interval.as_millis()
        );
        for r in &d.plan.requests {
            println!(
                "  READ fc={:02} {} address={} quantity={}",
                r.area().function_code(),
                r.area(),
                r.address(),
                r.quantity()
            );
        }
    }
}

fn run(cfg: DriverConfig, args: &Args) {
    let stop = Arc::new(AtomicBool::new(false));
    let mut statuses = Vec::new();
    let mut handles = Vec::new();
    for d in &cfg.devices {
        let epoch = match scada_driver_sdk::epoch::new_epoch() {
            Ok(e) => e,
            Err(e) => {
                eprintln!("FATAL: no random source for source_epoch: {e}");
                std::process::exit(1);
            }
        };
        let pcfg = cfg.publisher_config(d);
        let (w, st) = match Worker::new(d.clone(), pcfg, epoch, cfg.drain_timeout, stop.clone()) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("FATAL: device {}: {e}", d.name);
                std::process::exit(2);
            }
        };
        statuses.push(st);
        let name = format!("dev-{}", d.name);
        match std::thread::Builder::new().name(name).spawn(move || w.run()) {
            Ok(h) => handles.push(h),
            Err(e) => {
                eprintln!("FATAL: thread: {e}");
                std::process::exit(1);
            }
        }
    }
    println!("READY driver={} devices={} pid={}", cfg.driver_id, cfg.devices.len(), std::process::id());
    let interval = args.status_interval_ms.map_or(cfg.status_interval, Duration::from_millis);
    let deadline = args.run_for_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
    loop {
        let wait = deadline.map_or(interval, |d| interval.min(d.saturating_duration_since(Instant::now())));
        std::thread::sleep(wait);
        let done = deadline.is_some_and(|d| Instant::now() >= d);
        if done {
            stop.store(true, Ordering::Relaxed);
            for h in handles.drain(..) {
                let _ = h.join();
            }
        }
        report(&cfg, &statuses);
        if done {
            println!("STOPPED");
            return;
        }
    }
}

fn report(cfg: &DriverConfig, statuses: &[Arc<std::sync::Mutex<scada_driver_modbus_tcp::device::DeviceStatus>>]) {
    let mut snaps = Vec::with_capacity(statuses.len());
    for s in statuses {
        let mut g = s.lock().unwrap_or_else(|e| e.into_inner());
        let ts = std::mem::take(&mut g.transitions);
        let snap = g.clone();
        drop(g);
        for (from, to, reason) in ts {
            println!("TRANSITION device={} from={from} to={to} reason=\"{}\"", snap.name, reason.replace('"', "'"));
        }
        println!("{}", status::status_line(&snap));
        snaps.push(snap);
    }
    if let Some(h) = &cfg.health_file
        && let Err(e) = status::write_atomic(h, &status::health_json(&cfg.driver_id, &snaps))
    {
        eprintln!("health file {}: {e}", h.display());
    }
}
