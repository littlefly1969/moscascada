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
use scada_journal::progress::{JournalShared, JournalState};
use scada_journal::store::{Store, StoreOptions};
use scada_journal::writer::WriterOptions;
use scada_journal::{Durability, live};
use scada_runtime::alarms::{self, AlarmRuntime};
use scada_runtime::capture::CaptureWriter;
use scada_runtime::diag::Diag;
use scada_runtime::history::{self, HistoryInit, HistoryService};
use scada_runtime::persist::{ActionCache, CachedAction, JournalWaker, Persist, PersistInit, SourceTrack};
use scada_runtime::sequencer::{self, Counters, SequencerContext};
use scada_runtime::server::{self, Server};
use scada_runtime::supervision::{self, Supervisor, WatchdogShared};
use scada_runtime::time_authority::{TimeAuthority, TimeShared, spawn_probe};
use scada_runtime::{budget, config};
use std::sync::atomic::{AtomicBool, Ordering};

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
    /// VS-006 TEST-ONLY flags (fault injection, slow/stalled/full disk, injected clock).
    test: TestFlags,
}

/// VS-006 TEST-ONLY options. None of them is accepted from the configuration file.
#[derive(Default)]
struct TestFlags {
    fault: Option<String>,
    journal_delay_us: u64,
    journal_stall_file: Option<PathBuf>,
    journal_max_pages: Option<u32>,
    journal_eio_at_commit: Option<u64>,
    clock_offset_file: Option<PathBuf>,
    allow_non_durable: bool,
}

impl TestFlags {
    fn any(&self) -> bool {
        self.fault.is_some()
            || self.journal_delay_us > 0
            || self.journal_stall_file.is_some()
            || self.journal_max_pages.is_some()
            || self.journal_eio_at_commit.is_some()
            || self.clock_offset_file.is_some()
    }
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
        test: TestFlags::default(),
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
            "--test-fault" => a.test.fault = Some(val("--test-fault")?),
            "--test-journal-delay-us" => {
                a.test.journal_delay_us =
                    val("--test-journal-delay-us")?.parse().map_err(|e| format!("--test-journal-delay-us: {e}"))?
            }
            "--test-journal-stall-file" => a.test.journal_stall_file = Some(val("--test-journal-stall-file")?.into()),
            "--test-journal-max-pages" => {
                a.test.journal_max_pages = Some(
                    val("--test-journal-max-pages")?.parse().map_err(|e| format!("--test-journal-max-pages: {e}"))?,
                )
            }
            "--test-journal-eio-at-commit" => {
                a.test.journal_eio_at_commit = Some(
                    val("--test-journal-eio-at-commit")?
                        .parse()
                        .map_err(|e| format!("--test-journal-eio-at-commit: {e}"))?,
                )
            }
            "--test-clock-offset-file" => a.test.clock_offset_file = Some(val("--test-clock-offset-file")?.into()),
            "--test-allow-non-durable" => a.test.allow_non_durable = true,
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
        // VS-005 §10: MiB, and the share of this host's physical RAM (informational only: the
        // physical RAM is never assumed to be free for the kernel).
        let mib = |b: u128| b as f64 / f64::from(1u32 << 20);
        let phys = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| {
            m.lines().find(|l| l.starts_with("MemTotal:"))?.split_whitespace().nth(1)?.parse::<u64>().ok()
        });
        println!(
            "memory bound: {:.2} MiB application allocations (not RSS){}{}",
            mib(bound.total),
            cfg.runtime.memory_budget_bytes.map_or(String::new(), |l| format!(
                ", budget {:.2} MiB ({:.1} % used)",
                mib(u128::from(l)),
                bound.total as f64 / l as f64 * 100.0
            )),
            phys.map_or(String::new(), |kib| format!(
                ", {:.3} % of this host's physical RAM ({:.0} MiB)",
                bound.total as f64 / (kib as f64 * 1024.0) * 100.0,
                kib as f64 / 1024.0
            ))
        );
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
                     alarm_core_bytes={} ({:.2} MiB) control_plane_bytes={} ({:.2} MiB) sizes({}) durability=NONE (volatile until VS-006)",
                    t.len(),
                    t.max_rules(),
                    fanout,
                    worst.map_or(0, |s| s.0),
                    t.outbox_capacity(),
                    bound.alarm_core,
                    mib(bound.alarm_core),
                    bound.alarms,
                    mib(bound.alarms),
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
        // VS-006 (brief §8): RAM of the persistence path, itemized, and the share of a
        // 4 GiB "Micro" host. Estimates of application allocations, not RSS.
        const MICRO: f64 = 4.0 * 1024.0 * 1024.0 * 1024.0;
        match &cfg.persistence {
            Some(p) => {
                let pb = &bound.persistence_items;
                let ring = cfg
                    .core
                    .alarm_table()
                    .map_or(0u128, |t| u128::from(t.outbox_capacity()) * scada_core::layout_sizes()[1].1 as u128);
                println!(
                    "persistence: path={} durability={} queue_operations={} queue_bytes={} commit_max_operations={} \
                     commit_max_delay_ms={} checkpoint_every_operations={} checkpoints_keep={} compact_replay_payloads={} startup_check={:?} \
                     wal_max_bytes={} min_disk_free_bytes={} max_db_bytes={} history_socket={}",
                    p.path.display(),
                    if p.durable { "full (synchronous=FULL)" } else { "normal (NOT DURABLE, benchmark only)" },
                    p.journal_queue_operations,
                    p.journal_queue_bytes,
                    p.commit_max_operations,
                    p.commit_max_delay_ms,
                    p.checkpoint_every_operations,
                    p.checkpoints_keep,
                    p.compact_replay_payloads,
                    p.startup_check,
                    p.wal_max_bytes,
                    p.min_disk_free_bytes,
                    p.max_db_bytes.map_or("none".to_owned(), |b| b.to_string()),
                    p.history_socket_path.as_ref().map_or("off".to_owned(), |x| x.display().to_string()),
                );
                let line = |name: &str, b: u128| {
                    println!(
                        "persistence memory: {name:<18} {b:>12} bytes {:>9.2} MiB {:>7.3} % of 4 GiB Micro",
                        mib(b),
                        b as f64 / MICRO * 100.0
                    )
                };
                line("event_ring", ring);
                line("staging", pb.staging);
                line("journal_queue", pb.journal_queue);
                line("checkpoint", pb.checkpoint);
                line("history_sessions", pb.history_sessions);
                line("sqlite_cache", pb.sqlite_cache);
                line("action_cache", pb.action_cache);
                line("persistence_total", bound.persistence);
                line("kernel_total", bound.total);
                println!(
                    "persistence memory: event_ring is part of alarm_core (kernel); persistence_total excludes it; \
                     kernel_total includes both. SQLite page cache and WAL index are estimates; the OS page cache is not counted"
                );
            }
            None => println!("persistence: disabled (no [persistence] section): VS-005 behaviour, nothing is durable"),
        }
        return ExitCode::SUCCESS;
    }
    // VS-006: TEST-ONLY flags and durability rules.
    if args.test.any() && cfg.persistence.is_none() {
        eprintln!("error: --test-fault / --test-journal-* / --test-clock-offset-file require [persistence]");
        return ExitCode::from(2);
    }
    if let Some(p) = &cfg.persistence {
        if args.simulated_sink {
            eprintln!(
                "error: --test-simulated-durable-sink is incompatible with [persistence] (the journal is the durable consumer)"
            );
            return ExitCode::from(2);
        }
        if !p.durable && !args.test.allow_non_durable {
            eprintln!(
                "error: [persistence] durability = \"normal\" is NOT power-loss durable and is accepted only with --test-allow-non-durable (benchmarks)"
            );
            return ExitCode::from(2);
        }
        if !p.durable {
            eprintln!("WARNING: journal durability=normal: NOT DURABLE across power loss (benchmark only)");
        }
    }
    if let Some(f) = &args.test.fault {
        if let Err(e) = scada_journal::fault::arm(f) {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
        eprintln!("WARNING: TEST-ONLY fault injection armed: {f}");
    }
    eprintln!("memory bound (application allocations, not RSS): {bound}");
    if cfg.core.alarm_table().is_some() && cfg.persistence.is_none() {
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
    // Only the historian uses spawn_blocking (one thread per allowed history connection).
    .max_blocking_threads(cfg.persistence.as_ref().map_or(1, |p| p.max_history_connections.max(1) as usize))
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
        test: args.test,
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
    test: TestFlags,
}

/// VS-006: the journal side of a running kernel.
struct Journal {
    shared: Arc<JournalShared>,
    public: Arc<scada_runtime::persist::PersistPublic>,
    history: Option<Arc<HistoryService>>,
    info: server::JournalInfo,
    ready: String,
    writer: std::thread::JoinHandle<()>,
}

fn jerr(what: &str, e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!("persistence: {what}: {e}"))
}

/// Open the store, recover the kernel, register the boot and start the writer, the time
/// authority and (optionally) the historian (ADR 0013 D7, ADR 0014). Fail-closed: any
/// inconsistency stops the startup before READY.
fn open_journal(
    cfg: &config::RuntimeConfig,
    p: &config::PersistenceSettings,
    kernel_instance_id: [u8; 16],
    waker: scada_runtime::ingress::TickWaker,
    test: &TestFlags,
) -> std::io::Result<(Kernel, Persist, Journal)> {
    let o = StoreOptions {
        path: p.path.clone(),
        durability: if p.durable { Durability::Full } else { Durability::NormalBenchmarkOnly },
        startup_check: match p.startup_check {
            config::StartupCheck::None => scada_journal::store::StartupCheck::None,
            config::StartupCheck::Quick => scada_journal::store::StartupCheck::Quick,
            config::StartupCheck::Full => scada_journal::store::StartupCheck::Full,
        },
        cache_kib: p.sqlite_cache_kib,
        test_max_pages: test.journal_max_pages,
    };
    let mut store = Store::open(&o, cfg.core.fingerprint()).map_err(|e| jerr("open", e))?;
    let tl = store.info.timeline_no;
    let rec = scada_journal::recovery::recover(&store.conn, tl, Arc::new(cfg.core.clone()))
        .map_err(|e| jerr("recovery", e))?;
    let mut actions = ActionCache::new(p.operator_actions_cache as usize);
    let stored = scada_journal::recovery::load_actions(&store.conn, tl, p.operator_actions_cache)
        .map_err(|e| jerr("operator actions", e))?;
    for a in stored.into_iter().rev() {
        actions.insert(
            a.action_id,
            CachedAction {
                alarm_id: a.alarm_id,
                token: a.requested_token,
                operation_seq: a.operation_seq,
                event_seq: a.event_seq,
            },
        );
    }
    let ns = cfg.core.sources().len();
    let mut track = vec![SourceTrack { fenced: None, live: live::NEVER_CONNECTED, history: 0 }; ns];
    for r in scada_journal::reader::source_recovery(&store.conn, tl).map_err(|e| jerr("source recovery", e))? {
        if let Some(i) = cfg.core.source_index(scada_core::SourceId(r.source_id)) {
            track[i] = SourceTrack { fenced: None, live: r.live, history: r.history };
        }
    }
    let report = rec.report.clone();
    let os_boot = scada_journal::os_boot_id();
    let boot = store
        .begin_boot(
            kernel_instance_id,
            &os_boot,
            report.durable_op + 1,
            report.checkpoint_op,
            report.replayed_operations,
            u64::try_from(report.duration.as_micros()).unwrap_or(u64::MAX),
        )
        .map_err(|e| jerr("boot record", e))?;
    let info = store.info.clone();
    let shared = Arc::new(JournalShared::new(report.durable_op, report.durable_event));
    let (jtx, jrx) = std::sync::mpsc::sync_channel(p.journal_queue_operations as usize + 16);
    let wake_queued = Arc::new(AtomicBool::new(false));
    let jw = JournalWaker { waker, queued: wake_queued.clone() };
    let writer = scada_journal::writer::spawn(
        store,
        shared.clone(),
        jrx,
        WriterOptions {
            commit_max_operations: p.commit_max_operations as usize,
            commit_max_delay: Duration::from_millis(p.commit_max_delay_ms),
            checkpoints_keep: p.checkpoints_keep,
            compact_replay_payloads: p.compact_replay_payloads,
            wal_max_bytes: p.wal_max_bytes,
            durability: o.durability,
            boot_generation: boot,
            test_delay: Duration::from_micros(test.journal_delay_us),
            test_stall_file: test.journal_stall_file.clone(),
            test_eio_at_commit: test.journal_eio_at_commit,
        },
        Box::new(move || jw.wake()),
    )?;
    let time_shared = Arc::new(TimeShared::new(os_boot));
    time_shared.boot_generation.store(boot, Ordering::Relaxed);
    let ta =
        TimeAuthority::new(&cfg.time, info.last_logical_time_ns, time_shared.clone(), test.clock_offset_file.clone());
    spawn_probe(&cfg.time, time_shared.clone());
    let persist = Persist::new(
        &rec.kernel,
        PersistInit {
            tx: jtx,
            shared: shared.clone(),
            time: ta,
            settings: p.clone(),
            boot_generation: boot,
            track,
            actions,
            last_checkpoint_op: report.checkpoint_op.unwrap_or(0),
            wake_queued,
            timeline_id: info.timeline_id,
            recovered_op: report.durable_op,
        },
    );
    let public = persist.public.clone();
    let history = match &p.history_socket_path {
        Some(_) => Some(Arc::new(
            HistoryService::new(HistoryInit {
                timeline_no: tl,
                timeline_id: info.timeline_id,
                settings: p.clone(),
                shared: shared.clone(),
                public: public.clone(),
                time: time_shared,
                report: report.clone(),
                boot_generation: boot,
                source_ids: cfg.core.sources().iter().map(|s| s.source_id.0).collect(),
            })
            .map_err(|e| jerr("history", e))?,
        )),
        None => None,
    };
    let ready = format!(
        " persistence={} timeline={} boot={} durable_op={} durable_event={} checkpoint_op={} replayed_ops={} recovery_ms={:.1}",
        if p.durable { "full" } else { "normal_NOT_DURABLE" },
        hex(&info.timeline_id),
        boot,
        report.durable_op,
        report.durable_event,
        report.checkpoint_op.map_or("none".to_owned(), |c| c.to_string()),
        report.replayed_operations,
        report.duration.as_secs_f64() * 1e3,
    );
    eprintln!(
        "persistence: store={} timeline={} boot={} recovered durable_op={} (checkpoint {:?}, rejected {:?}, {} operations / {} events replayed in {:?})",
        p.path.display(),
        hex(&info.timeline_id),
        boot,
        report.durable_op,
        report.checkpoint_op,
        report.checkpoints_rejected,
        report.replayed_operations,
        report.replayed_events,
        report.duration
    );
    let journal = Journal {
        shared,
        public,
        history,
        info: server::JournalInfo { timeline_id: info.timeline_id, durable: p.durable },
        ready,
        writer,
    };
    Ok((rec.kernel, persist, journal))
}

async fn run(cfg: config::RuntimeConfig, opts: Options) -> std::io::Result<()> {
    let Options { introspection, throttle, diag, capture, fail_after, simulated_sink, memory_bound, test } = opts;
    let cfg = Arc::new(cfg);
    let kernel_instance_id = server::new_instance_id()?;
    let counters = Arc::new(Counters::default());
    let (tx, rx) = scada_runtime::ingress::channel(&cfg);
    // VS-006: with [persistence] the kernel is the recovered one (fail-closed); without it
    // nothing changes (J10).
    let (kernel, persist, journal) = match &cfg.persistence {
        Some(p) => {
            let (k, ps, j) = open_journal(&cfg, p, kernel_instance_id, tx.tick_waker(), &test)?;
            (k, Some(ps), Some(j))
        }
        None => (Kernel::new(Arc::new(cfg.core.clone())), None, None),
    };
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
        persist,
    )?;
    if let (Some(w), Some(waker)) = (watchdog, waker) {
        // Detached: it stops by itself once the ingress is closed.
        drop(supervision::spawn_watchdog(w, waker, counters.clone())?);
    }
    let (listener, _guard) = server::bind_socket(&cfg.socket_path, 0o660, cfg.socket_group_gid)?;
    let srv = Arc::new(Server {
        config: cfg.clone(),
        kernel_instance_id,
        counters,
        requests: tx,
        diag,
        journal: journal.as_ref().map(|j| j.info),
    });
    let intro = match &introspection {
        Some(p) => Some(server::bind_socket(p, 0o600, None)?),
        None => None,
    };
    // VS-005: alarm control socket (peer-uid allowlist on top of the file mode).
    let control = match cfg.alarms.as_ref().and_then(|a| a.control_socket_path.as_ref().map(|p| (a, p))) {
        Some((a, p)) => Some((server::bind_socket(p, 0o660, cfg.socket_group_gid)?, a.clone())),
        None => None,
    };
    // VS-006: historian socket (read-only connections, peer-uid allowlist).
    let history = match (
        journal.as_ref().and_then(|j| j.history.clone()),
        cfg.persistence.as_ref().and_then(|p| p.history_socket_path.as_ref()),
    ) {
        (Some(h), Some(path)) => Some((server::bind_socket(path, 0o660, cfg.socket_group_gid)?, h)),
        _ => None,
    };
    let alarm_durability = match &journal {
        Some(j) if j.info.durable => "journal",
        Some(_) => "journal_not_durable",
        None => "none",
    };
    println!(
        "READY instance={} socket={} scheduler={} ingress={} supervision={}{}{}",
        hex(&kernel_instance_id),
        cfg.socket_path.display(),
        cfg.runtime.scheduler,
        cfg.limits.ingress,
        cfg.supervision.as_ref().map_or("off".to_owned(), |s| format!("{}", s.sources.len())),
        // VS-005: appended only when alarms are configured (READY line unchanged otherwise).
        cfg.core.alarm_table().map_or(String::new(), |t| format!(
            " alarms={} outbox={} durability={alarm_durability}{}",
            t.len(),
            t.outbox_capacity(),
            if simulated_sink { " simulated_sink=test_only" } else { "" }
        )),
        // VS-006: appended only with [persistence].
        journal.as_ref().map_or("", |j| j.ready.as_str())
    );
    let _ = std::io::stdout().flush();
    if let Some(j) = &journal {
        j.public.recovering.store(false, Ordering::Release);
    }

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
    let history_task = async {
        match history {
            Some(((l, _g), h)) => {
                let ft = Duration::from_millis(cfg.limits.frame_timeout_ms);
                let wt = Duration::from_millis(cfg.limits.ack_write_timeout_ms);
                history::serve_history(h, l, ft, wt).await
            }
            None => std::future::pending().await,
        }
    };
    let what = if journal.is_some() {
        "the journal is drained, undrained operations were never acknowledged as durable"
    } else {
        "volatile state is discarded"
    };
    tokio::select! {
        _ = server::serve(srv.clone(), listener) => {}
        _ = intro_task => {}
        _ = control_task => {}
        _ = history_task => {}
        _ = term.recv() => eprintln!("SIGTERM: shutting down ({what})"),
        _ = int.recv() => eprintln!("SIGINT: shutting down ({what})"),
        _ = &mut seq_stopped => {
            return Err(std::io::Error::other("sequencer thread stopped unexpectedly; exiting"));
        }
    }
    drop(srv);
    if let Some(j) = journal {
        // Bounded drain: everything applied so far gets its commit chance (10 s). Operations
        // applied later were never acknowledged as durable (J1, J12).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let applied = j.public.applied_op.load(Ordering::Acquire);
            let durable = j.shared.durable_op.load(Ordering::Acquire);
            let st = j.shared.state();
            if durable >= applied || matches!(st, JournalState::Fault) || std::time::Instant::now() >= deadline {
                eprintln!("persistence: shutdown applied_op={applied} durable_op={durable} state={st:?}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        j.shared.shutdown.store(true, Ordering::Release);
        drop(j.writer);
    }
    // The sequencer stops when the last request sender is dropped; sessions still running
    // on the runtime are dropped with it.
    drop(seq_thread);
    Ok(())
}
