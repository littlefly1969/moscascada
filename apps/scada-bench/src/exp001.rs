//! EXP-001 controlled comparison (docs/experiments/exp-001-open-wal-backend.md §5): one
//! producer loop, three journal variants, identical kernel, configuration, operation
//! stream, batch rules and fsync policy:
//!
//! - `a`: VS-006 SQLite writer (`synchronous=FULL`, projections in the transaction);
//! - `b`: open-wal journal only (one record per operation, fdatasync per group commit;
//!   no historian: **not comparable as a system** with A);
//! - `c`: open-wal journal + derived SQLite projection (same schema, historian ready
//!   when the projector caught up; projection `synchronous=NORMAL`, rebuildable).
//!
//! ```text
//! scada-bench exp001 --variant a|b|c --store DIR --ops N [--sources S] [--obs M]
//!     [--events-per-op E] [--commit-max K] [--commit-delay-us D] [--checkpoint-every C]
//!     [--ring R] [--queries Q] [--read-interval-ms I] [--rebuild] [--label L] [--rep N]
//! ```
//! Output: one `kind=exp001 ...` key=value line (+ `kind=exp001-recovery`), parsed by
//! `scripts/exp001_campaign.py`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use scada_core::*;
use scada_journal::owal::projector::Projector;
use scada_journal::owal::writer::{self as owriter, OwalWriterOptions};
use scada_journal::owal::{self, OwalOptions, OwalStore};
use scada_journal::progress::{Hist, JournalShared};
use scada_journal::reader::{self, HistoryQuery};
use scada_journal::recovery::recover;
use scada_journal::store::{self, StartupCheck, Store, StoreOptions};
use scada_journal::writer::{self, WriterOptions};
use scada_journal::{
    CheckpointMsg, Durability, OpRecord, SourceTimes, TimeEnvelope, WriterMsg, boottime_ns, clock_quality, realtime_ns,
};

use crate::{arg, arg_str};

fn config(sources: u32, obs: u32, fanout: u32, ring: u64) -> Arc<ValidatedConfig> {
    let mut tags = Vec::new();
    let mut srcs = Vec::new();
    for s in 0..sources {
        let mut channels = Vec::new();
        for c in 0..obs {
            let tag = TagId(s * obs + c + 1);
            tags.push(TagSpec { tag_id: tag, name: format!("t{}", tag.0), value_type: ValueType::I64 });
            channels.push(ChannelSpec { channel_id: ChannelId(c + 1), tag_id: tag, value_type: ValueType::I64 });
        }
        srcs.push(SourceSpec {
            source_id: SourceId(s + 1),
            name: format!("s{s}"),
            channels,
            supervision: Supervision::NONE,
        });
    }
    let spec = ConfigSpec { limits: CoreLimits { max_observations_per_batch: obs.max(8) }, tags, sources: srcs };
    // E rules on the first tag of every source: every operation of a source toggles it,
    // so every operation produces E events.
    let rules: Vec<AlarmRuleSpec> = (0..sources)
        .flat_map(|s| {
            (0..fanout).map(move |i| AlarmRuleSpec {
                alarm_id: AlarmId(s * fanout + i + 1),
                name: format!("a{s}_{i}"),
                tag_id: TagId(s * obs + 1),
                condition: AlarmCondition::High { trip: 1, reset: 0 },
                severity: Severity::Major,
            })
        })
        .collect();
    let alarms = (fanout > 0).then_some(AlarmSpec {
        max_rules: (fanout * sources).max(1),
        outbox_capacity_events: ring.max(u64::from(fanout) * 2),
        rules,
    });
    Arc::new(ValidatedConfig::validate_with_alarms(spec, alarms).expect("config"))
}

fn io_write_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("write_bytes:"))?.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

fn status_kib(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with(key))?.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

/// CPU ticks (utime+stime, 1/100 s) and major/minor faults of the process.
fn proc_stat(path: &str) -> (u64, u64, u64) {
    let s = std::fs::read_to_string(path).unwrap_or_default();
    let tail = s.rsplit_once(')').map(|x| x.1).unwrap_or("");
    let f: Vec<u64> = tail.split_whitespace().map(|x| x.parse().unwrap_or(0)).collect();
    // after ')': state(0) ppid(1) ... minflt(7) cminflt(8) majflt(9) cmajflt(10) utime(11) stime(12)
    let g = |i: usize| f.get(i).copied().unwrap_or(0);
    (g(11) + g(12), g(7), g(9))
}

/// CPU seconds per thread name prefix.
fn thread_cpu() -> Vec<(String, f64)> {
    let mut out: Vec<(String, f64)> = Vec::new();
    let Ok(rd) = std::fs::read_dir("/proc/self/task") else { return out };
    for e in rd.flatten() {
        let p = e.path();
        let comm = std::fs::read_to_string(p.join("comm")).unwrap_or_default().trim().to_owned();
        let (t, _, _) = proc_stat(&p.join("stat").to_string_lossy());
        let name = if comm.starts_with("scada-journal") {
            "writer"
        } else if comm.starts_with("scada-owal-proj") {
            "projector"
        } else if comm.starts_with("exp001-reader") {
            "reader"
        } else {
            "producer"
        };
        match out.iter_mut().find(|(n, _)| n == name) {
            Some((_, v)) => *v += t as f64 / 100.0,
            None => out.push((name.to_owned(), t as f64 / 100.0)),
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn allocated(dir: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            if let Ok(m) = e.metadata() {
                if m.is_dir() {
                    n += allocated(&e.path());
                } else {
                    n += m.blocks() * 512;
                }
            }
        }
    }
    n
}

fn fsinfo(dir: &Path) -> String {
    let out = std::process::Command::new("df").arg("-T").arg(dir).output();
    out.ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).map(|l| l.split_whitespace().take(2).collect::<Vec<_>>().join(":")))
        .unwrap_or_else(|| "unknown".into())
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variant {
    A,
    B,
    C,
}

/// Concurrent historian reader (A: the journal file; C: the projection file).
fn spawn_reader(
    db: PathBuf,
    interval: Duration,
    stop: Arc<AtomicBool>,
    lat: Arc<Hist>,
    lag: Arc<Hist>,
    durable: Arc<JournalShared>,
) -> std::thread::JoinHandle<u64> {
    std::thread::Builder::new()
        .name("exp001-reader".into())
        .spawn(move || {
            let mut n = 0u64;
            let mut conn = None;
            while !stop.load(Relaxed) {
                if conn.is_none() {
                    conn = store::open_read_only(&db, 2048).ok();
                }
                if let Some(c) = &conn
                    && let Ok((tl, _)) = reader::current_timeline(c)
                {
                    let t = Instant::now();
                    let ev = reader::as_of(c, tl).map(|x| x.0).unwrap_or(0);
                    let after = ev.saturating_sub(1000);
                    let q =
                        HistoryQuery { after_event_seq: after, limit: 1000, max_scan: 100_000, ..Default::default() };
                    if let Ok(p) = reader::history(c, tl, &q) {
                        lat.record_ns(t.elapsed().as_nanos() as u64);
                        // Freshness of what the reader sees vs the durable log, in events.
                        lag.record_ns(durable.durable_event.load(Acquire).saturating_sub(p.as_of_event_seq));
                        n += 1;
                    }
                }
                std::thread::sleep(interval);
            }
            n
        })
        .expect("reader")
}

pub fn exp001_bench() {
    let variant = match arg_str("--variant").as_deref() {
        Some("a") => Variant::A,
        Some("b") => Variant::B,
        Some("c") => Variant::C,
        other => panic!("--variant a|b|c (got {other:?})"),
    };
    let dir = PathBuf::from(arg_str("--store").expect("--store DIR"));
    let ops = arg("--ops", 200_000);
    let sources = arg("--sources", 1).max(1) as u32;
    let obs = arg("--obs", 1).max(1) as u32;
    let fanout = arg("--events-per-op", 1) as u32;
    let commit_max = arg("--commit-max", 1024) as usize;
    let commit_delay = Duration::from_micros(arg("--commit-delay-us", 0));
    let checkpoint_every = arg("--checkpoint-every", 100_000);
    let ring = arg("--ring", 65_536);
    let queries = arg("--queries", 1000) as usize;
    let read_interval = arg("--read-interval-ms", 0);
    let label = arg_str("--label").unwrap_or_default();
    let rep = arg("--rep", 0);
    let rebuild = std::env::args().any(|a| a == "--rebuild");
    // Soak: paced acquisition (operations per second, 0 = saturation) and periodic samples.
    let rate = arg("--rate", 0);
    let report_every = arg("--report-every-s", 0);
    {
        use std::os::unix::fs::DirBuilderExt;
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).expect("store dir");
    }
    let cfg = config(sources, obs, fanout, ring);
    let shared;
    let (tx, rx) = sync_channel::<WriterMsg>(4096 + 16);
    let db = dir.join("journal.db");
    let wal_dir = dir.join("owal");
    let proj_db = dir.join("projection.db");
    let sqlite_o = StoreOptions {
        path: db.clone(),
        durability: Durability::Full,
        startup_check: StartupCheck::Quick,
        cache_kib: 8192,
        test_max_pages: None,
    };
    let mut proj_handle = None;
    let mut proj_shared = None;
    let (writer_handle, mut k) = match variant {
        Variant::A => {
            let mut st = Store::open(&sqlite_o, cfg.fingerprint()).expect("open");
            let rec = recover(&st.conn, st.info.timeline_no, cfg.clone()).expect("recover");
            let boot = st.begin_boot([1; 16], &scada_journal::os_boot_id(), 1, None, 0, 0).expect("boot");
            shared = Arc::new(JournalShared::new(0, 0));
            let w = writer::spawn(
                st,
                shared.clone(),
                rx,
                WriterOptions {
                    commit_max_operations: commit_max,
                    commit_max_delay: commit_delay,
                    checkpoints_keep: 2,
                    compact_replay_payloads: false,
                    wal_max_bytes: 64 << 20,
                    durability: Durability::Full,
                    boot_generation: boot,
                    test_delay: Duration::ZERO,
                    test_stall_file: None,
                    test_eio_at_commit: None,
                },
                Box::new(|| {}),
            )
            .expect("writer");
            (w, rec.kernel)
        }
        Variant::B | Variant::C => {
            let (st, wal) = OwalStore::open(&wal_dir, OwalOptions::default(), cfg.fingerprint()).expect("open owal");
            let rec = owal::recover(&st, &wal, cfg.clone()).expect("recover owal");
            shared = Arc::new(JournalShared::new(0, 0));
            let ptx = if variant == Variant::C {
                let mut pr = Projector::open(
                    &proj_db,
                    Durability::NormalBenchmarkOnly,
                    cfg.fingerprint(),
                    st.manifest.timeline_id,
                )
                .expect("projection");
                pr.catch_up(&wal, 4096).expect("catch up");
                proj_shared = Some(pr.shared.clone());
                // 64 groups in flight at most (each <= commit_max records): bounded memory.
                let (ptx, prx) = sync_channel(64);
                proj_handle = Some(pr.spawn(prx, 8192).expect("projector"));
                Some(ptx)
            } else {
                None
            };
            let w = owriter::spawn(
                st,
                wal,
                shared.clone(),
                rx,
                OwalWriterOptions {
                    commit_max_operations: commit_max,
                    commit_max_delay: commit_delay,
                    checkpoints_keep: 2,
                    boot_generation: rec.position.last_boot_generation + 1,
                    last_logical_time_ns: rec.position.last_logical_time_ns,
                    config: cfg.clone(),
                    test_eio_at_commit: None,
                    test_delay: Duration::ZERO,
                },
                owriter::ring_seed(&rec.kernel),
                ptx,
                Box::new(|| {}),
            )
            .expect("writer");
            (w, rec.kernel)
        }
    };
    // Concurrent historian reader (A and C only).
    let stop = Arc::new(AtomicBool::new(false));
    let read_lat = Arc::new(Hist::default());
    let read_lag = Arc::new(Hist::default());
    let reader_h = (read_interval > 0 && variant != Variant::B).then(|| {
        let p = if variant == Variant::A { db.clone() } else { proj_db.clone() };
        spawn_reader(
            p,
            Duration::from_millis(read_interval),
            stop.clone(),
            read_lat.clone(),
            read_lag.clone(),
            shared.clone(),
        )
    });

    let io0 = io_write_bytes();
    let (cpu0, minflt0, majflt0) = proc_stat("/proc/self/stat");
    let t0 = Instant::now();
    let mut seq = k.last_operation_sequence();
    let st_obs = AtomicU64::new(0);
    let apply = |k: &mut Kernel, op: Operation, seq: &mut u64| -> Result<(), ApplyError> {
        let s = *seq + 1;
        let before = k.outbox_status().map_or(1, |x| x.next_event_seq);
        let input = SequencedInput { operation_sequence: s, operation: op };
        let now = realtime_ns();
        k.apply_ref(&input)?;
        *seq = s;
        let after = k.outbox_status().map_or(1, |x| x.next_event_seq);
        let events: Vec<AlarmEvent> = (before..after).filter_map(|e| k.alarm_event(e)).collect();
        // Source timestamps on every batch (wire 1.2 sidecar), as the runtime keeps them.
        let source_times = if let Operation::ObservationBatch(b) = &input.operation {
            st_obs.fetch_add(b.observations.len() as u64, Relaxed);
            Some(Box::new(SourceTimes {
                acquired_at_utc_ns: Some(now - 1000),
                clock_quality: clock_quality::SYNCHRONIZED,
                clock_id: "ptp0".into(),
                observations: b.observations.iter().map(|o| (o.channel_id.0, now - 2000)).collect(),
            }))
        } else {
            None
        };
        let mut r = OpRecord {
            operation_seq: s,
            operation: input.operation,
            time: TimeEnvelope {
                sequenced_at_utc_ns: now,
                boottime_ns: boottime_ns(),
                logical_time_ns: now,
                clock_quality: clock_quality::SYNCHRONIZED,
                clock_flags: 0,
                received_at_utc_ns: Some(now - 500),
            },
            source_times,
            origins: Vec::new(),
            events,
            reconciliation: false,
            action: None,
            recovery: Vec::new(),
            alarm_updates: Vec::new(),
            anomaly: None,
            reserved_bytes: 0,
            receipt: None,
        };
        r.reserved_bytes = r.queued_bytes();
        shared.reserved_ops.fetch_add(1, Relaxed);
        shared.reserved_bytes.fetch_add(r.reserved_bytes, Relaxed);
        tx.send(WriterMsg::Op(Box::new(r))).expect("writer alive");
        if checkpoint_every > 0 && s.is_multiple_of(checkpoint_every) {
            let t0 = Instant::now();
            let cp = k.export_checkpoint();
            let event_seq_high = k.outbox_status().map_or(0, |x| x.next_event_seq - 1);
            let state_digest = k.state_digest();
            shared.checkpoint_pause.record_ns(t0.elapsed().as_nanos() as u64);
            tx.send(WriterMsg::Checkpoint(Box::new(CheckpointMsg { checkpoint: cp, state_digest, event_seq_high })))
                .expect("writer alive");
        }
        Ok(())
    };
    for s in 1..=sources {
        apply(
            &mut k,
            Operation::SourceConnected { source_id: SourceId(s), source_epoch: 1, link_generation: 1 },
            &mut seq,
        )
        .expect("connect");
    }
    let mut src_seq = vec![0u64; sources as usize];
    let mut ring_waits = 0u64;
    let mut data_ops = 0u64;
    let mut v = 0i64;
    let mut next_report = report_every;
    while data_ops < ops {
        if rate > 0 && data_ops.is_multiple_of(64) {
            let due = Duration::from_secs_f64(data_ops as f64 / rate as f64);
            let el = t0.elapsed();
            if due > el {
                std::thread::sleep(due - el);
            }
        }
        if report_every > 0 && t0.elapsed().as_secs() >= next_report {
            next_report += report_every;
            let dop = shared.durable_op.load(Acquire);
            let pop = proj_shared.as_ref().map_or(dop, |p| p.projected_op.load(Acquire));
            let dl = shared.durable_lag.summary();
            println!(
                "kind=exp001-sample variant={:?} t_s={} applied_op={seq} durable_lag_ops={} projection_lag_ops={} rss_kib={} disk_bytes={} durable_lag_us_p99_cum={} durable_lag_us_max_cum={}",
                variant,
                t0.elapsed().as_secs(),
                seq - dop,
                dop - pop.min(dop),
                status_kib("VmRSS:"),
                allocated(&dir),
                dl.p99_ns / 1000,
                dl.max_ns / 1000
            );
        }
        let si = (data_ops % u64::from(sources)) as usize;
        src_seq[si] += 1;
        let mode = if src_seq[si] == 1 { BatchMode::FullState } else { BatchMode::Delta };
        let toggle = (data_ops / u64::from(sources)) as i64 & 1;
        let observations = (0..obs)
            .map(|c| Observation {
                channel_id: ChannelId(c + 1),
                value: Value::I64(if c == 0 { if fanout > 0 { toggle } else { v } } else { v + i64::from(c) }),
            })
            .collect();
        let op = Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(si as u32 + 1),
            link_generation: 1,
            mode,
            source_sequence: src_seq[si],
            observations,
        });
        loop {
            match apply(&mut k, op.clone(), &mut seq) {
                Ok(()) => break,
                Err(ApplyError::OutboxCapacityExceeded { .. }) => {
                    ring_waits += 1;
                    std::thread::sleep(Duration::from_micros(200));
                    let d = shared.durable_event.load(Acquire);
                    if k.outbox_status().is_some_and(|s| s.durable_watermark < d) {
                        let _ = apply(&mut k, Operation::AdvanceDurableWatermark { up_to_event_seq: d }, &mut seq);
                    }
                }
                Err(e) => panic!("{e:?}"),
            }
        }
        data_ops += 1;
        v += 1;
        if data_ops.is_multiple_of(256) {
            let d = shared.durable_event.load(Acquire);
            if k.outbox_status().is_some_and(|s| s.durable_watermark < d) {
                let _ = apply(&mut k, Operation::AdvanceDurableWatermark { up_to_event_seq: d }, &mut seq);
            }
        }
    }
    let applied_at = t0.elapsed();
    while shared.durable_op.load(Acquire) < seq {
        std::thread::sleep(Duration::from_micros(100));
    }
    let durable_at = t0.elapsed();
    // C: historian ready = projection caught up with the durable log.
    let projected_at = match &proj_shared {
        Some(ps) => {
            while ps.projected_op.load(Acquire) < seq {
                std::thread::sleep(Duration::from_micros(100));
            }
            t0.elapsed()
        }
        None => durable_at,
    };
    let cpu_threads = thread_cpu();
    let (cpu1, minflt1, majflt1) = proc_stat("/proc/self/stat");
    let events = shared.durable_event.load(Acquire);
    let commits = shared.commits.load(Relaxed);
    let cl = shared.commit_latency.summary();
    let dl = shared.durable_lag.summary();
    let gs = shared.group_size.summary();
    let checkpoints = shared.checkpoints_written.load(Relaxed);
    let cwrite = shared.checkpoint_write.summary();
    stop.store(true, Relaxed);
    let reads = reader_h.map(|h| h.join().expect("reader")).unwrap_or(0);
    drop(tx);
    writer_handle.join().expect("writer");
    let proj_batches = proj_shared.as_ref().map_or(0, |p| p.batches.load(Relaxed));
    let proj_batch = proj_shared.as_ref().map(|p| p.batch_latency.summary()).unwrap_or_default();
    let projector = proj_handle.map(|h| h.join().expect("projector"));
    drop(projector);
    let io_bytes = io_write_bytes().saturating_sub(io0);
    let rss_hwm_kib = status_kib("VmHWM:");
    let (journal_bytes, projection_bytes) = match variant {
        Variant::A => (allocated(&dir), 0),
        _ => (allocated(&wal_dir), if variant == Variant::C { allocated(&dir) - allocated(&wal_dir) } else { 0 }),
    };
    let total_ops = seq;
    let dsecs = durable_at.as_secs_f64();
    let psecs = projected_at.as_secs_f64();
    let cpu_s = (cpu1 - cpu0) as f64 / 100.0;
    let threads = cpu_threads.iter().map(|(n, s)| format!("cpu_{n}_s={s:.2}")).collect::<Vec<_>>().join(" ");
    let variant_s = format!("{variant:?}").to_lowercase();
    println!(
        "kind=exp001 variant={variant_s} label={label} rep={rep} fs={} ops={data_ops} total_ops={total_ops} sources={sources} obs={obs} \
         events_per_op={fanout} events={events} observations={} commit_max={commit_max} commit_delay_us={} ring={ring} ring_waits={ring_waits} \
         applied_s={:.3} durable_s={dsecs:.3} historian_ready_s={psecs:.3} ops_per_s={:.0} events_per_s={:.0} obs_per_s={:.0} \
         historian_ops_per_s={:.0} commits={commits} fsync_per_s={:.0} group_p50={} group_max={} commit_us_p50={} commit_us_p99={} \
         commit_us_p999={} commit_us_max={} durable_lag_us_p50={} durable_lag_us_p99={} durable_lag_us_p999={} durable_lag_us_max={} \
         checkpoints={checkpoints} checkpoint_write_us_p50={} checkpoint_write_us_max={} proj_batches={proj_batches} \
         proj_batch_us_p50={} proj_batch_us_p99={} journal_bytes={journal_bytes} projection_bytes={projection_bytes} \
         io_write_bytes={io_bytes} io_mib_per_s={:.1} io_bytes_per_event={:.1} io_bytes_per_op={:.1} rss_hwm_kib={rss_hwm_kib} \
         cpu_total_s={cpu_s:.2} {threads} minflt={} majflt={} reads={reads} read_us_p50={} read_us_p99={} read_lag_events_p50={} \
         read_lag_events_p99={}",
        fsinfo(&dir),
        st_obs.load(Relaxed),
        commit_delay.as_micros(),
        applied_at.as_secs_f64(),
        total_ops as f64 / dsecs,
        events as f64 / dsecs,
        st_obs.load(Relaxed) as f64 / dsecs,
        total_ops as f64 / psecs,
        commits as f64 / dsecs,
        gs.p50_ns,
        gs.max_ns,
        cl.p50_ns / 1000,
        cl.p99_ns / 1000,
        cl.p999_ns / 1000,
        cl.max_ns / 1000,
        dl.p50_ns / 1000,
        dl.p99_ns / 1000,
        dl.p999_ns / 1000,
        dl.max_ns / 1000,
        cwrite.p50_ns / 1000,
        cwrite.max_ns / 1000,
        proj_batch.p50_ns / 1000,
        proj_batch.p99_ns / 1000,
        io_bytes as f64 / dsecs / 1048576.0,
        if events > 0 { io_bytes as f64 / events as f64 } else { 0.0 },
        io_bytes as f64 / total_ops as f64,
        minflt1 - minflt0,
        majflt1 - majflt0,
        read_lat.summary().p50_ns / 1000,
        read_lat.summary().p99_ns / 1000,
        read_lag.summary().p50_ns,
        read_lag.summary().p99_ns,
    );

    // Historian queries after the run (A: journal file, C: projection file).
    let hist_db = match variant {
        Variant::A => Some(db.clone()),
        Variant::C => Some(proj_db.clone()),
        Variant::B => None,
    };
    if let Some(hdb) = hist_db
        && events > 0
        && queries > 0
    {
        let r = store::open_read_only(&hdb, 2048).expect("ro");
        let (tl, _) = reader::current_timeline(&r).expect("tl");
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut lat = Vec::with_capacity(queries);
        for _ in 0..queries {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let q =
                HistoryQuery { after_event_seq: rng % events, limit: 1000, max_scan: 100_000, ..Default::default() };
            let t = Instant::now();
            let _ = reader::history(&r, tl, &q).expect("history");
            lat.push(t.elapsed().as_nanos() as u64);
        }
        println!(
            "kind=exp001-query variant={variant_s} label={label} rep={rep} queries={queries} page_us_p50={} page_us_p99={}",
            pct(&mut lat, 0.5) / 1000,
            pct(&mut lat, 0.99) / 1000
        );
    }

    // Restart: open + recovery (A: quick_check + checkpoint + replay; B/C: open-wal scan +
    // checkpoint + replay; C also reopens the projection and catches up).
    let t = Instant::now();
    let mut rebuild_t = Duration::ZERO;
    let (digest, rep_ops, cp_op, extra) = match variant {
        Variant::A => {
            let st = Store::open(&sqlite_o, cfg.fingerprint()).expect("reopen");
            let r = recover(&st.conn, st.info.timeline_no, cfg.clone()).expect("recover");
            (r.kernel.state_digest(), r.report.replayed_operations, r.report.checkpoint_op, String::new())
        }
        Variant::B | Variant::C => {
            let (st, wal) = OwalStore::open(&wal_dir, OwalOptions::default(), cfg.fingerprint()).expect("reopen");
            let r = owal::recover(&st, &wal, cfg.clone()).expect("recover");
            let mut extra = format!(
                " kernel_recover_ms={:.1} open_wal_segments={}",
                t.elapsed().as_secs_f64() * 1e3,
                st.open_report.segments_scanned
            );
            if variant == Variant::C {
                let t2 = Instant::now();
                let mut pr = Projector::open(
                    &proj_db,
                    Durability::NormalBenchmarkOnly,
                    cfg.fingerprint(),
                    st.manifest.timeline_id,
                )
                .expect("projection");
                let n = pr.catch_up(&wal, 4096).expect("catch up");
                extra += &format!(
                    " projection_reopen_ms={:.1} projection_catch_up_ops={n}",
                    t2.elapsed().as_secs_f64() * 1e3
                );
                drop(pr);
                if rebuild {
                    let t3 = Instant::now();
                    let mut pr = Projector::rebuild(
                        &proj_db,
                        Durability::NormalBenchmarkOnly,
                        cfg.fingerprint(),
                        st.manifest.timeline_id,
                    )
                    .expect("rebuild");
                    let n = pr.catch_up(&wal, 8192).expect("rebuild catch up");
                    rebuild_t = t3.elapsed();
                    extra += &format!(
                        " projection_rebuild_ms={:.1} projection_rebuild_ops={n}",
                        rebuild_t.as_secs_f64() * 1e3
                    );
                }
            }
            (r.kernel.state_digest(), r.report.replayed_operations, r.report.checkpoint_op, extra)
        }
    };
    // recover_ms: until the kernel is rebuilt AND (C) the historian is caught up again; the
    // optional full rebuild of the projection is reported separately.
    let rec_t = t.elapsed() - rebuild_t;
    assert_eq!(digest, k.state_digest(), "recovered state = live state");
    println!(
        "kind=exp001-recovery variant={variant_s} label={label} rep={rep} total_ops={total_ops} recover_ms={:.1} checkpoint_op={} replayed_ops={rep_ops} digest_equal=true{extra}",
        rec_t.as_secs_f64() * 1e3,
        cp_op.map_or("none".to_owned(), |c| c.to_string()),
    );
    if std::env::args().all(|a| a != "--keep") {
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Raw `fdatasync` latency of the filesystem (context for every number above).
pub fn fsync_probe() {
    use std::io::Write;
    let dir = PathBuf::from(arg_str("--store").expect("--store DIR"));
    let n = arg("--n", 2000) as usize;
    let size = arg("--size", 4096) as usize;
    let _ = std::fs::create_dir_all(&dir);
    let p = dir.join("fsync-probe.bin");
    let mut f = std::fs::OpenOptions::new().create(true).truncate(true).write(true).open(&p).expect("probe file");
    let buf = vec![0xA5u8; size];
    let mut lat = Vec::with_capacity(n);
    for _ in 0..n {
        f.write_all(&buf).expect("write");
        let t = Instant::now();
        f.sync_data().expect("fdatasync");
        lat.push(t.elapsed().as_nanos() as u64);
    }
    let _ = std::fs::remove_file(&p);
    println!(
        "kind=fsync-probe fs={} n={n} append_bytes={size} fdatasync_us_p50={} fdatasync_us_p99={} fdatasync_us_p999={} fdatasync_us_max={}",
        fsinfo(&dir),
        pct(&mut lat, 0.5) / 1000,
        pct(&mut lat, 0.99) / 1000,
        pct(&mut lat, 0.999) / 1000,
        pct(&mut lat, 1.0) / 1000
    );
}
