//! VS-006 in-process journal and historian benchmark (brief §7): the real writer
//! (`scada-journal`, group commit, `synchronous=FULL` unless `--durability normal`) fed by
//! a real kernel, without sockets. Measures sustained durable throughput (μ, operations
//! and events per second), fsync/s, commit latency, bytes per event, WAL size, write
//! amplification, historian query latency, startup recovery and replay.
//!
//! ```text
//! scada-bench journal --store DIR --ops N [--events-per-op E] [--commit-max M]
//!     [--checkpoint-every K] [--durability full|normal] [--queries Q] [--ring C]
//!     [--label L] [--keep]
//! ```
//! `--events-per-op 0` = zero-event operations (one tag, no rule). Otherwise one tag with
//! E `high` rules toggled at every operation: every operation produces E events
//! (fan-out E). The ring (outbox) is released by the durable watermark exactly as in the
//! runtime (J3); a full ring makes the producer wait for the writer.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::mpsc::sync_channel;
use std::time::{Duration, Instant};

use scada_core::*;
use scada_journal::progress::JournalShared;
use scada_journal::reader::{self, HistoryQuery};
use scada_journal::recovery::recover;
use scada_journal::store::{self, StartupCheck, Store, StoreOptions};
use scada_journal::writer::{self, WriterOptions};
use scada_journal::{
    CheckpointMsg, Durability, OpRecord, TimeEnvelope, WriterMsg, boottime_ns, clock_quality, realtime_ns,
};

use crate::{arg, arg_str};

fn config(fanout: u32, ring: u64) -> Arc<ValidatedConfig> {
    let spec = ConfigSpec {
        limits: CoreLimits { max_observations_per_batch: 8 },
        tags: vec![TagSpec { tag_id: TagId(1), name: "t1".into(), value_type: ValueType::I64 }],
        sources: vec![SourceSpec {
            source_id: SourceId(1),
            name: "s".into(),
            channels: vec![ChannelSpec { channel_id: ChannelId(1), tag_id: TagId(1), value_type: ValueType::I64 }],
            supervision: Supervision::NONE,
        }],
    };
    let rules: Vec<AlarmRuleSpec> = (1..=fanout)
        .map(|i| AlarmRuleSpec {
            alarm_id: AlarmId(i),
            name: format!("a{i}"),
            tag_id: TagId(1),
            condition: AlarmCondition::High { trip: 1, reset: 0 },
            severity: Severity::Major,
        })
        .collect();
    // Zero-event operations: no alarm engine at all (J4: they are journaled anyway).
    let alarms = (fanout > 0).then_some(AlarmSpec { max_rules: fanout, outbox_capacity_events: ring, rules });
    Arc::new(ValidatedConfig::validate_with_alarms(spec, alarms).expect("config"))
}

/// `--compact`: the writer compacts replay payloads after checkpoints (ADR 0013 D13).
static COMPACT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn io_write_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("write_bytes:"))?.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

fn file_size(p: &Path) -> u64 {
    std::fs::metadata(p).map_or(0, |m| m.len())
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

fn fsinfo(dir: &Path) -> String {
    let out = std::process::Command::new("df").arg("-T").arg(dir).output();
    out.ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).map(|l| l.split_whitespace().take(2).collect::<Vec<_>>().join(":")))
        .unwrap_or_else(|| "unknown".into())
}

pub fn journal_bench() {
    let dir = PathBuf::from(arg_str("--store").expect("--store DIR"));
    let ops = arg("--ops", 1_000_000);
    let fanout = arg("--events-per-op", 1) as u32;
    let commit_max = arg("--commit-max", 1024) as usize;
    let checkpoint_every = arg("--checkpoint-every", 100_000);
    let queries = arg("--queries", 2000) as usize;
    let ring = arg("--ring", 65_536);
    let label = arg_str("--label").unwrap_or_default();
    COMPACT.store(std::env::args().any(|a| a == "--compact"), std::sync::atomic::Ordering::Relaxed);
    let durability = match arg_str("--durability").as_deref() {
        Some("normal") => Durability::NormalBenchmarkOnly,
        _ => Durability::Full,
    };
    let durable_label = if durability == Durability::Full { "FULL" } else { "NORMAL_NOT_DURABLE" };
    {
        use std::os::unix::fs::DirBuilderExt;
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).expect("store dir");
    }
    let db = dir.join("bench.db");
    let cfg = config(fanout, ring);
    let o = StoreOptions {
        path: db.clone(),
        durability,
        startup_check: StartupCheck::Quick,
        cache_kib: 8192,
        test_max_pages: None,
    };
    let mut st = Store::open(&o, cfg.fingerprint()).expect("open");
    let rec = recover(&st.conn, st.info.timeline_no, cfg.clone()).expect("recover");
    let boot = st.begin_boot([1; 16], &scada_journal::os_boot_id(), 1, None, 0, 0).expect("boot");
    let tl = st.info.timeline_no;
    let shared = Arc::new(JournalShared::new(0, 0));
    let (tx, rx) = sync_channel::<WriterMsg>(4096 + 16);
    let w = writer::spawn(
        st,
        shared.clone(),
        rx,
        WriterOptions {
            commit_max_operations: commit_max,
            commit_max_delay: Duration::ZERO,
            checkpoints_keep: 2,
            compact_replay_payloads: COMPACT.load(std::sync::atomic::Ordering::Relaxed),
            wal_max_bytes: 64 << 20,
            durability,
            boot_generation: boot,
            test_delay: Duration::ZERO,
            test_stall_file: None,
            test_eio_at_commit: None,
        },
        Box::new(|| {}),
    )
    .expect("writer");
    let mut k = rec.kernel;
    let io0 = io_write_bytes();
    let t0 = Instant::now();
    let mut seq = k.last_operation_sequence();
    let apply = |k: &mut Kernel, op: Operation, seq: &mut u64| -> Result<(), ApplyError> {
        let s = *seq + 1;
        let before = k.outbox_status().map_or(1, |x| x.next_event_seq);
        let input = SequencedInput { operation_sequence: s, operation: op };
        let now = realtime_ns();
        k.apply_ref(&input)?;
        *seq = s;
        let after = k.outbox_status().map_or(1, |x| x.next_event_seq);
        let events: Vec<AlarmEvent> = (before..after).filter_map(|e| k.alarm_event(e)).collect();
        let mut r = OpRecord {
            operation_seq: s,
            operation: input.operation,
            time: TimeEnvelope {
                sequenced_at_utc_ns: now,
                boottime_ns: boottime_ns(),
                logical_time_ns: now,
                clock_quality: clock_quality::UNKNOWN,
                clock_flags: 0,
                received_at_utc_ns: None,
            },
            source_times: None,
            events,
            origins: Vec::new(),
            reconciliation: false,
            action: None,
            recovery: Vec::new(),
            anomaly: None,
            reserved_bytes: 0,
            receipt: None,
        };
        r.reserved_bytes = r.queued_bytes();
        shared.reserved_ops.fetch_add(1, Relaxed);
        shared.reserved_bytes.fetch_add(r.reserved_bytes, Relaxed);
        tx.send(WriterMsg::Op(Box::new(r))).expect("writer alive");
        if checkpoint_every > 0 && s.is_multiple_of(checkpoint_every) {
            // Sequencer-side pause (as in the kernel): export + digest.
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
    // Fresh store: the first link generation is 1.
    let link = 1u64;
    apply(
        &mut k,
        Operation::SourceConnected { source_id: SourceId(1), source_epoch: 1, link_generation: link },
        &mut seq,
    )
    .expect("connect");
    let mut v = 0i64;
    let mut src_seq = 0u64;
    let mut ring_waits = 0u64;
    let mut data_ops = 0u64;
    while data_ops < ops {
        src_seq += 1;
        let mode = if src_seq == 1 { BatchMode::FullState } else { BatchMode::Delta };
        let op = Operation::ObservationBatch(ObservationBatch {
            source_id: SourceId(1),
            link_generation: link,
            mode,
            source_sequence: src_seq,
            observations: vec![Observation { channel_id: ChannelId(1), value: Value::I64(v) }],
        });
        loop {
            match apply(&mut k, op.clone(), &mut seq) {
                Ok(()) => break,
                Err(ApplyError::OutboxCapacityExceeded { .. }) => {
                    // Ring full: wait for durable progress and release (as the runtime does).
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
        if fanout > 0 {
            v ^= 1;
        } else {
            v += 1;
        }
        // Release the ring as the runtime does after commits (every 256 operations here).
        if data_ops.is_multiple_of(256) {
            let d = shared.durable_event.load(Acquire);
            if k.outbox_status().is_some_and(|s| s.durable_watermark < d) {
                let _ = apply(&mut k, Operation::AdvanceDurableWatermark { up_to_event_seq: d }, &mut seq);
            }
        }
    }
    let applied_at = t0.elapsed();
    while shared.durable_op.load(Acquire) < seq {
        std::thread::sleep(Duration::from_micros(200));
    }
    let durable_at = t0.elapsed();
    let events = shared.durable_event.load(Acquire);
    let commits = shared.commits.load(Relaxed);
    let compacted = shared.compacted_operations.load(Relaxed);
    let cl = shared.commit_latency.summary();
    let gs = shared.group_size.summary();
    let checkpoints = shared.checkpoints_written.load(Relaxed);
    let cpause = shared.checkpoint_pause.summary();
    let cwrite = shared.checkpoint_write.summary();

    drop(tx);
    w.join().expect("writer");
    let io_bytes = io_write_bytes().saturating_sub(io0);
    let wal_before = file_size(&db.with_extension("db-wal"));
    // Sizes after a final TRUNCATE checkpoint of the WAL (what a restart sees).
    let c = rusqlite::Connection::open(&db).expect("open");
    let _: (i64, i64, i64) =
        c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).expect("ckpt");
    let page: i64 = c.query_row("PRAGMA page_size", [], |r| r.get(0)).expect("page");
    let pages: i64 = c.query_row("PRAGMA page_count", [], |r| r.get(0)).expect("pages");
    let ev_bytes: Option<i64> = c
        .query_row("SELECT sum(pgsize) FROM dbstat WHERE name LIKE 'alarm_events%' OR name LIKE 'sqlite_autoindex_alarm_events%'", [], |r| r.get(0))
        .ok();
    drop(c);
    let db_bytes = (page * pages) as u64;
    let total_ops = seq;
    let dsecs = durable_at.as_secs_f64();
    println!(
        "kind=journal label={label} durability={durable_label} fs={} ops={data_ops} total_ops={total_ops} events_per_op={fanout} events={events} \
         commit_max={commit_max} ring={ring} ring_waits={ring_waits} applied_s={:.3} durable_s={dsecs:.3} ops_per_s={:.0} events_per_s={:.0} \
         commits={commits} fsync_per_s={:.0} group_p50={} group_max={} commit_us_p50={} commit_us_p99={} commit_us_max={} \
         checkpoints={checkpoints} checkpoint_pause_us_p99={} checkpoint_write_us_p50={} checkpoint_write_us_max={} compacted_upto_op={compacted} db_bytes={db_bytes} wal_bytes_at_close={wal_before} \
         bytes_per_event={:.1} bytes_per_op={:.1} event_table_bytes={} io_write_bytes={io_bytes} write_amplification={:.2}",
        fsinfo(&dir),
        applied_at.as_secs_f64(),
        total_ops as f64 / dsecs,
        events as f64 / dsecs,
        commits as f64 / dsecs,
        gs.p50_ns,
        gs.max_ns,
        cl.p50_ns / 1000,
        cl.p99_ns / 1000,
        cl.max_ns / 1000,
        cpause.p99_ns / 1000,
        cwrite.p50_ns / 1000,
        cwrite.max_ns / 1000,
        if events > 0 { db_bytes as f64 / events as f64 } else { 0.0 },
        db_bytes as f64 / total_ops as f64,
        ev_bytes.map_or("n/a".to_owned(), |b| b.to_string()),
        io_bytes as f64 / db_bytes.max(1) as f64,
    );

    // Historian queries (read-only connection, as the history socket uses).
    if events > 0 && queries > 0 {
        let r = store::open_read_only(&db, 2048).expect("ro");
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut page_lat = Vec::with_capacity(queries);
        let mut alarm_lat = Vec::with_capacity(queries);
        let mut rows = 0u64;
        for _ in 0..queries {
            let after = next() % events;
            let q = HistoryQuery {
                after_event_seq: after,
                limit: 1000,
                alarm_id: None,
                max_scan: 100_000,
                ..Default::default()
            };
            let t = Instant::now();
            let p = reader::history(&r, tl, &q).expect("history");
            page_lat.push(t.elapsed().as_nanos() as u64);
            rows += p.rows.len() as u64;
            if fanout > 0 {
                let a = 1 + (next() % u64::from(fanout)) as u32;
                let q = HistoryQuery {
                    after_event_seq: after,
                    limit: 100,
                    alarm_id: Some(a),
                    max_scan: 100_000,
                    ..Default::default()
                };
                let t = Instant::now();
                let _ = reader::history(&r, tl, &q).expect("history by alarm");
                alarm_lat.push(t.elapsed().as_nanos() as u64);
            }
        }
        let t = Instant::now();
        let _ = reader::projection(&r, tl, 0, 1000).expect("projection");
        let proj = t.elapsed();
        println!(
            "kind=journal-query label={label} events={events} queries={queries} page_rows=1000 page_us_p50={} page_us_p99={} rows_per_page_avg={:.0} \
             by_alarm_limit=100 by_alarm_us_p50={} by_alarm_us_p99={} projection_us={}",
            pct(&mut page_lat, 0.5) / 1000,
            pct(&mut page_lat, 0.99) / 1000,
            rows as f64 / queries as f64,
            pct(&mut alarm_lat, 0.5) / 1000,
            pct(&mut alarm_lat, 0.99) / 1000,
            proj.as_micros()
        );
    }

    // Startup: open (quick_check) + recovery from the last checkpoint + replay of the tail.
    let t = Instant::now();
    let st = Store::open(&o, cfg.fingerprint()).expect("reopen");
    let open_t = t.elapsed();
    let t = Instant::now();
    let rec = recover(&st.conn, st.info.timeline_no, cfg.clone()).expect("recover");
    let rec_t = t.elapsed();
    assert_eq!(rec.kernel.last_operation_sequence(), total_ops, "recovered position");
    assert_eq!(rec.kernel.state_digest(), k.state_digest(), "recovered state = live state");
    println!(
        "kind=journal-recovery label={label} total_ops={total_ops} open_quick_check_ms={:.1} recover_ms={:.1} checkpoint_op={} replayed_ops={} replayed_events={} replay_ops_per_s={:.0} digest_equal=true",
        open_t.as_secs_f64() * 1e3,
        rec_t.as_secs_f64() * 1e3,
        rec.report.checkpoint_op.map_or("none".to_owned(), |c| c.to_string()),
        rec.report.replayed_operations,
        rec.report.replayed_events,
        rec.report.replayed_operations as f64 / rec_t.as_secs_f64().max(1e-9)
    );
    drop(st);
    if std::env::args().all(|a| a != "--keep") {
        let _ = std::fs::remove_dir_all(&dir);
    }
}
