//! Offline tool of the VS-006 journal (ADR 0013 D8): inspect, verify, read history, start a
//! new timeline. Read-only unless `new-timeline --confirm` is given; never runs while a
//! kernel holds the store for writing (except the read-only commands, which are safe).

use std::path::PathBuf;
use std::process::ExitCode;

use scada_journal::reader::{self, HistoryQuery};
use scada_journal::store::{self, open_read_only};
use scada_journal::{continuity, hex, live};

fn usage() -> ExitCode {
    eprintln!(
        "usage:\n  scada-journal info <db>\n  scada-journal verify <db>\n  scada-journal history <db> [--after N] [--limit N] [--alarm ID]\n  \
         scada-journal gaps <db>\n  scada-journal new-timeline <db> --fingerprint <hex64> --reason <text> --confirm"
    );
    ExitCode::from(2)
}

fn utc(ns: i64) -> String {
    // Display only: UTC, RFC 3339 with nanoseconds (time zones are a display concern of
    // the HMI, ADR 0014 §4).
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    // Civil from days (H. Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{nanos:09}Z", sod / 3600, sod % 3600 / 60, sod % 60)
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (Some(cmd), Some(db)) = (args.first(), args.get(1)) else { return Err("missing arguments".into()) };
    let path = PathBuf::from(db);
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned();
    match cmd.as_str() {
        "info" | "verify" => {
            let c = open_read_only(&path, 8192).map_err(|e| e.to_string())?;
            if cmd == "verify" {
                store::integrity(&c, store::StartupCheck::Full).map_err(|e| e.to_string())?;
                println!("integrity_check: ok");
            }
            let (tl, tid) = reader::current_timeline(&c).map_err(|e| e.to_string())?;
            let (op, ev) = scada_journal::recovery::durable_position(&c, tl).map_err(|e| e.to_string())?;
            let boot: i64 =
                c.query_row("SELECT boot_generation FROM schema_meta", [], |r| r.get(0)).map_err(|e| e.to_string())?;
            println!("timeline_no={tl} timeline_id={} boot_generation={boot}", hex(&tid));
            println!("durable_operation_seq={op} durable_event_seq={ev}");
            let cps: Vec<(i64, i64)> = c
                .prepare("SELECT operation_seq, length(blob) FROM journal_checkpoints WHERE timeline_no = ?1 AND complete = 1 ORDER BY operation_seq")
                .and_then(|mut s| s.query_map([tl], |r| Ok((r.get(0)?, r.get(1)?)))?.collect())
                .map_err(|e| e.to_string())?;
            for (o, l) in cps {
                println!("checkpoint operation_seq={o} bytes={l}");
            }
            for r in reader::source_recovery(&c, tl).map_err(|e| e.to_string())? {
                let lv = match r.live {
                    live::UNVERIFIED_RECOVERED => "UNVERIFIED_RECOVERED",
                    live::AWAITING_FULL_STATE => "AWAITING_FULL_STATE",
                    live::LIVE_VERIFIED => "LIVE_VERIFIED",
                    live::NEVER_CONNECTED => "NEVER_CONNECTED",
                    _ => "?",
                };
                let hc = match r.history {
                    continuity::COMPLETE => "COMPLETE",
                    continuity::UNKNOWN => "UNKNOWN",
                    continuity::GAP => "GAP",
                    _ => "?",
                };
                println!(
                    "source {} live={lv} history={hc} epoch={:?} last_seq={:?} gaps={}",
                    r.source_id, r.source_epoch, r.last_source_sequence, r.gaps
                );
            }
            Ok(())
        }
        "history" => {
            let c = open_read_only(&path, 8192).map_err(|e| e.to_string())?;
            let (tl, _) = reader::current_timeline(&c).map_err(|e| e.to_string())?;
            let q = HistoryQuery {
                after_event_seq: flag("--after").and_then(|v| v.parse().ok()).unwrap_or(0),
                limit: flag("--limit").and_then(|v| v.parse().ok()).unwrap_or(100),
                alarm_id: flag("--alarm").and_then(|v| v.parse().ok()),
                max_scan: 1_000_000,
                ..HistoryQuery::default()
            };
            let p = reader::history(&c, tl, &q).map_err(|e| e.to_string())?;
            for r in &p.rows {
                println!(
                    "{} op={} alarm={} occ={} kind={} status={:#04x} sequenced_at={} logical={} durable_at={} origin={} reconciliation={} uid={}",
                    r.event_seq,
                    r.operation_seq,
                    r.alarm_id,
                    r.occurrence_id,
                    r.kind,
                    r.status_after,
                    utc(r.sequenced_at_utc_ns),
                    r.logical_time_ns,
                    r.durable_at_utc_ns.map_or_else(|| "-".into(), utc),
                    r.origin_utc_ns.map_or_else(|| "null".into(), utc),
                    r.reconciliation,
                    r.actor_uid
                );
            }
            println!("next_after={} has_more={} as_of={}", p.next_after_event_seq, p.has_more, p.as_of_event_seq);
            Ok(())
        }
        "gaps" => {
            let c = open_read_only(&path, 8192).map_err(|e| e.to_string())?;
            let (tl, _) = reader::current_timeline(&c).map_err(|e| e.to_string())?;
            for g in reader::gaps(&c, tl).map_err(|e| e.to_string())? {
                println!("{g:?}");
            }
            Ok(())
        }
        "new-timeline" => {
            if !args.iter().any(|a| a == "--confirm") {
                return Err("new-timeline needs --confirm: the current state will not be recovered on the new timeline (history stays readable)".into());
            }
            let fp = flag("--fingerprint").ok_or("--fingerprint <hex64> required")?;
            let reason = flag("--reason").ok_or("--reason <text> required")?;
            let mut f = [0u8; 32];
            if fp.len() != 64 {
                return Err("fingerprint must be 64 hex digits".into());
            }
            for (i, b) in f.iter_mut().enumerate() {
                *b = u8::from_str_radix(fp.get(2 * i..2 * i + 2).ok_or("bad hex")?, 16).map_err(|e| e.to_string())?;
            }
            let (no, id) = store::new_timeline(&path, f, &reason).map_err(|e| e.to_string())?;
            println!("new timeline {no} id {}", hex(&id));
            Ok(())
        }
        _ => Err(format!("unknown command {cmd}")),
    }
}

fn main() -> ExitCode {
    if std::env::args().len() < 3 {
        return usage();
    }
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("scada-journal: {e}");
            ExitCode::FAILURE
        }
    }
}
