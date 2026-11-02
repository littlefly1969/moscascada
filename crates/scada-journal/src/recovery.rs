//! Startup recovery (ADR 0013 D8): last valid complete checkpoint, then a verified replay
//! of the durable suffix in an isolated kernel. Every inconsistency is fail-closed.
//!
//! The replay is bounded (J9): at most `checkpoint_every_operations` plus the backlog that
//! was durable but not yet checkpointed. The ring is reloaded from the store (at most C
//! events); events already archived are never pushed through the ring again.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension, Row, params};
use scada_core::{AlarmEvent, Kernel, KernelCheckpoint, SequencedInput, TransitionOutcome, ValidatedConfig};

use crate::codec::{decode_checkpoint, decode_op, event_from_row, payload_digest};
use crate::{JournalError, sq, uq};

/// What the recovery did (persisted in `runtime_boots`, served by `GetRecoveryStatus`).
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub checkpoint_op: Option<u64>,
    /// Checkpoints that were skipped, with the reason (a newer corrupt one falls back to
    /// the previous one).
    pub checkpoints_rejected: Vec<String>,
    pub replayed_operations: u64,
    pub replayed_events: u64,
    pub durable_op: u64,
    pub durable_event: u64,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct Recovered {
    pub kernel: Kernel,
    pub report: RecoveryReport,
}

const EVENT_COLS: &str =
    "event_seq, operation_seq, ordinal, alarm_id, occurrence_id, activation_token, kind, status_after, actor_uid";

fn event_of(r: &Row<'_>) -> rusqlite::Result<Option<AlarmEvent>> {
    let g = |i: usize| -> rusqlite::Result<i64> { r.get(i) };
    Ok(event_from_row(
        uq(g(0)?),
        uq(g(1)?),
        u32::try_from(g(2)?).unwrap_or(u32::MAX),
        u32::try_from(g(3)?).unwrap_or(u32::MAX),
        uq(g(4)?),
        uq(g(5)?),
        u8::try_from(g(6)?).unwrap_or(0),
        u8::try_from(g(7)?).unwrap_or(0),
        u32::try_from(g(8)?).unwrap_or(u32::MAX),
    ))
}

/// Events `from ..= to` in order (must be complete).
pub fn load_events(c: &Connection, tl: i64, from: u64, to: u64) -> Result<Vec<AlarmEvent>, JournalError> {
    let mut st = c.prepare(&format!(
        "SELECT {EVENT_COLS} FROM alarm_events WHERE timeline_no = ?1 AND event_seq BETWEEN ?2 AND ?3 ORDER BY event_seq"
    ))?;
    let mut rows = st.query(params![tl, sq(from), sq(to)])?;
    let mut out =
        Vec::with_capacity(usize::try_from(to.saturating_sub(from).saturating_add(1)).unwrap_or(0).min(1 << 22));
    while let Some(r) = rows.next()? {
        out.push(event_of(r)?.ok_or_else(|| JournalError::Corrupt("event with an unknown kind".into()))?);
    }
    Ok(out)
}

/// Highest durable operation and event of a timeline.
pub fn durable_position(c: &Connection, tl: i64) -> Result<(u64, u64), JournalError> {
    let op: Option<i64> =
        c.query_row("SELECT max(operation_seq) FROM journal_operations WHERE timeline_no = ?1", params![tl], |r| {
            r.get(0)
        })?;
    let ev: Option<i64> =
        c.query_row("SELECT max(event_seq) FROM alarm_events WHERE timeline_no = ?1", params![tl], |r| r.get(0))?;
    Ok((op.map_or(0, uq), ev.map_or(0, uq)))
}

fn try_checkpoint(
    c: &Connection,
    tl: i64,
    config: &Arc<ValidatedConfig>,
    blob: &[u8],
    blob_digest: &[u8],
    state_digest: &[u8],
    durable_op: u64,
) -> Result<Kernel, String> {
    if blake3::hash(blob).as_bytes().as_slice() != blob_digest {
        return Err("blob digest mismatch".into());
    }
    let cp: KernelCheckpoint = decode_checkpoint(blob).map_err(|e| format!("decode: {e}"))?;
    if !cp.recoverable {
        return Err("checkpoint of a non-recoverable kernel".into());
    }
    if cp.last_operation_sequence > durable_op {
        return Err(format!("checkpoint at {} beyond the journal ({durable_op})", cp.last_operation_sequence));
    }
    let ring = match Kernel::checkpoint_ring_window(config, &cp) {
        Some((a, b)) => load_events(c, tl, a, b).map_err(|e| e.to_string())?,
        None => Vec::new(),
    };
    let k = Kernel::from_checkpoint(config.clone(), &cp, &ring).map_err(|e| format!("restore: {e}"))?;
    if k.state_digest().as_slice() != state_digest {
        return Err("state digest after restore differs from the recorded one".into());
    }
    Ok(k)
}

/// Rebuild the kernel of timeline `tl` (fail-closed on any inconsistency).
pub fn recover(c: &Connection, tl: i64, config: Arc<ValidatedConfig>) -> Result<Recovered, JournalError> {
    let t0 = Instant::now();
    let (durable_op, durable_event) = durable_position(c, tl)?;
    let mut report = RecoveryReport { durable_op, durable_event, ..RecoveryReport::default() };
    let mut kernel = None;
    {
        let mut st = c.prepare(
            "SELECT operation_seq, blob, blob_digest, state_digest FROM journal_checkpoints \
             WHERE timeline_no = ?1 AND complete = 1 ORDER BY operation_seq DESC",
        )?;
        let mut rows = st.query(params![tl])?;
        while let Some(r) = rows.next()? {
            let op = uq(r.get(0)?);
            let blob: Vec<u8> = r.get(1)?;
            let bd: Vec<u8> = r.get(2)?;
            let sd: Vec<u8> = r.get(3)?;
            match try_checkpoint(c, tl, &config, &blob, &bd, &sd, durable_op) {
                Ok(k) => {
                    report.checkpoint_op = Some(op);
                    kernel = Some(k);
                    break;
                }
                Err(e) => report.checkpoints_rejected.push(format!("checkpoint at operation {op}: {e}")),
            }
        }
    }
    let mut kernel = match kernel {
        Some(k) => k,
        None => {
            let first: Option<i64> = c
                .query_row(
                    "SELECT min(operation_seq) FROM journal_operations WHERE timeline_no = ?1",
                    params![tl],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let compacted: i64 = c.query_row(
                "SELECT count(*) FROM retention_markers WHERE timeline_no = ?1 AND table_name = ?2",
                params![tl, crate::writer::COMPACTED_TABLE],
                |r| r.get(0),
            )?;
            if compacted > 0 {
                return Err(JournalError::Corrupt(format!(
                    "no valid checkpoint ({}) and the replay payloads before it were compacted (compact_replay_payloads)",
                    report.checkpoints_rejected.join("; ")
                )));
            }
            if first.is_some_and(|f| f != 1) {
                return Err(JournalError::Corrupt(format!(
                    "no valid checkpoint ({}) and the journal no longer starts at operation 1",
                    report.checkpoints_rejected.join("; ")
                )));
            }
            Kernel::new_recoverable(config.clone())
        }
    };
    replay(c, tl, &mut kernel, &mut report).map_err(|e| match e {
        // A row whose column has the wrong type or range is corruption, not an I/O error.
        JournalError::Sqlite(
            ref x @ (rusqlite::Error::InvalidColumnType(..)
            | rusqlite::Error::FromSqlConversionFailure(..)
            | rusqlite::Error::IntegralValueOutOfRange(..)),
        ) => JournalError::Corrupt(x.to_string()),
        other => other,
    })?;
    if kernel.last_operation_sequence() != durable_op {
        return Err(JournalError::Corrupt(format!(
            "replay ended at operation {} but the journal holds {durable_op}",
            kernel.last_operation_sequence()
        )));
    }
    let next_ev = kernel.outbox_status().map_or(1, |s| s.next_event_seq);
    if next_ev.saturating_sub(1) != durable_event {
        return Err(JournalError::Corrupt(format!(
            "kernel event sequence {} differs from the archived {durable_event}",
            next_ev.saturating_sub(1)
        )));
    }
    report.duration = t0.elapsed();
    Ok(Recovered { kernel, report })
}

fn replay(c: &Connection, tl: i64, k: &mut Kernel, report: &mut RecoveryReport) -> Result<(), JournalError> {
    let from = k.last_operation_sequence();
    let mut ops = c.prepare(
        "SELECT operation_seq, payload, payload_digest, first_event_seq, event_count FROM journal_operations \
         WHERE timeline_no = ?1 AND operation_seq > ?2 ORDER BY operation_seq",
    )?;
    let first_ev = k.outbox_status().map_or(1, |s| s.next_event_seq);
    let mut evs = c.prepare(&format!(
        "SELECT {EVENT_COLS} FROM alarm_events WHERE timeline_no = ?1 AND event_seq >= ?2 ORDER BY event_seq"
    ))?;
    let mut ev_rows = evs.query(params![tl, sq(first_ev)])?;
    let mut op_rows = ops.query(params![tl, sq(from)])?;
    while let Some(r) = op_rows.next()? {
        let seq = uq(r.get(0)?);
        let payload: Vec<u8> = r.get(1)?;
        let digest: i64 = r.get(2)?;
        let rec_first = uq(r.get(3)?);
        let rec_count = uq(r.get(4)?);
        let expected = k.last_operation_sequence().saturating_add(1);
        if seq != expected {
            return Err(JournalError::Corrupt(format!("operation sequence gap: expected {expected}, found {seq}")));
        }
        if payload_digest(&payload) != digest {
            return Err(JournalError::Corrupt(format!("operation {seq}: payload digest mismatch")));
        }
        let operation = decode_op(&payload).map_err(|e| JournalError::Corrupt(format!("operation {seq}: {e}")))?;
        let before = k.outbox_status().map_or(1, |s| s.next_event_seq);
        match k.apply_ref(&SequencedInput { operation_sequence: seq, operation }) {
            Ok(TransitionOutcome::Applied { operation_sequence, .. }) if operation_sequence == seq => {}
            other => {
                return Err(JournalError::Corrupt(format!("operation {seq}: replay diverged ({other:?})")));
            }
        }
        let after = k.outbox_status().map_or(1, |s| s.next_event_seq);
        let produced = after.saturating_sub(before);
        if produced != rec_count || (produced > 0 && rec_first != before) {
            return Err(JournalError::Corrupt(format!(
                "operation {seq}: replay produced {produced} events from {before}, journal records {rec_count} from {rec_first}"
            )));
        }
        for s in before..after {
            let got = k.alarm_event(s);
            let row = ev_rows.next()?.ok_or_else(|| JournalError::Corrupt(format!("event {s} missing")))?;
            let stored = event_of(row)?;
            if got.is_none() || got != stored {
                return Err(JournalError::Corrupt(format!(
                    "event {s}: replay {got:?} differs from archived {stored:?}"
                )));
            }
        }
        report.replayed_operations += 1;
        report.replayed_events += produced;
    }
    if let Some(r) = ev_rows.next()? {
        let s: i64 = r.get(0)?;
        return Err(JournalError::Corrupt(format!("archived event {s} produced by no journaled operation")));
    }
    Ok(())
}

/// Recently recorded operator actions (dedup cache), newest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredAction {
    pub action_id: [u8; 16],
    pub alarm_id: u32,
    pub requested_token: u64,
    pub operation_seq: u64,
    pub event_seq: Option<u64>,
    pub outcome: u8,
}

pub fn load_actions(c: &Connection, tl: i64, limit: u32) -> Result<Vec<StoredAction>, JournalError> {
    let mut st = c.prepare(
        "SELECT action_id, alarm_id, requested_token, operation_seq, event_seq, outcome FROM operator_actions \
         WHERE timeline_no = ?1 ORDER BY operation_seq DESC LIMIT ?2",
    )?;
    let mut rows = st.query(params![tl, i64::from(limit)])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let id: Vec<u8> = r.get(0)?;
        let Ok(action_id) = <[u8; 16]>::try_from(id.as_slice()) else { continue };
        let ev: Option<i64> = r.get(4)?;
        out.push(StoredAction {
            action_id,
            alarm_id: u32::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
            requested_token: uq(r.get(2)?),
            operation_seq: uq(r.get(3)?),
            event_seq: ev.map(uq),
            outcome: u8::try_from(r.get::<_, i64>(5)?).unwrap_or(0),
        });
    }
    Ok(out)
}
