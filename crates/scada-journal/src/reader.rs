//! Historian queries on read-only connections (ADR 0013 D12). Every page is read in one
//! read transaction (WAL snapshot), so rows and `as_of` are consistent; everything
//! visible was committed under the store's durability (J1). Keyset cursors only; filtered
//! scans are bounded by `max_scan` rows per page (the cursor still advances).

use rusqlite::{Connection, OptionalExtension, params};

use crate::{JournalError, sq, uq};

#[derive(Debug, Clone, Default)]
pub struct HistoryQuery {
    pub after_event_seq: u64,
    pub limit: u32,
    pub alarm_id: Option<u32>,
    pub occurrence_id: Option<u64>,
    pub from_sequenced_at: Option<i64>,
    pub to_sequenced_at: Option<i64>,
    pub reconciliation_only: bool,
    /// Rows examined per page at most (bounded cost of filtered queries).
    pub max_scan: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    pub event_seq: u64,
    pub operation_seq: u64,
    pub ordinal: u32,
    pub alarm_id: u32,
    pub occurrence_id: u64,
    pub activation_token: u64,
    pub kind: u8,
    pub status_after: u8,
    pub actor_uid: u32,
    pub sequenced_at_utc_ns: i64,
    pub logical_time_ns: i64,
    pub clock_quality: u8,
    pub clock_flags: u8,
    pub durable_at_utc_ns: Option<i64>,
    pub origin_utc_ns: Option<i64>,
    pub origin_source: u8,
    pub origin_clock: u8,
    pub reconciliation: bool,
    pub action_id: Option<Vec<u8>>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct HistoryPageData {
    pub rows: Vec<HistoryRow>,
    pub next_after_event_seq: u64,
    pub has_more: bool,
    pub as_of_event_seq: u64,
    pub oldest_retained_event_seq: u64,
}

fn u8c(v: i64) -> u8 {
    u8::try_from(v).unwrap_or(u8::MAX)
}
fn u32c(v: i64) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

/// Committed `as_of` of a timeline (`projection_meta`).
pub fn as_of(c: &Connection, tl: i64) -> Result<(u64, u64), JournalError> {
    Ok(c.query_row(
        "SELECT as_of_event_seq, as_of_operation_seq FROM projection_meta WHERE timeline_no = ?1",
        params![tl],
        |r| Ok((uq(r.get(0)?), uq(r.get(1)?))),
    )
    .optional()?
    .unwrap_or((0, 0)))
}

/// Current timeline number and id.
pub fn current_timeline(c: &Connection) -> Result<(i64, Vec<u8>), JournalError> {
    Ok(c.query_row(
        "SELECT t.timeline_no, t.timeline_id FROM schema_meta m JOIN timelines t ON t.timeline_no = m.current_timeline",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?)
}

/// Timeline number of a 16-byte timeline id.
pub fn timeline_no(c: &Connection, id: &[u8]) -> Result<Option<i64>, JournalError> {
    Ok(c.query_row("SELECT timeline_no FROM timelines WHERE timeline_id = ?1", params![id], |r| r.get(0)).optional()?)
}

pub fn history(c: &Connection, tl: i64, q: &HistoryQuery) -> Result<HistoryPageData, JournalError> {
    let tx = c.unchecked_transaction()?;
    let (as_of_ev, _) = as_of(&tx, tl)?;
    let oldest: Option<i64> =
        tx.query_row("SELECT min(event_seq) FROM alarm_events WHERE timeline_no = ?1", params![tl], |r| r.get(0))?;
    let limit = q.limit.max(1);
    let filtered = q.from_sequenced_at.is_some()
        || q.to_sequenced_at.is_some()
        || q.reconciliation_only
        || q.occurrence_id.is_some();
    // Scan window: unbounded for the plain keyset (the PK range serves it), bounded for
    // filters that cannot use an index.
    let scan_end = if filtered {
        q.after_event_seq.saturating_add(q.max_scan.max(u64::from(limit))).min(as_of_ev)
    } else {
        as_of_ev
    };
    let sql = String::from(
        "SELECT e.event_seq, e.operation_seq, e.ordinal, e.alarm_id, e.occurrence_id, e.activation_token, e.kind, e.status_after, \
         e.actor_uid, o.sequenced_at_utc_ns, o.logical_time_ns, o.clock_quality, o.clock_flags, \
         (SELECT jc.durable_at_utc_ns FROM journal_commits jc WHERE jc.timeline_no = e.timeline_no AND jc.first_op <= e.operation_seq \
          ORDER BY jc.first_op DESC LIMIT 1), \
         e.origin_utc_ns, e.origin_source, e.origin_clock, e.reconciliation, a.action_id, a.note \
         FROM alarm_events e {INDEX} \
         JOIN journal_operations o ON o.timeline_no = e.timeline_no AND o.operation_seq = e.operation_seq \
         LEFT JOIN operator_actions a ON e.kind = 4 AND a.timeline_no = e.timeline_no AND a.operation_seq = e.operation_seq \
         WHERE e.timeline_no = ?1 AND e.event_seq > ?2 AND e.event_seq <= ?3",
    );
    // Per-alarm queries use the (timeline, alarm, event_seq) index: without the hint the
    // planner may walk the primary key range and scan every other alarm's rows (measured:
    // 100 ms p50 on 6 M rows with 20 000 alarms, docs/benchmarks/vs006.md).
    let sql = sql.replace("{INDEX}", if q.alarm_id.is_some() { "INDEXED BY alarm_events_by_alarm" } else { "" });
    let mut sql = sql;
    if q.alarm_id.is_some() {
        sql.push_str(" AND e.alarm_id = ?5");
    }
    if q.occurrence_id.is_some() {
        sql.push_str(" AND e.occurrence_id = ?6");
    }
    if q.from_sequenced_at.is_some() {
        sql.push_str(" AND o.sequenced_at_utc_ns >= ?7");
    }
    if q.to_sequenced_at.is_some() {
        sql.push_str(" AND o.sequenced_at_utc_ns < ?8");
    }
    if q.reconciliation_only {
        sql.push_str(" AND e.reconciliation = 1");
    }
    sql.push_str(" ORDER BY e.event_seq LIMIT ?4");
    let mut st = tx.prepare(&sql)?;
    let mut idx: Vec<(usize, Box<dyn rusqlite::ToSql>)> = vec![
        (1, Box::new(tl)),
        (2, Box::new(sq(q.after_event_seq))),
        (3, Box::new(sq(scan_end))),
        (4, Box::new(i64::from(limit))),
    ];
    if let Some(a) = q.alarm_id {
        idx.push((5, Box::new(i64::from(a))));
    }
    if let Some(o) = q.occurrence_id {
        idx.push((6, Box::new(sq(o))));
    }
    if let Some(t) = q.from_sequenced_at {
        idx.push((7, Box::new(t)));
    }
    if let Some(t) = q.to_sequenced_at {
        idx.push((8, Box::new(t)));
    }
    for (i, v) in &idx {
        st.raw_bind_parameter(*i, v.as_ref())?;
    }
    let mut rows = st.raw_query();
    let mut out = Vec::with_capacity(limit as usize);
    while let Some(r) = rows.next()? {
        out.push(HistoryRow {
            event_seq: uq(r.get(0)?),
            operation_seq: uq(r.get(1)?),
            ordinal: u32c(r.get(2)?),
            alarm_id: u32c(r.get(3)?),
            occurrence_id: uq(r.get(4)?),
            activation_token: uq(r.get(5)?),
            kind: u8c(r.get(6)?),
            status_after: u8c(r.get(7)?),
            actor_uid: u32c(r.get(8)?),
            sequenced_at_utc_ns: r.get(9)?,
            logical_time_ns: r.get(10)?,
            clock_quality: u8c(r.get(11)?),
            clock_flags: u8c(r.get(12)?),
            durable_at_utc_ns: r.get(13)?,
            origin_utc_ns: r.get(14)?,
            origin_source: u8c(r.get(15)?),
            origin_clock: u8c(r.get(16)?),
            reconciliation: r.get::<_, i64>(17)? != 0,
            action_id: r.get(18)?,
            note: r.get(19)?,
        });
    }
    drop(rows);
    drop(st);
    let full = out.len() as u32 >= limit;
    let next = if full { out.last().map_or(scan_end, |r| r.event_seq) } else { scan_end };
    Ok(HistoryPageData {
        has_more: next < as_of_ev,
        next_after_event_seq: next,
        rows: out,
        as_of_event_seq: as_of_ev,
        oldest_retained_event_seq: oldest.map_or(0, uq),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectedRow {
    pub alarm_id: u32,
    pub status_after: u8,
    pub occurrence_id: u64,
    pub activation_token: u64,
    pub last_event_seq: u64,
}

/// Persisted active/unacknowledged alarms after `after_alarm_id` (state as of the last
/// archived event of each alarm).
pub fn projection(
    c: &Connection,
    tl: i64,
    after_alarm_id: u32,
    limit: u32,
) -> Result<(Vec<ProjectedRow>, bool, u64), JournalError> {
    let tx = c.unchecked_transaction()?;
    let (as_of_ev, _) = as_of(&tx, tl)?;
    let mut st = tx.prepare(
        "SELECT alarm_id, status_after, occurrence_id, activation_token, last_event_seq FROM alarm_active_projection \
         WHERE timeline_no = ?1 AND alarm_id > ?2 AND (status_after & 3) != 0 ORDER BY alarm_id LIMIT ?3",
    )?;
    let lim = limit.max(1);
    let rows: Vec<ProjectedRow> = st
        .query_map(params![tl, i64::from(after_alarm_id), i64::from(lim) + 1], |r| {
            Ok(ProjectedRow {
                alarm_id: u32c(r.get(0)?),
                status_after: u8c(r.get(1)?),
                occurrence_id: uq(r.get(2)?),
                activation_token: uq(r.get(3)?),
                last_event_seq: uq(r.get(4)?),
            })
        })?
        .collect::<Result<_, _>>()?;
    let more = rows.len() > lim as usize;
    let rows = rows.into_iter().take(lim as usize).collect();
    Ok((rows, more, as_of_ev))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRecoveryRow {
    pub source_id: u32,
    pub source_epoch: Option<u64>,
    pub boot_generation: u64,
    pub live: u8,
    pub history: u8,
    pub verified_at_operation_seq: u64,
    pub last_source_sequence: Option<u64>,
    pub gaps: u64,
}

pub fn source_recovery(c: &Connection, tl: i64) -> Result<Vec<SourceRecoveryRow>, JournalError> {
    let mut st = c.prepare(
        "SELECT r.source_id, r.source_epoch, r.boot_generation, r.live_verification, r.history_continuity, \
         r.last_verified_operation_seq, r.last_source_sequence, \
         (SELECT count(*) FROM source_history_gaps g WHERE g.timeline_no = r.timeline_no AND g.source_id = r.source_id) \
         FROM source_recovery r WHERE r.timeline_no = ?1 ORDER BY r.source_id",
    )?;
    let rows = st
        .query_map(params![tl], |r| {
            Ok(SourceRecoveryRow {
                source_id: u32c(r.get(0)?),
                source_epoch: r.get::<_, Option<i64>>(1)?.map(uq),
                boot_generation: uq(r.get(2)?),
                live: u8c(r.get(3)?),
                history: u8c(r.get(4)?),
                verified_at_operation_seq: uq(r.get(5)?),
                last_source_sequence: r.get::<_, Option<i64>>(6)?.map(uq),
                gaps: uq(r.get(7)?),
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapRow {
    pub source_id: u32,
    pub source_epoch: Option<u64>,
    pub from_seq: Option<u64>,
    pub to_seq: Option<u64>,
    pub recorded_operation_seq: u64,
    pub bounds_unknown: bool,
    pub reason: u8,
    pub boot_generation: u64,
}

pub fn gaps(c: &Connection, tl: i64) -> Result<Vec<GapRow>, JournalError> {
    let mut st = c.prepare(
        "SELECT source_id, source_epoch, from_seq, to_seq, recorded_operation_seq, bounds_unknown, reason, boot_generation \
         FROM source_history_gaps WHERE timeline_no = ?1 ORDER BY gap_id",
    )?;
    let rows = st
        .query_map(params![tl], |r| {
            Ok(GapRow {
                source_id: u32c(r.get(0)?),
                source_epoch: r.get::<_, Option<i64>>(1)?.map(uq),
                from_seq: r.get::<_, Option<i64>>(2)?.map(uq),
                to_seq: r.get::<_, Option<i64>>(3)?.map(uq),
                recorded_operation_seq: uq(r.get(4)?),
                bounds_unknown: r.get::<_, i64>(5)? != 0,
                reason: u8c(r.get(6)?),
                boot_generation: uq(r.get(7)?),
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

/// Time anomalies recorded for a timeline (`kind`, `delta_ns`, `operation_seq`).
pub fn time_anomalies(c: &Connection, tl: i64) -> Result<Vec<(u64, u8, i64)>, JournalError> {
    let mut st = c.prepare(
        "SELECT operation_seq, kind, delta_ns FROM time_observations WHERE timeline_no = ?1 ORDER BY operation_seq, kind",
    )?;
    let rows =
        st.query_map(params![tl], |r| Ok((uq(r.get(0)?), u8c(r.get(1)?), r.get(2)?)))?.collect::<Result<_, _>>()?;
    Ok(rows)
}
