//! VS-006 historian socket (`scada.history.v1`, ADR 0013 D12). Served off the sequencer
//! by a bounded pool of read-only SQLite connections (one per allowed connection), each
//! query on a blocking thread. Reads see only committed (hence durable) rows.

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use scada_journal::reader::{self, HistoryQuery};
use scada_journal::recovery::RecoveryReport;
use scada_journal::{JournalShared, LatSummary, live};
use scada_protocol::Message;
use scada_protocol::frame;
use scada_protocol::history_v1 as hpb;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::config::PersistenceSettings;
use crate::persist::PersistPublic;
use crate::time_authority::TimeShared;

/// Largest accepted history request frame.
pub const MAX_HISTORY_REQUEST_BYTES: u32 = 1024;
/// Upper bound of one encoded `HistoryEvent` (keys + varints + action id + note).
pub const MAX_HISTORY_EVENT_WIRE_BYTES: u128 = 160 + 16 + 256;

/// Everything the historian needs (immutable after startup, plus shared atomics).
pub struct HistoryService {
    pub db: PathBuf,
    pub timeline_no: i64,
    pub timeline_id: [u8; 16],
    pub settings: PersistenceSettings,
    pub shared: Arc<JournalShared>,
    pub public: Arc<PersistPublic>,
    pub time: Arc<TimeShared>,
    pub report: RecoveryReport,
    pub boot_generation: u64,
    pub durable: bool,
    pub source_ids: Vec<u32>,
    pub started: Instant,
    pool: Mutex<Vec<Connection>>,
}

impl std::fmt::Debug for HistoryService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryService").field("db", &self.db).finish_non_exhaustive()
    }
}

/// Construction parameters of [`HistoryService`].
pub struct HistoryInit {
    pub timeline_no: i64,
    pub timeline_id: [u8; 16],
    pub settings: PersistenceSettings,
    pub shared: Arc<JournalShared>,
    pub public: Arc<PersistPublic>,
    pub time: Arc<TimeShared>,
    pub report: RecoveryReport,
    pub boot_generation: u64,
    pub source_ids: Vec<u32>,
}

impl HistoryService {
    pub fn new(i: HistoryInit) -> Result<HistoryService, scada_journal::JournalError> {
        let n = i.settings.max_history_connections as usize;
        let cache = (i.settings.sqlite_cache_kib / 4).max(256);
        let mut pool = Vec::with_capacity(n);
        for _ in 0..n {
            pool.push(scada_journal::store::open_read_only(&i.settings.path, cache)?);
        }
        Ok(HistoryService {
            db: i.settings.path.clone(),
            timeline_no: i.timeline_no,
            timeline_id: i.timeline_id,
            durable: i.settings.durable,
            settings: i.settings,
            shared: i.shared,
            public: i.public,
            time: i.time,
            report: i.report,
            boot_generation: i.boot_generation,
            source_ids: i.source_ids,
            started: Instant::now(),
            pool: Mutex::new(pool),
        })
    }
}

/// Memory bound of the historian for `s` (pages and response buffers per connection).
pub fn history_bound_bytes(s: &PersistenceSettings) -> u128 {
    if s.history_socket_path.is_none() {
        return 0;
    }
    let page = u128::from(s.max_history_page);
    let per = page
        .saturating_mul(size_of::<reader::HistoryRow>() as u128 + 256 + size_of::<hpb::HistoryEvent>() as u128)
        .saturating_add(page.saturating_mul(MAX_HISTORY_EVENT_WIRE_BYTES))
        .saturating_add(u128::from(MAX_HISTORY_REQUEST_BYTES))
        .saturating_add(crate::alarms::CONTROL_SESSION_ESTIMATE_BYTES);
    per.saturating_mul(u128::from(s.max_history_connections))
}

fn err(code: hpb::ErrorCode, detail: &str) -> hpb::HistoryResponse {
    hpb::HistoryResponse {
        body: Some(hpb::history_response::Body::Error(hpb::ErrorResponse {
            code: code as i32,
            detail: detail.to_owned(),
        })),
    }
}

fn lat(s: LatSummary) -> hpb::LatencySummary {
    hpb::LatencySummary { count: s.count, p50_ns: s.p50_ns, p99_ns: s.p99_ns, p999_ns: s.p999_ns, max_ns: s.max_ns }
}

fn episode(flags: u8) -> hpb::EpisodeState {
    match scada_core::AlarmFlags(flags).episode() {
        scada_core::EpisodeState::Normal => hpb::EpisodeState::Normal,
        scada_core::EpisodeState::ActiveUnacked => hpb::EpisodeState::ActiveUnacked,
        scada_core::EpisodeState::ActiveAcked => hpb::EpisodeState::ActiveAcked,
        scada_core::EpisodeState::ReturnedUnacked => hpb::EpisodeState::ReturnedUnacked,
    }
}

fn clock(q: u8) -> i32 {
    i32::from(q.min(4))
}

fn event(r: &reader::HistoryRow, durability: hpb::Durability) -> hpb::HistoryEvent {
    let _ = durability;
    hpb::HistoryEvent {
        event_seq: r.event_seq,
        operation_seq: r.operation_seq,
        ordinal_in_operation: r.ordinal,
        alarm_id: r.alarm_id,
        occurrence_id: r.occurrence_id,
        activation_token: r.activation_token,
        kind: i32::from(r.kind),
        state_after: episode(r.status_after) as i32,
        input_valid_after: scada_core::AlarmFlags(r.status_after).input_valid(),
        actor_uid: r.actor_uid,
        sequenced_at_utc_ns: r.sequenced_at_utc_ns,
        logical_time_ns: r.logical_time_ns,
        sequencer_clock: clock(r.clock_quality),
        clock_regressed: r.clock_flags & scada_journal::clock_flags::REGRESSED != 0,
        durable_at_utc_ns: r.durable_at_utc_ns,
        origin_utc_ns: r.origin_utc_ns,
        origin_source: i32::from(r.origin_source),
        origin_clock: clock(r.origin_clock),
        reconciliation: r.reconciliation,
        actual_field_time_unknown: r.reconciliation,
        action_id: r.action_id.clone().unwrap_or_default(),
        note: r.note.clone().unwrap_or_default(),
    }
}

impl HistoryService {
    fn durability(&self) -> hpb::Durability {
        if self.durable { hpb::Durability::PowerLossDurable } else { hpb::Durability::NotDurableBenchmarkOnly }
    }

    fn recovering(&self) -> bool {
        self.public
            .live
            .iter()
            .any(|l| matches!(l.load(Relaxed), live::UNVERIFIED_RECOVERED | live::AWAITING_FULL_STATE))
    }

    fn journal_status(&self) -> hpb::JournalStatus {
        let s = &self.shared;
        let ld = |a: &AtomicU64| a.load(Relaxed);
        let state = match s.state() {
            scada_journal::JournalState::Ok => hpb::JournalState::Ok,
            scada_journal::JournalState::Backpressure => hpb::JournalState::Backpressure,
            scada_journal::JournalState::DiskFull => hpb::JournalState::DiskFull,
            scada_journal::JournalState::Fault => hpb::JournalState::Fault,
        };
        hpb::JournalStatus {
            state: state as i32,
            durability: self.durability() as i32,
            timeline_id: self.timeline_id.to_vec(),
            durable_operation_seq: s.durable_op.load(Acquire),
            durable_event_seq: s.durable_event.load(Acquire),
            backlog_operations: ld(&s.reserved_ops),
            backlog_bytes: ld(&s.reserved_bytes),
            queue_capacity_operations: u64::from(self.settings.journal_queue_operations),
            queue_capacity_bytes: self.settings.journal_queue_bytes,
            commits: ld(&s.commits),
            committed_operations: ld(&s.committed_ops),
            committed_events: ld(&s.committed_events),
            commit_latency: Some(lat(s.commit_latency.summary())),
            durable_lag: Some(lat(s.durable_lag.summary())),
            checkpoints_written: ld(&s.checkpoints_written),
            last_checkpoint_operation_seq: ld(&s.last_checkpoint_op),
            wal_bytes: ld(&s.wal_bytes),
            db_bytes: ld(&s.db_bytes),
            disk_free_bytes: ld(&s.disk_free),
            last_error: s.error(),
            refused_queue_full: ld(&s.refused_queue_full),
            refused_disk_full: ld(&s.refused_disk_full),
            refused_fault: ld(&s.refused_fault),
            uptime_ms: u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }
    }

    fn time_status(&self) -> hpb::TimeStatus {
        let t = &self.time;
        hpb::TimeStatus {
            last_sequenced_at_utc_ns: t.last_sequenced_at.load(Relaxed),
            last_logical_time_ns: t.last_logical.load(Relaxed),
            os_boot_id: t.os_boot_id.clone(),
            boot_generation: self.boot_generation,
            host_clock: clock(t.host_quality.load(Relaxed)),
            host_offset_uncertainty_ns: t.host_uncertainty_ns.load(Relaxed),
            host_sync_source: t.host_source.lock().map(|g| g.clone()).unwrap_or_default(),
            clock_regressions: t.regressions.load(Relaxed),
            clock_jumps_forward: t.jumps_forward.load(Relaxed),
            last_anomaly_utc_ns: t.last_anomaly_at.load(Relaxed),
            last_anomaly_delta_ns: t.last_anomaly_delta.load(Relaxed),
        }
    }

    /// Runs on a blocking thread with one pooled connection.
    fn query(&self, c: &Connection, body: hpb::history_request::Body) -> hpb::HistoryResponse {
        use hpb::history_request::Body as B;
        use hpb::history_response::Body as R;
        let max = self.settings.max_history_page;
        let lim = |l: u32| if l == 0 { max } else { l.min(max) };
        let fail = |e: scada_journal::JournalError| err(hpb::ErrorCode::Unavailable, &e.to_string());
        match body {
            B::Query(q) => {
                let tl = if q.timeline_id.is_empty() {
                    self.timeline_no
                } else {
                    match reader::timeline_no(c, &q.timeline_id) {
                        Ok(Some(t)) => t,
                        Ok(None) => return err(hpb::ErrorCode::UnknownTimeline, "unknown timeline_id"),
                        Err(e) => return fail(e),
                    }
                };
                if q.occurrence_id != 0 && q.alarm_id == 0 {
                    return err(hpb::ErrorCode::InvalidRequest, "occurrence_id requires alarm_id");
                }
                let hq = HistoryQuery {
                    after_event_seq: q.after_event_seq,
                    limit: lim(q.limit),
                    alarm_id: (q.alarm_id != 0).then_some(q.alarm_id),
                    occurrence_id: (q.occurrence_id != 0).then_some(q.occurrence_id),
                    from_sequenced_at: q.from_sequenced_at_utc_ns,
                    to_sequenced_at: q.to_sequenced_at_utc_ns,
                    reconciliation_only: q.reconciliation_only,
                    max_scan: self.settings.history_max_scan,
                };
                match reader::history(c, tl, &hq) {
                    Ok(p) => {
                        let tid = reader::current_timeline(c).ok().filter(|(n, _)| *n == tl).map_or_else(
                            || if q.timeline_id.is_empty() { self.timeline_id.to_vec() } else { q.timeline_id.clone() },
                            |(_, id)| id,
                        );
                        let d = self.durability();
                        hpb::HistoryResponse {
                            body: Some(R::History(hpb::HistoryPage {
                                events: p.rows.iter().map(|r| event(r, d)).collect(),
                                next_after_event_seq: p.next_after_event_seq,
                                has_more: p.has_more,
                                as_of_durable_event_seq: p.as_of_event_seq,
                                timeline_id: tid,
                                oldest_retained_event_seq: p.oldest_retained_event_seq,
                            })),
                        }
                    }
                    Err(e) => fail(e),
                }
            }
            B::Projection(q) => match reader::projection(c, self.timeline_no, q.after_alarm_id, lim(q.limit)) {
                Ok((rows, more, as_of)) => hpb::HistoryResponse {
                    body: Some(R::Projection(hpb::ActiveAlarmProjection {
                        next_after_alarm_id: rows.last().map_or(q.after_alarm_id, |r| r.alarm_id),
                        alarms: rows
                            .iter()
                            .map(|r| hpb::ProjectedAlarm {
                                alarm_id: r.alarm_id,
                                state: episode(r.status_after) as i32,
                                input_valid: scada_core::AlarmFlags(r.status_after).input_valid(),
                                occurrence_id: r.occurrence_id,
                                activation_token: r.activation_token,
                                last_event_seq: r.last_event_seq,
                            })
                            .collect(),
                        has_more: more,
                        as_of_durable_event_seq: as_of,
                        recovery_in_progress: self.recovering(),
                    })),
                },
                Err(e) => fail(e),
            },
            B::Recovery(_) => {
                let rows = match reader::source_recovery(c, self.timeline_no) {
                    Ok(r) => r,
                    Err(e) => return fail(e),
                };
                let sources = self
                    .source_ids
                    .iter()
                    .enumerate()
                    .map(|(i, &sid)| {
                        let row = rows.iter().find(|r| r.source_id == sid);
                        let live = self.public.live.get(i).map_or(live::NEVER_CONNECTED, |a| a.load(Relaxed));
                        let hist = self.public.history.get(i).map_or(0, |a| a.load(Relaxed));
                        hpb::SourceRecovery {
                            source_id: sid,
                            live: i32::from(live),
                            continuity: i32::from(if hist == 0 { row.map_or(0, |r| r.history) } else { hist }),
                            source_epoch: row.and_then(|r| r.source_epoch),
                            last_source_sequence: row.and_then(|r| r.last_source_sequence),
                            gap_records: row.map_or(0, |r| r.gaps),
                            verified_at_operation_seq: row.map_or(0, |r| r.verified_at_operation_seq),
                            link_connected: self.public.connected.get(i).is_some_and(|a| a.load(Relaxed)),
                        }
                    })
                    .collect();
                let r = &self.report;
                hpb::HistoryResponse {
                    body: Some(R::Recovery(hpb::RecoveryStatus {
                        timeline_id: self.timeline_id.to_vec(),
                        boot_generation: self.boot_generation,
                        recovered_from_checkpoint: r.checkpoint_op.is_some(),
                        checkpoint_operation_seq: r.checkpoint_op.unwrap_or(0),
                        replayed_operations: r.replayed_operations,
                        recovery_duration_us: u64::try_from(r.duration.as_micros()).unwrap_or(u64::MAX),
                        recovered_operation_seq: r.durable_op,
                        recovered_event_seq: r.durable_event,
                        sources,
                    })),
                }
            }
            B::Journal(_) => hpb::HistoryResponse { body: Some(R::Journal(self.journal_status())) },
            B::Time(_) => hpb::HistoryResponse { body: Some(R::Time(self.time_status())) },
        }
    }
}

/// Accept loop of the history socket.
pub async fn serve_history(
    svc: Arc<HistoryService>,
    listener: UnixListener,
    frame_timeout: Duration,
    write_timeout: Duration,
) {
    let permits = Arc::new(Semaphore::new(svc.settings.max_history_connections as usize));
    let mut scratch = Vec::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        match permits.clone().try_acquire_owned() {
            Ok(permit) => {
                let svc = svc.clone();
                tokio::spawn(async move {
                    session(&svc, stream, frame_timeout, write_timeout).await;
                    drop(permit);
                });
            }
            Err(_) => {
                let m = err(hpb::ErrorCode::TooManyConnections, "history connection limit reached");
                if frame::encode_frame(&m, frame::MAX_FRAME_BYTES_CEILING, &mut scratch).is_ok() {
                    let _ = stream.try_write(&scratch);
                }
            }
        }
    }
}

async fn write(
    wr: &mut tokio::net::unix::OwnedWriteHalf,
    m: &hpb::HistoryResponse,
    scratch: &mut Vec<u8>,
    t: Duration,
) -> bool {
    use tokio::io::AsyncWriteExt;
    if frame::encode_frame(m, frame::MAX_FRAME_BYTES_CEILING, scratch).is_err() {
        return false;
    }
    matches!(timeout(t, wr.write_all(scratch)).await, Ok(Ok(())))
}

async fn session(svc: &Arc<HistoryService>, stream: UnixStream, ft: Duration, wt: Duration) {
    let uid = stream.peer_cred().ok().map(|c| c.uid());
    let (mut rd, mut wr) = stream.into_split();
    let mut scratch = Vec::new();
    if !uid.is_some_and(|u| svc.settings.history_authorized_uids.binary_search(&u).is_ok()) {
        let _ = write(
            &mut wr,
            &err(hpb::ErrorCode::Unauthorized, "peer uid not in history_authorized_uids"),
            &mut scratch,
            wt,
        )
        .await;
        return;
    }
    let mut buf = Vec::new();
    loop {
        let r = timeout(
            crate::alarms::CONTROL_IDLE_TIMEOUT,
            crate::server::read_frame_pub(&mut rd, MAX_HISTORY_REQUEST_BYTES, &mut buf, ft),
        )
        .await;
        match r {
            Ok(Ok(true)) => {}
            Ok(Err(Some(_))) => {
                let _ = write(&mut wr, &err(hpb::ErrorCode::FrameTooLarge, "invalid frame"), &mut scratch, wt).await;
                return;
            }
            _ => return,
        }
        let Ok(req) = hpb::HistoryRequest::decode(buf.as_slice()) else {
            let _ = write(&mut wr, &err(hpb::ErrorCode::InvalidRequest, "malformed HistoryRequest"), &mut scratch, wt)
                .await;
            return;
        };
        if req.history_api_version != 1 {
            let _ =
                write(&mut wr, &err(hpb::ErrorCode::InvalidRequest, "history_api_version must be 1"), &mut scratch, wt)
                    .await;
            return;
        }
        let Some(body) = req.body else {
            let _ = write(&mut wr, &err(hpb::ErrorCode::InvalidRequest, "empty request"), &mut scratch, wt).await;
            return;
        };
        let s = svc.clone();
        let resp = tokio::task::spawn_blocking(move || {
            let conn = s.pool.lock().ok().and_then(|mut p| p.pop());
            let Some(conn) = conn else { return err(hpb::ErrorCode::Unavailable, "no reader connection") };
            let r = s.query(&conn, body);
            if let Ok(mut p) = s.pool.lock() {
                p.push(conn);
            }
            r
        })
        .await
        .unwrap_or_else(|_| err(hpb::ErrorCode::Unavailable, "query task failed"));
        if !write(&mut wr, &resp, &mut scratch, wt).await {
            return;
        }
    }
}

impl std::fmt::Debug for HistoryInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistoryInit").field("timeline_no", &self.timeline_no).finish_non_exhaustive()
    }
}
