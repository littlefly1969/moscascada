//! SQLite schema version 1 (ADR 0013 D6). Integer columns only on the hot path; every
//! table is keyed by `timeline_no` (local integer mapped to the 16-byte `timeline_id`).

/// `PRAGMA application_id` of a MoscaSCADA journal ("MSJ1").
pub const APPLICATION_ID: i32 = 0x4D53_4A31;
/// `PRAGMA user_version` = schema version. Unknown versions are refused (fail-closed).
pub const SCHEMA_VERSION: i32 = 1;

pub const DDL: &str = r#"
CREATE TABLE schema_meta(
  id INTEGER PRIMARY KEY CHECK (id = 1),
  schema_version INTEGER NOT NULL,
  store_uuid BLOB NOT NULL,
  current_timeline INTEGER NOT NULL,
  boot_generation INTEGER NOT NULL,
  last_logical_time_ns INTEGER NOT NULL,
  created_at_utc_ns INTEGER NOT NULL
);
CREATE TABLE timelines(
  timeline_no INTEGER PRIMARY KEY,
  timeline_id BLOB NOT NULL UNIQUE,
  created_at_utc_ns INTEGER NOT NULL,
  config_fingerprint BLOB NOT NULL,
  reason TEXT NOT NULL,
  previous_timeline INTEGER
);
CREATE TABLE runtime_boots(
  timeline_no INTEGER NOT NULL,
  boot_generation INTEGER NOT NULL,
  kernel_instance_id BLOB NOT NULL,
  os_boot_id TEXT NOT NULL,
  first_operation_seq INTEGER NOT NULL,
  started_at_utc_ns INTEGER NOT NULL,
  recovered_from_checkpoint INTEGER,
  replayed_operations INTEGER NOT NULL,
  recovery_duration_us INTEGER NOT NULL,
  PRIMARY KEY (timeline_no, boot_generation)
) WITHOUT ROWID;
CREATE TABLE journal_operations(
  timeline_no INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL,
  kind INTEGER NOT NULL,
  payload BLOB NOT NULL,
  payload_digest INTEGER NOT NULL,
  first_event_seq INTEGER NOT NULL,
  event_count INTEGER NOT NULL,
  boot_generation INTEGER NOT NULL,
  sequenced_at_utc_ns INTEGER NOT NULL,
  boottime_ns INTEGER NOT NULL,
  logical_time_ns INTEGER NOT NULL,
  clock_quality INTEGER NOT NULL,
  clock_flags INTEGER NOT NULL,
  received_at_utc_ns INTEGER,
  source_times BLOB,
  reconciliation INTEGER NOT NULL,
  PRIMARY KEY (timeline_no, operation_seq)
) WITHOUT ROWID;
CREATE TABLE alarm_events(
  timeline_no INTEGER NOT NULL,
  event_seq INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL,
  ordinal INTEGER NOT NULL,
  alarm_id INTEGER NOT NULL,
  occurrence_id INTEGER NOT NULL,
  activation_token INTEGER NOT NULL,
  kind INTEGER NOT NULL,
  status_after INTEGER NOT NULL,
  actor_uid INTEGER NOT NULL,
  reconciliation INTEGER NOT NULL,
  origin_utc_ns INTEGER,
  origin_source INTEGER NOT NULL,
  origin_clock INTEGER NOT NULL,
  PRIMARY KEY (timeline_no, event_seq)
) WITHOUT ROWID;
CREATE INDEX alarm_events_by_alarm ON alarm_events(timeline_no, alarm_id, event_seq);
CREATE TABLE alarm_active_projection(
  timeline_no INTEGER NOT NULL,
  alarm_id INTEGER NOT NULL,
  status_after INTEGER NOT NULL,
  occurrence_id INTEGER NOT NULL,
  activation_token INTEGER NOT NULL,
  last_event_seq INTEGER NOT NULL,
  last_kind INTEGER NOT NULL,
  PRIMARY KEY (timeline_no, alarm_id)
) WITHOUT ROWID;
CREATE TABLE projection_meta(
  timeline_no INTEGER PRIMARY KEY,
  as_of_event_seq INTEGER NOT NULL,
  as_of_operation_seq INTEGER NOT NULL
);
CREATE TABLE journal_checkpoints(
  checkpoint_id INTEGER PRIMARY KEY,
  timeline_no INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL,
  event_seq_high INTEGER NOT NULL,
  config_fingerprint BLOB NOT NULL,
  state_digest BLOB NOT NULL,
  schema_version INTEGER NOT NULL,
  blob BLOB NOT NULL,
  blob_digest BLOB NOT NULL,
  complete INTEGER NOT NULL,
  created_at_utc_ns INTEGER NOT NULL,
  UNIQUE (timeline_no, operation_seq)
);
CREATE TABLE journal_commits(
  timeline_no INTEGER NOT NULL,
  first_op INTEGER NOT NULL,
  last_op INTEGER NOT NULL,
  operations INTEGER NOT NULL,
  events INTEGER NOT NULL,
  bytes INTEGER NOT NULL,
  commit_ns INTEGER,
  durable_at_utc_ns INTEGER,
  PRIMARY KEY (timeline_no, first_op)
) WITHOUT ROWID;
CREATE TABLE operator_actions(
  action_id BLOB PRIMARY KEY,
  timeline_no INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL,
  event_seq INTEGER,
  alarm_id INTEGER NOT NULL,
  occurrence_id INTEGER NOT NULL,
  requested_token INTEGER NOT NULL,
  actor_uid INTEGER NOT NULL,
  principal TEXT,
  note TEXT NOT NULL,
  outcome INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX operator_actions_by_op ON operator_actions(timeline_no, operation_seq);
CREATE TABLE source_recovery(
  timeline_no INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  source_epoch INTEGER,
  boot_generation INTEGER NOT NULL,
  live_verification INTEGER NOT NULL,
  history_continuity INTEGER NOT NULL,
  last_verified_operation_seq INTEGER NOT NULL,
  last_source_sequence INTEGER,
  updated_operation_seq INTEGER NOT NULL,
  PRIMARY KEY (timeline_no, source_id)
) WITHOUT ROWID;
CREATE TABLE source_history_gaps(
  gap_id INTEGER PRIMARY KEY,
  timeline_no INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  source_epoch INTEGER,
  from_seq INTEGER,
  to_seq INTEGER,
  recorded_operation_seq INTEGER NOT NULL,
  bounds_unknown INTEGER NOT NULL,
  reason INTEGER NOT NULL,
  boot_generation INTEGER NOT NULL
);
CREATE TABLE source_events(
  timeline_no INTEGER NOT NULL,
  source_id INTEGER NOT NULL,
  source_epoch INTEGER NOT NULL,
  source_event_seq INTEGER NOT NULL,
  source_event_id BLOB,
  kind INTEGER NOT NULL,
  source_occurred_at_utc_ns INTEGER,
  driver_acquired_at_utc_ns INTEGER,
  clock_quality INTEGER NOT NULL,
  provenance INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL,
  payload BLOB,
  PRIMARY KEY (timeline_no, source_id, source_epoch, source_event_seq)
) WITHOUT ROWID;
CREATE TABLE time_observations(
  timeline_no INTEGER NOT NULL,
  operation_seq INTEGER NOT NULL,
  kind INTEGER NOT NULL,
  delta_ns INTEGER NOT NULL,
  realtime_ns INTEGER NOT NULL,
  boottime_ns INTEGER NOT NULL,
  boot_generation INTEGER NOT NULL,
  PRIMARY KEY (timeline_no, operation_seq, kind)
) WITHOUT ROWID;
CREATE TABLE retention_markers(
  marker_id INTEGER PRIMARY KEY,
  timeline_no INTEGER NOT NULL,
  table_name TEXT NOT NULL,
  from_key INTEGER NOT NULL,
  to_key INTEGER NOT NULL,
  reason TEXT NOT NULL,
  persisted_at_utc_ns INTEGER NOT NULL
);
"#;
