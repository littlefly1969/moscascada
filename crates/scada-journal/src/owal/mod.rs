//! **EXP-001 — EXPERIMENTAL** open-wal 0.2.1 backend of the *authoritative journal only*
//! (docs/experiments/exp-001-open-wal-backend.md, docs/experiments/exp-001-report.md).
//!
//! Compiled only with the `experimental-open-wal` feature. Nothing in the runtime selects
//! it: SQLite (`crate::writer`) stays the default and the production baseline. This
//! module replaces exactly one role of the SQLite store — durable append + replay — and
//! leaves the historian to a *derived* SQL projection ([`projector`]).
//!
//! Directory layout (private directory, same rules as the SQLite store):
//!
//! ```text
//! <dir>/MANIFEST                 identity: timeline id, config fingerprint (atomic create)
//! <dir>/wal/LOCK, <dir>/wal/*.wal open-wal segments (single writer, flock)
//! <dir>/checkpoints/ckpt-<op>.bin kernel checkpoints + ring events (tmp+fsync+rename)
//! ```
//!
//! Invariants kept from VS-006 (ADR 0013):
//! - one applied operation = one open-wal record ([`envelope`]); the record is the unit of
//!   atomicity, a commit group is *not* a transaction;
//! - LSN = `operation_seq` on a timeline (dense from 1, no marker records): the mapping is
//!   verified on every replayed record;
//! - durable progress is published only after `Wal::commit` returned an LSN covering the
//!   operation; a failed write/fdatasync poisons the handle and the writer enters FAULT;
//! - recovery = newest valid checkpoint (fallback to older, then genesis) + verified replay
//!   in a fresh kernel; every divergence is fail-closed;
//! - retention is **disabled**: `Wal::checkpoint` (segment deletion) is never called.

pub mod envelope;
pub mod projector;
pub mod writer;

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use open_wal::{Lsn, Wal, WalConfig, WalError};
use scada_core::{AlarmEvent, AlarmFlags, AlarmId, Kernel, SequencedInput, TransitionOutcome, ValidatedConfig};

use crate::codec::{decode_checkpoint, encode_checkpoint, event_kind_of};
use crate::recovery::RecoveryReport;
use crate::{JournalError, realtime_ns};

pub use open_wal;

const MANIFEST_MAGIC: &[u8; 8] = b"MSOWMAN1";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_BYTES: usize = 8 + 2 + 16 + 32 + 8 + 4;
const CKPT_MAGIC: &[u8; 8] = b"MSOWCKP1";
const CKPT_VERSION: u16 = 1;

/// open-wal sizing of the spike.
#[derive(Debug, Clone, Copy)]
pub struct OwalOptions {
    /// Pre-allocated bytes per segment (open-wal default 128 MiB).
    pub segment_size: u64,
    /// Hard limit of one record = one operation (open-wal default 1 MiB).
    pub max_record_size: u32,
}

impl Default for OwalOptions {
    fn default() -> Self {
        let d = WalConfig::default();
        OwalOptions { segment_size: d.segment_size, max_record_size: d.max_record_size }
    }
}

impl OwalOptions {
    fn wal_config(self) -> WalConfig {
        WalConfig { segment_size: self.segment_size, max_record_size: self.max_record_size }
    }
}

/// Identity of an open-wal journal directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Manifest {
    pub timeline_id: [u8; 16],
    pub config_fingerprint: [u8; 32],
    pub created_at_utc_ns: i64,
}

impl Manifest {
    fn encode(&self) -> [u8; MANIFEST_BYTES] {
        let mut b = [0u8; MANIFEST_BYTES];
        b[0..8].copy_from_slice(MANIFEST_MAGIC);
        b[8..10].copy_from_slice(&MANIFEST_VERSION.to_le_bytes());
        b[10..26].copy_from_slice(&self.timeline_id);
        b[26..58].copy_from_slice(&self.config_fingerprint);
        b[58..66].copy_from_slice(&self.created_at_utc_ns.to_le_bytes());
        let crc = open_wal::crc32c(&b[..66]);
        b[66..70].copy_from_slice(&crc.to_le_bytes());
        b
    }

    fn decode(b: &[u8]) -> Result<Manifest, JournalError> {
        let bad = |w: &str| Err(JournalError::Corrupt(format!("open-wal MANIFEST: {w}")));
        if b.len() != MANIFEST_BYTES {
            return bad("wrong size");
        }
        if &b[0..8] != MANIFEST_MAGIC {
            return Err(JournalError::Incompatible("not a MoscaSCADA open-wal journal (bad MANIFEST magic)".into()));
        }
        if open_wal::crc32c(&b[..66]) != u32::from_le_bytes([b[66], b[67], b[68], b[69]]) {
            return bad("checksum mismatch");
        }
        if u16::from_le_bytes([b[8], b[9]]) != MANIFEST_VERSION {
            return Err(JournalError::Incompatible("unknown open-wal MANIFEST version".into()));
        }
        let mut timeline_id = [0u8; 16];
        timeline_id.copy_from_slice(&b[10..26]);
        let mut config_fingerprint = [0u8; 32];
        config_fingerprint.copy_from_slice(&b[26..58]);
        let mut t = [0u8; 8];
        t.copy_from_slice(&b[58..66]);
        Ok(Manifest { timeline_id, config_fingerprint, created_at_utc_ns: i64::from_le_bytes(t) })
    }
}

fn fsync_dir(dir: &Path) -> Result<(), JournalError> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// Write `bytes` to `dir/name` atomically and durably (tmp + fsync + rename + dir fsync).
fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), JournalError> {
    let tmp = dir.join(format!("{name}.tmp"));
    let _ = std::fs::remove_file(&tmp);
    let mut f = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, dir.join(name))?;
    fsync_dir(dir)
}

fn map_wal(e: WalError) -> JournalError {
    match e {
        WalError::Corruption { .. }
        | WalError::TornMidLog { .. }
        | WalError::BadSegmentHeader
        | WalError::ContiguityViolation => JournalError::Corrupt(format!("open-wal: {e}")),
        WalError::Locked => JournalError::Locked(format!("open-wal: {e}")),
        WalError::InvalidConfig => JournalError::Incompatible(format!("open-wal: {e}")),
        other => JournalError::Io(format!("open-wal: {other}")),
    }
}

/// An open journal directory: identity + the single-writer `Wal` handle.
#[derive(Debug)]
pub struct OwalStore {
    pub dir: PathBuf,
    pub manifest: Manifest,
    pub options: OwalOptions,
    pub created: bool,
    /// What `Wal::open` found (torn tail truncated, segments scanned).
    pub open_report: open_wal::RecoveryReport,
}

impl OwalStore {
    pub fn wal_dir(dir: &Path) -> PathBuf {
        dir.join("wal")
    }
    pub fn checkpoint_dir(dir: &Path) -> PathBuf {
        dir.join("checkpoints")
    }

    /// Open (or create) the journal for a configuration with `fingerprint`. Fail-closed:
    /// unknown manifest, another configuration, segments without manifest, a second
    /// writer, any open-wal recovery error.
    pub fn open(dir: &Path, options: OwalOptions, fingerprint: [u8; 32]) -> Result<(OwalStore, Wal), JournalError> {
        if !dir.exists() {
            std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
        }
        crate::store::check_private_dir(dir)?;
        let mpath = dir.join("MANIFEST");
        let waldir = Self::wal_dir(dir);
        let (manifest, created) = match std::fs::read(&mpath) {
            Ok(b) => (Manifest::decode(&b)?, false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let has_segments = std::fs::read_dir(&waldir)
                    .map(|rd| rd.flatten().any(|e| e.file_name().to_string_lossy().ends_with(".wal")))
                    .unwrap_or(false);
                if has_segments {
                    return Err(JournalError::Corrupt(format!(
                        "{}: open-wal segments without MANIFEST (identity lost)",
                        dir.display()
                    )));
                }
                let m = Manifest {
                    timeline_id: crate::random_id()?,
                    config_fingerprint: fingerprint,
                    created_at_utc_ns: realtime_ns(),
                };
                write_atomic(dir, "MANIFEST", &m.encode())?;
                (m, true)
            }
            Err(e) => return Err(e.into()),
        };
        if manifest.config_fingerprint != fingerprint {
            return Err(JournalError::Incompatible(format!(
                "open-wal journal {} was created for configuration {} but this configuration is {}",
                dir.display(),
                crate::hex(&manifest.config_fingerprint),
                crate::hex(&fingerprint)
            )));
        }
        for d in [&waldir, &Self::checkpoint_dir(dir)] {
            if !d.exists() {
                std::fs::DirBuilder::new().mode(0o700).create(d)?;
                fsync_dir(dir)?;
            }
        }
        let (wal, open_report) = Wal::open(&waldir, options.wal_config()).map_err(map_wal)?;
        Ok((OwalStore { dir: dir.to_path_buf(), manifest, options, created, open_report }, wal))
    }
}

// ---- checkpoints ----

/// Header of a checkpoint file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointHeader {
    pub operation_seq: u64,
    pub event_seq_high: u64,
    pub boot_generation: u64,
    pub last_logical_time_ns: i64,
    pub state_digest: [u8; 32],
}

fn ckpt_name(op: u64) -> String {
    format!("ckpt-{op:020}.bin")
}

fn put_event(b: &mut Vec<u8>, e: &AlarmEvent) {
    b.extend_from_slice(&e.event_seq.to_le_bytes());
    b.extend_from_slice(&e.operation_seq.to_le_bytes());
    b.extend_from_slice(&e.ordinal_in_operation.to_le_bytes());
    b.extend_from_slice(&e.alarm_id.0.to_le_bytes());
    b.extend_from_slice(&e.occurrence_id.to_le_bytes());
    b.extend_from_slice(&e.activation_token.to_le_bytes());
    b.push(e.kind.code());
    b.push(e.status_after.0);
    b.extend_from_slice(&e.actor_uid.to_le_bytes());
}

fn get_event(b: &[u8]) -> Option<AlarmEvent> {
    let u64at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap_or([0; 8]));
    let u32at = |o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap_or([0; 4]));
    if b.len() != envelope::EVENT_BYTES {
        return None;
    }
    Some(AlarmEvent {
        event_seq: u64at(0),
        operation_seq: u64at(8),
        ordinal_in_operation: u32at(16),
        alarm_id: AlarmId(u32at(20)),
        occurrence_id: u64at(24),
        activation_token: u64at(32),
        kind: event_kind_of(b[40])?,
        status_after: AlarmFlags(b[41]),
        actor_uid: u32at(42),
    })
}

/// Durably write a checkpoint (kernel blob + the ring events it needs) and keep only the
/// newest `keep` files. Deletion happens only after the new file is durable.
pub fn write_checkpoint(
    dir: &Path,
    manifest: &Manifest,
    h: &CheckpointHeader,
    checkpoint: &scada_core::KernelCheckpoint,
    ring: &[AlarmEvent],
    keep: u32,
) -> Result<(), JournalError> {
    let blob = encode_checkpoint(checkpoint);
    let mut b = Vec::with_capacity(blob.len() + ring.len() * envelope::EVENT_BYTES + 256);
    b.extend_from_slice(CKPT_MAGIC);
    b.extend_from_slice(&CKPT_VERSION.to_le_bytes());
    b.extend_from_slice(&manifest.timeline_id);
    b.extend_from_slice(&manifest.config_fingerprint);
    b.extend_from_slice(&h.operation_seq.to_le_bytes());
    b.extend_from_slice(&h.event_seq_high.to_le_bytes());
    b.extend_from_slice(&h.boot_generation.to_le_bytes());
    b.extend_from_slice(&h.last_logical_time_ns.to_le_bytes());
    b.extend_from_slice(&h.state_digest);
    b.extend_from_slice(&u32::try_from(blob.len()).unwrap_or(u32::MAX).to_le_bytes());
    b.extend_from_slice(&u32::try_from(ring.len()).unwrap_or(u32::MAX).to_le_bytes());
    b.extend_from_slice(&blob);
    for e in ring {
        put_event(&mut b, e);
    }
    let digest = blake3::hash(&b);
    b.extend_from_slice(digest.as_bytes());
    let cdir = OwalStore::checkpoint_dir(dir);
    crate::fault::hit("during_checkpoint");
    write_atomic(&cdir, &ckpt_name(h.operation_seq), &b)?;
    let mut all = list_checkpoints(&cdir)?;
    all.sort_unstable_by(|a, b| b.cmp(a));
    for op in all.into_iter().skip(keep.max(1) as usize) {
        let _ = std::fs::remove_file(cdir.join(ckpt_name(op)));
    }
    Ok(())
}

fn list_checkpoints(cdir: &Path) -> Result<Vec<u64>, JournalError> {
    let mut v = Vec::new();
    for e in std::fs::read_dir(cdir)? {
        let n = e?.file_name();
        let n = n.to_string_lossy();
        if let Some(d) = n.strip_prefix("ckpt-").and_then(|s| s.strip_suffix(".bin"))
            && d.len() == 20
            && let Ok(op) = d.parse::<u64>()
        {
            v.push(op);
        }
    }
    Ok(v)
}

type LoadedCheckpoint = (CheckpointHeader, scada_core::KernelCheckpoint, Vec<AlarmEvent>);

fn read_checkpoint(path: &Path, manifest: &Manifest, file_op: u64) -> Result<LoadedCheckpoint, String> {
    let b = std::fs::read(path).map_err(|e| e.to_string())?;
    const FIXED: usize = 8 + 2 + 16 + 32 + 8 + 8 + 8 + 8 + 32 + 4 + 4;
    if b.len() < FIXED + 32 {
        return Err("file too short".into());
    }
    let (body, dig) = b.split_at(b.len() - 32);
    if blake3::hash(body).as_bytes().as_slice() != dig {
        return Err("file digest mismatch".into());
    }
    if &body[0..8] != CKPT_MAGIC || u16::from_le_bytes([body[8], body[9]]) != CKPT_VERSION {
        return Err("unknown checkpoint format".into());
    }
    if body[10..26] != manifest.timeline_id || body[26..58] != manifest.config_fingerprint {
        return Err("checkpoint of another timeline or configuration".into());
    }
    let u64at = |o: usize| u64::from_le_bytes(body[o..o + 8].try_into().unwrap_or([0; 8]));
    let mut state_digest = [0u8; 32];
    state_digest.copy_from_slice(&body[90..122]);
    let h = CheckpointHeader {
        operation_seq: u64at(58),
        event_seq_high: u64at(66),
        boot_generation: u64at(74),
        last_logical_time_ns: u64at(82) as i64,
        state_digest,
    };
    if h.operation_seq != file_op {
        return Err("file name and header disagree".into());
    }
    let blob_len = u32::from_le_bytes(body[122..126].try_into().unwrap_or([0; 4])) as usize;
    let ring_n = u32::from_le_bytes(body[126..130].try_into().unwrap_or([0; 4])) as usize;
    let need = blob_len.checked_add(ring_n.checked_mul(envelope::EVENT_BYTES).ok_or("ring count overflow")?);
    if need.and_then(|n| n.checked_add(FIXED)) != Some(body.len()) {
        return Err("length fields disagree with the file".into());
    }
    let cp = decode_checkpoint(&body[FIXED..FIXED + blob_len]).map_err(|e| format!("decode: {e}"))?;
    let mut ring = Vec::with_capacity(ring_n);
    for c in body[FIXED + blob_len..].chunks_exact(envelope::EVENT_BYTES) {
        ring.push(get_event(c).ok_or("bad ring event")?);
    }
    Ok((h, cp, ring))
}

/// Where the journal stands after recovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct Position {
    pub durable_op: u64,
    pub durable_event: u64,
    pub last_boot_generation: u64,
    pub last_logical_time_ns: i64,
}

#[derive(Debug)]
pub struct Recovered {
    pub kernel: Kernel,
    pub report: RecoveryReport,
    pub position: Position,
}

/// Rebuild the kernel: newest valid checkpoint (older ones as fallback, then genesis),
/// then a verified replay of every durable record after it. Fail-closed.
pub fn recover(store: &OwalStore, wal: &Wal, config: Arc<ValidatedConfig>) -> Result<Recovered, JournalError> {
    let t0 = Instant::now();
    let durable_op = wal.durable_lsn().0;
    if store.open_report.oldest_lsn != Lsn::FIRST {
        // Retention is disabled in the spike: a log that no longer starts at 1 was
        // truncated by someone else.
        return Err(JournalError::Corrupt(format!(
            "open-wal log starts at LSN {} (retention is disabled in EXP-001)",
            store.open_report.oldest_lsn
        )));
    }
    let mut report = RecoveryReport { durable_op, ..RecoveryReport::default() };
    let mut pos = Position { durable_op, ..Position::default() };
    let cdir = OwalStore::checkpoint_dir(&store.dir);
    let mut cps = list_checkpoints(&cdir)?;
    cps.sort_unstable_by(|a, b| b.cmp(a));
    let mut kernel = None;
    for op in cps {
        let r = read_checkpoint(&cdir.join(ckpt_name(op)), &store.manifest, op).and_then(|(h, cp, ring)| {
            if !cp.recoverable {
                return Err("checkpoint of a non-recoverable kernel".into());
            }
            if cp.last_operation_sequence != h.operation_seq {
                return Err("blob and header disagree on the operation".into());
            }
            if h.operation_seq > durable_op {
                return Err(format!("checkpoint at {} beyond the durable log ({durable_op})", h.operation_seq));
            }
            let want = Kernel::checkpoint_ring_window(&config, &cp);
            let have = ring.first().zip(ring.last()).map(|(a, b)| (a.event_seq, b.event_seq));
            if want != have {
                return Err(format!("ring window {want:?} but the file holds {have:?}"));
            }
            let k = Kernel::from_checkpoint(config.clone(), &cp, &ring).map_err(|e| format!("restore: {e}"))?;
            if k.state_digest() != h.state_digest {
                return Err("state digest after restore differs from the recorded one".into());
            }
            Ok((h, k))
        });
        match r {
            Ok((h, k)) => {
                report.checkpoint_op = Some(op);
                pos.durable_event = h.event_seq_high;
                pos.last_boot_generation = h.boot_generation;
                pos.last_logical_time_ns = h.last_logical_time_ns;
                kernel = Some(k);
                break;
            }
            Err(e) => report.checkpoints_rejected.push(format!("checkpoint at operation {op}: {e}")),
        }
    }
    let mut k = kernel.unwrap_or_else(|| Kernel::new_recoverable(config.clone()));
    if report.checkpoint_op.is_none() {
        pos.last_logical_time_ns = i64::MIN;
    }
    let from = k.last_operation_sequence() + 1;
    if from <= durable_op {
        let mut rd = wal.reader_from(Lsn(from)).map_err(map_wal)?;
        while let Some(item) = rd.next() {
            let (lsn, payload) = item.map_err(map_wal)?;
            let seq = lsn.0;
            let expected = k.last_operation_sequence() + 1;
            if seq != expected {
                return Err(JournalError::Corrupt(format!("LSN {seq}: expected operation {expected}")));
            }
            let d = envelope::decode(payload).map_err(|e| JournalError::Corrupt(format!("LSN {seq}: {e}")))?;
            if d.timeline_id != store.manifest.timeline_id {
                return Err(JournalError::Corrupt(format!("LSN {seq}: record of another timeline")));
            }
            if d.record.operation_seq != seq {
                return Err(JournalError::Corrupt(format!(
                    "LSN {seq} holds operation {} (LSN↔operation mapping broken)",
                    d.record.operation_seq
                )));
            }
            let before = k.outbox_status().map_or(1, |s| s.next_event_seq);
            match k.apply_ref(&SequencedInput { operation_sequence: seq, operation: d.record.operation }) {
                Ok(TransitionOutcome::Applied { operation_sequence, .. }) if operation_sequence == seq => {}
                other => return Err(JournalError::Corrupt(format!("operation {seq}: replay diverged ({other:?})"))),
            }
            let after = k.outbox_status().map_or(1, |s| s.next_event_seq);
            let produced = after - before;
            if produced != d.record.events.len() as u64 {
                return Err(JournalError::Corrupt(format!(
                    "operation {seq}: replay produced {produced} events, the record holds {}",
                    d.record.events.len()
                )));
            }
            for (i, s) in (before..after).enumerate() {
                let got = k.alarm_event(s);
                if got.as_ref() != d.record.events.get(i) {
                    return Err(JournalError::Corrupt(format!(
                        "event {s}: replay {got:?} differs from the record {:?}",
                        d.record.events.get(i)
                    )));
                }
            }
            if produced > 0 {
                pos.durable_event = after - 1;
            }
            pos.last_boot_generation = pos.last_boot_generation.max(d.boot_generation);
            pos.last_logical_time_ns = pos.last_logical_time_ns.max(d.record.time.logical_time_ns);
            report.replayed_operations += 1;
            report.replayed_events += produced;
        }
    }
    if k.last_operation_sequence() != durable_op {
        return Err(JournalError::Corrupt(format!(
            "replay ended at operation {} but the durable log holds {durable_op}",
            k.last_operation_sequence()
        )));
    }
    let next_ev = k.outbox_status().map_or(1, |s| s.next_event_seq);
    if next_ev - 1 != pos.durable_event {
        return Err(JournalError::Corrupt(format!(
            "kernel event sequence {} differs from the logged {}",
            next_ev - 1,
            pos.durable_event
        )));
    }
    report.durable_event = pos.durable_event;
    report.duration = t0.elapsed();
    Ok(Recovered { kernel: k, report, position: pos })
}

/// Time to wait between polls in helpers.
pub const POLL: Duration = Duration::from_micros(200);
