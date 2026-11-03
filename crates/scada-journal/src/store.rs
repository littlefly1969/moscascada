//! Opening, locking and validating the store (ADR 0013 D1, D3, D8). Everything here is
//! fail-closed: an unknown schema, another application's file, another configuration, a
//! failed integrity check or a second writer stop the startup with an explicit error.

use std::fs::{File, OpenOptions, TryLockError};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

use crate::schema::{APPLICATION_ID, DDL, SCHEMA_VERSION};
use crate::{Durability, JournalError, realtime_ns, sq, uq};

/// Integrity check at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupCheck {
    None,
    Quick,
    Full,
}

#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub path: PathBuf,
    pub durability: Durability,
    pub startup_check: StartupCheck,
    /// SQLite page cache of the writer connection, KiB.
    pub cache_kib: u32,
    /// TEST-ONLY: `PRAGMA max_page_count` (makes `SQLITE_FULL` reproducible).
    pub test_max_pages: Option<u32>,
}

/// Identity and position of the open store.
#[derive(Debug, Clone)]
pub struct StoreInfo {
    pub path: PathBuf,
    pub store_uuid: [u8; 16],
    pub timeline_no: i64,
    pub timeline_id: [u8; 16],
    pub created: bool,
    /// Boot generation of the **previous** boot (0 for a new store); the current one is
    /// assigned by [`Store::begin_boot`].
    pub last_boot_generation: u64,
    pub last_logical_time_ns: i64,
}

/// The open store: writer connection plus the exclusive lock (held until dropped).
#[derive(Debug)]
pub struct Store {
    pub conn: Connection,
    pub info: StoreInfo,
    _lock: File,
}

fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}

/// The store directory must be private (same rules as the socket directories).
pub fn check_private_dir(dir: &Path) -> Result<(), JournalError> {
    let m = std::fs::symlink_metadata(dir).map_err(|e| JournalError::Io(format!("{}: {e}", dir.display())))?;
    let deny = |why: String| Err(JournalError::Io(format!("journal directory {}: {why}", dir.display())));
    if m.file_type().is_symlink() {
        return deny("is a symbolic link".into());
    }
    if !m.is_dir() {
        return deny("is not a directory".into());
    }
    let me = effective_uid();
    if m.uid() != me && m.uid() != 0 {
        return deny(format!("owned by uid {}, not by this user ({me}) or root", m.uid()));
    }
    if m.mode() & 0o022 != 0 {
        return deny(format!("mode {:o} is writable by group or others", m.mode() & 0o7777));
    }
    Ok(())
}

fn parent(path: &Path) -> PathBuf {
    match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".lock");
    PathBuf::from(p)
}

/// Take the single-writer lock (`<path>.lock`, `flock` exclusive, non-blocking).
pub fn take_lock(path: &Path) -> Result<File, JournalError> {
    let lp = lock_path(path);
    let f = OpenOptions::new().read(true).write(true).create(true).truncate(false).mode(0o600).open(&lp)?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(TryLockError::WouldBlock) => {
            Err(JournalError::Locked(format!("{} is held by another process (one writer per store)", lp.display())))
        }
        Err(TryLockError::Error(e)) => Err(JournalError::Io(format!("{}: {e}", lp.display()))),
    }
}

fn pragma_i64(c: &Connection, name: &str) -> Result<i64, JournalError> {
    Ok(c.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))?)
}

/// Apply the connection pragmas of the writer and verify they took effect.
fn configure(c: &Connection, o: &StoreOptions) -> Result<(), JournalError> {
    let mode: String = c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(JournalError::Incompatible(format!("journal_mode WAL refused (got {mode})")));
    }
    let want = match o.durability {
        Durability::Full => 2,
        Durability::NormalBenchmarkOnly => 1,
    };
    c.execute_batch(&format!(
        "PRAGMA synchronous={want}; PRAGMA foreign_keys=ON; PRAGMA cache_size=-{};",
        o.cache_kib.max(256)
    ))?;
    let got = pragma_i64(c, "synchronous")?;
    if got != want {
        return Err(JournalError::Incompatible(format!("PRAGMA synchronous={want} not applied (got {got})")));
    }
    if let Some(p) = o.test_max_pages {
        let _: i64 = c.query_row(&format!("PRAGMA max_page_count={p}"), [], |r| r.get(0))?;
    }
    Ok(())
}

/// Run the configured integrity check; anything but `ok` is corruption.
pub fn integrity(c: &Connection, check: StartupCheck) -> Result<(), JournalError> {
    let sql = match check {
        StartupCheck::None => return Ok(()),
        StartupCheck::Quick => "PRAGMA quick_check",
        StartupCheck::Full => "PRAGMA integrity_check",
    };
    let mut st = c.prepare(sql)?;
    let rows: Vec<String> = st.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
    if rows.len() == 1 && rows.first().is_some_and(|r| r == "ok") {
        Ok(())
    } else {
        Err(JournalError::Corrupt(format!("{sql}: {}", rows.join("; "))))
    }
}

impl Store {
    /// Open (or create) the store for a kernel whose configuration has `fingerprint`.
    pub fn open(o: &StoreOptions, fingerprint: [u8; 32]) -> Result<Store, JournalError> {
        let dir = parent(&o.path);
        if !dir.exists() {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir)?;
        }
        check_private_dir(&dir)?;
        let lock = take_lock(&o.path)?;
        match std::fs::symlink_metadata(&o.path) {
            Ok(m) if m.file_type().is_symlink() || !m.is_file() => {
                return Err(JournalError::Io(format!("{} is not a regular file", o.path.display())));
            }
            Ok(m) if m.uid() != effective_uid() => {
                return Err(JournalError::Io(format!("{} is owned by uid {}", o.path.display(), m.uid())));
            }
            Ok(_) => {}
            // Pre-created with 0600: SQLite gives the -wal/-shm files the same mode.
            Err(_) => {
                OpenOptions::new().write(true).create_new(true).mode(0o600).open(&o.path)?;
            }
        }
        let conn = Connection::open_with_flags(
            &o.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
        )?;
        let app = pragma_i64(&conn, "application_id")?;
        let ver = pragma_i64(&conn, "user_version")?;
        let tables: i64 = conn.query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))?;
        let created = if app == 0 && ver == 0 && tables == 0 {
            true
        } else if app != i64::from(APPLICATION_ID) {
            return Err(JournalError::Incompatible(format!(
                "{} is not a MoscaSCADA journal (application_id {app:#x})",
                o.path.display()
            )));
        } else if ver != i64::from(SCHEMA_VERSION) {
            return Err(JournalError::Incompatible(format!(
                "schema version {ver} not supported by this build (expects {SCHEMA_VERSION}); no automatic migration"
            )));
        } else {
            false
        };
        configure(&conn, o)?;
        if created {
            create_schema(&conn, fingerprint)?;
        } else {
            integrity(&conn, o.startup_check)?;
        }
        let info = read_info(&conn, &o.path, created)?;
        let fp: Vec<u8> = conn.query_row(
            "SELECT config_fingerprint FROM timelines WHERE timeline_no = ?1",
            params![info.timeline_no],
            |r| r.get(0),
        )?;
        if fp.as_slice() != fingerprint.as_slice() {
            return Err(JournalError::Incompatible(format!(
                "the current timeline of {} was created for configuration {} but this configuration is {}; \
                 the restored state would be reinterpreted. Start a new timeline with the offline tool \
                 (`scada-journal new-timeline --confirm`) or use the matching configuration",
                o.path.display(),
                crate::hex(&fp),
                crate::hex(&fingerprint)
            )));
        }
        Ok(Store { conn, info, _lock: lock })
    }

    /// Register the boot that is starting (one committed transaction). Returns its
    /// generation (previous + 1).
    #[allow(clippy::too_many_arguments)]
    pub fn begin_boot(
        &mut self,
        kernel_instance_id: [u8; 16],
        os_boot_id: &str,
        first_operation_seq: u64,
        recovered_from_checkpoint: Option<u64>,
        replayed_operations: u64,
        recovery_duration_us: u64,
    ) -> Result<u64, JournalError> {
        let g = self
            .info
            .last_boot_generation
            .checked_add(1)
            .ok_or(JournalError::Corrupt("boot generation overflow".into()))?;
        let tx = self.conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("UPDATE schema_meta SET boot_generation = ?1 WHERE id = 1", params![sq(g)])?;
        tx.execute(
            "INSERT INTO runtime_boots VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                self.info.timeline_no,
                sq(g),
                kernel_instance_id.as_slice(),
                os_boot_id,
                sq(first_operation_seq),
                realtime_ns(),
                recovered_from_checkpoint.map(sq),
                sq(replayed_operations),
                sq(recovery_duration_us)
            ],
        )?;
        tx.commit()?;
        self.info.last_boot_generation = g;
        Ok(g)
    }
}

fn create_schema(c: &Connection, fingerprint: [u8; 32]) -> Result<(), JournalError> {
    let uuid = crate::random_id()?;
    let tl = crate::random_id()?;
    let now = realtime_ns();
    c.execute_batch("BEGIN IMMEDIATE")?;
    let r = (|| -> Result<(), JournalError> {
        c.execute_batch(DDL)?;
        c.execute(
            "INSERT INTO timelines VALUES (1, ?1, ?2, ?3, 'created', NULL)",
            params![tl.as_slice(), now, fingerprint.as_slice()],
        )?;
        c.execute(
            "INSERT INTO schema_meta VALUES (1, ?1, ?2, 1, 0, ?3, ?4)",
            params![SCHEMA_VERSION, uuid.as_slice(), i64::MIN, now],
        )?;
        c.execute("INSERT INTO projection_meta VALUES (1, 0, 0)", [])?;
        c.execute_batch(&format!("PRAGMA application_id={APPLICATION_ID}; PRAGMA user_version={SCHEMA_VERSION};"))?;
        Ok(())
    })();
    match r {
        Ok(()) => {
            c.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = c.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn blob16(v: Vec<u8>) -> Result<[u8; 16], JournalError> {
    <[u8; 16]>::try_from(v.as_slice())
        .map_err(|_| JournalError::Corrupt("16-byte identifier has another length".into()))
}

fn read_info(c: &Connection, path: &Path, created: bool) -> Result<StoreInfo, JournalError> {
    let (schema, uuid, tl, boot, logical): (i64, Vec<u8>, i64, i64, i64) = c
        .query_row(
            "SELECT schema_version, store_uuid, current_timeline, boot_generation, last_logical_time_ns FROM schema_meta WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?
        .ok_or_else(|| JournalError::Corrupt("schema_meta row missing".into()))?;
    if schema != i64::from(SCHEMA_VERSION) {
        return Err(JournalError::Incompatible(format!("schema_meta.schema_version {schema}")));
    }
    let tid: Vec<u8> = c
        .query_row("SELECT timeline_id FROM timelines WHERE timeline_no = ?1", params![tl], |r| r.get(0))
        .optional()?
        .ok_or_else(|| JournalError::Corrupt(format!("current timeline {tl} missing")))?;
    Ok(StoreInfo {
        path: path.to_path_buf(),
        store_uuid: blob16(uuid)?,
        timeline_no: tl,
        timeline_id: blob16(tid)?,
        created,
        last_boot_generation: uq(boot),
        last_logical_time_ns: logical,
    })
}

/// Open a read-only connection (historian, offline tool). Never takes the writer lock.
pub fn open_read_only(path: &Path, cache_kib: u32) -> Result<Connection, JournalError> {
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let app = pragma_i64(&c, "application_id")?;
    if app != i64::from(APPLICATION_ID) {
        return Err(JournalError::Incompatible(format!("{} is not a MoscaSCADA journal", path.display())));
    }
    let ver = pragma_i64(&c, "user_version")?;
    if ver != i64::from(SCHEMA_VERSION) {
        return Err(JournalError::Incompatible(format!("schema version {ver} not supported")));
    }
    c.execute_batch(&format!("PRAGMA query_only=1; PRAGMA cache_size=-{};", cache_kib.max(256)))?;
    Ok(c)
}

/// Offline only (`scada-journal new-timeline --confirm`): start a new timeline for
/// `fingerprint`. The old timeline stays readable. Requires the writer lock.
pub fn new_timeline(path: &Path, fingerprint: [u8; 32], reason: &str) -> Result<(i64, [u8; 16]), JournalError> {
    let _lock = take_lock(path)?;
    let c = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let app = pragma_i64(&c, "application_id")?;
    if app != i64::from(APPLICATION_ID) {
        return Err(JournalError::Incompatible(format!("{} is not a MoscaSCADA journal", path.display())));
    }
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    let tl = crate::random_id()?;
    let tx = c.unchecked_transaction()?;
    let prev: i64 = tx.query_row("SELECT current_timeline FROM schema_meta WHERE id = 1", [], |r| r.get(0))?;
    tx.execute(
        "INSERT INTO timelines (timeline_id, created_at_utc_ns, config_fingerprint, reason, previous_timeline) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![tl.as_slice(), realtime_ns(), fingerprint.as_slice(), reason, prev],
    )?;
    let no = tx.last_insert_rowid();
    tx.execute("UPDATE schema_meta SET current_timeline = ?1 WHERE id = 1", params![no])?;
    tx.execute("INSERT INTO projection_meta VALUES (?1, 0, 0)", params![no])?;
    tx.execute(
        "INSERT INTO retention_markers (timeline_no, table_name, from_key, to_key, reason, persisted_at_utc_ns) VALUES (?1, 'timelines', ?2, ?3, ?4, ?5)",
        params![prev, prev, no, format!("new timeline: {reason}"), realtime_ns()],
    )?;
    tx.commit()?;
    Ok((no, tl))
}
