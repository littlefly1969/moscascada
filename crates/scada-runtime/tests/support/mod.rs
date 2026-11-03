#![allow(dead_code)]
//! Multi-process test harness: spawns the real `scada-kernel` and `scada-sim-driver`
//! binaries and talks to them over Unix sockets. Nothing runs in-process with the kernel.

use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use scada_protocol::frame;
use scada_protocol::introspect_v1::{IntrospectRequest, IntrospectResponse, Quality, TagState};

pub fn kernel_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_scada-kernel"))
}

/// The simulator binary lives next to the kernel binary in the same target directory.
pub fn sim_bin() -> PathBuf {
    let p = kernel_bin().with_file_name("scada-sim-driver");
    assert!(p.exists(), "{} missing: run `cargo test --workspace` (or `cargo build -p scada-sim-driver`)", p.display());
    p
}

pub struct Limits {
    pub max_frame_bytes: u32,
    pub max_driver_connections: u32,
    pub input_queue_capacity: u32,
    pub max_inflight_per_connection: u32,
    pub handshake_timeout_ms: u64,
    pub frame_timeout_ms: u64,
    pub ack_write_timeout_ms: u64,
    pub read_buffer_bytes: u32,
    pub ack_coalesce_max: u32,
    /// VS-003 `[ipc]` ingress lines (empty = `fifo`). Default from the environment
    /// variable `SCADA_TEST_INGRESS` (`source_fair:4`, `source_fair:1`, ...), so that the
    /// whole VS-001/VS-002 suite can be run under every policy.
    pub ingress: String,
    /// VS-004: `[runtime]` lines (e.g. `watchdog_poll_ms = 20`).
    pub runtime: String,
    /// VS-004: extra lines of source 1001 (e.g. `liveness_timeout_ms = 150`). Default from
    /// `SCADA_TEST_SUPERVISION=live_ms:fresh_ms` (with a 100 ms watchdog), so that the whole
    /// earlier suite can also run with supervision enabled on that source.
    pub source_1001: String,
    /// VS-005: `[alarms]` section (`None` = alarms disabled, the default). Default from
    /// `SCADA_TEST_ALARMS=1`, so that the whole earlier suite also runs with alarms enabled
    /// (rules on every tag, an outbox large enough never to refuse).
    pub alarms: Option<Alarms>,
    /// VS-006: `Some(extra lines)` adds `[persistence]` with the store at
    /// `<dir>/journal/kernel.db` and the historian at `<dir>/history.sock` (current uid);
    /// the extra lines may also open a `[time]` section. Default from
    /// `SCADA_TEST_PERSISTENCE=1`, so that the whole earlier suite also runs with the
    /// durable journal enabled.
    pub persistence: Option<String>,
}

/// `SCADA_TEST_PERSISTENCE=1` -> journal with default settings except a queue large enough
/// for the pipelined load tests (journal backpressure itself is tested in vs006.rs).
pub fn persistence_from_env() -> Option<String> {
    match std::env::var("SCADA_TEST_PERSISTENCE") {
        Ok(v) if v == "1" => Some("journal_queue_operations = 262144\n".into()),
        _ => None,
    }
}

/// Kernel for replaying a capture of a kernel started with `cfg`: a journaling kernel is
/// recoverable (its own digest version, ADR 0013 D3).
pub fn replay_kernel(cfg: &scada_runtime::config::RuntimeConfig) -> scada_core::Kernel {
    let c = std::sync::Arc::new(cfg.core.clone());
    if cfg.persistence.is_some() { scada_core::Kernel::new_recoverable(c) } else { scada_core::Kernel::new(c) }
}

/// `[persistence]` section of the test configuration.
pub fn persistence_section(dir: &Path, extra: &str) -> String {
    format!(
        "\n[persistence]\npath = \"{}\"\nhistory_socket_path = \"{}\"\nhistory_authorized_uids = [{}]\n{extra}\n",
        dir.join("journal").join("kernel.db").display(),
        dir.join("history.sock").display(),
        current_uid()
    )
}

/// VS-005 `[alarms]` of the test configuration. The control socket is `<dir>/alarm.sock`.
#[derive(Clone, Debug)]
pub struct Alarms {
    pub capacity: u64,
    /// `None` = only the current uid.
    pub authorized_uids: Option<Vec<u32>>,
    /// Extra `[alarms]` lines (e.g. `control_page_max = 4`).
    pub extra: String,
    /// `[[alarms.rules]]` blocks.
    pub rules: String,
    /// No control socket at all.
    pub no_socket: bool,
}

/// One `[[alarms.rules]]` block.
pub fn rule(id: u32, tag: u32, kind: &str, params: &str, severity: &str) -> String {
    format!(
        "[[alarms.rules]]\nid = {id}\nname = \"a{id}\"\ntag_id = {tag}\nkind = \"{kind}\"\n{params}severity = \"{severity}\"\n"
    )
}

/// Rules on every tag of the demo configuration (used by `SCADA_TEST_ALARMS=1`).
pub fn demo_rules() -> String {
    let mut r = rule(1, 101, "bool_equals", "expected = true\n", "major");
    r.push_str(&rule(2, 102, "bool_equals", "expected = true\n", "critical"));
    r.push_str(&rule(3, 103, "high", "trip = 3000\nreset = 2900\n", "warning"));
    r.push_str(&rule(4, 103, "bad_quality", "", "info"));
    r.push_str(&rule(5, 101, "bad_quality", "", "info"));
    r.push_str(&rule(6, 201, "bad_quality", "", "info"));
    for n in 1..=16 {
        r.push_str(&rule(100 + n, 300 + n, "low", "trip = -1000\nreset = -900\n", "info"));
    }
    r
}

pub fn alarms_from_env() -> Option<Alarms> {
    match std::env::var("SCADA_TEST_ALARMS").ok().as_deref() {
        Some("1") => Some(Alarms {
            // Without a journal nothing releases the slots, so the ring must hold every
            // event of a test. With SCADA_TEST_PERSISTENCE=1 the commits release them: a
            // 64 Ki ring suffices and keeps the RSS assertions of VS-002 meaningful.
            capacity: if persistence_from_env().is_some() { 1 << 16 } else { 1 << 20 },
            authorized_uids: None,
            extra: String::new(),
            rules: demo_rules(),
            no_socket: false,
        }),
        _ => None,
    }
}

impl Alarms {
    pub fn with(capacity: u64, rules: String) -> Alarms {
        Alarms { capacity, authorized_uids: None, extra: String::new(), rules, no_socket: false }
    }

    fn section(&self, dir: &Path) -> String {
        let uids = self.authorized_uids.clone().unwrap_or_else(|| vec![current_uid()]);
        let sock = if self.no_socket {
            String::new()
        } else {
            format!(
                "control_socket_path = \"{}\"\ncontrol_authorized_uids = [{}]\n",
                dir.join("alarm.sock").display(),
                uids.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", ")
            )
        };
        format!("\n[alarms]\noutbox_capacity_events = {}\n{sock}{}\n{}", self.capacity, self.extra, self.rules)
    }
}

/// `SCADA_TEST_SUPERVISION=live_ms:fresh_ms` -> (`[runtime]` lines, source lines).
pub fn supervision_from_env() -> (String, String) {
    match std::env::var("SCADA_TEST_SUPERVISION") {
        Ok(v) if !v.is_empty() => {
            let (l, f) = v.split_once(':').expect("live_ms:fresh_ms");
            ("watchdog_poll_ms = 100\n".into(), format!("liveness_timeout_ms = {l}\nfreshness_timeout_ms = {f}\n"))
        }
        _ => (String::new(), String::new()),
    }
}

/// `SCADA_TEST_INGRESS=policy:limit` -> `[ipc]` lines.
pub fn ingress_from_env() -> String {
    match std::env::var("SCADA_TEST_INGRESS") {
        Ok(v) if !v.is_empty() && v != "fifo" => ingress_lines(&v),
        _ => String::new(),
    }
}

/// `"source_fair:4"` -> the two `[ipc]` lines.
pub fn ingress_lines(spec: &str) -> String {
    let (p, l) = spec.split_once(':').expect("policy:limit");
    format!("ingress_policy = \"{p}\"\nper_source_pending_limit = {l}\n")
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_frame_bytes: 65_536,
            max_driver_connections: 16,
            input_queue_capacity: 64,
            max_inflight_per_connection: 32,
            handshake_timeout_ms: 2_000,
            frame_timeout_ms: 2_000,
            ack_write_timeout_ms: 5_000,
            // Product defaults (VS-002).
            read_buffer_bytes: 8_192,
            ack_coalesce_max: 32,
            ingress: ingress_from_env(),
            runtime: supervision_from_env().0,
            source_1001: supervision_from_env().1,
            alarms: alarms_from_env(),
            persistence: persistence_from_env(),
        }
    }
}

pub fn demo_config(dir: &Path, l: &Limits, authorized_uids: Option<&[u32]>) -> String {
    let auth = authorized_uids.map_or(String::new(), |u| {
        format!("authorized_uids = [{}]\n", u.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", "))
    });
    format!(
        r#"[ipc]
socket_path = "{sock}"
max_frame_bytes = {mf}
max_driver_connections = {mc}
input_queue_capacity = {q}
max_inflight_per_connection = {inf}
handshake_timeout_ms = {ht}
frame_timeout_ms = {ft}
ack_write_timeout_ms = {wt}
read_buffer_bytes = {rb}
ack_coalesce_max = {co}
{ingress}
[runtime]
{runtime}
[[tags]]
id = 101
name = "door_contact"
type = "bool"
[[tags]]
id = 102
name = "tamper_switch"
type = "bool"
[[tags]]
id = 103
name = "case_temperature_centi_c"
type = "i64"
[[tags]]
id = 201
name = "aux_counter"
type = "i64"

[[sources]]
id = 1001
name = "sim-enclosure-1"
{auth}{sup}channels = [
  {{ channel = 1, tag = 101, type = "bool" }},
  {{ channel = 2, tag = 102, type = "bool" }},
  {{ channel = 3, tag = 103, type = "i64" }},
]

[[sources]]
id = 2001
name = "restricted"
authorized_uids = [4294967294]
channels = [ {{ channel = 1, tag = 201, type = "i64" }} ]
{load}{alarms}{persistence}"#,
        load = (1..=16)
            .map(|n| format!(
                "\n[[tags]]\nid = {t}\nname = \"load_{n}\"\ntype = \"i64\"\n\n[[sources]]\nid = {s}\nname = \"load-{n}\"\nchannels = [ {{ channel = 1, tag = {t}, type = \"i64\" }} ]\n",
                t = 300 + n,
                s = 3000 + n
            ))
            .collect::<String>(),
        sock = dir.join("driver.sock").display(),
        mf = l.max_frame_bytes,
        mc = l.max_driver_connections,
        q = l.input_queue_capacity,
        inf = l.max_inflight_per_connection,
        ht = l.handshake_timeout_ms,
        ft = l.frame_timeout_ms,
        wt = l.ack_write_timeout_ms,
        rb = l.read_buffer_bytes,
        co = l.ack_coalesce_max,
        ingress = l.ingress,
        runtime = l.runtime,
        sup = l.source_1001,
        alarms = l.alarms.as_ref().map_or(String::new(), |a| a.section(dir)),
        persistence = l.persistence.as_ref().map_or(String::new(), |e| persistence_section(dir, e)),
    )
}

pub struct KernelProc {
    pub child: Child,
    pub socket: PathBuf,
    pub intro: PathBuf,
    /// VS-005: `<dir>/alarm.sock` (exists only when alarms are configured with a socket).
    pub alarm: PathBuf,
    pub instance: String,
    pub dir: tempfile::TempDir,
    stderr: Option<std::thread::JoinHandle<String>>,
    /// The READY line.
    pub ready: String,
}

impl KernelProc {
    pub fn start(limits: &Limits) -> KernelProc {
        Self::start_with(limits, None, &[])
    }

    pub fn start_with(limits: &Limits, uids: Option<&[u32]>, extra: &[&str]) -> KernelProc {
        let dir = tempfile::Builder::new().prefix("scada-it").tempdir().unwrap();
        Self::start_in(dir, limits, uids, extra)
    }

    pub fn start_in(dir: tempfile::TempDir, limits: &Limits, uids: Option<&[u32]>, extra: &[&str]) -> KernelProc {
        let cfg = dir.path().join("kernel.toml");
        std::fs::write(&cfg, demo_config(dir.path(), limits, uids)).unwrap();
        let socket = dir.path().join("driver.sock");
        let intro = dir.path().join("intro.sock");
        let mut child = Command::new(kernel_bin())
            .arg("--config")
            .arg(&cfg)
            .arg("--introspection-socket")
            .arg(&intro)
            .args(scheduler_args_from_env(extra))
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                let _ = tx.send(line);
            }
        });
        let stderr = child.stderr.take().unwrap();
        let stderr = std::thread::spawn(move || {
            let mut s = String::new();
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                s.push_str(&line);
                s.push('\n');
            }
            s
        });
        let line = rx.recv_timeout(Duration::from_secs(10)).expect("kernel did not print READY");
        assert!(line.starts_with("READY instance="), "{line}");
        let instance = line["READY instance=".len()..].split(' ').next().unwrap().to_owned();
        let alarm = dir.path().join("alarm.sock");
        KernelProc { child, socket, intro, alarm, instance, dir, stderr: Some(stderr), ready: line }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    pub fn introspect(&self) -> IntrospectResponse {
        introspect_at(&self.intro)
    }
}

/// One introspection call on `path` (any kernel started with an introspection socket).
pub fn introspect_at(path: &Path) -> IntrospectResponse {
    {
        let mut s = UnixStream::connect(path).expect("introspection socket");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let mut w = Vec::new();
        frame::write_message(
            &mut s,
            &IntrospectRequest { include_tags: true, introspect_version: 1, ..Default::default() },
            1024,
            &mut w,
        )
        .unwrap();
        let mut r = Vec::new();
        frame::read_message(&mut s, frame::MAX_FRAME_BYTES_CEILING, &mut r).unwrap().expect("response")
    }
}

impl KernelProc {
    /// Poll the introspection until `pred` holds (state changes are asynchronous to the test).
    pub fn wait_for(&self, what: &str, pred: impl Fn(&IntrospectResponse) -> bool) -> IntrospectResponse {
        let start = Instant::now();
        loop {
            let r = self.introspect();
            if pred(&r) {
                return r;
            }
            assert!(start.elapsed() < Duration::from_secs(10), "timeout waiting for {what}: {r:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Resident set size of the kernel process in KiB (from /proc).
    pub fn rss_kib(&self) -> u64 {
        proc_status(self.pid(), "VmRSS:")
    }

    pub fn threads(&self) -> u64 {
        proc_status(self.pid(), "Threads:")
    }

    /// VS-006: SIGKILL, keep the directory (store) for a restart: (stderr, dir).
    pub fn kill_keep(mut self) -> (String, tempfile::TempDir) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let err = self.stderr.take().map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        (err, self.take_dir())
    }

    /// VS-006: wait for the process to end by itself (e.g. an injected fault aborts it);
    /// returns (exit status, stderr, dir).
    pub fn wait_exit(mut self, timeout: Duration) -> (std::process::ExitStatus, String, tempfile::TempDir) {
        let start = Instant::now();
        let status = loop {
            if let Some(st) = self.child.try_wait().unwrap() {
                break st;
            }
            if start.elapsed() > timeout {
                let _ = self.child.kill();
                break self.child.wait().unwrap();
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let err = self.stderr.take().map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        (status, err, self.take_dir())
    }

    /// VS-006: SIGTERM (graceful), keep the directory: (stderr, dir).
    pub fn stop_keep(mut self) -> (String, tempfile::TempDir) {
        unsafe_free_sigterm(self.child.id());
        let status = self.child.wait().unwrap();
        let err = self.stderr.take().map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        assert!(status.success(), "kernel exit {status:?}\n{err}");
        assert!(!err.contains("panicked"), "kernel panicked:\n{err}");
        (err, self.take_dir())
    }

    fn take_dir(&mut self) -> tempfile::TempDir {
        let placeholder = tempfile::Builder::new().prefix("scada-it-empty").tempdir().unwrap();
        std::mem::replace(&mut self.dir, placeholder)
    }

    /// SIGKILL and collect stderr.
    pub fn kill(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stderr.take().map(|h| h.join().unwrap_or_default()).unwrap_or_default()
    }

    /// SIGTERM, wait, return (exit success, stderr). Checks the kernel did not panic.
    pub fn stop(mut self) -> String {
        unsafe_free_sigterm(self.child.id());
        let status = self.child.wait().unwrap();
        let err = self.stderr.take().map(|h| h.join().unwrap_or_default()).unwrap_or_default();
        assert!(status.success(), "kernel exit {status:?}\n{err}");
        assert!(!err.contains("panicked"), "kernel panicked:\n{err}");
        err
    }
}

/// `SCADA_TEST_SCHEDULER=current_thread|multi_thread:N` runs every multi-process test with
/// that scheduler (CI runs the suite with `current_thread` and `multi_thread:2`). Tests that
/// choose a scheduler explicitly (`--scheduler` in `extra`) are left alone.
pub fn scheduler_args_from_env(extra: &[&str]) -> Vec<String> {
    if extra.contains(&"--scheduler") {
        return Vec::new();
    }
    match std::env::var("SCADA_TEST_SCHEDULER").ok().as_deref() {
        None | Some("") | Some("current_thread") => Vec::new(),
        Some(v) => {
            let n = v.strip_prefix("multi_thread:").expect("SCADA_TEST_SCHEDULER=current_thread|multi_thread:N");
            vec!["--scheduler".into(), "multi_thread".into(), "--worker-threads".into(), n.into()]
        }
    }
}

/// Scheduler arguments for an explicit mode: `None` = current_thread, `Some(n)` = multi_thread(n).
pub fn scheduler_args(workers: Option<u32>) -> Vec<String> {
    match workers {
        None => vec!["--scheduler".into(), "current_thread".into()],
        Some(n) => vec!["--scheduler".into(), "multi_thread".into(), "--worker-threads".into(), n.to_string()],
    }
}

/// Send SIGTERM through the `kill` utility (keeps the tests free of `unsafe`/libc).
pub fn unsafe_free_sigterm(pid: u32) {
    let ok = Command::new("kill").arg("-TERM").arg(pid.to_string()).status().unwrap().success();
    assert!(ok);
}

pub fn proc_status(pid: u32, key: &str) -> u64 {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with(key))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

pub fn current_uid() -> u32 {
    use std::os::unix::fs::MetadataExt;
    // Files we create are owned by our effective uid.
    let f = tempfile::NamedTempFile::new().unwrap();
    f.as_file().metadata().unwrap().uid()
}

pub fn tag(r: &IntrospectResponse, id: u32) -> TagState {
    r.tags.iter().find(|t| t.tag_id == id).cloned().expect("tag")
}

pub fn quality(t: &TagState) -> Quality {
    Quality::try_from(t.quality).unwrap()
}
