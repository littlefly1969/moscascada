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
{auth}channels = [
  {{ channel = 1, tag = 101, type = "bool" }},
  {{ channel = 2, tag = 102, type = "bool" }},
  {{ channel = 3, tag = 103, type = "i64" }},
]

[[sources]]
id = 2001
name = "restricted"
authorized_uids = [4294967294]
channels = [ {{ channel = 1, tag = 201, type = "i64" }} ]
{load}"#,
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
    )
}

pub struct KernelProc {
    pub child: Child,
    pub socket: PathBuf,
    pub intro: PathBuf,
    pub instance: String,
    pub dir: tempfile::TempDir,
    stderr: Option<std::thread::JoinHandle<String>>,
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
        KernelProc { child, socket, intro, instance, dir, stderr: Some(stderr) }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    pub fn introspect(&self) -> IntrospectResponse {
        let mut s = UnixStream::connect(&self.intro).expect("introspection socket");
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
