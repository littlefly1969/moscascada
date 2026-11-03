#![allow(dead_code)]
//! End-to-end harness: the REAL kernel binary (`scada-kernel`, unchanged VS-006) and the
//! REAL driver binary as separate processes, the Modbus simulator in the test process (it
//! plays the field device). Nothing of the kernel runs in-process.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use scada_modbus::Area;
use scada_modbus_sim::{SimConfig, Simulator};
use scada_protocol::frame;
use scada_protocol::introspect_v1::{IntrospectRequest, IntrospectResponse, Quality, TagState};

pub fn driver_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_scada-driver-modbus-tcp"))
}

/// The kernel binary of the same target directory (built by `cargo test --workspace` or
/// `cargo build -p scada-runtime`). Missing => the test FAILS (never a silent skip).
pub fn kernel_bin() -> PathBuf {
    let p = driver_bin().with_file_name("scada-kernel");
    assert!(
        p.exists(),
        "{} missing: run `cargo test --workspace` or `cargo build -p scada-runtime` first (same profile)",
        p.display()
    );
    p
}

pub fn catalog_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/driver-catalog/generic-io.v1.toml")
}

pub fn current_uid() -> u32 {
    use std::os::unix::fs::MetadataExt;
    let f = tempfile::NamedTempFile::new().unwrap();
    f.as_file().metadata().unwrap().uid()
}

// ------------------------------------------------------------------ simulator

/// Simulator with the generic-io image.
pub fn sim() -> Simulator {
    let s = Simulator::start(SimConfig { units: vec![1], ..SimConfig::default() }).unwrap();
    set_image(&s, &Image::default());
    s
}

/// Values of the generic-io profile points.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Image {
    pub di_0: bool,
    pub di_1: bool,
    pub coil_0: bool,
    pub temperature_centi: i16,
    pub status_word: u16,
    pub counter: u32,
    pub level: f32,
    pub signed: i32,
}

impl Default for Image {
    fn default() -> Self {
        Image {
            di_0: false,
            di_1: true,
            coil_0: false,
            temperature_centi: 2150,
            status_word: 0x0008,
            counter: 70_000,
            level: 1.25,
            signed: -5,
        }
    }
}

pub fn set_image(s: &Simulator, i: &Image) {
    s.with_memory(|m| {
        m.discrete_inputs[0] = i.di_0;
        m.discrete_inputs[1] = i.di_1;
        m.coils[0] = i.coil_0;
        m.input[0] = i.temperature_centi as u16;
        m.holding[0] = i.status_word;
        m.set_u32(Area::InputRegister, 2, i.counter);
        m.set_u32(Area::InputRegister, 4, i.level.to_bits());
        // low_first: first register = low word.
        let v = i.signed as u32;
        m.holding[2] = v as u16;
        m.holding[3] = (v >> 16) as u16;
    });
}

/// Expected kernel values of tags 101..=109 for an image.
pub fn expected(i: &Image) -> Vec<(u32, Expect)> {
    vec![
        (101, Expect::B(i.di_0)),
        (102, Expect::B(i.di_1)),
        (103, Expect::B(i.coil_0)),
        (104, Expect::I(i64::from(i.temperature_centi))),
        (105, Expect::I(i64::from(i.status_word))),
        (106, Expect::B(i.status_word & 0x8 != 0)),
        (107, Expect::I(i64::from(i.counter))),
        (108, Expect::I((f64::from(i.level) * 1000.0).round_ties_even() as i64)),
        (109, Expect::I(i64::from(i.signed))),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    B(bool),
    I(i64),
}

pub fn tag_value(t: &TagState) -> Option<Expect> {
    use scada_protocol::introspect_v1::tag_state::Value;
    match t.value {
        Some(Value::BoolValue(b)) => Some(Expect::B(b)),
        Some(Value::I64Value(i)) => Some(Expect::I(i)),
        None => None,
    }
}

// ------------------------------------------------------------------ kernel

#[derive(Clone, Debug, Default)]
pub struct KernelOpts {
    /// `[[alarms.rules]]` blocks; `Some` enables `[alarms]`.
    pub alarm_rules: Option<String>,
    pub outbox_capacity: u64,
    /// `Some(extra lines)` enables `[persistence]`.
    pub persistence: Option<String>,
    /// Extra command-line arguments (test flags).
    pub args: Vec<String>,
    /// Extra sources: (source_id, first tag id) with the same 9 channels.
    pub extra_sources: Vec<(u32, u32)>,
}

pub const TAG_TYPES: [&str; 9] = ["bool", "bool", "bool", "i64", "i64", "bool", "i64", "i64", "i64"];

fn source_block(source: u32, first_tag: u32) -> String {
    let mut s = String::new();
    for (i, t) in TAG_TYPES.iter().enumerate() {
        s.push_str(&format!("[[tags]]\nid = {}\nname = \"s{source}_t{i}\"\ntype = \"{t}\"\n", first_tag + i as u32));
    }
    s.push_str(&format!("[[sources]]\nid = {source}\nname = \"modbus-{source}\"\nchannels = [\n"));
    for (i, t) in TAG_TYPES.iter().enumerate() {
        s.push_str(&format!("  {{ channel = {}, tag = {}, type = \"{t}\" }},\n", i + 1, first_tag + i as u32));
    }
    s.push_str("]\n");
    s
}

pub fn kernel_config(dir: &Path, o: &KernelOpts) -> String {
    let mut s = format!(
        "[ipc]\nsocket_path = \"{}\"\nmax_frame_bytes = 65536\nmax_driver_connections = 16\ninput_queue_capacity = 64\n\
         max_inflight_per_connection = 32\nhandshake_timeout_ms = 2000\nframe_timeout_ms = 2000\n\n[core]\nmax_observations_per_batch = 128\n\n",
        dir.join("driver.sock").display()
    );
    s.push_str(&source_block(1001, 101));
    for (src, tag) in &o.extra_sources {
        s.push_str(&source_block(*src, *tag));
    }
    if let Some(rules) = &o.alarm_rules {
        s.push_str(&format!(
            "\n[alarms]\noutbox_capacity_events = {}\ncontrol_socket_path = \"{}\"\ncontrol_authorized_uids = [{}]\n\n{rules}",
            o.outbox_capacity.max(4),
            dir.join("alarm.sock").display(),
            current_uid()
        ));
    }
    if let Some(extra) = &o.persistence {
        s.push_str(&format!(
            "\n[persistence]\npath = \"{}\"\nhistory_socket_path = \"{}\"\nhistory_authorized_uids = [{}]\n{extra}\n",
            dir.join("journal").join("kernel.db").display(),
            dir.join("history.sock").display(),
            current_uid()
        ));
    }
    s
}

pub fn rule(id: u32, tag: u32, kind: &str, params: &str, severity: &str) -> String {
    format!(
        "[[alarms.rules]]\nid = {id}\nname = \"a{id}\"\ntag_id = {tag}\nkind = \"{kind}\"\n{params}severity = \"{severity}\"\n"
    )
}

pub struct Kernel {
    pub child: Child,
    pub dir: PathBuf,
    pub socket: PathBuf,
    pub intro: PathBuf,
    pub alarm: PathBuf,
    pub history: PathBuf,
    pub ready: String,
    stderr: Option<std::thread::JoinHandle<String>>,
}

impl Kernel {
    /// Start in `dir` (kept by the caller, so that a restart finds the same store).
    pub fn start(dir: &Path, o: &KernelOpts) -> Kernel {
        let cfg = dir.join("kernel.toml");
        std::fs::write(&cfg, kernel_config(dir, o)).unwrap();
        let intro = dir.join("intro.sock");
        let mut child = Command::new(kernel_bin())
            .arg("--config")
            .arg(&cfg)
            .arg("--introspection-socket")
            .arg(&intro)
            .args(&o.args)
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
        let ready = rx.recv_timeout(Duration::from_secs(20)).expect("kernel did not print READY");
        assert!(ready.starts_with("READY instance="), "{ready}");
        Kernel {
            child,
            dir: dir.to_path_buf(),
            socket: dir.join("driver.sock"),
            intro,
            alarm: dir.join("alarm.sock"),
            history: dir.join("history.sock"),
            ready,
            stderr: Some(stderr),
        }
    }

    pub fn instance(&self) -> String {
        self.ready["READY instance=".len()..].split(' ').next().unwrap().to_owned()
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

    pub fn wait_for(
        &self,
        what: &str,
        timeout: Duration,
        pred: impl Fn(&IntrospectResponse) -> bool,
    ) -> IntrospectResponse {
        let start = Instant::now();
        loop {
            let r = self.introspect();
            if pred(&r) {
                return r;
            }
            assert!(start.elapsed() < timeout, "timeout waiting for {what}: {:?}", summary(&r));
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// SIGKILL; returns stderr.
    pub fn kill(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stderr.take().map(|h| h.join().unwrap_or_default()).unwrap_or_default()
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for Kernel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn summary(r: &IntrospectResponse) -> String {
    let tags: Vec<String> = r
        .tags
        .iter()
        .map(|t| {
            format!(
                "{}={:?}/{:?}",
                t.tag_id,
                tag_value(t),
                Quality::try_from(t.quality).unwrap_or(Quality::Unspecified)
            )
        })
        .collect();
    format!("op={} tags=[{}] sources={:?}", r.last_operation_sequence, tags.join(" "), r.sources)
}

pub fn tag(r: &IntrospectResponse, id: u32) -> TagState {
    r.tags.iter().find(|t| t.tag_id == id).cloned().unwrap_or_else(|| panic!("tag {id}"))
}

pub fn quality(r: &IntrospectResponse, id: u32) -> Quality {
    Quality::try_from(tag(r, id).quality).unwrap()
}

/// Every tag of source 1001 has exactly the expected value and is Good.
pub fn matches_image(r: &IntrospectResponse, i: &Image) -> bool {
    matches_image_at(r, i, 101)
}

pub fn matches_image_at(r: &IntrospectResponse, i: &Image, first_tag: u32) -> bool {
    expected(i).iter().all(|(id, v)| {
        let t = tag(r, id - 101 + first_tag);
        tag_value(&t) == Some(*v) && Quality::try_from(t.quality) == Ok(Quality::Good)
    })
}

pub fn all_quality(r: &IntrospectResponse, q: Quality, first_tag: u32) -> bool {
    (first_tag..first_tag + 9).all(|id| quality(r, id) == q)
}

// ------------------------------------------------------------------ driver

#[derive(Clone, Debug)]
pub struct DriverOpts {
    pub scan_ms: u64,
    pub durable: bool,
    pub require_durable: bool,
    pub max_pending: usize,
    pub max_inflight: u32,
    pub response_timeout_ms: u64,
    pub device_backoff: (u64, u64),
    pub kernel_backoff: (u64, u64),
    pub retry: (u64, u64),
    pub heartbeat_ms: u64,
    /// Extra devices: (name, source_id, port).
    pub extra_devices: Vec<(String, u32, u16)>,
}

impl Default for DriverOpts {
    fn default() -> Self {
        DriverOpts {
            scan_ms: 50,
            durable: false,
            require_durable: false,
            max_pending: 64,
            max_inflight: 8,
            response_timeout_ms: 300,
            device_backoff: (50, 400),
            kernel_backoff: (50, 400),
            retry: (20, 200),
            heartbeat_ms: 500,
            extra_devices: Vec::new(),
        }
    }
}

const CHANNELS: &str = r#"channels = [
  { channel = 1, point = "di_0" },
  { channel = 2, point = "di_1" },
  { channel = 3, point = "coil_0" },
  { channel = 4, point = "temperature_centi" },
  { channel = 5, point = "status_word" },
  { channel = 6, point = "status_alarm_bit" },
  { channel = 7, point = "counter" },
  { channel = 8, point = "level_milli" },
  { channel = 9, point = "signed_i32" },
]
"#;

fn device_block(name: &str, source: u32, port: u16, o: &DriverOpts) -> String {
    format!(
        "\n[[devices]]\nname = \"{name}\"\nsource_id = {source}\nhost = \"127.0.0.1\"\nport = {port}\nunit_id = 1\n\
         profile = \"generic-io\"\nprofile_version = 1\nscan_interval_ms = {}\nconnect_timeout_ms = 500\n\
         response_timeout_ms = {}\ndevice_backoff_min_ms = {}\ndevice_backoff_max_ms = {}\n{CHANNELS}",
        o.scan_ms, o.response_timeout_ms, o.device_backoff.0, o.device_backoff.1
    )
}

pub fn driver_config(kernel_socket: &Path, port: u16, o: &DriverOpts) -> String {
    let mut s = format!(
        "schema = \"moscada.driver.modbus-tcp/1\"\n\n[driver]\ndriver_id = \"modbus-e2e\"\nkernel_socket = \"{}\"\n\
         catalog_files = [\"{}\"]\nrequest_durable_receipts = {}\nrequire_durable_receipts = {}\nmax_pending_batches = {}\n\
         max_inflight_batches = {}\nheartbeat_interval_ms = {}\nkernel_reconnect_min_ms = {}\nkernel_reconnect_max_ms = {}\n\
         retry_min_ms = {}\nretry_max_ms = {}\ndrain_timeout_ms = 300\nstatus_interval_ms = 1000\n",
        kernel_socket.display(),
        catalog_file().display(),
        o.durable,
        o.require_durable,
        o.max_pending,
        o.max_inflight,
        o.heartbeat_ms,
        o.kernel_backoff.0,
        o.kernel_backoff.1,
        o.retry.0,
        o.retry.1,
    );
    s.push_str(&device_block("dev-a", 1001, port, o));
    for (n, src, p) in &o.extra_devices {
        s.push_str(&device_block(n, *src, *p, o));
    }
    s
}

/// Parsed `STATUS` line.
pub type Status = BTreeMap<String, String>;

pub fn parse_status(line: &str) -> Status {
    let mut m = BTreeMap::new();
    let body = line.strip_prefix("STATUS ").unwrap_or(line);
    let mut chars = body.chars().peekable();
    loop {
        while chars.peek() == Some(&' ') {
            chars.next();
        }
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == '=' {
                break;
            }
            key.push(c);
            chars.next();
        }
        if chars.next().is_none() {
            break;
        }
        let mut val = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => {
                        if let Some(n) = chars.next() {
                            val.push(n);
                        }
                    }
                    '"' => break,
                    c => val.push(c),
                }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == ' ' {
                    break;
                }
                val.push(c);
                chars.next();
            }
        }
        m.insert(key, val);
    }
    m
}

pub fn num(s: &Status, k: &str) -> u64 {
    s.get(k).and_then(|v| v.parse().ok()).unwrap_or_else(|| panic!("STATUS key {k} missing or not a number: {s:?}"))
}

pub struct Driver {
    pub child: Child,
    lines: Arc<Mutex<Vec<String>>>,
    pub config: PathBuf,
}

impl Driver {
    pub fn start(dir: &Path, cfg: &str) -> Driver {
        Self::start_named(dir, "driver.toml", cfg)
    }

    pub fn start_named(dir: &Path, file: &str, cfg: &str) -> Driver {
        let path = dir.join(file);
        std::fs::write(&path, cfg).unwrap();
        let mut child = Command::new(driver_bin())
            .arg("--config")
            .arg(&path)
            .arg("--status-interval-ms")
            .arg("25")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let l2 = lines.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                l2.lock().unwrap().push(line);
            }
        });
        Driver { child, lines, config: path }
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    /// Latest STATUS of a device.
    pub fn status(&self, device: &str) -> Option<Status> {
        let want = format!("STATUS device={device} ");
        self.lines.lock().unwrap().iter().rev().find(|l| l.starts_with(&want)).map(|l| parse_status(l))
    }

    pub fn wait_status(&self, device: &str, what: &str, timeout: Duration, pred: impl Fn(&Status) -> bool) -> Status {
        let start = Instant::now();
        loop {
            if let Some(s) = self.status(device)
                && pred(&s)
            {
                return s;
            }
            assert!(start.elapsed() < timeout, "timeout waiting for driver {what}: {:?}", self.status(device));
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn transitions(&self) -> Vec<String> {
        self.lines().into_iter().filter(|l| l.starts_with("TRANSITION")).collect()
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.kill();
    }
}

pub fn wait_until(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < timeout, "timeout waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub const T: Duration = Duration::from_secs(20);
