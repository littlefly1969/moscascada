//! Driver configuration (schema `moscada.driver.modbus-tcp/1`) and its static validation,
//! including the optional cross-check against the kernel configuration (ADR 0015 D11).
//! Nothing here opens a socket.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use scada_driver_sdk::{Backoff, ChannelType, PublisherConfig};
use scada_modbus::plan::{self, Location, Plan, PointSpan};
use scada_modbus::value::Decoding;
use serde::Deserialize;

use crate::catalog::{Catalog, Profile, valid_name};

pub const CONFIG_SCHEMA: &str = "moscada.driver.modbus-tcp/1";
pub const MAX_DEVICES_CEILING: usize = 256;
pub const MAX_CONFIG_FILE_BYTES: u64 = 4 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn bad<T>(s: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError(s.into()))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    schema: String,
    driver: FileDriver,
    #[serde(default)]
    devices: Vec<FileDevice>,
}

fn d_max_pending() -> usize {
    64
}
fn d_max_inflight() -> u32 {
    8
}
fn d_heartbeat() -> u64 {
    1000
}
fn d_5000() -> u64 {
    5000
}
fn d_100() -> u64 {
    100
}
fn d_10000() -> u64 {
    10_000
}
fn d_20() -> u64 {
    20
}
fn d_1000() -> u64 {
    1000
}
fn d_max_devices() -> usize {
    64
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDriver {
    driver_id: String,
    kernel_socket: PathBuf,
    #[serde(default)]
    catalog_dir: Option<PathBuf>,
    #[serde(default)]
    catalog_files: Vec<PathBuf>,
    #[serde(default)]
    request_durable_receipts: bool,
    #[serde(default)]
    require_durable_receipts: bool,
    #[serde(default = "d_max_pending")]
    max_pending_batches: usize,
    #[serde(default = "d_max_inflight")]
    max_inflight_batches: u32,
    #[serde(default = "d_heartbeat")]
    heartbeat_interval_ms: u64,
    #[serde(default = "d_5000")]
    handshake_timeout_ms: u64,
    #[serde(default = "d_5000")]
    write_timeout_ms: u64,
    #[serde(default = "d_100")]
    kernel_reconnect_min_ms: u64,
    #[serde(default = "d_10000")]
    kernel_reconnect_max_ms: u64,
    #[serde(default = "d_20")]
    retry_min_ms: u64,
    #[serde(default = "d_5000")]
    retry_max_ms: u64,
    #[serde(default = "d_1000")]
    drain_timeout_ms: u64,
    #[serde(default = "d_10000")]
    status_interval_ms: u64,
    #[serde(default)]
    health_file: Option<PathBuf>,
    #[serde(default = "d_max_devices")]
    max_devices: usize,
}

fn d_port() -> u16 {
    scada_modbus::DEFAULT_PORT
}
fn d_connect() -> u64 {
    2000
}
fn d_response() -> u64 {
    1000
}
fn d_dev_min() -> u64 {
    500
}
fn d_dev_max() -> u64 {
    30_000
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDevice {
    name: String,
    source_id: u32,
    host: String,
    #[serde(default = "d_port")]
    port: u16,
    unit_id: u8,
    profile: String,
    profile_version: u32,
    scan_interval_ms: u64,
    #[serde(default = "d_connect")]
    connect_timeout_ms: u64,
    #[serde(default = "d_response")]
    response_timeout_ms: u64,
    #[serde(default)]
    inter_request_delay_ms: u64,
    #[serde(default = "d_dev_min")]
    device_backoff_min_ms: u64,
    #[serde(default = "d_dev_max")]
    device_backoff_max_ms: u64,
    channels: Vec<FileChannel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileChannel {
    channel: u32,
    point: String,
}

/// One mapped channel of a device (sorted by channel id inside the device).
#[derive(Debug, Clone)]
pub struct Channel {
    pub channel_id: u32,
    pub ty: ChannelType,
    pub point: String,
    pub decoding: Decoding,
    pub location: Location,
}

/// A validated device with its read plan.
#[derive(Debug, Clone)]
pub struct Device {
    pub name: String,
    pub source_id: u32,
    pub host: String,
    pub port: u16,
    pub unit_id: u8,
    pub profile_id: String,
    pub profile_version: u32,
    pub scan_interval: Duration,
    pub connect_timeout: Duration,
    pub response_timeout: Duration,
    pub inter_request_delay: Duration,
    pub backoff: Backoff,
    pub channels: Vec<Channel>,
    pub plan: Plan,
}

impl Device {
    pub fn sdk_channels(&self) -> Vec<(u32, ChannelType)> {
        self.channels.iter().map(|c| (c.channel_id, c.ty)).collect()
    }
}

/// The whole validated configuration.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    pub driver_id: String,
    pub kernel_socket: PathBuf,
    pub request_durable_receipts: bool,
    pub require_durable_receipts: bool,
    pub max_pending: usize,
    pub max_inflight: u32,
    pub heartbeat_interval: Option<Duration>,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    pub kernel_backoff: Backoff,
    pub retry_backoff: Backoff,
    pub drain_timeout: Duration,
    pub status_interval: Duration,
    pub health_file: Option<PathBuf>,
    pub devices: Vec<Device>,
    pub catalog: Catalog,
}

impl DriverConfig {
    /// SDK configuration of one device's source.
    pub fn publisher_config(&self, d: &Device) -> PublisherConfig {
        let mut p = PublisherConfig::new(self.kernel_socket.clone(), &self.driver_id, d.source_id, d.sdk_channels());
        p.request_durable_receipts = self.request_durable_receipts;
        p.require_durable_receipts = self.require_durable_receipts;
        p.max_pending = self.max_pending;
        p.max_inflight = self.max_inflight;
        p.heartbeat_interval = self.heartbeat_interval;
        p.handshake_timeout = self.handshake_timeout;
        p.write_timeout = self.write_timeout;
        p.reconnect_backoff = self.kernel_backoff;
        p.retry_backoff = self.retry_backoff;
        p
    }
}

fn ms(name: &str, v: u64, lo: u64, hi: u64) -> Result<Duration, ConfigError> {
    if v < lo || v > hi {
        return bad(format!("{name} = {v} outside {lo}..={hi}"));
    }
    Ok(Duration::from_millis(v))
}

fn backoff(name: &str, lo: u64, hi: u64) -> Result<Backoff, ConfigError> {
    let min = ms(&format!("{name} min"), lo, 1, 3_600_000)?;
    let max = ms(&format!("{name} max"), hi, 1, 3_600_000)?;
    if min > max {
        return bad(format!("{name}: min > max"));
    }
    Ok(Backoff { min, max })
}

fn read_bounded(path: &Path) -> Result<String, ConfigError> {
    let meta = std::fs::metadata(path).map_err(|e| ConfigError(format!("{}: {e}", path.display())))?;
    if meta.len() > MAX_CONFIG_FILE_BYTES {
        return bad(format!("{}: larger than {MAX_CONFIG_FILE_BYTES} bytes", path.display()));
    }
    std::fs::read_to_string(path).map_err(|e| ConfigError(format!("{}: {e}", path.display())))
}

/// Load and validate the configuration file and the catalog it names (relative paths are
/// relative to the configuration file).
pub fn load(path: &Path) -> Result<DriverConfig, ConfigError> {
    let text = read_bounded(path)?;
    let base = path.parent().unwrap_or(Path::new("."));
    parse(&text, base)
}

pub fn parse(text: &str, base: &Path) -> Result<DriverConfig, ConfigError> {
    let f: FileConfig = toml::from_str(text).map_err(|e| ConfigError(format!("configuration: {e}")))?;
    if f.schema != CONFIG_SCHEMA {
        return bad(format!("schema \"{}\" (expected \"{CONFIG_SCHEMA}\")", f.schema));
    }
    let d = &f.driver;
    if d.driver_id.is_empty() || d.driver_id.len() > scada_protocol_max_driver_id() {
        return bad("driver.driver_id must be 1..=64 bytes");
    }
    if d.max_devices == 0 || d.max_devices > MAX_DEVICES_CEILING {
        return bad(format!("driver.max_devices must be 1..={MAX_DEVICES_CEILING}"));
    }
    if f.devices.is_empty() {
        return bad("at least one [[devices]] entry is required");
    }
    if f.devices.len() > d.max_devices {
        return bad(format!("{} devices exceed driver.max_devices = {}", f.devices.len(), d.max_devices));
    }
    if d.max_pending_batches == 0 || d.max_pending_batches > scada_driver_sdk::publisher::MAX_PENDING_CEILING {
        return bad("driver.max_pending_batches must be 1..=65536");
    }
    if d.max_inflight_batches == 0 || d.max_inflight_batches > 1024 {
        return bad("driver.max_inflight_batches must be 1..=1024");
    }
    if d.require_durable_receipts && !d.request_durable_receipts {
        return bad("driver.require_durable_receipts needs request_durable_receipts = true");
    }
    let heartbeat_interval = match d.heartbeat_interval_ms {
        0 => None,
        v => Some(ms("driver.heartbeat_interval_ms", v, 100, 600_000)?),
    };
    let resolve = |p: &Path| if p.is_absolute() { p.to_path_buf() } else { base.join(p) };
    let catalog_dir = d.catalog_dir.as_deref().map(resolve);
    let catalog_files: Vec<PathBuf> = d.catalog_files.iter().map(|p| resolve(p)).collect();
    let catalog = Catalog::load(catalog_dir.as_deref(), &catalog_files).map_err(|e| ConfigError(e.0))?;
    let mut names = BTreeSet::new();
    let mut sources = BTreeSet::new();
    let mut devices = Vec::new();
    for fd in &f.devices {
        let ctx = format!("device \"{}\"", fd.name);
        if !valid_name(&fd.name) {
            return bad(format!("{ctx}: name must match [a-z][a-z0-9_-]{{0,63}}"));
        }
        if !names.insert(fd.name.clone()) {
            return bad(format!("{ctx}: duplicate device name"));
        }
        if !sources.insert(fd.source_id) {
            return bad(format!("{ctx}: source_id {} used by two devices (one connection per source)", fd.source_id));
        }
        if fd.host.is_empty() || fd.host.len() > 253 {
            return bad(format!("{ctx}: host must be 1..=253 bytes"));
        }
        if fd.port == 0 {
            return bad(format!("{ctx}: port 0"));
        }
        let profile = catalog.get(&fd.profile, fd.profile_version).ok_or_else(|| {
            ConfigError(format!("{ctx}: profile {} v{} not in the catalog", fd.profile, fd.profile_version))
        })?;
        let device = build_device(fd, profile, &ctx)?;
        devices.push(device);
    }
    Ok(DriverConfig {
        driver_id: d.driver_id.clone(),
        kernel_socket: resolve(&d.kernel_socket),
        request_durable_receipts: d.request_durable_receipts,
        require_durable_receipts: d.require_durable_receipts,
        max_pending: d.max_pending_batches,
        max_inflight: d.max_inflight_batches,
        heartbeat_interval,
        handshake_timeout: ms("driver.handshake_timeout_ms", d.handshake_timeout_ms, 10, 60_000)?,
        write_timeout: ms("driver.write_timeout_ms", d.write_timeout_ms, 10, 60_000)?,
        kernel_backoff: backoff("driver.kernel_reconnect", d.kernel_reconnect_min_ms, d.kernel_reconnect_max_ms)?,
        retry_backoff: backoff("driver.retry", d.retry_min_ms, d.retry_max_ms)?,
        drain_timeout: ms("driver.drain_timeout_ms", d.drain_timeout_ms, 0, 60_000)?,
        status_interval: ms("driver.status_interval_ms", d.status_interval_ms, 100, 3_600_000)?,
        health_file: d.health_file.as_deref().map(resolve),
        devices,
        catalog,
    })
}

fn scada_protocol_max_driver_id() -> usize {
    64
}

fn build_device(fd: &FileDevice, profile: &Profile, ctx: &str) -> Result<Device, ConfigError> {
    if fd.channels.is_empty() {
        return bad(format!("{ctx}: at least one channel"));
    }
    let mut by_channel: BTreeMap<u32, &FileChannel> = BTreeMap::new();
    let mut points = BTreeSet::new();
    for c in &fd.channels {
        if by_channel.insert(c.channel, c).is_some() {
            return bad(format!("{ctx}: channel {} mapped twice", c.channel));
        }
        if !points.insert(c.point.clone()) {
            return bad(format!("{ctx}: point \"{}\" mapped to two channels", c.point));
        }
    }
    let mut spans = Vec::new();
    let mut mapped = Vec::new();
    for (ch, c) in &by_channel {
        let p = profile.points.get(&c.point).ok_or_else(|| {
            ConfigError(format!(
                "{ctx}: channel {ch}: point \"{}\" not in profile {} v{}",
                c.point, profile.id, profile.version
            ))
        })?;
        spans.push(PointSpan { area: p.area, address: p.address, width: p.decoding.raw.width() });
        mapped.push((*ch, p));
    }
    let plan = plan::build(&profile.blocks, &spans, profile.max_requests_per_scan as usize)
        .map_err(|e| ConfigError(format!("{ctx}: read plan: {e}")))?;
    let channels = mapped
        .into_iter()
        .zip(&plan.locations)
        .map(|((ch, p), loc)| Channel {
            channel_id: ch,
            ty: if p.is_bool() { ChannelType::Bool } else { ChannelType::I64 },
            point: p.name.clone(),
            decoding: p.decoding,
            location: *loc,
        })
        .collect();
    let scan_interval = ms(&format!("{ctx}: scan_interval_ms"), fd.scan_interval_ms, 10, 86_400_000)?;
    let response_timeout = ms(&format!("{ctx}: response_timeout_ms"), fd.response_timeout_ms, 1, 60_000)?;
    Ok(Device {
        name: fd.name.clone(),
        source_id: fd.source_id,
        host: fd.host.clone(),
        port: fd.port,
        unit_id: fd.unit_id,
        profile_id: profile.id.clone(),
        profile_version: profile.version,
        scan_interval,
        connect_timeout: ms(&format!("{ctx}: connect_timeout_ms"), fd.connect_timeout_ms, 1, 60_000)?,
        response_timeout,
        inter_request_delay: ms(&format!("{ctx}: inter_request_delay_ms"), fd.inter_request_delay_ms, 0, 10_000)?,
        backoff: backoff(&format!("{ctx}: device_backoff"), fd.device_backoff_min_ms, fd.device_backoff_max_ms)?,
        channels,
        plan,
    })
}

// ------------------------------------------------------------------ kernel cross-check

#[derive(Debug, Deserialize)]
struct KernelFile {
    ipc: KernelIpc,
    #[serde(default)]
    core: Option<KernelCore>,
    #[serde(default)]
    sources: Vec<KernelSource>,
}

#[derive(Debug, Deserialize)]
struct KernelIpc {
    max_driver_connections: u32,
    #[serde(default)]
    socket_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct KernelCore {
    #[serde(default)]
    max_observations_per_batch: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct KernelSource {
    id: u32,
    channels: Vec<KernelChannel>,
}

#[derive(Debug, Deserialize)]
struct KernelChannel {
    channel: u32,
    #[serde(rename = "type")]
    ty: String,
}

/// Static check of the driver configuration against the kernel's TOML: every device's
/// source exists, its channels are exactly the source's channels with the same types
/// (anything else makes a FullState impossible or a batch rejected), the batch limit and
/// the connection limit are respected. Returns warnings (non-fatal).
pub fn check_against_kernel(cfg: &DriverConfig, kernel_toml: &str) -> Result<Vec<String>, ConfigError> {
    let k: KernelFile = toml::from_str(kernel_toml).map_err(|e| ConfigError(format!("kernel configuration: {e}")))?;
    let max_obs = k.core.and_then(|c| c.max_observations_per_batch).unwrap_or(128) as usize;
    let mut warnings = Vec::new();
    if cfg.devices.len() > k.ipc.max_driver_connections as usize {
        return bad(format!(
            "{} devices need {} connections; kernel max_driver_connections = {}",
            cfg.devices.len(),
            cfg.devices.len(),
            k.ipc.max_driver_connections
        ));
    }
    if let Some(sock) = &k.ipc.socket_path
        && sock != &cfg.kernel_socket
    {
        warnings.push(format!(
            "driver.kernel_socket {} differs from the kernel's ipc.socket_path {}",
            cfg.kernel_socket.display(),
            sock.display()
        ));
    }
    for d in &cfg.devices {
        let ctx = format!("device \"{}\" (source {})", d.name, d.source_id);
        let s = k
            .sources
            .iter()
            .find(|s| s.id == d.source_id)
            .ok_or_else(|| ConfigError(format!("{ctx}: source not configured in the kernel")))?;
        let kc: BTreeMap<u32, &str> = s.channels.iter().map(|c| (c.channel, c.ty.as_str())).collect();
        for c in &d.channels {
            match kc.get(&c.channel_id) {
                None => return bad(format!("{ctx}: channel {} not configured in the kernel", c.channel_id)),
                Some(t) if *t != c.ty.name() => {
                    return bad(format!(
                        "{ctx}: channel {} is {} in the kernel, point \"{}\" decodes to {}",
                        c.channel_id,
                        t,
                        c.point,
                        c.ty.name()
                    ));
                }
                _ => {}
            }
        }
        let mine: BTreeSet<u32> = d.channels.iter().map(|c| c.channel_id).collect();
        let missing: Vec<u32> = kc.keys().filter(|c| !mine.contains(c)).copied().collect();
        if !missing.is_empty() {
            return bad(format!(
                "{ctx}: kernel channels {missing:?} are not mapped: a FullState (every channel of the source) is impossible"
            ));
        }
        if d.channels.len() > max_obs {
            return bad(format!("{ctx}: {} channels > kernel max_observations_per_batch {max_obs}", d.channels.len()));
        }
    }
    Ok(warnings)
}
