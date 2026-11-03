//! One worker thread per device (fault isolation per endpoint, ADR 0015 D9).

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use scada_driver_sdk::{Counters, LinkState, Publish, PublisherConfig, SdkError, SourcePublisher, Value};
use scada_modbus::client::{Client, ClientCounters, connect_tcp};
use scada_modbus::scan;
use scada_modbus::value;

use crate::config::Device;

/// State of the field side of a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceState {
    /// Never scanned successfully yet.
    Starting,
    /// Last scan complete.
    Up,
    /// Last attempt failed; retried after backoff.
    Down,
}

impl DeviceState {
    pub fn name(self) -> &'static str {
        match self {
            DeviceState::Starting => "starting",
            DeviceState::Up => "up",
            DeviceState::Down => "down",
        }
    }
}

/// Snapshot of a device worker, shared with the status thread.
#[derive(Debug, Clone)]
pub struct DeviceStatus {
    pub name: String,
    pub source_id: u32,
    pub epoch: u64,
    pub device_state: DeviceState,
    pub link_state: LinkState,
    pub scans_ok: u64,
    pub scans_failed: u64,
    /// Complete scans not published because the kernel was unreachable.
    pub scans_unpublished_no_kernel: u64,
    pub scan_overruns: u64,
    pub modbus_connects: u64,
    pub scan_errors: BTreeMap<&'static str, u64>,
    pub last_error: Option<String>,
    pub last_spread_us: u64,
    pub max_spread_us: u64,
    pub requests_per_scan: usize,
    pub items_per_scan: u64,
    pub publisher: Counters,
    pub pending: usize,
    pub inflight: usize,
    pub next_sequence: u64,
    pub durable_confirmed: Option<u64>,
    pub durable_receipts: bool,
    pub negotiated_minor: Option<u32>,
    pub last_continuity: Option<&'static str>,
    pub last_publisher_error: Option<String>,
    pub client: ClientCounters,
    /// New transitions since the last status read: (from, to, reason).
    pub transitions: Vec<(String, String, String)>,
    pub fatal: Option<String>,
    /// publish_scan → successful ACK (µs, bucket upper bounds): count, p50, p99, p99.9, max.
    pub ack_latency: Option<AckLatency>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckLatency {
    pub n: u64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

impl DeviceStatus {
    fn new(d: &Device, epoch: u64) -> DeviceStatus {
        DeviceStatus {
            name: d.name.clone(),
            source_id: d.source_id,
            epoch,
            device_state: DeviceState::Starting,
            link_state: LinkState::Disconnected,
            scans_ok: 0,
            scans_failed: 0,
            scans_unpublished_no_kernel: 0,
            scan_overruns: 0,
            modbus_connects: 0,
            scan_errors: BTreeMap::new(),
            last_error: None,
            last_spread_us: 0,
            max_spread_us: 0,
            requests_per_scan: d.plan.requests.len(),
            items_per_scan: d.plan.items(),
            publisher: Counters::default(),
            pending: 0,
            inflight: 0,
            next_sequence: 1,
            durable_confirmed: None,
            durable_receipts: false,
            negotiated_minor: None,
            last_continuity: None,
            last_publisher_error: None,
            client: ClientCounters::default(),
            transitions: Vec::new(),
            fatal: None,
            ack_latency: None,
        }
    }
}

fn add(a: &mut ClientCounters, b: &ClientCounters) {
    a.requests += b.requests;
    a.responses_ok += b.responses_ok;
    a.exceptions += b.exceptions;
    a.timeouts += b.timeouts;
    a.closed += b.closed;
    a.framing_errors += b.framing_errors;
    a.transaction_mismatches += b.transaction_mismatches;
    a.unit_mismatches += b.unit_mismatches;
    a.incoherent_responses += b.incoherent_responses;
    a.io_errors += b.io_errors;
}

pub struct Worker {
    dev: Device,
    publisher: SourcePublisher,
    status: Arc<Mutex<DeviceStatus>>,
    stop: Arc<AtomicBool>,
    drain_timeout: Duration,
    client: Option<Client<TcpStream>>,
    /// Counters of closed clients.
    client_totals: ClientCounters,
    attempts: u32,
    retry_at: Instant,
    seen_transitions: u64,
    local: DeviceStatus,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker").field("device", &self.dev.name).finish()
    }
}

impl Worker {
    pub fn new(
        dev: Device,
        pcfg: PublisherConfig,
        epoch: u64,
        drain_timeout: Duration,
        stop: Arc<AtomicBool>,
    ) -> Result<(Worker, Arc<Mutex<DeviceStatus>>), SdkError> {
        let publisher = SourcePublisher::new(pcfg, epoch)?;
        let local = DeviceStatus::new(&dev, epoch);
        let status = Arc::new(Mutex::new(local.clone()));
        let now = Instant::now();
        Ok((
            Worker {
                dev,
                publisher,
                status: status.clone(),
                stop,
                drain_timeout,
                client: None,
                client_totals: ClientCounters::default(),
                attempts: 0,
                retry_at: now,
                seen_transitions: 0,
                local,
            },
            status,
        ))
    }

    fn publish_status(&mut self) {
        let p = &self.publisher;
        self.local.link_state = p.state();
        self.local.publisher = p.counters();
        self.local.pending = p.pending_len();
        self.local.inflight = p.inflight_len();
        self.local.next_sequence = p.next_sequence();
        self.local.durable_confirmed = p.durable_confirmed_sequence();
        self.local.durable_receipts = p.durable_receipts_active();
        self.local.negotiated_minor = p.negotiated_minor().or(self.local.negotiated_minor);
        self.local.last_continuity = p.last_continuity().map(|c| c.as_str_name());
        self.local.last_publisher_error = p.last_error().map(str::to_owned);
        let mut c = self.client_totals;
        if let Some(cl) = &self.client {
            add(&mut c, &cl.counters);
        }
        self.local.client = c;
        let h = p.ack_latency();
        self.local.ack_latency = match (h.quantile_us(0.5), h.quantile_us(0.99), h.quantile_us(0.999)) {
            (Some(p50_us), Some(p99_us), Some(p999_us)) => {
                Some(AckLatency { n: h.count(), p50_us, p99_us, p999_us, max_us: h.max_us() })
            }
            _ => None,
        };
        let total = p.counters().transitions;
        let new = total.saturating_sub(self.seen_transitions) as usize;
        let ts: Vec<_> = p.transitions().collect();
        let fresh: Vec<(String, String, String)> = ts
            .iter()
            .skip(ts.len().saturating_sub(new))
            .map(|t| (t.from.name().to_owned(), t.to.name().to_owned(), t.reason.clone()))
            .collect();
        self.seen_transitions = total;
        let mut g = self.status.lock().unwrap_or_else(|e| e.into_inner());
        let mut pending_ts = std::mem::take(&mut g.transitions);
        pending_ts.extend(fresh);
        // Bounded: the status thread drains it; keep the newest 256.
        let excess = pending_ts.len().saturating_sub(256);
        pending_ts.drain(..excess);
        *g = self.local.clone();
        g.transitions = pending_ts;
    }

    fn drop_client(&mut self) {
        if let Some(c) = self.client.take() {
            add(&mut self.client_totals, &c.counters);
        }
    }

    /// Failure of the device: degrade the source in the kernel, retry with backoff.
    fn device_failed(&mut self, kind: &'static str, reason: String, poisoned: bool) {
        self.local.scans_failed += 1;
        *self.local.scan_errors.entry(kind).or_insert(0) += 1;
        self.local.device_state = DeviceState::Down;
        self.local.last_error = Some(reason.clone());
        if poisoned {
            self.drop_client();
        }
        if self.publisher.state().is_connected() {
            self.publisher.drain(self.drain_timeout);
            self.publisher.disconnect(&format!("device unavailable: {reason}"));
        }
        self.attempts = self.attempts.saturating_add(1);
        let b = self.dev.backoff;
        let f = 1u32.checked_shl(self.attempts.saturating_sub(1).min(20)).unwrap_or(u32::MAX);
        self.retry_at = Instant::now() + b.min.saturating_mul(f).min(b.max);
    }

    /// One scan attempt.
    fn scan_once(&mut self) -> Result<(), String> {
        if self.client.is_none() {
            match connect_tcp(&self.dev.host, self.dev.port, self.dev.connect_timeout) {
                Ok(s) => {
                    self.local.modbus_connects += 1;
                    self.client = Some(Client::new(s, self.dev.unit_id, self.dev.response_timeout));
                }
                Err(e) => {
                    self.device_failed("connect", format!("connect {}:{}: {e}", self.dev.host, self.dev.port), true);
                    return Ok(());
                }
            }
        }
        let Some(client) = self.client.as_mut() else { return Ok(()) };
        let img = match scan::execute(client, &self.dev.plan, self.dev.inter_request_delay) {
            Ok(i) => i,
            Err(e) => {
                let poisoned = e.error.is_fatal_for_connection();
                let kind = e.error.kind();
                self.device_failed(kind, format!("scan: {e}"), poisoned);
                return Ok(());
            }
        };
        let mut values = Vec::with_capacity(self.dev.channels.len());
        for c in &self.dev.channels {
            match img.decode(c.location, &c.decoding) {
                Ok(value::Value::Bool(b)) => values.push(Value::Bool(b)),
                Ok(value::Value::I64(i)) => values.push(Value::I64(i)),
                Err(e) => {
                    let reason = format!("channel {} (point {}): {e}", c.channel_id, c.point);
                    self.device_failed("decode", reason, false);
                    return Ok(());
                }
            }
        }
        let spread = img.spread().as_micros() as u64;
        self.local.last_spread_us = spread;
        self.local.max_spread_us = self.local.max_spread_us.max(spread);
        self.local.scans_ok += 1;
        self.local.device_state = DeviceState::Up;
        self.attempts = 0;
        // A complete scan: publish (connect first if needed: replay, then FullState).
        if !self.publisher.ensure_connected().map_err(|e| e.to_string())? {
            self.local.scans_unpublished_no_kernel += 1;
            return Ok(());
        }
        match self.publisher.publish_scan(&values, img.acquired_at_utc_ns) {
            Ok(Publish::FullState { .. } | Publish::Delta { .. } | Publish::NoChange | Publish::Deferred) => Ok(()),
            Err(SdkError::NotConnected) => {
                self.local.scans_unpublished_no_kernel += 1;
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// Run until `stop`. A configuration error found at runtime (e.g. more channels than
    /// the kernel accepts per batch) stops this device only, recorded as `fatal`.
    pub fn run(mut self) {
        let interval = self.dev.scan_interval;
        let mut next_scan = Instant::now();
        loop {
            if self.stop.load(Ordering::Relaxed) {
                self.publisher.drain(self.drain_timeout);
                self.publisher.disconnect("driver stopping");
                self.publish_status();
                return;
            }
            let now = Instant::now();
            if now >= next_scan && now >= self.retry_at {
                let delayed_by_backoff = self.retry_at > next_scan;
                next_scan += interval;
                if delayed_by_backoff {
                    // Waiting for a device retry is not an overrun.
                    next_scan = now + interval;
                } else if next_scan <= now {
                    let behind = now.duration_since(next_scan).as_nanos() / interval.as_nanos().max(1);
                    self.local.scan_overruns += behind as u64 + 1;
                    next_scan = now + interval;
                }
                // Kernel unreachable and its backoff not elapsed: do not poll the device
                // for nothing (the next connection starts with a FullState anyway).
                if !self.publisher.connect_due() {
                    self.local.scans_unpublished_no_kernel += 1;
                } else if let Err(e) = self.scan_once() {
                    self.local.fatal = Some(e);
                    self.publisher.disconnect("fatal configuration error");
                    self.publish_status();
                    while !self.stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    return;
                }
                self.publish_status();
            }
            let wake = next_scan.max(self.retry_at);
            let left = wake.saturating_duration_since(Instant::now()).min(Duration::from_millis(200));
            if self.publisher.state().is_connected() {
                self.publisher.pump(left, false);
            } else {
                std::thread::sleep(left);
            }
            self.publish_status();
        }
    }
}
