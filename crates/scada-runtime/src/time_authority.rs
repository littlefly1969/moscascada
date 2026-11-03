//! VS-006 time authority (ADR 0014). Samples, once per applied operation and on the
//! sequencer thread, the time annotation recorded in the journal. Never decides anything
//! causal (J7): the order is the operation sequence. Anomalies (steps backwards or
//! forwards, regression across boots) are flagged and recorded, never corrected.

use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use scada_journal::{TimeAnomaly, TimeEnvelope, boottime_ns, clock_flags, clock_quality, realtime_ns};

use crate::config::{HostSyncProbe, TimeSettings};

/// Anomaly kinds of `time_observations.kind`.
pub mod anomaly {
    pub const REGRESSION: u8 = 1;
    pub const JUMP_FORWARD: u8 = 2;
    pub const CROSS_BOOT_REGRESSION: u8 = 3;
}

/// Published time status (`GetTimeStatus`). Diagnostics only.
#[derive(Debug)]
pub struct TimeShared {
    pub last_sequenced_at: AtomicI64,
    pub last_logical: AtomicI64,
    pub regressions: AtomicU64,
    pub jumps_forward: AtomicU64,
    pub last_anomaly_at: AtomicI64,
    pub last_anomaly_delta: AtomicI64,
    pub host_quality: AtomicU8,
    pub host_uncertainty_ns: AtomicU64,
    pub host_source: Mutex<String>,
    pub os_boot_id: String,
    pub boot_generation: AtomicU64,
}

impl TimeShared {
    pub fn new(os_boot_id: String) -> TimeShared {
        TimeShared {
            last_sequenced_at: AtomicI64::new(0),
            last_logical: AtomicI64::new(0),
            regressions: AtomicU64::new(0),
            jumps_forward: AtomicU64::new(0),
            last_anomaly_at: AtomicI64::new(0),
            last_anomaly_delta: AtomicI64::new(0),
            host_quality: AtomicU8::new(clock_quality::UNKNOWN),
            host_uncertainty_ns: AtomicU64::new(0),
            host_source: Mutex::new("none".into()),
            os_boot_id,
            boot_generation: AtomicU64::new(0),
        }
    }
}

/// TEST-ONLY injected clock (`--test-clock-offset-file`): the file holds a signed offset
/// in nanoseconds added to `CLOCK_REALTIME` (re-read at most every 5 ms). Simulates clock
/// steps without privileges on the host clock (ADR 0014 §9).
#[derive(Debug)]
struct Offset {
    path: PathBuf,
    ns: i64,
    read_at: Option<Instant>,
}

impl Offset {
    fn get(&mut self) -> i64 {
        if self.read_at.is_none_or(|t| t.elapsed() >= Duration::from_millis(5)) {
            self.read_at = Some(Instant::now());
            if let Ok(s) = std::fs::read_to_string(&self.path)
                && let Ok(v) = s.trim().parse::<i64>()
            {
                self.ns = v;
            }
        }
        self.ns
    }
}

/// Sequencer-owned sampler.
#[derive(Debug)]
pub struct TimeAuthority {
    pub shared: Arc<TimeShared>,
    prev: Option<(i64, u64)>,
    /// Non-regressive key, persisted by the journal (`schema_meta.last_logical_time_ns`).
    logical: i64,
    regression_alert_ns: i64,
    jump_alert_ns: i64,
    offset: Option<Offset>,
}

fn ms_ns(ms: u64) -> i64 {
    i64::try_from(ms).unwrap_or(i64::MAX / 1_000_000).saturating_mul(1_000_000)
}

impl TimeAuthority {
    pub fn new(
        settings: &TimeSettings,
        persisted_logical: i64,
        shared: Arc<TimeShared>,
        offset_file: Option<PathBuf>,
    ) -> Self {
        TimeAuthority {
            shared,
            prev: None,
            logical: persisted_logical,
            regression_alert_ns: ms_ns(settings.clock_regression_alert_ms),
            jump_alert_ns: ms_ns(settings.clock_jump_alert_ms),
            offset: offset_file.map(|path| Offset { path, ns: 0, read_at: None }),
        }
    }

    /// Sample the envelope of the operation about to be applied. Nothing is updated until
    /// [`Self::accept`] (a refused operation has no `sequenced_at`, ADR 0014 §2).
    pub fn sample(&mut self) -> (TimeEnvelope, Option<TimeAnomaly>) {
        let off = self.offset.as_mut().map_or(0, Offset::get);
        let rt = realtime_ns().saturating_add(off);
        let boot = boottime_ns();
        let mut flags = 0u8;
        let mut anomaly = None;
        match self.prev {
            None => {
                if self.logical != i64::MIN && rt < self.logical {
                    flags |= clock_flags::CROSS_BOOT_REGRESSION;
                    let d = rt.saturating_sub(self.logical);
                    if d < -self.regression_alert_ns {
                        anomaly = Some(TimeAnomaly { kind: anomaly::CROSS_BOOT_REGRESSION, delta_ns: d });
                    }
                }
            }
            Some((prt, pboot)) => {
                if rt < prt {
                    flags |= clock_flags::REGRESSED;
                }
                // Step = realtime advance minus elapsed boottime: an idle period is not a jump.
                let elapsed = i64::try_from(boot.saturating_sub(pboot)).unwrap_or(i64::MAX);
                let step = rt.saturating_sub(prt).saturating_sub(elapsed);
                if step < -self.regression_alert_ns {
                    anomaly = Some(TimeAnomaly { kind: anomaly::REGRESSION, delta_ns: step });
                } else if step > self.jump_alert_ns {
                    flags |= clock_flags::JUMP_FORWARD;
                    anomaly = Some(TimeAnomaly { kind: anomaly::JUMP_FORWARD, delta_ns: step });
                }
            }
        }
        let logical = self.logical.max(rt);
        if logical != rt {
            flags |= clock_flags::LOGICAL_HELD;
        }
        let env = TimeEnvelope {
            sequenced_at_utc_ns: rt,
            boottime_ns: boot,
            logical_time_ns: logical,
            clock_quality: self.shared.host_quality.load(Relaxed),
            clock_flags: flags,
            received_at_utc_ns: None,
        };
        (env, anomaly)
    }

    /// The sampled operation was applied: advance the state and publish.
    pub fn accept(&mut self, env: &TimeEnvelope, anomaly: Option<&TimeAnomaly>) {
        self.prev = Some((env.sequenced_at_utc_ns, env.boottime_ns));
        self.logical = env.logical_time_ns;
        self.shared.last_sequenced_at.store(env.sequenced_at_utc_ns, Relaxed);
        self.shared.last_logical.store(env.logical_time_ns, Relaxed);
        if let Some(a) = anomaly {
            match a.kind {
                anomaly::JUMP_FORWARD => self.shared.jumps_forward.fetch_add(1, Relaxed),
                _ => self.shared.regressions.fetch_add(1, Relaxed),
            };
            self.shared.last_anomaly_at.store(env.sequenced_at_utc_ns, Relaxed);
            self.shared.last_anomaly_delta.store(a.delta_ns, Relaxed);
            eprintln!(
                "time: clock anomaly kind={} delta_ns={} at operation time {} (recorded, nothing reordered)",
                a.kind, a.delta_ns, env.sequenced_at_utc_ns
            );
        }
    }

    /// Received-at sample for the runtime side (`runtime_received_at`).
    pub fn received_now(&mut self) -> i64 {
        let off = self.offset.as_mut().map_or(0, Offset::get);
        realtime_ns().saturating_add(off)
    }
}

/// Parse `chronyc tracking` (ADR 0014 §5): quality and uncertainty estimate.
pub fn parse_chronyc(out: &str) -> (u8, u64) {
    let mut q = clock_quality::UNKNOWN;
    let mut unc = 0u64;
    let secs = |l: &str| -> Option<f64> { l.split(':').nth(1)?.split_whitespace().next()?.parse().ok() };
    let (mut delay, mut disp) = (None, None);
    for l in out.lines() {
        let t = l.trim_start();
        if t.starts_with("Leap status") {
            q = if t.contains("Normal") || t.contains("Insert") || t.contains("Delete") {
                clock_quality::SYNCHRONIZED
            } else {
                clock_quality::HOLDOVER
            };
        } else if t.starts_with("Root delay") {
            delay = secs(t);
        } else if t.starts_with("Root dispersion") {
            disp = secs(t);
        }
    }
    if let (Some(d), Some(p)) = (delay, disp) {
        unc = ((d / 2.0 + p) * 1e9) as u64;
    }
    (q, unc)
}

/// Start the host sync probe thread when configured. Detached.
pub fn spawn_probe(settings: &TimeSettings, shared: Arc<TimeShared>) {
    let (cmd, args): (&str, &[&str]) = match settings.host_sync_probe {
        HostSyncProbe::None => return,
        HostSyncProbe::Chronyc => ("chronyc", &["tracking"]),
        HostSyncProbe::Timedatectl => ("timedatectl", &["show", "-p", "NTPSynchronized", "--value"]),
    };
    let period = Duration::from_millis(settings.host_sync_probe_period_ms);
    let probe = settings.host_sync_probe;
    let r = std::thread::Builder::new().name("scada-time-probe".into()).spawn(move || {
        loop {
            let (q, unc, src) = match std::process::Command::new(cmd).args(args).output() {
                Ok(o) if o.status.success() => {
                    let s = String::from_utf8_lossy(&o.stdout);
                    match probe {
                        HostSyncProbe::Chronyc => {
                            let (q, u) = parse_chronyc(&s);
                            (q, u, "chronyc".to_owned())
                        }
                        _ => {
                            let q =
                                if s.trim() == "yes" { clock_quality::SYNCHRONIZED } else { clock_quality::UNKNOWN };
                            (q, 0, "timedatectl".to_owned())
                        }
                    }
                }
                Ok(_) => (clock_quality::FAULT, 0, format!("{cmd} failed")),
                Err(e) => (clock_quality::UNKNOWN, 0, format!("{cmd} unavailable: {e}")),
            };
            shared.host_quality.store(q, Relaxed);
            shared.host_uncertainty_ns.store(unc, Relaxed);
            if let Ok(mut g) = shared.host_source.lock() {
                *g = src;
            }
            std::thread::sleep(period);
        }
    });
    if let Err(e) = r {
        eprintln!("time: host sync probe not started: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chronyc_parsing() {
        let out = "Reference ID    : C0A80101 (gw)\nStratum         : 3\nRoot delay      : 0.002000000 seconds\n\
                   Root dispersion : 0.000500000 seconds\nLeap status     : Normal\n";
        let (q, u) = parse_chronyc(out);
        assert_eq!(q, clock_quality::SYNCHRONIZED);
        assert_eq!(u, 1_500_000);
        assert_eq!(parse_chronyc("Leap status     : Not synchronised\n").0, clock_quality::HOLDOVER);
    }

    fn ta(offset: &std::path::Path, persisted: i64) -> TimeAuthority {
        TimeAuthority::new(
            &TimeSettings::default(),
            persisted,
            Arc::new(TimeShared::new(String::new())),
            Some(offset.to_path_buf()),
        )
    }

    fn step(t: &mut TimeAuthority) -> (TimeEnvelope, Option<TimeAnomaly>) {
        let (e, a) = t.sample();
        t.accept(&e, a.as_ref());
        (e, a)
    }

    /// T15: steps of -1 s, -1 h, +24 h: flagged, recorded, logical time never regresses.
    #[test]
    fn clock_steps_are_flagged_and_logical_is_monotonic() {
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("off");
        std::fs::write(&f, "0").unwrap();
        let mut t = ta(&f, i64::MIN);
        let (e0, a0) = step(&mut t);
        assert!(a0.is_none());
        let mut last_logical = e0.logical_time_ns;
        for (off, kind) in [
            (-2_000_000_000i64, Some(anomaly::REGRESSION)),
            (-3_600_000_000_000 - 2_000_000_000, Some(anomaly::REGRESSION)),
            (86_400_000_000_000, Some(anomaly::JUMP_FORWARD)),
        ] {
            std::fs::write(&f, off.to_string()).unwrap();
            std::thread::sleep(Duration::from_millis(6));
            let (e, a) = step(&mut t);
            assert_eq!(a.map(|a| a.kind), kind, "offset {off}");
            assert!(e.logical_time_ns >= last_logical, "logical regressed");
            if kind == Some(anomaly::REGRESSION) {
                assert!(e.clock_flags & clock_flags::REGRESSED != 0);
                assert!(e.clock_flags & clock_flags::LOGICAL_HELD != 0);
            }
            last_logical = e.logical_time_ns;
        }
        assert_eq!(t.shared.regressions.load(Relaxed), 2);
        assert_eq!(t.shared.jumps_forward.load(Relaxed), 1);
    }

    /// T17: RTC moved backwards across a restart: flagged against the persisted logical
    /// time, which keeps the key monotonic.
    #[test]
    fn cross_boot_regression() {
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("off");
        std::fs::write(&f, "-7200000000000").unwrap();
        let persisted = realtime_ns();
        let mut t = ta(&f, persisted);
        let (e, a) = step(&mut t);
        assert_eq!(a.map(|a| a.kind), Some(anomaly::CROSS_BOOT_REGRESSION));
        assert!(e.clock_flags & clock_flags::CROSS_BOOT_REGRESSION != 0);
        assert_eq!(e.logical_time_ns, persisted);
    }

    /// An idle period is not a forward jump (realtime and boottime advance together).
    #[test]
    fn idle_is_not_a_jump() {
        let d = tempfile::tempdir().unwrap();
        let f = d.path().join("off");
        std::fs::write(&f, "0").unwrap();
        let mut t = TimeAuthority::new(
            &TimeSettings { clock_jump_alert_ms: 10, ..TimeSettings::default() },
            i64::MIN,
            Arc::new(TimeShared::new(String::new())),
            Some(f),
        );
        step(&mut t);
        std::thread::sleep(Duration::from_millis(50));
        assert!(step(&mut t).1.is_none());
    }
}
