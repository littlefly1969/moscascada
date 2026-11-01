//! VS-004 (ADR 0011): source liveness and per-tag freshness supervision in the runtime.
//!
//! The kernel never reads a clock. Elapsed monotonic time becomes explicit, sequenced
//! operations here, on the sequencer thread, so a capture of the applied stream replays
//! them without any time source:
//!
//! * One watchdog OS thread (only when a source is supervised) raises a *tick* every
//!   `watchdog_poll_ms`: it sets [`WatchdogShared::tick_due`] and, if no wake-up request is
//!   already queued, tries to enqueue one ([`crate::ingress::TickWaker`]) so an idle
//!   sequencer wakes. It never blocks on the queue and holds no request slot.
//! * The sequencer checks the flag after every request it handles (one relaxed load) and
//!   on its wake-up request, so a flood of driver requests can delay a tick by at most one
//!   `apply`, never starve it. Ticks raised while one is pending are coalesced.
//! * On a tick, [`Supervisor::tick`] reads the clock **once** and, for every supervised
//!   source in configuration order, decides against the kernel's current state:
//!   - liveness evidence since the previous tick = a new applied batch (the kernel's
//!     `last_batch_operation` changed) or a new valid heartbeat of the *current* link (its
//!     per-link counter changed). Evidence is time-stamped with the tick that observes it,
//!     an upper bound of its real time, so a silence is never declared early;
//!   - `SourceSilenceExpired` when no evidence for `>= liveness_timeout`;
//!     `SourceLivenessRestored` when a heartbeat arrives on a silent link;
//!   - `FreshnessExpired { observed_up_to }` with the largest operation sequence that the
//!     source's last applied batch had at a tick at least `freshness_timeout` ago (a ring of
//!     `freshness_timeout / poll + 2` samples per source, preallocated). Every tag whose
//!     marker is `<=` that token was observed at least `freshness_timeout` ago; a tag
//!     observed later has a larger marker and is not touched (anti-stale token).
//!
//! Precision: a timeout is detected between `T` and `T + poll + tick lateness` after the
//! last evidence; never before `T`. No hard real-time guarantee.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use scada_core::{Kernel, LinkState, Operation, SourceId, TransitionOutcome};

use crate::config::RuntimeConfig;
use crate::ingress::{TickWaker, Wake};
use crate::sequencer::Counters;

/// Per-link heartbeat counter shared by the session (writer) and the sequencer (reader).
pub type HeartbeatCounter = Arc<AtomicU64>;

/// State shared by the watchdog thread and the sequencer.
#[derive(Debug)]
pub struct WatchdogShared {
    base: Instant,
    poll: Duration,
    tick_due: AtomicBool,
    /// A wake-up request is in the ingress (cleared by the sequencer when it takes it).
    tick_queued: AtomicBool,
    /// When the pending tick was raised, ns since `base`.
    raised_ns: AtomicU64,
}

impl WatchdogShared {
    pub fn new(poll: Duration) -> WatchdogShared {
        WatchdogShared {
            base: Instant::now(),
            poll,
            tick_due: AtomicBool::new(false),
            tick_queued: AtomicBool::new(false),
            raised_ns: AtomicU64::new(0),
        }
    }

    pub fn poll(&self) -> Duration {
        self.poll
    }

    /// Monotonic time since the watchdog was created.
    pub fn now(&self) -> Duration {
        self.base.elapsed()
    }

    /// Sequencer: take the pending tick, if any; returns when it was raised.
    #[inline]
    pub fn take(&self) -> Option<Duration> {
        if self.tick_due.load(Ordering::Relaxed) && self.tick_due.swap(false, Ordering::Acquire) {
            Some(Duration::from_nanos(self.raised_ns.load(Ordering::Relaxed)))
        } else {
            None
        }
    }

    /// Sequencer: the queued wake-up request was taken.
    pub fn woken(&self) {
        self.tick_queued.store(false, Ordering::Release);
    }

    /// Watchdog: raise a tick. Returns true when the previous one was still pending.
    fn raise(&self) -> bool {
        let ns = u64::try_from(self.now().as_nanos()).unwrap_or(u64::MAX);
        self.raised_ns.store(ns, Ordering::Relaxed);
        self.tick_due.swap(true, Ordering::Release)
    }
}

/// Start the watchdog thread. It ends when the ingress is closed (the sequencer stopped).
pub fn spawn_watchdog(
    shared: Arc<WatchdogShared>,
    waker: TickWaker,
    counters: Arc<Counters>,
) -> std::io::Result<thread::JoinHandle<()>> {
    thread::Builder::new().name("scada-watchdog".into()).spawn(move || {
        let mut next = Instant::now() + shared.poll;
        loop {
            let now = Instant::now();
            if next > now {
                thread::sleep(next - now);
            }
            // Fixed rate; after a stall, skip the missed ticks instead of bursting.
            next += shared.poll;
            let now = Instant::now();
            if next <= now {
                next = now + shared.poll;
            }
            Counters::inc(&counters.watchdog_ticks);
            if shared.raise() {
                Counters::inc(&counters.watchdog_ticks_coalesced);
            }
            if !shared.tick_queued.swap(true, Ordering::AcqRel) {
                match waker.try_wake() {
                    Wake::Queued => {}
                    // Queue full: the sequencer is busy and sees the flag after its next
                    // request; nothing to queue.
                    Wake::Full => shared.tick_queued.store(false, Ordering::Release),
                    Wake::Closed => return,
                }
            }
        }
    })
}

/// Liveness bookkeeping of the active link of one source (sequencer-owned).
#[derive(Debug)]
struct LinkWatch {
    generation: u64,
    heartbeats: Option<HeartbeatCounter>,
    seen_heartbeats: u64,
    seen_batch_operation: u64,
    /// Time of the last evidence (`None` until the first tick after the Welcome).
    evidence_at: Option<Duration>,
    /// The kernel had this link `Silent` at the previous tick (or this tick made it so):
    /// a later tick that finds it `Alive` on the same link counts a recovery.
    silent: bool,
}

#[derive(Debug)]
struct Watch {
    source_id: SourceId,
    liveness: Option<Duration>,
    freshness: Option<Duration>,
    link: Option<LinkWatch>,
    /// (tick time, source's last applied batch operation at that tick); preallocated.
    samples: VecDeque<(Duration, u64)>,
    samples_cap: usize,
}

/// Sequencer-owned supervision state: bounded by the configuration, allocated once.
#[derive(Debug)]
pub struct Supervisor {
    poll: Duration,
    /// Indexed like the kernel's sources (configuration order); `None` = unsupervised.
    watches: Vec<Option<Watch>>,
}

/// Number of freshness samples kept for a timeout (enough to always hold one sample at
/// least `timeout` old when ticks are at least `poll` apart).
pub fn freshness_samples(timeout: Duration, poll: Duration) -> usize {
    let polls = timeout.as_nanos().div_ceil(poll.as_nanos().max(1));
    usize::try_from(polls).unwrap_or(usize::MAX).saturating_add(2)
}

/// Byte size of one freshness sample.
pub const SAMPLE_BYTES: usize = std::mem::size_of::<(Duration, u64)>();

impl Supervisor {
    /// `None` when no source is supervised (the default): nothing is allocated.
    pub fn new(cfg: &RuntimeConfig) -> Option<Supervisor> {
        let sup = cfg.supervision.as_ref()?;
        let poll = Duration::from_millis(sup.watchdog_poll_ms);
        let watches = cfg
            .core
            .sources()
            .iter()
            .map(|m| {
                sup.policy(m.source_id).map(|p| {
                    let freshness = p.freshness_timeout_ms.map(Duration::from_millis);
                    let samples_cap = freshness.map_or(0, |t| freshness_samples(t, poll));
                    Watch {
                        source_id: m.source_id,
                        liveness: p.liveness_timeout_ms.map(Duration::from_millis),
                        freshness,
                        link: None,
                        samples: VecDeque::with_capacity(samples_cap),
                        samples_cap,
                    }
                })
            })
            .collect();
        Some(Supervisor { poll, watches })
    }

    pub fn poll(&self) -> Duration {
        self.poll
    }

    /// Whether the source (by configuration index) has liveness supervision, i.e. whether
    /// its sessions need a heartbeat counter.
    pub fn has_liveness(&self, index: usize) -> bool {
        self.watches.get(index).and_then(Option::as_ref).is_some_and(|w| w.liveness.is_some())
    }

    /// A new link was welcomed (after its `SourceConnected` was applied). Replaces the
    /// previous link of the source: heartbeats of a superseded link are never read again.
    pub fn on_connected(&mut self, index: usize, generation: u64, heartbeats: Option<HeartbeatCounter>, k: &Kernel) {
        let Some(Some(w)) = self.watches.get_mut(index) else { return };
        let seen_batch_operation = k.source_status(w.source_id).map_or(0, |s| s.last_batch_operation);
        let seen_heartbeats = heartbeats.as_ref().map_or(0, |c| c.load(Ordering::Relaxed));
        w.link = Some(LinkWatch {
            generation,
            heartbeats,
            seen_heartbeats,
            seen_batch_operation,
            evidence_at: None,
            silent: false,
        });
    }

    /// One watchdog tick at monotonic time `now`. `apply` sequences, captures and applies
    /// one operation (returns its outcome, `None` if refused). Operations are emitted for
    /// sources in configuration order, at most one silence or restoration and one
    /// freshness expiry per source and tick.
    pub fn tick(
        &mut self,
        now: Duration,
        k: &mut Kernel,
        apply: &mut dyn FnMut(&mut Kernel, Operation) -> Option<TransitionOutcome>,
        counters: &Counters,
    ) {
        for w in self.watches.iter_mut().flatten() {
            let Some(st) = k.source_status(w.source_id) else { continue };
            let generation = match st.link {
                LinkState::Connected { generation } => Some(generation),
                _ => None,
            };
            // Freshness sample first (the batch operation does not depend on liveness).
            if w.freshness.is_some() {
                if w.samples.len() >= w.samples_cap {
                    w.samples.pop_front();
                }
                w.samples.push_back((now, st.last_batch_operation));
            }
            let Some(generation) = generation else {
                w.link = None;
                continue;
            };
            if w.link.as_ref().is_none_or(|l| l.generation != generation) {
                // Connected without `on_connected` (cannot happen with the sequencer):
                // supervise it without heartbeats.
                w.link = Some(LinkWatch {
                    generation,
                    heartbeats: None,
                    seen_heartbeats: 0,
                    seen_batch_operation: st.last_batch_operation,
                    evidence_at: None,
                    silent: st.silent,
                });
            }
            let Some(link) = w.link.as_mut() else { continue };
            let hb = link.heartbeats.as_ref().map_or(link.seen_heartbeats, |c| c.load(Ordering::Relaxed));
            let new_heartbeats = hb.wrapping_sub(link.seen_heartbeats);
            if new_heartbeats > 1 {
                counters.heartbeats_coalesced.fetch_add(new_heartbeats - 1, Ordering::Relaxed);
            }
            let new_batch = st.last_batch_operation != link.seen_batch_operation;
            link.seen_heartbeats = hb;
            link.seen_batch_operation = st.last_batch_operation;
            if new_heartbeats > 0 || new_batch || link.evidence_at.is_none() {
                link.evidence_at = Some(now);
            }
            if let Some(t) = w.liveness {
                if st.silent {
                    link.silent = true;
                    // A batch would already have cleared `silent` in the kernel.
                    if new_heartbeats > 0
                        && let Some(TransitionOutcome::Applied { .. }) = apply(
                            k,
                            Operation::SourceLivenessRestored { source_id: w.source_id, link_generation: generation },
                        )
                    {
                        Counters::inc(&counters.liveness_recoveries);
                        link.silent = false;
                    }
                } else {
                    if link.silent {
                        // Silent at the previous tick, Alive now on the same link: an
                        // applied batch cleared it (review P2: counted as a recovery).
                        Counters::inc(&counters.liveness_recoveries);
                        Counters::inc(&counters.liveness_recoveries_by_batch);
                        link.silent = false;
                    }
                    if link.evidence_at.is_some_and(|e| now.saturating_sub(e) >= t)
                        && let Some(TransitionOutcome::Applied { .. }) = apply(
                            k,
                            Operation::SourceSilenceExpired { source_id: w.source_id, link_generation: generation },
                        )
                    {
                        Counters::inc(&counters.liveness_expiries);
                        link.silent = true;
                    }
                }
            }
            if let Some(t) = w.freshness {
                // Keep at the front the newest sample that is at least `t` old.
                while w.samples.get(1).is_some_and(|&(at, _)| at + t <= now) {
                    w.samples.pop_front();
                }
                if let Some(&(at, up_to)) = w.samples.front()
                    && at + t <= now
                    && up_to > 0
                    && k.freshness_candidates(w.source_id, up_to) > 0
                    && let Some(TransitionOutcome::Applied {
                        kind: scada_core::AppliedKind::FreshnessExpired { expired_tags },
                        ..
                    }) = apply(
                        k,
                        Operation::FreshnessExpired {
                            source_id: w.source_id,
                            link_generation: generation,
                            observed_up_to: up_to,
                        },
                    )
                {
                    Counters::inc(&counters.freshness_expiries);
                    counters.freshness_tags_expired.fetch_add(u64::from(expired_tags), Ordering::Relaxed);
                }
            }
        }
    }

    /// Heap bytes this supervisor can hold (watch table + preallocated sample rings).
    pub fn bound_bytes(cfg: &RuntimeConfig) -> u128 {
        let Some(sup) = cfg.supervision.as_ref() else { return 0 };
        let poll = Duration::from_millis(sup.watchdog_poll_ms);
        let table = (cfg.core.sources().len() as u128).saturating_mul(std::mem::size_of::<Option<Watch>>() as u128);
        let rings: u128 = sup
            .sources
            .iter()
            .filter_map(|p| p.freshness_timeout_ms)
            .map(|t| (freshness_samples(Duration::from_millis(t), poll) as u128).saturating_mul(SAMPLE_BYTES as u128))
            .sum();
        table.saturating_add(rings)
    }
}

#[cfg(test)]
mod tests {
    //! Controlled time: `now` is a parameter; nothing sleeps.
    use super::*;
    use scada_core::{
        BatchMode, ChannelId, Observation, ObservationBatch, Quality, SequencedInput, UncertainReason, Value,
    };

    const S: SourceId = SourceId(1);
    const MS: fn(u64) -> Duration = Duration::from_millis;

    fn cfg(live: Option<u64>, fresh: Option<u64>) -> RuntimeConfig {
        let opt = |n: &str, v: Option<u64>| v.map_or(String::new(), |x| format!("{n} = {x}\n"));
        crate::config::parse(&format!(
            "[ipc]\nsocket_path = \"/run/x/d.sock\"\n[runtime]\nwatchdog_poll_ms = 100\n\
             [[tags]]\nid = 1\nname = \"a\"\ntype = \"i64\"\n[[tags]]\nid = 2\nname = \"b\"\ntype = \"i64\"\n\
             [[sources]]\nid = 1\nname = \"s\"\n{}{}channels = [{{ channel = 1, tag = 1, type = \"i64\" }}, {{ channel = 2, tag = 2, type = \"i64\" }}]\n",
            opt("liveness_timeout_ms", live),
            opt("freshness_timeout_ms", fresh)
        ))
        .unwrap()
    }

    struct Rig {
        k: Kernel,
        sup: Supervisor,
        counters: Counters,
        hb: HeartbeatCounter,
        generation: u64,
        seq: u64,
        log: Vec<Operation>,
    }

    impl Rig {
        fn new(live: Option<u64>, fresh: Option<u64>) -> Rig {
            let c = cfg(live, fresh);
            let k = Kernel::new(Arc::new(c.core.clone()));
            let sup = Supervisor::new(&c).unwrap();
            Rig {
                k,
                sup,
                counters: Counters::default(),
                hb: Arc::new(AtomicU64::new(0)),
                generation: 0,
                seq: 0,
                log: vec![],
            }
        }
        fn op(&mut self, operation: Operation) -> Option<TransitionOutcome> {
            let operation_sequence = self.k.last_operation_sequence() + 1;
            self.k.apply(SequencedInput { operation_sequence, operation }).ok()
        }
        fn connect(&mut self) {
            self.generation += 1;
            let g = self.generation;
            self.op(Operation::SourceConnected { source_id: S, link_generation: g, source_epoch: 3 });
            self.hb = Arc::new(AtomicU64::new(0));
            let hb = self.hb.clone();
            self.sup.on_connected(0, g, Some(hb), &self.k);
        }
        fn batch(&mut self, mode: BatchMode, obs: &[(u32, i64)]) {
            self.seq += 1;
            let b = ObservationBatch {
                source_id: S,
                link_generation: self.generation,
                mode,
                source_sequence: self.seq,
                observations: obs
                    .iter()
                    .map(|&(c, v)| Observation { channel_id: ChannelId(c), value: Value::I64(v) })
                    .collect(),
            };
            assert!(matches!(self.op(Operation::ObservationBatch(b)), Some(TransitionOutcome::Applied { .. })));
        }
        fn heartbeat(&self, n: u64) {
            self.hb.fetch_add(n, Ordering::Relaxed);
        }
        fn tick(&mut self, at_ms: u64) -> Vec<Operation> {
            let before = self.log.len();
            let log = &mut self.log;
            let mut apply = |k: &mut Kernel, operation: Operation| {
                log.push(operation.clone());
                let operation_sequence = k.last_operation_sequence() + 1;
                k.apply(SequencedInput { operation_sequence, operation }).ok()
            };
            self.sup.tick(MS(at_ms), &mut self.k, &mut apply, &self.counters);
            self.log[before..].to_vec()
        }
        fn q(&self, t: u32) -> Quality {
            self.k.tag(scada_core::TagId(t)).unwrap().quality
        }
        fn silent(&self) -> bool {
            self.k.source_status(S).unwrap().silent
        }
    }

    fn is_silence(ops: &[Operation]) -> bool {
        matches!(ops, [Operation::SourceSilenceExpired { .. }])
    }

    /// Brief §7.6: exact boundaries. Evidence observed at tick 0 ms; T = 300 ms.
    #[test]
    fn liveness_boundaries_are_never_early() {
        let mut r = Rig::new(Some(300), None);
        r.connect();
        assert!(r.tick(0).is_empty(), "first tick stamps the Welcome");
        assert!(r.tick(299).is_empty(), "T - 1 ms: not yet");
        assert!(is_silence(&r.tick(300)), "exactly T: silent");
        assert!(r.silent());
        assert!(r.tick(301).is_empty(), "already silent: no repeated expiry");
        assert!(r.tick(10_000).is_empty());
        assert_eq!(
            r.k.source_status(S).unwrap().link,
            LinkState::Connected { generation: 1 },
            "no fictitious disconnect"
        );
        assert_eq!(r.counters.liveness_expiries.load(Ordering::Relaxed), 1);
    }

    /// A late tick declares the silence late, never early; evidence is stamped at the tick
    /// that sees it (upper bound of its real time).
    #[test]
    fn late_ticks_and_evidence_stamping() {
        let mut r = Rig::new(Some(300), None);
        r.connect();
        r.tick(0);
        r.heartbeat(1); // really at ~50 ms
        assert!(r.tick(250).is_empty()); // seen at 250 ms
        assert!(r.tick(549).is_empty(), "250 + 300 - 1");
        assert!(is_silence(&r.tick(2_000)), "late tick (watchdog stalled): detected late, once");
    }

    /// Brief §7.3: heartbeats keep the source alive and end a silence; they never make a
    /// tag Good. Bursts are coalesced (one piece of evidence per tick).
    #[test]
    fn heartbeats_keep_alive_and_restore_without_good() {
        let mut r = Rig::new(Some(300), Some(500));
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(0);
        for t in (100..=2_000).step_by(100) {
            r.heartbeat(1_000); // a flood between two ticks
            let ops = r.tick(t);
            assert!(!ops.iter().any(|o| matches!(o, Operation::SourceSilenceExpired { .. })), "alive at {t}");
        }
        assert!(!r.silent());
        // Freshness: the only batch is older than 500 ms: both tags stale, never Good again.
        assert_eq!(
            (r.q(1), r.q(2)),
            (Quality::Uncertain(UncertainReason::StaleData), Quality::Uncertain(UncertainReason::StaleData))
        );
        assert_eq!(r.counters.heartbeats_coalesced.load(Ordering::Relaxed), 20 * 999);
        // Silence, then a single heartbeat: restored, tags still uncertain.
        assert!(is_silence(&r.tick(2_300)));
        assert_eq!(r.q(1), Quality::Uncertain(UncertainReason::SourceSilent));
        r.heartbeat(1);
        assert!(matches!(r.tick(2_400).as_slice(), [Operation::SourceLivenessRestored { .. }]));
        assert!(!r.silent());
        assert_eq!(r.q(1), Quality::Uncertain(UncertainReason::SourceSilent), "a heartbeat is not a measurement");
        assert_eq!(r.counters.liveness_recoveries.load(Ordering::Relaxed), 1);
    }

    /// Brief §7.4: per-tag freshness with a partial Delta; boundaries of the sample ring.
    #[test]
    fn freshness_per_tag_with_partial_deltas() {
        let mut r = Rig::new(None, Some(300));
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(0); // sample (0, full)
        r.batch(BatchMode::Delta, &[(1, 5)]);
        r.tick(100); // sample (100, delta)
        assert!(r.tick(299).is_empty(), "T - 1: the full state is not yet 300 ms old");
        let ops = r.tick(300);
        assert!(matches!(ops.as_slice(), [Operation::FreshnessExpired { .. }]), "{ops:?}");
        assert_eq!((r.q(1), r.q(2)), (Quality::Good, Quality::Uncertain(UncertainReason::StaleData)));
        assert!(r.tick(350).is_empty(), "nothing more to expire");
        assert!(matches!(r.tick(400).as_slice(), [Operation::FreshnessExpired { .. }]));
        assert_eq!(r.q(1), Quality::Uncertain(UncertainReason::StaleData));
        assert_eq!(r.counters.freshness_tags_expired.load(Ordering::Relaxed), 2);
    }

    /// Brief §7.5: an identical value with a new sequence just before the deadline renews
    /// the tag; the token computed from older samples cannot expire it.
    #[test]
    fn renewal_just_before_the_deadline_wins() {
        let mut r = Rig::new(None, Some(300));
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(0);
        r.tick(100);
        r.tick(200);
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]); // same values, new sequence
        assert!(r.tick(300).is_empty(), "token = op of the old full state, markers are newer");
        assert_eq!(r.q(1), Quality::Good);
        assert!(r.tick(599).is_empty());
        assert!(!r.tick(600).is_empty(), "300 ms after the tick that saw the renewal");
    }

    /// Brief §7.7: takeover. The new link's liveness starts at its own Welcome; heartbeats
    /// of the superseded link are not read; old samples never degrade the new link.
    #[test]
    fn takeover_isolates_the_old_link() {
        let mut r = Rig::new(Some(300), Some(300));
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(0);
        let old = r.hb.clone();
        r.connect(); // generation 2; tags SourceUnavailable
        r.tick(100);
        r.batch(BatchMode::Delta, &[(1, 7)]); // new link observes tag 1
        old.fetch_add(10, Ordering::Relaxed); // old link keeps beating
        r.tick(200);
        // At 300 the sample of tick 0 (old full state) is due: tag 1 is newer, tag 2 is
        // SourceUnavailable (higher precedence): nothing to expire.
        assert!(r.tick(300).is_empty());
        assert_eq!((r.q(1), r.q(2)), (Quality::Good, Quality::Uncertain(UncertainReason::SourceUnavailable)));
        // Liveness of generation 2: evidence at 200 (batch seen); old heartbeats ignored.
        old.fetch_add(10, Ordering::Relaxed);
        let ops = r.tick(500);
        assert!(ops.iter().any(|o| matches!(o, Operation::SourceSilenceExpired { link_generation: 2, .. })), "{ops:?}");
    }

    /// Disconnected sources are not supervised; a reconnection starts a fresh liveness.
    #[test]
    fn disconnection_stops_supervision() {
        let mut r = Rig::new(Some(300), Some(300));
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(0);
        r.op(Operation::SourceDisconnected { source_id: S, link_generation: 1 });
        assert!(r.tick(10_000).is_empty());
        r.connect();
        assert!(r.tick(10_100).is_empty());
        assert!(is_silence(&r.tick(10_400)));
    }

    /// Review P2: `liveness_recoveries` counts every Silent -> Alive of the same link, by
    /// heartbeat or by an applied batch (`liveness_recoveries_by_batch`); a disconnection
    /// or a takeover of a silent link is not a recovery.
    #[test]
    fn recoveries_count_heartbeats_and_batches_not_disconnections() {
        let rec = |r: &Rig| {
            (
                r.counters.liveness_recoveries.load(Ordering::Relaxed),
                r.counters.liveness_recoveries_by_batch.load(Ordering::Relaxed),
            )
        };
        let mut r = Rig::new(Some(300), None);
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(0);
        assert!(is_silence(&r.tick(300)));
        // Recovery by a batch (it clears Silent inside its own apply, no operation).
        r.batch(BatchMode::Delta, &[(1, 5)]);
        assert!(!r.silent());
        assert!(r.tick(400).is_empty());
        assert_eq!(rec(&r), (1, 1));
        assert!(r.tick(500).is_empty(), "counted once");
        assert_eq!(rec(&r), (1, 1));
        // Recovery by a heartbeat.
        assert!(is_silence(&r.tick(800)));
        r.heartbeat(1);
        assert!(matches!(r.tick(900).as_slice(), [Operation::SourceLivenessRestored { .. }]));
        assert!(r.tick(1_000).is_empty());
        assert_eq!(rec(&r), (2, 1));
        // Silent, then disconnected: not a recovery.
        assert!(is_silence(&r.tick(1_300)));
        r.op(Operation::SourceDisconnected { source_id: S, link_generation: r.generation });
        assert!(r.tick(1_400).is_empty());
        // Silent, then taken over by a new link: not a recovery either.
        r.connect();
        r.batch(BatchMode::FullState, &[(1, 1), (2, 2)]);
        r.tick(1_500);
        assert!(is_silence(&r.tick(1_800)));
        r.connect();
        assert!(r.tick(1_900).is_empty());
        assert_eq!(rec(&r), (2, 1));
        assert_eq!(r.counters.liveness_expiries.load(Ordering::Relaxed), 4);
    }

    /// Bounded state: the sample ring never grows past its preallocated capacity whatever
    /// the tick pattern.
    #[test]
    fn sample_ring_is_bounded() {
        let mut r = Rig::new(None, Some(1_000));
        r.connect();
        let cap = freshness_samples(MS(1_000), MS(100));
        assert_eq!(cap, 12);
        for t in 0..10_000u64 {
            if t % 7 == 0 {
                r.batch(BatchMode::FullState, &[(1, t as i64), (2, 0)]);
            }
            r.tick(t * 100);
            let w = r.sup.watches[0].as_ref().unwrap();
            assert!(w.samples.len() <= cap && w.samples.capacity() == w.samples_cap.max(w.samples.capacity()));
        }
        let w = r.sup.watches[0].as_ref().unwrap();
        assert_eq!(w.samples.capacity(), VecDeque::<(Duration, u64)>::with_capacity(cap).capacity());
    }

    #[test]
    fn freshness_sample_count() {
        assert_eq!(freshness_samples(MS(300), MS(100)), 5);
        assert_eq!(freshness_samples(MS(301), MS(100)), 6);
        // Largest accepted ratio (config::MAX_FRESHNESS_POLLS).
        assert_eq!(freshness_samples(MS(4_096 * 250), MS(250)), 4_098);
    }
}
