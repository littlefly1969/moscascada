//! Discrete-event **model** (not a measurement) of the ingress policies compared in
//! VS-003: one single writer serving requests of `hv` saturating sources and one rare
//! open-loop source. It predicts how many requests (and how much modelled service time)
//! stand in front of a rare request under each policy, given explicit assumptions:
//!
//! * the I/O side is infinitely fast: a session re-submits as soon as admission allows
//!   (on a real host the I/O threads can be the bottleneck, see vs002.md §2);
//! * a high-volume client keeps `window` batches outstanding (closed loop, zero RTT);
//! * the service time of a batch is `fixed_ns + per_obs_ns * observations`;
//! * blocked submitters of the global FIFO are admitted in FIFO order of blocking (as
//!   Tokio's bounded `mpsc` does), each session holding at most one decoded batch.
//!
//! Everything is deterministic (no clock, no threads, own PRNG). The real runtime must be
//! measured: this model only ranks the policies and gives the logical bounds a test can
//! check.

use std::collections::VecDeque;

use crate::{Discipline, FairQueue, Kind, Params};

/// Ingress policy variants of the VS-003 brief §3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// A: one global bounded FIFO.
    Fifo { capacity: u32 },
    /// B: global bounded FIFO + at most `per_source` queued batches per source.
    FifoCapped { capacity: u32, per_source: u32 },
    /// C: per-source lanes (`per_source` each), round-robin with `quantum`.
    RoundRobin { per_source: u32, quantum: u32 },
    /// D: per-source lanes, deficit round-robin on cost = observations.
    Deficit { per_source: u32, quantum: u64 },
}

impl Policy {
    pub fn label(&self) -> String {
        match *self {
            Policy::Fifo { capacity } => format!("A-fifo-q{capacity}"),
            Policy::FifoCapped { capacity, per_source } => format!("B-cap{per_source}-q{capacity}"),
            Policy::RoundRobin { per_source, quantum } => format!("C-rr{quantum}-lane{per_source}"),
            Policy::Deficit { per_source, quantum } => format!("D-drr{quantum}-lane{per_source}"),
        }
    }
}

/// Workload of one model run.
#[derive(Debug, Clone, Copy)]
pub struct Workload {
    pub hv: u32,
    pub hv_obs: u32,
    pub window: u32,
    /// Rare sources (open loop, 1 observation per batch).
    pub rare: u32,
    pub rare_period_ns: u64,
    pub fixed_ns: u64,
    pub per_obs_ns: u64,
    pub duration_ns: u64,
    pub seed: u64,
}

/// Result of one model run.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub label: String,
    pub hv_batches_per_s: f64,
    pub rare_n: usize,
    /// Rare latency (arrival -> end of its service), modelled ns, sorted.
    pub rare_latency_ns: Vec<u64>,
    /// Requests served between the rare request's arrival and its own service.
    pub rare_ahead: Vec<u64>,
    pub queue_high_watermark: usize,
    pub sequencer_busy: f64,
}

impl Outcome {
    pub fn pct(v: &[u64], q: f64) -> u64 {
        if v.is_empty() {
            return 0;
        }
        let i = ((q * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1;
        v[i]
    }
}

/// SplitMix64: tiny deterministic PRNG for the rare source's jitter.
#[derive(Debug, Clone)]
pub struct SplitMix(pub u64);

impl SplitMix {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[derive(Debug, Clone, Copy)]
struct Req {
    source: u32,
    obs: u32,
    /// Arrival (rare) / submission (hv) time.
    t: u64,
    /// `served` counter at arrival.
    served_at_arrival: u64,
}

/// Queue abstraction over the four policies.
enum Q {
    Fifo { q: VecDeque<Req>, cap: usize, per_source: Option<u32> },
    Fair(FairQueue<Req>),
}

/// Run the model.
pub fn run(policy: Policy, w: &Workload) -> Outcome {
    let sources = w.hv + w.rare;
    let mut queue = match policy {
        Policy::Fifo { capacity } => {
            Q::Fifo { q: VecDeque::with_capacity(capacity as usize), cap: capacity as usize, per_source: None }
        }
        Policy::FifoCapped { capacity, per_source } => Q::Fifo {
            q: VecDeque::with_capacity(capacity as usize),
            cap: capacity as usize,
            per_source: Some(per_source),
        },
        Policy::RoundRobin { per_source, quantum } => Q::Fair(
            FairQueue::new(Params {
                lanes: sources,
                lane_capacity: per_source,
                control_capacity: 1,
                discipline: Discipline::RoundRobin { quantum },
            })
            .expect("params"),
        ),
        Policy::Deficit { per_source, quantum } => Q::Fair(
            FairQueue::new(Params {
                lanes: sources,
                lane_capacity: per_source,
                control_capacity: 1,
                discipline: Discipline::Deficit {
                    quantum: quantum.max(u64::from(w.hv_obs)),
                    max_cost: u64::from(w.hv_obs.max(1)),
                },
            })
            .expect("params"),
        ),
    };
    // Per source: batches queued in the kernel (a high-volume client refills instantly,
    // so its session always has a decoded batch while fewer than `window` are queued).
    let mut queued = vec![0u32; sources as usize];
    // Rare backlog at the client side (open loop): arrival times not yet admitted.
    let mut rare_backlog: Vec<VecDeque<(u64, u64)>> = (0..w.rare).map(|_| VecDeque::new()).collect();
    // FIFO waiters (sources blocked on the global capacity), FIFO order of blocking.
    let mut waiters: VecDeque<u32> = VecDeque::new();
    let mut waiting = vec![false; sources as usize];
    let mut rng = SplitMix(w.seed);
    let jitter = |rng: &mut SplitMix| rng.next_u64() % (w.rare_period_ns / 2).max(1);
    let mut next_rare: Vec<u64> = (0..w.rare).map(|_| jitter(&mut rng)).collect();
    let (mut t, mut served, mut hv_served, mut busy) = (0u64, 0u64, 0u64, 0u64);
    let mut rare_lat = Vec::new();
    let mut rare_ahead = Vec::new();
    let mut hw = 0usize;
    let warmup = w.duration_ns / 10;

    let is_rare = |s: u32| s >= w.hv;
    while t < w.duration_ns {
        // Rare arrivals up to `t`.
        for r in 0..w.rare {
            while next_rare[r as usize] <= t {
                rare_backlog[r as usize].push_back((next_rare[r as usize], served));
                next_rare[r as usize] += w.rare_period_ns / 2 + jitter(&mut rng);
            }
        }
        // Admission: every session submits while its policy allows.
        admit(&mut queue, w, t, &mut queued, &mut rare_backlog, &mut waiters, &mut waiting, &is_rare);
        let len = match &queue {
            Q::Fifo { q, .. } => q.len(),
            Q::Fair(f) => f.len(),
        };
        hw = hw.max(len);
        let next = match &mut queue {
            Q::Fifo { q, .. } => q.pop_front(),
            Q::Fair(f) => f.pop().map(|p| p.item),
        };
        let Some(req) = next else {
            // Idle: jump to the next rare arrival.
            t = next_rare.iter().copied().min().unwrap_or(w.duration_ns).max(t + 1);
            continue;
        };
        let cost = w.fixed_ns + w.per_obs_ns * u64::from(req.obs);
        t += cost;
        busy += cost;
        served += 1;
        let s = req.source as usize;
        queued[s] -= 1;
        if is_rare(req.source) {
            if req.t >= warmup {
                rare_lat.push(t - req.t);
                rare_ahead.push(served - 1 - req.served_at_arrival);
            }
        } else if t >= warmup {
            hv_served += 1;
        }
    }
    rare_lat.sort_unstable();
    rare_ahead.sort_unstable();
    let secs = (w.duration_ns - warmup) as f64 / 1e9;
    Outcome {
        label: policy.label(),
        hv_batches_per_s: hv_served as f64 / secs,
        rare_n: rare_lat.len(),
        rare_latency_ns: rare_lat,
        rare_ahead,
        queue_high_watermark: hw,
        sequencer_busy: busy as f64 / t as f64,
    }
}

#[allow(clippy::too_many_arguments)]
fn admit(
    queue: &mut Q,
    w: &Workload,
    t: u64,
    queued: &mut [u32],
    rare_backlog: &mut [VecDeque<(u64, u64)>],
    waiters: &mut VecDeque<u32>,
    waiting: &mut [bool],
    is_rare: &dyn Fn(u32) -> bool,
) {
    let sources = w.hv + w.rare;
    // Does source `s` have a decoded batch it wants to submit?
    let wants = |s: u32, queued: &[u32], rare_backlog: &[VecDeque<(u64, u64)>]| {
        if is_rare(s) {
            // Rare: everything not yet queued is in its client backlog.
            !rare_backlog[(s - w.hv) as usize].is_empty()
        } else {
            queued[s as usize] < w.window
        }
    };
    let make = |s: u32, rare_backlog: &mut [VecDeque<(u64, u64)>]| {
        if is_rare(s) {
            let (arr, at) = rare_backlog[(s - w.hv) as usize].pop_front().expect("wants");
            Req { source: s, obs: 1, t: arr, served_at_arrival: at }
        } else {
            Req { source: s, obs: w.hv_obs, t, served_at_arrival: 0 }
        }
    };
    match queue {
        Q::Fifo { q, cap, per_source } => {
            let cap_ok = |s: u32, queued: &[u32]| per_source.is_none_or(|c| queued[s as usize] < c);
            // Sources that are not blocked yet try in index order; blocked ones queue up.
            for s in 0..sources {
                if !waiting[s as usize] && wants(s, queued, rare_backlog) && cap_ok(s, queued) {
                    waiters.push_back(s);
                    waiting[s as usize] = true;
                }
            }
            // Admit waiters in FIFO order while there is global room.
            while q.len() < *cap {
                let Some(s) = waiters.pop_front() else { break };
                waiting[s as usize] = false;
                let req = make(s, rare_backlog);
                queued[s as usize] += 1;
                q.push_back(req);
                // The session decodes its next batch and blocks again at the tail.
                if wants(s, queued, rare_backlog) && cap_ok(s, queued) {
                    waiters.push_back(s);
                    waiting[s as usize] = true;
                }
            }
        }
        Q::Fair(f) => {
            for s in 0..sources {
                while f.has_room(s) && wants(s, queued, rare_backlog) {
                    let req = make(s, rare_backlog);
                    queued[s as usize] += 1;
                    let cost = u64::from(req.obs.max(1));
                    if f.push(s, Kind::Data, cost, req).is_err() {
                        unreachable!("has_room checked");
                    }
                }
            }
        }
    }
}
