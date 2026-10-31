//! Pure model of the VS-003 ingress scheduler (ADR 0010).
//!
//! This crate is deliberately separate from `scada-core` and from the runtime: it has no
//! Tokio, no clock, no I/O, no lock and no `unsafe`. It decides **in which order** admitted
//! requests are handed to the single writer; it never looks inside a request and never
//! calls `Kernel::apply`. The runtime is expected to own one [`FairQueue`] behind its own
//! synchronization and to call [`FairQueue::pop`] from the sequencer thread only.
//!
//! # Structure
//!
//! * One **lane** per source (dense index `0..lanes`), a bounded FIFO. Requests of one
//!   lane are always served in push order (intra-source order is preserved).
//! * A **ready ring** holds every non-empty lane **at most once** (flag `queued`). It has
//!   a fixed capacity equal to the number of lanes, so it never grows and pushing into it
//!   can never fail.
//! * [`Discipline::RoundRobin`]: each lane turn serves at most `quantum` requests.
//!   [`Discipline::Deficit`]: deficit round-robin (Shreedhar & Varghese, 1996) on a
//!   caller-supplied cost; each turn adds `quantum` credits.
//!
//! # Admission
//!
//! A **data** push is refused with [`PushError::Full`] when its lane already holds
//! `lane_capacity` data requests; the value is handed back, never dropped (cancel-safe by
//! construction: nothing is lost if the caller gives up). **Control** pushes (connect /
//! disconnect barriers) do not count against `lane_capacity` but against a separate,
//! equally fixed `control_capacity`, so a lane full of data never blocks the control event
//! of the same source, and a control event is still ordered **after** the data already in
//! the lane (this is the disconnect barrier of ADR 0010 §5).
//!
//! # Guarantees (logical, proved by the tests in `tests/`)
//!
//! * Bounded: `len() <= lanes * (lane_capacity + control_capacity)`; no allocation after
//!   [`FairQueue::new`] (all `VecDeque`s are created with their final capacity and never
//!   exceed it).
//! * No loss and no double service: every pushed value is returned exactly once, by
//!   [`FairQueue::pop`] or [`FairQueue::drain`].
//! * Per-lane FIFO order.
//! * Fair progress (round-robin): once a lane's head request is in the ring, at most
//!   `(active_lanes - 1) * quantum` requests of other lanes are served before it, where
//!   `active_lanes` is bounded by the number of lanes. With deficit round-robin the bound
//!   is in cost credits, `(active_lanes - 1) * (quantum + max_cost)`. These are bounds in
//!   **requests (or credits) served**, not a wall-clock deadline.
//! * Wake-up: [`Popped::freed`] is `true` exactly when the pop took a data lane from full
//!   to not full, i.e. when a producer refused with `Full` on that lane may now succeed.

#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fmt;

pub mod sim;

/// Service discipline among non-empty lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Discipline {
    /// Plain round-robin: at most `quantum` requests per lane turn (`quantum >= 1`).
    RoundRobin { quantum: u32 },
    /// Deficit round-robin: each turn adds `quantum` credits to the lane; a request is
    /// served while its cost fits the lane's credits. `quantum >= max_cost`, so every turn
    /// serves at least one request (O(1) per pop, no empty rounds).
    Deficit { quantum: u64, max_cost: u64 },
}

/// Construction parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Params {
    pub lanes: u32,
    /// Max queued **data** requests per lane (`>= 1`).
    pub lane_capacity: u32,
    /// Max queued **control** requests per lane (`>= 1`).
    pub control_capacity: u32,
    pub discipline: Discipline,
}

/// Invalid [`Params`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamError(pub &'static str);

impl fmt::Display for ParamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ParamError {}

/// Kind of a queued request. Only the admission accounting differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Data,
    Control,
}

/// A refused push; the value is always handed back.
#[derive(Debug, PartialEq, Eq)]
pub enum PushError<T> {
    /// The lane holds `lane_capacity` data (or `control_capacity` control) requests.
    Full(T),
    /// No such lane.
    UnknownLane(T),
    /// Deficit round-robin only: `cost > max_cost` (or `cost == 0`).
    InvalidCost(T),
}

impl<T> PushError<T> {
    /// The refused value.
    pub fn into_inner(self) -> T {
        match self {
            PushError::Full(t) | PushError::UnknownLane(t) | PushError::InvalidCost(t) => t,
        }
    }
}

/// One served request.
#[derive(Debug, PartialEq, Eq)]
pub struct Popped<T> {
    pub lane: u32,
    pub kind: Kind,
    pub item: T,
    /// The lane went from `lane_capacity` data requests to fewer: wake its producer.
    pub freed: bool,
}

#[derive(Debug)]
struct Entry<T> {
    item: T,
    kind: Kind,
    cost: u64,
}

#[derive(Debug)]
struct Lane<T> {
    items: VecDeque<Entry<T>>,
    data: u32,
    control: u32,
    /// In the ready ring or being served (`FairQueue::turn`).
    queued: bool,
    deficit: u64,
}

/// Bounded per-source lanes served round-robin. See the crate documentation.
#[derive(Debug)]
pub struct FairQueue<T> {
    lanes: Box<[Lane<T>]>,
    ready: VecDeque<u32>,
    params: Params,
    len: usize,
    /// Lane currently being served and, for round-robin, how many requests it may still
    /// take in this turn. `None` between turns.
    turn: Option<(u32, u32)>,
    high_watermark: usize,
}

impl<T> FairQueue<T> {
    /// Allocate every lane and the ready ring with their final capacity.
    pub fn new(params: Params) -> Result<FairQueue<T>, ParamError> {
        if params.lanes == 0 {
            return Err(ParamError("lanes must be >= 1"));
        }
        if params.lane_capacity == 0 || params.control_capacity == 0 {
            return Err(ParamError("lane_capacity and control_capacity must be >= 1"));
        }
        match params.discipline {
            Discipline::RoundRobin { quantum: 0 } => return Err(ParamError("quantum must be >= 1")),
            Discipline::Deficit { quantum, max_cost } if max_cost == 0 || quantum < max_cost => {
                return Err(ParamError("deficit round-robin needs 1 <= max_cost <= quantum"));
            }
            _ => {}
        }
        let per_lane = params.lane_capacity as usize + params.control_capacity as usize;
        let lanes = (0..params.lanes)
            .map(|_| Lane { items: VecDeque::with_capacity(per_lane), data: 0, control: 0, queued: false, deficit: 0 })
            .collect();
        Ok(FairQueue {
            lanes,
            ready: VecDeque::with_capacity(params.lanes as usize),
            params,
            len: 0,
            turn: None,
            high_watermark: 0,
        })
    }

    pub fn params(&self) -> Params {
        self.params
    }

    /// Queued requests (data + control) over all lanes.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Largest `len()` ever observed.
    pub fn high_watermark(&self) -> usize {
        self.high_watermark
    }

    /// Queued data requests of `lane` (0 for an unknown lane).
    pub fn lane_data_len(&self, lane: u32) -> u32 {
        self.lanes.get(lane as usize).map_or(0, |l| l.data)
    }

    /// Non-empty lanes (ring + the one being served).
    pub fn active_lanes(&self) -> usize {
        self.ready.len() + usize::from(self.turn.is_some())
    }

    /// Whether a data push into `lane` would be accepted now.
    pub fn has_room(&self, lane: u32) -> bool {
        self.lanes.get(lane as usize).is_some_and(|l| l.data < self.params.lane_capacity)
    }

    /// Admit `item` at the tail of `lane`. `cost` is used by deficit round-robin only.
    pub fn push(&mut self, lane: u32, kind: Kind, cost: u64, item: T) -> Result<(), PushError<T>> {
        if let Discipline::Deficit { max_cost, .. } = self.params.discipline
            && (cost == 0 || cost > max_cost)
        {
            return Err(PushError::InvalidCost(item));
        }
        let (cap_data, cap_ctl) = (self.params.lane_capacity, self.params.control_capacity);
        let Some(l) = self.lanes.get_mut(lane as usize) else { return Err(PushError::UnknownLane(item)) };
        match kind {
            Kind::Data if l.data >= cap_data => return Err(PushError::Full(item)),
            Kind::Control if l.control >= cap_ctl => return Err(PushError::Full(item)),
            Kind::Data => l.data += 1,
            Kind::Control => l.control += 1,
        }
        l.items.push_back(Entry { item, kind, cost });
        if !l.queued {
            l.queued = true;
            // Capacity = lanes and each lane is in the ring at most once: never grows.
            self.ready.push_back(lane);
        }
        self.len += 1;
        self.high_watermark = self.high_watermark.max(self.len);
        Ok(())
    }

    /// Serve the next request, or `None` when every lane is empty. O(1).
    pub fn pop(&mut self) -> Option<Popped<T>> {
        loop {
            let (lane, left) = match self.turn.take() {
                Some(t) => t,
                None => {
                    let lane = self.ready.pop_front()?;
                    let left = match self.params.discipline {
                        Discipline::RoundRobin { quantum } => quantum,
                        Discipline::Deficit { quantum, .. } => {
                            let l = &mut self.lanes[lane as usize];
                            l.deficit = l.deficit.saturating_add(quantum);
                            0
                        }
                    };
                    (lane, left)
                }
            };
            let cap_data = self.params.lane_capacity;
            let discipline = self.params.discipline;
            let l = &mut self.lanes[lane as usize];
            // A lane is queued only while non-empty.
            let head_cost = l.items.front().map_or(0, |e| e.cost);
            let serve = match discipline {
                Discipline::RoundRobin { .. } => left > 0,
                Discipline::Deficit { .. } => head_cost <= l.deficit,
            };
            if !serve {
                // DRR: credits exhausted for this turn; the lane stays non-empty.
                self.ready.push_back(lane);
                continue;
            }
            let e = l.items.pop_front()?;
            if let Discipline::Deficit { .. } = discipline {
                l.deficit -= e.cost;
            }
            let freed = match e.kind {
                Kind::Data => {
                    let was_full = l.data == cap_data;
                    l.data -= 1;
                    was_full
                }
                Kind::Control => {
                    l.control -= 1;
                    false
                }
            };
            self.len -= 1;
            if l.items.is_empty() {
                l.queued = false;
                l.deficit = 0; // DRR: an idle lane keeps no credit
            } else {
                match discipline {
                    Discipline::RoundRobin { .. } if left > 1 => self.turn = Some((lane, left - 1)),
                    Discipline::RoundRobin { .. } => self.ready.push_back(lane),
                    Discipline::Deficit { .. } => self.turn = Some((lane, 0)),
                }
            }
            return Some(Popped { lane, kind: e.kind, item: e.item, freed });
        }
    }

    /// Remove every queued request (shutdown), lane by lane in service order of the ring,
    /// each lane in FIFO order. Afterwards the queue is empty and reusable.
    pub fn drain(&mut self) -> Vec<(u32, Kind, T)> {
        let mut out = Vec::with_capacity(self.len);
        let order: Vec<u32> = self.turn.take().map(|t| t.0).into_iter().chain(self.ready.drain(..)).collect();
        for lane in order {
            let l = &mut self.lanes[lane as usize];
            out.extend(l.items.drain(..).map(|e| (lane, e.kind, e.item)));
            l.data = 0;
            l.control = 0;
            l.queued = false;
            l.deficit = 0;
        }
        self.len = 0;
        out
    }

    /// Internal consistency check used by the tests (O(lanes)).
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut total = 0usize;
        let mut in_ring = vec![0u32; self.lanes.len()];
        for &r in &self.ready {
            in_ring[r as usize] += 1;
        }
        if let Some((t, _)) = self.turn {
            in_ring[t as usize] += 1;
        }
        let per_lane = self.params.lane_capacity as usize + self.params.control_capacity as usize;
        for (i, l) in self.lanes.iter().enumerate() {
            let d = l.items.iter().filter(|e| e.kind == Kind::Data).count() as u32;
            let c = l.items.len() as u32 - d;
            if d != l.data || c != l.control {
                return Err(format!("lane {i}: counters {}/{} vs items {d}/{c}", l.data, l.control));
            }
            if l.data > self.params.lane_capacity || l.control > self.params.control_capacity {
                return Err(format!("lane {i}: over capacity"));
            }
            if l.items.capacity() < per_lane {
                return Err(format!("lane {i}: capacity shrank"));
            }
            let want = u32::from(!l.items.is_empty());
            if in_ring[i] != want || l.queued != (want == 1) {
                return Err(format!(
                    "lane {i}: ring presence {} queued {} len {}",
                    in_ring[i],
                    l.queued,
                    l.items.len()
                ));
            }
            total += l.items.len();
        }
        if total != self.len {
            return Err(format!("len {} vs items {total}", self.len));
        }
        if self.ready.capacity() < self.lanes.len() {
            return Err("ready ring capacity shrank".into());
        }
        Ok(())
    }
}
