//! Property tests of the pure scheduler model against a naive reference (oracle).
//!
//! Checked on random sequences of push (data/control) / pop / drain:
//! identical service order to the oracle, internal invariants, boundedness, no loss and no
//! double service, per-lane FIFO order, the fair-progress bound (in requests or credits
//! served, not wall-clock) and the absence of lost wake-ups for a producer protocol with
//! Tokio `Notify::notify_one` permit semantics.

use std::collections::{HashSet, VecDeque};

use proptest::prelude::*;
use scada_sched::{Discipline, FairQueue, Kind, Params, PushError};

/// Naive reference: explicit list of active lanes in activation order, O(lanes) per op.
struct Oracle {
    lanes: Vec<VecDeque<(u64, Kind, u64)>>,
    active: Vec<u32>,
    cap: u32,
    ctl: u32,
    discipline: Discipline,
    current: Option<u32>,
    left: u32,
    deficit: Vec<u64>,
}

impl Oracle {
    fn new(p: Params) -> Oracle {
        Oracle {
            lanes: (0..p.lanes).map(|_| VecDeque::new()).collect(),
            active: Vec::new(),
            cap: p.lane_capacity,
            ctl: p.control_capacity,
            discipline: p.discipline,
            current: None,
            left: 0,
            deficit: vec![0; p.lanes as usize],
        }
    }

    fn count(&self, lane: u32, kind: Kind) -> u32 {
        self.lanes[lane as usize].iter().filter(|e| e.1 == kind).count() as u32
    }

    fn push(&mut self, lane: u32, kind: Kind, cost: u64, id: u64) -> bool {
        let limit = if kind == Kind::Data { self.cap } else { self.ctl };
        if self.count(lane, kind) >= limit {
            return false;
        }
        let was_empty = self.lanes[lane as usize].is_empty();
        self.lanes[lane as usize].push_back((id, kind, cost));
        if was_empty {
            self.active.push(lane);
        }
        true
    }

    fn pop(&mut self) -> Option<(u32, u64)> {
        loop {
            let lane = match self.current {
                Some(l) => l,
                None => {
                    if self.active.is_empty() {
                        return None;
                    }
                    let l = self.active.remove(0);
                    match self.discipline {
                        Discipline::RoundRobin { quantum } => self.left = quantum,
                        Discipline::Deficit { quantum, .. } => self.deficit[l as usize] += quantum,
                    }
                    self.current = Some(l);
                    l
                }
            };
            let head = *self.lanes[lane as usize].front().expect("active lane is non-empty");
            let ok = match self.discipline {
                Discipline::RoundRobin { .. } => self.left > 0,
                Discipline::Deficit { .. } => head.2 <= self.deficit[lane as usize],
            };
            if !ok {
                self.current = None;
                self.active.push(lane);
                continue;
            }
            self.lanes[lane as usize].pop_front();
            match self.discipline {
                Discipline::RoundRobin { .. } => self.left -= 1,
                Discipline::Deficit { .. } => self.deficit[lane as usize] -= head.2,
            }
            if self.lanes[lane as usize].is_empty() {
                self.current = None;
                self.deficit[lane as usize] = 0;
            } else if matches!(self.discipline, Discipline::RoundRobin { .. }) && self.left == 0 {
                self.current = None;
                self.active.push(lane);
            }
            return Some((lane, head.0));
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Push { lane: u32, control: bool, cost: u64 },
    Pop,
    Drain,
}

fn ops(lanes: u32, max_cost: u64) -> impl Strategy<Value = Vec<Op>> {
    let op = prop_oneof![
        5 => (0..lanes, prop::bool::weighted(0.1), 1..=max_cost)
            .prop_map(|(lane, control, cost)| Op::Push { lane, control, cost }),
        4 => Just(Op::Pop),
        1 => Just(Op::Pop),
    ];
    // A rare drain (shutdown) in the middle.
    (prop::collection::vec(op, 1..400), prop::bool::weighted(0.1)).prop_map(|(mut v, drain)| {
        if drain {
            let at = v.len() / 2;
            v.insert(at, Op::Drain);
        }
        v
    })
}

fn discipline() -> impl Strategy<Value = (Discipline, u64)> {
    prop_oneof![
        (1u32..=4).prop_map(|q| (Discipline::RoundRobin { quantum: q }, 1)),
        (1u64..=128, 0u64..=128).prop_map(|(m, extra)| (Discipline::Deficit { quantum: m + extra, max_cost: m }, m)),
    ]
}

struct Run {
    q: FairQueue<u64>,
    oracle: Oracle,
    next_id: u64,
    pushed: HashSet<u64>,
    done: HashSet<u64>,
    /// Per lane, ids in push order not yet served (FIFO check).
    order: Vec<VecDeque<u64>>,
    /// Per lane: (served counter, cost served) when its current head became head.
    head_since: Vec<Option<(u64, u64)>>,
    served: u64,
    cost_served: u64,
    costs: std::collections::HashMap<u64, u64>,
}

fn check(params: Params, max_cost: u64, ops: &[Op]) -> Result<(), TestCaseError> {
    let mut r = Run {
        q: FairQueue::new(params).expect("params"),
        oracle: Oracle::new(params),
        next_id: 0,
        pushed: HashSet::new(),
        done: HashSet::new(),
        order: (0..params.lanes).map(|_| VecDeque::new()).collect(),
        head_since: vec![None; params.lanes as usize],
        served: 0,
        cost_served: 0,
        costs: Default::default(),
    };
    let bound_items = params.lanes as usize * (params.lane_capacity + params.control_capacity) as usize;
    let others = u64::from(params.lanes - 1);
    for op in ops {
        match *op {
            Op::Push { lane, control, cost } => {
                let kind = if control { Kind::Control } else { Kind::Data };
                let id = r.next_id;
                r.next_id += 1;
                let cost = match params.discipline {
                    Discipline::RoundRobin { .. } => cost,
                    Discipline::Deficit { .. } => cost.min(max_cost),
                };
                let had_room = match kind {
                    Kind::Data => r.q.has_room(lane),
                    Kind::Control => true,
                };
                let ok_oracle = r.oracle.push(lane, kind, cost, id);
                match r.q.push(lane, kind, cost, id) {
                    Ok(()) => {
                        prop_assert!(ok_oracle, "oracle refused a push the model accepted");
                        prop_assert!(had_room);
                        r.pushed.insert(id);
                        r.costs.insert(id, cost);
                        if r.order[lane as usize].is_empty() {
                            r.head_since[lane as usize] = Some((r.served, r.cost_served));
                        }
                        r.order[lane as usize].push_back(id);
                    }
                    Err(PushError::Full(v)) => {
                        prop_assert!(!ok_oracle, "model refused a push the oracle accepted");
                        prop_assert_eq!(v, id, "refused value handed back");
                    }
                    Err(e) => prop_assert!(false, "unexpected {:?}", e),
                }
            }
            Op::Pop => {
                let lens: Vec<u32> = (0..params.lanes).map(|l| r.q.lane_data_len(l)).collect();
                let got = r.q.pop();
                let want = r.oracle.pop();
                prop_assert_eq!(got.as_ref().map(|p| (p.lane, p.item)), want, "service order differs from oracle");
                if let Some(p) = got {
                    prop_assert!(r.done.insert(p.item), "served twice");
                    prop_assert!(r.pushed.contains(&p.item), "served a value never pushed");
                    let lane = p.lane as usize;
                    prop_assert_eq!(r.order[lane].pop_front(), Some(p.item), "per-lane FIFO violated");
                    let want_freed = p.kind == Kind::Data && lens[lane] == params.lane_capacity;
                    prop_assert_eq!(p.freed, want_freed, "freed flag");
                    // Fair progress: other lanes served since this item became head.
                    let (s0, c0) = r.head_since[lane].expect("head time");
                    let other_items = r.served - s0;
                    let other_cost = r.cost_served - c0;
                    match params.discipline {
                        Discipline::RoundRobin { quantum } => {
                            prop_assert!(
                                other_items <= others * u64::from(quantum),
                                "RR bound: {} others served, bound {}",
                                other_items,
                                others * u64::from(quantum)
                            );
                        }
                        Discipline::Deficit { quantum, max_cost } => {
                            prop_assert!(
                                other_cost <= others * (quantum + max_cost),
                                "DRR bound: {} credits served, bound {}",
                                other_cost,
                                others * (quantum + max_cost)
                            );
                        }
                    }
                    r.served += 1;
                    r.cost_served += r.costs[&p.item];
                    r.head_since[lane] = if r.order[lane].is_empty() { None } else { Some((r.served, r.cost_served)) };
                }
            }
            Op::Drain => {
                let drained = r.q.drain();
                let mut per_lane: Vec<VecDeque<u64>> = (0..params.lanes).map(|_| VecDeque::new()).collect();
                for (lane, _, id) in drained {
                    prop_assert!(r.done.insert(id), "drained a served value");
                    per_lane[lane as usize].push_back(id);
                }
                prop_assert_eq!(&per_lane, &r.order, "drain lost values or broke FIFO");
                for o in &mut r.order {
                    o.clear();
                }
                r.head_since.iter_mut().for_each(|h| *h = None);
                r.oracle = Oracle::new(params);
            }
        }
        prop_assert!(r.q.check_invariants().is_ok(), "{:?}", r.q.check_invariants());
        prop_assert!(r.q.len() <= bound_items);
        prop_assert!(r.q.active_lanes() <= params.lanes as usize);
    }
    // Final drain: nothing lost.
    for (_, _, id) in r.q.drain() {
        prop_assert!(r.done.insert(id));
    }
    prop_assert_eq!(r.done, r.pushed, "every pushed value is served or drained exactly once");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    #[test]
    fn matches_oracle_and_keeps_invariants(
        lanes in 1u32..=8,
        cap in 1u32..=8,
        ctl in 1u32..=3,
        (disc, max_cost) in discipline(),
        seq in ops(8, 128),
    ) {
        let params = Params { lanes, lane_capacity: cap, control_capacity: ctl, discipline: disc };
        let seq: Vec<Op> = seq
            .into_iter()
            .map(|o| match o {
                Op::Push { lane, control, cost } => Op::Push { lane: lane % lanes, control, cost },
                o => o,
            })
            .collect();
        check(params, max_cost, &seq)?;
    }
}

/// Producer protocol with Tokio `Notify::notify_one` semantics, with the race window made
/// explicit: a refused producer first *decides* to wait (`Checking`) and only in a later
/// step *registers* (`Parked`). A notification with no registered waiter is stored as one
/// permit (`store_permit`); registering consumes it and retries instead of parking. The
/// consumer notifies on `freed`. Invariant: a parked producer's lane is full — otherwise
/// its wake-up was lost.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Prod {
    Idle,
    Checking,
    Parked,
}

fn wake_protocol(ops: &[(bool, u32)], lanes: u32, cap: u32, store_permit: bool) -> Result<(), String> {
    let mut q: FairQueue<u32> = FairQueue::new(Params {
        lanes,
        lane_capacity: cap,
        control_capacity: 1,
        discipline: Discipline::RoundRobin { quantum: 1 },
    })
    .map_err(|e| e.to_string())?;
    let mut prod = vec![Prod::Idle; lanes as usize];
    let mut permit = vec![false; lanes as usize];
    for &(producer_step, lane) in ops {
        let lane = lane % lanes;
        let l = lane as usize;
        if producer_step {
            prod[l] = match prod[l] {
                Prod::Idle => match q.push(lane, Kind::Data, 1, lane) {
                    Ok(()) => Prod::Idle,
                    Err(PushError::Full(_)) => Prod::Checking,
                    Err(e) => return Err(format!("{e:?}")),
                },
                Prod::Checking if permit[l] => {
                    permit[l] = false;
                    Prod::Idle // woken (maybe spuriously): it re-checks on its next step
                }
                Prod::Checking => Prod::Parked,
                Prod::Parked => Prod::Parked,
            };
        } else if let Some(p) = q.pop()
            && p.freed
        {
            let pl = p.lane as usize;
            if prod[pl] == Prod::Parked {
                prod[pl] = Prod::Idle;
            } else if store_permit {
                permit[pl] = true;
            }
        }
        for (i, s) in prod.iter().enumerate() {
            if *s == Prod::Parked && q.has_room(i as u32) {
                return Err(format!("lost wake-up on lane {i}"));
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, ..ProptestConfig::default() })]

    #[test]
    fn no_lost_wakeups(ops in prop::collection::vec((any::<bool>(), 0u32..4), 1..300), cap in 1u32..=3) {
        prop_assert_eq!(wake_protocol(&ops, 4, cap, true), Ok(()));
    }
}

#[test]
fn the_wakeup_test_detects_a_protocol_without_stored_permits() {
    // Mutation check of the test itself. Lane capacity 1: push (ok, full), push (Full ->
    // Checking), consumer pops (freed while nobody is registered), producer registers.
    let race = [(true, 0), (true, 0), (false, 0), (true, 0)];
    assert_eq!(wake_protocol(&race, 1, 1, true), Ok(()));
    assert!(wake_protocol(&race, 1, 1, false).is_err(), "the race must be detected without permits");
}

#[test]
fn rare_lane_admitted_while_others_full_and_served_within_bound() {
    // 16 noisy lanes always full (capacity 4) and one rare lane: the rare request is
    // admitted immediately and served after at most 16 other requests (quantum 1).
    let lanes = 17;
    let mut q: FairQueue<(u32, u64)> = FairQueue::new(Params {
        lanes,
        lane_capacity: 4,
        control_capacity: 1,
        discipline: Discipline::RoundRobin { quantum: 1 },
    })
    .expect("params");
    let mut id = 0u64;
    let refill = |q: &mut FairQueue<(u32, u64)>, id: &mut u64| {
        for l in 0..16 {
            while q.push(l, Kind::Data, 1, (l, *id)).is_ok() {
                *id += 1;
            }
        }
    };
    refill(&mut q, &mut id);
    for round in 0..1000u64 {
        // Serve a variable number of noisy requests first (rare arrives mid-round).
        for _ in 0..(round % 23) {
            let _ = q.pop();
            refill(&mut q, &mut id);
        }
        assert!(q.push(16, Kind::Data, 1, (16, u64::MAX)).is_ok(), "rare refused although others are full");
        let mut ahead = 0;
        loop {
            let p = q.pop().expect("non-empty");
            refill(&mut q, &mut id);
            if p.lane == 16 {
                break;
            }
            ahead += 1;
        }
        assert!(ahead <= 16, "rare served after {ahead} others");
    }
}

#[test]
fn control_event_is_admitted_on_a_full_lane_and_ordered_after_its_data() {
    let mut q: FairQueue<&str> = FairQueue::new(Params {
        lanes: 2,
        lane_capacity: 2,
        control_capacity: 1,
        discipline: Discipline::RoundRobin { quantum: 1 },
    })
    .expect("params");
    q.push(0, Kind::Data, 1, "b1").expect("b1");
    q.push(0, Kind::Data, 1, "b2").expect("b2");
    assert!(matches!(q.push(0, Kind::Data, 1, "b3"), Err(PushError::Full("b3"))));
    q.push(0, Kind::Control, 1, "disconnect").expect("control bypasses the data cap");
    assert!(matches!(q.push(0, Kind::Control, 1, "x"), Err(PushError::Full("x"))), "control cap is bounded");
    let served: Vec<&str> = std::iter::from_fn(|| q.pop().map(|p| p.item)).collect();
    assert_eq!(served, ["b1", "b2", "disconnect"], "barrier: control after the lane's data");
}

#[test]
fn params_are_validated() {
    let p = |lanes, cap, ctl, discipline| Params { lanes, lane_capacity: cap, control_capacity: ctl, discipline };
    let rr = Discipline::RoundRobin { quantum: 1 };
    assert!(FairQueue::<u8>::new(p(0, 1, 1, rr)).is_err());
    assert!(FairQueue::<u8>::new(p(1, 0, 1, rr)).is_err());
    assert!(FairQueue::<u8>::new(p(1, 1, 0, rr)).is_err());
    assert!(FairQueue::<u8>::new(p(1, 1, 1, Discipline::RoundRobin { quantum: 0 })).is_err());
    assert!(FairQueue::<u8>::new(p(1, 1, 1, Discipline::Deficit { quantum: 10, max_cost: 11 })).is_err());
    assert!(FairQueue::<u8>::new(p(1, 1, 1, Discipline::Deficit { quantum: 10, max_cost: 0 })).is_err());
    let mut q = FairQueue::<u8>::new(p(1, 1, 1, Discipline::Deficit { quantum: 10, max_cost: 10 })).expect("ok");
    assert!(matches!(q.push(0, Kind::Data, 11, 1), Err(PushError::InvalidCost(1))));
    assert!(matches!(q.push(0, Kind::Data, 0, 1), Err(PushError::InvalidCost(1))));
    assert!(matches!(q.push(5, Kind::Data, 1, 1), Err(PushError::UnknownLane(1))));
}
