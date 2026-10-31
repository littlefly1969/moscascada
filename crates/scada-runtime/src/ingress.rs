//! VS-003 ingress (ADR 0010): how admitted requests reach the single writer.
//!
//! * `fifo` (A, default): one bounded Tokio `mpsc` (`input_queue_capacity`), VS-002.
//! * `source_capped` (B): the same `mpsc`, but a batch first takes one of
//!   `per_source_pending_limit` permits of its source's semaphore. Not fair scheduling.
//! * `source_fair` (C): a batch takes a permit of its source, then one of
//!   `input_queue_capacity` global permits, then is pushed into its source's lane of a
//!   [`scada_sched::FairQueue`] (round-robin, one request per turn) under a mutex; the
//!   sequencer thread waits on a `Condvar`.
//!
//! Ownership: a request belongs to the kernel once `submit` returned `Ok`. Before that it
//! belongs to the session; every wait inside `submit` is cancel-safe (Tokio
//! `Semaphore::acquire_owned`, `mpsc::Sender::send`): a cancelled submit owns nothing and
//! consumed no permit. The permits travel inside the request ([`Permits`]) and are
//! released when the sequencer drops it after `apply` (or at shutdown), so a permit can
//! leak only together with its request.
//!
//! Connect / Disconnect go into their source's lane as **control** requests (no permits):
//! a `Hello` is admitted even when the data path is saturated, and a `Disconnect` is
//! served after every batch the same session admitted (lane FIFO = barrier, ADR 0010
//! §4.5). Introspection uses one extra control lane. Each session has at most one control
//! request queued at a time, so control entries are bounded by `max_driver_connections`
//! (+4 introspection sessions).

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use scada_sched::{Discipline, FairQueue, Kind, Params, PushError};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::config::{IngressPolicy, RuntimeConfig};
use crate::sequencer::Request;

/// Permits held by an admitted batch; released when the request is dropped.
#[derive(Debug, Default)]
pub struct Permits {
    _source: Option<OwnedSemaphorePermit>,
    _global: Option<OwnedSemaphorePermit>,
}

/// Introspection sessions (fixed in `server::serve_introspection`).
const INTROSPECTION_SESSIONS: u32 = 4;

/// Producer side, owned by the `Server` (one instance; dropping it closes the ingress).
#[derive(Debug)]
pub enum Ingress {
    Fifo(mpsc::Sender<Request>),
    Capped { tx: mpsc::Sender<Request>, sources: Arc<SourceSemaphores> },
    Fair(FairSender),
}

/// Sequencer side.
#[derive(Debug)]
pub enum Inbox {
    Fifo(mpsc::Receiver<Request>),
    Fair(FairReceiver),
}

/// One semaphore per configured source (config order = dense index).
#[derive(Debug)]
pub struct SourceSemaphores {
    per_source: Box<[Arc<Semaphore>]>,
    cfg: Arc<RuntimeConfig>,
}

impl SourceSemaphores {
    fn new(cfg: &Arc<RuntimeConfig>, limit: u32) -> SourceSemaphores {
        let n = cfg.core.sources().len();
        SourceSemaphores {
            per_source: (0..n).map(|_| Arc::new(Semaphore::new(limit as usize))).collect(),
            cfg: cfg.clone(),
        }
    }

    fn lane(&self, req: &Request) -> Option<usize> {
        let id = match req {
            Request::Connect { source_id, .. } | Request::Disconnect { source_id, .. } => *source_id,
            Request::Batch { batch, .. } => batch.source_id,
            Request::Introspect { .. } => return None,
        };
        self.cfg.core.source_index(id)
    }

    async fn acquire(&self, lane: usize) -> Result<OwnedSemaphorePermit, ()> {
        let s = self.per_source.get(lane).ok_or(())?;
        s.clone().acquire_owned().await.map_err(|_| ())
    }

    fn close(&self) {
        for s in self.per_source.iter() {
            s.close();
        }
    }

    /// Permits currently available for `lane` (tests / introspection of leaks).
    pub fn available(&self, lane: usize) -> usize {
        self.per_source.get(lane).map_or(0, |s| s.available_permits())
    }
}

/// Create the ingress pair for `cfg`.
pub fn channel(cfg: &Arc<RuntimeConfig>) -> (Ingress, Inbox) {
    let q = cfg.limits.input_queue_capacity as usize;
    match cfg.limits.ingress {
        IngressPolicy::Fifo => {
            let (tx, rx) = mpsc::channel(q);
            (Ingress::Fifo(tx), Inbox::Fifo(rx))
        }
        IngressPolicy::SourceCapped { per_source } => {
            let (tx, rx) = mpsc::channel(q);
            (Ingress::Capped { tx, sources: Arc::new(SourceSemaphores::new(cfg, per_source)) }, Inbox::Fifo(rx))
        }
        IngressPolicy::SourceFair { per_source } => {
            let n = cfg.core.sources().len() as u32;
            let control = cfg.limits.max_driver_connections.saturating_add(INTROSPECTION_SESSIONS);
            let queue = FairQueue::new(Params {
                lanes: n + 1, // + introspection lane
                lane_capacity: per_source,
                control_capacity: control,
                discipline: Discipline::RoundRobin { quantum: 1 },
            })
            .expect("validated ingress parameters");
            let shared = Arc::new(Shared {
                state: Mutex::new(State { queue, senders_gone: false, receiver_gone: false }),
                ready: Condvar::new(),
                sources: SourceSemaphores::new(cfg, per_source),
                global: Arc::new(Semaphore::new(q)),
                introspection_lane: n,
            });
            (Ingress::Fair(FairSender { shared: shared.clone() }), Inbox::Fair(FairReceiver { shared }))
        }
    }
}

/// Returned when the sequencer is gone (or the request cannot be routed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl Ingress {
    /// Hand `req` to the sequencer. Waiting here is the backpressure (the session stops
    /// reading its socket). Cancel-safe. `full` is called once when the request had to
    /// wait for room (counter `input_queue_full_waits`).
    pub async fn submit(&self, mut req: Request, full: impl FnOnce()) -> Result<(), Closed> {
        match self {
            Ingress::Fifo(tx) => send_fifo(tx, req, full).await,
            Ingress::Capped { tx, sources } => {
                // Only batches take a quota permit; control requests go straight in.
                if matches!(req, Request::Batch { .. }) {
                    let lane = sources.lane(&req).ok_or(Closed)?;
                    let p = match sources.per_source[lane].clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => sources.acquire(lane).await.map_err(|_| Closed)?,
                    };
                    if let Request::Batch { permits, .. } = &mut req {
                        permits._source = Some(p);
                    }
                }
                send_fifo(tx, req, full).await
            }
            Ingress::Fair(f) => f.submit(req, full).await,
        }
    }
}

async fn send_fifo(tx: &mpsc::Sender<Request>, req: Request, full: impl FnOnce()) -> Result<(), Closed> {
    match tx.try_send(req) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(req)) => {
            full();
            tx.send(req).await.map_err(|_| Closed)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(Closed),
    }
}

#[derive(Debug)]
struct State {
    queue: FairQueue<Request>,
    senders_gone: bool,
    receiver_gone: bool,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    ready: Condvar,
    sources: SourceSemaphores,
    global: Arc<Semaphore>,
    introspection_lane: u32,
}

impl Shared {
    /// A poisoned mutex means a thread panicked while holding it; the queue itself has no
    /// half-done state across a panic point (push/pop are not interrupted), so keep going.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Producer handle of `source_fair`. Dropping it closes the ingress: the sequencer
/// applies what is queued and stops.
#[derive(Debug)]
pub struct FairSender {
    shared: Arc<Shared>,
}

impl FairSender {
    async fn submit(&self, mut req: Request, full: impl FnOnce()) -> Result<(), Closed> {
        let sh = &self.shared;
        let (lane, kind) = match &req {
            Request::Introspect { .. } => (sh.introspection_lane, Kind::Control),
            Request::Connect { .. } | Request::Disconnect { .. } => {
                (sh.sources.lane(&req).ok_or(Closed)? as u32, Kind::Control)
            }
            Request::Batch { .. } => (sh.sources.lane(&req).ok_or(Closed)? as u32, Kind::Data),
        };
        if kind == Kind::Data {
            // Fixed order: source permit, then global permit (no cycle, no deadlock).
            let mut waited = false;
            let src = match sh.sources.per_source[lane as usize].clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    waited = true;
                    sh.sources.acquire(lane as usize).await.map_err(|_| Closed)?
                }
            };
            let glob = match sh.global.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    waited = true;
                    sh.global.clone().acquire_owned().await.map_err(|_| Closed)?
                }
            };
            if waited {
                full();
            }
            if let Request::Batch { permits, .. } = &mut req {
                *permits = Permits { _source: Some(src), _global: Some(glob) };
            }
        }
        let mut st = sh.lock();
        if st.receiver_gone {
            return Err(Closed);
        }
        let was_empty = st.queue.is_empty();
        match st.queue.push(lane, kind, 1, req) {
            Ok(()) => {}
            // Data: impossible while the permits are held (lane data <= permits). Control:
            // impossible while each session has at most one control request queued. Both
            // would be bugs; refuse instead of growing.
            Err(PushError::Full(_) | PushError::UnknownLane(_) | PushError::InvalidCost(_)) => return Err(Closed),
        }
        drop(st);
        if was_empty {
            sh.ready.notify_one();
        }
        Ok(())
    }
}

impl Drop for FairSender {
    fn drop(&mut self) {
        self.shared.lock().senders_gone = true;
        self.shared.ready.notify_all();
    }
}

/// Sequencer handle of `source_fair`. Dropping it (normal end or panic of the sequencer)
/// closes every semaphore and drops every queued request, so no session waits forever
/// and no reply is ever invented (the replies' `oneshot` senders are dropped).
#[derive(Debug)]
pub struct FairReceiver {
    shared: Arc<Shared>,
}

impl FairReceiver {
    fn recv(&mut self) -> Option<Request> {
        let sh = &self.shared;
        let mut st = sh.lock();
        loop {
            if let Some(p) = st.queue.pop() {
                return Some(p.item);
            }
            if st.senders_gone {
                return None;
            }
            // Re-checked under the same lock that every push takes: no lost wake-up.
            st = sh.ready.wait(st).unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl Drop for FairReceiver {
    fn drop(&mut self) {
        let drained = {
            let mut st = self.shared.lock();
            st.receiver_gone = true;
            st.queue.drain()
        };
        self.shared.sources.close();
        self.shared.global.close();
        drop(drained); // outside the lock: dropping replies wakes session tasks
    }
}

impl Inbox {
    /// Next request in service order; `None` when every producer is gone and nothing is
    /// queued. Blocking: sequencer thread only.
    pub fn blocking_recv(&mut self) -> Option<Request> {
        match self {
            Inbox::Fifo(rx) => rx.blocking_recv(),
            Inbox::Fair(f) => f.recv(),
        }
    }
}
