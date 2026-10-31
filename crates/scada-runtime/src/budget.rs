//! Configured worst-case bound of the kernel's application-level allocations (VS-002 E1).
//!
//! Every term is a product of configured limits and `size_of` of the types that hold the
//! data, computed in `u128` so that no configuration can overflow it. The result is a
//! conservative bound of what the kernel's own code can hold at once (buffers, queued
//! requests, decoded batches, RTDB). It is NOT a bound of the process RSS: thread stacks,
//! allocator fragmentation and metadata, the Tokio runtime's internal structures, the
//! binary and libc, and the Linux socket buffers (kernel memory) come on top; the per
//! task Tokio state is covered only by the fixed estimate [`TASK_ESTIMATE_BYTES`].
//!
//! `[runtime] memory_budget_bytes`, when set, refuses startup if the bound exceeds it.

use std::fmt;
use std::mem::size_of;

use scada_core::{Kernel, Observation, ValidatedConfig};
use scada_protocol::driver_v1 as pb;

use crate::config::RuntimeConfig;
use crate::sequencer::{BatchReply, Request};

/// Fixed per-session estimate for Tokio task state, split socket halves, channel headers
/// and the futures of reader and writer. An estimate (documented as such), not a measure.
pub const TASK_ESTIMATE_BYTES: u128 = 16 * 1024;
/// Upper bound of one encoded ACK frame (header + InputAck with 16-byte instance id and
/// four 10-byte varints): measured 52 bytes worst case, rounded up.
pub const MAX_ACK_FRAME_BYTES: u128 = 96;
/// Upper bound of the handshake messages the kernel encodes (Welcome / HandshakeRejected).
pub const MAX_HANDSHAKE_FRAME_BYTES: u128 = 512;
/// Diagnostic histograms when `--diag` is on.
pub const DIAG_BYTES: u128 = 12 * 1184 * 8 + 12 * 64;

/// Itemized bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Budget {
    /// Per connection (one session) bound.
    pub per_connection: u128,
    /// `per_connection * max_driver_connections`.
    pub connections: u128,
    /// Transient decode of an oversized frame, one per runtime worker (review P1).
    pub decode_workers: u128,
    /// Sequencer queue and the request being applied, with their observation vectors.
    pub sequencer_queue: u128,
    /// VS-003 ingress structures (semaphores, lanes, ready ring, control entries); 0 with
    /// `fifo`.
    pub ingress: u128,
    /// Kernel RTDB + configuration metadata.
    pub kernel: u128,
    /// Introspection (4 sessions with a response bounded by the frame ceiling) – only when
    /// the introspection socket is enabled.
    pub introspection: u128,
    pub diagnostics: u128,
    pub total: u128,
}

impl fmt::Display for Budget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total={} (connections={} [{} each], decode_workers={}, sequencer_queue={}, ingress={}, kernel={}, introspection={}, diagnostics={})",
            self.total,
            self.connections,
            self.per_connection,
            self.decode_workers,
            self.sequencer_queue,
            self.ingress,
            self.kernel,
            self.introspection,
            self.diagnostics
        )
    }
}

fn m(a: u128, b: u128) -> u128 {
    a.saturating_mul(b)
}

/// Bound for `cfg`. `introspection_response_max`: largest introspection frame (0 when
/// the endpoint is disabled). `diag`: whether `--diag` is on.
pub fn compute(cfg: &RuntimeConfig, introspection: bool, diag: bool) -> Budget {
    let l = cfg.limits;
    let max_obs = u128::from(cfg.core.limits().max_observations_per_batch);
    let obs_core = m(max_obs, size_of::<Observation>() as u128);
    // Review P1: the reused scratch never retains capacity for more than
    // `retained_capacity_bound(max_obs)` observations (Vec growth); an oversized frame
    // can grow it transiently (see `decode_workers` below).
    let obs_wire = m(
        crate::convert::retained_capacity_bound(cfg.core.limits().max_observations_per_batch) as u128,
        size_of::<pb::Observation>() as u128,
    );
    let inflight = u128::from(l.max_inflight_per_connection);
    // One oneshot reply allocation per in-flight batch (Arc'd cell: header + value).
    let reply_cell = (size_of::<BatchReply>() as u128).saturating_add(64);
    let per_connection = u128::from(l.max_frame_bytes) // payload buffer (grows to the largest frame)
        .saturating_add(u128::from(l.read_buffer_bytes)) // C5 read buffer
        .saturating_add(obs_wire) // C3 reused decoded batch (retained capacity)
        // Transient, one at a time: a frame that is not exactly one `batch` field is decoded
        // generically (at most `max_obs` observations, P1) or a `hello` is validated; its
        // strings and observations are bounded by the frame and freed at once.
        .saturating_add(u128::from(l.max_frame_bytes).max(obs_wire))
        .saturating_add(obs_core) // converted batch held while waiting for a slot / the queue
        .saturating_add(m(inflight, 64)) // pending queue entries
        .saturating_add(m(inflight, reply_cell))
        .saturating_add(m(u128::from(l.ack_coalesce_max), MAX_ACK_FRAME_BYTES).saturating_add(MAX_ACK_FRAME_BYTES))
        .saturating_add(m(u128::from(l.ack_coalesce_max), 16)) // diag timestamps
        .saturating_add(MAX_HANDSHAKE_FRAME_BYTES)
        .saturating_add(TASK_ESTIMATE_BYTES);
    let connections = m(per_connection, u128::from(l.max_driver_connections));
    // Review P1: the fast-path merge of an oversized frame is transient and synchronous
    // (dropped before any await), so at most one per runtime worker exists at a time.
    let workers = match cfg.runtime.scheduler {
        crate::config::Scheduler::CurrentThread => 1,
        crate::config::Scheduler::MultiThread { worker_threads } => u128::from(worker_threads),
    };
    let decode_workers = m(workers, crate::convert::transient_decode_bound_bytes(l.max_frame_bytes));
    // Requests that left the sessions: queued (capacity) + the one being applied. Each
    // batch request owns its observation vector.
    let queued = u128::from(l.input_queue_capacity).saturating_add(1);
    let sequencer_queue = m(queued, (size_of::<Request>() as u128).saturating_add(obs_core));
    let kernel = Kernel::storage_bound_bytes(&cfg.core).saturating_add(cfg.core.heap_bytes());
    let introspection = if introspection {
        // 4 sessions; each response is encoded in one buffer bounded by the frame ceiling,
        // and the owned snapshot it is built from is of the same order.
        let tags = cfg.core.tags().len() as u128;
        let one = m(tags, 32).saturating_add(m(cfg.core.sources().len() as u128, 64)).saturating_add(4096);
        m(4, m(one, 3))
    } else {
        0
    };
    let diagnostics = if diag { DIAG_BYTES } else { 0 };
    let ingress = ingress_bytes(cfg);
    let total = connections
        .saturating_add(ingress)
        .saturating_add(decode_workers)
        .saturating_add(sequencer_queue)
        .saturating_add(kernel)
        .saturating_add(introspection)
        .saturating_add(diagnostics);
    Budget {
        per_connection,
        connections,
        decode_workers,
        sequencer_queue,
        ingress,
        kernel,
        introspection,
        diagnostics,
        total,
    }
}

/// Estimated bytes of one Tokio `Semaphore` behind an `Arc` (state, waiter list head,
/// `Arc` counters, slice pointer). An estimate, documented as such.
pub const SEMAPHORE_ESTIMATE_BYTES: u128 = 96;
/// Per-lane bookkeeping of `scada_sched::FairQueue` besides the entries (`VecDeque`
/// header, counters, flags, deficit). Estimate rounded up.
pub const LANE_OVERHEAD_BYTES: u128 = 64;
/// Per-entry overhead of a queued request in a lane (kind + cost), rounded up.
pub const ENTRY_OVERHEAD_BYTES: u128 = 16;

/// VS-003 (ADR 0010 §6): structures of the ingress policy, besides the requests and their
/// observation vectors (already in `sequencer_queue`: at most `input_queue_capacity`
/// admitted data batches + the one being applied under every policy).
fn ingress_bytes(cfg: &RuntimeConfig) -> u128 {
    let l = cfg.limits;
    let sources = cfg.core.sources().len() as u128;
    let semaphores = m(sources, SEMAPHORE_ESTIMATE_BYTES.saturating_add(8));
    match l.ingress {
        crate::config::IngressPolicy::Fifo => 0,
        crate::config::IngressPolicy::SourceCapped { .. } => semaphores,
        crate::config::IngressPolicy::SourceFair { per_source } => {
            let lanes = sources.saturating_add(1);
            let entry = (size_of::<Request>() as u128).saturating_add(ENTRY_OVERHEAD_BYTES);
            let prealloc = u128::from(per_source).saturating_add(u128::from(scada_sched::PREALLOC_CONTROL));
            // Control requests (connect/disconnect/introspect) are not in the data permits:
            // at most one per session (+4 introspection) at once; a lane that receives more
            // than PREALLOC_CONTROL of them grows, x2 for `VecDeque` doubling.
            let control = m(u128::from(l.max_driver_connections).saturating_add(4), m(entry, 2));
            m(lanes, m(prealloc, entry).saturating_add(LANE_OVERHEAD_BYTES))
                .saturating_add(m(lanes, 4)) // ready ring
                .saturating_add(control)
                .saturating_add(semaphores)
                .saturating_add(SEMAPHORE_ESTIMATE_BYTES) // global semaphore
        }
    }
}

/// Validate the bound against `[runtime] memory_budget_bytes`, if configured.
pub fn check(cfg: &RuntimeConfig, b: &Budget) -> Result<(), String> {
    match cfg.runtime.memory_budget_bytes {
        Some(limit) if b.total > u128::from(limit) => Err(format!(
            "configured worst-case allocation bound {} bytes exceeds memory_budget_bytes = {limit} ({b})",
            b.total
        )),
        _ => Ok(()),
    }
}

/// Exposed for tests: the configured core used by the bound.
pub fn core(cfg: &RuntimeConfig) -> &ValidatedConfig {
    &cfg.core
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse;

    fn cfg(extra_ipc: &str) -> RuntimeConfig {
        parse(&format!(
            r#"[ipc]
socket_path = "/run/x/d.sock"
{extra_ipc}
[[tags]]
id = 1
name = "t"
type = "i64"
[[sources]]
id = 1
name = "s"
channels = [{{ channel = 1, tag = 1, type = "i64" }}]
"#
        ))
        .unwrap()
    }

    #[test]
    fn bound_is_monotonic_in_the_limits_and_never_overflows() {
        let small = compute(&cfg(""), false, false);
        let big = compute(
            &cfg(
                "max_frame_bytes = 16777216\nmax_driver_connections = 1024\ninput_queue_capacity = 65536\nmax_inflight_per_connection = 1024\nread_buffer_bytes = 1048576\nack_coalesce_max = 1024\n",
            ),
            true,
            true,
        );
        assert!(small.total > 0 && small.total < big.total);
        assert!(big.total < u128::from(u64::MAX), "{big}");
        assert_eq!(big.connections, big.per_connection * 1024);
        assert_eq!(
            big.total,
            big.connections
                + big.decode_workers
                + big.sequencer_queue
                + big.ingress
                + big.kernel
                + big.introspection
                + big.diagnostics
        );
    }

    #[test]
    fn budget_limit_is_enforced() {
        let mut c = cfg("");
        let b = compute(&c, false, false);
        c.runtime.memory_budget_bytes = Some(u64::try_from(b.total).unwrap());
        assert!(check(&c, &b).is_ok());
        c.runtime.memory_budget_bytes = Some(u64::try_from(b.total - 1).unwrap());
        assert!(check(&c, &b).is_err());
    }

    /// VS-003: the ingress structures are in the bound, grow with sources × limit and
    /// never overflow; `fifo` adds nothing (VS-002 bound unchanged).
    #[test]
    fn ingress_policy_is_in_the_bound() {
        let fifo = compute(&cfg(""), false, false);
        assert_eq!(fifo.ingress, 0);
        let capped = compute(&cfg("ingress_policy = \"source_capped\"\nper_source_pending_limit = 4\n"), false, false);
        let fair4 = compute(&cfg("ingress_policy = \"source_fair\"\nper_source_pending_limit = 4\n"), false, false);
        let fair32 = compute(&cfg("ingress_policy = \"source_fair\"\nper_source_pending_limit = 32\n"), false, false);
        assert!(0 < capped.ingress && capped.ingress < fair4.ingress && fair4.ingress < fair32.ingress);
        assert_eq!(fair4.total, fifo.total + fair4.ingress);
        // Many sources × the largest limit: still a finite u128, far from overflow.
        let mut text = String::from(
            "[ipc]\nsocket_path = \"/run/x/d.sock\"\nmax_driver_connections = 1024\nmax_inflight_per_connection = 1024\ninput_queue_capacity = 65536\ningress_policy = \"source_fair\"\nper_source_pending_limit = 1024\n",
        );
        for i in 1..=2000u32 {
            text.push_str(&format!("[[tags]]\nid = {i}\nname = \"t{i}\"\ntype = \"i64\"\n"));
        }
        for i in 1..=2000u32 {
            text.push_str(&format!(
                "[[sources]]\nid = {i}\nname = \"s{i}\"\nchannels = [{{ channel = 1, tag = {i}, type = \"i64\" }}]\n"
            ));
        }
        let big = compute(&parse(&text).unwrap(), true, true);
        assert!(big.ingress > 2000 * 1024 * 64, "{big}");
        assert!(big.total < u128::from(u64::MAX));
        // ... and memory_budget_bytes refuses it.
        let mut c = parse(&text).unwrap();
        c.runtime.memory_budget_bytes = Some(16 << 20);
        assert!(check(&c, &big).is_err());
    }

    #[test]
    fn invalid_ingress_settings_are_refused() {
        let bad = |ipc: &str| {
            parse(&format!(
                "[ipc]\nsocket_path = \"/run/x/d.sock\"\n{ipc}\n[[tags]]\nid = 1\nname = \"t\"\ntype = \"i64\"\n[[sources]]\nid = 1\nname = \"s\"\nchannels = [{{ channel = 1, tag = 1, type = \"i64\" }}]\n"
            ))
            .is_err()
        };
        assert!(bad("ingress_policy = \"priority\""));
        assert!(bad("ingress_policy = \"source_fair\""), "limit required");
        assert!(bad("ingress_policy = \"source_fair\"\nper_source_pending_limit = 0"));
        assert!(bad("ingress_policy = \"source_fair\"\nper_source_pending_limit = 33"), "> max_inflight (32)");
        assert!(bad("per_source_pending_limit = 4"), "limit without a per-source policy");
        assert!(bad("ingress_policy = \"fifo\"\nper_source_pending_limit = 4"));
        assert!(!bad("ingress_policy = \"fifo\""));
        assert!(!bad("ingress_policy = \"source_capped\"\nper_source_pending_limit = 32"));
    }
}
