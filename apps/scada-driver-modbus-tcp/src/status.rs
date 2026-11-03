//! Status rendering: one `STATUS` line per device on stdout and an atomically replaced
//! JSON health file. Stable key names (used by the tests and the playbook).

use std::fmt::Write as _;
use std::path::Path;

use crate::device::DeviceStatus;

fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if c.is_control() => {
                let _ = write!(o, "\\u{:04x}", c as u32);
            }
            c => o.push(c),
        }
    }
    o
}

fn opt<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map_or("-".into(), |x| x.to_string())
}

/// `STATUS key=value ...` (strings quoted).
pub fn status_line(s: &DeviceStatus) -> String {
    let p = &s.publisher;
    let errs: Vec<String> = s.scan_errors.iter().map(|(k, v)| format!("{k}:{v}")).collect();
    format!(
        "STATUS device={} source={} epoch={:016x} device_state={} link={} minor={} durable={} scans_ok={} scans_failed={} \
         scan_errors={} unpublished_no_kernel={} overruns={} modbus_connects={} requests_per_scan={} items_per_scan={} \
         spread_us={} max_spread_us={} full_states={} deltas={} no_change={} deferred={} resent={} pending={} inflight={} \
         pending_high_water={} next_seq={} durable_seq={} acks_volatile={} acks_durable={} acks_not_durable={} \
         acks_duplicate={} rejected_retryable={} rejected_collateral={} rejected_fatal={} lost_kernel_restart={} \
         connects={} connect_failures={} disconnects={} heartbeats={} continuity={} modbus_requests={} \
         modbus_timeouts={} modbus_exceptions={} observations={} ack_n={} ack_p50_us={} ack_p99_us={} ack_p999_us={} \
         ack_max_us={} last_error=\"{}\" kernel_error=\"{}\" fatal=\"{}\"",
        s.name,
        s.source_id,
        s.epoch,
        s.device_state.name(),
        s.link_state.name(),
        opt(s.negotiated_minor),
        s.durable_receipts,
        s.scans_ok,
        s.scans_failed,
        if errs.is_empty() { "-".into() } else { errs.join(",") },
        s.scans_unpublished_no_kernel,
        s.scan_overruns,
        s.modbus_connects,
        s.requests_per_scan,
        s.items_per_scan,
        s.last_spread_us,
        s.max_spread_us,
        p.full_states_sent,
        p.deltas_sent,
        p.scans_no_change,
        p.scans_deferred,
        p.batches_resent,
        s.pending,
        s.inflight,
        p.pending_high_water,
        s.next_sequence,
        opt(s.durable_confirmed),
        p.acks_applied_volatile,
        p.acks_applied_durable,
        p.acks_applied_not_durable,
        p.acks_duplicate,
        p.rejections_retryable,
        p.rejections_collateral,
        p.rejections_fatal,
        p.batches_lost_kernel_restart,
        p.connects,
        p.connect_failures,
        p.disconnects,
        p.heartbeats_sent,
        s.last_continuity.unwrap_or("-"),
        s.client.requests,
        s.client.timeouts,
        s.client.exceptions,
        p.observations_published,
        s.ack_latency.map_or(0, |l| l.n),
        opt(s.ack_latency.map(|l| l.p50_us)),
        opt(s.ack_latency.map(|l| l.p99_us)),
        opt(s.ack_latency.map(|l| l.p999_us)),
        opt(s.ack_latency.map(|l| l.max_us)),
        esc(s.last_error.as_deref().unwrap_or("")),
        esc(s.last_publisher_error.as_deref().unwrap_or("")),
        esc(s.fatal.as_deref().unwrap_or("")),
    )
}

/// JSON document of every device (hand-written: no serde_json dependency).
pub fn health_json(driver_id: &str, devices: &[DeviceStatus]) -> String {
    let mut o = String::new();
    let _ = write!(o, "{{\"driver_id\":\"{}\",\"devices\":[", esc(driver_id));
    for (i, s) in devices.iter().enumerate() {
        if i > 0 {
            o.push(',');
        }
        let p = &s.publisher;
        let _ = write!(
            o,
            "{{\"name\":\"{}\",\"source_id\":{},\"device_state\":\"{}\",\"link_state\":\"{}\",\"scans_ok\":{},\
             \"scans_failed\":{},\"pending\":{},\"inflight\":{},\"deferred\":{},\"lost_kernel_restart\":{},\
             \"durable_receipts\":{},\"durable_seq\":{},\"max_spread_us\":{},\"last_error\":\"{}\",\"fatal\":\"{}\"}}",
            esc(&s.name),
            s.source_id,
            s.device_state.name(),
            s.link_state.name(),
            s.scans_ok,
            s.scans_failed,
            s.pending,
            s.inflight,
            p.scans_deferred,
            p.batches_lost_kernel_restart,
            s.durable_receipts,
            s.durable_confirmed.map_or("null".into(), |v| v.to_string()),
            s.max_spread_us,
            esc(s.last_error.as_deref().unwrap_or("")),
            esc(s.fatal.as_deref().unwrap_or("")),
        );
    }
    o.push_str("]}\n");
    o
}

/// Write `content` to `path` atomically (temporary file + rename in the same directory).
pub fn write_atomic(path: &Path, content: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, path)
}
