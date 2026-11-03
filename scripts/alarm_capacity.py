#!/usr/bin/env python3
"""Alarm outbox and journal capacity model (VS-005 ADR 0012, VS-006 ADR 0013 D15, brief §7).

Sizes `[alarms] outbox_capacity_events` and `[persistence]` and states for how long a
configuration survives an alarm storm. Three levels are kept apart in the output:

  A. MEASURED   numbers read from benchmark raw results (--bench-results, repeatable):
                record sizes, kernel event throughput, per-rule memory and, VS-006,
                the durable journal (synchronous=FULL on a real disk): events/s,
                operations/s, fsync/s, bytes per event / per operation, write
                amplification, checkpoint cost. tmpfs and NORMAL (NOT DURABLE) cells are
                listed but NEVER used as mu.
  B. ASSUMED    plant/installation assumptions given on the command line: event rate,
                operation rate, events per operation, storm duration, reserve, initial
                backlog, disk free, journal queue. Not measured.
  C. COMPUTED   pure arithmetic from A and B (formulas below). Never a measurement.

mu (durable releases/s):
  * without [persistence] (VS-005 kernel) mu = 0: every event occupies a slot until the
    process restarts (scenario `vs005_mu_0`, kept for comparison);
  * with the VS-006 journal, mu = the MEASURED journal rate (level A, scenario
    `vs006_journal_measured`): the smallest of the process storm rate with the journal
    as sink (alarm-storm sink=journal, unthrottled, safety PASS: released_s) and the in-process journal rate at
    the closest measured fan-out not above --events-per-op (events/s grows with
    fan-out, so the lower cell is the conservative one);
  * `--durable-rate` is an ASSUMPTION (e.g. other storage): scenario `vs006_projection`,
    labelled. mu never comes from the TEST-ONLY simulated sink of VS-005.

Formulas (events, seconds):
  slots              = min(--outbox-slots, slots that fit in the RAM budget)
  usable             = slots - reserve - initial_backlog
  net_growth         = lambda - mu
  time_to_full       = usable / net_growth                (inf if net_growth <= 0)
  required_slots(T)  = initial_backlog + reserve + max(0, net_growth) * T
  burst requirement  = initial_backlog + reserve + max(0, max prefix sum of (e_t - mu))
                       over a per-second event profile (--burst-profile), i.e. the
                       worst cumulative excess at any instant of the burst
  backlog_drain_time = backlog / (mu - lambda)            (inf if mu <= lambda)
  VS-006 journal (operations include zero-event ones):
  journal_queue_time_to_full = queue_ops / (ops_rate - mu_ops)   (inf if ops_rate <= mu_ops)
  journal_queue_bytes_needed = queue_ops * bytes_per_op_queued   (--op-queue-bytes, assumed)
  disk_growth_bytes_s = ops_rate * bytes_per_op(E0) + lambda * event_table_bytes_per_event
  time_to_disk_floor  = (disk_free - min_disk_free) / disk_growth_bytes_s
                        (no automatic history retention: DISK_FULL admission stop then)

  python3 scripts/alarm_capacity.py --bench-results docs/benchmarks/results/vs005/storm.txt \\
      --ram-budget-mib 64 --outbox-slots 1048576 --event-rate 200 --storm-seconds 600 --json
"""
import argparse
import json
import math
import re
import sys

EVENT_RECORD_BYTES = 48  # size_of::<AlarmEvent>() (asserted by the core layout test)
STATE_BYTES = 32  # size_of::<AlarmState>()
RULE_BYTES = 24  # size_of::<CompiledRule>()
INF = float("inf")


def _num(kv, k):
    try:
        return float(kv[k])
    except (KeyError, ValueError):
        return None


def parse_bench(paths):
    """Level A: extract measured numbers from scada-bench raw lines (key=value)."""
    if isinstance(paths, str):
        paths = [paths]
    paths = [p for p in (paths or []) if p]
    a = {"source": paths, "storm_events_s": [], "core_events_s": [], "layout": {}, "per_rule_bytes": None,
         "journal": [], "journal_storm_released_s": [], "journal_recovery": []}
    for path in paths:
        _parse_file(path, a)
    return a


def _parse_file(path, a):
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            kv = dict(re.findall(r"(\w+)=(\S+)", line))
            kind = kv.get("kind")
            if kind == "journal":
                a["journal"].append({
                    "label": kv.get("label"),
                    "durable": kv.get("durability") == "FULL",
                    "tmpfs": "tmpfs" in kv.get("fs", ""),
                    "events_per_op": _num(kv, "events_per_op"),
                    "ops_s": _num(kv, "ops_per_s"),
                    "events_s": _num(kv, "events_per_s"),
                    "fsync_s": _num(kv, "fsync_per_s"),
                    "bytes_per_event": _num(kv, "bytes_per_event"),
                    "bytes_per_op": _num(kv, "bytes_per_op"),
                    "event_table_bytes_per_event": (_num(kv, "event_table_bytes") / _num(kv, "events"))
                    if _num(kv, "event_table_bytes") and _num(kv, "events") else None,
                    "write_amplification": _num(kv, "write_amplification"),
                    "checkpoint_pause_us_p99": _num(kv, "checkpoint_pause_us_p99"),
                    "checkpoint_write_us_max": _num(kv, "checkpoint_write_us_max"),
                    "commit_us_p99": _num(kv, "commit_us_p99"),
                    "commit_max": _num(kv, "commit_max"),
                })
                continue
            if kind == "journal-recovery":
                a["journal_recovery"].append({"label": kv.get("label"), "total_ops": _num(kv, "total_ops"),
                                              "recover_ms": _num(kv, "recover_ms"),
                                              "replayed_ops": _num(kv, "replayed_ops")})
                continue
            # Journal as sink, unthrottled producers: the released rate is the durable drain
            # the journal sustained (saturated storms refuse the excess: that is the point).
            if (kind == "alarm-storm" and kv.get("sink") == "journal"
                    and kv.get("safety") == "PASS" and kv.get("interval_us", "0") == "0"
                    and not re.search(r"slow|stall|NORMAL", kv.get("label", ""))):
                a["journal_storm_released_s"].append(float(kv["released_s"]))
                continue
            # Sustained kernel rate: unthrottled storms whose drain kept up (no refusal) and
            # that passed the safety checks.
            if (kind == "alarm-storm" and kv.get("sink") == "drain" and kv.get("storm60") == "PASS"
                    and kv.get("safety") == "PASS" and kv.get("interval_us", "0") == "0"):
                a["storm_events_s"].append(float(kv["events_s"]))
            elif kind == "alarms-core" and "events_s" in kv and float(kv["events_s"]) > 0:
                a["core_events_s"].append(float(kv["events_s"]))
            elif kind == "alarms-cold" and "per_rule_b" in kv:
                a["per_rule_bytes"] = max(a["per_rule_bytes"] or 0.0, float(kv["per_rule_b"]))
    return a


def max_prefix_excess(profile, mu):
    """Largest cumulative sum of (e_t - mu) over any prefix (0 if never positive)."""
    best = acc = 0.0
    for e in profile:
        acc += e - mu
        best = max(best, acc)
    return best


def model(args, measured):
    out = {"level_A_measured": {}, "level_B_assumed": {}, "level_C_computed": {}, "warnings": []}
    A, B, C = out["level_A_measured"], out["level_B_assumed"], out["level_C_computed"]
    A["event_record_bytes"] = EVENT_RECORD_BYTES
    A["alarm_state_bytes"] = STATE_BYTES
    A["compiled_rule_bytes"] = RULE_BYTES
    A["per_rule_bytes_measured"] = measured["per_rule_bytes"]
    sustained = measured["storm_events_s"]
    A["kernel_events_s_process_drain_pass"] = min(sustained) if sustained else None
    A["kernel_events_s_in_memory_max"] = max(measured["core_events_s"]) if measured["core_events_s"] else None
    A["bench_results"] = measured["source"]
    # VS-006 journal (level A only from FULL on a real disk).
    durable_cells = [j for j in measured["journal"] if j["durable"] and not j["tmpfs"] and j["events_s"] is not None
                     and (j["commit_max"] or 1024) > 1]
    A["journal_cells_ignored_not_durable_or_tmpfs"] = sorted(
        j["label"] for j in measured["journal"] if not j["durable"] or j["tmpfs"])
    epo = args.events_per_op
    under = [j for j in durable_cells if j["events_per_op"] is not None and 0 < j["events_per_op"] <= epo]
    cell = max(under, key=lambda j: (j["events_per_op"], -j["events_s"])) if under else None
    if cell is not None:
        same = [j for j in under if j["events_per_op"] == cell["events_per_op"]]
        cell = min(same, key=lambda j: j["events_s"])  # the slowest repetition of that fan-out
    zero = [j for j in durable_cells if j["events_per_op"] == 0]
    z = min(zero, key=lambda j: j["ops_s"]) if zero else None
    A["journal_cell_used"] = cell["label"] if cell else None
    A["journal_events_s_in_process"] = cell["events_s"] if cell else None
    A["journal_ops_s_in_process_at_fanout"] = cell["ops_s"] if cell else None
    A["journal_ops_s_zero_event"] = z["ops_s"] if z else None
    A["journal_fsync_s_at_fanout"] = cell["fsync_s"] if cell else None
    A["bytes_per_event"] = cell["bytes_per_event"] if cell else None
    A["event_table_bytes_per_event"] = cell["event_table_bytes_per_event"] if cell else None
    A["bytes_per_op_zero_event"] = z["bytes_per_op"] if z else None
    A["write_amplification_max"] = max((j["write_amplification"] for j in durable_cells
                                        if j["write_amplification"] is not None), default=None)
    A["checkpoint_pause_us_p99_max"] = max((j["checkpoint_pause_us_p99"] for j in durable_cells
                                            if j["checkpoint_pause_us_p99"] is not None), default=None)
    A["checkpoint_write_us_max"] = max((j["checkpoint_write_us_max"] for j in durable_cells
                                        if j["checkpoint_write_us_max"] is not None), default=None)
    rs = measured["journal_storm_released_s"]
    A["journal_events_s_process_storm_pass"] = min(rs) if rs else None
    if measured["journal_recovery"]:
        worst = max(measured["journal_recovery"], key=lambda r: r["recover_ms"] or 0)
        A["recovery_ms_worst"] = worst["recover_ms"]
        A["recovery_worst_label"] = worst["label"]
    mus = [x for x in (A["journal_events_s_process_storm_pass"], A["journal_events_s_in_process"]) if x]
    mu_measured = min(mus) if mus else None
    A["mu_events_s_measured"] = mu_measured

    lam, mu_proj = args.event_rate, args.durable_rate
    ops_rate = args.ops_rate if args.ops_rate is not None else (lam / epo if epo > 0 else 0.0)
    B.update(
        event_rate_per_s=lam,
        ops_rate_per_s=ops_rate,
        events_per_op=epo,
        disk_free_mib=args.disk_free_mib,
        min_disk_free_mib=args.min_disk_free_mib,
        journal_queue_operations=args.journal_queue_operations,
        op_queue_bytes=args.op_queue_bytes,
        durable_rate_per_s_projection=mu_proj,
        durable_rate_per_s_vs005=0.0,
        storm_seconds=args.storm_seconds,
        reserve_slots=args.reserve,
        initial_backlog=args.initial_backlog,
        rules=args.rules,
        ram_budget_mib=args.ram_budget_mib,
        outbox_slots=args.outbox_slots,
        burst_profile=args.burst_profile,
    )
    if mu_proj > 0:
        out["warnings"].append("durable_rate given on the command line is a PROJECTION (level B assumption), "
                               "not the measured journal mu; without [persistence] VS-005 mu = 0")

    # Slots: configured and/or what fits in the RAM budget after the rule tables.
    per_rule = measured["per_rule_bytes"] or float(STATE_BYTES + RULE_BYTES + 96)
    fixed = per_rule * args.rules
    slots_ram = None
    if args.ram_budget_mib is not None:
        slots_ram = max(0, int((args.ram_budget_mib * (1 << 20) - fixed) // EVENT_RECORD_BYTES))
    cands = [s for s in (args.outbox_slots, slots_ram) if s is not None]
    if not cands:
        raise SystemExit("give --outbox-slots and/or --ram-budget-mib")
    slots = min(cands)
    C["slots_from_ram_budget"] = slots_ram
    C["slots"] = slots
    C["outbox_bytes"] = slots * EVENT_RECORD_BYTES
    C["rule_bytes_estimate"] = int(fixed)
    usable = slots - args.reserve - args.initial_backlog
    C["usable_slots"] = usable
    if usable < 0:
        out["warnings"].append("reserve + initial backlog exceed the outbox: the first transition is refused")

    profile = None
    if args.burst_profile:
        with open(args.burst_profile) as f:
            profile = [float(x) for x in f.read().replace(",", " ").split()]

    def scenario(mu):
        s = {}
        net = lam - mu
        s["net_growth_per_s"] = net
        s["time_to_full_s"] = (usable / net) if net > 0 and usable >= 0 else (0.0 if usable < 0 else INF)
        req = args.initial_backlog + args.reserve + max(0.0, net) * args.storm_seconds
        s["required_slots_for_storm"] = math.ceil(req)
        s["storm_verdict"] = "PASS" if req <= slots else "FAIL"
        if profile is not None:
            exc = max_prefix_excess(profile, mu)
            s["burst_max_prefix_excess"] = exc
            s["required_slots_for_burst"] = math.ceil(args.initial_backlog + args.reserve + exc)
            s["burst_verdict"] = "PASS" if s["required_slots_for_burst"] <= slots else "FAIL"
        backlog = args.initial_backlog
        s["backlog_drain_time_s"] = (backlog / (mu - lam)) if mu > lam else (0.0 if backlog == 0 else INF)
        return s

    C["vs005_mu_0"] = scenario(0.0)
    if mu_proj > 0:
        C["vs006_projection"] = scenario(mu_proj)
    if mu_measured:
        C["vs006_journal_measured"] = scenario(mu_measured)
        if lam > mu_measured:
            out["warnings"].append(
                f"event rate {lam}/s exceeds the measured durable journal rate {mu_measured:.0f}/s: the outbox fills, "
                "then input is refused (JOURNAL_QUEUE_FULL or OUTBOX_CAPACITY_EXCEEDED, whichever fills first), never silently lost")
    # Journal queue (operations, including zero-event ones) and disk.
    mu_ops = A["journal_ops_s_zero_event"] if epo == 0 else A["journal_ops_s_in_process_at_fanout"]
    j = {"mu_ops_s_measured": mu_ops}
    if mu_ops:
        net = ops_rate - mu_ops
        j["queue_time_to_full_s"] = (args.journal_queue_operations / net) if net > 0 else INF
        if net > 0:
            out["warnings"].append(f"operation rate {ops_rate}/s exceeds the measured journal rate {mu_ops:.0f} ops/s: "
                                   "JOURNAL_QUEUE_FULL refusals after the queue fills")
    if args.op_queue_bytes:
        j["queue_bytes_needed"] = args.journal_queue_operations * args.op_queue_bytes
    bpo, bpe = A["bytes_per_op_zero_event"], A["event_table_bytes_per_event"]
    if bpo is not None and bpe is not None:
        growth = ops_rate * bpo + lam * bpe
        j["disk_growth_bytes_s"] = growth
        j["disk_growth_gib_day"] = growth * 86400 / (1 << 30)
        if args.disk_free_mib is not None:
            room = (args.disk_free_mib - args.min_disk_free_mib) * (1 << 20)
            j["time_to_disk_floor_s"] = (room / growth) if growth > 0 and room > 0 else (0.0 if room <= 0 else INF)
    C["journal"] = j
    kmax = A["kernel_events_s_process_drain_pass"]
    if kmax is not None and lam > kmax:
        out["warnings"].append(f"event rate {lam}/s exceeds the measured sustained kernel rate {kmax}/s (level A)")
    if profile is not None and kmax is not None and max(profile, default=0) > kmax:
        out["warnings"].append("a second of the burst profile exceeds the measured sustained kernel rate")
    out["limitation"] = (
        "NON PRODUCTION-READY FOR LONG-RUN ALARM RETENTION without [persistence]: no slot is ever released "
        "(vs005_mu_0). With the VS-006 journal, mu is what was MEASURED on the benchmark storage: a different "
        "disk (SD card, eMMC, ARM64 Micro: NOT TESTED ON HARDWARE) needs its own level-A run. The alarm history "
        "has no automatic retention: the disk floor stops admission (DISK_FULL)."
    )
    return out


def fmt_s(x):
    if x == INF:
        return "never"
    if x >= 86400:
        return f"{x / 86400:.1f} d"
    if x >= 3600:
        return f"{x / 3600:.1f} h"
    return f"{x:.1f} s"


def human(o):
    A, B, C = o["level_A_measured"], o["level_B_assumed"], o["level_C_computed"]
    lines = ["== A. MEASURED (benchmark raw results)"]
    for k, v in A.items():
        lines.append(f"  {k} = {v}")
    lines.append("== B. ASSUMED (plant, not measured)")
    for k, v in B.items():
        lines.append(f"  {k} = {v}")
    lines.append("== C. COMPUTED")
    lines.append(f"  slots = {C['slots']} ({C['outbox_bytes'] / (1 << 20):.1f} MiB of events); usable = {C['usable_slots']}")
    for name in ("vs005_mu_0", "vs006_projection", "vs006_journal_measured"):
        if name in C:
            s = C[name]
            lines.append(f"  [{name}] net growth {s['net_growth_per_s']}/s, time to full {fmt_s(s['time_to_full_s'])}, "
                         f"storm needs {s['required_slots_for_storm']} slots -> {s['storm_verdict']}, "
                         f"backlog drain {fmt_s(s['backlog_drain_time_s'])}")
            if "required_slots_for_burst" in s:
                lines.append(f"  [{name}] burst: max cumulative excess {s['burst_max_prefix_excess']:.0f} -> "
                             f"{s['required_slots_for_burst']} slots -> {s['burst_verdict']}")
    jq = C.get("journal", {})
    if jq:
        parts = []
        if "queue_time_to_full_s" in jq:
            parts.append(f"journal queue full in {fmt_s(jq['queue_time_to_full_s'])}")
        if "disk_growth_gib_day" in jq:
            parts.append(f"disk growth {jq['disk_growth_gib_day']:.2f} GiB/day")
        if "time_to_disk_floor_s" in jq:
            parts.append(f"disk floor in {fmt_s(jq['time_to_disk_floor_s'])}")
        lines.append("  [journal] " + ", ".join(parts))
    for w in o["warnings"]:
        lines.append(f"WARNING: {w}")
    lines.append(o["limitation"])
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bench-results", action="append",
                    help="scada-bench raw lines (alarm-storm / alarms-core / alarms-cold / journal); repeatable")
    ap.add_argument("--ram-budget-mib", type=float, help="RAM for the alarm outbox + rule tables")
    ap.add_argument("--outbox-slots", type=int, help="configured outbox_capacity_events")
    ap.add_argument("--initial-backlog", type=int, default=0, help="events already in the outbox")
    ap.add_argument("--event-rate", type=float, required=True, help="lambda: sustained alarm events/s (assumed)")
    ap.add_argument("--durable-rate", type=float, default=0.0,
                    help="mu ASSUMED (level B, e.g. other storage); the measured journal mu comes from --bench-results")
    ap.add_argument("--events-per-op", type=float, default=1.0, help="alarm events per operation (0 = zero-event ops)")
    ap.add_argument("--ops-rate", type=float, help="operations/s incl. zero-event ones (default lambda / events-per-op)")
    ap.add_argument("--disk-free-mib", type=float, help="free space of the journal filesystem")
    ap.add_argument("--min-disk-free-mib", type=float, default=256.0, help="[persistence] min_disk_free_bytes in MiB")
    ap.add_argument("--journal-queue-operations", type=int, default=65536, help="[persistence] journal_queue_operations")
    ap.add_argument("--op-queue-bytes", type=float, help="bytes one queued operation reserves (assumed)")
    ap.add_argument("--storm-seconds", type=float, default=60.0, help="storm duration to survive")
    ap.add_argument("--reserve", type=int, default=0, help="slots kept free (e.g. >= worst fan-out of one operation)")
    ap.add_argument("--rules", type=int, default=0, help="number of rules (for the RAM split)")
    ap.add_argument("--burst-profile", help="file with per-second event counts (whitespace or comma separated)")
    ap.add_argument("--json", action="store_true", help="JSON only")
    a = ap.parse_args(argv)
    for name in ("initial_backlog", "reserve", "rules"):
        if getattr(a, name) < 0:
            ap.error(f"--{name.replace('_', '-')} must be >= 0")
    if a.event_rate < 0 or a.durable_rate < 0 or a.storm_seconds < 0 or a.events_per_op < 0 \
            or (a.ops_rate is not None and a.ops_rate < 0):
        ap.error("rates and durations must be >= 0")
    o = model(a, parse_bench(a.bench_results))
    if a.json:
        print(json.dumps(o, indent=2, default=lambda x: None if x == INF else x).replace("Infinity", "null"))
    else:
        print(human(o))
        print()
        print(json.dumps(o, default=str))
    return o


if __name__ == "__main__":
    main()
    sys.exit(0)
