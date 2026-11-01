#!/usr/bin/env python3
"""VS-005 alarm outbox capacity model (ADR 0012 §capacity, brief T15).

Sizes `[alarms] outbox_capacity_events` and states for how long a configuration
survives an alarm storm. Three levels are kept apart in the output:

  A. MEASURED   numbers read from benchmark raw results (--bench-results):
                record sizes, kernel event throughput, per-rule memory.
  B. ASSUMED    plant assumptions given on the command line: event rate, durable
                drain rate, storm duration, reserve, initial backlog. Not measured.
  C. COMPUTED   pure arithmetic from A and B (formulas below). Never a measurement.

VS-005 has NO durable consumer: slots are released only by the VS-006 journal
watermark (or the TEST-ONLY simulated sink). The real drain rate mu is therefore 0
and every event ever produced occupies a slot until the process restarts. A
`--durable-rate` > 0 is reported as a VS-006 PROJECTION, side by side with the
VS-005 reality (mu = 0).

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


def parse_bench(path):
    """Level A: extract measured numbers from scada-bench raw lines (key=value)."""
    a = {"source": path, "storm_events_s": [], "core_events_s": [], "layout": {}, "per_rule_bytes": None}
    if not path:
        return a
    with open(path) as f:
        for line in f:
            if line.startswith("#"):
                continue
            kv = dict(re.findall(r"(\w+)=(\S+)", line))
            kind = kv.get("kind")
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

    lam, mu_proj = args.event_rate, args.durable_rate
    B.update(
        event_rate_per_s=lam,
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
        out["warnings"].append("durable_rate > 0 is a VS-006 PROJECTION: VS-005 has no durable consumer (mu = 0)")

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
    kmax = A["kernel_events_s_process_drain_pass"]
    if kmax is not None and lam > kmax:
        out["warnings"].append(f"event rate {lam}/s exceeds the measured sustained kernel rate {kmax}/s (level A)")
    if profile is not None and kmax is not None and max(profile, default=0) > kmax:
        out["warnings"].append("a second of the burst profile exceeds the measured sustained kernel rate")
    out["limitation"] = (
        "NON PRODUCTION-READY FOR LONG-RUN ALARM RETENTION: without the VS-006 journal no slot is ever "
        "released; time_to_full is the lifetime of the process at the assumed rate."
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
    for name in ("vs005_mu_0", "vs006_projection"):
        if name in C:
            s = C[name]
            lines.append(f"  [{name}] net growth {s['net_growth_per_s']}/s, time to full {fmt_s(s['time_to_full_s'])}, "
                         f"storm needs {s['required_slots_for_storm']} slots -> {s['storm_verdict']}, "
                         f"backlog drain {fmt_s(s['backlog_drain_time_s'])}")
            if "required_slots_for_burst" in s:
                lines.append(f"  [{name}] burst: max cumulative excess {s['burst_max_prefix_excess']:.0f} -> "
                             f"{s['required_slots_for_burst']} slots -> {s['burst_verdict']}")
    for w in o["warnings"]:
        lines.append(f"WARNING: {w}")
    lines.append(o["limitation"])
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bench-results", help="scada-bench raw lines (alarm-storm / alarms-core / alarms-cold)")
    ap.add_argument("--ram-budget-mib", type=float, help="RAM for the alarm outbox + rule tables")
    ap.add_argument("--outbox-slots", type=int, help="configured outbox_capacity_events")
    ap.add_argument("--initial-backlog", type=int, default=0, help="events already in the outbox")
    ap.add_argument("--event-rate", type=float, required=True, help="lambda: sustained alarm events/s (assumed)")
    ap.add_argument("--durable-rate", type=float, default=0.0, help="mu: durable releases/s (VS-006 projection; 0 in VS-005)")
    ap.add_argument("--storm-seconds", type=float, default=60.0, help="storm duration to survive")
    ap.add_argument("--reserve", type=int, default=0, help="slots kept free (e.g. >= worst fan-out of one operation)")
    ap.add_argument("--rules", type=int, default=0, help="number of rules (for the RAM split)")
    ap.add_argument("--burst-profile", help="file with per-second event counts (whitespace or comma separated)")
    ap.add_argument("--json", action="store_true", help="JSON only")
    a = ap.parse_args(argv)
    for name in ("initial_backlog", "reserve", "rules"):
        if getattr(a, name) < 0:
            ap.error(f"--{name.replace('_', '-')} must be >= 0")
    if a.event_rate < 0 or a.durable_rate < 0 or a.storm_seconds < 0:
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
