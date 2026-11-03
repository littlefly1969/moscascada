#!/usr/bin/env python3
"""Markdown tables (median [min-max] over the runs of each label) from the raw VS-007 lines."""
import collections
import re
import statistics
import sys

COLS = [("scans_per_s", "scan/s"), ("observations_per_s", "oss/s"), ("acks_durable_per_s", "ACK dur./s"),
        ("ack_p50_us_worst", "p50 µs"), ("ack_p99_us_worst", "p99 µs"), ("ack_p999_us_worst", "p99.9 µs"),
        ("cpu_kernel_pct", "CPU kernel %"), ("cpu_driver_pct", "CPU driver %"), ("cpu_sim_pct", "CPU sim %"),
        ("hwm_kernel_kib", "picco RSS kernel KiB"), ("hwm_driver_kib", "picco RSS driver KiB"), ("overruns", "overrun")]


def cell(vals):
    vals = sorted(vals)
    m = statistics.median(vals)
    f = (lambda x: f"{x:.0f}") if max(vals) >= 10 else (lambda x: f"{x:.1f}")
    return f(m) if len(vals) == 1 or vals[0] == vals[-1] else f"{f(m)} [{f(vals[0])}–{f(vals[-1])}]"


for path in sys.argv[1:]:
    g = collections.OrderedDict()
    for line in open(path):
        if line.startswith("kind=vs007"):
            r = dict(re.findall(r"(\w+)=(\S+)", line))
            g.setdefault(r["label"], []).append(r)
    print(f"\n#### `{path.split('/')[-1]}`\n")
    print("| cella | n | " + " | ".join(c for _, c in COLS) + " |")
    print("|---|---:|" + "---:|" * len(COLS))
    for lab, rs in g.items():
        vals = []
        for k, _ in COLS:
            v = [float(r[k]) for r in rs if r.get(k, "-") != "-"]
            vals.append(cell(v) if v else "-")
        print(f"| {lab} | {len(rs)} | " + " | ".join(vals) + " |")
