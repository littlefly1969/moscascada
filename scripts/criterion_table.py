#!/usr/bin/env python3
"""Print criterion results (target/criterion/**/new/estimates.json) as a Markdown table:
median point estimate and its 95 % confidence interval, in ns."""
import json, os, sys
root = sys.argv[1] if len(sys.argv) > 1 else "target/criterion"
rows = []
for d, _, files in os.walk(root):
    if d.endswith(os.sep + "new") and "estimates.json" in files:
        bench = json.load(open(os.path.join(d, "benchmark.json")))
        e = json.load(open(os.path.join(d, "estimates.json")))["median"]
        rows.append((bench["full_id"], e["point_estimate"], e["confidence_interval"]["lower_bound"], e["confidence_interval"]["upper_bound"]))
print("| benchmark | median ns | 95 % CI ns |")
print("|---|---:|---:|")
for name, m, lo, hi in sorted(rows):
    print(f"| `{name}` | {m:.1f} | {lo:.1f} – {hi:.1f} |")
