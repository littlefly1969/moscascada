#!/usr/bin/env python3
"""Aggregate raw campaign lines (bench_campaign.py) into Markdown tables.

  bench_analyze.py FILE --baseline NAME [--metrics batches_s,p99_ns,...] [--group spec]

For each (spec, variant): median and [min-max] over runs, plus the change of the median
against the baseline variant and an exact two-sided permutation test on the difference
of means (all splits for small n, otherwise 20 000 random splits, fixed seed). p < 0.05
with |Δ| above the baseline's own spread is marked as a repeatable difference; anything
else is reported as "~" (within noise). No interpolation, no invented percentiles: the
latency columns are the per-run percentiles measured by scada-bench.
"""
import argparse, itertools, random, statistics, sys
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("file")
ap.add_argument("--baseline", required=True)
ap.add_argument("--metrics", default="batches_s,p50_ns,p99_ns,p999_ns,cpu_pct,rss_steady_kib")
ap.add_argument("--variants", default="")
a = ap.parse_args()

rows = defaultdict(lambda: defaultdict(list))
order_specs, order_vars = [], []
for line in open(a.file):
    if not line.startswith("round="):
        continue
    kv = dict(x.split("=", 1) for x in line.split() if "=" in x)
    spec, var = kv["spec"], kv["variant"]
    if spec not in order_specs:
        order_specs.append(spec)
    if var not in order_vars:
        order_vars.append(var)
    rows[spec][var].append(kv)
if a.variants:
    order_vars = a.variants.split(",")

def perm_p(x, y, n_rand=20000):
    obs = abs(statistics.mean(x) - statistics.mean(y))
    pool, n = x + y, len(x)
    combos = None
    from math import comb
    if comb(len(pool), n) <= 50000:
        combos = itertools.combinations(range(len(pool)), n)
        tot = hit = 0
        for c in combos:
            s = set(c)
            xa = [pool[i] for i in s]
            ya = [pool[i] for i in range(len(pool)) if i not in s]
            tot += 1
            hit += abs(statistics.mean(xa) - statistics.mean(ya)) >= obs - 1e-12
        return hit / tot
    rng = random.Random(7)
    hit = 0
    for _ in range(n_rand):
        rng.shuffle(pool)
        hit += abs(statistics.mean(pool[:n]) - statistics.mean(pool[n:])) >= obs - 1e-12
    return hit / n_rand

def fmt(m, v):
    if m.endswith("_ns"):
        return f"{v / 1000:.1f}"
    if m.endswith("_s") or m.endswith("_kib"):
        return f"{v:.0f}"
    return f"{v:.1f}"

metrics = a.metrics.split(",")
unit = {m: ("µs" if m.endswith("_ns") else "") for m in metrics}
for spec in order_specs:
    print(f"\n#### `{spec.replace('/', ' ')}`\n")
    hdr = ["variant", "n"] + [f"{m}{' ' + unit[m] if unit[m] else ''} median [min–max] (Δ vs {a.baseline})" for m in metrics]
    print("| " + " | ".join(hdr) + " |")
    print("|" + "---|" * len(hdr))
    base = rows[spec].get(a.baseline)
    for var in order_vars:
        runs = rows[spec].get(var)
        if not runs:
            continue
        cells = [var, str(len(runs))]
        for m in metrics:
            vals = [float(r[m]) for r in runs if m in r]
            if not vals:
                cells.append("–")
                continue
            med = statistics.median(vals)
            txt = f"{fmt(m, med)} [{fmt(m, min(vals))}–{fmt(m, max(vals))}]"
            if base and var != a.baseline:
                bvals = [float(r[m]) for r in base if m in r]
                if bvals and statistics.median(bvals) != 0:
                    bmed = statistics.median(bvals)
                    d = (med - bmed) / bmed * 100
                    p = perm_p(vals, bvals)
                    spread = (max(bvals) - min(bvals)) / abs(bmed) * 100 / 2
                    sig = "**" if (p < 0.05 and abs(d) > spread) else ""
                    mark = f"{d:+.1f}%" if sig else f"~{d:+.1f}%"
                    txt += f" ({sig}{mark}{sig}, p={p:.3f})"
            cells.append(txt)
        print("| " + " | ".join(cells) + " |")
