#!/usr/bin/env python3
"""Summarize EXP-001 campaign CSV files into Markdown tables (median [min-max] n=...).

    python3 scripts/exp001_summary.py docs/benchmarks/results/exp001/*.csv > docs/benchmarks/results/exp001/summary.md
"""
import csv
import statistics
import sys
from collections import defaultdict

MAIN = [
    ("ops_per_s", "durable op/s", 0),
    ("historian_ops_per_s", "historian-ready op/s", 0),
    ("events_per_s", "durable ev/s", 0),
    ("commit_us_p99", "commit p99 µs", 0),
    ("durable_lag_us_p50", "receipt p50 µs", 0),
    ("durable_lag_us_p99", "receipt p99 µs", 0),
    ("durable_lag_us_p999", "receipt p99.9 µs", 0),
    ("fsync_per_s", "commits/s", 0),
    ("io_bytes_per_op", "written B/op", 0),
    ("disk_mib", "disk MiB (alloc)", 1),
    ("rss_hwm_mib", "RSS peak MiB", 1),
    ("cpu_total_s", "CPU s", 2),
]
REC = [("recover_ms", "recovery ms", 1), ("projection_rebuild_ms", "projection rebuild ms", 0)]
QRY = [("page_us_p50", "page(1000) p50 µs", 0), ("page_us_p99", "page(1000) p99 µs", 0)]


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fmt(vals, dec):
    vals = [v for v in vals if v is not None]
    if not vals:
        return "—"
    m = statistics.median(vals)
    f = f"{{:,.{dec}f}}"
    if len(vals) == 1:
        return f.format(m)
    return f"{f.format(m)} [{f.format(min(vals))}–{f.format(max(vals))}]"


def main():
    rows = []
    for p in sys.argv[1:]:
        with open(p) as f:
            rows += list(csv.DictReader(f))
    for r in rows:
        if r.get("kind") == "exp001":
            jb, pb = num(r.get("journal_bytes")) or 0, num(r.get("projection_bytes")) or 0
            r["disk_mib"] = (jb + pb) / 1048576
            r["rss_hwm_mib"] = (num(r.get("rss_hwm_kib")) or 0) / 1024
    by = defaultdict(lambda: defaultdict(list))
    order = []
    for r in rows:
        if r.get("kind") not in ("exp001", "exp001-recovery", "exp001-query"):
            continue
        key = (r["phase"], r["cell"])
        if key not in order:
            order.append(key)
        by[key][(r["kind"], r["variant"])].append(r)
    fs = [r for r in rows if r.get("kind") == "fsync-probe"]
    if fs:
        print("### fdatasync probe (same filesystem)\n")
        print("| append B | p50 µs | p99 µs | p99.9 µs | max µs |\n|---|---|---|---|---|")
        for r in fs:
            print(f"| {r['append_bytes']} | {r['fdatasync_us_p50']} | {r['fdatasync_us_p99']} | {r['fdatasync_us_p999']} | {r['fdatasync_us_max']} |")
        print()
    for key in order:
        g = by[key]
        variants = sorted({v for (_, v) in g})
        n = {v: len(g.get(("exp001", v), [])) for v in variants}
        print(f"### {key[0]} / {key[1]}  (n = {', '.join(f'{v.upper()}:{n[v]}' for v in variants)})\n")
        print("| metric | " + " | ".join(v.upper() for v in variants) + " |")
        print("|---|" + "---|" * len(variants))
        for col, title, dec in MAIN:
            cells = [fmt([num(r.get(col)) for r in g.get(("exp001", v), [])], dec) for v in variants]
            print(f"| {title} | " + " | ".join(cells) + " |")
        for col, title, dec in QRY:
            cells = [fmt([num(r.get(col)) for r in g.get(("exp001-query", v), [])], dec) for v in variants]
            if any(c != "—" for c in cells):
                print(f"| {title} | " + " | ".join(cells) + " |")
        for col, title, dec in REC:
            cells = [fmt([num(r.get(col)) for r in g.get(("exp001-recovery", v), [])], dec) for v in variants]
            if any(c != "—" for c in cells):
                print(f"| {title} | " + " | ".join(cells) + " |")
        for col, title, dec in (("read_us_p99", "concurrent read p99 µs", 0), ("read_lag_events_p99", "reader lag p99 (events)", 0)):
            vals = [fmt([num(r.get(col)) for r in g.get(("exp001", v), [])], dec) for v in variants]
            if any(c not in ("—", "0", "0 [0–0]") for c in vals):
                print(f"| {title} | " + " | ".join(vals) + " |")
        # Ratios of medians against A (same cell).
        def med(v, col):
            xs = [num(r.get(col)) for r in g.get(("exp001", v), [])]
            xs = [x for x in xs if x]
            return statistics.median(xs) if xs else None
        a = med("a", "ops_per_s")
        if a and "c" in variants:
            ch, cd = med("c", "historian_ops_per_s"), med("c", "ops_per_s")
            print(f"| C/A historian-ready op/s | | | {ch / a:.2f}× |" if len(variants) == 3 else f"| C/A historian-ready op/s | | {ch / a:.2f}× |")
            print(f"| C/A durable op/s | | | {cd / a:.2f}× |" if len(variants) == 3 else f"| C/A durable op/s | | {cd / a:.2f}× |")
        print()


if __name__ == "__main__":
    main()
