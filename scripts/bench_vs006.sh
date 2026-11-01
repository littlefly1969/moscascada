#!/usr/bin/env bash
# VS-006 benchmark campaign (brief §7). Raw lines only; analysis in docs/benchmarks/vs006.md.
#
#   scripts/bench_vs006.sh ab        A/B process cells (n=RUNS, default 5)
#   scripts/bench_vs006.sh confirm   n=10 on the critical cells (default-off regression)
#   scripts/bench_vs006.sh journal   in-process journal: fan-out, rows, disk vs tmpfs, FULL vs NORMAL
#   scripts/bench_vs006.sh storm     60 s process storms with the journal as sink
#
# Variants (same scada-bench binary for all):
#   A  = merged main 9f335df (VS-005) kernel, no persistence (BASE)
#   B  = VS-006 kernel, [persistence] absent (default-off: must not regress vs A)
#   P  = VS-006 kernel, [persistence] durability=full, store on the VM disk (ext4)
#   D  = P + clients negotiate 1.2 durable receipts (ACK only after COMMIT + fsync)
#   C  = B + 2057 alarm rules (VS-005 quiet scenario); CP = C + persistence full
# tmpfs results are DIAGNOSTIC ONLY (fsync is a no-op): never a durability figure.
# NORMAL results are labelled NOT DURABLE (WAL not fsynced at every commit).
set -euo pipefail
cd "$(dirname "$0")/.."
OUTDIR=${OUTDIR:-docs/benchmarks/results/vs006}
BASE=${BASE:-/tmp/claude-0/base-bin/scada-kernel}
NEW=${NEW:-/tmp/claude-0/vs006-bin/scada-kernel}
BENCH=${BENCH:-/tmp/claude-0/vs006-bin/scada-bench}
RUNS=${RUNS:-5}
MT="--scheduler,multi_thread,--worker-threads,2"
CELLS=(--cell 'cell clients=1 size=1 pipeline=1' --cell 'cell clients=1 size=128 pipeline=1'
       --cell 'cell clients=8 size=1 pipeline=1' --cell 'cell clients=8 size=128 pipeline=32'
       --cell 'cell clients=16 size=128 pipeline=32')
P='@bench:--persistence=full'
D='@flag:--durable-receipts'
AL='@bench:--alarms=1:65536'

case "${1:-}" in
ab)
  python3 scripts/bench_campaign.py --out "$OUTDIR/ab.txt" --runs "$RUNS" --seed 606 --bench "$BENCH" \
    --variant "A_ct=$BASE" --variant "B_ct=$NEW" --variant "P_ct=$NEW,$P" --variant "D_ct=$NEW,$P,$D" \
    --variant "A_mt2=$BASE,$MT" --variant "B_mt2=$NEW,$MT" --variant "P_mt2=$NEW,$MT,$P" --variant "D_mt2=$NEW,$MT,$P,$D" \
    --variant "C_ct=$NEW,$AL" --variant "CP_ct=$NEW,$AL,$P" --variant "C_mt2=$NEW,$MT,$AL" --variant "CP_mt2=$NEW,$MT,$AL,$P" \
    "${CELLS[@]}"
  ;;
confirm)
  python3 scripts/bench_campaign.py --out "$OUTDIR/ab-confirm.txt" --runs 10 --seed 607 --bench "$BENCH" \
    --variant "A_ct=$BASE" --variant "B_ct=$NEW" --variant "A_mt2=$BASE,$MT" --variant "B_mt2=$NEW,$MT" \
    --cell 'cell clients=1 size=1 pipeline=1' --cell 'cell clients=8 size=128 pipeline=32' \
    --cell 'cell clients=16 size=128 pipeline=32'
  ;;
journal)
  O="$OUTDIR/journal.txt"
  J() { echo "# $*" >> "$O"; "$BENCH" journal "$@" 2>/dev/null | grep '^kind=' >> "$O"; tail -3 "$O"; }
  echo "# vs006 journal campaign $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(uname -srm)" >> "$O"
  mkdir -p /tmp/claude-0/jdisk /dev/shm/jtmpfs
  for fs in disk tmpfs; do
    d=/tmp/claude-0/jdisk; [ $fs = tmpfs ] && d=/dev/shm/jtmpfs
    J --store "$d" --label "E0_${fs}_full" --ops 1000000 --events-per-op 0 --queries 200
    J --store "$d" --label "E1_${fs}_full" --ops 1000000 --events-per-op 1
    J --store "$d" --label "E100_${fs}_full" --ops 20000 --events-per-op 100
    J --store "$d" --label "E1000_${fs}_full" --ops 2000 --events-per-op 1000
  done
  J --store /tmp/claude-0/jdisk --label "E1_disk_NORMAL_NOT_DURABLE" --ops 1000000 --events-per-op 1 --durability normal
  J --store /tmp/claude-0/jdisk --label "F8192_disk_full" --ops 500 --events-per-op 8192 --ring 1048576
  J --store /tmp/claude-0/jdisk --label "F20000_disk_full" --ops 300 --events-per-op 20000 --ring 1048576
  # Historian size points: 1M and 10M event rows.
  J --store /tmp/claude-0/jdisk --label "rows10M_E1_disk_full" --ops 10000000 --events-per-op 1 --checkpoint-every 1000000
  J --store /tmp/claude-0/jdisk --label "rows10M_E100_disk_full" --ops 100000 --events-per-op 100 --checkpoint-every 10000
  # Group-commit size: commit_max 1 = one fsync per operation (the floor of the disk).
  J --store /tmp/claude-0/jdisk --label "E1_disk_full_commit1" --ops 20000 --events-per-op 1 --commit-max 1
  rm -rf /tmp/claude-0/jdisk /dev/shm/jtmpfs
  ;;
storm)
  O="$OUTDIR/storm.txt"
  SECS=${SECS:-60}
  run() { echo "# $*" >> "$O"; "$BENCH" alarm-storm --kernel "$NEW" --secs "$SECS" --warmup-secs 2 "$@" 2>/dev/null | grep '^kind=' >> "$O"; tail -2 "$O"; }
  KMT=(--kernel-arg --scheduler --kernel-arg multi_thread --kernel-arg --worker-threads --kernel-arg 2)
  echo "# vs006 storm campaign $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(uname -srm) secs=$SECS" >> "$O"
  # J1 regular: unthrottled storm, journal draining the outbox (released = durable watermark).
  run --label J1_ct_regular --alarms 1:1048576:storm --clients 4 --size 128 --persistence full
  run --label J1_mt2_regular --alarms 1:1048576:storm --clients 4 --size 128 --persistence full "${KMT[@]}"
  # J2 plant-like 1000 events/s: must absorb 60 s with zero refusals.
  run --label J2_ct_plant_1000eps --alarms 1:65536:storm --clients 1 --size 10 --interval-us 10000 --persistence full
  # J3 slow disk: +2 ms per commit (test hook), regular storm.
  run --label J3_ct_slow_2ms --alarms 1:1048576:storm --clients 4 --size 128 --persistence full --kernel-arg --test-journal-delay-us --kernel-arg 2000
  # J4 stalled 20 s then catch-up: backpressure, then drain of the backlog.
  run --label J4_ct_stall20s_catchup --alarms 1:65536:storm --clients 1 --size 10 --interval-us 10000 --persistence full --stall-at 10 --stall-for 20
  run --label J4b_ct_stall20s_unthrottled --alarms 1:1048576:storm --clients 4 --size 128 --persistence full --stall-at 10 --stall-for 20
  # J5 100k rules (49 per tag x 2048 tags = 100 352): fan-out 49 events per observation.
  run --label J5_ct_100k_rules --alarms 49:1048576:storm --clients 1 --size 16 --persistence full
  # J6 NORMAL (NOT DURABLE) for the cost split fsync vs rest.
  run --label J6_ct_regular_NORMAL_NOT_DURABLE --alarms 1:1048576:storm --clients 4 --size 128 --persistence 'durability = "normal"' --kernel-arg --test-allow-non-durable
  ;;
*) echo "usage: $0 ab|confirm|journal|storm" >&2; exit 2 ;;
esac
