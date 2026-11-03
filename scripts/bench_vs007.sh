#!/usr/bin/env bash
# VS-007 benchmark campaign (spec §7). Raw lines only; analysis in docs/benchmarks/vs007.md.
#
#   scripts/bench_vs007.sh raw      Modbus client alone vs simulator (no kernel), 1/16/125 registers
#   scripts/bench_vs007.sh matrix   devices 1/8/16 x block 1/16/125, journal off vs FULL+durable (n=RUNS)
#   scripts/bench_vs007.sh variants FIFO/source_fair, current_thread/multi_thread(2), volatile ACK on FULL
#   scripts/bench_vs007.sh plant    realistic slow scan: 16 devices, 1 s scan, rare changes, 60 s
#
# Every cell runs the REAL processes on loopback: scada-kernel (unchanged VS-006),
# scada-driver-modbus-tcp, scada-modbus-sim. 10 ms is the lowest scan interval the driver
# accepts: "saturated" here means every device at 100 scans/s with every register changing
# at every scan (--mutate-every 1), not an unbounded loop.
set -euo pipefail
cd "$(dirname "$0")/.."
OUTDIR=${OUTDIR:-docs/benchmarks/results/vs007}
RUNS=${RUNS:-3}
SECS=${SECS:-10}
mkdir -p "$OUTDIR"
cargo build -q --release -p scada-runtime -p scada-driver-modbus-tcp -p scada-modbus-sim --bins --examples
B() { python3 scripts/bench_vs007.py --secs "$SECS" --warmup 2 "$@"; }
hdr() { echo "# vs007 $1 $(date -u +%Y-%m-%dT%H:%M:%SZ) host=$(uname -srm) commit=$(git rev-parse --short HEAD) runs=$RUNS secs=$SECS" >> "$2"; }

case "${1:-}" in
raw)
  O="$OUTDIR/raw.txt"; hdr raw "$O"
  port=15${RANDOM:0:3}
  target/release/scada-modbus-sim --listen 127.0.0.1:$port --registers 125 > /dev/null &
  sim=$!; sleep 0.5
  for r in $(seq "$RUNS"); do for q in 1 16 125; do
    target/release/examples/scan_loop 127.0.0.1:$port input_register $q 5 | tee -a "$O"
  done; done
  kill $sim
  ;;
matrix)
  O="$OUTDIR/matrix.txt"; hdr matrix "$O"
  for r in $(seq "$RUNS"); do
    for dev in 1 8 16; do for q in 1 16 125; do
      B --out "$O" --label "off_d${dev}_q${q}" --devices $dev --qty $q --journal off
      B --out "$O" --label "full_durable_d${dev}_q${q}" --devices $dev --qty $q --journal full --durable
    done; done
  done
  ;;
variants)
  O="$OUTDIR/variants.txt"; hdr variants "$O"
  for r in $(seq "$RUNS"); do
    for j in "off" "full --durable"; do
      jl=${j// /_}; jl=${jl//--/}
      B --out "$O" --label "${jl}_fifo_ct_d16_q125" --devices 16 --qty 125 --journal $j
      B --out "$O" --label "${jl}_fair4_ct_d16_q125" --devices 16 --qty 125 --journal $j --ingress source_fair:4
      B --out "$O" --label "${jl}_fifo_mt2_d16_q125" --devices 16 --qty 125 --journal $j --scheduler multi_thread:2
      B --out "$O" --label "${jl}_fair4_mt2_d16_q125" --devices 16 --qty 125 --journal $j --ingress source_fair:4 --scheduler multi_thread:2
    done
    B --out "$O" --label "full_volatile_fifo_ct_d16_q125" --devices 16 --qty 125 --journal full
    B --out "$O" --label "full_volatile_fifo_ct_d1_q16" --devices 1 --qty 16 --journal full
  done
  ;;
plant)
  O="$OUTDIR/plant.txt"; hdr plant "$O"
  SECS=60 B --out "$O" --label "plant_full_durable_d16_q16_1s" --devices 16 --qty 16 --scan-ms 1000 --mutate-every 50 --journal full --durable
  SECS=60 B --out "$O" --label "plant_off_d16_q16_1s" --devices 16 --qty 16 --scan-ms 1000 --mutate-every 50 --journal off
  ;;
*)
  sed -n 2,12p "$0"; exit 2 ;;
esac
