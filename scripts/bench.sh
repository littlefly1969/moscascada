#!/usr/bin/env bash
# Reproducible benchmark run. Writes docs/benchmarks/results/<host>-<date>.md.
# Run on an otherwise idle machine; results are only meaningful with their environment.
set -euo pipefail
cd "$(dirname "$0")/.."
SECS=${SECS:-10}; WARMUP=${WARMUP:-2}; SAT_SECS=${SAT_SECS:-60}; OPS=${OPS:-1000000}
cargo build --release --workspace
out="docs/benchmarks/results/$(hostname)-$(date -u +%Y%m%dT%H%M%SZ).md"
mkdir -p "$(dirname "$out")"
{
  echo "# VS-001 benchmark run $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "## Environment"
  echo '```'
  uname -srmo; lscpu | grep -E 'Model name|^CPU\(s\)|Hypervisor|Thread\(s\)'; free -m | head -2
  rustc -Vv | head -4; echo "profile: release (lto=thin, codegen-units=1)"
  echo '```'
  echo
  ./target/release/scada-bench core --ops "$OPS"
  ./target/release/scada-bench e2e --secs "$SECS" --warmup-secs "$WARMUP"
  ./target/release/scada-bench saturation --secs "$SAT_SECS"
  echo "## Binaries (release, stripped size in parentheses)"
  echo
  for b in scada-kernel scada-sim-driver; do
    sz=$(stat -c %s "target/release/$b"); cp "target/release/$b" /tmp/$b.strip; strip /tmp/$b.strip
    echo "- $b: $sz bytes ($(stat -c %s /tmp/$b.strip) stripped); dynamic libs: $(ldd target/release/$b | awk '{print $1}' | tr '\n' ' ')"
  done
  echo "- runtime crates linked into scada-kernel: $(cargo tree -p scada-runtime -e normal --prefix none | sed 's/ (.*//' | sort -u | wc -l)"
  echo "- runtime crates linked into scada-core: $(cargo tree -p scada-core -e normal --prefix none | sed 's/ (.*//' | sort -u | wc -l)"
} | tee "$out"
