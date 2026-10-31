#!/usr/bin/env bash
# Full stable gate: format, lints, tests, unsafe audit, schema checks, ARM64 compile check.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== toolchain"; rustc --version; cargo --version
echo "== fmt";    cargo fmt --all --check
echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings
echo "== test";   cargo test --workspace
echo "== test (release, runtime under multi_thread(2))"
# The multiprocess tests spawn the simulator binary, which `-p scada-runtime` does not build.
cargo build --release -p scada-sim-driver
SCADA_TEST_SCHEDULER=multi_thread:2 cargo test --release -p scada-runtime
echo "== test (release, VS-001/VS-002/VS-003 runtime suite under the VS-003 ingress policies, ADR 0010)"
SCADA_TEST_SCHEDULER=multi_thread:2 SCADA_TEST_INGRESS=source_fair:4 cargo test --release -p scada-runtime
SCADA_TEST_INGRESS=source_fair:1 cargo test --release -p scada-runtime
SCADA_TEST_INGRESS=source_capped:2 cargo test --release -p scada-runtime
echo "== unsafe audit (only the benchmark allocator may contain unsafe, ADR 0003)"
hits=$(grep -rln --include=*.rs -E '\bunsafe\b *(\{|fn|impl)' crates apps | grep -v '^apps/scada-bench/src/alloc.rs$' || true)
if [ -n "$hits" ]; then echo "unexpected unsafe in: $hits" >&2; exit 1; fi
echo "ok"
echo "== core dependency audit (no tokio/prost/toml/serde in scada-core)"
if cargo tree -p scada-core -e normal --prefix none | grep -E '^(tokio|prost|bytes|toml|serde|mio|socket2|libc) '; then
  echo "forbidden dependency in scada-core" >&2; exit 1
fi
echo "ok"
echo "== core purity audit (no clocks, sockets, threads, randomness in scada-core/src)"
if grep -rnE 'SystemTime|Instant::|std::net|std::thread|std::sync::(Mutex|RwLock|mpsc|Condvar)|tokio|rand::|std::fs|std::io::(Read|Write)|\bunsafe\b *(\{|fn|impl)' crates/scada-core/src; then
  echo "impure construct in scada-core" >&2; exit 1
fi
echo "ok"
if command -v cargo-deny >/dev/null; then echo "== cargo deny"; cargo deny check; fi
if command -v buf >/dev/null; then echo "== proto"; ./scripts/check-proto.sh; else echo "== proto: buf not installed, skipped"; fi
if rustup target list --installed | grep -q aarch64-unknown-linux-gnu; then
  # Criterion (bench dev-dependency) pulls a C build script that needs an aarch64 C
  # compiler; the benchmark harness is checked without its dev targets.
  echo "== aarch64 compile check (not a runtime proof: NOT TESTED ON HARDWARE)"
  cargo check --workspace --all-targets --exclude scada-bench --target aarch64-unknown-linux-gnu
  cargo check -p scada-bench --bins --target aarch64-unknown-linux-gnu
fi
echo "ALL CHECKS PASSED"
