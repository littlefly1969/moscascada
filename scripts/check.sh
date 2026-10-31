#!/usr/bin/env bash
# Full stable gate: format, lints, tests, unsafe audit, schema checks, ARM64 compile check.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "== toolchain"; rustc --version; cargo --version
echo "== fmt";    cargo fmt --all --check
echo "== clippy"; cargo clippy --workspace --all-targets -- -D warnings
echo "== test";   cargo test --workspace
echo "== unsafe audit (only the benchmark allocator may contain unsafe, ADR 0003)"
hits=$(grep -rln --include=*.rs -E '\bunsafe\b *(\{|fn|impl)' crates apps | grep -v '^apps/scada-bench/src/alloc.rs$' || true)
if [ -n "$hits" ]; then echo "unexpected unsafe in: $hits" >&2; exit 1; fi
echo "ok"
echo "== core dependency audit (no tokio/prost/toml/serde in scada-core)"
if cargo tree -p scada-core -e normal --prefix none | grep -E '^(tokio|prost|toml|serde|mio) '; then
  echo "forbidden dependency in scada-core" >&2; exit 1
fi
echo "ok"
echo "== core purity audit (no clocks, sockets, threads, randomness in scada-core/src)"
if grep -rnE 'SystemTime|Instant::|std::net|std::thread|tokio|rand::|std::fs|std::io::(Read|Write)' crates/scada-core/src; then
  echo "impure construct in scada-core" >&2; exit 1
fi
echo "ok"
if command -v buf >/dev/null; then echo "== proto"; ./scripts/check-proto.sh; else echo "== proto: buf not installed, skipped"; fi
if rustup target list --installed | grep -q aarch64-unknown-linux-gnu; then
  echo "== aarch64 compile check"; cargo check --workspace --all-targets --target aarch64-unknown-linux-gnu
fi
echo "ALL CHECKS PASSED"
