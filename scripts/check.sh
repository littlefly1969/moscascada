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
cargo build --release -p scada-sim-driver -p scada-journal
SCADA_TEST_SCHEDULER=multi_thread:2 cargo test --release -p scada-runtime
echo "== test (release, VS-001/VS-002/VS-003 runtime suite under the VS-003 ingress policies, ADR 0010)"
SCADA_TEST_SCHEDULER=multi_thread:2 SCADA_TEST_INGRESS=source_fair:4 cargo test --release -p scada-runtime
SCADA_TEST_INGRESS=source_fair:1 cargo test --release -p scada-runtime
echo "== test (release, VS-001..VS-003 runtime suite with VS-004 supervision enabled on source 1001, ADR 0011)"
# Generous timeouts (60 s): enabling supervision must not change any earlier outcome.
SCADA_TEST_SCHEDULER=multi_thread:2 SCADA_TEST_SUPERVISION=60000:60000 cargo test --release -p scada-runtime
echo "== test (release, VS-001..VS-004 runtime suite with VS-005 alarms enabled on every tag, ADR 0012)"
# Default-off is covered by every run above (no [alarms] section). Here the same suite runs
# with rules on every tag and a large outbox: alarms must not change any earlier outcome.
SCADA_TEST_ALARMS=1 cargo test --release -p scada-runtime
SCADA_TEST_ALARMS=1 SCADA_TEST_SCHEDULER=multi_thread:2 SCADA_TEST_INGRESS=source_fair:4 cargo test --release -p scada-runtime
SCADA_TEST_ALARMS=1 SCADA_TEST_INGRESS=source_fair:1 SCADA_TEST_SUPERVISION=60000:60000 cargo test --release -p scada-runtime
SCADA_TEST_ALARMS=1 SCADA_TEST_SCHEDULER=multi_thread:2 SCADA_TEST_SUPERVISION=60000:60000 cargo test --release -p scada-runtime
echo "== test (release, VS-001..VS-005 runtime suite with the VS-006 durable journal enabled, ADR 0013)"
# Default-off (J10) is covered by every run above (no [persistence] section). Here the same
# suite runs with [persistence] (synchronous=FULL) and the historian: the journal must not
# change any earlier outcome (tests whose subject is volatility pin persistence off).
SCADA_TEST_PERSISTENCE=1 cargo test --release -p scada-runtime
SCADA_TEST_PERSISTENCE=1 SCADA_TEST_ALARMS=1 SCADA_TEST_SCHEDULER=multi_thread:2 SCADA_TEST_INGRESS=source_fair:4 cargo test --release -p scada-runtime
SCADA_TEST_PERSISTENCE=1 SCADA_TEST_ALARMS=1 SCADA_TEST_SUPERVISION=60000:60000 cargo test --release -p scada-runtime
echo "== alarm capacity model (VS-005 T15)"
python3 scripts/test_alarm_capacity.py
echo "== alarm catalog example (VS-005, docs/alarm-catalog.md)"
python3 scripts/check_alarm_catalog.py docs/catalog/alarm-catalog.example.json config/examples/alarms.toml
if sed 's/"alarm_id": 5,/"alarm_id": 55,/' docs/catalog/alarm-catalog.example.json > target/bad-catalog.json \
   && python3 scripts/check_alarm_catalog.py target/bad-catalog.json config/examples/alarms.toml >/dev/null; then
  echo "catalog checker accepted an unknown alarm_id" >&2; exit 1
fi
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
