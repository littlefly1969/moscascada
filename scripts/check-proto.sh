#!/usr/bin/env bash
# Schema checks: buf lint + buf breaking against the committed v1.0 baseline image.
# Also proves that the breaking check is wired and effective by applying a deliberately
# incompatible change to a temporary copy and expecting buf to reject it.
set -euo pipefail
cd "$(dirname "$0")/.."
BUF=${BUF:-buf}
"$BUF" --version
"$BUF" lint
"$BUF" breaking --against proto/baseline/scada-v1.0.binpb
echo "buf lint: ok; buf breaking vs baseline v1.0: ok"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp -r buf.yaml proto "$tmp/"
# Renumber a field (wire-incompatible): must be detected.
sed -i 's/uint64 source_sequence = 2;/uint64 source_sequence = 9;/' "$tmp/proto/scada/driver/v1/driver.proto"
out=$(cd "$tmp" && "$BUF" breaking --against proto/baseline/scada-v1.0.binpb 2>&1 || true)
if ! grep -q 'field "2" with name "source_sequence"' <<<"$out"; then
  echo "ERROR: buf breaking did not detect a renumbered field: $out" >&2
  exit 1
fi
echo "buf breaking detects a renumbered field: ok"
