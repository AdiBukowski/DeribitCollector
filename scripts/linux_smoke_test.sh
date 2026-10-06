#!/usr/bin/env bash
# Builds on Linux, runs unit tests, then a short live run ending with SIGTERM (as systemd does).
set -u
SRC="$(dirname "$(readlink -f "$0")")/.."
BUILD=/tmp/dc-build
OUT=/tmp/dc-out
cmake -S "$SRC" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release -DDERIBIT_COLLECTOR_TESTS=ON >/dev/null
cmake --build "$BUILD" -j"$(nproc)" 2>&1 | grep -E "DeribitCollector/(src|tests).*(error|warning)|Built target"
"$BUILD/collector_tests" --gtest_brief=1 | tail -1
rm -rf "$OUT"; mkdir -p "$OUT"
timeout -s TERM "${1:-45}" "$BUILD/deribit_collector" --out "$OUT" --flush-seconds 15 --stats-path "$OUT/health.json" 2>&1 | tail -2
echo "part files: $(find "$OUT" -name '*.part' | wc -l)  final files: $(find "$OUT" -name '*.zst' | wc -l)"
grep -E '"(instruments|books_valid|gaps|parse_errors|rpc_errors|write_errors)"' "$OUT/health.json"
