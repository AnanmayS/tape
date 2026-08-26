#!/usr/bin/env bash
# A guided tour of Tape against the live Coinbase feed.
# Captures real market data twice, so the second session opens with a
# discontinuity, then proves replay is deterministic over the result.
#
#   ./demo.sh          full tour, about 70 seconds
#   ./demo.sh 10       shorter capture sessions (seconds each)

set -euo pipefail
cd "$(dirname "$0")"

SECS="${1:-20}"
DIR="demo-data"
W="$DIR/v1/symbol=BTC-USD"

say() { printf '\n\033[1m== %s\033[0m\n\n' "$*"; }

say "Building"
go build -o tape ./cmd/tape
rm -rf "$DIR"

say "Capturing ${SECS}s of live BTC-USD  (session 1 of 2)"
./tape capture -dir "$DIR" -duration "${SECS}s" -live

say "Capturing again  (session 2 — reconnecting leaves a discontinuity)"
./tape capture -dir "$DIR" -duration "${SECS}s" -live

say "What landed on disk"
./tape verify "$W" || true

say "Determinism: replaying the same window twice"
A=$(./tape replay -continue-on-gap "$W" | sha256sum | cut -d' ' -f1)
B=$(./tape replay -continue-on-gap "$W" | sha256sum | cut -d' ' -f1)
printf '  replay 1  %s\n  replay 2  %s\n\n' "$A" "$B"
if [ "$A" = "$B" ]; then
  printf '  \033[32mPASS\033[0m — byte-identical across runs\n'
else
  printf '  \033[31mFAIL\033[0m — the two replays differ\n'; exit 1
fi

say "What the window costs to store"
./tape stat "$W" 2>&1 | head -12

say "The same events, rendered to be read"
./tape replay -pretty -continue-on-gap "$W" 2>/dev/null | head -14 || true

say "Done — captured data is in $DIR/ (delete it whenever)"
