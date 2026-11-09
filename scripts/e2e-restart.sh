#!/usr/bin/env bash
# Gate 1 "restart output is identical", end to end through Redpanda.
#
# Runs steady traffic while killing matchers (SIGKILL), killing one again
# during its replay, and stopping one cleanly (SIGTERM). Then `e2e verify`
# rebuilds the expected events from orders.commands and checks
# matching.events matches exactly, with repeats byte-identical.
#
# Usage: scripts/e2e-restart.sh [commands] [rate_per_sec]
set -euo pipefail
cd "$(dirname "$0")/.."

COUNT="${1:-60000}"
RATE="${2:-2000}"
# Small interval so restarts recover from snapshots, not from offset 0.
export SNAPSHOT_INTERVAL="${SNAPSHOT_INTERVAL:-2000}"
MATCHERS=(matching-0 matching-1 matching-2 matching-3)

step() { printf '\n== %s\n' "$*"; }

step "fresh stack (volumes removed)"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build redpanda topics "${MATCHERS[@]}"

step "producing $COUNT commands at $RATE/s"
docker compose run --rm e2e e2e produce "$COUNT" "$RATE" 42 &
PRODUCER=$!
trap 'kill $PRODUCER 2>/dev/null || true' EXIT

sleep 6
step "SIGKILL matching-1, restart after 2 s"
docker compose kill -s SIGKILL matching-1
sleep 2
docker compose start matching-1

sleep 5
step "SIGKILL matching-2 and matching-3 together"
docker compose kill -s SIGKILL matching-2 matching-3
sleep 1
docker compose start matching-2 matching-3

sleep 4
step "SIGKILL matching-1 again, then kill it 1 s into its replay"
docker compose kill -s SIGKILL matching-1
sleep 3
docker compose start matching-1
sleep 1
docker compose kill -s SIGKILL matching-1
sleep 1
docker compose start matching-1

sleep 4
step "clean stop of matching-0 (SIGTERM), restart"
docker compose stop matching-0
docker compose start matching-0

wait $PRODUCER
trap - EXIT

step "verifying matching.events against a fresh engine run"
docker compose run --rm e2e e2e verify 180

step "recovery times (restart to ready)"
for m in "${MATCHERS[@]}"; do
  docker compose logs --no-log-prefix "$m" 2>/dev/null \
    | grep -o '"recovery_ms":[0-9]*' | sed "s/^/$m /" || true
done
