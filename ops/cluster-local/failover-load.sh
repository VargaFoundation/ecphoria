#!/usr/bin/env bash
# What losing a node costs, under load.
#
# `failover-test.sh` proves a run *survives* a leader kill. This measures what clients saw while it
# happened: it brings up a 3-node cluster, drives a steady read/write load at it, kills the leader
# partway through, and prints a per-second timeline around the event.
#
#   ops/cluster-local/failover-load.sh              # 60s run, leader killed at t+20s
#   SECONDS=90 KILL_AFTER=30 ops/cluster-local/failover-load.sh
#   KILL_AFTER=0 ops/cluster-local/failover-load.sh  # baseline, nothing killed
#
# Leaves the cluster running if it was already up when the script started, so an interactive
# session can keep poking at it.
set -euo pipefail

cd "$(dirname "$0")/../.."

RUN_DIR="${RUN_DIR:-/tmp/ecphoria-cluster}"
HTTP_BASE="${HTTP_BASE:-18001}"
NODES="${NODES:-3}"
export RUN_DIR HTTP_BASE NODES

BIN="${ECPHORIA_BIN:-./target/release/ecphoria-server}"
if [[ ! -x "$BIN" ]]; then
  echo "== building $BIN =="
  cargo build --release --bin ecphoria-server
fi

started_here=0
if ! curl -fsS --max-time 2 "http://127.0.0.1:${HTTP_BASE}/cluster/status" >/dev/null 2>&1; then
  echo "== starting a ${NODES}-node cluster =="
  bash ops/cluster-local/run-cluster.sh
  started_here=1
  # Give the election a moment; the bench refuses to start without a leader anyway.
  sleep 5
fi

urls=""
for i in $(seq 1 "$NODES"); do
  port=$((HTTP_BASE + i - 1))
  urls="${urls:+$urls,}http://127.0.0.1:${port}"
done

echo "== driving load =="
NODES="$urls" \
SECONDS="${SECONDS:-60}" \
KILL_AFTER="${KILL_AFTER:-20}" \
READS_PER_SEC="${READS_PER_SEC:-50}" \
WRITES_PER_SEC="${WRITES_PER_SEC:-5}" \
RUN_DIR="$RUN_DIR" \
  cargo run --release -p ecphoria-core --example failover_bench

if [[ "$started_here" == "1" ]]; then
  echo
  echo "== stopping the cluster =="
  # The killed node's pid file is stale; stop-cluster tolerates that.
  bash ops/cluster-local/stop-cluster.sh || true
else
  echo
  echo "cluster was already running — left up (ops/cluster-local/stop-cluster.sh to stop it)"
fi
