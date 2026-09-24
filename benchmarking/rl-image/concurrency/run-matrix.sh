#!/usr/bin/env bash
# The full 1/10/100 matrix, in an order that separates cache-cold from cache-warm restores:
# one-image template starts twice at 100 (second = cache-warm on every node), then 100 distinct
# templates (bakes 100 goldens first), pause and suspend on those, and cold boots last.
set -u
cd "$(dirname "$0")"
export GUEST_IMAGE=${GUEST_IMAGE:?} SKIP_POOL=1 JOB_TIMEOUT_S=3600
PFX=${LABEL_PREFIX:-}   # e.g. LABEL_PREFIX=mvm- with the microvm env set (see run.sh)
run() { echo "=== $(date +%T) run $*"; ./run.sh "$1" "$2" "$PFX$3" 2>&1 | tail -2 || true; }
run golden-one 10  golden-one-10
run golden-one 100 golden-one-100
run golden-one 100 golden-one-100-b
run golden-one 1   golden-one-1-b
run golden 100 golden-100
run golden 100 golden-100-b
run golden 10  golden-10
run golden 1   golden-1
run pause 1   pause-1
run pause 10  pause-10
run pause 100 pause-100
run suspend 1   suspend-1
run suspend 10  suspend-10
run suspend 100 suspend-100
run cold 1   cold-1
run cold 10  cold-10
run cold 100 cold-100
echo "=== $(date +%T) matrix done"
python3 summarize.py results/*.jsonl
