#!/usr/bin/env bash
# Run the substrate concurrency harness as an in-cluster Job and save its output.
#   benchmarking/rl-image/concurrency/run.sh MODE N [LABEL]   MODE = golden | golden-one | cold | pause | suspend
# Env: CPU (2), MEM (2Gi), GUEST_IMAGE (required), CTX, JOB_TIMEOUT_S (3600), SKIP_POOL=1 to leave the WorkerPool alone.
#      SANDBOX_CLASS (SANDBOX_CLASS_GVISOR | SANDBOX_CLASS_MICROVM), SANDBOX_CONFIG (gvisor-default | microvm),
#      POOL_LABEL (workload=rl-bench-ateom | workload=rl-bench-microvm), TEMPLATE_PREFIX (swe | mvm).
set -euo pipefail
cd "$(dirname "$0")"
MODE=$1; N=$2; LABEL=${3:-$MODE-$N}; CPU=${CPU:-2}; MEM=${MEM:-2Gi}
SANDBOX_CLASS=${SANDBOX_CLASS:-SANDBOX_CLASS_GVISOR}; SANDBOX_CONFIG=${SANDBOX_CONFIG:-gvisor-default}
POOL_LABEL=${POOL_LABEL:-workload=rl-bench-ateom}; TEMPLATE_PREFIX=${TEMPLATE_PREFIX:-swe}
: "${GUEST_IMAGE:?set GUEST_IMAGE to the digest-pinned ate-env-guest image}"
CTX=${CTX:-gke_gke-ai-eco-dev_us-central1_glottman-sandbox-test-1}; K="kubectl --context $CTX"
[ "${SKIP_POOL:-0}" = 1 ] || $K apply -f workerpool.yaml >/dev/null
$K -n ate-system create configmap rl-bench-harness --from-file=substrate_concurrency.py --from-file=images.tsv --dry-run=client -o yaml | $K apply -f - >/dev/null
$K -n ate-system create configmap rl-bench-proto --from-file=proto/ateapi.proto --from-file=proto/guest.proto --dry-run=client -o yaml | $K apply -f - >/dev/null
$K get clustertrustbundle servicedns.podcert.ate.dev:identity:primary-bundle -o jsonpath='{.spec.trustBundle}' > /tmp/rl-bench-ca.pem
$K -n ate-system create configmap rl-bench-ca --from-file=ca.pem=/tmp/rl-bench-ca.pem --dry-run=client -o yaml | $K apply -f - >/dev/null
job="rl-bench-$TEMPLATE_PREFIX-$MODE-$N"
$K -n ate-system delete job "$job" --ignore-not-found --wait=true >/dev/null
sed "s/__MODE__/$MODE/g; s/__N__/$N/g; s/__LABEL__/$LABEL/g; s/__CPU__/$CPU/g; s|__MEM__|$MEM|g; s|__GUEST_IMAGE__|$GUEST_IMAGE|g; s|__POOL_LABEL__|$POOL_LABEL|g; s/__SANDBOX_CLASS__/$SANDBOX_CLASS/g; s/__SANDBOX_CONFIG__/$SANDBOX_CONFIG/g; s/__TEMPLATE_PREFIX__/$TEMPLATE_PREFIX/g" job.yaml.tmpl | $K apply -f - >/dev/null
t0=$(date +%s)
while :; do
  st=$($K -n ate-system get job "$job" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
  case "$st" in
    1/*) echo "$job: done in $(( $(date +%s) - t0 )) s"; break;;
    */1) echo "$job: FAILED"; $K -n ate-system logs "job/$job" | tail -30; exit 1;;
  esac
  if (( $(date +%s) - t0 > ${JOB_TIMEOUT_S:-3600} )); then echo "$job: timed out"; exit 1; fi
  sleep 5
done
mkdir -p results
$K -n ate-system logs "job/$job" | grep '^{' > "results/$LABEL.jsonl"
$K -n ate-system delete job "$job" --ignore-not-found >/dev/null
echo "results: results/$LABEL.jsonl"
