#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Regenerate the Python gRPC stubs in src/nemo_gym_substrate/_gen from
# ate-env's EnvironmentService proto (proto/ateenv/v1/env.proto in an
# agent-substrate/env checkout). Environment lifecycle moved from HTTP REST
# to this gRPC service (env#18); the guest data plane (shell/file) is still
# proxied over HTTP.
#
# Requires grpcio-tools; set PYTHON to a python that has it, or install uv
# (the default path below runs it via `uv run`).
#
# Env:
#   ATE_ENV_REPO   path to an agent-substrate/env checkout (default: ~/dev/substrate-env)
#   PYTHON         python with grpcio-tools (default: uv run --with grpcio-tools==1.83.0)
set -o errexit -o nounset -o pipefail

DEMO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_REPO="${ATE_ENV_REPO:-${HOME}/dev/substrate-env}"
OUT_DIR="${DEMO_DIR}/src/nemo_gym_substrate/_gen"
PROTO_DIR="${ENV_REPO}/proto/ateenv/v1"

[[ -f "${PROTO_DIR}/env.proto" ]] || {
  echo "env.proto not found at ${PROTO_DIR} (set \$ATE_ENV_REPO)" >&2; exit 1; }

run_protoc() {
  if [[ -n "${PYTHON:-}" ]]; then
    "${PYTHON}" -m grpc_tools.protoc "$@"
  else
    uv run --with 'grpcio-tools==1.83.0' python -m grpc_tools.protoc "$@"
  fi
}

mkdir -p "${OUT_DIR}"
run_protoc \
  -I "${PROTO_DIR}" \
  --python_out="${OUT_DIR}" \
  --pyi_out="${OUT_DIR}" \
  --grpc_python_out="${OUT_DIR}" \
  env.proto

# protoc emits `import env_pb2 as env__pb2`, which doesn't resolve inside
# the _gen package; rewrite to a relative import. `sed -i` with a suffix is
# the only form portable across GNU and BSD/macOS sed.
sed -i.bak 's/^import env_pb2 as env__pb2/from . import env_pb2 as env__pb2/' \
  "${OUT_DIR}/env_pb2_grpc.py"
rm -f "${OUT_DIR}/env_pb2_grpc.py.bak"

echo "Regenerated ${OUT_DIR} from ${PROTO_DIR}/env.proto"
