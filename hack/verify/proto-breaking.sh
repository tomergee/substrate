#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Fails if pkg/proto/ateapipb/ateapi.proto contains wire/JSON-breaking
# changes relative to the branch point (merge-base with origin/main).
# External SDKs generate their own stubs from a vendored copy of this proto,
# so method paths, field numbers, and JSON names are a published contract.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

BASE_REMOTE="${BASE_REMOTE:-origin}"
if ! git rev-parse --verify --quiet "${BASE_REMOTE}/main" >/dev/null; then
  echo "proto-breaking: ${BASE_REMOTE}/main not available; skipping"
  exit 0
fi
AGAINST_REF="$(git merge-base HEAD "${BASE_REMOTE}/main")"

if git diff --quiet "${AGAINST_REF}" -- pkg/proto/ateapipb/ateapi.proto; then
  echo "proto-breaking: ateapi.proto unchanged since ${AGAINST_REF}; OK"
  exit 0
fi

bash hack/run-tool.sh buf breaking pkg/proto/ateapipb \
  --against ".git#ref=${AGAINST_REF},subdir=pkg/proto/ateapipb"
echo "proto-breaking: no wire/JSON-breaking changes; OK"
