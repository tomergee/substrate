# NeMo Gym sandbox provider for Agent Substrate

Runs [NeMo Gym](https://github.com/NVIDIA-NeMo/Gym) rollout sandboxes as **Substrate actors**,
fronted by the [`ate-env`](https://github.com/agent-substrate/env) API. Registers as provider
`substrate` next to the built-ins (Docker, Daytona, ECS Fargate, Enroot, OpenShell, OpenSandbox,
Apptainer) via the `nemo_gym.sandbox_providers` entry point — **no changes to NeMo Gym required**,
and no changes to your resource servers beyond the `sandbox:` config block.

Why run rollouts on Substrate:

- **Idle sandboxes hold no worker.** A sandbox that is waiting on model inference is suspended by
  Substrate; the atenet router resumes it transparently on the next command. A rollout fleet is
  mostly waiting, so effective density is bounded by the *active* set, not the fleet size.
- **Repeat sandboxes start from golden snapshots.** The first sandbox on an image pays the boot;
  later ones restore a memory snapshot instead of booting.
- **Prepared states fork.** A sandbox that finished expensive task setup can be snapshotted and
  branched into N independent rollouts (best-of-N from a mid-episode state).

Verified against **nemo-gym 0.5.0**: the registry resolves this provider through the entry point
(`create_provider({"substrate": {...}})`) and the full unit suite passes against the real
`nemo_gym.sandbox.providers.base` types.

## Prerequisites

On the cluster:

1. **Agent Substrate installed** (`ate-system` namespace healthy — api-server, controller,
   atelet, atenet-router, valkey). See `hack/install-ate.sh` at the repo root.
2. **The `ate-env` system deployed** — namespace, WorkerPool, ActorTemplate(s), and the
   `ate-env-api` service:

   ```bash
   ate-env deploy \
     --guest-image  <ate-env-guest image> \
     --api-image    <ate-env-api image> \
     --ateom-image  <ateom-gvisor image matching your substrate build> \
     --snapshots-bucket gs://<bucket>/ate-env/ | kubectl apply -f -
   kubectl get pods -n ate-env   # api + warm workers Running
   ```

   If your `ateapi` requires authentication (it does on any standard install), the
   `ate-env-api` Deployment needs a projected ServiceAccount token with audience
   `api.ate-system.svc` and the `-ateapi-token-file` flag pointing at it.
3. **An ActorTemplate per task image**, pre-provisioned in the `ate-env` namespace (the substrate
   analog of "the image is available"). The `ate-env deploy` default is `default-env`; add one
   template per additional image and map them under `create.image_templates` below.
4. **Network path from wherever Gym's rollout workers run** to `ate-env-api`: in-cluster DNS
   (`http://ate-env-api.ate-env:7777`) or, for a workstation, a port-forward:

   ```bash
   kubectl port-forward -n ate-env svc/ate-env-api 7777:7777
   ```

Locally:

5. **Python ≥ 3.10** and pip. `nemo-gym` itself is only needed on the machine running the Gym
   resource servers — this package's tests run without it (see Testing).

## Installation

```bash
pip install ./demos/nemo-gym-provider        # installs nemo-gym-substrate + httpx
# with NeMo Gym in the same environment:
pip install './demos/nemo-gym-provider[gym]'
```

The entry point makes the provider discoverable immediately:

```python
from nemo_gym.sandbox.providers.registry import create_provider
provider = create_provider({"substrate": {"connection": {"api_url": "http://127.0.0.1:7777"}}})
```

## How to use

### From NeMo Gym config (the normal path)

Add the `sandbox:` block to your resource-server config and select the `substrate` provider —
nothing else in the Gym setup changes:

```yaml
sandbox:
  default_metadata:
    sandbox-api: substrate
  substrate:
    connection:
      api_url: http://ate-env-api.ate-env:7777   # in-cluster; port-forward for dev
      request_timeout_s: 30
    create:
      template: default-env          # ActorTemplate when the spec names none
      namespace: ate-env             # namespace the templates live in
      ready_timeout_s: 120
      ready_poll_interval_s: 1.0
      image_templates:               # optional SandboxSpec.image → template map
        python:3.12-slim: gym-py312
```

Then run your resource server / `gym env start` as usual. Sandboxes created by Gym's resource
lifecycle now appear as Substrate actors; `kubectl get pods -n ate-env` shows the warm workers
hosting them.

### Directly from Python (debugging, scripts)

```python
import asyncio
from nemo_gym_substrate import SubstrateSandboxProvider
from nemo_gym.sandbox.providers.base import SandboxSpec   # or nemo_gym_substrate._compat

async def main():
    provider = SubstrateSandboxProvider({"connection": {"api_url": "http://127.0.0.1:7777"}})
    handle = await provider.create(SandboxSpec(
        files={"/task/hello.sh": "echo hello from substrate"},
        workdir="/task",
        env={"EPISODE": "1"},
    ))
    result = await provider.exec(handle, "sh /task/hello.sh && echo episode=$EPISODE")
    print(result.stdout, result.return_code)
    await provider.close(handle)
    await provider.aclose()

asyncio.run(main())
```

### Per-sandbox `provider_options` (on `SandboxSpec`)

| Option | Meaning |
|---|---|
| `template` | ActorTemplate for this sandbox (overrides `create.template` and `image_templates`) |
| `namespace` | Namespace of that template |

Unknown keys are rejected at `create()` time, as are unknown keys anywhere in the config block.

## Testing

```bash
pip install './demos/nemo-gym-provider[test]'

# Unit/contract tests — hermetic, no cluster, no nemo-gym needed (base types
# fall back to structural mirrors in _compat.py; with nemo-gym installed the
# same tests run against the real types):
python -m pytest tests/test_provider.py -q

# End-to-end against a live cluster (full lifecycle on a real actor):
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777 &
SUBSTRATE_E2E_API_URL=http://127.0.0.1:7777 python -m pytest tests/test_e2e.py -q
```

## Benchmark

`benchmarks/rollout_bench.py` shapes load like a NeMo-RL rollout batch: N parallel rollouts,
each *create → seed task file → T agent turns (exec + mocked model think time) → close*, on real
actors:

```bash
python benchmarks/rollout_bench.py --rollouts 5 --turns 3 --think-s 2
```

First numbers on a small test cluster (5 warm workers, gVisor, cold creates):

| Phase | p50 | max | Note |
|---|---|---|---|
| create | 4.95 s | 18.1 s | cold resume; max shows 5-way contention for 5 workers |
| exec (turn) | 0.14 s | 0.15 s | router + guest round trip |
| close | 3.06 s | 4.0 s | delete path |

Turn overhead is already negligible against model think time; create/close dominate — which is
exactly what golden-snapshot starts and fork-from-golden creates are for.

## Resource mapping and isolation

- One Gym sandbox = one Substrate **actor**: a gVisor sandbox multiplexed onto a warm WorkerPool
  worker. Isolation is gVisor (syscall interception, private network namespace); actors share
  worker nodes.
- `SandboxSpec.image` resolves to a pre-provisioned ActorTemplate via `image_templates` — there
  is no dynamic image pull per create; that is what makes creates fast and repeatable.
- `spec.files` are seeded through the guest fs API after readiness. `spec.env` / `spec.workdir`
  are applied per exec (the guest supports `env`/`cwd` natively).
- `exec(timeout_s=…)` is enforced both guest-side (`timeout(1)` wrapper) and client-side; a
  client-side deadline returns a `return_code=-1` sentinel result rather than raising.
- `status()` probes with a trivial exec through the router. A **suspended (parked) actor reports
  RUNNING** — the router auto-resumes it on the next command, which is the behavior a rollout
  loop wants.

Known limits (tracked in the RL-on-Substrate proposal):

- `spec.ttl_s` is **not enforced** — substrate has no server-side TTL yet; clean up via
  `close()`. A crashed harness leaks actors until deleted.
- `exec(user=…)` raises: commands run as the actor's configured user.
- `spec.ports` / `SandboxEndpoint` exposure is not implemented in this demo.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `SandboxCreateError: no ActorTemplate mapped for image …` | The spec named an image with no template. Pre-provision a template and add it to `create.image_templates`, or set `provider_options.template` |
| Create times out (`SandboxCreateVerificationError`) | Check warm workers exist (`kubectl get pods -n ate-env`) and the template is Ready (`kubectl get actortemplate -n ate-env`). More parallel creates than warm workers will queue |
| Every exec returns 503 with `CERTIFICATE_EXPIRED` | The atenet router's pod certificate expired and wasn't hot-reloaded (seen on routers running > ~1 day): `kubectl -n ate-system rollout restart deploy/atenet-router` |
| `ConnectError` from the provider | `api_url` unreachable — port-forward died, or wrong kubectl context (`kubectl config current-context`) |
| Actor stuck `RESUMING` forever | Restore landed on a CPU-incompatible node (mixed Intel/AMD pools) or raced a router restart. Delete the env and recreate; long-term fix is CPU-aware placement |
