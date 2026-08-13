# NeMo Gym sandbox provider for Agent Substrate

Runs [NeMo Gym](https://github.com/NVIDIA-NeMo/Gym) rollout sandboxes as **Substrate actors**,
fronted by the [`ate-env`](https://github.com/agent-substrate/env) API. Registers as provider
`substrate` next to the built-ins (Docker, Daytona, ECS Fargate, Enroot, OpenShell, OpenSandbox,
Apptainer) via the `nemo_gym.sandbox_providers` entry point — no changes to Gym required.

Why run rollouts on Substrate: sandboxes that idle between agent turns are **suspended and hold
no worker** (the atenet router resumes them transparently on the next exec), repeated sandboxes
of the same image start from **golden snapshots** instead of booting, and prepared states can be
**forked** for branching rollouts.

## Setup

Prerequisites: a cluster running Agent Substrate with the `ate-env` system deployed
(`ate-env deploy … | kubectl apply -f -`), and the ActorTemplates your tasks use pre-provisioned
(templates are the substrate analog of "the image is available").

```bash
pip install ./demos/nemo-gym-provider          # brings httpx; nemo-gym extra: .[gym]
kubectl port-forward -n ate-env svc/ate-env-api 7777:7777   # or run in-cluster
```

## Configuration

Named block under `sandbox:` — the provider key is `substrate`:

```yaml
sandbox:
  default_metadata:
    sandbox-api: substrate
  substrate:
    connection:
      api_url: http://127.0.0.1:7777      # ate-env-api endpoint
      request_timeout_s: 30
    create:
      template: default-env               # ActorTemplate when the spec names none
      namespace: ate-env                  # namespace the templates live in
      ready_timeout_s: 120
      ready_poll_interval_s: 1.0
      image_templates:                    # optional SandboxSpec.image → template map
        python:3.12-slim: gym-py312
```

## `provider_options` (per sandbox, on `SandboxSpec`)

| Option | Meaning |
|---|---|
| `template` | ActorTemplate for this sandbox (overrides `create.template` and `image_templates`) |
| `namespace` | Namespace of that template |

Unknown keys are rejected at `create()` time.

## Resource mapping and isolation

- One Gym sandbox = one Substrate **actor**: a gVisor sandbox multiplexed onto a warm WorkerPool
  worker. Isolation is gVisor (syscall interception, private netns); actors share worker nodes.
- `SandboxSpec.image` is resolved to a pre-provisioned ActorTemplate via `image_templates` —
  there is no dynamic image pull per create (that is what makes creates fast).
- `spec.files` are seeded through the guest fs API after readiness; `spec.env` / `spec.workdir`
  are applied per exec (the guest supports `env`/`cwd` natively).
- `exec(timeout_s=…)` is enforced both guest-side (`timeout(1)` wrapper) and client-side.
- `spec.ttl_s` is **not yet enforced** (substrate has no server-side TTL); clean up via `close()`.
- `exec(user=…)` is not supported: commands run as the actor's configured user.
- `status()` probes with a trivial exec through the router; a **suspended (parked) actor reports
  RUNNING** — the router auto-resumes it on the next command, which is what a rollout loop wants.

## First run

```python
import asyncio
from nemo_gym_substrate import SubstrateSandboxProvider
from nemo_gym.sandbox.providers.base import SandboxSpec

async def main():
    provider = SubstrateSandboxProvider({"connection": {"api_url": "http://127.0.0.1:7777"}})
    handle = await provider.create(SandboxSpec(files={"/task/hello.py": "print('hi')"}))
    result = await provider.exec(handle, "python3 /task/hello.py")
    print(result.stdout, result.return_code)   # hi 0
    await provider.close(handle)
    await provider.aclose()

asyncio.run(main())
```

Or select it from Gym config: set the `sandbox: substrate:` block and run your resource server
as usual — the entry point makes the provider discoverable.

## Operational notes

- **Auth**: the provider speaks to `ate-env-api`, which holds the substrate credential
  (projected SA token). Reach it in-cluster or by port-forward; there is no public gateway yet.
- **Creates are as fast as the template's start path**: an image's first sandbox pays the boot,
  later ones restore from its golden snapshot. Density comes from substrate parking idle actors.
- **Cleanup**: `close()` deletes the actor and is 404-safe. A crashed harness currently leaks
  actors until deleted manually (server-side TTL is on the substrate roadmap).
- Tests: `python -m pytest tests/` — contract tests against an in-memory fake of the ate-env
  API; no cluster or nemo-gym install needed (base types fall back to structural mirrors in
  `_compat.py`).
