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

"""NeMo Gym sandbox provider backed by Agent Substrate.

Each sandbox is a Substrate actor fronted by the ``ate-env`` API
(https://github.com/agent-substrate/env): creation goes through
``POST /v1/envs``, command execution and file transfer through the
``ate-env-guest`` daemon that runs inside every actor. Idle sandboxes can be
suspended by Substrate and are resumed transparently on the next request by
the atenet router, so a fleet of mostly-waiting rollout sandboxes holds no
workers.

Contract: https://docs.nvidia.com/nemo/gym/main/infrastructure/sandbox/adding-a-provider/
"""

from __future__ import annotations

import asyncio
import base64
import logging
import shlex
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx

from ._compat import (
    SandboxCreateError,
    SandboxCreateVerificationError,
    SandboxExecResult,
    SandboxHandle,
    SandboxSpec,
    SandboxStatus,
)

logger = logging.getLogger(__name__)

_PROVIDER_NAME = "substrate"


def _require_keys(options: Mapping[str, Any], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(options) - allowed)
    if unknown:
        raise ValueError(f"{where}: unknown option(s) {unknown}; allowed: {sorted(allowed)}")


@dataclass(frozen=True)
class ConnectionConfig:
    """`connection:` block of the provider config."""

    api_url: str = "http://127.0.0.1:7777"
    request_timeout_s: float = 30.0

    _ALLOWED = frozenset({"api_url", "request_timeout_s"})

    @classmethod
    def from_mapping(cls, options: Mapping[str, Any]) -> "ConnectionConfig":
        _require_keys(options, cls._ALLOWED, "sandbox.substrate.connection")
        return cls(
            api_url=str(options.get("api_url", cls.api_url)).rstrip("/"),
            request_timeout_s=float(options.get("request_timeout_s", cls.request_timeout_s)),
        )


@dataclass(frozen=True)
class CreateConfig:
    """`create:` block of the provider config."""

    template: str = "default-env"
    namespace: str = "ate-env"
    ready_timeout_s: float = 120.0
    ready_poll_interval_s: float = 1.0
    # Optional mapping from SandboxSpec.image to an ActorTemplate name, for
    # workloads that select sandboxes by image reference. Templates must be
    # pre-provisioned on the cluster (see README).
    image_templates: Mapping[str, str] = None  # type: ignore[assignment]

    _ALLOWED = frozenset(
        {"template", "namespace", "ready_timeout_s", "ready_poll_interval_s", "image_templates"}
    )

    @classmethod
    def from_mapping(cls, options: Mapping[str, Any]) -> "CreateConfig":
        _require_keys(options, cls._ALLOWED, "sandbox.substrate.create")
        return cls(
            template=str(options.get("template", cls.template)),
            namespace=str(options.get("namespace", cls.namespace)),
            ready_timeout_s=float(options.get("ready_timeout_s", cls.ready_timeout_s)),
            ready_poll_interval_s=float(
                options.get("ready_poll_interval_s", cls.ready_poll_interval_s)
            ),
            image_templates=dict(options.get("image_templates") or {}),
        )


@dataclass(frozen=True)
class SubstrateProviderOptions:
    """Per-sandbox options carried in ``SandboxSpec.provider_options``."""

    template: str | None = None
    namespace: str | None = None

    _ALLOWED = frozenset({"template", "namespace"})

    @classmethod
    def from_mapping(cls, options: Mapping[str, Any]) -> "SubstrateProviderOptions":
        _require_keys(options, cls._ALLOWED, "SandboxSpec.provider_options")
        return cls(
            template=options.get("template"),
            namespace=options.get("namespace"),
        )


class SubstrateSandboxProvider:
    """NeMo Gym sandbox provider running sandboxes as Substrate actors."""

    name = _PROVIDER_NAME

    def __init__(self, config: Mapping[str, Any] | None = None, **kwargs: Any) -> None:
        config = dict(config or {})
        config.update(kwargs)
        _require_keys(config, frozenset({"connection", "create"}), "sandbox.substrate")
        self._connection = ConnectionConfig.from_mapping(config.get("connection") or {})
        self._create = CreateConfig.from_mapping(config.get("create") or {})
        self._client = httpx.AsyncClient(
            base_url=self._connection.api_url,
            timeout=self._connection.request_timeout_s,
        )

    # -- provider contract -------------------------------------------------

    async def create(self, spec: SandboxSpec) -> SandboxHandle:
        opts = SubstrateProviderOptions.from_mapping(spec.provider_options or {})
        template = self._resolve_template(spec, opts)
        namespace = opts.namespace or self._create.namespace
        env_id = f"gym-{uuid.uuid4().hex[:10]}"

        resp = await self._client.post(
            "/v1/envs",
            json={"id": env_id, "template": template, "namespace": namespace},
        )
        if resp.status_code // 100 != 2:
            raise SandboxCreateError(
                f"creating substrate env {env_id!r} (template {template!r}): "
                f"HTTP {resp.status_code}: {resp.text.strip()}"
            )

        try:
            await self._wait_ready(env_id, spec.ready_timeout_s or self._create.ready_timeout_s)
            for target_path, content in (spec.files or {}).items():
                await self._write_file(env_id, target_path, content.encode(), mode="644")
        except Exception:
            await self._best_effort_delete(env_id)
            raise

        return SandboxHandle(
            sandbox_id=env_id,
            provider_name=self.name,
            raw={"env_id": env_id, "workdir": spec.workdir, "env": dict(spec.env or {})},
        )

    async def exec(
        self,
        handle: SandboxHandle,
        command: str,
        *,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_s: int | float | None = None,
        user: str | int | None = None,
    ) -> SandboxExecResult:
        if user is not None:
            raise ValueError(
                "the substrate provider does not support per-exec `user`: "
                "the guest daemon runs commands as the actor's configured user"
            )
        raw = handle.raw or {}
        merged_env = {**(raw.get("env") or {}), **(env or {})}
        body: dict[str, Any] = {"command": command}
        if merged_env:
            body["env"] = merged_env
        effective_cwd = cwd or raw.get("workdir")
        if effective_cwd:
            body["cwd"] = effective_cwd

        timeout = httpx.Timeout(timeout_s + 5.0) if timeout_s else httpx.USE_CLIENT_DEFAULT
        if timeout_s:
            # Enforce the deadline guest-side too, so a runaway process does
            # not outlive the HTTP request that started it.
            body["command"] = f"timeout {int(timeout_s)} sh -c {shlex.quote(command)}"

        try:
            resp = await self._client.post(
                f"/v1/envs/{handle.sandbox_id}/shell", json=body, timeout=timeout
            )
        except httpx.TimeoutException:
            return SandboxExecResult(
                stdout=None,
                stderr=f"substrate provider: exec timed out after {timeout_s}s",
                return_code=-1,
            )
        resp.raise_for_status()
        payload = resp.json()
        return SandboxExecResult(
            stdout=payload.get("stdout"),
            stderr=payload.get("stderr"),
            return_code=int(payload.get("exit_code", -1)),
        )

    async def upload_file(self, handle: SandboxHandle, source_path: Path, target_path: str) -> None:
        await self._write_file(
            handle.sandbox_id, target_path, Path(source_path).read_bytes(), mode="644"
        )

    async def download_file(self, handle: SandboxHandle, source_path: str, target_path: Path) -> None:
        resp = await self._client.get(
            f"/v1/envs/{handle.sandbox_id}/file", params={"path": source_path}
        )
        resp.raise_for_status()
        content = base64.b64decode(resp.json().get("content") or b"")
        target = Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    async def status(self, handle: SandboxHandle) -> SandboxStatus:
        # A trivial exec through the router doubles as a liveness probe. A
        # suspended actor is auto-resumed by the router, so from Gym's point
        # of view a parked sandbox is still RUNNING — which is the behavior a
        # rollout loop wants.
        try:
            resp = await self._client.post(
                f"/v1/envs/{handle.sandbox_id}/shell", json={"command": "true"}, timeout=10.0
            )
        except httpx.HTTPError:
            return SandboxStatus.UNKNOWN
        if resp.status_code == 404:
            return SandboxStatus.STOPPED
        if resp.status_code // 100 != 2:
            return SandboxStatus.ERROR
        return SandboxStatus.RUNNING

    async def close(self, handle: SandboxHandle) -> None:
        await self._best_effort_delete(handle.sandbox_id)

    async def aclose(self) -> None:
        await self._client.aclose()

    # -- internals ----------------------------------------------------------

    def _resolve_template(self, spec: SandboxSpec, opts: SubstrateProviderOptions) -> str:
        if opts.template:
            return opts.template
        if spec.image:
            mapped = (self._create.image_templates or {}).get(spec.image)
            if mapped:
                return mapped
            raise SandboxCreateError(
                f"no ActorTemplate mapped for image {spec.image!r}: add it to "
                "sandbox.substrate.create.image_templates or set "
                "provider_options.template (templates are pre-provisioned; see README)"
            )
        return self._create.template

    async def _wait_ready(self, env_id: str, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last_error = "no probe attempted"
        while time.monotonic() < deadline:
            try:
                resp = await self._client.post(
                    f"/v1/envs/{env_id}/shell", json={"command": "true"}, timeout=10.0
                )
                if resp.status_code // 100 == 2 and resp.json().get("exit_code") == 0:
                    return
                last_error = f"HTTP {resp.status_code}: {resp.text.strip()[:200]}"
            except httpx.HTTPError as exc:
                last_error = str(exc)
            await asyncio.sleep(self._create.ready_poll_interval_s)
        raise SandboxCreateVerificationError(
            f"substrate env {env_id!r} not ready after {timeout_s}s: {last_error}"
        )

    async def _write_file(self, env_id: str, path: str, content: bytes, *, mode: str) -> None:
        resp = await self._client.post(
            f"/v1/envs/{env_id}/file",
            json={
                "path": path,
                "mode": mode,
                "content": base64.b64encode(content).decode(),
            },
        )
        resp.raise_for_status()

    async def _best_effort_delete(self, env_id: str) -> None:
        try:
            resp = await self._client.delete(f"/v1/envs/{env_id}")
            if resp.status_code not in (200, 202, 204, 404):
                logger.warning(
                    "deleting substrate env %s: HTTP %s: %s",
                    env_id,
                    resp.status_code,
                    resp.text.strip()[:200],
                )
        except httpx.HTTPError as exc:
            logger.warning("deleting substrate env %s: %s", env_id, exc)
