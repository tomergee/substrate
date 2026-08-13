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

"""Contract tests for the substrate NeMo Gym provider.

Runs against an in-memory fake of the ate-env HTTP API (httpx.MockTransport),
asserting the rules from the adding-a-provider contract: create returns only
when the sandbox executes commands, exec never raises on nonzero exits, close
is cleanup-safe, and provider_options are validated strictly.
"""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import httpx
import pytest

from nemo_gym_substrate._compat import (
    SandboxCreateError,
    SandboxCreateVerificationError,
    SandboxSpec,
    SandboxStatus,
)
from nemo_gym_substrate.provider import SubstrateSandboxProvider


class FakeAteEnv:
    """In-memory ate-env API: envs, files, and scripted shell behavior."""

    def __init__(self) -> None:
        self.envs: dict[str, dict] = {}
        self.files: dict[tuple[str, str], bytes] = {}
        self.shell_log: list[dict] = []
        self.create_status = 201
        self.ready_after_probes = 0  # fail this many readiness probes first
        self._probes = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        if path == "/v1/envs" and method == "POST":
            body = json.loads(request.content)
            if self.create_status // 100 != 2:
                return httpx.Response(self.create_status, text="create refused")
            self.envs[body["id"]] = body
            return httpx.Response(201)
        parts = path.split("/")  # ['', 'v1', 'envs', '<id>', ...]
        env_id = parts[3] if len(parts) > 3 else ""
        if method == "DELETE" and len(parts) == 4:
            return httpx.Response(200 if self.envs.pop(env_id, None) else 404)
        if env_id not in self.envs:
            return httpx.Response(404, text="no such env")
        rest = "/".join(parts[4:])
        if rest == "shell" and method == "POST":
            body = json.loads(request.content)
            self.shell_log.append({"env_id": env_id, **body})
            cmd = body["command"]
            if "true" in cmd and self._probes < self.ready_after_probes:
                self._probes += 1
                return httpx.Response(500, text="guest not up yet")
            if "exit 7" in cmd:
                return httpx.Response(
                    200, json={"stdout": "", "stderr": "boom", "exit_code": 7}
                )
            return httpx.Response(200, json={"stdout": "ok", "stderr": "", "exit_code": 0})
        if rest == "file" and method == "POST":
            body = json.loads(request.content)
            self.files[(env_id, body["path"])] = base64.b64decode(body["content"])
            return httpx.Response(200)
        if rest == "file" and method == "GET":
            content = self.files.get((env_id, request.url.params["path"]))
            if content is None:
                return httpx.Response(404, text="no such file")
            return httpx.Response(
                200,
                json={"content": base64.b64encode(content).decode(), "size": len(content)},
            )
        return httpx.Response(404, text=f"unhandled {method} {path}")


@pytest.fixture()
def fake() -> FakeAteEnv:
    return FakeAteEnv()


@pytest.fixture()
def provider(fake: FakeAteEnv) -> SubstrateSandboxProvider:
    p = SubstrateSandboxProvider(
        {"create": {"ready_poll_interval_s": 0.01, "image_templates": {"img:1": "tpl-img1"}}}
    )
    p._client = httpx.AsyncClient(  # noqa: SLF001 - test seam
        transport=httpx.MockTransport(fake.handler), base_url="http://fake"
    )
    return p


def run(coro):
    return asyncio.run(coro)


def test_create_returns_ready_handle_and_seeds_files(provider, fake):
    handle = run(
        provider.create(SandboxSpec(files={"/task/input.json": '{"n": 1}'}, workdir="/task"))
    )
    assert handle.provider_name == "substrate"
    assert handle.sandbox_id in fake.envs
    assert fake.envs[handle.sandbox_id]["template"] == "default-env"
    assert fake.files[(handle.sandbox_id, "/task/input.json")] == b'{"n": 1}'


def test_create_waits_for_readiness(provider, fake):
    fake.ready_after_probes = 2
    handle = run(provider.create(SandboxSpec()))
    assert handle.sandbox_id in fake.envs
    probes = [e for e in fake.shell_log if e["command"] == "true"]
    assert len(probes) == 3  # two failures + one success


def test_create_failure_raises_create_error(provider, fake):
    fake.create_status = 500
    with pytest.raises(SandboxCreateError):
        run(provider.create(SandboxSpec()))


def test_create_readiness_timeout_cleans_up(provider, fake):
    fake.ready_after_probes = 10_000
    with pytest.raises(SandboxCreateVerificationError):
        run(provider.create(SandboxSpec(ready_timeout_s=0.05)))
    assert fake.envs == {}  # the half-created env was deleted


def test_image_resolves_via_mapping_or_fails(provider, fake):
    handle = run(provider.create(SandboxSpec(image="img:1")))
    assert fake.envs[handle.sandbox_id]["template"] == "tpl-img1"
    with pytest.raises(SandboxCreateError, match="no ActorTemplate mapped"):
        run(provider.create(SandboxSpec(image="img:unmapped")))


def test_exec_nonzero_exit_returns_result_not_raise(provider, fake):
    handle = run(provider.create(SandboxSpec()))
    result = run(provider.exec(handle, "exit 7"))
    assert (result.return_code, result.stderr) == (7, "boom")


def test_exec_merges_spec_env_and_cwd(provider, fake):
    handle = run(provider.create(SandboxSpec(workdir="/w", env={"A": "1", "B": "spec"})))
    run(provider.exec(handle, "echo hi", env={"B": "call"}))
    sent = fake.shell_log[-1]
    assert sent["env"] == {"A": "1", "B": "call"}
    assert sent["cwd"] == "/w"
    run(provider.exec(handle, "echo hi", cwd="/other"))
    assert fake.shell_log[-1]["cwd"] == "/other"


def test_exec_timeout_wraps_command(provider, fake):
    handle = run(provider.create(SandboxSpec()))
    run(provider.exec(handle, "sleep 100", timeout_s=3))
    assert fake.shell_log[-1]["command"].startswith("timeout 3 sh -c ")


def test_exec_rejects_user(provider, fake):
    handle = run(provider.create(SandboxSpec()))
    with pytest.raises(ValueError, match="user"):
        run(provider.exec(handle, "id", user="root"))


def test_file_roundtrip(provider, fake, tmp_path: Path):
    handle = run(provider.create(SandboxSpec()))
    src = tmp_path / "up.bin"
    src.write_bytes(b"\x00\x01payload")
    run(provider.upload_file(handle, src, "/data/up.bin"))
    dst = tmp_path / "down" / "up.bin"
    run(provider.download_file(handle, "/data/up.bin", dst))
    assert dst.read_bytes() == b"\x00\x01payload"


def test_status_running_stopped_unknown(provider, fake):
    handle = run(provider.create(SandboxSpec()))
    assert run(provider.status(handle)) == SandboxStatus.RUNNING
    run(provider.close(handle))
    assert run(provider.status(handle)) == SandboxStatus.STOPPED


def test_close_is_idempotent(provider, fake):
    handle = run(provider.create(SandboxSpec()))
    run(provider.close(handle))
    run(provider.close(handle))  # second delete hits 404; must not raise
    assert handle.sandbox_id not in fake.envs


def test_unknown_provider_option_rejected(provider):
    with pytest.raises(ValueError, match="unknown option"):
        run(provider.create(SandboxSpec(provider_options={"tempalte": "oops"})))


def test_unknown_config_key_rejected():
    with pytest.raises(ValueError, match="unknown option"):
        SubstrateSandboxProvider({"connection": {"api_urll": "typo"}})
