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

Runs against in-memory fakes of the ate-env API, asserting the rules from
the adding-a-provider contract: create returns only when the sandbox
executes commands, exec never raises on nonzero exits, close is
cleanup-safe, and provider_options are validated strictly.

The fakes mirror ate-env after the EnvironmentService migration (env#18):
environment lifecycle is the gRPC ``ateenv.v1.EnvironmentService``
(FakeEnvService), while exec and file transfer stay on the HTTP guest proxy
(FakeAteEnv via httpx.MockTransport).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from pathlib import Path

import grpc
import grpc.aio
import httpx
import pytest

from nemo_gym_substrate._compat import (
    SandboxCreateError,
    SandboxCreateVerificationError,
    SandboxSpec,
    SandboxStatus,
)
from nemo_gym_substrate._gen import env_pb2
from nemo_gym_substrate.provider import SubstrateSandboxProvider


def _rpc_error(code: grpc.StatusCode, details: str) -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        code, grpc.aio.Metadata(), grpc.aio.Metadata(), details=details
    )


class FakeAteEnv:
    """In-memory ate-env guest proxy: envs, files, scripted shell behavior."""

    def __init__(self) -> None:
        self.envs: dict[str, dict] = {}
        self.files: dict[tuple[str, str], bytes] = {}
        self.shell_log: list[dict] = []
        self.create_code: grpc.StatusCode | None = None  # scripted create failure
        self.delete_code: grpc.StatusCode | None = None  # scripted delete failure
        self.ready_after_probes = 0  # fail this many readiness probes first
        self._probes = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        path, method = request.url.path, request.method
        parts = path.split("/")  # ['', 'v1', 'envs', '<id>', ...]
        env_id = parts[3] if len(parts) > 3 else ""
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


class FakeEnvService:
    """gRPC EnvironmentService fake sharing env state with FakeAteEnv."""

    def __init__(self, fake: FakeAteEnv) -> None:
        self._fake = fake

    async def CreateEnvironment(self, request, timeout=None):
        if self._fake.create_code is not None:
            raise _rpc_error(self._fake.create_code, "create refused")
        self._fake.envs[request.id] = {
            "template": request.template.name,
            "namespace": request.template.namespace,
        }
        return env_pb2.CreateEnvironmentResponse()

    async def DeleteEnvironment(self, request, timeout=None):
        if self._fake.delete_code is not None:
            raise _rpc_error(self._fake.delete_code, "delete refused")
        if self._fake.envs.pop(request.id, None) is None:
            raise _rpc_error(grpc.StatusCode.NOT_FOUND, "no such env")
        return env_pb2.DeleteEnvironmentResponse()


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
    p._env_service = FakeEnvService(fake)  # noqa: SLF001 - test seam
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
    fake.create_code = grpc.StatusCode.RESOURCE_EXHAUSTED
    with pytest.raises(SandboxCreateError, match="RESOURCE_EXHAUSTED"):
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


def test_exec_subsecond_timeout_floors_to_one(provider, fake):
    # `timeout 0` disables the limit in coreutils, so a sub-second deadline
    # must round up to 1s rather than floor to 0.
    handle = run(provider.create(SandboxSpec()))
    run(provider.exec(handle, "sleep 100", timeout_s=0.5))
    assert fake.shell_log[-1]["command"].startswith("timeout 1 sh -c ")


def test_exec_zero_timeout_means_shortest_deadline(provider, fake):
    # An explicit 0 is a deadline (the 1s floor), not "unlimited".
    handle = run(provider.create(SandboxSpec()))
    run(provider.exec(handle, "sleep 100", timeout_s=0))
    assert fake.shell_log[-1]["command"].startswith("timeout 1 sh -c ")


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
    run(provider.close(handle))  # second delete hits NOT_FOUND; must not raise
    assert handle.sandbox_id not in fake.envs


def test_close_logs_but_does_not_raise_on_delete_failure(provider, fake, caplog):
    handle = run(provider.create(SandboxSpec()))
    fake.delete_code = grpc.StatusCode.INTERNAL
    with caplog.at_level(logging.WARNING, logger="nemo_gym_substrate.provider"):
        run(provider.close(handle))  # best-effort: never raises
    assert any("deleting substrate env" in r.message for r in caplog.records)


def test_unknown_provider_option_rejected(provider):
    with pytest.raises(ValueError, match="unknown option"):
        run(provider.create(SandboxSpec(provider_options={"tempalte": "oops"})))


def test_unknown_config_key_rejected():
    with pytest.raises(ValueError, match="unknown option"):
        SubstrateSandboxProvider({"connection": {"api_urll": "typo"}})


def test_exec_client_timeout_returns_sentinel_result(provider, fake):
    handle = run(provider.create(SandboxSpec()))

    def raise_timeout(request: httpx.Request) -> httpx.Response:
        if b"sleepy" in request.content:
            raise httpx.ReadTimeout("deadline", request=request)
        return fake.handler(request)

    provider._client = httpx.AsyncClient(  # noqa: SLF001 - test seam
        transport=httpx.MockTransport(raise_timeout), base_url="http://fake"
    )
    result = run(provider.exec(handle, "sleepy", timeout_s=1))
    assert result.return_code == -1
    assert "timed out" in (result.stderr or "")


def test_status_error_and_unknown(provider, fake):
    handle = run(provider.create(SandboxSpec()))

    def flaky(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/shell"):
            return httpx.Response(503, text="router drain")
        return fake.handler(request)

    provider._client = httpx.AsyncClient(  # noqa: SLF001
        transport=httpx.MockTransport(flaky), base_url="http://fake"
    )
    assert run(provider.status(handle)) == SandboxStatus.ERROR

    def unreachable(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    provider._client = httpx.AsyncClient(  # noqa: SLF001
        transport=httpx.MockTransport(unreachable), base_url="http://fake"
    )
    assert run(provider.status(handle)) == SandboxStatus.UNKNOWN


def test_provider_options_override_template_and_namespace(provider, fake):
    handle = run(
        provider.create(
            SandboxSpec(provider_options={"template": "tpl-x", "namespace": "ns-x"})
        )
    )
    assert fake.envs[handle.sandbox_id]["template"] == "tpl-x"
    assert fake.envs[handle.sandbox_id]["namespace"] == "ns-x"


def test_download_missing_file_raises(provider, fake, tmp_path: Path):
    handle = run(provider.create(SandboxSpec()))
    with pytest.raises(httpx.HTTPStatusError):
        run(provider.download_file(handle, "/nope", tmp_path / "out"))


def test_aclose_releases_client(provider):
    run(provider.aclose())
    with pytest.raises(RuntimeError):
        run(provider._client.post("/v1/envs/x/shell", json={}))  # noqa: SLF001
