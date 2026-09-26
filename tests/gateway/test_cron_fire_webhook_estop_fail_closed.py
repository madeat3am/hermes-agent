"""Cron fire webhook must fail closed when the ESTOP gate is unimportable.

POST /api/cron/fire consults ``agent.estop.check_paused`` after JWT verify and
before provider resolve/claim/fire. If that import fails (mixed modules after
an upgrade under a running process), the webhook must return 503 +
Retry-After and never resolve the provider, claim, or fire — the same
retryable posture as an engaged ESTOP, which the misfire backstop/NAS retry
recovers. When the import succeeds and no sentinel is engaged, the fire
proceeds normally (covered by test_cron_fire_webhook.py).
"""

from __future__ import annotations

import asyncio
import sys

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from agent import estop
from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter, cors_middleware


def _make_adapter() -> APIServerAdapter:
    return APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "sk-secret"}))


def _create_app(adapter: APIServerAdapter) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["api_server_adapter"] = adapter
    app.router.add_post("/api/cron/fire", adapter._handle_cron_fire)
    return app


@pytest.fixture
def adapter():
    return _make_adapter()


class _SpyProvider:
    """Records durable admission and claimed dispatch calls."""

    def __init__(self):
        self.claimed = []
        self.fired = []

    def claim_fire(self, job_id):
        self.claimed.append(job_id)
        return {"id": job_id, "execution_id": f"exec-{job_id}"}

    def fire_claimed(self, job, *, adapters=None, loop=None):
        self.fired.append(job["id"])
        return True


def _block_estop_import(monkeypatch):
    """Make ``from agent.estop import ...`` raise ImportError for this test."""
    monkeypatch.setitem(sys.modules, "agent.estop", None)


@pytest.mark.asyncio
async def test_estop_import_failure_fails_closed_before_provider(adapter, monkeypatch, tmp_path):
    """ImportError on the estop gate → 503 + Retry-After, no resolve/claim/fire."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    estop._logged_components.clear()
    assert estop.is_engaged() is False

    provider = _SpyProvider()
    resolve_calls = []

    def _resolve():
        resolve_calls.append(1)
        return provider

    monkeypatch.setattr("cron.scheduler_provider.resolve_cron_scheduler", _resolve)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: {"purpose": "cron_fire"}),
    )
    _block_estop_import(monkeypatch)

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        response = await cli.post(
            "/api/cron/fire",
            headers={"Authorization": "Bearer [REDACTED]"},
            json={"job_id": "abc123"},
        )
        assert response.status == 503
        assert response.headers.get("Retry-After") == "60"
        assert (await response.json())["job_id"] == "abc123"

    await asyncio.sleep(0.05)
    assert resolve_calls == [], "unimportable ESTOP gate must not resolve the provider"
    assert provider.claimed == [] and provider.fired == []
    assert adapter.active_agent_work_count() == 0
