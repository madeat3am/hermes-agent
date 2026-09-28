from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def review_worker(monkeypatch, tmp_path):
    home = tmp_path / ".hermes"
    home.mkdir()
    report = tmp_path / "frozen report.md"
    report.write_text("  frozen report\n", encoding="utf-8")
    digest = hashlib.sha256("frozen report".encode()).hexdigest()
    body = "\n".join(
        (
            "# Independent review",
            "request_id: req-123",
            f"artifact_path: {report}",
            f"artifact_sha256: {digest}",
            "answering_provider: openai-codex",
            "answering_model: gpt-6-astra",
        )
    )
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_PROFILE", "reviewer")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    from hermes_cli import kanban_db as kb
    from hermes_cli import kanban_db_connect as kbc

    kb._INITIALIZED_PATHS.clear()
    kb.init_db()
    with kbc.connect() as conn:
        task_id = kb.create_task(conn, title="frozen review", body=body, assignee="reviewer")
        assert kb.claim_task(conn, task_id) is not None
        run_id = kb.get_task(conn, task_id).current_run_id
    monkeypatch.setenv("HERMES_KANBAN_TASK", task_id)
    monkeypatch.setenv("HERMES_KANBAN_RUN_ID", str(run_id))
    return SimpleNamespace(task_id=task_id, run_id=run_id, report=report, digest=digest)


def _call_binding(*, provider="xai-oauth", model="grok-4.7"):
    import model_tools

    return json.loads(
        model_tools.handle_function_call(
            "kanban_review_binding",
            {},
            runtime_provider=provider,
            runtime_model=model,
            skip_pre_tool_call_hook=True,
            skip_tool_request_middleware=True,
            skip_tool_execution_middleware=True,
        )
    )


def test_review_binding_reads_owned_task_and_computes_exact_binding(review_worker):
    result = _call_binding()

    assert result["ok"] is True
    assert result["artifact_matches"] is True
    assert result["artifact_path"] == str(review_worker.report)
    assert result["expected_artifact_sha256"] == review_worker.digest
    assert result["task_id"] == review_worker.task_id
    assert result["run_id"] == review_worker.run_id
    assert result["binding"] == {
            "request_id": "req-123",
            "artifact_sha256": review_worker.digest,
            "reviewer_provider": "xai-oauth",
            "reviewer_model": "grok-4.7",
            "same_family": False,
    }
    assert _call_binding(provider="openai-codex", model="gpt-6-sol")["binding"]["same_family"] is True


def test_review_binding_fails_closed_when_artifact_changed(review_worker):
    review_worker.report.write_text("changed", encoding="utf-8")

    result = _call_binding()

    assert result["ok"] is False
    assert result["artifact_matches"] is False
    assert result["error"] == "artifact identity mismatch"
    assert result["expected_artifact_sha256"] == review_worker.digest
    assert result["binding"]["artifact_sha256"] == hashlib.sha256(b"changed").hexdigest()


def test_review_binding_has_no_model_supplied_path_or_identity(review_worker):
    result = json.loads(
        __import__("model_tools").handle_function_call(
            "kanban_review_binding",
            {"artifact_path": "/tmp/other", "reviewer_model": "fake"},
            runtime_provider="xai-oauth",
            runtime_model="grok-4.7",
            skip_pre_tool_call_hook=True,
            skip_tool_request_middleware=True,
            skip_tool_execution_middleware=True,
        )
    )

    assert "unknown parameter(s): artifact_path, reviewer_model" in result["error"]


def test_agent_dispatch_injects_active_route(monkeypatch):
    import model_tools
    from agent.agent_runtime_helpers import invoke_tool

    captured = {}

    def fake_handle(name, args, task_id=None, **kwargs):
        captured.update(name=name, args=args, task_id=task_id, kwargs=kwargs)
        return "ok"

    monkeypatch.setattr(model_tools, "handle_function_call", fake_handle)
    agent = SimpleNamespace(
        session_id="session-1",
        valid_tool_names=["kanban_review_binding"],
        enabled_toolsets=["kanban"],
        disabled_toolsets=[],
        provider="openai-codex",
        model="gpt-6-astra",
        _memory_manager=None,
    )

    assert invoke_tool(
        agent,
        "kanban_review_binding",
        {},
        "task-1",
        pre_tool_block_checked=True,
        skip_tool_request_middleware=True,
        skip_tool_execution_middleware=True,
    ) == "ok"
    assert captured["kwargs"]["runtime_provider"] == "openai-codex"
    assert captured["kwargs"]["runtime_model"] == "gpt-6-astra"


def test_native_sequential_dispatch_binds_active_route(review_worker):
    from agent.tool_executor import _ToolCallRef, _resolve_sequential_dispatch

    agent = SimpleNamespace(
        _context_engine_tool_names=set(),
        _memory_manager=None,
        session_id="session-1",
        valid_tool_names=["kanban_review_binding"],
        enabled_toolsets=["kanban"],
        disabled_toolsets=[],
        provider="xai-oauth",
        model="grok-4.7",
        quiet_mode=False,
    )
    ref = _ToolCallRef(
        name="kanban_review_binding",
        args={},
        task_id=review_worker.task_id,
        call_id="call-1",
        trace=[],
    )

    result = json.loads(_resolve_sequential_dispatch(agent, ref, []).execute({}))

    assert result["ok"] is True
    assert result["binding"]["reviewer_provider"] == "xai-oauth"
    assert result["binding"]["reviewer_model"] == "grok-4.7"
