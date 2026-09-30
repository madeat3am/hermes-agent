"""Real facade, native SQLite/RPC, registered authority and fake transport only."""
import json
import pytest
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from hermes_state import SessionDB
from run_agent import AIAgent
import tui_gateway.server as server


MARKER = "UNRELEASED_REPORT_CANARY_739c"
NOTICE = "Output withheld."


@pytest.mark.parametrize("mode", ["deny", "tool_round", "ordinary", "cleanup_error"])
def test_managed_sink_candidate(tmp_path, monkeypatch, mode):
    monkeypatch.setattr("agent.title_generator.maybe_auto_title", lambda *a, **k: None)
    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {
        "agent": {"output_release": {"required_policy": None if mode == "ordinary" else "release-test/evidence"}}})
    from hermes_cli.plugins import PluginContext, get_plugin_manager
    from hermes_cli.plugins_manifest import PluginManifest
    def decide(**kwargs):
            server._sessions["release-tracer-ui"].update(history=list(agent._session_messages), running=True, agent=agent)
            with patch.object(reader, "get_messages_as_conversation", side_effect=RuntimeError("unavailable")):
                observations.append(server.handle_request({"id": "memory-probe", "method": "session.history",
                    "params": {"session_id": "release-tracer-ui"}}))
            observations.append(server.handle_request({"id": "probe", "method": "session.history",
                "params": {"session_id": "release-tracer-ui"}}))
            assert kwargs["payload"]["final_response"] == "Approved public answer."
            if mode == "decision_error":
                raise RuntimeError(MARKER)
            if mode == "revoked":
                handle.dispose()
            return {"version": 1, "action": "deny" if mode == "deny" else "release", "binding": kwargs["binding"],
                    "candidate_id": kwargs["candidate_id"], "payload_digest": "wrong" if mode == "wrong_digest" else kwargs["payload_digest"]}
    ctx = PluginContext(PluginManifest(name="release-test"), get_plugin_manager())
    def prepare(**k):
        if mode == "prepare_error":
            raise RuntimeError(MARKER)
        return {"version": 1, "action": "ready", "binding": k["binding"]}
    handle = ctx.register_output_release_policy(id="evidence", version=1, prepare=prepare, decide=decide)
    with (
        patch("model_tools.get_tool_definitions", return_value=[]),
        patch("model_tools.check_toolset_requirements", return_value={}),
        patch("agent.process_bootstrap.OpenAI"),
        patch("agent.model_metadata.fetch_model_metadata", return_value={}),
    ):
        agent = AIAgent(api_key="test-key", base_url="https://openrouter.ai/api/v1",
                        quiet_mode=True, skip_context_files=True, skip_memory=True)
    agent.client = MagicMock()
    agent._cached_system_prompt = "Fixed cache prefix."
    agent.compression_enabled = False
    agent.save_trajectories = False
    emissions = []
    executed = []
    agent._sync_external_memory_for_turn = lambda **k: emissions.append(str(k))
    agent.reasoning_callback = emissions.append
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=MARKER, tool_calls=None),
                                 finish_reason="stop")], model="test/model", usage=None)
    if mode == "tool_round":
        response.choices[0].message.tool_calls = [SimpleNamespace(id="call-1", type="function", function=SimpleNamespace(name="terminal", arguments=json.dumps({"command": MARKER})))]
        response.choices[0].finish_reason = "tool_calls"
        agent.valid_tool_names = {"terminal"}
        agent._execute_tool_calls = lambda *a, **k: executed.append(MARKER) or (_ for _ in ()).throw(RuntimeError("managed tool executed: " + MARKER))
    def completion(**kwargs):
        agent.stream_delta_callback = emissions.append
        agent._stream_callback = emissions.append
        agent.interim_assistant_callback = lambda text, **k: emissions.append(text)
        from tools.send_message_tool import send_message_tool
        with patch("tools.send_message_tool._handle_send", side_effect=lambda args: emissions.append(args) or "sent"):
            send_message_tool({"target": "telegram:123", "message": MARKER, "media": MARKER})
        agent._claim_stream_writer()
        agent.tool_gen_callback = emissions.append
        agent._fire_tool_gen_started(MARKER)
        agent._fire_reasoning_delta(MARKER)
        agent._fire_stream_delta(MARKER)
        agent._emit_interim_assistant_message({"role": "assistant", "content": MARKER})
        agent._reset_stream_delivery_tracking()
        from agent.turn_tool_round import stage_tool_call_message
        agent._should_emit_quiet_tool_messages = lambda: True
        agent._vprint = lambda *a, **k: emissions.append(str(a))
        tool_message = SimpleNamespace(content=MARKER, tool_calls=[SimpleNamespace(
            id="call-1", type="function", function=SimpleNamespace(name="terminal", arguments=json.dumps({"command": MARKER})))])
        stage_tool_call_message(agent, assistant_message=tool_message, finish_reason="tool_calls", messages=[])
        return response
    agent.client.chat.completions.create.side_effect = completion
    db = SessionDB(db_path=tmp_path / "state.db")
    db.create_session("release-tracer", source="desktop", model="test/model")
    agent._session_db = db
    agent._session_db_created = True
    agent.session_id = "release-tracer"
    agent._last_flushed_db_idx = 0
    agent._flushed_db_message_ids = set()
    agent._flushed_db_message_session_id = None
    agent._persist_disabled = False
    reader = SessionDB(db_path=tmp_path / "state.db")
    monkeypatch.setattr(server, "_db", reader)
    monkeypatch.setitem(server._sessions, "release-tracer-ui", {
        "session_key": "release-tracer", "history": [], "history_lock": threading.Lock(),
        "running": False, "agent": None})
    observations = []
    transform = ctx.register_hook("transform_llm_output", lambda **k: "Approved public answer.")
    observer = ctx.register_hook("post_api_request", lambda **k: emissions.append(str(k)))
    if mode == "missing":
        handle.dispose()
    if mode == "cleanup_error":
        def fail_cleanup(*a, **k):
            raise RuntimeError(MARKER)
        monkeypatch.setattr("agent.relay_runtime.SESSION_COORDINATOR.end_turn", fail_cleanup)
    try:
        result = agent.run_conversation("Produce the report.")
        if mode == "ordinary":
            assert result["final_response"] == "Approved public answer."
            assert MARKER in json.dumps(emissions)
            assert MARKER in json.dumps(reader.get_messages_as_conversation("release-tracer"))
            return
        if mode in {"missing", "prepare_error"}:
            assert agent.client.chat.completions.create.call_count == 0
            assert result["final_response"] == NOTICE
            return
        assert agent.client.chat.completions.create.call_count == 1
        assert result["final_response"] == ("Approved public answer." if mode == "release" else NOTICE)
        if mode == "tool_round":
            assert result["final_response"] == NOTICE
            assert not executed
            assert MARKER not in json.dumps(emissions)
            return
        assert observations and "error" not in observations[0]
        # A different SQLite connection plus the actual RPC history consumer saw the
        # candidate DURING the hook, not merely after finalization.
        assert MARKER in json.dumps(db.get_messages_as_conversation("release-tracer", trusted_raw=True))
        assert MARKER in json.dumps(agent._session_messages)
        surfaces = {"history_during_denial": observations, "returned_result": result, "callbacks": emissions,
                    "history_after": reader.get_messages_as_conversation("release-tracer")}
        leaked = [name for name, value in surfaces.items() if MARKER in json.dumps(value, default=str)]
        assert not leaked, f"unreleased marker escaped via {leaked}"
    finally:
        observer.dispose()
        transform.dispose()
        handle.dispose()
        reader.close()
        db.close()
