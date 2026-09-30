"""Required authority admission and real read-through tracer."""
import pytest
from unittest.mock import patch
from run_agent import AIAgent


def test_missing_authority_precedes_initializers(monkeypatch):
    monkeypatch.setattr('hermes_cli.config.load_config_readonly', lambda: {
        'agent': {'output_release': {'required_policy': 'absent/evidence'}}})
    with patch('agent.agent_init._build_client', side_effect=AssertionError('client reached')):
        with pytest.raises(ValueError, match='Required output release authority unavailable'):
            AIAgent(api_key='test', quiet_mode=True, skip_memory=True)


@pytest.mark.parametrize("release", [False, True, "veto"])
def test_native_web_read_reaches_answer_without_preview(tmp_path, monkeypatch, release):
    import json
    import threading
    import urllib.request
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from types import SimpleNamespace as NS
    from unittest.mock import MagicMock
    from hermes_cli.plugins import PluginContext, get_plugin_manager
    from hermes_cli.plugins_manifest import PluginManifest
    from hermes_state import SessionDB
    from tools import web_tools
    marker = 'PRIVATE_EVIDENCE_BODY_724'
    hits, emissions, decisions = [], [], []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            self.send_response(200); self.end_headers()
            self.wfile.write(marker.encode())
        def log_message(self, *a): pass
    http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=http.serve_forever, daemon=True); worker.start()
    url = f'http://127.0.0.1:{http.server_port}/evidence'
    class Backend:
        name = 'tracer-web'
        def supports_search(self): return True
        def search(self, query, limit):
            return {'success': True, 'data': {'web': [{'url': url, 'title': marker, 'description': marker}]}}
        def extract(self, urls, format=None):
            return [{'url': u, 'title': marker, 'content': urllib.request.urlopen(u).read().decode()} for u in urls]
    backend = Backend()
    monkeypatch.setattr(web_tools, '_ensure_web_plugins_loaded', lambda: None)
    monkeypatch.setattr(web_tools, '_get_search_backend', lambda: backend.name)
    monkeypatch.setattr(web_tools, '_get_extract_backend', lambda: backend.name)
    monkeypatch.setattr('agent.web_search_registry.get_provider', lambda name: backend)
    monkeypatch.setattr(web_tools, '_resolve_extract_provider', lambda name: (backend, None))
    async def local_fixture_only(u): return u == url
    monkeypatch.setattr(web_tools, 'async_is_safe_url', local_fixture_only)
    ctx = PluginContext(PluginManifest(name='read-tracer'), get_plugin_manager())
    def decide(**k):
        decisions.append(k)
        return {'version': 1, 'action': 'release' if release is True else 'deny', 'binding': k['binding'],
                'candidate_id': k['candidate_id'], 'payload_digest': k['payload_digest']}
    authority = ctx.register_output_release_policy(id='evidence', version=1,
        prepare=lambda **k: {'version': 1, 'action': 'ready', 'binding': k['binding']}, decide=decide)
    observers = [ctx.register_hook(name, lambda **k: emissions.append(k))
                 for name in ('pre_api_request', 'post_api_request', 'post_tool_call')]
    if release == 'veto':
        observers.append(ctx.register_hook('pre_tool_call', lambda **k: {'action': 'block', 'message': 'Policy veto'}))
    monkeypatch.setattr('hermes_cli.config.load_config_readonly', lambda: {
        'agent': {'output_release': {'required_policy': 'read-tracer/evidence'}}})
    monkeypatch.setattr('agent.title_generator.maybe_auto_title', lambda *a, **k: None)
    with (patch('model_tools.get_tool_definitions', return_value=[]),
          patch('model_tools.check_toolset_requirements', return_value={}),
          patch('agent.process_bootstrap.OpenAI'),
          patch('agent.model_metadata.fetch_model_metadata', return_value={})):
        agent = AIAgent(api_key='test', base_url='https://openrouter.ai/api/v1', quiet_mode=True,
                        skip_memory=True, skip_context_files=True)
    agent.valid_tool_names = {'web_search', 'web_extract'}
    agent._cached_system_prompt = 'Byte stable prefix.'
    agent.compression_enabled = False
    agent.save_trajectories = False
    for name in ('stream_delta_callback', 'tool_progress_callback', 'tool_start_callback',
                 'tool_complete_callback', 'status_callback', 'thinking_callback', 'reasoning_callback'):
        setattr(agent, name, lambda *a, **k: emissions.append((a, k)))
    agent._print_fn = lambda *a, **k: emissions.append((a, k))
    db = SessionDB(db_path=tmp_path/'state.db')
    db.create_session('reads', source='desktop', model='test/model')
    agent._session_db, agent.session_id, agent._session_db_created = db, 'reads', True
    agent._last_flushed_db_idx = 0
    agent._flushed_db_message_ids = set()
    agent._persist_disabled = False
    requests = []
    def completion(**k):
        requests.append(k)
        n = len(requests)
        if n == 3:
            assert marker in json.dumps(k['messages'])
        name = 'web_search' if n == 1 else 'web_extract'
        args = {'query': 'fixture evidence ' + url} if n == 1 else {'urls': [url]}
        calls = [NS(id=f'call-{n}', type='function', function=NS(name=name, arguments=json.dumps(args)))] if n < 3 else None
        msg = NS(content=marker if n < 3 else 'Approved answer.', tool_calls=calls)
        return NS(choices=[NS(message=msg, finish_reason='tool_calls' if calls else 'stop')], model='test/model', usage=None)
    agent.client = MagicMock()
    agent.client.chat.completions.create.side_effect = completion
    try:
        result = agent.run_conversation('Read evidence then answer.')
        if release == 'veto':
            assert hits == []
            assert 'Policy veto' in json.dumps(requests[-1]['messages'])
            assert marker not in json.dumps(emissions, default=str)
            assert result['final_response'] == 'Output withheld.'
            return
        assert hits == ['/evidence'], (hits, result)
        assert len(requests) == 3
        assert decisions and decisions[0]['payload']['final_response'] == 'Approved answer.'
        assert result['final_response'] == ('Approved answer.' if release else 'Output withheld.')
        if release: assert result['output_release']['payload_digest'] == decisions[0]['payload_digest']
        assert marker not in json.dumps(emissions, default=str)
        assert marker not in json.dumps(db.get_messages_as_conversation('reads'))
        assert marker in json.dumps(db.get_messages_as_conversation('reads', trusted_raw=True))
        assert agent._cached_system_prompt == 'Byte stable prefix.'
    finally:
        for observer in observers: observer.dispose()
        authority.dispose(); db.close(); http.shutdown(); http.server_close(); worker.join()

def test_prepare_precedes_context_start_and_timeout_stops_model(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from hermes_cli.plugins import PluginContext, get_plugin_manager
    from hermes_cli.plugins_manifest import PluginManifest
    from hermes_state import SessionDB
    import threading
    calls = []
    entered, unblock = threading.Event(), threading.Event()
    def prepare(**k):
        calls.append('prepare'); entered.set(); unblock.wait(2)
        return {'version': 1, 'action': 'ready', 'binding': k['binding']}
    ctx = PluginContext(PluginManifest(name='timeout-tracer'), get_plugin_manager())
    handle = ctx.register_output_release_policy(id='evidence', version=1, prepare=prepare, decide=lambda **k: {})
    monkeypatch.setattr('agent.output_release.POLICY_TIMEOUT', .02)
    monkeypatch.setattr('hermes_cli.config.load_config_readonly', lambda: {
        'agent': {'output_release': {'required_policy': 'timeout-tracer/evidence'}}})
    with (patch('model_tools.get_tool_definitions', return_value=[]),
          patch('model_tools.check_toolset_requirements', return_value={}),
          patch('agent.process_bootstrap.OpenAI'),
          patch('agent.context_compressor.ContextCompressor.on_session_start', side_effect=lambda *a, **k: calls.append('start')),
          patch('agent.model_metadata.fetch_model_metadata', return_value={})):
        agent = AIAgent(api_key='test', base_url='https://openrouter.ai/api/v1', quiet_mode=True, skip_memory=True, skip_context_files=True)
    db = SessionDB(db_path=tmp_path/'state.db'); db.create_session('timeout', source='cli')
    agent._session_db, agent.session_id = db, 'timeout'
    try:
        assert calls == [], 'constructor ran a model-capable on-start before prepare'
        result = agent.run_conversation('Evidence')
        assert entered.is_set() and calls == ['prepare']
        assert result['final_response'] == 'Output withheld.'
        assert not agent.client.chat.completions.create.called
    finally:
        unblock.set(); handle.dispose(); db.close()

@pytest.mark.parametrize('required', [False, True])
def test_direct_provider_codex_status_sinks(required, capsys):
    from agent.codex_runtime import make_codex_app_server_event_bridge
    agent = AIAgent.__new__(AIAgent)
    agent._required_output_release_policy = 'test/evidence' if required else None
    emissions = []
    agent._print_fn = None
    agent.stream_delta_callback = emissions.append
    agent.tool_progress_callback = lambda *a, **k: emissions.append((a, k))
    captured = agent.stream_delta_callback
    captured('private-body')
    bridge = make_codex_app_server_event_bridge(agent)
    bridge({'method': 'item/started', 'params': {'item': {'type': 'commandExecution', 'id': 'c', 'command': 'private-body'}}})
    agent._safe_print('private-body')
    if required:
        assert not emissions
        assert 'private-body' not in capsys.readouterr().out
    else:
        assert emissions and 'private-body' in capsys.readouterr().out


def test_unknown_and_execute_code_cannot_claim_read_classification():
    from types import SimpleNamespace as NS
    from agent.output_release import read_batch_allowed
    agent = NS(_output_release_turn=NS(current=lambda: True))
    for name in ('execute_code', 'terminal', 'send_message', 'unknown', 'write_file'):
        assert not read_batch_allowed(agent, [NS(function=NS(name=name), read_only=True)])


