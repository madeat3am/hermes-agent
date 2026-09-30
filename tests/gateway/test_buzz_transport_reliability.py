"""Native transport seam regressions; only the external Buzz CLI is faked."""
import asyncio
import json
import socket
import subprocess

import pytest

from gateway.config import PlatformConfig
from tests.gateway._plugin_adapter_loader import load_plugin_adapter

buzz = load_plugin_adapter("buzz")
CHANNEL = "11111111-1111-4111-8111-111111111111"


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("external transport is forbidden in this regression")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)


@pytest.fixture
def adapter():
    a = buzz.BuzzAdapter(PlatformConfig(enabled=True, extra={
        "relay_url": "https://relay.invalid", "require_mention": False,
    }))
    a.cli_path = "/offline/buzz"
    a._self_pubkey = "f" * 64
    a._user_names["b" * 64] = "Fixture author"
    a._channel_state[CHANNEL] = a._new_channel_state("group")
    return a


@pytest.mark.asyncio
@pytest.mark.parametrize("cli_result", [(0, "{}", ""), (0, "not json", ""),
    (2, "", '{"error":"network","message":"connection lost"}')])
async def test_unknown_ack_does_not_publish_again(adapter, cli_result):
    publications = []

    async def cli(args, **kwargs):
        assert args[:2] == ["messages", "send"]
        publications.append(kwargs["input_text"])
        return cli_result

    adapter._run_cli = cli
    result = await adapter._send_with_retry(CHANNEL, "Offline report", max_retries=0)
    assert publications == ["Offline report"]
    assert not result.success
    assert result.acceptance == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("preflight_failures", [0, 1, 2])
async def test_mention_recovery_stops_at_unknown_publish_result(adapter, preflight_failures):
    membership = "mentioned pubkeys are not channel members"
    unresolved = "mention '@session' does not match a current channel member"
    calls = []
    replies = [(1, "", membership), (1, "", unresolved)][:preflight_failures]
    replies.append((2, "", membership if not preflight_failures else unresolved))
    async def cli(args, **kwargs):
        calls.append((args, kwargs))
        return replies[min(len(calls) - 1, len(replies) - 1)]
    adapter._run_cli = cli
    result = await adapter._run_message_send(
        ["messages", "send", "--channel", CHANNEL, "--content", "-"],
        "@session report", ["b" * 64],
    )
    assert len(calls) == preflight_failures + 1
    assert adapter._send_result(CHANNEL, *result).acceptance == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("chat_type", ["group", "dm"])
async def test_cancellation_of_seen_reaction_does_not_readmit_active_work(adapter, chat_type):
    adapter._channel_state[CHANNEL]["chat_type"] = chat_type
    raw = event()
    reaction_started = asyncio.Event()
    received = []
    async def cli(args, **kwargs):
        assert args[:2] == ["reactions", "add"]
        reaction_started.set()
        await asyncio.Event().wait()
    async def handler(message):
        received.append(message.message_id)
    adapter._run_cli = cli
    adapter.set_message_handler(handler)
    state = adapter._channel_state[CHANNEL]
    first = asyncio.create_task(adapter._handle_events(CHANNEL, state, [raw]))
    await asyncio.wait_for(reaction_started.wait(), 2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    adapter._run_cli = Relay([])
    await adapter._handle_events(CHANNEL, state, [raw])
    await asyncio.gather(*adapter._session_tasks.values())
    assert received == [raw["id"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["/status", "/stop", "/new", "/reset", "clarify"])
@pytest.mark.parametrize("reply", ["none", "cancel", "fail", "pre_handler_fail"])
async def test_inline_admission_survives_optional_reply(adapter, command, reply):
    from tools import clarify_gateway
    started, release, replying = asyncio.Event(), asyncio.Event(), asyncio.Event()
    handled = []
    async def handler(message):
        if message.message_id == event()["id"]:
            started.set()
            await release.wait()
            return None
        if reply == "pre_handler_fail":
            raise RuntimeError("before handling")
        handled.append(message.message_id)
        if command == "clarify":
            clarify_gateway.resolve_gateway_clarify("fixture-inline", message.text)
        return None if reply == "none" else "optional reply"
    async def cli(args, **kwargs):
        if args[:2] == ["messages", "send"]:
            replying.set()
            if reply == "cancel":
                await asyncio.Event().wait()
            raise RuntimeError("reply failed")
        return await Relay([])(args, **kwargs)
    adapter.set_message_handler(handler)
    adapter._run_cli = cli
    state = adapter._channel_state[CHANNEL]
    first = None
    try:
        await adapter._handle_events(CHANNEL, state, [event()])
        await asyncio.wait_for(started.wait(), 2)
        if command == "clarify":
            clarify_gateway.register("fixture-inline", next(iter(adapter._active_sessions)), "Question", None)
        raw = event(2, 1002)
        raw["content"] = command
        first = asyncio.create_task(adapter._handle_events(CHANNEL, state, [raw]))
        if reply == "cancel":
            await asyncio.wait_for(replying.wait(), 2)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        else:
            await first
        await adapter._handle_events(CHANNEL, state, [raw])
        assert handled == ([] if reply == "pre_handler_fail" else [raw["id"]])
        assert (raw["id"] in state["seen"]) is (reply != "pre_handler_fail")
    finally:
        if first and not first.done():
            first.cancel()
        release.set()
        await asyncio.gather(*adapter._session_tasks.values(), return_exceptions=True)
        clarify_gateway.wait_for_response("fixture-inline", 0.001)


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["/new", "/reset"])
@pytest.mark.parametrize("boundary", ["pre_effect", "cleanup", "store_commit", "post_commit"])
async def test_native_reset_cancellation_receipt(adapter, tmp_path, monkeypatch, command, boundary):
    from types import SimpleNamespace
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore, AsyncSessionStore, build_session_key
    from gateway.slash_commands_session import GatewaySessionCommandsMixin

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "isolated-hermes"))
    started, release, entered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    import threading
    loop = asyncio.get_running_loop()
    worker_release, worker_done = threading.Event(), asyncio.Event()
    class PausedStore(SessionStore):
        def reset_session(self, *args, **kwargs):
            entry = super().reset_session(*args, **kwargs)
            if boundary == "store_commit":
                loop.call_soon_threadsafe(entered.set)
                try:
                    assert worker_release.wait(5), "reset worker was not released"
                finally:
                    loop.call_soon_threadsafe(worker_done.set)
            return entry
    store = PausedStore(tmp_path / "sessions", GatewayConfig())
    messages, resets = [], []
    async def cleanup(key):
        if boundary == "cleanup":
            entered.set()
            await asyncio.Event().wait()
    async def hook(source, key, old, new):
        resets.append(new)
        entered.set()
        await asyncio.Event().wait()
    owner = SimpleNamespace(
        session_store=store, async_session_store=AsyncSessionStore(store),
        _session_key_for_source=build_session_key,
        _invalidate_session_run_generation=lambda *a, **k: None,
        _release_running_agent_state=lambda *a, **k: None,
        _cleanup_old_agent_for_reset=cleanup,
        _evict_cached_agent=lambda *a, **k: None,
        _clear_conversation_scope=lambda *a, **k: None,
        _fire_session_reset_hooks=hook,
    )
    async def handler(message):
        if message.text != command:
            store.get_or_create_session(message.source)
            started.set()
            await release.wait()
            return
        messages.append(message)
        if boundary == "pre_effect":
            entered.set()
            await asyncio.Event().wait()
        return await GatewaySessionCommandsMixin._handle_reset_command(owner, message)
    adapter.set_message_handler(handler)
    adapter._run_cli = Relay([])
    state = adapter._channel_state[CHANNEL]
    state["last_ts"] = 1000
    task = None
    try:
        await adapter._handle_events(CHANNEL, state, [event()])
        await asyncio.wait_for(started.wait(), 2)
        key = next(iter(store._entries))
        old_sid = store._entries[key].session_id
        raw = dict(event(2, 1002), content=command)
        task = asyncio.create_task(adapter._handle_events(CHANNEL, state, [raw], checkpoint=True))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert state["last_ts"] == 1000
        persisted = SessionStore(tmp_path / "sessions", GatewayConfig())
        new_sid = persisted.get_or_create_session(messages[0].source, touch_activity=False).session_id
        assert (new_sid != old_sid) is (boundary in ("post_commit", "store_commit"))
        assert messages[0]._gateway_accepted is (boundary == "post_commit")
        assert (raw["id"] in state["seen"]) is (boundary == "post_commit")
        # Re-open the actual cursor owner, not a fixture-supplied receipt.
        adapter._load_cursors()
        assert adapter._restore_channel_state(CHANNEL, "group")
        state = adapter._channel_state[CHANNEL]
        entered.clear()
        task = asyncio.create_task(adapter._handle_events(CHANNEL, state, [raw], checkpoint=True))
        if boundary == "pre_effect":
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert len(messages) == 2
        else:
            await asyncio.wait_for(task, 2)
            assert len(messages) == 1
            assert state["last_ts"] == (1002 if boundary == "post_commit" else 1000)
            if boundary != "post_commit":
                assert state["backlog"] == "admission_unknown"
        assert resets == ([new_sid] if boundary == "post_commit" else [])
    finally:
        worker_release.set()
        if boundary == "store_commit":
            await asyncio.wait_for(worker_done.wait(), 2)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        release.set()
        await asyncio.gather(*adapter._session_tasks.values(), return_exceptions=True)


def event(index=1, timestamp=1001):
    return {"id": f"{index:064x}", "pubkey": "b" * 64, "content": "Fixture ask",
            "created_at": timestamp, "kind": 9, "tags": [["h", CHANNEL]]}


@pytest.mark.asyncio
async def test_unaccepted_event_can_replay_when_handler_becomes_available(adapter):
    raw = event()
    state = adapter._channel_state[CHANNEL]
    async def cli(args, **kwargs):
        assert args[:2] == ["reactions", "add"]
        return 0, "{}", ""
    adapter._run_cli = cli
    # No registered gateway owner means there was no acceptance.
    await adapter._handle_events(CHANNEL, state, [raw])
    received = []
    async def handler(message):
        received.append(message)
    adapter.set_message_handler(handler)
    await adapter._handle_events(CHANNEL, state, [raw])
    await asyncio.gather(*adapter._session_tasks.values())
    assert [e.message_id for e in received] == [raw["id"]]
    assert received[0].source.user_id == received[0].user_id == raw["pubkey"]
    assert received[0].source.user_name == received[0].user_name == "Fixture author"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [0, 1, 2, 124])
async def test_standalone_recovery_preserves_publication_receipt(monkeypatch, code):
    calls = []
    async def cli(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            return code, "{}", "mention '@session' does not match a current channel member"
        return 0, json.dumps({"accepted": True, "event_id": "a" * 64}), ""
    monkeypatch.setattr(buzz, "_exec_buzz", cli)
    monkeypatch.setenv("BUZZ_PRIVATE_KEY", "1" * 64)
    auth_tag = json.dumps(["auth", "a" * 64, "", "b" * 128], separators=(",", ":"))
    monkeypatch.setenv("BUZZ_AUTH_TAG", auth_tag)
    config = PlatformConfig(enabled=True, extra={
        "relay_url": "https://relay.invalid", "cli_path": "/usr/bin/false",

    })
    result = await buzz._standalone_send(config, CHANNEL, "@session report")
    assert len(calls) == (2 if code == 1 else 1)
    assert result["acceptance"] == ("accepted" if code == 1 else "unknown")
    assert result["retryable"] is False
    if code == 1:
        assert calls[1][1]["auth_tag"] == calls[0][1]["auth_tag"] == auth_tag


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_page", ["not JSON", "{}", "[null]", "[{}]", None])
async def test_invalid_older_fetch_cannot_certify_coverage(adapter, bad_page):
    rows = [event(i, 1000 + i) for i in range(1, 61)]
    relay = Relay(rows)
    async def cli(args, **kwargs):
        if "--before" in args:
            if bad_page is None:
                invalid = dict(rows[0], created_at="1001")
                return 0, json.dumps([rows[0], invalid]), ""
            return 0, bad_page, ""
        return await relay(args, **kwargs)
    adapter._run_cli = cli
    state = adapter._channel_state[CHANNEL]
    state["last_ts"] = 1000
    received = []
    async def handler(message):
        received.append(message.message_id)
    adapter.set_message_handler(handler)
    await adapter._poll_channel(CHANNEL)
    await asyncio.gather(*adapter._session_tasks.values())
    assert state["last_ts"] == 1000
    assert state["backlog"] == "fetch_invalid"
    assert received == []
    saved = json.loads(adapter._cursor_path().read_text())["channels"][CHANNEL]
    assert saved["last_ts"] == 1000 and saved["backlog"] == "fetch_invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize("codes,acceptance,success", [
    ([2], "unknown", False), ([0, 2], "partial", True),
    ([0, 0], "accepted", True), ([1, 1], "not_attempted", False),
])
async def test_multi_image_keeps_per_part_acceptance(adapter, codes, acceptance, success):
    calls = []
    async def cli(args, **kwargs):
        code = codes[len(calls)]
        calls.append(args)
        return code, json.dumps({"accepted": True, "event_id": f"{len(calls):064x}"}), "usage" if code == 1 else "lost ack"
    adapter._run_cli = cli
    result = await adapter.send_multiple_images(CHANNEL, [
        (f"https://relay.invalid/{i}.png", "") for i in range(len(codes))])
    assert result.success is success
    assert result.acceptance == acceptance
    assert result.retryable is False
    assert len(calls) == len(codes)
    assert [part.acceptance for part in result.parts] == [
        {0: "accepted", 1: "not_attempted", 2: "unknown"}[code] for code in codes]


class Relay:
    """CLI native contract: newest limit, inclusive before/since, ascending output."""
    def __init__(self, events):
        self.events = events
        self.fetches = []

    async def __call__(self, args, **kwargs):
        if args[:2] == ["reactions", "add"]:
            await asyncio.sleep(0)
            return 0, "{}", ""
        assert args[:2] == ["messages", "get"]
        options = dict(zip(args[2::2], args[3::2]))
        assert set(options) <= {"--channel", "--limit", "--since", "--before"}
        self.fetches.append(options)
        since, before = int(options.get("--since", 0)), int(options.get("--before", 999999))
        limit = min(int(options["--limit"]), 200)
        selected = sorted((e for e in self.events if since <= e["created_at"] <= before),
                          key=lambda e: (e["created_at"], e["id"]), reverse=True)[:limit]
        return 0, json.dumps(sorted(selected, key=lambda e: (e["created_at"], e["id"]))), ""


@pytest.mark.asyncio
@pytest.mark.parametrize("same_second", [False, True])
async def test_catchup_accepts_every_event_before_checkpointing(adapter, same_second):
    events = [event(i, 1001 if same_second else 1000 + i) for i in range(1, 61)]
    for raw in events:
        raw["tags"].append(["e", raw["id"], "", "root"])
    relay = Relay(events)
    adapter._run_cli = relay
    state = adapter._channel_state[CHANNEL]
    state["last_ts"] = 1000
    received = []
    async def handler(message):
        received.append(message.message_id)
    adapter.set_message_handler(handler)
    await adapter._poll_channel(CHANNEL)
    await asyncio.gather(*adapter._session_tasks.values())
    await adapter._poll_channel(CHANNEL)
    await asyncio.gather(*adapter._session_tasks.values())
    assert sorted(received) == sorted(e["id"] for e in events)
    saved = json.loads(adapter._cursor_path().read_text())["channels"][CHANNEL]
    assert saved["last_ts"] == max(e["created_at"] for e in events)


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,count,same_second", [
    ("timestamp_saturated", 201, True), ("page_bound", 600, False),
])
async def test_bounded_backlog_is_persisted_without_skipping(adapter, reason, count, same_second):
    events = [event(i, 1001 if same_second else 1000 + i) for i in range(1, count + 1)]
    relay = Relay(events)
    adapter._run_cli = relay
    state = adapter._channel_state[CHANNEL]
    state["last_ts"] = 1000
    await adapter._poll_channel(CHANNEL)
    assert len(relay.fetches) <= 10
    saved = json.loads(adapter._cursor_path().read_text())["channels"][CHANNEL]
    assert saved["last_ts"] == 1000
    assert saved["backlog"] == reason
    adapter._load_cursors()
    assert adapter._restore_channel_state(CHANNEL, "group")
    assert adapter._channel_state[CHANNEL]["backlog"] == reason


@pytest.mark.asyncio
async def test_ws_arrival_cannot_move_checkpoint_past_unfetched_gap(adapter):
    events = [event(i, 1000 + i) for i in range(1, 61)]
    for raw in events:
        raw["tags"].append(["e", raw["id"], "", "root"])
    relay = Relay(events)
    adapter._run_cli = relay
    state = adapter._channel_state[CHANNEL]
    state["last_ts"] = 1000
    received = []
    async def handler(message):
        received.append(message.message_id)
    adapter.set_message_handler(handler)
    await adapter._handle_ws_message(None, {"channel": CHANNEL}, ["EVENT", "channel", events[-1]])
    await adapter._poll_channel(CHANNEL)
    await asyncio.gather(*adapter._session_tasks.values())
    assert sorted(received) == sorted(e["id"] for e in events)
