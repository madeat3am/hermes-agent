"""Offline inline receipts: unreported effects are indistinguishable, not retryable."""
import asyncio
from types import SimpleNamespace

import pytest

from tests.gateway.test_buzz_transport_reliability import adapter, offline, event, Relay, CHANNEL


@pytest.mark.asyncio
@pytest.mark.parametrize("hidden_effect,known_not_started", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("failure", ["cancel", "exception"])
async def test_unreported_inline_unwind_holds(adapter, hidden_effect, failure, known_not_started):
    entered = asyncio.Event()
    calls, effects = [], []
    async def handler(message):
        calls.append(message)
        if known_not_started:
            message._gateway_effect_started = False
        if hidden_effect:
            effects.append(message.message_id)
        entered.set()
        if failure == "exception":
            raise ValueError("callback unwound without a receipt")
        await asyncio.Event().wait()
    adapter.set_message_handler(handler)
    adapter._run_cli = Relay([])
    raw = dict(event(2, 1002), content="/stop")
    source = adapter.build_source(chat_id=CHANNEL, chat_type="group", user_id="b" * 64)
    from gateway.session import build_session_key
    adapter._active_sessions[build_session_key(source)] = asyncio.Event()
    state = adapter._channel_state[CHANNEL]
    state["last_ts"] = 1000
    async def attempt():
        task = asyncio.create_task(adapter._handle_events(CHANNEL, state, [raw], checkpoint=True))
        await asyncio.wait_for(entered.wait(), 2)
        if failure == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
    await attempt()
    assert not calls[0]._gateway_accepted
    assert raw["id"] not in state["seen"]
    assert state["last_ts"] == 1000
    assert bool(state.get("admission_unknown")) is not known_not_started
    adapter._load_cursors()
    assert adapter._restore_channel_state(CHANNEL, "group")
    state = adapter._channel_state[CHANNEL]
    if known_not_started:
        entered.clear()
        # The command guard is released when no background owner exists.
        adapter._active_sessions[build_session_key(source)] = asyncio.Event()
        await attempt()
        assert len(calls) == 2
    else:
        await adapter._handle_events(CHANNEL, state, [raw], checkpoint=True)
        assert len(calls) == 1
        assert state["last_ts"] == 1000
        assert state["backlog"] == "admission_unknown"
    assert effects == ([raw["id"]] if hidden_effect else [])
