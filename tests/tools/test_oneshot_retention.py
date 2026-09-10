"""Retention must not discard another child's pending terminal write."""
import threading

from tools import async_delegation as ad
from tools.process_registry import process_registry


def test_shutdown_terminalizes_all_children_above_retention_cap(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 1)
    ad._reset_for_tests()
    release = threading.Event()
    finished = [threading.Event() for _ in range(3)]
    handles = []
    try:
        for done in finished:
            def runner(done=done):
                release.wait(10)
                done.set()
                return {"status": "completed", "summary": "late"}
            handle = ad.dispatch_async_delegation(
                goal="retention survivor", context=None, toolsets=None, role="leaf",
                model="fixture", session_key="", runner=runner, interrupt_fn=lambda: None,
            )
            assert handle["status"] == "dispatched"
            handles.append(handle["delegation_id"])
        counts = ad.finalize_for_oneshot_shutdown(grace_seconds=0)
        assert counts == {"signaled": 3, "interrupted": 0, "unknown": 3}
        assert [ad.get_durable_delegation(i)["state"] for i in handles] == ["unknown"] * 3
        events = [process_registry.completion_queue.get_nowait() for _ in handles]
        assert {e["delegation_id"] for e in events} == set(handles)
        assert all(e["status"] == "unknown" for e in events)
        assert process_registry.completion_queue.empty()
    finally:
        release.set()
        for done in finished:
            assert done.wait(5)
        ad._reset_for_tests()
        while not process_registry.completion_queue.empty():
            process_registry.completion_queue.get_nowait()
