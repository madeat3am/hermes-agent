"""Cron ESTOP must fail closed when the estop gate is unimportable.

``cron/scheduler_tick.py::tick`` and ``cron/scheduler_provider.py::fire_overdue_jobs``
both consult ``agent.estop.check_paused`` before dispatching. If that import
fails (mixed modules after an upgrade under a running process), the tick must
skip its due-job scan and the misfire sweep must skip claim/fire — returning 0 —
rather than proceeding with no pause gate. When the import succeeds and no
sentinel is engaged, both paths proceed normally.
"""

from __future__ import annotations

import sys
from datetime import timedelta

from agent import estop
from cron.jobs import _hermes_now, create_job, get_job, load_jobs, save_jobs
from cron.scheduler_provider import CronScheduler, fire_overdue_jobs


def _block_estop_import(monkeypatch):
    """Make ``from agent.estop import ...`` raise ImportError for this test."""
    monkeypatch.setitem(sys.modules, "agent.estop", None)


def test_tick_skips_scan_when_estop_unimportable(tmp_path, monkeypatch):
    """An ImportError on the estop gate must skip the due-job scan, not dispatch."""
    import cron.scheduler as scheduler

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    estop._logged_components.clear()
    calls = []
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: calls.append(1) or [])
    _block_estop_import(monkeypatch)

    assert scheduler.tick(verbose=False) == 0
    assert calls == [], "unimportable ESTOP gate must skip the due-job scan entirely"


def test_tick_scans_when_estop_importable_and_unpaused(tmp_path, monkeypatch):
    """Normal flow: importable gate + no sentinel → the scan proceeds."""
    import cron.scheduler as scheduler

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    estop._logged_components.clear()
    assert estop.is_engaged() is False
    calls = []
    monkeypatch.setattr(scheduler, "get_due_jobs", lambda: calls.append(1) or [])

    assert scheduler.tick(verbose=False) == 0  # idle: scanned, nothing due
    assert calls == [1], "unpaused tick must reach the due-job scan"


class _RecordingProvider(CronScheduler):
    """External-provider stand-in: real store-CAS claim, recorded fire."""

    def __init__(self):
        import threading

        self.fired = []
        self._done = threading.Event()

    @property
    def name(self):
        return "recording"

    def start(self, stop_event, **kw):  # pragma: no cover - unused
        return None

    def fire_claimed(self, claimed_job, *, adapters=None, loop=None,
                     cancel_event=None):
        self.fired.append(claimed_job["id"])
        self._done.set()
        return True

    def wait_fired(self, timeout=5.0):
        return self._done.wait(timeout)


def _park_in_past(job_id, minutes):
    jobs = load_jobs()
    for job in jobs:
        if job["id"] == job_id:
            job["next_run_at"] = (_hermes_now() - timedelta(minutes=minutes)).isoformat()
    save_jobs(jobs)


def test_misfire_skips_claim_and_fire_when_estop_unimportable(
    tmp_path, monkeypatch
):
    """An ImportError on the estop gate must skip the misfire sweep's claim/fire."""
    import cron.jobs as jobs_mod

    monkeypatch.setattr(jobs_mod, "CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr(jobs_mod, "JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr(jobs_mod, "OUTPUT_DIR", tmp_path / "cron" / "output")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    estop._logged_components.clear()

    job = create_job(prompt="p", schedule="every 1h")
    _park_in_past(job["id"], minutes=30)
    parked_at = get_job(job["id"])["next_run_at"]
    provider = _RecordingProvider()
    _block_estop_import(monkeypatch)

    assert fire_overdue_jobs(provider) == 0
    assert provider.fired == []
    assert get_job(job["id"])["next_run_at"] == parked_at  # nothing claimed or re-armed


def test_misfire_fires_when_estop_importable_and_unpaused(tmp_path, monkeypatch):
    """Normal flow: importable gate + no sentinel → overdue job fires."""
    import cron.jobs as jobs_mod

    monkeypatch.setattr(jobs_mod, "CRON_DIR", tmp_path / "cron")
    monkeypatch.setattr(jobs_mod, "JOBS_FILE", tmp_path / "cron" / "jobs.json")
    monkeypatch.setattr(jobs_mod, "OUTPUT_DIR", tmp_path / "cron" / "output")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    estop._logged_components.clear()
    assert estop.is_engaged() is False

    job = create_job(prompt="p", schedule="every 1h")
    _park_in_past(job["id"], minutes=30)
    provider = _RecordingProvider()

    assert fire_overdue_jobs(provider) == 1
    assert provider.wait_fired()
    assert provider.fired == [job["id"]]
