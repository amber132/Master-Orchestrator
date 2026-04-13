from __future__ import annotations

from datetime import datetime, timedelta

from claude_orchestrator.autonomous_helpers import compute_task_duration_stats
from claude_orchestrator.model import TaskResult, TaskStatus


def test_update_task_duration_stats_aggregates_finished_and_fallback_durations():
    started = datetime(2026, 4, 3, 10, 0, 0)
    results = {
        "timed": TaskResult(
            task_id="timed",
            status=TaskStatus.SUCCESS,
            started_at=started,
            finished_at=started + timedelta(seconds=4),
        ),
        "fallback": TaskResult(
            task_id="fallback",
            status=TaskStatus.SUCCESS,
            duration_seconds=2.0,
        ),
    }

    stats = compute_task_duration_stats(results)

    assert stats == (3.0, 4.0)


def test_update_task_duration_stats_ignores_non_positive_entries():
    started = datetime(2026, 4, 3, 10, 0, 0)
    results = {
        "zero": TaskResult(
            task_id="zero",
            status=TaskStatus.SUCCESS,
            started_at=started,
            finished_at=started,
        ),
        "missing": TaskResult(
            task_id="missing",
            status=TaskStatus.SUCCESS,
        ),
    }

    stats = compute_task_duration_stats(results)

    assert stats is None
