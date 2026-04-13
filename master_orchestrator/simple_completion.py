"""Completion-state helpers shared by simple foreground loops and watchdogs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field


@dataclass
class SimpleCompletionSnapshot:
    completed_count: int = 0
    unresolved_count: int = 0
    unresolved_fingerprint: str = ""
    unresolved_categories: dict[str, int] = field(default_factory=dict)


@dataclass
class SimpleCompletionState:
    wave: int = 1
    last_completed_count: int = 0
    last_unresolved_count: int = 0
    last_failure_fingerprint: str = ""
    stagnant_waves: int = 0
    identical_failure_waves: int = 0


def count_failure_categories(items) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        category = item.attempt_state.last_error_category or "unknown"
        counts[category] = counts.get(category, 0) + 1
    return counts


def build_unresolved_fingerprint(items) -> str:
    rows = [
        (
            item.target,
            item.status.value,
            item.attempt_state.last_error_category or "",
        )
        for item in sorted(items, key=lambda current: current.target)
    ]
    raw = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16] if rows else ""


def build_completion_snapshot(*, completed_count: int, unresolved_items) -> SimpleCompletionSnapshot:
    unresolved = list(unresolved_items)
    return SimpleCompletionSnapshot(
        completed_count=completed_count,
        unresolved_count=len(unresolved),
        unresolved_fingerprint=build_unresolved_fingerprint(unresolved),
        unresolved_categories=count_failure_categories(unresolved),
    )


def track_completion_state(state: SimpleCompletionState, snapshot: SimpleCompletionSnapshot) -> None:
    made_progress = (
        snapshot.completed_count > state.last_completed_count
        or (
            state.last_unresolved_count > 0
            and snapshot.unresolved_count < state.last_unresolved_count
        )
    )
    if made_progress:
        state.stagnant_waves = 0
    else:
        state.stagnant_waves += 1

    if snapshot.unresolved_fingerprint and snapshot.unresolved_fingerprint == state.last_failure_fingerprint:
        state.identical_failure_waves += 1
    elif snapshot.unresolved_count > 0:
        state.identical_failure_waves = 1
    else:
        state.identical_failure_waves = 0

    state.last_completed_count = snapshot.completed_count
    state.last_unresolved_count = snapshot.unresolved_count
    state.last_failure_fingerprint = snapshot.unresolved_fingerprint


def completion_exit_reason(
    state: SimpleCompletionState,
    snapshot: SimpleCompletionSnapshot,
    *,
    max_retry_waves: int,
    max_stagnant_waves: int,
    max_identical_failure_waves: int,
) -> str | None:
    if snapshot.unresolved_count <= 0:
        return None
    if max_retry_waves > 0 and state.wave >= max_retry_waves:
        return (
            f"completion retry waves exhausted at {state.wave} with "
            f"{snapshot.unresolved_count} unresolved items"
        )
    if state.stagnant_waves >= max_stagnant_waves:
        return (
            f"no net progress for {state.stagnant_waves} terminal waves; "
            f"still unresolved={snapshot.unresolved_count}"
        )
    if (
        snapshot.unresolved_fingerprint
        and state.identical_failure_waves >= max_identical_failure_waves
    ):
        return (
            f"same unresolved failure set repeated for {state.identical_failure_waves} waves; "
            f"fingerprint={snapshot.unresolved_fingerprint}"
        )
    return None
