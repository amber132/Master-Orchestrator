from __future__ import annotations

from claude_orchestrator.simple_model import AttemptState, SimpleItemStatus, SimpleItemType, SimpleWorkItem
from claude_orchestrator.simple_scheduler import SimpleScheduler


def _item(item_id: str, status: SimpleItemStatus) -> SimpleWorkItem:
    return SimpleWorkItem(
        item_id=item_id,
        item_type=SimpleItemType.FILE,
        target=f"{item_id}.py",
        bucket="src",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(max_attempts=3),
        timeout_seconds=30,
        status=status,
    )


def test_scheduler_does_not_requeue_failed_items() -> None:
    failed = _item("failed", SimpleItemStatus.FAILED)
    ready = _item("ready", SimpleItemStatus.READY)

    scheduler = SimpleScheduler([failed, ready], max_pending_tasks=10)
    chosen = scheduler.pop_ready(10)

    assert [item.item_id for item in chosen] == ["ready"]


def test_scheduler_marks_selected_items_as_preparing() -> None:
    ready = _item("ready", SimpleItemStatus.READY)

    scheduler = SimpleScheduler([ready], max_pending_tasks=10)
    chosen = scheduler.pop_ready(1)

    assert [item.item_id for item in chosen] == ["ready"]
    assert chosen[0].status == SimpleItemStatus.PREPARING
