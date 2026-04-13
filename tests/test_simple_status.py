from __future__ import annotations

from claude_orchestrator.simple_model import (
    AttemptState,
    SimpleItemStatus,
    SimpleItemType,
    SimpleRun,
    SimpleWorkItem,
)
from claude_orchestrator.simple_status import build_simple_status_payload


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


def test_build_simple_status_payload_exposes_active_pipeline_counts() -> None:
    run = SimpleRun(instruction_template="annotate", working_dir="/tmp/repo")
    items = [
        _item("a", SimpleItemStatus.READY),
        _item("b", SimpleItemStatus.PREPARING),
        _item("c", SimpleItemStatus.EXECUTING),
        _item("d", SimpleItemStatus.VALIDATING),
        _item("e", SimpleItemStatus.SUCCEEDED),
    ]

    payload = build_simple_status_payload(run, None, items, [])

    assert payload["active_counts"] == {
        "ready": 1,
        "preparing": 1,
        "executing": 1,
        "validating": 1,
        "retry_wait": 0,
    }
    assert payload["terminal_counts"]["succeeded"] == 1


def test_build_simple_status_payload_preserves_manifest_execution_stats() -> None:
    run = SimpleRun(instruction_template="annotate", working_dir="/tmp/repo")
    items = [_item("a", SimpleItemStatus.SUCCEEDED)]
    manifest = {
        "execution_stats": {
            "attempts_total": 3,
            "tool_uses_total": 9,
            "token_input_total": 100,
        },
        "stage_timing_stats": {
            "prepare": {"count": 3, "avg_ms": 10.0, "max_ms": 12.0},
        },
    }

    payload = build_simple_status_payload(run, manifest, items, [])

    assert payload["manifest"]["execution_stats"]["attempts_total"] == 3
    assert payload["manifest"]["stage_timing_stats"]["prepare"]["avg_ms"] == 10.0
