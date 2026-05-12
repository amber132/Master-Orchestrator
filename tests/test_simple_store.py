from __future__ import annotations

from datetime import datetime
from pathlib import Path

from claude_orchestrator.simple_model import (
    AttemptState,
    SimpleAttempt,
    SimpleErrorCategory,
    SimpleItemStatus,
    SimpleItemType,
    SimpleRun,
    SimpleValidationProfile,
    SimpleWorkItem,
    ValidationReport,
    ValidationStageResult,
)
from claude_orchestrator.simple_store import SimpleStore
from claude_orchestrator.store import Store


def test_simple_attempt_roundtrips_validation_and_telemetry(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    run = SimpleRun(
        instruction_template="annotate",
        working_dir=str(tmp_path),
        started_at=datetime.now(),
    )
    item = SimpleWorkItem(
        item_id="item-1",
        item_type=SimpleItemType.FILE,
        target="a.py",
        bucket="src",
        priority=0,
        instruction="annotate",
        attempt_state=AttemptState(attempt=1, max_attempts=3),
        validation_profile=SimpleValidationProfile(),
        timeout_seconds=30,
    )
    report = ValidationReport(
        passed=False,
        stage_results=[ValidationStageResult(name="target_touched", passed=False, details="b.py")],
        changed_files=["a.py", "b.py"],
        target_touched=True,
        target_exists_after=True,
        target_content_changed=True,
        unauthorized_changes=["b.py"],
        rollback_performed=True,
        recovered_unauthorized_changes=["b.py"],
        failure_code="unauthorized_side_files",
        failure_reason="unauthorized files modified",
        target_changed_files=["a.py"],
    )
    attempt = SimpleAttempt(
        item_id=item.item_id,
        attempt=1,
        status=SimpleItemStatus.FAILED,
        worker_id="worker-01",
        started_at=datetime.now(),
        finished_at=datetime.now(),
        exit_code=1,
        error_category=SimpleErrorCategory.UNAUTHORIZED_SIDE_FILES.value,
        failure_reason=report.failure_reason,
        changed_files=["a.py", "b.py"],
        validation_report=report,
        output="stdout",
        error="stderr",
        cost_usd=0.42,
        model_used="sonnet",
        pid=321,
        token_input=123,
        token_output=45,
        cli_duration_ms=678.9,
        tool_uses=7,
        turn_started=3,
        turn_completed=2,
        max_turns_exceeded=True,
    )

    with Store(db_path) as store:
        simple_store = SimpleStore(store)
        simple_store.create_run(run, [item])
        simple_store.save_attempt(run.run_id, item, attempt)

        loaded = store.get_simple_attempts(run.run_id)

    assert len(loaded) == 1
    restored = loaded[0]
    assert restored.validation_report is not None
    assert restored.validation_report.failure_code == "unauthorized_side_files"
    assert restored.validation_report.stage_results[0].name == "target_touched"
    assert restored.validation_report.target_exists_after is True
    assert restored.validation_report.target_content_changed is True
    assert restored.validation_report.recovered_unauthorized_changes == ["b.py"]
    assert restored.error_category == SimpleErrorCategory.UNAUTHORIZED_SIDE_FILES.value
    assert restored.token_input == 123
    assert restored.token_output == 45
    assert restored.cli_duration_ms == 678.9
    assert restored.tool_uses == 7
    assert restored.turn_started == 3
    assert restored.turn_completed == 2
    assert restored.max_turns_exceeded is True


def test_simple_run_heartbeat_roundtrips_and_counts_live_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    now = datetime.now()
    run = SimpleRun(
        instruction_template="annotate",
        working_dir=str(tmp_path),
        started_at=now,
        last_heartbeat_at=now,
    )

    with Store(db_path) as store:
        simple_store = SimpleStore(store)
        simple_store.create_run(run, [])

        loaded = simple_store.load_run(run.run_id)
        assert loaded is not None
        assert loaded.last_heartbeat_at is not None
        assert store.count_live_simple_runs(
            now.replace(year=now.year - 1),
            statuses=[loaded.status],
        ) == 1


def test_simple_store_create_run_bundle_persists_items_and_buckets(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    now = datetime.now()
    run = SimpleRun(
        instruction_template="annotate",
        working_dir=str(tmp_path),
        started_at=now,
        last_heartbeat_at=now,
    )
    items = [
        SimpleWorkItem(
            item_id="item-1",
            item_type=SimpleItemType.FILE,
            target="pkg/a.py",
            bucket="pkg",
            priority=1,
            instruction="annotate",
            attempt_state=AttemptState(max_attempts=3),
            validation_profile=SimpleValidationProfile(),
            timeout_seconds=30,
        ),
        SimpleWorkItem(
            item_id="item-2",
            item_type=SimpleItemType.FILE,
            target="tests/test_a.py",
            bucket="tests",
            priority=0,
            instruction="annotate",
            attempt_state=AttemptState(max_attempts=2),
            validation_profile=SimpleValidationProfile(),
            timeout_seconds=45,
        ),
    ]

    with Store(db_path) as store:
        simple_store = SimpleStore(store)
        simple_store.create_run(run, items)

        loaded_run = simple_store.load_run(run.run_id)
        loaded_items = simple_store.load_items(run.run_id)
        loaded_buckets = store.get_simple_buckets(run.run_id)
        run_row = store._conn.execute(
            "SELECT dag_name, status FROM runs WHERE run_id = ?",
            (run.run_id,),
        ).fetchone()

    assert loaded_run is not None
    assert loaded_run.working_dir == str(tmp_path)
    assert [item.item_id for item in loaded_items] == ["item-1", "item-2"]
    assert loaded_buckets["pkg"].total_items == 1
    assert loaded_buckets["tests"].total_items == 1
    assert run_row == ("simple:annotate", "running")


def test_simple_event_accessors_decode_json_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"

    with Store(db_path) as store:
        store.save_simple_event("run-1", "item_started", {"target": "a.py"}, item_id="item-1", bucket="src")

        events = store.get_simple_events("run-1")
        latest = store.get_latest_simple_event("run-1")

    assert len(events) == 1
    assert events[0]["data"] == {"target": "a.py"}
    assert latest is not None
    assert latest["data"] == {"target": "a.py"}
