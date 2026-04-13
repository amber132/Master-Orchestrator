from __future__ import annotations

from claude_orchestrator.null_objects import NullBlackboard, NullDriftDetector, NullQuarantine


def test_null_drift_detector_reports_no_drift() -> None:
    detector = NullDriftDetector()

    result = detector.detect("task-1", "original", "output")

    assert result.task_id == "task-1"
    assert result.similarity == 1.0
    assert result.drifted is False
    assert "Feature disabled" in result.detail


def test_null_blackboard_absorbs_writes_and_returns_empty_state() -> None:
    board = NullBlackboard()

    board.post("facts", "key", {"value": 1}, "task-1")
    board.subscribe("facts", lambda *_args: None)

    assert board.query() == []
    assert board.get_snapshot() == {
        "facts": [],
        "hypotheses": [],
        "intermediate_results": [],
    }


def test_null_quarantine_is_passthrough_and_stateless() -> None:
    quarantine = NullQuarantine()
    outputs = {"producer": {"ok": True}}

    assert quarantine.get_safe_output("task-1", outputs) is outputs
    quarantine.quarantine("task-1", "reason")
    quarantine.release("task-1")
    quarantine.clear()
    assert quarantine.is_quarantined("task-1") is False
    assert quarantine.get_quarantine_reason("task-1") is None
    assert quarantine.get_all_quarantined() == {}
