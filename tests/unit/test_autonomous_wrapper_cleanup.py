from __future__ import annotations

import pytest

from claude_orchestrator.autonomous import AutonomousController
from claude_orchestrator.exceptions import PreflightError


def test_autonomous_controller_no_longer_keeps_helper_wrapper_methods():
    removed_wrappers = {
        "_sync_budget_from_orchestrator",
        "_build_correction_feedback",
        "_apply_phase_timeout_multiplier",
        "_build_review_feedback",
        "_log_orchestrator_result_summary",
        "_collect_task_outputs_and_errors",
        "_update_task_duration_stats",
    }

    for method_name in removed_wrappers:
        assert not hasattr(AutonomousController, method_name), method_name


def test_run_pipeline_stage_returns_result_without_recording_diagnostic():
    controller = AutonomousController.__new__(AutonomousController)
    calls: list[dict] = []
    controller._record_diagnostic = lambda **kwargs: calls.append(kwargs)

    result = controller._run_pipeline_stage(lambda: "ok", stage="analysis", exit_prefix="analysis")

    assert result == "ok"
    assert calls == []


def test_run_pipeline_stage_records_diagnostic_and_reraises():
    controller = AutonomousController.__new__(AutonomousController)
    calls: list[dict] = []
    controller._record_diagnostic = lambda **kwargs: calls.append(kwargs)

    with pytest.raises(RuntimeError):
        controller._run_pipeline_stage(
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            stage="analysis",
            diagnostic_stage="project_analysis",
            exit_prefix="analysis",
        )

    assert len(calls) == 1
    assert calls[0]["stage"] == "project_analysis"
    assert calls[0]["exit_status"] == "analysis:RuntimeError"
    assert calls[0]["error_detail"] == "boom"
    assert "stack_trace" in calls[0]


def test_run_pipeline_stage_allows_passthrough_exception():
    controller = AutonomousController.__new__(AutonomousController)
    calls: list[dict] = []
    controller._record_diagnostic = lambda **kwargs: calls.append(kwargs)

    with pytest.raises(PreflightError):
        controller._run_pipeline_stage(
            lambda: (_ for _ in ()).throw(PreflightError("stop")),
            stage="preflight",
            exit_prefix="preflight",
            include_stack_trace=False,
            passthrough=(PreflightError,),
        )

    assert calls == []
