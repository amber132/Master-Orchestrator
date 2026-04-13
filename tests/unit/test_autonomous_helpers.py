from __future__ import annotations

import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from pathlib import Path

import pytest

import claude_orchestrator.autonomous_helpers as autonomous_helpers
from claude_orchestrator.auto_model import (
    ContextNotebook,
    DeteriorationLevel,
    CorrectiveAction,
    FailureCategory,
    FailureClassification,
    GoalStatus,
    IterationHandoff,
    IterationRecord,
    Phase,
    PhaseStatus,
    ReviewResult,
    ReviewVerdict,
    TaskError,
)
from claude_orchestrator.claude_cli import BudgetTracker
from claude_orchestrator.autonomous_helpers import (
    apply_simple_goal_result,
    apply_phase_timeout_multiplier,
    build_correction_feedback,
    build_review_feedback,
    collect_task_outputs_and_errors,
    compute_task_duration_stats,
    infer_verification_commands,
    log_orchestrator_result_summary,
    resolve_surgical_verification_commands,
    sync_budget_from_orchestrator,
    sync_surgical_result_to_state,
)
from claude_orchestrator.model import DAG, TaskNode, TaskResult, TaskStatus


def test_build_correction_feedback_combines_sections():
    feedback = build_correction_feedback(
        correction_outputs={"fix-a": {"ok": True}},
        correction_errors=[TaskError(task_id="fix-b", error="boom", attempt=2)],
        architecture_summary="架构摘要",
    )

    assert "已预执行的确定性补救动作" in feedback
    assert "当前架构执行状态" in feedback
    assert "预执行补救失败" in feedback


def test_build_review_feedback_consumes_strategy_hint():
    phase = Phase(
        id="phase-1",
        name="Phase 1",
        description="desc",
        order=1,
        strategy_hint="先缩小范围",
        review_result=ReviewResult(
            phase_id="phase-1",
            verdict=ReviewVerdict.MAJOR_ISSUES,
            score=0.61,
            summary="phase review summary",
        ),
    )
    handoff = IterationHandoff(iteration=1, review_summary="handoff summary", review_score=0.72)

    feedback = build_review_feedback(
        phase,
        handoff=handoff,
        correction_feedback="补救反馈",
        learning_text="\n学习记忆",
        handoff_enabled=True,
        handoff_max_chars=4000,
    )

    assert "策略调整指令" in feedback
    assert "补救反馈" in feedback
    assert "handoff summary" in feedback
    assert feedback.endswith("学习记忆")
    assert phase.strategy_hint == ""


def test_apply_phase_timeout_multiplier_updates_positive_timeouts():
    phase = Phase(id="p1", name="Phase 1", description="desc", order=1, timeout_multiplier=1.5)
    dag = DAG(
        name="test",
        tasks={
            "a": TaskNode(id="a", prompt_template="x", timeout=10),
            "b": TaskNode(id="b", prompt_template="x", timeout=0),
        },
    )

    changed = apply_phase_timeout_multiplier(phase, dag)

    assert changed == 1
    assert dag.tasks["a"].timeout == 15
    assert dag.tasks["b"].timeout == 0


def test_collect_task_outputs_and_errors_uses_fallback_and_default_error():
    outputs, errors, empty_successes = collect_task_outputs_and_errors(
        {
            "task-1": TaskResult(task_id="task-1", status=TaskStatus.SUCCESS, output="raw", parsed_output=None),
            "task-2": TaskResult(task_id="task-2", status=TaskStatus.FAILED, error=None, attempt=3),
        },
        failed_task_default_error="修正任务失败",
    )

    assert outputs == {"task-1": "raw"}
    assert errors == [TaskError(task_id="task-2", error="修正任务失败", attempt=3)]
    assert empty_successes == 0


def test_compute_task_duration_stats_returns_aggregates():
    started = datetime(2026, 4, 3, 10, 0, 0)
    stats = compute_task_duration_stats(
        {
            "timed": TaskResult(
                task_id="timed",
                status=TaskStatus.SUCCESS,
                started_at=started,
                finished_at=started + timedelta(seconds=4),
            ),
            "fallback": TaskResult(task_id="fallback", status=TaskStatus.SUCCESS, duration_seconds=2.0),
        }
    )

    assert stats == (3.0, 4.0)


def test_log_orchestrator_result_summary_returns_distribution(caplog):
    phase = Phase(id="p1", name="Phase 1", description="desc", order=1)
    dag = DAG(name="test")
    dag.tasks = {"a": object(), "b": object(), "c": object()}
    results = {
        "a": TaskResult(task_id="a", status=TaskStatus.SUCCESS),
        "b": TaskResult(task_id="b", status=TaskStatus.FAILED),
    }

    with caplog.at_level(logging.INFO):
        summary = log_orchestrator_result_summary(
            phase,
            dag,
            results,
            lru_max_results=10,
        )

    assert summary == {"success": 1, "failed": 1}
    assert "结果数量(2) < DAG任务数量(3)" in caplog.text


def test_sync_budget_from_orchestrator_uses_higher_run_info_and_updates_state():
    budget = BudgetTracker(max_budget_usd=100.0)
    budget.spent = 1.2
    state = SimpleNamespace(total_cost_usd=1.2)
    orch = SimpleNamespace(_budget=SimpleNamespace(spent=0.4))
    run_info = SimpleNamespace(total_cost_usd=0.6)

    synced_cost = sync_budget_from_orchestrator(
        budget,
        orch,
        run_info=run_info,
        state=state,
        sync_state=True,
    )

    assert synced_cost == pytest.approx(0.6, abs=1e-6)
    assert budget.spent == pytest.approx(1.8, abs=1e-6)
    assert state.total_cost_usd == pytest.approx(1.8, abs=1e-6)


def test_infer_verification_commands_prefers_project_tools(tmp_path: Path):
    (tmp_path / "pyproject.toml").write_text("[project]\nname='demo'\n", encoding="utf-8")

    commands = infer_verification_commands(str(tmp_path))

    assert commands == [
        "python -m pytest -x --tb=short -q",
        "python -m ruff check .",
    ]


def test_resolve_surgical_verification_commands_uses_plan_then_fallback():
    plan = SimpleNamespace(commands=[SimpleNamespace(command="python -m pytest -q")])
    commands = resolve_surgical_verification_commands(
        auto_quality_commands=["ruff check ."],
        verification_plan=plan,
        working_dir="unused",
        infer_commands=lambda _cwd: ["fallback"],
    )

    assert commands == ["ruff check .", "python -m pytest -q"]

    fallback = resolve_surgical_verification_commands(
        auto_quality_commands=[],
        verification_plan=None,
        working_dir="workspace",
        infer_commands=lambda cwd: [f"infer:{cwd}"],
    )

    assert fallback == ["infer:workspace"]


def test_sync_surgical_result_to_state_updates_goal_status():
    state = SimpleNamespace(
        notebook=None,
        total_iterations=2,
        total_cost_usd=1.0,
        stop_reason="",
        status=GoalStatus.EXECUTING,
    )
    notebook = ContextNotebook(goal="goal", verification_commands=["pytest"])

    status = sync_surgical_result_to_state(
        state,
        notebook=notebook,
        iteration_count=3,
        budget_spent=4.5,
        result=SimpleNamespace(status=GoalStatus.FAILED, issues_fixed=1, summary="partial"),
    )

    assert status == GoalStatus.PARTIAL_SUCCESS
    assert state.notebook is notebook
    assert state.total_iterations == 5
    assert state.total_cost_usd == pytest.approx(4.5, abs=1e-6)
    assert state.stop_reason == "partial"
    assert state.status == GoalStatus.PARTIAL_SUCCESS


def test_apply_simple_goal_result_sets_success_phase_and_state():
    state = SimpleNamespace(phases=[], status=GoalStatus.EXECUTING)
    phase = apply_simple_goal_result(
        state,
        goal="create README",
        result=TaskResult(task_id="_simple_goal", status=TaskStatus.SUCCESS, output="done"),
    )

    assert phase.status == PhaseStatus.COMPLETED
    assert phase.task_outputs == {"_simple_goal": "done"}
    assert phase.task_result_statuses == {"_simple_goal": TaskStatus.SUCCESS.value}
    assert phase.review_result is not None
    assert state.phases == [phase]
    assert state.status == GoalStatus.CONVERGED


def test_apply_simple_goal_result_sets_failed_phase_and_state():
    state = SimpleNamespace(phases=[], status=GoalStatus.EXECUTING)
    phase = apply_simple_goal_result(
        state,
        goal="create README",
        result=TaskResult(task_id="_simple_goal", status=TaskStatus.FAILED, error="boom"),
    )

    assert phase.status == PhaseStatus.FAILED
    assert state.phases == [phase]
    assert state.status == GoalStatus.FAILED


def test_prepare_dag_generation_feedback_runs_corrections_and_combines_feedback():
    assert hasattr(autonomous_helpers, "prepare_dag_generation_feedback")

    phase = Phase(
        id="phase-1",
        name="Phase 1",
        description="desc",
        order=1,
        iteration=2,
        review_result=ReviewResult(
            phase_id="phase-1",
            verdict=ReviewVerdict.MINOR_ISSUES,
            score=0.55,
            summary="phase review summary",
        ),
    )
    handoff = IterationHandoff(
        iteration=1,
        review_summary="handoff summary",
        review_score=0.72,
        corrective_actions=[
            CorrectiveAction(
                action_id="fix-a",
                description="repair",
                prompt_template="do repair",
            )
        ],
        task_errors=[TaskError(task_id="prior-task", error="prior boom", attempt=1)],
    )
    captured: dict[str, object] = {}

    def execute_correction_dag(target_phase: Phase, review: ReviewResult) -> tuple[dict[str, object], list[TaskError]]:
        captured["phase"] = target_phase
        captured["review"] = review
        return (
            {"fix-a": {"ok": True}},
            [TaskError(task_id="fix-b", error="boom", attempt=2)],
        )

    def capture_architecture_summary(target_phase: Phase, outputs: dict[str, object]) -> str:
        captured["architecture_phase"] = target_phase
        captured["architecture_outputs"] = outputs
        return "架构摘要"

    def query_relevant_learnings(target_phase: Phase, prior_errors: list[TaskError]) -> str:
        captured["learning_phase"] = target_phase
        captured["learning_errors"] = prior_errors
        return "\n学习记忆"

    prepared = autonomous_helpers.prepare_dag_generation_feedback(
        phase,
        handoff=handoff,
        execute_correction_dag=execute_correction_dag,
        capture_architecture_summary=capture_architecture_summary,
        query_relevant_learnings=query_relevant_learnings,
        handoff_enabled=True,
        handoff_max_chars=4000,
    )

    retry_review = captured["review"]
    assert captured["phase"] is phase
    assert isinstance(retry_review, ReviewResult)
    assert retry_review.verdict == ReviewVerdict.MINOR_ISSUES
    assert retry_review.score == pytest.approx(0.72, abs=1e-6)
    assert retry_review.corrective_actions == handoff.corrective_actions
    assert retry_review.corrective_actions is not handoff.corrective_actions
    assert captured["architecture_phase"] is phase
    assert captured["architecture_outputs"] == {"fix-a": {"ok": True}}
    assert captured["learning_phase"] is phase
    assert captured["learning_errors"] == handoff.task_errors
    assert prepared.correction_outputs == {"fix-a": {"ok": True}}
    assert prepared.correction_errors == [TaskError(task_id="fix-b", error="boom", attempt=2)]
    assert prepared.correction_feedback
    assert prepared.learning_text == "\n学习记忆"
    assert "已预执行的确定性补救动作" in prepared.review_feedback
    assert "架构摘要" in prepared.review_feedback
    assert "handoff summary" in prepared.review_feedback
    assert prepared.review_feedback.endswith("学习记忆")


def test_prepare_dag_generation_feedback_skips_stale_handoff_corrections():
    assert hasattr(autonomous_helpers, "prepare_dag_generation_feedback")

    phase = Phase(
        id="phase-1",
        name="Phase 1",
        description="desc",
        order=1,
        iteration=1,
        review_result=ReviewResult(
            phase_id="phase-1",
            verdict=ReviewVerdict.MAJOR_ISSUES,
            score=0.48,
            summary="phase review summary",
        ),
    )
    handoff = IterationHandoff(
        iteration=1,
        review_summary="handoff summary",
        review_score=0.72,
        corrective_actions=[
            CorrectiveAction(
                action_id="fix-a",
                description="repair",
                prompt_template="do repair",
            )
        ],
    )
    events: list[str] = []

    def execute_correction_dag(target_phase: Phase, review: ReviewResult) -> tuple[dict[str, object], list[TaskError]]:
        events.append("execute")
        return {"fix-a": {"ok": True}}, []

    def capture_architecture_summary(target_phase: Phase, outputs: dict[str, object]) -> str:
        events.append("architecture")
        return "架构摘要"

    def query_relevant_learnings(target_phase: Phase, prior_errors: list[TaskError]) -> str:
        events.append("learning")
        return ""

    prepared = autonomous_helpers.prepare_dag_generation_feedback(
        phase,
        handoff=handoff,
        execute_correction_dag=execute_correction_dag,
        capture_architecture_summary=capture_architecture_summary,
        query_relevant_learnings=query_relevant_learnings,
        handoff_enabled=True,
        handoff_max_chars=4000,
    )

    assert events == ["learning"]
    assert prepared.correction_outputs == {}
    assert prepared.correction_errors == []
    assert prepared.correction_feedback == ""
    assert "handoff summary" in prepared.review_feedback


def test_collect_dag_run_artifacts_combines_budget_outputs_and_duration_stats(caplog):
    assert hasattr(autonomous_helpers, "collect_dag_run_artifacts")

    phase = Phase(id="phase-1", name="Phase 1", description="desc", order=1)
    started = datetime(2026, 4, 3, 12, 0, 0)
    orch = SimpleNamespace(
        results={
            "task-ok": TaskResult(
                task_id="task-ok",
                status=TaskStatus.SUCCESS,
                parsed_output={"done": True},
                started_at=started,
                finished_at=started + timedelta(seconds=4),
            ),
            "task-fail": TaskResult(
                task_id="task-fail",
                status=TaskStatus.FAILED,
                error="boom",
                attempt=2,
            ),
            "task-empty": TaskResult(
                task_id="task-empty",
                status=TaskStatus.SUCCESS,
                output="",
                parsed_output=None,
            ),
        },
        _lru_max_results=8,
        _budget=SimpleNamespace(spent=0.4),
    )
    budget = BudgetTracker(max_budget_usd=100.0)
    budget.spent = 1.0
    state = SimpleNamespace(total_cost_usd=1.0)
    run_info = SimpleNamespace(total_cost_usd=0.6)

    with caplog.at_level(logging.INFO):
        collected = autonomous_helpers.collect_dag_run_artifacts(
            phase,
            orch=orch,
            budget=budget,
            run_info=run_info,
            state=state,
            sync_state=True,
        )

    assert collected.task_outputs == {"task-ok": {"done": True}}
    assert collected.task_errors == [TaskError(task_id="task-fail", error="boom", attempt=2)]
    assert collected.empty_successes == 1
    assert collected.average_duration == pytest.approx(4.0, abs=1e-6)
    assert collected.max_duration == pytest.approx(4.0, abs=1e-6)
    assert budget.spent == pytest.approx(1.6, abs=1e-6)
    assert state.total_cost_usd == pytest.approx(1.6, abs=1e-6)
    assert "输出收集: task_outputs=1, task_errors=1, total_results=3" in caplog.text


def test_execute_orchestrator_run_builds_and_runs_orchestrator(monkeypatch, caplog):
    assert hasattr(autonomous_helpers, "execute_orchestrator_run")

    dag = DAG(
        name="test",
        tasks={"task-1": TaskNode(id="task-1", prompt_template="do work")},
    )
    created: dict[str, object] = {}
    run_info = SimpleNamespace(total_cost_usd=0.6)

    class FakeOrchestrator:
        def __init__(self, **kwargs):
            created["kwargs"] = kwargs
            self.results = {"task-1": TaskResult(task_id="task-1", status=TaskStatus.SUCCESS)}
            self.requested_exit_code = 0

        def run(self):
            created["ran"] = True
            return run_info

    monkeypatch.setattr(autonomous_helpers, "Orchestrator", FakeOrchestrator)

    with caplog.at_level(logging.INFO):
        executed = autonomous_helpers.execute_orchestrator_run(
            dag,
            config="cfg",
            store="store",
            working_dir="D:/tmp/work",
            log_file="orchestrator.log",
            pool_runtime="pool",
            on_task_result=lambda result: None,
            log_task_count_message="执行 %d 个修正任务",
        )

    assert created["ran"] is True
    kwargs = created["kwargs"]
    assert kwargs["dag"] is dag
    assert kwargs["config"] == "cfg"
    assert kwargs["store"] == "store"
    assert kwargs["working_dir"] == "D:/tmp/work"
    assert kwargs["log_file"] == "orchestrator.log"
    assert kwargs["pool_runtime"] == "pool"
    assert callable(kwargs["on_task_result"])
    assert executed.orchestrator.results == {"task-1": TaskResult(task_id="task-1", status=TaskStatus.SUCCESS)}
    assert executed.run_info is run_info
    assert "执行 1 个修正任务" in caplog.text


def test_collect_dag_run_artifacts_supports_correction_defaults(caplog):
    phase = Phase(id="phase-1", name="Phase 1", description="desc", order=1)
    orch = SimpleNamespace(
        results={
            "fix-fail": TaskResult(
                task_id="fix-fail",
                status=TaskStatus.FAILED,
                error=None,
                attempt=3,
            ),
        },
        _lru_max_results=4,
        _budget=SimpleNamespace(spent=0.2),
    )
    budget = BudgetTracker(max_budget_usd=100.0)

    with caplog.at_level(logging.INFO):
        collected = autonomous_helpers.collect_dag_run_artifacts(
            phase,
            orch=orch,
            budget=budget,
            failed_task_default_error="修正任务失败",
            budget_log_prefix="修正 DAG Budget 同步",
            log_result_summary=False,
        )

    assert collected.task_outputs == {}
    assert collected.task_errors == [TaskError(task_id="fix-fail", error="修正任务失败", attempt=3)]
    assert "修正 DAG Budget 同步" in caplog.text
    assert "DAG 执行完毕" not in caplog.text


def test_record_iteration_history_appends_trims_and_filters_phase_records():
    assert hasattr(autonomous_helpers, "record_iteration_history")

    phase = Phase(id="phase-2", name="Phase 2", description="desc", order=2)
    review = ReviewResult(
        phase_id="phase-2",
        verdict=ReviewVerdict.MAJOR_ISSUES,
        score=0.66,
        summary="needs work",
        corrective_actions=[
            CorrectiveAction(
                action_id="fix-a",
                description="repair A",
                prompt_template="do repair",
            )
        ],
    )
    history = [
        IterationRecord(iteration=1, phase_id="phase-1", score=0.4, verdict=ReviewVerdict.MINOR_ISSUES),
        IterationRecord(iteration=2, phase_id="phase-2", score=0.5, verdict=ReviewVerdict.MAJOR_ISSUES),
    ]
    classification = FailureClassification(
        category=FailureCategory.LOGIC_ERROR,
        retriable=True,
        feedback="retry",
    )

    updated = autonomous_helpers.record_iteration_history(
        history,
        total_iterations=3,
        phase=phase,
        review=review,
        task_errors=[TaskError(task_id="task-1", error="boom", attempt=2)],
        classification=classification,
        gate_passed=True,
        regression_detected=True,
        duration_seconds=12.5,
        max_entries=2,
    )

    assert len(history) == 2
    assert history[-1] is updated.record
    assert updated.record.iteration == 3
    assert updated.record.phase_id == "phase-2"
    assert updated.record.actions_taken == ["repair A"]
    assert updated.record.failure_category == FailureCategory.LOGIC_ERROR.value
    assert updated.record.gate_passed is True
    assert updated.record.regression_detected is True
    assert updated.record.task_error_count == 1
    assert updated.record.duration_seconds == pytest.approx(12.5, abs=1e-6)
    assert [record.phase_id for record in updated.phase_records] == ["phase-2", "phase-2"]


def test_detect_plateau_strategy_hint_returns_hint_after_two_stagnant_steps():
    assert hasattr(autonomous_helpers, "detect_plateau_strategy_hint")

    phase_records = [
        IterationRecord(iteration=1, phase_id="phase-1", score=0.61, verdict=ReviewVerdict.MAJOR_ISSUES),
        IterationRecord(iteration=2, phase_id="phase-1", score=0.615, verdict=ReviewVerdict.MAJOR_ISSUES),
        IterationRecord(iteration=3, phase_id="phase-1", score=0.612, verdict=ReviewVerdict.MAJOR_ISSUES),
    ]

    plateau = autonomous_helpers.detect_plateau_strategy_hint(phase_records)

    assert plateau is not None
    assert plateau.plateau_count == 2
    assert plateau.previous_score == pytest.approx(0.615, abs=1e-6)
    assert plateau.current_score == pytest.approx(0.612, abs=1e-6)
    assert "连续 2 轮分数无明显提升" in plateau.strategy_hint
    assert "(0.61)" in plateau.strategy_hint


def test_sync_iteration_record_deterioration_marks_gate_regression_when_needed():
    assert hasattr(autonomous_helpers, "sync_iteration_record_deterioration")

    none_record = IterationRecord(
        iteration=1,
        phase_id="phase-1",
        score=0.6,
        verdict=ReviewVerdict.MINOR_ISSUES,
        regression_detected=False,
    )
    autonomous_helpers.sync_iteration_record_deterioration(
        none_record,
        deterioration_level=DeteriorationLevel.NONE,
        regression_detected=True,
    )

    serious_record = IterationRecord(
        iteration=2,
        phase_id="phase-1",
        score=0.5,
        verdict=ReviewVerdict.MAJOR_ISSUES,
        regression_detected=False,
    )
    autonomous_helpers.sync_iteration_record_deterioration(
        serious_record,
        deterioration_level=DeteriorationLevel.SERIOUS,
        regression_detected=False,
    )

    assert none_record.regression_detected is False
    assert none_record.deterioration_level == "gate_regression"
    assert serious_record.regression_detected is True
    assert serious_record.deterioration_level == DeteriorationLevel.SERIOUS.value


def test_finalize_phase_execution_metrics_updates_phase_and_history():
    assert hasattr(autonomous_helpers, "finalize_phase_execution_metrics")

    phase = Phase(id="phase-1", name="Phase 1", description="desc", order=1, iteration=3)
    phase.status = PhaseStatus.COMPLETED
    phase.raw_tasks = [{"id": "task-1"}, {"id": "task-2"}]
    phase.review_result = ReviewResult(
        phase_id="phase-1",
        verdict=ReviewVerdict.PASS,
        score=0.91,
        summary="done",
    )
    phase.execution_metrics = {
        "started_at": "2026-04-03T09:00:00",
        "model_used": "sonnet",
    }
    state = SimpleNamespace(phase_history=[])
    finished_at = datetime(2026, 4, 3, 9, 30, 0)

    entry = autonomous_helpers.finalize_phase_execution_metrics(
        state,
        phase,
        finished_at=finished_at,
        duration_seconds=12.345,
    )

    assert phase.completed_at == finished_at
    assert phase.execution_metrics["finished_at"] == finished_at.isoformat()
    assert phase.execution_metrics["duration_seconds"] == pytest.approx(12.35, abs=1e-6)
    assert state.phase_history == [entry]
    assert entry["phase_id"] == "phase-1"
    assert entry["phase_name"] == "Phase 1"
    assert entry["iteration_count"] == 3
    assert entry["final_score"] == pytest.approx(0.91, abs=1e-6)
    assert entry["status"] == PhaseStatus.COMPLETED.value
    assert entry["task_count"] == 2
    assert entry["model_used"] == "sonnet"
