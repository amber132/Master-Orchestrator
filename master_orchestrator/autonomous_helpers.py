from __future__ import annotations

import logging
import os
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable

from .auto_model import (
    DeteriorationLevel,
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
from .claude_cli import BudgetTracker
from .model import DAG, TaskResult, TaskStatus
from .orchestrator import Orchestrator

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DagGenerationFeedback:
    """Structured feedback bundle prepared before DAG generation."""

    correction_outputs: dict[str, object] = field(default_factory=dict)
    correction_errors: list[TaskError] = field(default_factory=list)
    correction_feedback: str = ""
    learning_text: str = ""
    review_feedback: str | None = None


@dataclass(slots=True)
class DagRunArtifacts:
    """Collected artifacts derived from an orchestrator run."""

    task_outputs: dict[str, object] = field(default_factory=dict)
    task_errors: list[TaskError] = field(default_factory=list)
    empty_successes: int = 0
    average_duration: float | None = None
    max_duration: float | None = None


@dataclass(slots=True)
class OrchestratorExecution:
    """Executed orchestrator plus its run metadata."""

    orchestrator: Orchestrator
    run_info: object | None = None


@dataclass(slots=True)
class IterationHistoryUpdate:
    """Iteration history append result plus phase-local view."""

    record: IterationRecord
    phase_records: list[IterationRecord] = field(default_factory=list)


@dataclass(slots=True)
class PlateauStrategyHint:
    """Detected plateau metadata and suggested strategy hint."""

    plateau_count: int
    previous_score: float
    current_score: float
    strategy_hint: str


def apply_simple_goal_result(
    state: object,
    *,
    goal: str,
    result: TaskResult,
) -> Phase:
    """Apply a simple-goal task result onto GoalState-like state."""
    phase = Phase(
        id="simple_execution",
        name="简单目标直接执行",
        description=goal,
        order=1,
        raw_tasks=[{"id": "_simple_goal", "prompt": goal}],
    )
    if result.status == TaskStatus.SUCCESS:
        phase.status = PhaseStatus.COMPLETED
        phase.task_outputs = {"_simple_goal": result.output or ""}
        phase.task_result_statuses = {"_simple_goal": TaskStatus.SUCCESS.value}
        phase.review_result = ReviewResult(
            phase_id=phase.id,
            verdict=ReviewVerdict.PASS,
            score=0.8,
            summary="简单目标直接执行成功",
        )
        state.status = GoalStatus.CONVERGED
    else:
        phase.status = PhaseStatus.FAILED
        state.status = GoalStatus.FAILED
    state.phases = [phase]
    return phase


def infer_verification_commands(working_dir: str) -> list[str]:
    """Infer a minimal verification command list from the workspace shape."""
    commands: list[str] = []
    if (
        os.path.isfile(os.path.join(working_dir, "pytest.ini"))
        or os.path.isfile(os.path.join(working_dir, "pyproject.toml"))
        or os.path.isfile(os.path.join(working_dir, "setup.cfg"))
    ):
        commands.append("python -m pytest -x --tb=short -q")
    if (
        os.path.isfile(os.path.join(working_dir, "ruff.toml"))
        or os.path.isfile(os.path.join(working_dir, "pyproject.toml"))
    ):
        commands.append("python -m ruff check .")
    if not commands:
        commands.append("python -m py_compile claude_orchestrator/")
    return commands


def resolve_surgical_verification_commands(
    *,
    auto_quality_commands: list[str],
    verification_plan: object | None,
    working_dir: str | None,
    infer_commands: Callable[[str], list[str]] = infer_verification_commands,
) -> list[str]:
    """Assemble verification commands for surgical mode."""
    commands = list(auto_quality_commands)
    if verification_plan is not None:
        commands.extend(cmd.command for cmd in getattr(verification_plan, "commands", []))
    if not commands and working_dir:
        commands = infer_commands(working_dir)
    return commands


def sync_surgical_result_to_state(
    state: object,
    *,
    notebook: object,
    iteration_count: int,
    budget_spent: float,
    result: object,
) -> GoalStatus:
    """Apply a SurgicalController result back onto GoalState-like state."""
    state.notebook = notebook
    state.total_iterations += iteration_count
    state.total_cost_usd = budget_spent
    state.stop_reason = getattr(result, "summary", "")

    if getattr(result, "status", None) == GoalStatus.CONVERGED:
        state.status = GoalStatus.CONVERGED
    elif getattr(result, "issues_fixed", 0) > 0:
        state.status = GoalStatus.PARTIAL_SUCCESS
    else:
        state.status = GoalStatus.FAILED
    return state.status


def sync_budget_from_orchestrator(
    budget: BudgetTracker,
    orch: object,
    run_info: object | None = None,
    *,
    state: object | None = None,
    state_lock: Any = None,
    sync_state: bool = False,
    log_prefix: str = "Budget 同步",
) -> float:
    """Merge a child orchestrator budget into the parent budget tracker."""
    orch_budget = getattr(orch, "_budget", None)
    orch_spent = float(getattr(orch_budget, "spent", 0.0) or 0.0)
    run_info_cost = float(getattr(run_info, "total_cost_usd", 0.0) or 0.0)
    effective_cost = orch_spent

    if orch_spent > 0 or run_info_cost > 0:
        diff = abs(orch_spent - run_info_cost)
        if diff > 0.01:
            logger.warning(
                "Budget 交叉校验差异: orch._budget.spent=%.4f vs run_info.total_cost_usd=%.4f (diff=%.4f)",
                orch_spent, run_info_cost, diff,
            )
            effective_cost = max(orch_spent, run_info_cost)
            if effective_cost > orch_spent:
                logger.info(
                    "交叉校验修正: 使用 run_info 的较大值 %.4f 替代 %.4f",
                    effective_cost, orch_spent,
                )

    budget_before = budget.spent
    budget.add_spent(effective_cost)
    logger.info(
        "%s: orch._budget.spent=%.4f, run_info.total_cost_usd=%.4f, "
        "synced_cost=%.4f, auto._budget.spent %.4f -> %.4f",
        log_prefix,
        orch_spent,
        run_info_cost,
        effective_cost,
        budget_before,
        budget.spent,
    )

    if sync_state and state is not None:
        if state_lock is not None:
            with state_lock:
                state.total_cost_usd = budget.spent
        else:
            state.total_cost_usd = budget.spent

    return effective_cost


def build_correction_feedback(
    *,
    correction_outputs: dict[str, object],
    correction_errors: list[TaskError],
    architecture_summary: str = "",
) -> str:
    """Render correction execution results into prompt feedback text."""
    sections: list[str] = []

    if correction_outputs:
        executed_lines = [f"- `{task_id}`" for task_id in correction_outputs]
        sections.append("## 已预执行的确定性补救动作\n" + "\n".join(executed_lines))
        if architecture_summary:
            sections.append(f"## 当前架构执行状态\n{architecture_summary}")

    if correction_errors:
        failed_lines = [f"- `{err.task_id}`: {err.error[:200]}" for err in correction_errors[:5]]
        sections.append("## 预执行补救失败\n" + "\n".join(failed_lines))

    return "\n\n".join(sections)


def apply_phase_timeout_multiplier(phase: Phase, dag: DAG) -> int:
    """Apply a phase timeout multiplier to DAG task timeouts in place."""
    if phase.timeout_multiplier <= 1.0:
        return 0

    changed = 0
    for task in dag.tasks.values():
        if task.timeout:
            original = task.timeout
            task.timeout = int(task.timeout * phase.timeout_multiplier)
            changed += 1
            logger.debug("任务 '%s' 超时从 %ds 放大到 %ds", task.id, original, task.timeout)
    return changed


def build_review_feedback(
    phase: Phase,
    *,
    handoff: IterationHandoff | None,
    correction_feedback: str = "",
    learning_text: str = "",
    handoff_enabled: bool = True,
    handoff_max_chars: int = 4000,
) -> str | None:
    """Compose review feedback text for the DAG generator."""
    review_feedback = None

    if handoff and handoff_enabled:
        prompt_handoff = deepcopy(handoff)
        if correction_feedback:
            prompt_handoff.corrective_actions = []
        review_feedback = prompt_handoff.to_prompt_text(handoff_max_chars)
    elif phase.review_result:
        review_feedback = phase.review_result.summary

    if correction_feedback:
        review_feedback = (
            f"{correction_feedback}\n\n{review_feedback}" if review_feedback else correction_feedback
        )

    if phase.strategy_hint:
        strategy_prefix = f"\n\n## 策略调整指令\n{phase.strategy_hint}\n"
        review_feedback = (strategy_prefix + review_feedback) if review_feedback else strategy_prefix
        phase.strategy_hint = ""

    if learning_text:
        review_feedback = (review_feedback + learning_text) if review_feedback else learning_text

    return review_feedback


def prepare_dag_generation_feedback(
    phase: Phase,
    *,
    handoff: IterationHandoff | None,
    execute_correction_dag: Callable[[Phase, ReviewResult], tuple[dict[str, object], list[TaskError]]],
    capture_architecture_summary: Callable[[Phase, dict[str, object]], str],
    query_relevant_learnings: Callable[[Phase, list[TaskError]], str],
    handoff_enabled: bool = True,
    handoff_max_chars: int = 4000,
) -> DagGenerationFeedback:
    """Prepare correction outputs and prompt feedback before DAG generation."""
    correction_outputs: dict[str, object] = {}
    correction_errors: list[TaskError] = []
    correction_feedback = ""

    if handoff and handoff.corrective_actions and phase.iteration > handoff.iteration:
        retry_review = ReviewResult(
            phase_id=phase.id,
            verdict=phase.review_result.verdict if phase.review_result else ReviewVerdict.MAJOR_ISSUES,
            score=handoff.review_score,
            summary=handoff.review_summary,
            issues=deepcopy(handoff.review_issues),
            corrective_actions=deepcopy(handoff.corrective_actions),
        )
        correction_outputs, correction_errors = execute_correction_dag(phase, retry_review)
        if correction_outputs:
            correction_feedback = build_correction_feedback(
                correction_outputs=correction_outputs,
                correction_errors=correction_errors,
                architecture_summary=capture_architecture_summary(phase, correction_outputs) or "",
            )

    prior_errors = handoff.task_errors if handoff else []
    learning_text = query_relevant_learnings(phase, prior_errors)
    review_feedback = build_review_feedback(
        phase,
        handoff=handoff,
        correction_feedback=correction_feedback,
        learning_text=learning_text,
        handoff_enabled=handoff_enabled,
        handoff_max_chars=handoff_max_chars,
    )
    return DagGenerationFeedback(
        correction_outputs=correction_outputs,
        correction_errors=correction_errors,
        correction_feedback=correction_feedback,
        learning_text=learning_text,
        review_feedback=review_feedback,
    )


def execute_orchestrator_run(
    dag: DAG,
    *,
    config: object,
    store: object,
    working_dir: object,
    log_file: object,
    pool_runtime: object | None = None,
    on_task_result: Callable[[TaskResult], None] | None = None,
    log_task_count_message: str | None = None,
) -> OrchestratorExecution:
    """Create and run an orchestrator for a DAG."""
    if log_task_count_message:
        logger.info(log_task_count_message, len(dag.tasks))

    orchestrator = Orchestrator(
        dag=dag,
        config=config,
        store=store,
        working_dir=working_dir,
        log_file=log_file,
        pool_runtime=pool_runtime,
        on_task_result=on_task_result,
    )
    run_info = orchestrator.run()
    return OrchestratorExecution(orchestrator=orchestrator, run_info=run_info)


def record_iteration_history(
    history: list[IterationRecord],
    *,
    total_iterations: int,
    phase: Phase,
    review: ReviewResult,
    task_errors: list[TaskError],
    classification: FailureClassification | None = None,
    gate_passed: bool | None = None,
    regression_detected: bool = False,
    duration_seconds: float = 0.0,
    max_entries: int = 200,
) -> IterationHistoryUpdate:
    """Append one iteration record and return phase-local history slice."""
    actions_taken = [action.description for action in review.corrective_actions] if review.corrective_actions else []
    record = IterationRecord(
        iteration=total_iterations,
        phase_id=phase.id,
        score=review.score,
        verdict=review.verdict,
        actions_taken=actions_taken,
        failure_category=classification.category.value if classification else "",
        gate_passed=gate_passed,
        regression_detected=regression_detected,
        task_error_count=len(task_errors),
        duration_seconds=duration_seconds,
    )
    history.append(record)
    if len(history) > max_entries:
        del history[:-max_entries]
    return IterationHistoryUpdate(
        record=record,
        phase_records=[item for item in history if item.phase_id == phase.id],
    )


def detect_plateau_strategy_hint(
    phase_records: list[IterationRecord],
    *,
    delta_threshold: float = 0.01,
    min_plateau_count: int = 2,
) -> PlateauStrategyHint | None:
    """Detect sustained score stagnation and suggest a strategy shift."""
    if len(phase_records) < 2:
        return None

    last_score = phase_records[-1].score
    prev_score = phase_records[-2].score
    if abs(last_score - prev_score) >= delta_threshold:
        return None

    plateau_count = 0
    for index in range(len(phase_records) - 1, 0, -1):
        if abs(phase_records[index].score - phase_records[index - 1].score) < delta_threshold:
            plateau_count += 1
        else:
            break

    if plateau_count < min_plateau_count:
        return None

    strategy_hint = (
        f"[平台期策略调整] 连续 {plateau_count} 轮分数无明显提升 "
        f"({last_score:.2f})。请采用不同的实现策略：\n"
        f"1. 如果是代码修复，尝试从不同角度分析根因\n"
        f"2. 如果是功能实现，尝试简化方案或分步完成\n"
        f"3. 如果是测试/门禁问题，优先修复最关键的失败项\n"
        f"避免重复相同的修改模式。"
    )
    return PlateauStrategyHint(
        plateau_count=plateau_count,
        previous_score=prev_score,
        current_score=last_score,
        strategy_hint=strategy_hint,
    )


def sync_iteration_record_deterioration(
    record: IterationRecord,
    *,
    deterioration_level: DeteriorationLevel,
    regression_detected: bool,
) -> None:
    """Keep iteration record deterioration/regression fields consistent."""
    record.deterioration_level = deterioration_level.value
    if deterioration_level != DeteriorationLevel.NONE:
        record.regression_detected = True
    elif regression_detected:
        record.deterioration_level = "gate_regression"


def finalize_phase_execution_metrics(
    state: object,
    phase: Phase,
    *,
    finished_at: object,
    duration_seconds: float,
) -> dict[str, object]:
    """Persist final phase execution metrics and append one phase history entry."""
    phase.completed_at = finished_at
    phase.execution_metrics["finished_at"] = finished_at.isoformat()
    phase.execution_metrics["duration_seconds"] = round(duration_seconds, 2)
    phase_history_entry = {
        "phase_id": phase.id,
        "phase_name": phase.name,
        "started_at": phase.execution_metrics.get("started_at", ""),
        "finished_at": phase.execution_metrics.get("finished_at", ""),
        "duration_seconds": phase.execution_metrics.get("duration_seconds", 0),
        "iteration_count": phase.iteration,
        "final_score": phase.review_result.score if phase.review_result else 0.0,
        "status": phase.status.value,
        "task_count": len(phase.raw_tasks),
        "model_used": phase.execution_metrics.get("model_used", ""),
    }
    getattr(state, "phase_history").append(phase_history_entry)
    return phase_history_entry


def collect_dag_run_artifacts(
    phase: Phase,
    *,
    orch: object,
    budget: BudgetTracker,
    run_info: object | None = None,
    dag: DAG | None = None,
    state: object | None = None,
    state_lock: Any = None,
    sync_state: bool = False,
    failed_task_default_error: str | None = None,
    budget_log_prefix: str = "Budget 同步",
    log_result_summary: bool = True,
) -> DagRunArtifacts:
    """Collect outputs, errors, durations, and budget updates from an orchestrator run."""
    results = getattr(orch, "results", {}) or {}
    dag_for_logging = dag or getattr(orch, "dag", None)
    if log_result_summary and dag_for_logging is None:
        dag_for_logging = DAG(
            name=f"{phase.id}-results",
            tasks={task_id: object() for task_id in results},
        )

    if log_result_summary:
        log_orchestrator_result_summary(
            phase,
            dag_for_logging,
            results,
            lru_max_results=int(getattr(orch, "_lru_max_results", 0) or 0),
        )
    sync_budget_from_orchestrator(
        budget,
        orch,
        run_info=run_info,
        state=state,
        state_lock=state_lock,
        sync_state=sync_state,
        log_prefix=budget_log_prefix,
    )
    task_outputs, task_errors, empty_successes = collect_task_outputs_and_errors(
        results,
        failed_task_default_error=failed_task_default_error,
    )
    if empty_successes > 0:
        logger.warning(
            "阶段 '%s': %d 个 SUCCESS 任务的 parsed_output 和 output 均为空",
            phase.id, empty_successes,
        )
    logger.info(
        "阶段 '%s' 输出收集: task_outputs=%d, task_errors=%d, total_results=%d",
        phase.id, len(task_outputs), len(task_errors), len(results),
    )

    stats = compute_task_duration_stats(results)
    average_duration, max_duration = stats if stats else (None, None)
    return DagRunArtifacts(
        task_outputs=task_outputs,
        task_errors=task_errors,
        empty_successes=empty_successes,
        average_duration=average_duration,
        max_duration=max_duration,
    )


def log_orchestrator_result_summary(
    phase: Phase,
    dag: DAG,
    results: dict[str, TaskResult],
    *,
    lru_max_results: int,
) -> dict[str, int]:
    """Log orchestrator result distribution for diagnostics."""
    status_counts: dict[str, int] = {}
    for result in results.values():
        status_key = result.status.value if hasattr(result.status, "value") else str(result.status)
        status_counts[status_key] = status_counts.get(status_key, 0) + 1

    logger.info(
        "阶段 '%s' DAG 执行完毕: total_results=%d, status_distribution=%s, lru_max_results=%d",
        phase.id, len(results), status_counts, lru_max_results,
    )
    if len(results) < len(dag.tasks):
        logger.warning(
            "阶段 '%s' 结果数量(%d) < DAG任务数量(%d)，部分任务可能被 LRU 淘汰或未执行",
            phase.id, len(results), len(dag.tasks),
        )
    return status_counts


def collect_task_outputs_and_errors(
    results: dict[str, TaskResult],
    *,
    failed_task_default_error: str | None = None,
) -> tuple[dict[str, object], list[TaskError], int]:
    """Extract successful outputs plus structured task errors from orchestrator results."""
    task_outputs: dict[str, object] = {}
    task_errors: list[TaskError] = []
    empty_successes = 0

    for task_id, result in results.items():
        status_value = result.status.value if hasattr(result.status, "value") else str(result.status)
        if result.status == TaskStatus.SUCCESS:
            has_parsed = result.parsed_output is not None
            has_raw = result.output is not None and result.output != ""
            if not has_parsed and not has_raw:
                empty_successes += 1
                logger.warning(
                    "任务 '%s' SUCCESS 但 parsed_output=None 且 output 为空，将跳过收集",
                    task_id,
                )
                continue
            if result.parsed_output is None:
                logger.info(
                    "任务 '%s' SUCCESS 但 parsed_output=None（JSON解析失败或output_format非json），"
                    "回退到 raw output (len=%d)",
                    task_id, len(result.output) if result.output else 0,
                )
            task_outputs[task_id] = result.parsed_output or result.output
            logger.debug("收集成功输出: task='%s', output_type=%s", task_id, type(task_outputs[task_id]).__name__)
            continue

        if result.status == TaskStatus.FAILED:
            logger.debug("跳过失败任务: task='%s', error=%.200s", task_id, (result.error or "none")[:200])
            error_message = result.error or failed_task_default_error
            if error_message:
                task_errors.append(TaskError(
                    task_id=task_id,
                    error=error_message,
                    attempt=getattr(result, "attempt", 1),
                ))
            continue

        logger.debug("跳过非终态任务: task='%s', status=%s", task_id, status_value)

    return task_outputs, task_errors, empty_successes


def compute_task_duration_stats(results: dict[str, TaskResult]) -> tuple[float, float] | None:
    """Compute average and max task durations from results."""
    durations: list[float] = []

    for result in results.values():
        started = getattr(result, "started_at", None)
        finished = getattr(result, "finished_at", None)
        if started and finished:
            try:
                duration = (finished - started).total_seconds()
            except Exception:
                duration = 0.0
            if duration > 0:
                durations.append(duration)
                continue

        fallback_duration = getattr(result, "duration_seconds", 0.0) or 0.0
        if fallback_duration > 0:
            durations.append(fallback_duration)

    if not durations:
        return None

    return (sum(durations) / len(durations), max(durations))
