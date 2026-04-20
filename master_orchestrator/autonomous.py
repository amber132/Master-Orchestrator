"""Autonomous controller: drives the full goal -> decompose -> execute -> review -> iterate loop.

支持 GoalState 持久化：每个阶段完成后自动落盘，中断后可从文件恢复。
支持质量门禁：在 AI 审查前执行外部命令（测试/lint/build），用退出码硬判定。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import traceback
from copy import deepcopy
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime, timedelta
from pathlib import Path

# psutil 是可选依赖，不可用时跳过内存检查
try:
    import psutil
    _HAS_PSUTIL = True
except ImportError:
    psutil = None  # type: ignore[assignment]
    _HAS_PSUTIL = False

from .architecture_contract import (
    ArchitectureContract,
    load_architecture_contract,
    render_architecture_summary,
    save_architecture_contract,
)
from .architecture_execution import (
    ArchitectureExecutionReport,
    architecture_execution_report_from_dict,
    architecture_execution_report_to_dict,
    build_architecture_execution_report,
    load_architecture_execution_report,
    render_architecture_execution_summary,
    save_architecture_execution_report,
)
from .architecture_council import ArchitectureCouncil
from .architecture_trigger import ArchitectureTrigger, ArchitectureTriggerDecision
from .autonomous_helpers import (
    apply_simple_goal_result as _apply_simple_goal_result_helper,
    apply_phase_timeout_multiplier as _apply_phase_timeout_multiplier_helper,
    collect_dag_run_artifacts as _collect_dag_run_artifacts_helper,
    detect_plateau_strategy_hint as _detect_plateau_strategy_hint_helper,
    execute_orchestrator_run as _execute_orchestrator_run_helper,
    finalize_phase_execution_metrics as _finalize_phase_execution_metrics_helper,
    infer_verification_commands as _infer_verification_commands_helper,
    prepare_dag_generation_feedback as _prepare_dag_generation_feedback_helper,
    record_iteration_history as _record_iteration_history_helper,
    resolve_surgical_verification_commands as _resolve_surgical_verification_commands_helper,
    sync_surgical_result_to_state as _sync_surgical_result_to_state_helper,
    sync_iteration_record_deterioration as _sync_iteration_record_deterioration_helper,
)
from .auto_model import (
    AutoConfig,
    ComplexityEstimate,
    CorrectiveAction,
    DeteriorationLevel,
    DiagnosticEntry,
    FailureCategory,
    FailureClassification,
    GoalState,
    GoalStatus,
    IterationHandoff,
    Phase,
    PhaseStatus,
    QualityGateResult,
    RegressionBaseline,
    ReviewResult,
    ReviewVerdict,
    SafeStopReason,
    TaskError,
    load_goal_state,
    save_goal_state,
)
from .agent_cli import run_agent_task
from .claude_cli import BudgetTracker, bind_execution_lease_scope, run_claude_task, verify_cli_available
from .catastrophic_guard import CatastrophicGuard
from .closure_planner import ClosurePlanner
from .command_runtime import normalize_python_command
from .config import Config
from .convergence import ConvergenceDetector, DeteriorationDetector
from .dag_generator import DAGGenerator
from .evidence_graph import EvidenceGraphBuilder, save_evidence_graph
from .exceptions import PreflightError
from .exceptions import BudgetExhaustedError
from .execution_lease import ExecutionLeaseManager
from .failover_pool import PoolRuntime
from .goal_decomposer import GoalDecomposer
from .model import ControllerConfig, RunInfo, RunStatus, TaskNode, TaskStatus
from .repo_profile import RepoProfile
from .review_engine import ReviewEngine
from .runtime_layout import RuntimeLayout
from .heartbeat import Heartbeat
from .health_server import update_dashboard_state
from .store import Store
from .task_contract import TaskContract
from .verification_planner import VerificationPlan, VerificationPlanner
from .verification_runner import VerificationRunner

logger = logging.getLogger(__name__)

# ============================================================================
# GoalStatus 状态机定义
# ============================================================================
#
# 活跃状态（中间态，运行中可互相转换）:
#   INITIALIZING ──→ GATHERING ──→ DECOMPOSING ──→ EXECUTING ──→ REVIEWING ──→ ITERATING
#        │               │               │              │              │            │
#        │               │               │              │              │            └──→ EXECUTING（循环迭代）
#        │               │               │              │              │
#        │               │               │              │              └──→ DECIDING（审查裁决后）
#        │               │               │              │
#        │               │               │              └──→ DECOMPOSING（零阶段恢复时回退）
#
# 终态（一旦进入不再转换）:
#   CONVERGED          所有阶段完成且质量达标
#   PARTIAL_SUCCESS    超过半数阶段成功，但未完全收敛
#   FAILED             分解失败 / 执行失败 / 假收敛 / 评分持续偏低
#   SAFE_STOP          预算耗尽 / 超时 / 低评分触发安全停机
#   CATASTROPHIC_STOP  资源耗尽（磁盘/内存/进程）导致不可恢复
#   TIMEOUT            超过 deadline 时间限制
#   CANCELLED          用户 Ctrl+C 中断
#
# 转换触发条件:
#   → INITIALIZING     execute() 入口，项目分析开始                    [L959]
#   → GATHERING        需求收集阶段启动                                [L999]
#   → DECOMPOSING      目标分解阶段启动                                [L1383]
#   → EXECUTING        简单目标快速路径 / 并行执行阶段启动              [L1076, L1544]
#   → REVIEWING        质量门禁后 AI 审查阶段启动                      [L3006]
#   → ITERATING        审查裁决为继续迭代                              [L3322]
#   → CONVERGED        简单目标执行成功 / 所有阶段完成且有输出          [L1132, L4152]
#   → PARTIAL_SUCCESS  超过半数阶段完成                                [L4178]
#   → FAILED           分解异常 / 执行异常 / 假收敛 / 零阶段            [L1109, L1146, L1403, L1473, L1489, L1590, L1734, L1757, L4157, L4161, L4191]
#   → SAFE_STOP        预算/超时/低评分                                [L2856, L4141]
#   → CATASTROPHIC_STOP 磁盘满/内存溢出等资源灾难                      [L914, L2865, L4103]
#   → TIMEOUT          超过 deadline                                  [L4111]
#   → CANCELLED        KeyboardInterrupt                              [L1742]
#
# 约束:
#   1. REVIEWING 仅在 _execute_phase_with_review() 中设置，不得用于其他阶段
#   2. ITERATING 仅在审查裁决后设置，表示即将进入下一轮 EXECUTING
#   3. 终态之间不互相转换（_determine_final_status 仅在非终态时执行）
# ============================================================================

# GoalState 持久化文件名
_STATE_FILENAME = "goal_state.json"
_NON_REPLANNABLE_REPLAN_CATEGORIES = {
    FailureCategory.BLOCKED,
    FailureCategory.ENV_MISSING,
    FailureCategory.GOAL_PARSE_ERROR,
    FailureCategory.INIT_ERROR,
}
_TRANSIENT_FAILURE_KEYWORDS = [
    "timed out", "timeout", "超时",
    "connection reset", "connection refused", "connection closed",
    "502 bad gateway", "503 service unavailable", "504 gateway",
    "network is unreachable", "temporary failure",
    "too many requests", "429", "rate limit",
    "econnreset", "etimedout", "enotfound",
    "stream disconnected",
    "retrying sampling request",
]
_ENVIRONMENT_FAILURE_KEYWORDS = [
    "permissionerror",
    "permission denied",
    "access is denied",
    "拒绝访问",
    "[winerror 5]",
    "claude cli not found",
    "cli not found",
    "failed to register process",
    "the system cannot find the file specified",
    "no such file or directory",
    "is not recognized as an internal or external command",
    "command not found",
    "not executable",
]


def _goal_status_to_run_status(goal_status: GoalStatus) -> RunStatus:
    """将 GoalStatus 映射为 RunStatus，用于 Store.latest_run 与 goal_state.json 的状态同步。"""
    mapping = {
        GoalStatus.CONVERGED: RunStatus.COMPLETED,
        GoalStatus.PARTIAL_SUCCESS: RunStatus.COMPLETED,
        GoalStatus.FAILED: RunStatus.FAILED,
        GoalStatus.CATASTROPHIC_STOP: RunStatus.FAILED,
        GoalStatus.SAFE_STOP: RunStatus.CANCELLED,
        GoalStatus.CANCELLED: RunStatus.CANCELLED,
        GoalStatus.TIMEOUT: RunStatus.FAILED,
    }
    return mapping.get(goal_status, RunStatus.RUNNING)


class _PoolSwitchRequested(RuntimeError):
    def __init__(self, exit_code: int):
        super().__init__(f"pool switch requested (exit_code={exit_code})")
        self.exit_code = exit_code


def validate_goal_length(goal: str, min_length: int = 10) -> tuple[bool, str]:
    """检查 goal 文本长度是否满足最低要求。

    Returns:
        (True, '') 如果长度足够;
        (False, 'Goal too short') 如果长度不足。
    """
    if len(goal.strip()) < min_length:
        return False, 'Goal too short'
    return True, ''


class AutonomousController:
    """Top-level controller that autonomously decomposes, executes, reviews, and iterates."""

    def __init__(
        self,
        goal: str | ControllerConfig = "",
        working_dir: str | None = None,
        config: Config | None = None,
        auto_config: AutoConfig | None = None,
        store: Store | None = None,
        log_file: str | None = None,
        resume: bool = False,
        gather_enabled: bool = False,
        gather_mode: str = "interactive",
        gather_max_rounds: int = 3,
        gather_file: str | None = None,
        task_contract: TaskContract | None = None,
        repo_profile: RepoProfile | None = None,
        runtime_layout: RuntimeLayout | None = None,
        verification_plan: VerificationPlan | None = None,
        architecture_contract: ArchitectureContract | None = None,
        backup_manifest: object | None = None,
        pool_runtime: PoolRuntime | None = None,
    ):
        # 兼容双入口：接受 ControllerConfig 或逐参数构造
        if isinstance(goal, ControllerConfig):
            cfg = goal
        else:
            cfg = ControllerConfig(
                goal=goal,
                working_dir=working_dir,
                config=config,
                auto_config=auto_config,
                store=store,
                log_file=log_file,
                resume=resume,
                gather_enabled=gather_enabled,
                gather_mode=gather_mode,
                gather_max_rounds=gather_max_rounds,
                gather_file=gather_file,
                task_contract=task_contract,
                repo_profile=repo_profile,
                runtime_layout=runtime_layout,
                verification_plan=verification_plan,
                architecture_contract=architecture_contract,
                backup_manifest=backup_manifest,
                pool_runtime=pool_runtime,
            )

        # 从 cfg 解包基本参数（cfg 中各字段已提供默认值，此处断言必要字段）
        assert cfg.goal is not None, "ControllerConfig.goal 不能为 None"
        assert cfg.working_dir is not None, "ControllerConfig.working_dir 不能为 None"
        assert cfg.config is not None, "ControllerConfig.config 不能为 None"

        self._goal = cfg.goal
        self._working_dir = cfg.working_dir
        self._explicit_mode = getattr(cfg, 'explicit_mode', '') or ''  # 策略选择器显式覆盖
        self._task_contract = cfg.task_contract
        self._repo_profile = cfg.repo_profile
        self._runtime_layout = cfg.runtime_layout
        self._verification_plan = cfg.verification_plan
        self._architecture_contract = cfg.architecture_contract
        self._backup_manifest = cfg.backup_manifest
        self._pool_runtime = cfg.pool_runtime
        self.requested_exit_code: int = 0
        self._config = cfg.config
        self._auto_config = cfg.auto_config or AutoConfig()
        self._owns_store = cfg.store is None
        self._store = cfg.store or Store(cfg.config.checkpoint.db_path)
        self._log_file = cfg.log_file
        self._resume = cfg.resume if cfg.resume is not None else False
        self._preferred_provider = cfg.preferred_provider or "auto"
        self._phase_provider_overrides = dict(cfg.phase_provider_overrides or {})
        # CRITICAL 恶化时用于 GoalState 级回滚的最近健康快照
        self._last_healthy_snapshot: dict | None = None

        # 4 个工厂方法依次构建
        self._create_components(cfg)
        self._restore_state(cfg)

        # 防御性初始化：从旧格式状态文件恢复时 failure_categories 可能缺失
        if not hasattr(self._state, 'failure_categories') or self._state.failure_categories is None:
            self._state.failure_categories = {}

        self._restore_handoffs()
        self._restore_baselines()

        # 恢复增强 goal：如果需求收集已完成且目标被增强过，从 spec 恢复
        if (self._state.requirement_spec
                and self._state.requirement_spec.sufficiency_verdict != "sufficient"
                and self._state.requirement_spec.scope):
            self._goal = self._state.requirement_spec.to_enhanced_goal()
            self._state.goal_text = self._goal
            logger.info("从 requirement_spec 恢复增强 goal (%d 字符)", len(self._goal))

        if self._architecture_contract is None and self._state.architecture_contract_path:
            contract_path = Path(self._state.architecture_contract_path)
            if contract_path.exists():
                try:
                    self._architecture_contract = load_architecture_contract(contract_path)
                except Exception as e:
                    logger.warning("架构合同恢复失败: %s", e)

        if self._runtime_layout:
            self._state.runtime_dir = str(self._runtime_layout.root)
            self._state.workspace_dir = str(Path(self._working_dir).resolve())
            self._state.handoff_dir = str(self._runtime_layout.handoff)
        if self._pool_runtime is not None:
            self._state.active_profile = self._pool_runtime.active_profile
            self._state.pool_state = self._pool_runtime.state.to_dict()
        if self._backup_manifest is not None and getattr(self._backup_manifest, "summary", ""):
            self._state.backup_summary = self._backup_manifest.summary

        # 需求收集配置
        self._gather_enabled = cfg.gather_enabled if cfg.gather_enabled is not None else False
        self._gather_mode = cfg.gather_mode or "interactive"
        self._gather_max_rounds = cfg.gather_max_rounds if cfg.gather_max_rounds is not None else 3
        self._gather_file = cfg.gather_file

        # 初始化需求收集器（延迟导入，避免循环依赖）
        self._gatherer = None
        if self._gather_enabled:
            from .requirement_gatherer import RequirementGatherer
            self._gatherer = RequirementGatherer(
                claude_config=self._config.claude,
                limits_config=self._config.limits,
                requirement_config=self._config.requirement,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
                gather_mode=self._gather_mode,
                gather_file=self._gather_file,
                max_rounds=self._gather_max_rounds,
            )

        # 追踪已写入的诊断条目数量，避免重复写入
        self._last_diagnostics_written: int = 0

        # 心跳
        self._heartbeat = Heartbeat()

        # 通知器（由 _pipeline_preflate 初始化，finally 中清理）
        self._notifier: object | None = None

        # 线程锁：保护共享状态的并发写入
        self._state_lock = threading.Lock()

        # 资源检查节流：至少间隔 60 秒
        self._last_resource_check: float = 0.0

        # Store 实时同步：记录 run_id 和上次同步时间戳（节流 30 秒）
        self._run_id_for_sync: str | None = None
        self._last_store_sync_ts: float = 0.0
        if self._owns_store:
            try:
                _latest = self._store.get_latest_run()
                if _latest:
                    self._run_id_for_sync = _latest.run_id
            except Exception:
                pass

    # ── 工厂方法 ──────────────────────────────────────────────────────

    def _create_components(self, cfg: ControllerConfig) -> None:
        """创建核心组件：BudgetTracker、Decomposer、DAGGenerator、ReviewEngine 等。"""
        # 持久化到 working_dir 下的 budget_tracker.json
        _budget_persist = str(Path(cfg.working_dir or ".") / "budget_tracker.json")
        self._budget = BudgetTracker(
            cfg.config.claude.max_budget_usd,
            persist_path=_budget_persist,
            enforcement_mode=cfg.config.claude.budget_enforcement_mode,
        )
        self._execution_lease_manager = self._build_execution_lease_manager()

        self._decomposer = GoalDecomposer(
            claude_config=cfg.config.claude,
            limits_config=cfg.config.limits,
            auto_config=self._auto_config,
            budget_tracker=self._budget,
            working_dir=cfg.working_dir,
            provider_config=cfg.config,
            preferred_provider=self._preferred_provider,
            phase_provider_overrides=self._phase_provider_overrides,
        )
        self._dag_generator = DAGGenerator(
            self._auto_config,
            max_parallel=cfg.config.orchestrator.max_parallel if cfg.config else 200,
        )
        self._review_engine = ReviewEngine(
            claude_config=cfg.config.claude,
            limits_config=cfg.config.limits,
            auto_config=self._auto_config,
            budget_tracker=self._budget,
            working_dir=cfg.working_dir,
            provider_config=cfg.config,
            preferred_provider=self._preferred_provider,
            phase_provider_overrides=self._phase_provider_overrides,
        )
        self._architecture_trigger = ArchitectureTrigger()
        self._architecture_council = ArchitectureCouncil(
            claude_config=cfg.config.claude,
            limits_config=cfg.config.limits,
            budget_tracker=self._budget,
            working_dir=cfg.working_dir,
        )
        self._closure_planner = ClosurePlanner()
        self._catastrophic_guard = CatastrophicGuard(cfg.config.limits)
        self._convergence = ConvergenceDetector(self._auto_config)

        # 回归基线：phase_id -> RegressionBaseline
        self._baselines: dict[str, RegressionBaseline] = {}

        # Handoff Protocol：phase_id -> 上次迭代的 Handoff 包
        self._last_handoff: dict[str, IterationHandoff] = {}
        self._replan_fingerprint_attempts: dict[str, int] = {}

        # 恶化检测器
        self._deterioration = DeteriorationDetector(self._auto_config)

    def _restore_state(self, cfg: ControllerConfig) -> None:
        """恢复或新建 GoalState。"""
        self._state_path = (
            self._runtime_layout.state / _STATE_FILENAME
            if self._runtime_layout is not None
            else Path(self._working_dir) / _STATE_FILENAME
        )

        if self._resume and self._state_path.exists():
            self._state = load_goal_state(self._state_path)
            # 从 safe_stop 恢复时，清除停止原因以允许继续执行
            self._state.safe_stop_reason = SafeStopReason.UNKNOWN
            self._budget.spent = self._state.total_cost_usd
            logger.info(
                "从 %s 恢复状态: goal_id=%s, 阶段=%d, 已完成迭代=%d, 已花费=$%.2f",
                self._state_path, self._state.goal_id,
                len(self._state.phases), self._state.total_iterations,
                self._state.total_cost_usd,
            )
        else:
            self._state = GoalState(
                goal_text=self._goal,
                deadline=datetime.now() + timedelta(hours=self._auto_config.max_hours),
            )

    def _restore_handoffs(self) -> None:
        """从 SQLite 恢复 Handoff 数据。"""
        if not (self._resume and self._state_path and self._state_path.exists()):
            return

        try:
            all_handoffs = self._store.get_all_handoffs()
            for phase_id, data_json in all_handoffs.items():
                data = json.loads(data_json)
                from .auto_model import ReviewIssue
                self._last_handoff[phase_id] = IterationHandoff(
                    iteration=data.get("iteration", 0),
                    review_summary=data.get("review_summary", ""),
                    review_issues=[
                        ReviewIssue(
                            severity=i.get("severity", "minor"),
                            category=i.get("category", ""),
                            description=i.get("description", ""),
                            affected_files=i.get("affected_files", []),
                            suggested_fix=i.get("suggested_fix", ""),
                        )
                        for i in data.get("review_issues", [])
                    ],
                    review_score=data.get("review_score", 0.0),
                    corrective_actions=[
                        CorrectiveAction(
                            action_id=item.get("action_id", ""),
                            description=item.get("description", ""),
                            prompt_template=item.get("prompt_template", ""),
                            priority=item.get("priority", 1),
                            depends_on_actions=item.get("depends_on_actions", []),
                            timeout=item.get("timeout", 1800),
                            action_type=item.get("action_type", "claude_cli"),
                            executor_config=item.get("executor_config"),
                        )
                        for item in data.get("corrective_actions", [])
                        if item.get("action_id")
                    ],
                    failure_category=data.get("failure_category", ""),
                    failure_feedback=data.get("failure_feedback", ""),
                    task_errors=[
                        TaskError(
                            task_id=e.get("task_id", ""),
                            error=e.get("error", ""),
                            attempt=e.get("attempt", 1),
                        )
                        for e in data.get("task_errors", [])
                    ],
                    gate_summary=data.get("gate_summary", ""),
                    gate_failed_commands=data.get("gate_failed_commands", []),
                    regression_detected=data.get("regression_detected", False),
                    regressed_commands=data.get("regressed_commands", []),
                    score_trend=data.get("score_trend", []),
                    trend_direction=data.get("trend_direction", ""),
                    architecture_execution_summary=data.get("architecture_execution_summary", ""),
                    architecture_gate_status=data.get("architecture_gate_status", ""),
                    architecture_unmet_cutover_gates=data.get("architecture_unmet_cutover_gates", []),
                    architecture_missing_evidence_refs=data.get("architecture_missing_evidence_refs", []),
                    architecture_missing_rollback_refs=data.get("architecture_missing_rollback_refs", []),
                    architecture_report_path=data.get("architecture_report_path", ""),
                )
            if all_handoffs:
                logger.info("恢复了 %d 个 Handoff 包", len(all_handoffs))
        except Exception as e:
            logger.warning("Handoff 恢复失败: %s", e)

    def _restore_baselines(self) -> None:
        """从 SQLite 恢复回归基线。"""
        if not (self._resume and self._state_path and self._state_path.exists()):
            return

        try:
            all_context = self._store.get_all_context(self._state.goal_id)
            for key, value in all_context.items():
                if key.startswith("_baseline_"):
                    phase_id = key[len("_baseline_"):]
                    baseline_data = json.loads(value)
                    self._baselines[phase_id] = RegressionBaseline(
                        phase_id=baseline_data.get("phase_id", phase_id),
                        passed_commands=baseline_data.get("passed_commands", []),
                        score=baseline_data.get("score", 0.0),
                    )
            if self._baselines:
                logger.info("恢复了 %d 个回归基线", len(self._baselines))
        except Exception as e:
            logger.warning("回归基线恢复失败: %s", e)

    def _build_execution_lease_manager(self) -> ExecutionLeaseManager | None:
        max_processes = max(0, self._auto_config.max_execution_processes)
        if max_processes <= 0:
            return None

        lease_db_path = (self._auto_config.execution_lease_db_path or "").strip()
        if lease_db_path:
            db_path = Path(lease_db_path).resolve()
        else:
            db_path = (Path(self._config.workspace.root_dir).resolve() / "auto_execution_leases.sqlite3")
        db_path.parent.mkdir(parents=True, exist_ok=True)
        manager = ExecutionLeaseManager(
            db_path,
            max_leases=max_processes,
            ttl_seconds=self._auto_config.execution_lease_ttl_seconds,
        )
        logger.info(
            "启用 auto 真实执行并发控制: max_execution_processes=%d db=%s ttl=%ds",
            max_processes,
            db_path,
            self._auto_config.execution_lease_ttl_seconds,
        )
        return manager

    @property
    def state(self) -> GoalState:
        return self._state

    def _save_state(self) -> None:
        """持久化当前 GoalState 到文件（线程安全）。"""
        with self._state_lock:
            try:
                _diff = abs(self._budget.spent - self._state.total_cost_usd)
                if _diff > 0.01:
                    logger.warning(
                        "Budget drift: tracker=%.4f state=%.4f diff=%.4f",
                        self._budget.spent,
                        self._state.total_cost_usd,
                        _diff,
                    )
                self._state.total_cost_usd = self._budget.spent
                if self._pool_runtime is not None:
                    self._state.active_profile = self._pool_runtime.active_profile
                    self._state.pool_state = self._pool_runtime.state.to_dict()
                save_goal_state(self._state, self._state_path)
                logger.debug("GoalState 已保存到 %s", self._state_path)
            except Exception as e:
                logger.warning("GoalState 保存失败: %s", e)

            # 如果有诊断日志，追加写入 diagnostics.jsonl
            if len(self._state.diagnostics) > self._last_diagnostics_written:
                try:
                    diagnostics_path = self._state_path.parent / 'diagnostics.jsonl'

                    # 日志轮转：超过 10MB 时重命名为 .old
                    _DIAG_MAX = 10 * 1024 * 1024
                    if diagnostics_path.exists():
                        try:
                            if diagnostics_path.stat().st_size > _DIAG_MAX:
                                old_path = diagnostics_path.with_suffix('.jsonl.old')
                                if old_path.exists():
                                    old_path.unlink()
                                diagnostics_path.rename(old_path)
                                logger.info("诊断日志轮转 (>10MB)")
                        except Exception as e:
                            logger.warning("诊断日志轮转失败: %s", e)

                    new_entries = self._state.diagnostics[self._last_diagnostics_written:]
                    with open(diagnostics_path, 'a', encoding='utf-8') as f:
                        for entry in new_entries:
                            record = {
                                'stage': entry.stage,
                                'entered_at': entry.entered_at.isoformat(),
                                'exit_status': entry.exit_status,
                                'error_detail': entry.error_detail,
                                'duration_seconds': entry.duration_seconds if entry.duration_seconds is not None else 0.0,
                                'stack_trace': entry.stack_trace,
                            }
                            f.write(json.dumps(record, ensure_ascii=False) + '\n')
                    self._last_diagnostics_written = len(self._state.diagnostics)
                    logger.debug("诊断日志已追加到 %s (%d 条新记录)", diagnostics_path, len(new_entries))
                except Exception as e:
                    logger.warning("诊断日志写入失败: %s", e)

            # Store 实时状态同步：将 GoalStatus 映射到 RunStatus 并写入 Store
            # 仅在 owns_store 且节流间隔（30秒）满足时执行
            if self._owns_store and self._run_id_for_sync:
                try:
                    _now = time.monotonic()
                    if _now - self._last_store_sync_ts >= 30.0:
                        _mapped = _goal_status_to_run_status(self._state.status)
                        self._store.update_run_status(
                            self._run_id_for_sync, _mapped,
                            cost=self._state.total_cost_usd,
                        )
                        self._last_store_sync_ts = _now
                        logger.debug(
                            "Store 状态实时同步: run_id=%s, status=%s, cost=%.4f",
                            self._run_id_for_sync, _mapped.value,
                            self._state.total_cost_usd,
                        )
                except Exception as _sync_err:
                    logger.debug("Store 实时同步失败（非致命）: %s", _sync_err)

    def _record_failure_history(self, error: str, category: str) -> None:
        """追加失败记录到 failure_history.json（基于 goal 内容 hash）。

        每次调用追加一条记录，包含时间戳、错误信息、goal SHA256 hash 和失败分类。
        使用原子写入（tmp → rename）确保数据完整性。
        """
        try:
            goal_hash = hashlib.sha256(self._goal.encode("utf-8")).hexdigest()
            record = {
                "timestamp": datetime.now().isoformat(),
                "error": error,
                "goal_hash": goal_hash,
                "category": category,
            }
            history_path = self._state_path.parent / "failure_history.json"

            # 读取已有历史
            history = []
            if history_path.exists():
                try:
                    with open(history_path, 'r', encoding='utf-8') as f:
                        history = json.load(f)
                except (json.JSONDecodeError, OSError):
                    history = []

            history.append(record)

            # 原子写入：先写临时文件再重命名
            tmp_path = history_path.with_suffix('.json.tmp')
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            tmp_path.replace(history_path)

            logger.debug(
                "失败历史已记录: category=%s, goal_hash=%s..%s",
                category, goal_hash[:8], goal_hash[-4:],
            )
        except Exception as e:
            logger.warning("记录失败历史失败: %s", e)

    def _record_diagnostic(
        self,
        stage: str,
        exit_status: str = 'ok',
        error_detail: str = '',
        start_time: datetime | None = None,
        stack_trace: str = '',
    ) -> None:
        """记录诊断条目到状态中（线程安全）。

        Args:
            stage: 阶段名称
            exit_status: 退出状态，'ok' 表示正常
            error_detail: 错误详情
            start_time: 阶段开始时间，用于计算持续时间
            stack_trace: 完整堆栈跟踪信息
        """
        now = datetime.now()
        duration = (now - start_time).total_seconds() if start_time else 0.0

        entry = DiagnosticEntry(
            stage=stage,
            entered_at=start_time or now,
            exit_status=exit_status,
            error_detail=error_detail,
            duration_seconds=duration,
            stack_trace=stack_trace,
        )

        with self._state_lock:
            self._state.diagnostics.append(entry)

            # 防止 diagnostics 无限增长，保留最近 1000 条
            _MAX_DIAGNOSTICS = 1000
            if len(self._state.diagnostics) > _MAX_DIAGNOSTICS:
                self._state.diagnostics = self._state.diagnostics[-_MAX_DIAGNOSTICS:]

            # 如果状态不是 ok，更新失败分类计数并追加失败历史
            if exit_status != 'ok':
                self._state.failure_categories[exit_status] = (
                    self._state.failure_categories.get(exit_status, 0) + 1
                )
                self._record_failure_history(
                    error=error_detail or f"stage={stage} status={exit_status}",
                    category=exit_status,
                )

    def _ensure_failure_categories(self, context: str = '') -> None:
        """安全网：确保 FAILED/PARTIAL_SUCCESS 状态下 failure_categories 非空。

        统一提取自 execute() finally 块和 _finalize_state() 中的重复逻辑。
        必须在 _state_lock 外调用（调用方自行持有锁或不需要锁）。
        """
        if self._state.status not in (GoalStatus.FAILED, GoalStatus.PARTIAL_SUCCESS):
            return
        if self._state.failure_categories:
            return
        cat = 'unclassified_failure' if self._state.status == GoalStatus.FAILED else 'partial_success_no_category'
        self._state.failure_categories[cat] = 1
        suffix = f' ({context})' if context else ''
        logger.warning('Safety net: %s 状态缺少 failure_categories，已补充 %s%s', self._state.status.value, cat, suffix)

    def _on_task_result_callback(self, result: 'TaskResult') -> None:
        """Orchestrator 每完成一个任务后的回调：持久化最新统计到 goal_state.json。

        防止进程异常退出时丢失已完成任务的统计数据（成本、failure_categories、task_stats 等）。
        每次回调都会：同步预算 → 更新 failure_categories（失败时）→ 刷新 task_stats → _save_state。
        """
        try:
            status_val = result.status.value if hasattr(result.status, 'value') else str(result.status)

            with self._state_lock:
                # 同步 Orchestrator 内部 BudgetTracker 的花费
                self._state.total_cost_usd = self._budget.spent

                # 任务失败时更新 failure_categories（使用复合键：error_type:task_id）
                if result.status == TaskStatus.FAILED:
                    error_msg = result.error or 'unknown'
                    # 截断长错误消息，避免 key 爆炸
                    cat_key = f"task_failed:{error_msg[:80]}"
                    self._state.failure_categories[cat_key] = (
                        self._state.failure_categories.get(cat_key, 0) + 1
                    )

            # 刷新 task_stats（在锁外执行，_aggregate_task_stats 内部会自行加锁读数据）
            try:
                self._aggregate_task_stats()
            except Exception as agg_err:
                logger.debug("回调中聚合统计失败（不影响保存）: %s", agg_err)

            self._save_state()
            logger.debug(
                "任务 '%s' 完成回调：已持久化状态 (status=%s, cost=%.4f, fc=%d)",
                result.task_id,
                status_val,
                self._state.total_cost_usd,
                len(self._state.failure_categories),
            )
        except Exception as e:
            logger.warning("任务 '%s' 完成回调持久化失败: %s", result.task_id, e)

    def _check_resources(self) -> tuple[bool, str]:
        """检查系统资源是否充足。

        每次调用间隔至少 60 秒（通过 _last_resource_check 节流），
        避免频繁的系统调用影响性能。
        线程安全：通过 _state_lock 保护节流时间戳。

        Returns:
            (ok, message): ok=True 表示资源充足，False 表示需要降级或停止
        """
        now = time.monotonic()
        with self._state_lock:
            if now - self._last_resource_check < 60:
                return True, ""
            self._last_resource_check = now

        limits = self._config.limits
        problems: list[str] = []

        # 1. 磁盘空间检查
        try:
            usage = shutil.disk_usage(self._working_dir)
            free_mb = usage.free / (1024 * 1024)
            if free_mb < limits.min_disk_space_mb:
                problems.append(
                    f"磁盘空间不足: 剩余 {free_mb:.0f}MB < 阈值 {limits.min_disk_space_mb}MB"
                )
            else:
                logger.debug("磁盘空间充足: 剩余 %.0fMB", free_mb)
        except OSError as e:
            logger.warning("磁盘空间检查失败: %s", e)

        # 2. 系统内存使用率检查（需要 psutil）
        if _HAS_PSUTIL:
            try:
                vm = psutil.virtual_memory()
                if vm.percent >= limits.max_memory_percent:
                    problems.append(
                        f"系统内存使用率过高: {vm.percent:.1f}% >= 阈值 {limits.max_memory_percent}%"
                    )
                else:
                    logger.debug("系统内存使用率: %.1f%%", vm.percent)
            except Exception as e:
                logger.warning("系统内存检查失败: %s", e)

            # 3. 进程 RSS 检查
            try:
                proc = psutil.Process()
                rss_mb = proc.memory_info().rss / (1024 * 1024)
                if rss_mb > limits.max_process_rss_mb:
                    problems.append(
                        f"进程内存过高: RSS {rss_mb:.0f}MB > 阈值 {limits.max_process_rss_mb}MB"
                    )
                elif rss_mb > limits.max_process_rss_mb * 0.8:
                    # 超过 80% 阈值时发出警告，但不阻止执行
                    logger.warning(
                        "进程内存接近上限: RSS %.0fMB (阈值 %dMB)",
                        rss_mb, limits.max_process_rss_mb,
                    )
                else:
                    logger.debug("进程 RSS: %.0fMB", rss_mb)
            except Exception as e:
                logger.warning("进程内存检查失败: %s", e)
        else:
            logger.debug("psutil 不可用，跳过内存检查")

        if problems:
            msg = "; ".join(problems)
            logger.error("资源检查失败: %s", msg)
            return False, msg

        return True, ""

    def _run_preflight_checks(self) -> None:
        """启动前置校验：检查工作目录、CLI 可用性、目标有效性。"""
        # 1. 检查工作目录是否存在
        start_time = datetime.now()
        try:
            work_path = Path(self._working_dir)
            if not work_path.exists():
                error_msg = f"工作目录不存在: {self._working_dir}"
                self._record_diagnostic(
                    stage="preflight_workdir",
                    exit_status=FailureCategory.INIT_ERROR.value,
                    error_detail=error_msg,
                    start_time=start_time,
                    stack_trace=traceback.format_exc(),
                )
                raise PreflightError(error_msg)

            logger.info("[OK] 工作目录校验通过: %s", self._working_dir)
        except PreflightError:
            raise
        except Exception as e:
            error_msg = f"工作目录校验异常: {e}"
            self._record_diagnostic(
                stage="preflight_workdir",
                exit_status=FailureCategory.INIT_ERROR.value,
                error_detail=error_msg,
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            raise PreflightError(error_msg)

        # 2. 检查 Claude CLI 是否可用（委托给 claude_cli.verify_cli_available）
        start_time = datetime.now()
        try:
            verify_cli_available()
        except Exception as e:
            error_msg = f"Claude CLI 不可用: {e}"
            self._record_diagnostic(
                stage="preflight_cli",
                exit_status=FailureCategory.ENV_MISSING.value,
                error_detail=error_msg,
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            raise PreflightError(error_msg)

        # 3. 检查 goal 文本有效性
        start_time = datetime.now()
        if not self._goal or not self._goal.strip():
            error_msg = f"目标文本无效: 长度={len(self._goal) if self._goal else 0}"
            self._record_diagnostic(
                stage="preflight_goal",
                exit_status=FailureCategory.GOAL_PARSE_ERROR.value,
                error_detail=error_msg,
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            raise PreflightError(error_msg)

        # 4. 检查 goal 长度是否满足最低阈值
        start_time = datetime.now()
        passed, reason = validate_goal_length(self._goal, min_length=10)
        if not passed:
            error_msg = f"目标文本过短: 长度={len(self._goal.strip())}，至少需要 10 个字符 ({reason})"
            self._record_diagnostic(
                stage="preflight_goal_length",
                exit_status=FailureCategory.GOAL_PARSE_ERROR.value,
                error_detail=error_msg,
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            raise PreflightError(error_msg)

        logger.info("[OK] 目标文本校验通过: %d 字符", len(self._goal))

        # 5. 检查 goal 可操作性（动词+具体描述）
        start_time = datetime.now()
        from .requirement_gatherer import assess_goal_operability
        operability_score, operability_reason = assess_goal_operability(self._goal)
        logger.info("Goal 可操作性评估: score=%.2f, %s", operability_score, operability_reason)
        if operability_score < 0.3:
            error_msg = (
                f"目标可操作性不足 (score={operability_score:.2f}): {operability_reason}。"
                "请提供包含明确动作动词和具体技术描述的目标，"
                "例如 '修复 login API 的 500 错误' 而非 'login 问题'"
            )
            self._record_diagnostic(
                stage="preflight_goal_operability",
                exit_status=FailureCategory.GOAL_PARSE_ERROR.value,
                error_detail=error_msg,
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            raise PreflightError(error_msg)

        logger.info("[OK] Goal 可操作性校验通过: score=%.2f", operability_score)
        logger.info("前置校验全部通过")

    def _is_phase_already_done(self, phase: Phase) -> bool:
        """恢复模式下，跳过已完成/已跳过的阶段。"""
        return phase.status in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED)

    # ── 管道阶段函数 ──────────────────────────────────────────────
    # 每个方法返回 None 表示继续，返回 GoalState 表示提前终止。

    def _pipeline_preflight(self) -> GoalState | None:
        """管道阶段 0: 心跳、dashboard 推送、catastrophic guard、Webhook、前置校验。"""
        self._heartbeat.start_background()
        self._push_dashboard_state()

        # catastrophic guard 检查
        if self._runtime_layout:
            catastrophic = self._catastrophic_guard.check(self._runtime_layout)
            if catastrophic.is_catastrophic:
                self._state.status = GoalStatus.CATASTROPHIC_STOP
                self._state.stop_reason = catastrophic.reason
                # _record_diagnostic 内部会写入 failure_categories['catastrophic_stop']，无需手动写入
                self._record_diagnostic(
                    stage='catastrophic_guard',
                    exit_status='catastrophic_stop',
                    error_detail=catastrophic.reason,
                )
                return self._state

        # 启动 Webhook 心跳汇报（如果配置了 webhook_url）
        try:
            from .notification import get_notifier
            self._notifier = get_notifier()
            self._notifier.start_heartbeat()
        except Exception:
            self._notifier = None

        # 前置校验
        start_time = datetime.now()
        logger.debug("[DEBUG] >>> 开始前置校验")
        try:
            self._run_preflight_checks()
        except Exception as e:
            # 捕获原始异常类型，通过 _record_diagnostic 统一写入 failure_categories（线程安全）
            cat_key = f'preflight_raw:{type(e).__name__}'
            self._record_diagnostic(
                stage='preflight_raw',
                exit_status=cat_key,
                error_detail=str(e)[:200],
                start_time=start_time,
            )
            raise PreflightError(str(e)) from e
        self._record_diagnostic('preflight', start_time=start_time)
        logger.debug("[DEBUG] >>> 前置校验完成")
        return None

    def _pipeline_analyze_project(self) -> GoalState | None:
        """管道阶段 1: 项目分析（恢复模式下已有 context 则跳过）。"""
        if self._state.project_context:
            logger.debug("[DEBUG] >>> 项目分析阶段已跳过（有缓存）")
            return None

        logger.debug("[DEBUG] >>> 开始项目分析（无缓存）")
        start_time = datetime.now()
        self._state.status = GoalStatus.INITIALIZING
        try:
            if self._repo_profile:
                logger.info("使用仓库画像构建项目上下文，跳过 LLM 项目重分析")
                self._state.project_context = self._build_repo_profile_context(self._repo_profile)
            else:
                self._state.project_context = self._analyze_project()
            self._record_diagnostic('project_analysis', start_time=start_time)
        except Exception as e:
            # 结构化记录项目分析失败到 failure_categories（phase:error_type:message）
            error_type = type(e).__name__
            error_msg = str(e)[:80]
            cat_key = f'project_analysis:{error_type}:{error_msg}'
            # 零阶段诊断：用于日志记录（不再写入 failure_categories，避免双重写入）
            diagnosis = self._diagnose_zero_phase_failure(e, 'project_analysis')
            logger.warning(
                "项目分析失败: %s，诊断: %s (recoverable=%s)",
                e, diagnosis['detail'], diagnosis['recoverable'],
            )
            self._record_diagnostic(
                stage='project_analysis',
                exit_status=cat_key,
                error_detail=str(e),
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            self._state.project_context = (
                self._build_repo_profile_context(self._repo_profile) if self._repo_profile else ""
            )
        self._save_state()
        logger.debug("[DEBUG] >>> 项目分析阶段完成")
        return None

    def _pipeline_gather_requirements(self) -> GoalState | None:
        """管道阶段 1.5: 需求收集（恢复模式下已有 spec 则跳过）。"""
        if not self._gather_enabled or not self._gatherer or self._state.requirement_spec:
            logger.debug("[DEBUG] >>> 需求收集阶段已跳过或完成")
            return None

        start_time = datetime.now()
        self._state.status = GoalStatus.GATHERING
        try:
            spec = self._gatherer.gather(self._goal, self._state.project_context)
            self._state.requirement_spec = spec
            if spec.sufficiency_verdict != "sufficient":
                self._goal = spec.to_enhanced_goal()
                self._state.goal_text = self._goal
                logger.info(
                    "需求收集完成: %d 轮, %d 问 %d 答, 增强 goal %d 字符",
                    len(spec.rounds), spec.total_questions_asked,
                    spec.total_questions_answered, len(self._goal),
                )
            else:
                logger.info("目标已足够具体 (score=%.2f)，跳过需求收集", spec.sufficiency_score)
            self._record_diagnostic('requirement_gathering', start_time=start_time)
        except Exception as e:
            logger.warning("需求收集失败: %s，使用原始目标继续", e)
            self._record_diagnostic(
                stage='requirement_gathering',
                exit_status='gathering_error',
                error_detail=str(e),
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
        self._save_state()
        logger.debug("[DEBUG] >>> 需求收集阶段完成")
        return None

    # ── 简单目标快速路径 ──────────────────────────────────────────

    _SIMPLE_GOAL_MAX_LENGTH = 120
    _SIMPLE_GOAL_PATTERNS = [
        r'^create\s+\S+',
        r'^make\s+\S+',
        r'^write\s+\S+',
        r'^delete\s+\S+',
        r'^rename\s+\S+',
        r'^move\s+\S+',
        r'^copy\s+\S+',
        r'^touch\s+\S+',
        r'^mkdir\s+\S+',
        r'^echo\s+\S+',
        r'^add\s+an?\s+empty\s+\S+',
        r'^fix\s+the\s+\S+\s+(error|bug|typo)',
        r'^generate\s+a?\s*\S+',
    ]
    _SIMPLE_GOAL_COMPILED = re.compile(
        '|'.join(_SIMPLE_GOAL_PATTERNS), re.IGNORECASE
    )

    def _is_simple_goal(self, goal: str) -> bool:
        """判断目标是否足够简单，可以直接执行而无需分解。

        条件：
        1. 目标文本较短（≤120 字符）
        2. 以简单动词开头（create/make/write/delete 等）
        3. 不包含多阶段关键词（and/then/after/阶段/phase）
        """
        text = goal.strip()
        if not text or len(text) > self._SIMPLE_GOAL_MAX_LENGTH:
            return False
        # 包含多步骤关键词时不是简单目标
        multi_step_keywords = [' and ', ' then ', ' after ', ' before ',
                               '、', '接着', '然后', '之后', '阶段', 'phase', 'step']
        lower = text.lower()
        if any(kw in lower for kw in multi_step_keywords):
            return False
        return bool(self._SIMPLE_GOAL_COMPILED.match(text))

    def _execute_surgical(self) -> GoalState | None:
        """精确迭代模式：一次只改一个问题，用验证命令反馈驱动迭代。

        委托给 SurgicalController，修复完成后同步状态回 GoalState。
        """
        from .surgical_controller import SurgicalController

        verification_commands = _resolve_surgical_verification_commands_helper(
            auto_quality_commands=list(self._auto_config.quality_gate.commands),
            verification_plan=self._verification_plan,
            working_dir=self._working_dir,
            infer_commands=self._infer_verification_commands,
        )

        controller = SurgicalController(
            goal=self._goal,
            working_dir=self._working_dir,
            claude_config=self._config.claude,
            limits=self._config.limits,
            verification_commands=verification_commands,
            budget_tracker=self._budget,
            notebook=self._state.notebook if hasattr(self._state, 'notebook') else None,
            max_iterations=self._auto_config.max_total_iterations,
            deadline=self._state.deadline,
            execution_model=self._auto_config.execution_model,
        )

        result = controller.execute()

        _sync_surgical_result_to_state_helper(
            self._state,
            notebook=controller.notebook,
            iteration_count=controller.iteration_count,
            budget_spent=self._budget.spent if self._budget else 0.0,
            result=result,
        )
        self._save_state()
        return self._state

    def _infer_verification_commands(self) -> list[str]:
        """从项目结构自动推断验证命令。"""
        return _infer_verification_commands_helper(self._working_dir or ".")

    def _execute_simple_goal(self) -> GoalState | None:
        """简单目标的快速执行路径：跳过分解，直接用 claude CLI 执行。

        Returns:
            GoalState 表示提前终止（成功或失败），None 表示应走正常路径。
        """
        logger.info("简单目标快速路径: '%s'", self._goal[:80])
        start_time = datetime.now()
        self._state.status = GoalStatus.EXECUTING

        task_node = TaskNode(
            id="_simple_goal",
            prompt_template=(
                f"直接执行以下目标，不需要分析或规划：\n\n{self._goal}\n\n"
                "完成后简要说明做了什么。"
            ),
            timeout=300,
            model=self._auto_config.decomposition_model,
            output_format="text",
            provider=self._preferred_provider,
            type="agent_cli",
            executor_config={"phase": "execute"},
        )

        try:
            result = run_agent_task(
                task=task_node,
                prompt=task_node.prompt_template,
                config=self._config,
                limits=self._config.limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
                on_progress=None,
                cli_provider=self._preferred_provider if self._preferred_provider in {"claude", "codex"} else None,
                phase_provider_overrides=self._phase_provider_overrides,
            )
        except Exception as e:
            exc_type = type(e).__name__
            exc_msg = str(e)[:500]
            self._record_diagnostic(
                stage='simple_goal_execution',
                exit_status=f'early_failure:{exc_type}',
                error_detail=f"{exc_type}: {exc_msg}",
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            # _record_diagnostic 已写入 failure_categories，无需重复
            self._state.status = GoalStatus.FAILED
            self._save_state()
            return self._state

        phase = _apply_simple_goal_result_helper(
            self._state,
            goal=self._goal,
            result=result,
        )

        if result.status == TaskStatus.SUCCESS:
            self._record_diagnostic('simple_goal_execution', start_time=start_time)
            logger.info("简单目标执行成功")
        else:
            error = result.error or "执行失败"
            # _record_diagnostic 内部会写入 failure_categories['simple_goal_failed']，无需手动写入
            self._record_diagnostic(
                stage='simple_goal_execution',
                exit_status='simple_goal_failed',
                error_detail=error[:500],
                start_time=start_time,
            )
            logger.warning("简单目标执行失败: %s", error[:200])

        self._save_state()
        return self._state

    # ── 零阶段失败诊断与恢复 ──────────────────────────────────────

    def _diagnose_zero_phase_failure(self, exc: Exception, step_name: str) -> dict:
        """诊断零阶段失败（在阶段生成之前发生的失败）。

        根据异常内容将失败分为 4 类，并给出是否可自动恢复的判断。

        Args:
            exc: 捕获的异常
            step_name: 失败发生的步骤名称（如 'preflight', 'analyze', 'decompose'）

        Returns:
            诊断结果字典，包含：
            - category: 失败分类（transient / environment / logic / fatal）
            - recoverable: 是否可自动恢复
            - suggested_action: 建议的恢复动作
            - detail: 详细错误信息
        """
        exc_type = type(exc).__name__
        exc_msg = str(exc)[:500]
        # 将异常类型名和消息合并后再匹配关键词，确保 PermissionError 等也能命中
        combined = f"{exc_type} {exc_msg}".lower()

        # 1. 环境类失败：不可自动恢复
        _ENV_DIAG_KEYWORDS = [
            "permissionerror", "permission denied", "access denied",
            "filenotfounderror", "claude cli not found", "cli not found",
            "command not found", "is not recognized", "no such file or directory",
        ]
        if any(kw in combined for kw in _ENV_DIAG_KEYWORDS):
            return {
                "category": "environment",
                "recoverable": False,
                "suggested_action": "修复执行环境后重新启动",
                "detail": f"[{step_name}] 环境错误 ({exc_type}): {exc_msg}",
            }

        # 2. 临时性失败：可自动恢复（重试）
        _TRANSIENT_DIAG_KEYWORDS = [
            "timeouterror", "timed out", "timeout", "超时", "connection reset",
            "connection refused", "502", "503", "504",
            "network", "temporary failure", "rate limit", "429",
        ]
        if any(kw in combined for kw in _TRANSIENT_DIAG_KEYWORDS):
            return {
                "category": "transient",
                "recoverable": True,
                "suggested_action": "短暂等待后重试",
                "detail": f"[{step_name}] 临时性错误 ({exc_type}): {exc_msg}",
            }

        # 3. JSON 解析 / 输出格式错误：可降级重试
        _LOGIC_DIAG_KEYWORDS = [
            "jsondecodeerror", "json parse", "expecting value", "unexpected token",
        ]
        if any(kw in combined for kw in _LOGIC_DIAG_KEYWORDS):
            return {
                "category": "logic",
                "recoverable": True,
                "suggested_action": "使用简化模式重试分解",
                "detail": f"[{step_name}] 解析错误 ({exc_type}): {exc_msg}",
            }

        # 4. 默认：致命错误，不可自动恢复
        return {
            "category": "fatal",
            "recoverable": False,
            "suggested_action": "检查日志并手动修复",
            "detail": f"[{step_name}] 未分类错误 ({exc_type}): {exc_msg}",
        }

    def _auto_recover_zero_phase(self, goal: str, context: str) -> bool:
        """在零阶段失败后尝试自动恢复。

        依次尝试 3 种降级策略：
        1. 简化 goal 文本（截断到 500 字符）并重新分解
        2. 同时截断上下文后重试
        3. 最小化分解（无上下文，强制 1-2 个阶段）

        Args:
            goal: 原始目标文本
            context: 项目上下文

        Returns:
            True 表示恢复成功（self._state.phases 已填充），False 表示恢复失败
        """
        logger.info("尝试零阶段自动恢复...")

        # 策略 1：简化 goal
        if len(goal) > 500:
            simplified_goal = goal[:500] + "\n\n注意：请用最简化的方式完成此目标。"
            logger.info(
                "零阶段恢复策略1: 简化 goal (%d → %d 字符)",
                len(goal), len(simplified_goal),
            )
            try:
                self._state.phases = self._decomposer.decompose(
                    simplified_goal,
                    context or "",
                    self._task_contract,
                    self._architecture_contract,
                )
                if self._state.phases:
                    self._goal = simplified_goal
                    self._state.goal_text = simplified_goal
                    logger.info("零阶段恢复成功: 简化 goal 分解出 %d 个阶段", len(self._state.phases))
                    return True
            except Exception as e:
                logger.warning("零阶段恢复策略1 失败: %s", e)

        # 策略 2：截断上下文
        if context and len(context) > 2000:
            truncated_context = context[:2000] + "\n\n[上下文已截断]"
            logger.info(
                "零阶段恢复策略2: 截断上下文 (%d → %d 字符)",
                len(context), len(truncated_context),
            )
            try:
                self._state.phases = self._decomposer.decompose(
                    goal[:500] if len(goal) > 500 else goal,
                    truncated_context,
                    self._task_contract,
                    self._architecture_contract,
                )
                if self._state.phases:
                    logger.info("零阶段恢复成功: 截断上下文后分解出 %d 个阶段", len(self._state.phases))
                    return True
            except Exception as e:
                logger.warning("零阶段恢复策略2 失败: %s", e)

        # 策略 3：最小化分解（无上下文，无合同约束）
        logger.info("零阶段恢复策略3: 最小化分解（无上下文）")
        try:
            minimal_goal = goal[:300] if len(goal) > 300 else goal
            self._state.phases = self._decomposer.decompose(
                minimal_goal + "\n\n请分解为最少的阶段（1-2个），每个阶段最多3个任务。",
                "",
                None,
                None,
            )
            if self._state.phases:
                logger.info("零阶段恢复成功: 最小化分解出 %d 个阶段", len(self._state.phases))
                return True
        except Exception as e:
            logger.warning("零阶段恢复策略3 失败: %s", e)

        logger.warning("零阶段自动恢复失败: 所有策略均未成功")
        return False

    def _estimate_goal_complexity(self, goal_text: str) -> str:
        """在分解前估算目标复杂度（仅基于 goal 文本，无需 LLM 调用）。

        用于在分解阶段之前决定是否启用快速路径或特殊参数调整。
        与 _is_simple_goal 不同，此方法返回三档复杂度评估。

        Args:
            goal_text: 目标文本

        Returns:
            'simple' | 'normal' | 'complex'
        """
        text = goal_text.strip()
        length = len(text)

        # 极短目标直接判为简单
        if length <= 50:
            return 'simple'

        # 多步骤关键词计数
        _MULTI_STEP_KEYWORDS = [
            ' and ', ' then ', ' after ', ' before ', '、',
            '接着', '然后', '之后', '阶段', 'phase', 'step',
            'first', 'second', 'finally', 'additionally',
            'refactor', 'migrate', 'redesign',
        ]
        lower_text = text.lower()
        multi_step_count = sum(1 for kw in _MULTI_STEP_KEYWORDS if kw in lower_text)

        # 技术栈关键词计数
        _GOAL_COMPLEXITY_TECH = {
            'react', 'vue', 'angular', 'svelte',
            'express', 'django', 'flask', 'fastapi', 'spring',
            'postgresql', 'mysql', 'mongodb', 'redis',
            'docker', 'kubernetes', 'aws', 'azure', 'gcp',
            'websocket', 'graphql', 'rest',
            'typescript', 'python', 'java', 'go', 'rust',
        }
        tech_count = sum(1 for kw in _GOAL_COMPLEXITY_TECH if kw in lower_text)

        # 综合评分
        score = 0.0
        score += min(length / 100, 5)  # 长度贡献，上限 5
        score += multi_step_count * 2   # 多步骤贡献
        score += tech_count * 1.5       # 技术栈贡献

        if score <= 2:
            return 'simple'
        elif score <= 6:
            return 'normal'
        else:
            return 'complex'

    def _pipeline_decompose(self) -> GoalState | None:
        """管道阶段 2: 目标分解（恢复模式下已有 phases 则跳过）。"""
        if self._state.phases:
            return None

        # Fast-path：极简目标跳过分解，直接创建单阶段单任务 DAG
        goal_complexity = self._estimate_goal_complexity(self._goal)
        logger.info("目标复杂度评估: %s (goal 长度=%d)", goal_complexity, len(self._goal))

        if goal_complexity == 'simple' and len(self._goal) <= 100:
            logger.info("极简目标 fast-path: 跳过分解，直接创建单阶段单任务 DAG")
            simple_phase = Phase(
                id="fast_path_0",
                name="直接执行",
                description=self._goal,
                raw_tasks=[{"id": "fast_task_0", "prompt": self._goal}],
                depends_on_phases=[],
                order=0,
                max_iterations=3,
            )
            self._state.phases = [simple_phase]
            self._save_state()
            # 跳过分解，直接进入后续处理（复杂度评估等）
            start_time = datetime.now()
            self._post_decompose_processing(start_time)
            return None

        logger.debug("[DEBUG] >>> 开始目标分解（无缓存）")
        start_time = datetime.now()
        self._state.status = GoalStatus.DECOMPOSING

        # 目标分解，带重试机制（最多 2 次重试）和整体超时守卫
        _DECOMPOSE_MAX_RETRIES = 2
        _DECOMPOSE_OVERALL_TIMEOUT = 3600  # 整体超时上限（秒），防止重试无限累积
        _decompose_succeeded = False
        for _decompose_attempt in range(_DECOMPOSE_MAX_RETRIES + 1):
            # 每次重试前检查是否已超过整体超时上限
            _elapsed = (datetime.now() - start_time).total_seconds()
            if _elapsed > _DECOMPOSE_OVERALL_TIMEOUT:
                logger.error(
                    "目标分解整体超时（%.0fs > %ds），强制终止",
                    _elapsed, _DECOMPOSE_OVERALL_TIMEOUT,
                )
                self._record_diagnostic(
                    stage='goal_decomposition',
                    exit_status=FailureCategory.TIMEOUT.value,
                    error_detail=f"目标分解整体超时 {_elapsed:.0f}s",
                    start_time=start_time,
                )
                self._state.status = GoalStatus.FAILED
                self._save_state()
                self._print_final_summary()
                return self._state
            try:
                _current_goal = self._goal
                if _decompose_attempt == 2:
                    _current_goal = self._goal[:500] + (
                        "\n\n注意：请用最简化的方式分解此目标，每个阶段最多 3 个任务。"
                    )
                    logger.warning("目标分解第 %d 次重试，使用简化 goal（截断到 500 字符）", _decompose_attempt)
                elif _decompose_attempt > 0:
                    logger.warning("目标分解第 %d 次重试，使用原始 goal", _decompose_attempt)

                self._state.phases = self._decomposer.decompose(
                    _current_goal,
                    self._state.project_context,
                    self._task_contract,
                    self._architecture_contract,
                )
                if self._task_contract:
                    self._state.phases = self._closure_planner.plan_phases(self._state.phases, self._task_contract)
                self._record_diagnostic('goal_decomposition', start_time=start_time)
                _decompose_succeeded = True
                break
            except Exception as _de:
                if _decompose_attempt < _DECOMPOSE_MAX_RETRIES:
                    logger.warning(
                        "目标分解失败（第 %d/%d 次），将在 2 秒后重试: %s",
                        _decompose_attempt + 1, _DECOMPOSE_MAX_RETRIES + 1, _de,
                    )
                    self._record_diagnostic(
                        stage='goal_decomposition_retry',
                        exit_status=FailureCategory.GOAL_PARSE_ERROR.value,
                        error_detail=f"第 {_decompose_attempt + 1} 次分解失败，将重试: {_de}",
                        start_time=start_time,
                        stack_trace=traceback.format_exc(),
                    )
                    time.sleep(2)
                else:
                    # 调用零阶段诊断获取结构化分类
                    diag = self._diagnose_zero_phase_failure(_de, 'goal_decomposition')
                    # _record_diagnostic 内部已写入 failure_categories[exit_status]，无需手动重复写入
                    self._record_diagnostic(
                        stage='goal_decomposition',
                        exit_status=f"zero_phase:{diag['category']}",
                        error_detail=f"目标分解在 {_DECOMPOSE_MAX_RETRIES + 1} 次尝试后仍失败: {_de}",
                        start_time=start_time,
                        stack_trace=traceback.format_exc(),
                    )

                    # 可恢复时尝试自动恢复
                    if diag.get('recoverable'):
                        logger.info(
                            "零阶段诊断结果: 可恢复 (%s)，尝试自动恢复: %s",
                            diag['category'], diag['suggested_action'],
                        )
                        recovered = self._auto_recover_zero_phase(
                            self._goal, self._state.project_context,
                        )
                        if recovered and self._state.phases:
                            logger.info(
                                "零阶段自动恢复成功，得到 %d 个阶段，重置为 DECOMPOSING 继续管道",
                                len(self._state.phases),
                            )
                            self._post_decompose_processing(start_time)
                            return None  # 恢复成功，返回 None 让管道继续
                        else:
                            logger.warning("零阶段自动恢复失败，继续标记为 FAILED")

                    self._state.status = GoalStatus.FAILED
                    self._save_state()
                    self._print_final_summary()
                    return self._state

        # 不可达防御：for 循环只有 break（成功）和 return（失败）两种退出方式，
        # 到达此处意味着 _decompose_succeeded 必为 True（已通过 break 跳出）

        # 分解成功但返回空 phases 列表
        if not self._state.phases:
            self._record_diagnostic(
                stage='goal_decomposition',
                exit_status='decompose_empty',
                error_detail='GoalDecomposer 返回了空的 phases 列表',
                start_time=start_time,
            )
            self._state.status = GoalStatus.FAILED
            self._save_state()
            self._print_final_summary()
            return self._state

        # 分解成功后的后续处理（复杂度评估）
        self._post_decompose_processing(start_time)
        return None

    def _post_decompose_processing(self, start_time: datetime) -> None:
        """分解成功后的复杂度评估和自适应参数调整。"""
        try:
            complexity = self._estimate_complexity(self._state.phases)
            self._auto_config.adapt_to_complexity(complexity)
            for phase in self._state.phases:
                if phase.max_iterations == 3:
                    phase.max_iterations = self._auto_config.max_phase_iterations
            logger.info(
                "自适应参数调整 [%s]: threshold=%.2f, window=%d, "
                "plateau_min=%.3f, max_phase_iter=%d, "
                "det_window=%d, det_drop=%.2f",
                complexity.complexity_level,
                self._auto_config.convergence_threshold,
                self._auto_config.convergence_window,
                self._auto_config.score_improvement_min,
                self._auto_config.max_phase_iterations,
                self._auto_config.deterioration_window,
                self._auto_config.deterioration_drop_threshold,
            )
            if complexity.should_split:
                logger.warning("复杂度超阈值: %s", complexity.split_suggestion)
                complexity_info = (
                    f"\n\n## 复杂度评估\n"
                    f"- 预估子任务数: {complexity.estimated_subtasks}\n"
                    f"- 预估小时数: {complexity.estimated_hours:.1f}\n"
                    f"- 复杂度级别: {complexity.complexity_level}\n"
                    f"- 技术栈: {', '.join(complexity.tech_stacks)}\n"
                    f"- 建议: {complexity.split_suggestion}\n"
                )
                self._state.project_context += complexity_info
            self._save_state()
        except Exception as e:
            logger.error("目标分解后续处理失败: %s", e)
            self._record_diagnostic(
                stage='post_decomposition',
                error_detail=str(e),
                start_time=start_time,
                stack_trace=traceback.format_exc(),
            )
            self._save_state()

    def _pipeline_execute(self) -> GoalState | None:
        """管道阶段 3: 基于依赖图并行执行阶段。"""
        logger.debug("[DEBUG] >>> 进入并行执行阶段")
        start_time = datetime.now()
        self._state.status = GoalStatus.EXECUTING
        try:
            self._execute_phases_parallel()
            self._record_diagnostic('pipeline_execute', start_time=start_time)
        except Exception as e:
            # 早期故障捕获：当尚未产生任何阶段、迭代或费用时，
            # 使用零阶段诊断替代简单的 error_key 构造
            if (not self._state.phases
                    and self._state.total_iterations == 0
                    and self._state.total_cost_usd == 0):
                exc_type = type(e).__name__
                exc_msg = str(e)[:500]
                stack_trace = traceback.format_exc()

                # 调用零阶段诊断获取结构化分类
                diag = self._diagnose_zero_phase_failure(e, 'pipeline_execute')
                diag_exit_status = f"zero_phase:{diag['category']}"
                self._record_diagnostic(
                    stage='pipeline_execute',
                    exit_status=diag_exit_status,
                    error_detail=f"{exc_type}: {exc_msg}",
                    stack_trace=stack_trace,
                    start_time=start_time,
                )

                # 可恢复时尝试自动恢复（重置为 DECOMPOSING 重新分解）
                if diag.get('recoverable'):
                    logger.info(
                        "执行阶段零阶段诊断: 可恢复 (%s)，尝试自动恢复: %s",
                        diag['category'], diag['suggested_action'],
                    )
                    recovered = self._auto_recover_zero_phase(
                        self._goal, self._state.project_context,
                    )
                    if recovered and self._state.phases:
                        logger.info(
                            "执行阶段零阶段恢复成功，得到 %d 个阶段，重置为 DECOMPOSING 继续管道",
                            len(self._state.phases),
                        )
                        # 恢复成功：重置状态并重新执行
                        self._state.status = GoalStatus.DECOMPOSING
                        self._save_state()
                        return None  # 返回 None 让管道继续（将重新进入执行）
                    else:
                        logger.warning("执行阶段零阶段自动恢复失败，继续标记为 FAILED")

                self._state.status = GoalStatus.FAILED
                return self._state
            # 有进展时向上传播，由 execute() 的外层 except 处理
            raise

        if self.requested_exit_code != 0:
            return self._state
        return None

    # ── 主入口 ─────────────────────────────────────────────────────

    def _run_pipeline_stage(
        self,
        runner,
        *,
        stage: str,
        exit_prefix: str,
        diagnostic_stage: str | None = None,
        include_stack_trace: bool = True,
        passthrough: tuple[type[BaseException], ...] = (),
    ):
        """Run one pipeline stage and normalize failure diagnostics."""
        try:
            return runner()
        except passthrough:
            raise
        except Exception as exc:
            diagnostic_kwargs = {
                "stage": diagnostic_stage or stage,
                "exit_status": f"{exit_prefix}:{type(exc).__name__}",
                "error_detail": str(exc)[:200],
            }
            if include_stack_trace:
                diagnostic_kwargs["stack_trace"] = traceback.format_exc()
            self._record_diagnostic(**diagnostic_kwargs)
            raise

    def execute(self) -> GoalState:
        """Run the full autonomous loop. Returns final GoalState."""
        # 防御性初始化：确保 failure_categories 始终可用，
        # 防止从旧格式状态文件恢复时字段缺失导致 AttributeError
        if not hasattr(self._state, 'failure_categories') or self._state.failure_categories is None:
            self._state.failure_categories = {}

        # 前置检查：验证目标描述非空且长度足够，否则记录 WARNING 供后续参考
        _goal_stripped = (self._goal or '').strip()
        if not _goal_stripped:
            logger.warning("目标描述为空，后续分解和执行可能缺乏明确方向")
            self._record_diagnostic(
                stage='preflight',
                exit_status='goal_empty',
                error_detail='目标描述为空',
            )
        elif len(_goal_stripped) <= 10:
            logger.warning("目标描述可能过于模糊（长度=%d）: %s", len(_goal_stripped), _goal_stripped)
            self._record_diagnostic(
                stage='preflight',
                exit_status='goal_vague',
                error_detail=f'目标描述过短（{len(_goal_stripped)}字符），可能缺乏足够细节指导分解',
            )

        logger.info("=" * 60)
        logger.info("自主编排器启动")
        logger.info("目标: %s", self._goal)
        logger.info("截止时间: %s", self._state.deadline.isoformat())
        if self._state.total_iterations > 0:
            logger.info("恢复模式: 从第 %d 次迭代继续", self._state.total_iterations)
        logger.info("=" * 60)

        lease_run_id = self._state.goal_id or hashlib.sha1(self._goal.encode("utf-8")).hexdigest()[:12]
        try:
            with bind_execution_lease_scope(self._execution_lease_manager, run_id=lease_run_id):
                result = self._run_pipeline_stage(
                    self._pipeline_preflight,
                    stage='preflight',
                    exit_prefix='preflight',
                    include_stack_trace=False,
                    passthrough=(PreflightError,),
                )
                if result is not None:
                    return result

                result = self._run_pipeline_stage(
                    self._pipeline_analyze_project,
                    stage='analysis',
                    diagnostic_stage='project_analysis',
                    exit_prefix='analysis',
                )
                if result is not None:
                    return result

                result = self._run_pipeline_stage(
                    self._pipeline_gather_requirements,
                    stage='gathering',
                    diagnostic_stage='requirement_gather',
                    exit_prefix='gathering',
                )
                if result is not None:
                    return result

                # ── 策略分派：精确迭代 vs 并行 DAG ──
                from .strategy_selector import classify_execution_strategy as _classify
                _mode = self._explicit_mode
                _strategy = _classify(self._goal, explicit_mode=_mode)
                if _strategy == "surgical":
                    result = self._execute_surgical()
                    if result is not None:
                        return result

                result = self._run_pipeline_stage(
                    self._pipeline_decompose,
                    stage='decompose',
                    diagnostic_stage='goal_decompose',
                    exit_prefix='decompose',
                )
                if result is not None:
                    return result

                result = self._run_pipeline_stage(
                    self._pipeline_execute,
                    stage='pipeline_execute',
                    exit_prefix='pipeline_execute',
                )
                if result is not None:
                    return result

                self._finalize_state()

        except _PoolSwitchRequested as exc:
            self.requested_exit_code = exc.exit_code
        except PreflightError as e:
            logger.error('前置校验失败: %s', e)
            self._state.status = GoalStatus.FAILED
            # _pipeline_preflight 已通过 preflight_raw:{type} 写入具体原因，
            # 仅在完全无记录时兜底写入 preflight_failure
            if not self._state.failure_categories:
                with self._state_lock:
                    self._state.failure_categories['preflight_failure'] = 1
        except BudgetExhaustedError as e:
            logger.error("预算耗尽，触发 SAFE_STOP: %s", e)
            self._state.status = GoalStatus.SAFE_STOP
            self._state.safe_stop_reason = SafeStopReason.BUDGET_EXHAUSTED
            self._state.stop_reason = str(e)
            with self._state_lock:
                self._state.failure_categories['budget_exhausted'] = (
                    self._state.failure_categories.get('budget_exhausted', 0) + 1
                )
        except KeyboardInterrupt:
            logger.warning("用户中断")
            self._state.status = GoalStatus.CANCELLED
            with self._state_lock:
                self._state.failure_categories['user_cancelled'] = self._state.failure_categories.get('user_cancelled', 0) + 1
        except Exception as e:
            logger.exception("自主编排器异常: %s", e)
            stack_trace = traceback.format_exc()
            exc_type_name = type(e).__name__
            # 将异常类型编码到 key，避免所有异常都记为笼统的 fatal_error
            cat_key = f'fatal_error:{exc_type_name}'
            self._record_diagnostic(
                stage='unknown',
                exit_status=cat_key,
                error_detail=str(e)[:200],
                stack_trace=stack_trace,
            )
            self._state.status = GoalStatus.FAILED
            # _record_diagnostic 内部已写入 failure_categories[cat_key]，无需重复写入
        finally:
            self._heartbeat.stop()

            try:
                if self._notifier is not None:
                    self._notifier.stop_heartbeat()
            except Exception:
                pass

            _budget_diff = self._budget.spent - self._state.total_cost_usd
            logger.info(
                'Budget audit: tracker.spent=%.4f, state.cost=%.4f, diff=%.4f, diagnostics_count=%d',
                self._budget.spent,
                self._state.total_cost_usd,
                _budget_diff,
                len(self._state.diagnostics),
            )

            self._state.total_cost_usd = self._budget.spent

            self._ensure_failure_categories(context='execute finally')

            # 兜底：确保 task_stats 已聚合（_finalize_state 可能因异常跳过）
            try:
                self._aggregate_task_stats()
            except Exception:
                pass

            self._save_state()
            self._push_dashboard_state()
            self._print_final_summary()

            if self._owns_store:
                try:
                    self._store.close()
                except Exception as e:
                    logger.warning("Failed to close store: %s", e)
            if self._execution_lease_manager is not None:
                try:
                    self._execution_lease_manager.close()
                except Exception as e:
                    logger.warning("Failed to close execution lease manager: %s", e)

        return self._state

    def _build_repo_profile_context(self, profile: RepoProfile) -> str:
        frameworks = ', '.join(profile.detected_frameworks) or 'unknown'
        package_managers = ', '.join(profile.package_managers) or 'unknown'
        lines = [
            "## 仓库画像",
            f"- 根目录: {profile.root}",
            f"- 技术栈: {frameworks}",
            f"- 包管理: {package_managers}",
            f"- 后端: {'yes' if profile.has_backend else 'no'}",
            f"- 前端: {'yes' if profile.has_frontend else 'no'}",
        ]

        if profile.backend_commands or profile.frontend_commands or profile.docker_compose_file:
            lines.extend(["", "## 验证入口"])
            if profile.backend_commands:
                lines.append(f"- 后端验证: {'；'.join(profile.backend_commands[:3])}")
            if profile.frontend_commands:
                lines.append(f"- 前端验证: {'；'.join(profile.frontend_commands[:3])}")
            if profile.docker_compose_file:
                lines.append(f"- Compose: {profile.docker_compose_file}")

        if profile.file_backup_paths or profile.metadata_backup_paths or profile.database_backup_commands:
            lines.extend(["", "## 备份提示"])
            if profile.file_backup_paths:
                lines.append(f"- 文件数据: {', '.join(profile.file_backup_paths[:5])}")
            if profile.metadata_backup_paths:
                lines.append(f"- 元数据: {', '.join(profile.metadata_backup_paths[:5])}")
            if profile.database_backup_commands:
                lines.append(f"- 数据库备份: {profile.database_backup_commands[0]}")

        if profile.warnings:
            lines.extend(["", "## 风险提示"])
            for warning in profile.warnings[:5]:
                lines.append(f"- {warning}")

        return "\n".join(lines).strip()

    def _architecture_artifact_paths(self) -> tuple[Path, Path, Path, Path]:
        if self._runtime_layout is not None:
            return (
                self._runtime_layout.state / "architecture_contract.json",
                self._runtime_layout.evidence / "architecture_evidence.json",
                self._runtime_layout.handoff / "architecture_summary.md",
                self._runtime_layout.state / "architecture_trigger.json",
            )
        base = Path(self._working_dir)
        state_dir = base / "state"
        evidence_dir = base / "evidence"
        handoff_dir = base / "handoff"
        state_dir.mkdir(parents=True, exist_ok=True)
        evidence_dir.mkdir(parents=True, exist_ok=True)
        handoff_dir.mkdir(parents=True, exist_ok=True)
        return (
            state_dir / "architecture_contract.json",
            evidence_dir / "architecture_evidence.json",
            handoff_dir / "architecture_summary.md",
            state_dir / "architecture_trigger.json",
        )

    def _append_architecture_context(self, summary_text: str) -> None:
        if not summary_text.strip():
            return
        marker = "## 架构合同摘要"
        if marker in self._state.project_context:
            return
        addition = f"\n\n{marker}\n{summary_text.strip()}"
        self._state.project_context = (self._state.project_context + addition).strip()

    def _architecture_execution_report_path(self, phase_id: str) -> Path:
        if self._runtime_layout is not None:
            target = self._runtime_layout.evidence / "architecture_execution" / f"{phase_id}.json"
        else:
            target = Path(self._working_dir) / "evidence" / "architecture_execution" / f"{phase_id}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    def _capture_architecture_execution_report(
        self,
        phase: Phase,
        task_outputs: dict[str, object],
    ) -> ArchitectureExecutionReport | None:
        report = build_architecture_execution_report(phase, task_outputs)
        if report is None:
            return None

        target = self._architecture_execution_report_path(phase.id)
        try:
            save_architecture_execution_report(report, target)
        except Exception as e:
            logger.warning("架构执行证据写入失败: phase=%s error=%s", phase.id, e)
        report.summary = render_architecture_execution_summary(report)

        phase.metadata["architecture_execution_report_path"] = str(target)
        phase.metadata["architecture_execution_report"] = architecture_execution_report_to_dict(report)
        phase.metadata["architecture_execution_status"] = report.status
        phase.metadata["architecture_gate_status"] = report.gate_status
        phase.metadata["architecture_unmet_cutover_gates"] = list(report.unmet_cutover_gates)
        phase.metadata["architecture_missing_evidence_refs"] = list(report.missing_evidence_refs)
        phase.metadata["architecture_missing_rollback_refs"] = list(report.missing_rollback_refs)
        phase.metadata["architecture_execution_summary"] = report.summary

        try:
            self._store.set_context(
                self._state.goal_id,
                f"_architecture_execution_{phase.id}",
                json.dumps(architecture_execution_report_to_dict(report), ensure_ascii=False),
            )
        except Exception as e:
            logger.warning("架构执行证据持久化失败: phase=%s error=%s", phase.id, e)

        return report

    def _get_architecture_execution_report(self, phase: Phase) -> ArchitectureExecutionReport | None:
        payload = phase.metadata.get("architecture_execution_report")
        if isinstance(payload, dict):
            try:
                return architecture_execution_report_from_dict(payload)
            except Exception:
                pass
        report_path = str(phase.metadata.get("architecture_execution_report_path", "") or "").strip()
        if report_path:
            path = Path(report_path)
            if path.exists():
                try:
                    return load_architecture_execution_report(path)
                except Exception:
                    return None
        return None

    def _apply_architecture_execution_constraints(self, phase: Phase, review: ReviewResult) -> ReviewResult:
        report = self._get_architecture_execution_report(phase)
        if report is None or report.status in {"complete", "not_applicable"}:
            return review

        gate_scope = str(
            (report.metadata or {}).get("architecture_gate_scope")
            or (phase.metadata or {}).get("architecture_gate_scope")
            or ""
        ).strip().lower()
        effective_unmet_cutover_gates = (
            []
            if gate_scope in {"", "none"} or report.gate_status == "not_applicable"
            else report.unmet_cutover_gates
        )
        blockers: list[str] = []
        soft_gaps: list[str] = []
        if effective_unmet_cutover_gates:
            blockers.append(f"未满足切流门槛: {', '.join(effective_unmet_cutover_gates[:4])}")
        if report.missing_rollback_refs:
            blockers.append(f"缺少回滚引用: {', '.join(report.missing_rollback_refs[:4])}")
        if report.missing_evidence_refs:
            evidence_gap = f"缺少执行证据引用: {', '.join(report.missing_evidence_refs[:4])}"
            if not effective_unmet_cutover_gates and not report.missing_rollback_refs:
                soft_gaps.append(evidence_gap)
            else:
                blockers.append(evidence_gap)
        if soft_gaps and not blockers:
            review.summary = f"[架构执行证据待完善] {'；'.join(soft_gaps)}\n{review.summary}"
            return review
        if not blockers:
            blockers.append(report.summary)

        prefix = "[架构执行约束未满足] " + "；".join(blockers)
        review.summary = f"{prefix}\n{review.summary}"
        review.score = min(review.score, 0.59 if report.gate_status == "blocked" else 0.69)
        if review.verdict in (ReviewVerdict.PASS, ReviewVerdict.MINOR_ISSUES):
            review.verdict = ReviewVerdict.MAJOR_ISSUES
        self._inject_architecture_corrective_actions(phase, review, report)
        return review

    @staticmethod
    def _is_architecture_operation_task(raw_task: dict[str, object]) -> bool:
        if str(raw_task.get("type", "claude_cli") or "claude_cli") != "operation":
            return False
        tags = {str(tag) for tag in raw_task.get("tags", [])}
        return "architecture_playbook" in tags

    def _find_architecture_operation_task(self, phase: Phase) -> dict[str, object] | None:
        for raw_task in phase.raw_tasks:
            if isinstance(raw_task, dict) and self._is_architecture_operation_task(raw_task):
                return raw_task
        return None

    def _upsert_corrective_action(self, review: ReviewResult, action: CorrectiveAction) -> CorrectiveAction:
        for existing in review.corrective_actions:
            if existing.action_id != action.action_id:
                continue
            existing.depends_on_actions = list(
                dict.fromkeys([*existing.depends_on_actions, *action.depends_on_actions])
            )
            if existing.action_type == "claude_cli" and action.action_type != "claude_cli":
                existing.action_type = action.action_type
            if existing.executor_config is None and action.executor_config is not None:
                existing.executor_config = action.executor_config
            if not existing.description and action.description:
                existing.description = action.description
            if not existing.prompt_template and action.prompt_template:
                existing.prompt_template = action.prompt_template
            existing.priority = min(existing.priority, action.priority)
            existing.timeout = max(existing.timeout, action.timeout)
            return existing

        review.corrective_actions.append(action)
        return action

    def _inject_architecture_corrective_actions(
        self,
        phase: Phase,
        review: ReviewResult,
        report: ArchitectureExecutionReport,
    ) -> None:
        refresh_id = f"arch_refresh_operations_{phase.id}"
        reconcile_id = f"arch_reconcile_{phase.id}"
        operation_task = self._find_architecture_operation_task(phase)

        refresh_action: CorrectiveAction | None = None
        if operation_task is not None:
            raw_executor_config = operation_task.get("executor_config")
            executor_config = deepcopy(raw_executor_config) if isinstance(raw_executor_config, dict) else None
            refresh_action = self._upsert_corrective_action(
                review,
                CorrectiveAction(
                    action_id=refresh_id,
                    description="重新执行架构 playbook 的结构化操作检查",
                    prompt_template=str(operation_task.get("prompt", "") or "重新执行架构结构化操作检查。"),
                    priority=1,
                    timeout=int(operation_task.get("timeout", 900) or 900),
                    action_type="operation",
                    executor_config=executor_config,
                ),
            )

        required_evidence: list[str] = []
        rollback_action_ids: list[str] = []
        playbook_steps = phase.metadata.get("architecture_playbook_steps", []) if phase.metadata else []
        if isinstance(playbook_steps, list):
            for item in playbook_steps:
                if not isinstance(item, dict):
                    continue
                required_evidence.extend(str(v) for v in item.get("evidence_required", []) if v)
                rollback_action_ids.extend(str(v) for v in item.get("rollback_action_ids", []) if v)

        unmet_gate_lines = [f"- {item}" for item in report.unmet_cutover_gates] or ["- 无"]
        missing_evidence_lines = [f"- {item}" for item in report.missing_evidence_refs] or ["- 无"]
        missing_rollback_lines = [f"- {item}" for item in report.missing_rollback_refs] or ["- 无"]
        prompt_lines = [
            "根据最新架构执行报告补齐本阶段的架构证据、切流门禁和回滚引用。",
            "禁止伪造完成状态；若仍未满足，必须在输出中继续保留真实的 `UnmetCutoverGates` / `EvidenceRefs` / `RollbackRefs` 状态。",
            "",
            "当前未满足切流门槛:",
            *unmet_gate_lines,
            "当前缺少证据引用:",
            *missing_evidence_lines,
            "当前缺少回滚引用:",
            *missing_rollback_lines,
        ]
        if required_evidence:
            prompt_lines.extend(
                [
                    "Playbook 要求的证据引用:",
                    *[f"- {item}" for item in dict.fromkeys(required_evidence)],
                ]
            )
        if rollback_action_ids:
            prompt_lines.extend(
                [
                    "Playbook 要求的回滚动作引用:",
                    *[f"- {item}" for item in dict.fromkeys(rollback_action_ids)],
                ]
            )
        prompt_lines.append("完成后明确说明仍阻塞的门禁项，不能把未完成项写成已完成。")

        depends_on = [refresh_id] if refresh_action is not None or any(
            action.action_id == refresh_id for action in review.corrective_actions
        ) else []
        self._upsert_corrective_action(
            review,
            CorrectiveAction(
                action_id=reconcile_id,
                description="补齐架构执行证据与门禁状态",
                prompt_template="\n".join(prompt_lines),
                priority=2,
                depends_on_actions=depends_on,
                timeout=1800,
            ),
        )

    def _phase_allows_completion(self, phase: Phase) -> bool:
        report = self._get_architecture_execution_report(phase)
        if report is None or report.status in {"complete", "not_applicable"}:
            return True
        gate_scope = str(
            (report.metadata or {}).get("architecture_gate_scope")
            or (phase.metadata or {}).get("architecture_gate_scope")
            or ""
        ).strip().lower()
        effective_unmet_cutover_gates = (
            []
            if gate_scope in {"", "none"} or report.gate_status == "not_applicable"
            else report.unmet_cutover_gates
        )
        if not effective_unmet_cutover_gates and not report.missing_rollback_refs:
            return True
        return False

    def _ensure_architecture_contract(self) -> None:
        if self._task_contract is None:
            return

        mode = (self._task_contract.architecture_mode or "auto").strip().lower()
        if mode == "off":
            self._task_contract.requires_architecture_council = False
            self._state.architecture_triggered = False
            return

        if self._architecture_contract is not None:
            self._task_contract.requires_architecture_council = True
            self._task_contract.architecture_contract_path = self._state.architecture_contract_path
            if self._repo_profile is not None:
                self._verification_plan = VerificationPlanner().plan(
                    self._task_contract,
                    self._repo_profile,
                    architecture_contract=self._architecture_contract,
                )
            if self._state.architecture_summary:
                self._append_architecture_context(self._state.architecture_summary)
            return

        decision = self._architecture_trigger.decide(
            self._task_contract,
            self._repo_profile,
            self._state.project_context,
        )
        self._task_contract.architecture_trigger_reasons = list(decision.reasons)
        self._task_contract.requires_architecture_council = decision.should_trigger
        _, _, _, trigger_path = self._architecture_artifact_paths()
        trigger_path.write_text(
            json.dumps(
                {
                    "should_trigger": decision.should_trigger,
                    "reasons": decision.reasons,
                    "confidence": decision.confidence,
                    "suggested_roles": decision.suggested_roles,
                    "suggested_patterns": decision.suggested_patterns,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        if not decision.should_trigger:
            self._state.architecture_triggered = False
            return

        evidence_graph = EvidenceGraphBuilder().build(
            self._task_contract,
            self._repo_profile,
            project_context=self._state.project_context,
            verification_plan=self._verification_plan,
        )
        contract = self._architecture_council.deliberate(
            task_contract=self._task_contract,
            repo_profile=self._repo_profile,
            evidence_graph=evidence_graph,
            trigger_decision=decision,
            project_context=self._state.project_context,
        )
        contract_path, evidence_path, summary_path, _ = self._architecture_artifact_paths()
        save_architecture_contract(contract, contract_path)
        save_evidence_graph(evidence_graph, evidence_path)
        summary_text = render_architecture_summary(contract)
        summary_path.write_text(summary_text, encoding="utf-8")

        self._architecture_contract = contract
        self._task_contract.architecture_contract_path = str(contract_path)
        self._state.architecture_triggered = True
        self._state.architecture_contract_path = str(contract_path)
        self._state.architecture_summary = contract.selected_summary or summary_text.strip()
        self._state.architecture_decision_type = contract.decision_type
        self._append_architecture_context(summary_text)
        if self._repo_profile is not None:
            self._verification_plan = VerificationPlanner().plan(
                self._task_contract,
                self._repo_profile,
                architecture_contract=contract,
            )
        self._save_state()

    def _analyze_project(self) -> str:
        """Call Claude to analyze the project structure and produce a context summary."""
        logger.info("分析项目目录: %s", self._working_dir)

        prompt = (
            "分析当前工作目录的项目结构，包括：\n"
            "1. 项目类型和使用的技术栈\n"
            "2. 目录结构概览\n"
            "3. 关键文件和入口点\n"
            "4. 现有的测试、CI/CD 配置\n"
            "5. 依赖管理方式\n\n"
            "请简洁总结，控制在 2000 字以内。"
        )

        task_node = TaskNode(
            id="_analyze_project",
            prompt_template=prompt,
            timeout=300,
            model=self._auto_config.decomposition_model,
            output_format="text",
            provider=self._preferred_provider,
            type="agent_cli",
            executor_config={"phase": "discover"},
        )

        result = run_agent_task(
            task=task_node,
            prompt=prompt,
            config=self._config,
            limits=self._config.limits,
            budget_tracker=self._budget,
            working_dir=self._working_dir,
            on_progress=None,
            cli_provider=self._preferred_provider if self._preferred_provider in {"claude", "codex"} else None,
            phase_provider_overrides=self._phase_provider_overrides,
        )

        if result.status == TaskStatus.SUCCESS:
            context = result.output or ""
            logger.info("项目分析完成 (%d 字符)", len(context))
            return context

        logger.warning("项目分析失败: %s，继续执行", result.error)
        return ""

    def _estimate_complexity(self, phases: list[Phase]) -> ComplexityEstimate:
        """估算阶段列表的复杂度，用于前置校验和拆分建议。"""
        # 1. 统计所有阶段的 raw_tasks 总数
        estimated_subtasks = sum(len(phase.raw_tasks) for phase in phases)

        # 2. 预估小时数（每个子任务 0.15 小时）
        estimated_hours = estimated_subtasks * 0.15

        # 3. 从 task prompt 中提取技术栈关键词
        tech_keywords = {
            'React', 'Express', 'WebSocket', 'Python', 'TypeScript',
            'JavaScript', 'Node.js', 'Vue', 'Angular', 'Django',
            'Flask', 'FastAPI', 'PostgreSQL', 'MySQL', 'MongoDB',
            'Redis', 'Docker', 'Kubernetes', 'AWS', 'Azure', 'GCP',
            'Zustand', 'Vite', 'Jest', 'Vitest', 'Pytest',
        }

        tech_stacks = set()
        for phase in phases:
            for task in phase.raw_tasks:
                prompt = task.get('prompt', '').lower()
                for keyword in tech_keywords:
                    if keyword.lower() in prompt:
                        tech_stacks.add(keyword)

        # 4. 确定复杂度级别
        if estimated_subtasks <= 10:
            complexity_level = 'low'
        elif estimated_subtasks <= 25:
            complexity_level = 'medium'
        elif estimated_subtasks <= 50:
            complexity_level = 'high'
        else:
            complexity_level = 'extreme'

        # 5. 判断是否需要拆分
        should_split = complexity_level == 'extreme'

        # 6. 生成拆分建议
        split_suggestion = ""
        if should_split:
            split_suggestion = (
                f"检测到 {estimated_subtasks} 个子任务（预估 {estimated_hours:.1f} 小时），"
                f"建议将目标拆分为多个独立的子目标，每个目标不超过 50 个任务。"
                f"可以按功能模块、技术栈或开发阶段进行拆分。"
            )

        logger.info(
            "复杂度评估: %d 个子任务, %.1f 小时, 级别=%s, 技术栈=%s",
            estimated_subtasks, estimated_hours, complexity_level, list(tech_stacks),
        )

        return ComplexityEstimate(
            estimated_subtasks=estimated_subtasks,
            estimated_hours=estimated_hours,
            complexity_level=complexity_level,
            tech_stacks=list(tech_stacks),
            should_split=should_split,
            split_suggestion=split_suggestion,
        )

    def _generate_and_run_dag(self, phase: Phase) -> tuple[dict, list[TaskError], object, dict] | None:
        """生成并执行 DAG，收集结果。

        Args:
            phase: 当前阶段

        Returns:
            成功时返回 (task_outputs, task_errors, run_info, task_results)
            失败时返回 None（此时 phase.status 和 phase.last_classification 已设置）
        """
        logger.debug("[DEBUG] >>> _generate_and_run_dag 开始: %s", phase.name)
        # 生成 DAG：优先使用 Handoff 结构化文本，降级为 review summary
        prior_summaries = self._get_prior_phase_summaries(phase)
        handoff = self._last_handoff.get(phase.id)
        prepared_feedback = _prepare_dag_generation_feedback_helper(
            phase,
            handoff=handoff,
            execute_correction_dag=self._execute_correction_dag,
            capture_architecture_summary=lambda target_phase, outputs: (
                report.summary
                if (report := self._capture_architecture_execution_report(target_phase, outputs)) is not None
                and report.summary
                else ""
            ),
            query_relevant_learnings=self._query_relevant_learnings,
            handoff_enabled=self._auto_config.handoff_enabled,
            handoff_max_chars=self._auto_config.handoff_max_chars,
        )
        correction_outputs = prepared_feedback.correction_outputs
        correction_errors = prepared_feedback.correction_errors
        review_feedback = prepared_feedback.review_feedback

        try:
            logger.debug("[DEBUG] >>> 调用 DAG 生成器: %s", phase.name)
            dag = self._dag_generator.generate(
                phase=phase,
                project_context=self._state.project_context,
                prior_phase_summaries=prior_summaries,
                review_feedback=review_feedback,
                task_contract=self._task_contract,
            )
            logger.debug("[DEBUG] >>> DAG 生成完成: %s, tasks=%d", phase.name, len(dag.tasks) if dag else 0)
        except Exception as e:
            logger.error("DAG 生成失败: %s", e)
            # 创建失败分类
            phase.last_classification = FailureClassification(
                category=FailureCategory.LOGIC_ERROR,
                retriable=False,
                feedback=f"DAG 生成异常: {str(e)}",
            )
            phase.status = PhaseStatus.FAILED
            return None

        if not dag.tasks:
            logger.warning("阶段 '%s' 生成了空 DAG，跳过", phase.id)
            phase.status = PhaseStatus.COMPLETED
            return None

        _apply_phase_timeout_multiplier_helper(phase, dag)

        # 执行 DAG（复用现有 Orchestrator）
        executed_run = _execute_orchestrator_run_helper(
            dag,
            config=self._config,
            store=self._store,
            working_dir=self._working_dir,
            log_file=self._log_file,
            pool_runtime=self._pool_runtime,
            on_task_result=self._on_task_result_callback,
        )
        orch = executed_run.orchestrator
        run_info = executed_run.run_info

        if orch.requested_exit_code != 0:
            self.requested_exit_code = orch.requested_exit_code
            if self._pool_runtime is not None:
                self._state.active_profile = self._pool_runtime.active_profile
                self._state.pool_state = self._pool_runtime.state.to_dict()
            raise _PoolSwitchRequested(orch.requested_exit_code)

        collected_run = _collect_dag_run_artifacts_helper(
            phase,
            orch=orch,
            budget=self._budget,
            run_info=run_info,
            dag=dag,
            state=self._state,
            state_lock=self._state_lock,
            sync_state=False,
        )
        task_outputs = collected_run.task_outputs
        task_errors = collected_run.task_errors

        # 预算已通过 autonomous_helpers.sync_budget_from_orchestrator() 累加，
        # 由后续 _save_state() 统一同步到 state.total_cost_usd（避免多处赋值导致不一致）

        if collected_run.average_duration is not None and collected_run.max_duration is not None:
            with self._state_lock:
                self._state.task_stats_avg_duration = collected_run.average_duration
                self._state.task_stats_max_duration = collected_run.max_duration
                logger.debug(
                    "任务统计: %d 个任务, avg=%.1fs, max=%.1fs",
                    len(orch.results), self._state.task_stats_avg_duration, self._state.task_stats_max_duration,
                )

        merged_outputs = dict(correction_outputs)
        merged_outputs.update(task_outputs)
        return (merged_outputs, [*correction_errors, *task_errors], run_info, orch.results)

    # ------------------------------------------------------------------
    # Dashboard 数据推送
    # ------------------------------------------------------------------
    def _push_dashboard_state(self, current_phase: Phase | None = None) -> None:
        """将当前运行状态推送到 dashboard 数据源。"""
        try:
            with self._state_lock:
                state = self._state
            phases_info = []
            for p in state.phases:
                info: dict = {"name": p.name, "status": p.status.value}
                if p.review_result:
                    info["score"] = round(p.review_result.score, 2)
                if p.started_at:
                    info["started_at"] = p.started_at.isoformat()
                if p.completed_at:
                    elapsed = (p.completed_at - p.started_at).total_seconds() if p.started_at else 0
                    info["completed_at"] = p.completed_at.isoformat()
                    info["elapsed_seconds"] = round(elapsed, 1)
                phases_info.append(info)

            # 收集分数趋势
            score_trend: list[float] = []
            if current_phase:
                phase_records = [
                    r for r in state.iteration_history
                    if r.phase_id == current_phase.id
                ]
                score_trend = [r.score for r in phase_records]

            # 收集最近错误
            recent_errors: list[str] = []
            if current_phase:
                handoff = self._last_handoff.get(current_phase.id)
                if handoff:
                    for te in handoff.task_errors[:5]:
                        recent_errors.append(f"[{te.task_id}] {te.error[:150]}")

            update_dashboard_state({
                "goal": state.goal_text[:200] if state.goal_text else "",
                "run_status": state.status.value if state.status else "UNKNOWN",
                "cost_usd": round(state.total_cost_usd, 4),
                "phases": phases_info,
                "current_phase": current_phase.name if current_phase else "",
                "current_iteration": current_phase.iteration if current_phase else 0,
                "score_trend": [round(s, 2) for s in score_trend],
                "recent_errors": recent_errors,
            })
        except Exception as e:
            logger.debug("Dashboard 状态推送失败（非致命）: %s", e)

    # ------------------------------------------------------------------
    # 跨迭代学习记忆
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_pattern_hash(phase_name: str, error_text: str) -> str:
        """计算错误模式的哈希值，用于学习记忆的 key。"""
        # 规范化：去掉行号、路径前缀等易变部分
        import re
        normalized = re.sub(r'line \d+', 'line N', error_text)
        normalized = re.sub(r'[A-Z]:\\[^\s]+', '<path>', normalized)
        normalized = re.sub(r'/[^\s]+/', '<path>/', normalized)
        key = f"{phase_name}::{normalized[:200]}"
        return hashlib.md5(key.encode()).hexdigest()

    def _save_iteration_learning(
        self, phase: Phase, review: ReviewResult,
        task_errors: list[TaskError], success: bool,
    ) -> None:
        """将本次迭代的经验保存到学习记忆。"""
        if not task_errors and not success:
            return  # 没有错误信息也没成功，无可学习内容

        try:
            if success:
                # 成功时：记录"什么方法有效"
                pattern = f"phase:{phase.name}|verdict:{review.verdict.value}"
                resolution = (
                    f"score={review.score:.2f}, "
                    f"approach: {review.summary[:300] if review.summary else 'N/A'}"
                )
                ph = self._compute_pattern_hash(phase.name, pattern)
                self._store.save_learning(ph, pattern, resolution, success=True)
            else:
                # 失败时：记录每个错误模式
                for te in task_errors[:5]:  # 最多记录 5 个
                    ph = self._compute_pattern_hash(phase.name, te.error)
                    pattern = f"phase:{phase.name}|task:{te.task_id}|error:{te.error[:200]}"
                    resolution = (
                        f"verdict={review.verdict.value}, score={review.score:.2f}, "
                        f"feedback: {review.summary[:200] if review.summary else 'N/A'}"
                    )
                    self._store.save_learning(ph, pattern, resolution, success=False)
        except Exception as e:
            logger.debug("保存学习记忆失败（非致命）: %s", e)

    def _query_relevant_learnings(self, phase: Phase, task_errors: list[TaskError]) -> str:
        """查询与当前阶段相关的学习记忆，返回可注入 prompt 的文本。"""
        lessons: list[str] = []
        try:
            # 查询阶段级别的成功经验
            phase_pattern = f"phase:{phase.name}|verdict:pass"
            ph = self._compute_pattern_hash(phase.name, phase_pattern)
            best = self._store.get_best_resolution(ph)
            if best:
                lessons.append(f"✓ 历史成功经验: {best}")

            # 查询当前错误的历史记录
            for te in task_errors[:3]:
                ph = self._compute_pattern_hash(phase.name, te.error)
                learning = self._store.get_learning(ph)
                if learning:
                    ratio = learning['success_count'] / max(learning['success_count'] + learning['fail_count'], 1)
                    if ratio > 0.5:
                        lessons.append(f"✓ 类似错误的有效方案 (成功率{ratio:.0%}): {learning['resolution']}")
                    else:
                        lessons.append(f"✗ 类似错误的无效方案 (成功率{ratio:.0%}): {learning['resolution']}，请避免")
        except Exception as e:
            logger.debug("查询学习记忆失败（非致命）: %s", e)

        if not lessons:
            return ""
        return "\n## 跨迭代学习记忆\n" + "\n".join(lessons) + "\n"

    # ------------------------------------------------------------------
    # 动态重规划
    # ------------------------------------------------------------------
    def _maybe_replan(
        self,
        failed_phase: Phase,
        completed_ids: set[str],
    ) -> list[Phase] | None:
        """当阶段失败时，尝试动态重规划剩余阶段。

        策略：
        1. 收集已完成阶段的摘要和失败阶段的错误信息
        2. 调用 GoalDecomposer 重新分解剩余目标
        3. 返回新的阶段列表（仅替换 PENDING 阶段），或 None 表示不重规划

        Returns:
            新的 Phase 列表（替换剩余 PENDING 阶段），或 None。
        """
        # 只在有剩余 PENDING 阶段时才重规划
        pending_phases = [
            p for p in self._state.phases
            if p.status == PhaseStatus.PENDING and p.id != failed_phase.id
        ]
        if not pending_phases:
            logger.debug("无剩余 PENDING 阶段，跳过重规划")
            return None

        fingerprint = self._reserve_replan_attempt(failed_phase)
        if fingerprint is None:
            return None

        # 构建上下文
        completed_summaries = []
        for p in self._state.phases:
            if p.id in completed_ids:
                summary = f"- [{p.id}] {p.name}: 已完成"
                if p.task_outputs:
                    first_output = next(iter(p.task_outputs.values()), "")
                    summary += f" (输出摘要: {str(first_output)[:200]})"
                completed_summaries.append(summary)

        failure_info = (
            f"阶段 [{failed_phase.id}] {failed_phase.name} 失败。\n"
            f"失败分类: {failed_phase.last_classification.category.value if failed_phase.last_classification else 'unknown'}\n"
            f"反馈: {failed_phase.last_classification.feedback[:500] if failed_phase.last_classification else 'N/A'}"
        )

        replan_context = (
            f"{self._state.project_context}\n\n"
            f"## 执行进度\n"
            f"已完成阶段:\n{''.join(completed_summaries) or '无'}\n\n"
            f"## 失败信息\n{failure_info}\n\n"
            f"## 剩余待执行阶段（需要重新规划）\n"
            + "\n".join(f"- [{p.id}] {p.name}: {p.description}" for p in pending_phases)
        )

        logger.info("触发动态重规划，剩余 %d 个 PENDING 阶段", len(pending_phases))

        try:
            new_phases = self._decomposer.decompose(
                goal=f"基于已完成的工作和失败信息，重新规划剩余任务以完成目标: {self._goal}",
                project_context=replan_context,
                task_contract=self._task_contract,
                architecture_contract=self._architecture_contract,
            )

            if self._task_contract:
                new_phases = self._closure_planner.plan_phases(new_phases, self._task_contract)

            if not new_phases:
                logger.warning("重规划返回空阶段列表")
                return None

            # 为新阶段生成不冲突的 ID
            existing_ids = {p.id for p in self._state.phases}
            for i, np in enumerate(new_phases):
                if np.id in existing_ids:
                    np.id = f"replan_{failed_phase.id}_{i}"

            logger.info("重规划成功，生成 %d 个新阶段", len(new_phases))
            return new_phases

        except Exception as e:
            logger.warning("动态重规划失败: %s，继续原计划 (fingerprint=%s)", e, fingerprint)
            return None

    def _canonical_phase_lineage(self, phase_id: str) -> str:
        lineage = phase_id
        while lineage.startswith("replan_"):
            lineage = lineage[len("replan_"):]
            if "_" not in lineage:
                break
            parent, suffix = lineage.rsplit("_", 1)
            if not suffix.isdigit():
                break
            lineage = parent
        return lineage

    @staticmethod
    def _normalize_failure_text(text: str) -> str:
        normalized = re.sub(r"\s+", " ", (text or "").strip().lower())
        normalized = re.sub(r"0x[0-9a-f]+", "0xaddr", normalized)
        normalized = re.sub(r"\b\d+\b", "#", normalized)
        return normalized[:400]

    def _build_replan_fingerprint(self, failed_phase: Phase) -> str:
        classification = failed_phase.last_classification
        category = classification.category.value if classification else "unknown"
        signals: list[str] = []

        if classification and classification.feedback:
            signals.append(classification.feedback)

        handoff = self._last_handoff.get(failed_phase.id)
        if handoff:
            signals.extend(err.error for err in handoff.task_errors[:3] if err.error)

        metadata_errors = failed_phase.metadata.get("_last_task_errors")
        if isinstance(metadata_errors, list):
            signals.extend(str(err) for err in metadata_errors[:3] if err)

        if not signals:
            signals.append(failed_phase.name)

        normalized = " | ".join(self._normalize_failure_text(item) for item in signals if item)
        digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
        lineage = self._canonical_phase_lineage(failed_phase.id)
        return f"{lineage}:{category}:{digest}"

    def _reserve_replan_attempt(self, failed_phase: Phase) -> str | None:
        classification = failed_phase.last_classification
        if classification and classification.category in _NON_REPLANNABLE_REPLAN_CATEGORIES:
            logger.warning(
                "阶段 '%s' 属于不可重规划失败 (%s)，跳过重规划",
                failed_phase.id,
                classification.category.value,
            )
            return None

        fingerprint = self._build_replan_fingerprint(failed_phase)
        attempts = self._replan_fingerprint_attempts.get(fingerprint, 0)
        if attempts >= 1:
            logger.warning(
                "阶段 '%s' 的失败指纹已重规划过，停止重复重规划: %s",
                failed_phase.id,
                fingerprint,
            )
            return None

        self._replan_fingerprint_attempts[fingerprint] = attempts + 1
        return fingerprint

    @staticmethod
    def _summarize_task_errors(task_results: dict) -> list[str]:
        errors: list[str] = []
        for result in task_results.values():
            error = getattr(result, "error", "") or ""
            if error:
                errors.append(error.strip())
        return errors[:5]

    @staticmethod
    def _is_downstream_cancellation_error(error: str) -> bool:
        normalized = (error or "").strip().lower()
        return normalized.startswith("cancelled due to ") or normalized.startswith("canceled due to ")

    def _execute_phases_parallel(self) -> None:
        """基于依赖图并行执行阶段，依赖已满足的阶段同时启动。"""
        completed_ids: set[str] = set()
        failed = False

        logger.debug("[DEBUG] >>> _execute_phases_parallel 开始")

        # 恢复模式：跳过已完成/已跳过的阶段
        for phase in self._state.phases:
            if self._is_phase_already_done(phase):
                completed_ids.add(phase.id)
                logger.info("阶段 '%s' 已完成，跳过", phase.name)
                logger.debug("[DEBUG] >>> 跳过已完成阶段: %s (%s)", phase.name, phase.id)

        logger.debug("[DEBUG] >>> 已完成阶段数: %d, 总阶段数: %d", len(completed_ids), len(self._state.phases))

        with ThreadPoolExecutor(max_workers=self._auto_config.phase_parallelism) as pool:
            running: dict[Future, Phase] = {}

            while not failed:
                # 检查全局收敛（传入当前活跃阶段以支持阶段级统计检查）
                logger.debug("[DEBUG] >>> 检查全局收敛...")
                _active_phase = next(
                    (p for p in self._state.phases if p.status == PhaseStatus.RUNNING),
                    None,
                )
                signal = self._convergence.check(self._state, _active_phase)
                if signal.should_stop:
                    logger.info("全局收敛触发，停止执行: %s", signal.reason)
                    incomplete_exists = any(
                        phase.status not in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED)
                        for phase in self._state.phases
                    )
                    if incomplete_exists:
                        self._state.status = GoalStatus.SAFE_STOP
                        self._state.stop_reason = signal.reason
                        self._state.safe_stop_reason = SafeStopReason.LOW_SCORE
                        self._state.failure_categories['safe_stop'] = self._state.failure_categories.get('safe_stop', 0) + 1
                    break

                if self._runtime_layout:
                    catastrophic = self._catastrophic_guard.check(self._runtime_layout)
                    if catastrophic.is_catastrophic:
                        self._state.status = GoalStatus.CATASTROPHIC_STOP
                        self._state.stop_reason = catastrophic.reason
                        self._state.failure_categories['catastrophic_stop'] = self._state.failure_categories.get('catastrophic_stop', 0) + 1
                        failed = True
                        break

                # 运行时资源检查
                res_ok, res_msg = self._check_resources()
                if not res_ok:
                    self._record_diagnostic(
                        stage='resource_check',
                        exit_status='resource_exhausted',
                        error_detail=res_msg,
                    )
                    try:
                        from .notification import get_notifier
                        get_notifier().critical("资源不足，自主编排终止", detail=res_msg)
                    except Exception:
                        pass
                    # 取消所有尚未开始的 futures，避免资源不足时继续提交新任务
                    for fut in list(running.keys()):
                        fut.cancel()
                    self._state.status = GoalStatus.FAILED
                    failed = True
                    break

                # 找出就绪阶段：依赖已满足 + 未开始 + 未在运行中
                running_ids = {p.id for p in running.values()}
                ready = [
                    p for p in self._state.phases
                    if p.status == PhaseStatus.PENDING
                    and p.id not in running_ids
                    and all(dep in completed_ids for dep in p.depends_on_phases)
                ]

                logger.debug(
                    "[DEBUG] >>> 就绪阶段数: %d, 运行中: %d, 已完成: %d",
                    len(ready), len(running), len(completed_ids),
                )
                for p in ready:
                    logger.debug("[DEBUG] >>>   就绪: %s (%s)", p.name, p.id)

                # 提交就绪阶段到线程池
                for phase in ready:
                    logger.info("提交阶段 '%s' (%s) 到并行执行池", phase.name, phase.id)
                    logger.debug("[DEBUG] >>> 提交阶段到线程池: %s (%s)", phase.name, phase.id)
                    future = pool.submit(self._execute_phase_with_review, phase)
                    running[future] = phase

                # 没有运行中也没有就绪的 → 结束
                if not running:
                    logger.debug("[DEBUG] >>> 没有运行中也没有就绪的阶段，退出循环")
                    break

                # 等待至少一个完成
                logger.debug("[DEBUG] >>> 等待 %d 个运行中的阶段完成...", len(running))
                done_futures = set()
                for done_future in as_completed(list(running.keys())):
                    done_futures.add(done_future)
                    # 只处理第一个完成的，然后回到外层循环重新评估就绪阶段
                    break

                # 处理已完成的 future
                for done_future in done_futures:
                    done_phase = running.pop(done_future)

                    # 处理异常
                    exc = done_future.exception()
                    if exc:
                        if isinstance(exc, _PoolSwitchRequested):
                            self.requested_exit_code = exc.exit_code
                            for pending in list(running.keys()):
                                pending.cancel()
                            failed = True
                            break
                        logger.error("阶段 '%s' 异常: %s", done_phase.name, exc)
                        done_phase.status = PhaseStatus.FAILED

                    # 立即同步成本到 state（不等 _save_state）
                    with self._state_lock:
                        self._state.total_cost_usd = self._budget.spent

                    # 落盘
                    self._save_state()

                    if done_phase.status == PhaseStatus.COMPLETED:
                        completed_ids.add(done_phase.id)
                    elif done_phase.status == PhaseStatus.FAILED:
                        logger.warning("阶段 '%s' 失败，继续执行后续阶段", done_phase.id)

                        # 更新失败分类计数
                        if done_phase.last_classification:
                            category = done_phase.last_classification.category.value
                        else:
                            category = 'unknown_phase_failure'
                        with self._state_lock:
                            self._state.failure_categories[category] = (
                                self._state.failure_categories.get(category, 0) + 1
                            )
                        logger.debug("失败分类更新: %s", category)

                        # 动态重规划：尝试替换剩余 PENDING 阶段
                        new_phases = self._maybe_replan(done_phase, completed_ids)
                        if new_phases:
                            # 移除旧的 PENDING 阶段，替换为新阶段
                            self._state.phases = [
                                p for p in self._state.phases
                                if p.status != PhaseStatus.PENDING
                            ] + new_phases
                            self._save_state()
                            logger.info(
                                "动态重规划完成，替换为 %d 个新阶段",
                                len(new_phases),
                            )
                if self.requested_exit_code != 0:
                    break


    def _collect_and_review(
        self,
        phase: Phase,
        task_outputs: dict,
    ) -> tuple[ReviewResult, QualityGateResult | None, bool]:
        """收集结果并执行审查（门禁 + 回归检测 + AI 审查）。

        Args:
            phase: 当前阶段
            task_outputs: 任务输出字典

        Returns:
            (ReviewResult, QualityGateResult | None, regression_detected)
        """
        # 质量门禁：在 AI 审查前执行外部硬检查
        gate_result = self._run_quality_gate()

        # 回归检测：如果有基线，检查之前通过的命令是否仍然通过
        regression_detected = False
        if gate_result and self._auto_config.regression.enabled:
            regression_detected = self._check_regression(phase.id, gate_result)

        # 审查
        self._state.status = GoalStatus.REVIEWING

        if gate_result and not gate_result.passed and self._auto_config.quality_gate.skip_review_on_failure:
            # 门禁失败且配置为跳过审查 → 直接构造 MAJOR_ISSUES 结果
            review = ReviewResult(
                phase_id=phase.id,
                verdict=ReviewVerdict.MAJOR_ISSUES,
                score=0.4,
                summary=f"质量门禁未通过: {gate_result.summary}",
            )
            logger.warning("质量门禁失败，跳过 AI 审查: %s", gate_result.summary)
        else:
            review = self._review_engine.review_phase(
                phase=phase,
                goal_text=self._goal,
                task_outputs=task_outputs,
                task_contract=self._task_contract,
                repo_profile=self._repo_profile,
                verification_plan=self._verification_plan,
                architecture_contract=self._architecture_contract,
            )
            # 如果门禁失败但仍做了 AI 审查，将门禁结果注入审查摘要
            if gate_result and not gate_result.passed:
                review.summary = f"[门禁失败] {gate_result.summary}\n{review.summary}"
                # 门禁失败时，分数上限为 0.69（不允许标记为通过）
                review.score = min(review.score, 0.69)
                if review.verdict == ReviewVerdict.PASS:
                    review.verdict = ReviewVerdict.MAJOR_ISSUES

        # 回归约束：如果检测到回归，降级审查结果
        if regression_detected and self._auto_config.regression.block_on_regression:
            review.summary = f"[回归检测] 之前通过的门禁命令现在失败了\n{review.summary}"
            review.score = min(review.score, 0.59)
            if review.verdict in (ReviewVerdict.PASS, ReviewVerdict.MINOR_ISSUES):
                review.verdict = ReviewVerdict.MAJOR_ISSUES
                logger.warning("回归检测触发，审查结果降级为 MAJOR_ISSUES")

        review = self._apply_architecture_execution_constraints(phase, review)

        return review, gate_result, regression_detected

    def _handle_iteration_result(
        self,
        phase: Phase,
        review: ReviewResult,
        task_errors: list[TaskError],
        task_results: dict,
        gate_result: QualityGateResult | None,
        regression_detected: bool,
        classification: FailureClassification | None,
    ) -> bool:
        """处理迭代结果：记录历史、收敛检测、恶化检测、判定、纠正动作。

        Args:
            phase: 当前阶段
            review: 审查结果
            task_errors: 任务错误列表
            task_results: 任务结果字典
            gate_result: 门禁结果
            regression_detected: 是否检测到回归
            classification: 失败分类（可能为 None）

        Returns:
            是否继续迭代（True=继续，False=停止）
        """
        phase.review_result = review

        # 记录迭代历史（填充所有新字段）
        history_update = _record_iteration_history_helper(
            self._state.iteration_history,
            total_iterations=self._state.total_iterations,
            phase=phase,
            review=review,
            task_errors=task_errors,
            classification=classification,
            gate_passed=gate_result.passed if gate_result else None,
            regression_detected=regression_detected,
            duration_seconds=phase.execution_metrics.get("duration_seconds", 0.0),
        )
        record = history_update.record

        # 更新阶段最高分快照（用于 rollback）
        if review.score > phase.best_score:
            phase.best_score = review.score
            phase.best_outputs = dict(phase.task_outputs) if phase.task_outputs else {}

        # 收敛检测
        signal = self._convergence.check(self._state, phase)

        # 平台期检测：连续 2 轮分数差值 < 0.01 时注入策略调整指令
        phase_records = history_update.phase_records
        plateau = _detect_plateau_strategy_hint_helper(phase_records)
        if plateau is not None:
            logger.warning(
                "平台期检测: 阶段 '%s' 连续 %d 轮分数停滞 (最近 %.2f → %.2f)，注入策略调整",
                phase.name, plateau.plateau_count, plateau.previous_score, plateau.current_score,
            )
            phase.strategy_hint = plateau.strategy_hint

        # 恶化检测（在收敛检测后、判定前）
        deterioration = self._deterioration.check(self._state, phase)
        _sync_iteration_record_deterioration_helper(
            record,
            deterioration_level=deterioration.level,
            regression_detected=regression_detected,
        )

        if deterioration.level == DeteriorationLevel.CRITICAL:
            logger.error(
                "危急恶化: %s → 建议 %s", deterioration.reason, deterioration.recommended_action,
            )
            from .notification import get_notifier
            get_notifier().critical(
                "危急恶化检测触发",
                phase=phase.name,
                reason=deterioration.reason,
                action=deterioration.recommended_action,
            )
            # ---- CRITICAL 降级：切换保守模型 + 降低并行度 ----
            old_model = self._auto_config.execution_model
            old_parallelism = self._auto_config.phase_parallelism
            model_downgrade = {"opus": "sonnet", "sonnet": "haiku"}
            new_model = model_downgrade.get(old_model, old_model)
            new_parallelism = max(2, old_parallelism // 2)
            self._auto_config.execution_model = new_model
            self._auto_config.phase_parallelism = new_parallelism
            self._auto_config.degradation_level += 1
            logger.warning(
                "CRITICAL 降级: execution_model %s → %s, phase_parallelism %d → %d, degradation_level %d",
                old_model, new_model, old_parallelism, new_parallelism,
                self._auto_config.degradation_level,
            )

            # CRITICAL 恶化触发 GoalState 级回滚到最近健康快照
            if self._last_healthy_snapshot is not None:
                logger.info(
                    "CRITICAL 恶化触发 GoalState 级回滚 (阶段 '%s'，回滚前 %d 个 phase)",
                    phase.name, len(self._state.phases),
                )
                try:
                    self._state.restore_snapshot(self._last_healthy_snapshot)
                    logger.info("GoalState 已回滚到最近健康快照")
                except Exception as rollback_err:
                    logger.error("GoalState 回滚失败: %s", rollback_err)
            else:
                logger.warning("CRITICAL 恶化但无健康快照可回滚")
            # 创建失败分类
            phase.last_classification = FailureClassification(
                category=FailureCategory.LOGIC_ERROR,
                retriable=False,
                feedback=f"危急恶化: {deterioration.reason}. 建议: {deterioration.recommended_action}",
            )
            phase.status = PhaseStatus.FAILED
            return False
        elif deterioration.level == DeteriorationLevel.SERIOUS:
            logger.warning(
                "严重恶化: %s → %s", deterioration.reason, deterioration.recommended_action,
            )
            from .notification import get_notifier
            get_notifier().warning(
                "严重恶化检测触发",
                phase=phase.name,
                reason=deterioration.reason,
                action=deterioration.recommended_action,
            )

            # ---- SERIOUS 降级：降低并行度 ----
            old_parallelism = self._auto_config.phase_parallelism
            new_parallelism = max(2, old_parallelism // 2)
            self._auto_config.phase_parallelism = new_parallelism
            self._auto_config.degradation_level += 1
            logger.warning(
                "SERIOUS 降级: phase_parallelism %d → %d, degradation_level %d",
                old_parallelism, new_parallelism,
                self._auto_config.degradation_level,
            )

            # ---- 策略切换落地 ----
            action = deterioration.recommended_action

            if action == "switch_strategy":
                # 注入策略切换提示到 phase，下次 DAG 生成时会读取
                phase.strategy_hint = (
                    f"[策略切换] 前序迭代持续恶化 ({deterioration.reason})。"
                    f"请采用完全不同的实现方案。"
                    f"恶化维度: {', '.join(deterioration.correlated_dimensions or [])}"
                )
                logger.info("已注入策略切换提示到阶段 '%s'", phase.name)

            elif action == "rollback":
                # 回滚到历史最高分的输出
                if phase.best_score > 0 and phase.best_outputs:
                    phase.task_outputs = dict(phase.best_outputs)
                    logger.info(
                        "已回滚阶段 '%s' 到最高分 %.2f 的输出",
                        phase.name, phase.best_score,
                    )
                    phase.strategy_hint = (
                        f"[回滚后重试] 已回滚到分数 {phase.best_score:.2f} 的版本。"
                        f"请在此基础上做增量改进，避免重蹈覆辙: {deterioration.reason}"
                    )
                else:
                    logger.warning("无可回滚的历史输出，降级为策略切换")
                    phase.strategy_hint = (
                        f"[策略切换-降级] 无历史快照可回滚。{deterioration.reason}。"
                        f"请采用完全不同的实现方案。"
                    )

            elif action == "escalate":
                # 升级为 CRITICAL 处理：标记失败
                logger.error("严重恶化升级为失败: %s", deterioration.reason)
                phase.last_classification = FailureClassification(
                    category=FailureCategory.LOGIC_ERROR,
                    retriable=False,
                    feedback=f"严重恶化升级: {deterioration.reason}",
                )
                phase.status = PhaseStatus.FAILED
                return False

        # 判定
        if review.verdict == ReviewVerdict.PASS or signal.should_stop:
            if review.verdict == ReviewVerdict.PASS:
                logger.info("阶段 '%s' 审查通过 (score=%.2f)", phase.name, review.score)
                # 保存回归基线
                if gate_result and gate_result.passed:
                    self._save_baseline(phase.id, gate_result, review.score)
                # 保存成功经验到学习记忆
                self._save_iteration_learning(phase, review, task_errors, success=True)
                phase.status = PhaseStatus.COMPLETED
            else:
                logger.info("阶段 '%s' 收敛停止: %s", phase.name, signal.reason)
                self._state.failure_categories['safe_stop'] = self._state.failure_categories.get('safe_stop', 0) + 1
                self._state.stop_reason = signal.reason
                self._state.safe_stop_reason = SafeStopReason.LOW_SCORE
                phase.status = PhaseStatus.FAILED
            return False

        if review.verdict == ReviewVerdict.MINOR_ISSUES:
            logger.info("阶段 '%s' 有小问题，执行修正", phase.name)
            # 保存基线（minor issues 也算基本通过）
            if gate_result and gate_result.passed:
                self._save_baseline(phase.id, gate_result, review.score)
            # 保存成功经验到学习记忆
            self._save_iteration_learning(phase, review, task_errors, success=True)
            self._run_corrections(phase, review)
            phase.status = PhaseStatus.COMPLETED
            return False

        if review.verdict == ReviewVerdict.BLOCKED:
            logger.error("阶段 '%s' 被阻塞，需要人工介入", phase.name)
            classification = self._classify_failure(
                phase, task_results, gate_result=gate_result, review=review,
            )
            phase.last_classification = classification
            # 保存失败经验到学习记忆
            self._save_iteration_learning(phase, review, task_errors, success=False)
            phase.status = PhaseStatus.FAILED
            return False

        # MAJOR_ISSUES / CRITICAL → 分类失败并决定重试策略
        classification = self._classify_failure(
            phase, task_results, gate_result=gate_result, review=review,
        )
        phase.last_classification = classification
        # 保存失败经验到学习记忆
        self._save_iteration_learning(phase, review, task_errors, success=False)

        if not classification.retriable:
            logger.error(
                "阶段 '%s' 审查失败且不可重试 (%s)",
                phase.id, classification.category.value,
            )
            phase.status = PhaseStatus.FAILED
            return False

        logger.warning(
            "阶段 '%s' 审查: %s (score=%.2f, 分类=%s)，准备第 %d 次迭代",
            phase.name, review.verdict.value, review.score,
            classification.category.value, phase.iteration + 1,
        )

        # 构建 Handoff 包，传递给下一次迭代
        self._build_handoff(
            phase=phase,
            review=review,
            classification=classification,
            task_errors=task_errors,
            gate_result=gate_result,
            regression_detected=regression_detected,
        )

        self._state.status = GoalStatus.ITERATING
        return True  # 继续迭代

    def _execute_phase_with_review(self, phase: Phase) -> None:
        """Execute a phase with review loop: run → review → iterate if needed."""
        logger.debug("[DEBUG] >>> _execute_phase_with_review 开始: %s (%s)", phase.name, phase.id)
        logger.info("=" * 40)
        logger.info("开始阶段: %s (%s)", phase.name, phase.id)
        logger.info("=" * 40)

        phase.status = PhaseStatus.RUNNING
        phase.started_at = datetime.now()
        # 初始化阶段执行指标（记录 CLI 调用基线快照）
        _phase_start = time.monotonic()
        _cli_calls_baseline = self._budget.total_request_count
        phase.execution_metrics = {
            "started_at": datetime.now().isoformat(),
            "model_used": self._auto_config.execution_model,
        }

        def _flush_exec_metrics() -> None:
            """写入阶段执行结束指标，并记录 phase_history。"""
            _finalize_phase_execution_metrics_helper(
                self._state,
                phase,
                finished_at=datetime.now(),
                duration_seconds=time.monotonic() - _phase_start,
            )

        classification = None

        while phase.iteration < phase.max_iterations:
            deadline_exceeded, deadline_reason = self._check_phase_deadline(phase)
            if deadline_exceeded:
                logger.error("阶段 '%s' 超过截止时间，终止迭代: %s", phase.name, deadline_reason)
                self._record_diagnostic(
                    stage=f'deadline_check:{phase.id}',
                    exit_status='deadline_exceeded',
                    error_detail=deadline_reason,
                )
                phase.last_classification = FailureClassification(
                    category=FailureCategory.TIMEOUT,
                    retriable=False,
                    feedback=deadline_reason,
                )
                phase.status = PhaseStatus.FAILED
                self._state.status = GoalStatus.TIMEOUT
                _flush_exec_metrics()
                return

            # 运行时资源检查（每次迭代前）
            res_ok, res_msg = self._check_resources()
            if not res_ok:
                logger.error("阶段 '%s' 资源不足，终止迭代: %s", phase.name, res_msg)
                self._record_diagnostic(
                    stage=f'resource_check:{phase.id}',
                    exit_status='resource_exhausted',
                    error_detail=res_msg,
                )
                phase.last_classification = FailureClassification(
                    category=FailureCategory.TRANSIENT,
                    retriable=False,
                    feedback=f"系统资源不足: {res_msg}",
                )
                phase.status = PhaseStatus.FAILED
                _flush_exec_metrics()
                return

            phase.iteration += 1
            self._heartbeat.touch()
            # 计入全局迭代计数（包括首次尝试和重试）
            with self._state_lock:
                self._state.total_iterations += 1

            logger.info("阶段 '%s' 第 %d 次迭代", phase.name, phase.iteration)
            self._push_dashboard_state(current_phase=phase)

            # 生成并执行 DAG，收集结果
            result = self._generate_and_run_dag(phase)
            if result is None:
                # DAG 生成失败或为空，phase.status 已设置
                _flush_exec_metrics()
                return

            task_outputs, task_errors, run_info, task_results = result
            # 累计执行指标：实际 CLI 调用次数（基于 BudgetTracker 快照差值）
            phase.execution_metrics["cli_calls"] = (
                self._budget.total_request_count - _cli_calls_baseline
            )
            phase.execution_metrics["total_cost_usd"] = round(self._budget.spent, 4)
            # 汇总 token 用量（从 task_results 提取 input + output tokens）
            _prev_tokens = phase.execution_metrics.get("total_tokens", 0)
            _iter_tokens = sum(
                (tr.token_input or 0) + (tr.token_output or 0)
                for tr in task_results.values()
            )
            phase.execution_metrics["total_tokens"] = _prev_tokens + _iter_tokens
            phase.task_outputs = task_outputs
            # 保存每个任务的实际 status，供 _aggregate_task_stats 准确统计
            phase.task_result_statuses = {
                tid: tr.status.value if hasattr(tr.status, 'value') else str(tr.status)
                for tid, tr in task_results.items()
            }
            self._capture_architecture_execution_report(phase, phase.task_outputs)

            # DAG 执行失败检查 + 失败分类
            # 即使 run_info.status 不是 FAILED，也要检查 task_outputs 是否为空
            # （所有任务都失败但 _finalize 未正确标记的情况）
            raw_task_count = len(task_results)
            has_any_success = len(task_outputs) > 0

            if not has_any_success and raw_task_count > 0:
                logger.error(
                    "阶段 '%s': %d 个任务执行但 0 个成功输出，强制进入失败路径 (run_info.status=%s)",
                    phase.id, raw_task_count, run_info.status.value,
                )
                # 强制将 run_info 视为 FAILED 以进入失败分类路径
                run_info = RunInfo(
                    run_id=run_info.run_id,
                    dag_name=run_info.dag_name,
                    dag_hash=run_info.dag_hash,
                    status=RunStatus.FAILED,
                    started_at=run_info.started_at,
                    finished_at=run_info.finished_at,
                    total_cost_usd=run_info.total_cost_usd,
                    pool_id=run_info.pool_id,
                    active_profile=run_info.active_profile,
                )

            if run_info.status == RunStatus.FAILED:
                primary_task_results = {
                    tid: tr for tid, tr in task_results.items()
                    if not self._is_downstream_cancellation_error(getattr(tr, "error", "") or "")
                }
                effective_task_results = primary_task_results or task_results
                total_count = len(effective_task_results)
                failed_count = sum(
                    1 for tr in effective_task_results.values()
                    if tr.status == TaskStatus.FAILED
                )

                # 分类失败
                classification = self._classify_failure(phase, task_results, gate_result=None)
                phase.last_classification = classification

                if not classification.retriable:
                    logger.error(
                        "阶段 '%s' 失败且不可重试 (%s): %s",
                        phase.id, classification.category.value, classification.feedback,
                    )
                    phase.status = PhaseStatus.FAILED
                    _flush_exec_metrics()
                    return

                # transient 失败：直接重试，不做审查
                if classification.category == FailureCategory.TRANSIENT:
                    phase.timeout_multiplier *= classification.adjust_timeout
                    logger.warning(
                        "阶段 '%s' 临时性失败，直接重试 (timeout x%.1f → 累积倍率 x%.1f)",
                        phase.id, classification.adjust_timeout, phase.timeout_multiplier,
                    )
                    continue

                # 超过80%任务失败 → 根本性问题，显式记录为 mass_failure
                if total_count > 0 and failed_count / total_count > 0.8:
                    logger.error(
                        "阶段 '%s' 超过80%%任务失败 (%d/%d)，标记为失败",
                        phase.id, failed_count, total_count,
                    )
                    phase.status = PhaseStatus.FAILED
                    # 显式设置 mass_failure 分类，避免依赖之前的 transient 分类
                    self._record_diagnostic(
                        stage=f'phase:{phase.id}',
                        exit_status='mass_failure',
                        error_detail=f"阶段 '{phase.id}' 超过80%任务失败 ({failed_count}/{total_count})",
                        start_time=phase.started_at,
                    )
                    _flush_exec_metrics()
                    return

            # 收集结果并审查
            review, gate_result, regression_detected = self._collect_and_review(
                phase=phase,
                task_outputs=phase.task_outputs,
            )

            # 审查分数良好时保存 GoalState 快照，供 CRITICAL 恶化时回滚
            if review.score >= 0.5:
                try:
                    self._last_healthy_snapshot = self._state.create_snapshot()
                    logger.debug(
                        "保存健康快照: 阶段 '%s' 分数 %.2f (快照大小 %d 字节)",
                        phase.name, review.score, len(str(self._last_healthy_snapshot)),
                    )
                except Exception as e:
                    logger.warning("保存健康快照失败: %s", e)

            # 处理迭代结果
            should_continue = self._handle_iteration_result(
                phase=phase,
                review=review,
                task_errors=task_errors,
                task_results=task_results,
                gate_result=gate_result,
                regression_detected=regression_detected,
                classification=classification,
            )

            if not should_continue:
                _flush_exec_metrics()
                self._push_dashboard_state(current_phase=phase)
                return

        # 超过最大迭代次数
        logger.warning("阶段 '%s' 达到最大迭代次数 (%d)", phase.name, phase.max_iterations)
        # 如果最后一次分数还算可以，标记完成
        if phase.review_result and phase.review_result.score >= 0.6 and self._phase_allows_completion(phase):
            logger.info("最终分数 %.2f 尚可，标记为完成", phase.review_result.score)
            phase.status = PhaseStatus.COMPLETED
        else:
            phase.last_classification = FailureClassification(
                category=FailureCategory.LOGIC_ERROR,
                retriable=False,
                feedback=f"达到最大迭代次数 {phase.max_iterations}，最终分数 {phase.review_result.score if phase.review_result else 0:.2f}",
            )
            phase.status = PhaseStatus.FAILED
        _flush_exec_metrics()
        self._push_dashboard_state(current_phase=phase)


    def _check_phase_deadline(self, phase: Phase) -> tuple[bool, str]:
        if datetime.now() < self._state.deadline:
            return False, ""

        remaining_iterations = max(0, phase.max_iterations - phase.iteration)
        reason = (
            f"阶段 '{phase.name}' 已超过全局截止时间 {self._state.deadline.isoformat()}，"
            f"剩余可迭代次数 {remaining_iterations}"
        )
        return True, reason


    def _build_handoff(
        self,
        phase: Phase,
        review: 'ReviewResult',
        classification: FailureClassification | None,
        task_errors: list[TaskError],
        gate_result: QualityGateResult | None,
        regression_detected: bool,
    ) -> None:
        """构建迭代间 Handoff 包，保存到 _last_handoff。"""
        # 收集分数趋势
        phase_records = [r for r in self._state.iteration_history if r.phase_id == phase.id]
        score_trend = [r.score for r in phase_records]

        # 判断趋势方向
        if len(score_trend) >= 2:
            if score_trend[-1] > score_trend[-2]:
                trend_direction = "improving"
            elif score_trend[-1] < score_trend[-2]:
                trend_direction = "declining"
            else:
                trend_direction = "stable"
        else:
            trend_direction = "stable"

        # 门禁失败命令
        gate_summary = ""
        gate_failed_commands: list[str] = []
        if gate_result:
            gate_summary = gate_result.summary
            gate_failed_commands = [
                r["command"] for r in gate_result.command_results if not r["passed"]
            ]

        # 回归命令
        regressed_commands: list[str] = []
        if regression_detected:
            baseline = self._baselines.get(phase.id)
            if baseline and gate_result:
                current_passed = {
                    r["command"] for r in gate_result.command_results if r["passed"]
                }
                regressed_commands = [
                    cmd for cmd in baseline.passed_commands if cmd not in current_passed
                ]

        architecture_report = self._get_architecture_execution_report(phase)
        handoff = IterationHandoff(
            iteration=phase.iteration,
            review_summary=review.summary,
            review_issues=list(review.issues),
            review_score=review.score,
            corrective_actions=list(review.corrective_actions),
            failure_category=classification.category.value if classification else "",
            failure_feedback=classification.feedback if classification else "",
            task_errors=task_errors,
            gate_summary=gate_summary,
            gate_failed_commands=gate_failed_commands,
            regression_detected=regression_detected,
            regressed_commands=regressed_commands,
            score_trend=score_trend,
            trend_direction=trend_direction,
            architecture_execution_summary=architecture_report.summary if architecture_report else "",
            architecture_gate_status=architecture_report.gate_status if architecture_report else "",
            architecture_unmet_cutover_gates=list(architecture_report.unmet_cutover_gates) if architecture_report else [],
            architecture_missing_evidence_refs=list(architecture_report.missing_evidence_refs) if architecture_report else [],
            architecture_missing_rollback_refs=list(architecture_report.missing_rollback_refs) if architecture_report else [],
            architecture_report_path=str(phase.metadata.get("architecture_execution_report_path", "") or ""),
        )

        self._last_handoff[phase.id] = handoff
        logger.info(
            "Handoff 已构建: phase=%s, score=%.2f, errors=%d, trend=%s",
            phase.id, review.score, len(task_errors), trend_direction,
        )

        # 持久化 Handoff 到 SQLite
        try:
            handoff_dict = {
                "iteration": handoff.iteration,
                "review_summary": handoff.review_summary,
                "review_score": handoff.review_score,
                "corrective_actions": [
                    {
                        "action_id": action.action_id,
                        "description": action.description,
                        "prompt_template": action.prompt_template,
                        "priority": action.priority,
                        "depends_on_actions": action.depends_on_actions,
                        "timeout": action.timeout,
                        "action_type": action.action_type,
                        "executor_config": action.executor_config,
                    }
                    for action in handoff.corrective_actions
                ],
                "failure_category": handoff.failure_category,
                "failure_feedback": handoff.failure_feedback,
                "gate_summary": handoff.gate_summary,
                "gate_failed_commands": handoff.gate_failed_commands,
                "regression_detected": handoff.regression_detected,
                "regressed_commands": handoff.regressed_commands,
                "score_trend": handoff.score_trend,
                "trend_direction": handoff.trend_direction,
                "architecture_execution_summary": handoff.architecture_execution_summary,
                "architecture_gate_status": handoff.architecture_gate_status,
                "architecture_unmet_cutover_gates": handoff.architecture_unmet_cutover_gates,
                "architecture_missing_evidence_refs": handoff.architecture_missing_evidence_refs,
                "architecture_missing_rollback_refs": handoff.architecture_missing_rollback_refs,
                "architecture_report_path": handoff.architecture_report_path,
                "review_issues": [
                    {"severity": i.severity, "category": i.category,
                     "description": i.description, "affected_files": i.affected_files,
                     "suggested_fix": i.suggested_fix}
                    for i in handoff.review_issues
                ],
                "task_errors": [
                    {"task_id": e.task_id, "error": e.error, "attempt": e.attempt}
                    for e in handoff.task_errors
                ],
            }
            self._store.save_handoff(phase.id, json.dumps(handoff_dict, ensure_ascii=False))
        except Exception as e:
            logger.warning("Handoff 持久化失败: %s", e)

    def _save_baseline(self, phase_id: str, gate_result: QualityGateResult, score: float) -> None:
        """保存回归基线：记录当前通过的门禁命令。"""
        passed_cmds = [r["command"] for r in gate_result.command_results if r["passed"]]
        if not passed_cmds:
            return
        baseline = RegressionBaseline(
            phase_id=phase_id,
            passed_commands=passed_cmds,
            score=score,
        )
        self._baselines[phase_id] = baseline
        logger.info(
            "保存回归基线: phase=%s, %d 个通过命令, score=%.2f",
            phase_id, len(passed_cmds), score,
        )

        # 持久化基线到 SQLite context_data 表
        try:
            baseline_dict = {
                "phase_id": phase_id,
                "passed_commands": passed_cmds,
                "score": score,
            }
            self._store.set_context(
                self._state.goal_id,
                f"_baseline_{phase_id}",
                json.dumps(baseline_dict, ensure_ascii=False),
            )
        except Exception as e:
            logger.warning("回归基线持久化失败: %s", e)

    def _check_regression(self, phase_id: str, gate_result: QualityGateResult) -> bool:
        """检查是否发生回归：之前通过的命令现在失败了。"""
        baseline = self._baselines.get(phase_id)
        if not baseline:
            return False

        current_passed = {r["command"] for r in gate_result.command_results if r["passed"]}
        regressed = []
        for cmd in baseline.passed_commands:
            if cmd not in current_passed:
                regressed.append(cmd)

        if regressed:
            logger.warning(
                "回归检测: phase=%s, %d 个之前通过的命令现在失败: %s",
                phase_id, len(regressed), regressed,
            )
            return True
        return False

    def _classify_failure(
        self,
        phase: Phase,
        task_results: dict,
        gate_result: QualityGateResult | None = None,
        review: 'ReviewResult | None' = None,
    ) -> FailureClassification:
        """对阶段失败进行分类，决定重试策略。"""

        # 1. 认证过期检测 → 尝试恢复后可重试
        auth_keywords = ["auth_expired:", "unauthorized", "401", "authentication", "token expired", "session expired"]
        for r in task_results.values():
            error = getattr(r, 'error', '') or ''
            if any(kw in error.lower() for kw in auth_keywords):
                recovered = self._handle_auth_failure()
                return FailureClassification(
                    category=FailureCategory.AUTH_EXPIRED,
                    retriable=recovered,
                    feedback="认证过期" + ("，已自动恢复" if recovered else "，需要人工介入"),
                )

        # 2. 审查判定为 BLOCKED → 不可重试
        if review and review.verdict == ReviewVerdict.BLOCKED:
            return FailureClassification(
                category=FailureCategory.BLOCKED,
                retriable=False,
                feedback=review.summary,
            )

        # 3. 门禁失败 → 可重试，注入门禁输出
        if gate_result and not gate_result.passed:
            failed_details = []
            for r in gate_result.command_results:
                if not r["passed"]:
                    detail = f"命令 `{r['command']}` 失败 (exit={r['exit_code']})"
                    if r.get("stderr"):
                        detail += f"\nstderr: {r['stderr'][-500:]}"
                    failed_details.append(detail)
            return FailureClassification(
                category=FailureCategory.QUALITY_GATE,
                retriable=True,
                feedback="质量门禁失败:\n" + "\n".join(failed_details),
            )

        # 4. 检查任务失败详情
        failed_tasks = {
            tid: r for tid, r in task_results.items()
            if hasattr(r, 'status') and r.status == TaskStatus.FAILED
        }
        if failed_tasks:
            task_errors = self._summarize_task_errors(failed_tasks)
            phase.metadata["_last_task_errors"] = task_errors
            primary_task_errors = [
                error for error in task_errors
                if not self._is_downstream_cancellation_error(error)
            ]
            signal_errors = primary_task_errors or task_errors

            bootstrap_errors = [
                error for error in signal_errors
                if any(keyword in error.lower() for keyword in _ENVIRONMENT_FAILURE_KEYWORDS)
            ]
            if bootstrap_errors:
                return FailureClassification(
                    category=FailureCategory.ENV_MISSING,
                    retriable=False,
                    feedback="执行环境/启动失败:\n" + "\n".join(f"- {error[:300]}" for error in bootstrap_errors[:3]),
                )

            transient_count = 0
            for error in signal_errors:
                normalized = error.lower()
                if any(keyword in normalized for keyword in _TRANSIENT_FAILURE_KEYWORDS):
                    transient_count += 1
            # 全部失败任务都是临时性的
            if transient_count == len(signal_errors) and transient_count > 0:
                return FailureClassification(
                    category=FailureCategory.TRANSIENT,
                    retriable=True,
                    feedback="临时性失败（超时/CLI 错误），原样重试",
                    adjust_timeout=1.5,  # 放大超时
                )

        # 5. 默认：逻辑错误 → 可重试，注入审查反馈
        feedback = ""
        if review:
            feedback = review.summary
            if review.issues:
                issue_lines = [f"- [{i.severity}] {i.description}" for i in review.issues[:5]]
                feedback += "\n具体问题:\n" + "\n".join(issue_lines)

        return FailureClassification(
            category=FailureCategory.LOGIC_ERROR,
            retriable=True,
            feedback=feedback,
        )

    def _handle_auth_failure(self) -> bool:
        """尝试恢复认证。

        Returns:
            True 表示认证已恢复可重试，False 表示需要人工介入。
        """
        logger.warning("检测到认证过期，尝试验证认证状态...")
        try:
            clean_env = {k: v for k, v in os.environ.items() if k != "CLAUDE_CLI"}
            result = subprocess.run(
                ["claude", "--version"],
                capture_output=True,
                text=True,
                timeout=15,
                shell=(sys.platform == "win32"),
                env=clean_env,
            )
            if result.returncode == 0:
                logger.info("认证验证通过，可以重试")
                return True
            else:
                logger.error("认证验证失败 (exit=%d): %s", result.returncode, result.stderr[:200])
        except Exception as e:
            logger.error("认证验证异常: %s", e)

        # 发送 CRITICAL 告警
        try:
            from .notification import get_notifier
            get_notifier().critical("认证过期且无法自动恢复，需要人工介入")
        except Exception:
            pass
        return False

    def _run_quality_gate(self) -> QualityGateResult | None:
        """并行执行质量门禁命令，返回结果。未配置或禁用时返回 None。"""
        gate = self._auto_config.quality_gate
        if self._verification_plan and (not gate.commands):
            run_result = VerificationRunner().run(
                self._verification_plan,
                timeout=self._config.verification.command_timeout,
            )
            return QualityGateResult(
                passed=run_result.passed,
                command_results=run_result.command_results,
                summary=run_result.summary,
            )

        if not gate.enabled or not gate.commands:
            return None

        logger.info("并行执行质量门禁 (%d 个命令)", len(gate.commands))

        def _run_one(cmd_str: str) -> dict:
            """执行单个门禁命令，返回结果 dict。"""
            logger.info("  门禁命令: %s", cmd_str)
            rendered_command = normalize_python_command(cmd_str)
            try:
                proc = subprocess.run(
                    rendered_command,
                    shell=True,
                    cwd=self._working_dir,
                    capture_output=True,
                    text=True,
                    timeout=gate.timeout,
                    encoding="utf-8",
                    errors="replace",
                )
                passed = proc.returncode == 0
                result = {
                    "command": rendered_command,
                    "exit_code": proc.returncode,
                    "stdout": proc.stdout[-2000:] if proc.stdout else "",
                    "stderr": proc.stderr[-2000:] if proc.stderr else "",
                    "passed": passed,
                }
            except subprocess.TimeoutExpired:
                result = {
                    "command": rendered_command,
                    "exit_code": -1,
                    "stdout": "",
                    "stderr": f"命令超时 ({gate.timeout}s)",
                    "passed": False,
                }
            except Exception as e:
                result = {
                    "command": rendered_command,
                    "exit_code": -1,
                    "stdout": "",
                    "stderr": str(e),
                    "passed": False,
                }

            if result["passed"]:
                logger.info("  [OK] 通过: %s", cmd_str)
            else:
                logger.warning("  [X] 失败: %s (exit_code=%s)", cmd_str, result["exit_code"])
            return result

        with ThreadPoolExecutor(max_workers=len(gate.commands)) as pool:
            futures = {pool.submit(_run_one, cmd): cmd for cmd in gate.commands}
            # 全局超时 = 单命令超时 × 命令数 + 60s 缓冲，防止线程池永久挂起
            global_timeout = gate.timeout * len(gate.commands) + 60
            collected_futures: set = set()
            command_results = []
            try:
                for future in as_completed(futures, timeout=global_timeout):
                    collected_futures.add(future)
                    command_results.append(future.result())
            except (FuturesTimeoutError, TimeoutError):
                # FuturesTimeoutError 兼容 Python 3.10-; 3.11+ 统一为内置 TimeoutError
                logger.error("质量门禁整体超时 (%ds)，取消剩余任务", global_timeout)
                for f, cmd in futures.items():
                    if f in collected_futures:
                        continue  # 已收集
                    f.cancel()
                    # 已完成但未被迭代到的 → 取结果；未完成的 → 超时占位
                    if f.done() and not f.cancelled():
                        try:
                            command_results.append(f.result(timeout=0))
                        except Exception:
                            command_results.append({
                                "command": cmd,
                                "exit_code": -1,
                                "stdout": "",
                                "stderr": f"全局超时后取结果异常",
                                "passed": False,
                            })
                    else:
                        command_results.append({
                            "command": cmd,
                            "exit_code": -1,
                            "stdout": "",
                            "stderr": f"全局超时 ({global_timeout}s)",
                            "passed": False,
                        })

        all_passed = all(r["passed"] for r in command_results)
        passed_count = sum(1 for r in command_results if r["passed"])
        total_count = len(command_results)
        summary = f"{passed_count}/{total_count} 门禁命令通过"
        if not all_passed:
            failed_cmds = [r["command"] for r in command_results if not r["passed"]]
            summary += f"，失败: {', '.join(failed_cmds)}"

        logger.info("质量门禁结果: %s", summary)
        return QualityGateResult(
            passed=all_passed,
            command_results=command_results,
            summary=summary,
        )

    def _execute_correction_dag(
        self,
        phase: Phase,
        review: 'ReviewResult',
    ) -> tuple[dict[str, object], list[TaskError]]:
        """Execute a correction DAG and return structured outputs plus failures."""
        if not review.corrective_actions:
            logger.info("无修正动作，跳过")
            return {}, []

        try:
            fix_dag = self._dag_generator.generate_correction_dag(
                phase=phase,
                review=review,
                project_context=self._state.project_context,
                task_contract=self._task_contract,
            )
        except Exception as e:
            logger.warning("修正 DAG 生成失败: %s，跳过修正", e)
            return {}, []

        if not fix_dag.tasks:
            return {}, []

        executed_run = _execute_orchestrator_run_helper(
            fix_dag,
            config=self._config,
            store=self._store,
            working_dir=self._working_dir,
            log_file=self._log_file,
            pool_runtime=self._pool_runtime,
            on_task_result=self._on_task_result_callback,
            log_task_count_message="执行 %d 个修正任务",
        )
        orch = executed_run.orchestrator
        if orch.requested_exit_code != 0:
            self.requested_exit_code = orch.requested_exit_code
            raise _PoolSwitchRequested(orch.requested_exit_code)

        collected_run = _collect_dag_run_artifacts_helper(
            phase,
            orch=orch,
            budget=self._budget,
            state=self._state,
            state_lock=self._state_lock,
            sync_state=True,
            failed_task_default_error="修正任务失败",
            budget_log_prefix="修正 DAG Budget 同步",
            log_result_summary=False,
        )
        return collected_run.task_outputs, collected_run.task_errors

    def _run_corrections(self, phase: Phase, review: 'ReviewResult') -> None:
        """Run corrective actions from a review with minor issues."""
        correction_outputs, correction_errors = self._execute_correction_dag(phase, review)

        if correction_errors:
            logger.warning("部分修正任务失败: %s", ", ".join(err.task_id for err in correction_errors[:5]))
        if correction_outputs:
            phase.task_outputs.update(correction_outputs)
            self._capture_architecture_execution_report(phase, phase.task_outputs)

    def _phase_deps_met(self, phase: Phase) -> bool:
        """Check if all phase dependencies are completed."""
        if not phase.depends_on_phases:
            return True
        completed_ids = {p.id for p in self._state.phases if p.status == PhaseStatus.COMPLETED}
        return all(dep in completed_ids for dep in phase.depends_on_phases)

    def _get_prior_phase_summaries(self, current_phase: Phase) -> list[str]:
        """Get summaries of completed prior phases.

        包含具体的任务完成列表，让后续 Phase 知道前序阶段已经做了什么，
        避免重复劳动（如 Phase 1 修正迭代已完成的工作在 Phase 2 中被重做）。
        """
        summaries = []
        for p in self._state.phases:
            if p.order >= current_phase.order:
                break
            if p.status == PhaseStatus.COMPLETED:
                parts = [f"[{p.name}]"]
                # 审查摘要
                if p.review_result:
                    parts.append(f"score={p.review_result.score:.2f}: {p.review_result.summary[:200]}")
                # 具体完成的任务列表（从 task_outputs 提取）
                if p.task_outputs:
                    task_list = []
                    for task_id, output in p.task_outputs.items():
                        # 截取输出前 100 字符作为摘要
                        output_preview = str(output)[:100].replace("\n", " ") if output else "已完成"
                        task_list.append(f"  - {task_id}: {output_preview}")
                    if task_list:
                        parts.append("已完成任务:\n" + "\n".join(task_list[:10]))  # 最多 10 个
                summaries.append("\n".join(parts))
        return summaries

    def _finalize_state(self) -> None:
        """Determine final goal status based on phase results.

        判定规则：
        - 全部完成/跳过 → CONVERGED
        - 超过半数阶段成功 → PARTIAL_SUCCESS（保留已完成的改进）
        - 全部失败 → FAILED
        - 少于半数成功 → FAILED
        """
        # 安全网：在任何路径前先确保 FAILED 状态有 failure_categories
        if self._state.status == GoalStatus.FAILED and not self._state.failure_categories:
            self._state.failure_categories['unclassified_failure'] = 1
            logger.warning(
                "FAILED 但无 failure_categories，已自动添加 'unclassified_failure' (early check)"
            )

        if self._state.status in (
            GoalStatus.FAILED,
            GoalStatus.CANCELLED,
            GoalStatus.SAFE_STOP,
            GoalStatus.CATASTROPHIC_STOP,
        ):
            return

        if self._state.failure_categories.get('catastrophic_stop'):
            self._state.status = GoalStatus.CATASTROPHIC_STOP
            return

        if self._state.failure_categories.get('safe_stop'):
            self._state.status = GoalStatus.SAFE_STOP
            return

        if datetime.now() >= self._state.deadline:
            self._state.status = GoalStatus.TIMEOUT
            return

        # 只统计实际执行过的阶段（排除 PENDING 和 SKIPPED）
        executed = [
            p for p in self._state.phases
            if p.status not in (PhaseStatus.SKIPPED, PhaseStatus.PENDING)
        ]
        skipped_or_completed = all(
            p.status in (PhaseStatus.COMPLETED, PhaseStatus.SKIPPED)
            for p in self._state.phases
        )
        completed_count = sum(1 for p in executed if p.status == PhaseStatus.COMPLETED)
        total_executed = len(executed)

        # SAFE_STOP 判定：已执行多个阶段/迭代但评分持续偏低
        if total_executed >= 2 and self._state.total_iterations >= 3:
            recent_scores = [r.score for r in self._state.iteration_history[-5:]]
            avg_score = sum(recent_scores) / len(recent_scores) if recent_scores else 0
            if avg_score < 0.3:
                reasons = []
                if recent_scores:
                    reasons.append(f"最近5次评分均值={avg_score:.2f}")
                if self._state.failure_categories:
                    # 类型防御：只考虑 int 值的条目
                    int_cats = {k: v for k, v in self._state.failure_categories.items() if isinstance(v, int)}
                    if int_cats:
                        top_failure = max(int_cats.items(), key=lambda x: x[1])
                        reasons.append(f"主要失败类型={top_failure[0]}(x{top_failure[1]})")
                self._state.safe_stop_reason = "评分持续偏低触发安全停机: " + ", ".join(reasons)
                self._state.status = GoalStatus.SAFE_STOP
                logger.warning("安全停机: %s", self._state.safe_stop_reason)
                return

        if skipped_or_completed and self._state.phases:
            # 前置条件：至少有一个阶段产生了非空的 task_outputs 才允许 CONVERGED
            has_any_output = any(
                p.task_outputs for p in self._state.phases
                if p.status == PhaseStatus.COMPLETED
            )
            if has_any_output:
                self._state.status = GoalStatus.CONVERGED
            else:
                logger.error(
                    "所有阶段标记为 COMPLETED/SKIPPED 但 0 个阶段有 task_outputs，降级为 FAILED (假收敛)"
                )
                self._state.status = GoalStatus.FAILED
                if not self._state.failure_categories:
                    self._state.failure_categories['false_convergence'] = 1
        elif total_executed == 0:
            self._state.status = GoalStatus.FAILED
            if not self._state.failure_categories:
                self._state.failure_categories['empty_phases'] = 1
                logger.warning('零阶段执行，标记为 FAILED (empty_phases)')
            # 从 diagnostics 最后条目提取根因
            if self._state.diagnostics:
                last_diag = self._state.diagnostics[-1]
                root_cause_key = f'root_cause:{last_diag.stage}:{last_diag.exit_status}'
                self._state.failure_categories[root_cause_key] = (
                    self._state.failure_categories.get(root_cause_key, 0) + 1
                )
                logger.info(
                    '零阶段根因补充: stage=%s exit_status=%s',
                    last_diag.stage, last_diag.exit_status,
                )
        elif completed_count > total_executed / 2:
            # 超过半数成功 → 部分成功
            self._state.status = GoalStatus.PARTIAL_SUCCESS
            logger.info(
                "部分阶段成功 (%d/%d)，标记为 PARTIAL_SUCCESS",
                completed_count, total_executed,
            )
            # 安全网：PARTIAL_SUCCESS 绕过 FAILED 分支时 failure_categories 可能为空
            if not self._state.failure_categories:
                self._state.failure_categories['partial_success_no_category'] = 1
                logger.warning(
                    "PARTIAL_SUCCESS 但无 failure_categories，已补充 'partial_success_no_category'"
                )
        else:
            # 半数及以下成功 → 失败
            self._state.status = GoalStatus.FAILED
            logger.warning(
                "多数阶段失败 (%d/%d 成功)，标记为 FAILED",
                completed_count, total_executed,
            )

        # 兜底断言：所有判定路径结束后，FAILED/PARTIAL_SUCCESS 必须有 failure_categories
        self._ensure_failure_categories(context='finalize final check')

        # 聚合运行级别 deterioration_levels
        _det_counts: dict[str, int] = {}
        for _rec in self._state.iteration_history:
            _level = _rec.deterioration_level or 'none'
            _det_counts[_level] = _det_counts.get(_level, 0) + 1
        if not _det_counts:
            # 零迭代运行：填充默认值
            _det_counts = {'skipped': 1}
        self._state.deterioration_levels = _det_counts

        # 聚合任务级执行统计
        try:
            self._aggregate_task_stats()
        except Exception as e:
            logger.error("任务统计聚合失败（不影响状态保存）: %s", e, exc_info=True)

    def _aggregate_from_store(self) -> dict[str, int]:
        """从 Store 数据库查询任务统计（覆盖所有已持久化的任务结果）。

        Returns:
            包含 total_tasks, success_count, failed_count 的字典。
        """
        total = 0
        success = 0
        failed = 0
        # 从 _run_id_for_sync 或最近的 run 中查询
        run_id = self._run_id_for_sync
        if not run_id:
            latest_run = self._store.get_latest_run()
            if latest_run:
                run_id = latest_run.run_id
        if run_id:
            results = self._store.get_all_task_results(run_id)
            total = len(results)
            success = sum(1 for r in results.values() if r.status == TaskStatus.SUCCESS)
            failed = sum(1 for r in results.values() if r.status == TaskStatus.FAILED)
        return {"total_tasks": total, "success_count": success, "failed_count": failed}

    def _aggregate_task_stats(self) -> None:
        """从多个数据源按优先级统计 success/failed 计数。

        数据源优先级：
        1. Phase.task_result_statuses（来自 DAG 执行后的 TaskResult.status）
        2. Store 数据库查询（直接查 task_results 表，覆盖所有已持久化的任务结果）
        3. 旧逻辑回退（task_outputs + iteration_history + failure_categories 估算）
        """
        total_tasks = 0
        success_count = 0
        failed_count = 0
        timeout_count = 0

        # 优先路径：从 task_result_statuses 直接统计（准确）
        has_task_result_statuses = any(
            phase.task_result_statuses for phase in self._state.phases
        )

        if has_task_result_statuses:
            for phase in self._state.phases:
                total_tasks += len(phase.raw_tasks)
                for _tid, status_val in phase.task_result_statuses.items():
                    if status_val == TaskStatus.SUCCESS.value:
                        success_count += 1
                    elif status_val == TaskStatus.FAILED.value:
                        failed_count += 1
                    # running / pending / waiting / ready / skipped / cancelled 不计入

        # 第二优先级：从 Store 数据库查询（覆盖 task_result_statuses 为空但有持久化记录的场景）
        if success_count == 0 and failed_count == 0:
            try:
                store_stats = self._aggregate_from_store()
                if store_stats["success_count"] > 0 or store_stats["failed_count"] > 0:
                    success_count = store_stats["success_count"]
                    failed_count = store_stats["failed_count"]
                    total_tasks = store_stats["total_tasks"]
            except Exception as e:
                logger.debug("Store 统计查询失败（不影响后续回退逻辑）: %s", e)

        # 第三优先级：旧逻辑回退（兼容旧状态文件和 Store 无数据的场景）
        if success_count == 0 and failed_count == 0:
            for phase in self._state.phases:
                total_tasks += len(phase.raw_tasks)
                success_count += len(phase.task_outputs)

            if total_tasks == 0:
                total_tasks = success_count

            failed_count = sum(r.task_error_count for r in self._state.iteration_history)

            # 修正：若 task_error_count 全为 0 但 failure_categories 含实际失败记录
            if failed_count == 0 and self._state.failure_categories:
                fc_fallback = sum(
                    v for k, v in self._state.failure_categories.items()
                    if isinstance(v, int)
                )
                failed_count = max(failed_count, fc_fallback)

        # 如果 raw_tasks 为空（某些旧恢复状态），用已统计的任务数作为下限
        if total_tasks == 0:
            total_tasks = success_count + failed_count

        effective_total = max(total_tasks, success_count + failed_count, 1)
        success_rate = success_count / effective_total if effective_total > 0 else 0.0

        # 优先使用 _generate_and_run_dag 中已计算的统计值（更准确，基于每个 task 的实际耗时）
        avg_duration = self._state.task_stats_avg_duration
        max_duration = self._state.task_stats_max_duration

        # 回退：若 _generate_and_run_dag 未运行（值为 0），从 iteration_history 提取 duration
        if avg_duration == 0.0 and max_duration == 0.0 and self._state.iteration_history:
            durations: list[float] = [
                getattr(r, 'duration_seconds', 0.0)
                for r in self._state.iteration_history
                if getattr(r, 'duration_seconds', 0.0) > 0
            ]
            avg_duration = sum(durations) / len(durations) if durations else 0.0
            max_duration = max(durations) if durations else 0.0

        # 超时计数：从 failure_categories 中提取（类型防御：只累加 int 值）
        timeout_count = (
            (self._state.failure_categories.get("timeout", 0) if isinstance(self._state.failure_categories.get("timeout"), int) else 0)
            + (self._state.failure_categories.get("transient", 0) if isinstance(self._state.failure_categories.get("transient"), int) else 0)
        )

        # 平均每任务成本
        avg_cost_per_task = (
            self._state.total_cost_usd / effective_total
            if effective_total > 0 and self._state.total_cost_usd > 0
            else 0.0
        )

        self._state.task_stats = {
            "total_tasks": effective_total,
            "success_count": success_count,
            "failed_count": failed_count,
            "success_rate": round(success_rate, 4),
            "avg_duration_seconds": round(avg_duration, 2),
            "max_duration_seconds": round(max_duration, 2),
            "timeout_count": timeout_count,
            "avg_cost_per_task": round(avg_cost_per_task, 4),
        }

        logger.info(
            "任务统计聚合完成: total=%d, success=%d, failed=%d, "
            "rate=%.2f%%, avg_duration=%.1fs, avg_cost=$%.4f",
            effective_total, success_count, failed_count,
            success_rate * 100, avg_duration, avg_cost_per_task,
        )

    def _print_final_summary(self) -> None:
        """Print a final summary to stderr."""
        s = self._state
        lines = [
            "",
            "=" * 60,
            "  自主编排完成",
            f"  目标: {s.goal_text[:60]}",
            f"  状态: {s.status.value}",
            f"  阶段: {len(s.phases)} 个",
            f"  总迭代: {s.total_iterations}",
            f"  总花费: ${s.total_cost_usd:.2f}",
            f"  耗时: {(datetime.now() - s.started_at).total_seconds() / 60:.1f} 分钟",
            "",
        ]

        # 需求收集摘要
        if s.requirement_spec:
            spec = s.requirement_spec
            lines.append(f"  需求收集: {len(spec.rounds)} 轮, "
                         f"{spec.total_questions_asked} 问 {spec.total_questions_answered} 答, "
                         f"充分性={spec.sufficiency_score:.2f} ({spec.sufficiency_verdict})")
            lines.append("")

        for p in s.phases:
            score_str = f"score={p.review_result.score:.2f}" if p.review_result else "未审查"
            lines.append(f"  {'[OK]' if p.status == PhaseStatus.COMPLETED else '[X]'} {p.name}: {p.status.value} ({score_str}, {p.iteration}次迭代)")

        # 失败分类统计
        if s.failure_categories:
            lines.append("")
            lines.append("  失败分类:")
            for category, count in s.failure_categories.items():
                if not isinstance(count, int):
                    continue  # 类型防御：跳过非 int 的异常条目
                lines.append(f"    {category}: {count}")

        # 恶化级别统计
        if s.deterioration_levels:
            lines.append("")
            lines.append("  恶化级别统计:")
            for level, count in s.deterioration_levels.items():
                lines.append(f"    {level}: {count}")

        # 任务执行统计
        if s.task_stats:
            ts = s.task_stats
            lines.append("")
            lines.append("  任务统计:")
            lines.append(f"    总任务数: {ts.get('total_tasks', 0)}")
            lines.append(f"    成功: {ts.get('success_count', 0)}")
            lines.append(f"    失败: {ts.get('failed_count', 0)}")
            lines.append(f"    成功率: {ts.get('success_rate', 0) * 100:.1f}%")
            lines.append(f"    平均耗时: {ts.get('avg_duration_seconds', 0):.1f}s")
            lines.append(f"    最大耗时: {ts.get('max_duration_seconds', 0):.1f}s")
            lines.append(f"    超时次数: {ts.get('timeout_count', 0)}")
            lines.append(f"    平均成本/任务: ${ts.get('avg_cost_per_task', 0):.4f}")

        # 诊断轨迹
        if s.diagnostics:
            lines.append("")
            lines.append("  诊断轨迹:")
            for entry in s.diagnostics:
                duration_str = f"({entry.duration_seconds:.1f}s)" if entry.duration_seconds and entry.duration_seconds > 0 else "(N/A)"
                lines.append(f"    {entry.stage}: {entry.exit_status} {duration_str}")
                if entry.error_detail:
                    error_preview = entry.error_detail[:200]
                    lines.append(f"      错误: {error_preview}")
                if entry.stack_trace:
                    stack_preview = entry.stack_trace[:300]
                    lines.append(f"      堆栈: {stack_preview}...")

        # 零阶段诊断摘要：筛选所有 zero_phase 相关的诊断条目
        zero_phase_entries = [
            entry for entry in (s.diagnostics or [])
            if entry.exit_status and entry.exit_status.startswith("zero_phase:")
        ]
        if zero_phase_entries:
            lines.append("")
            lines.append("  零阶段诊断摘要:")
            for entry in zero_phase_entries:
                category = entry.exit_status.replace("zero_phase:", "")
                status_tag = "可恢复" if category in ("transient", "logic") else "不可恢复"
                error_brief = entry.error_detail[:120] if entry.error_detail else "N/A"
                lines.append(
                    f"    {entry.stage}: [{category}] {status_tag} — {error_brief}"
                )

        lines.append("=" * 60)
        sys.stderr.write("\n".join(lines) + "\n")
        sys.stderr.flush()
