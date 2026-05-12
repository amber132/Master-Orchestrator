"""Top-level orchestrator: main loop driving scheduler + execution + checkpoints."""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[assignment]

from .adaptive_pool import AdaptiveThreadPool
from .backpressure import BackpressureHandler
from .claude_cli import BudgetTracker
from .plugin_registry import PluginRegistry
from .concurrency_group import ConcurrencyGroupManager
from .config import Config, RateLimitConfig
from .context_summarizer import HierarchicalSummarizer
from .error_classifier import classify_error, classify_failover_reason, resolve_failover_status, should_retry_with_priority
from .failover_pool import PoolRuntime
from .escalation import EscalationManager, RiskLevel
from .health_server import HealthServer
from .heartbeat import Heartbeat
from .log_context import set_run_id
from .notification import get_notifier
from .process_cleaner import get_process_cleaner
from .redundancy_detector import RedundancyDetector
from .features import is_enabled
# blackboard、context_quarantine、semantic_drift 改为按特性开关懒加载（见 __init__）
from .exceptions import (
    BudgetExhaustedError,
    DAGValidationError,
    HealthCheckError,
    OrchestratorError,
)
from .field_transform import apply_transforms
from .graceful_shutdown import GracefulShutdownManager
from .link_resolver import resolve_links, inject_link_context
from .output_cache import OutputCache
from .prompt_prefix import extract_shared_prefix
from .task_cache import TaskCache
from .rate_limiter import RateLimiter
from .role_spec import RoleAssigner, apply_role
from .semantic_reset import SemanticResetProtocol
from .thread_monitor import ThreadMonitor
from .validation import IntraTaskValidator, InterTaskValidator
from .model import DAG, ErrorCategory, ErrorPolicy, FailoverStatus, RetryPolicy, RunInfo, RunStatus, TaskNode, TaskResult, TaskStatus
from .monitor import ProgressMonitor, setup_logging
from .scheduler import Scheduler
from .store import Store
from .failure_propagator import FailurePropagator
from .template import render_template
from .sanitizer import PromptSanitizer
from .metrics import MetricsCollector, TaskMetrics
from .guardrail import InputGuardrail, OutputGuardrail
from .checkpoint import CheckpointManager
from .audit_log import AuditLogger
from .command_runtime import normalize_python_command

# 防御性导入：diagnostics 模块在 workspace 副本中可能缺失，
# 缺少时降级为无诊断模式而非崩溃（这是 0% 成功率的根因之一）
try:
    from .diagnostics import DiagnosticEventType, DiagnosticLogger as _DiagnosticLogger
except ImportError:
    DiagnosticEventType = None  # type: ignore[assignment,misc]
    _DiagnosticLogger = None  # type: ignore[assignment]

logger = logging.getLogger("claude_orchestrator")


class _NoOpDiagnostics:
    """diagnostics 模块缺失时的无操作替代，确保诊断日志调用不会崩溃。"""

    def record_task_lifecycle(self, **kwargs) -> None:
        pass

    def close(self) -> None:
        pass


@dataclass
class _TaskExecutionContext:
    """_run_task_with_retry 重试循环期间的可变上下文，在子函数间传递。"""
    task_node: object          # TaskNode（可能已被 apply_role 修改）
    effective_task: object     # 实际执行用的 TaskNode（可能被模型切换修改）
    task_id_display: str       # 格式化后的任务 ID（含颜色标签）
    on_progress: object        # 流式事件回调函数
    cache_key: str             # OutputCache 的哈希 key
    effective_max_attempts: int
    execution_profile: str
    attempted_profiles: set    # set[str]
    failure_history: list      # list[TaskResult]
    current_prompt: str


@dataclass
class _RetryDecision:
    """_handle_attempt_failure 的返回值，告知调用方如何继续。"""
    action: str     # "abort" | "switch_model" | "switch_profile" | "retry_immediately" | "retry_with_backoff"
    result: object  # TaskResult | None
    model: str | None = None  # 仅 switch_model 时使用


class RetryStrategy:
    """封装任务重试决策逻辑：错误分类、failover 处理、模型降级、语义重置。

    从 Orchestrator 中提取的独立类，负责分析失败原因并决定下一步行动。
    """

    def __init__(
        self,
        config: Config,
        store: Store,
        pool_runtime: PoolRuntime | None,
        rate_limiter: RateLimiter,
        reset_protocol: SemanticResetProtocol,
        diagnostics,          # DiagnosticLogger 实例
        dag: DAG,
        execute_hook_fn,      # callable(hook_cmd, task_id, status) -> None
        get_run_info_fn,      # callable() -> RunInfo | None
        set_exit_code_fn,     # callable(code: int) -> None
    ):
        self._config = config
        self._store = store
        self._pool_runtime = pool_runtime
        self._rate_limiter = rate_limiter
        self._reset_protocol = reset_protocol
        self._diagnostics = diagnostics
        self._dag = dag
        self._execute_hook = execute_hook_fn
        self._get_run_info = get_run_info_fn
        self._set_exit_code = set_exit_code_fn
        # 模型连续 529 过载计数器（用于自动降级决策）
        self._model_overload_counts: dict[str, int] = {}
        self._model_overload_threshold: int = 2

    def _update_model_pressure(self, model_name: str, error_msg: str) -> None:
        """Track overload pressure per model for fallback decisions."""
        if '529' in error_msg or 'overloaded' in error_msg.lower():
            self._model_overload_counts[model_name] = self._model_overload_counts.get(model_name, 0) + 1
            logger.warning(
                "模型 '%s' 529 过载计数: %d/%d",
                model_name, self._model_overload_counts[model_name], self._model_overload_threshold,
            )
            return
        if model_name in self._model_overload_counts:
            self._model_overload_counts[model_name] = 0

    def _abort_decision(
        self,
        *,
        task_id_display: str,
        task_node: TaskNode,
        result: TaskResult,
        log_message: str,
        log_args: tuple[object, ...],
    ) -> _RetryDecision:
        logger.error(log_message, task_id_display, *log_args)
        if self._dag.hooks and self._dag.hooks.on_task_fail:
            self._execute_hook(self._dag.hooks.on_task_fail, task_node.id, "failed")
        return _RetryDecision(action="abort", result=result)

    def decide_retry_action(
        self,
        ctx: _TaskExecutionContext,
        result: TaskResult,
        attempt: int,
        task_start_time: datetime,
        model: str,
    ) -> _RetryDecision:
        """分析失败原因并决策下一步行动（重试/切换模型/中止等）。"""

        task_node = ctx.task_node
        task_id_display = ctx.task_id_display
        error_msg = result.error or ""

        # 记录失败到历史
        ctx.failure_history.append(result)

        # 检测 429 限流
        if '429' in error_msg or 'rate limit' in error_msg.lower() or 'too many requests' in error_msg.lower():
            self._rate_limiter.report_429()

        # 检测 529 模型过载
        model_name = ctx.effective_task.model or self._config.claude.default_model
        self._update_model_pressure(model_name, error_msg)

        # 错误分类
        error_category = classify_error(error_msg)
        logger.info(
            "Task '%s' failed (attempt %d/%d), error category: %s",
            task_id_display, attempt, ctx.effective_max_attempts, error_category.value,
        )

        # 根据错误类别决定是否继续重试
        effective_policy = task_node.error_policy or ErrorPolicy()
        if not should_retry_with_priority(error_category, effective_policy, is_critical=task_node.is_critical):
            return self._abort_decision(
                task_id_display=task_id_display,
                task_node=task_node,
                result=result,
                log_message="Task '%s' failed with %s error (is_critical=%s), aborting retry: %s",
                log_args=(error_category.value, task_node.is_critical, result.error),
            )

        # Failover 决策链
        failover_reason = classify_failover_reason(
            error_msg,
            exit_code=getattr(result, 'exit_code', 1),
            stderr=getattr(result, 'stderr', ''),
        )
        failover_status = resolve_failover_status(failover_reason, attempt, ctx.effective_max_attempts)
        logger.info(
            "Task '%s' failover: reason=%s, status=%s",
            task_id_display, failover_reason.value, failover_status.value,
        )

        # Pool 集成的 failover 处理
        if self._pool_runtime is not None:
            pool_decision = self.apply_failover(
                ctx, attempt, failover_reason, failover_status, task_id_display,
            )
            if pool_decision is not None:
                return pool_decision

        # 模型降级决策
        fallback_decision = self.select_model_fallback(
            ctx, attempt, failover_status, task_id_display, failover_reason, result,
        )
        if fallback_decision is not None:
            return fallback_decision

        # 根据 failover 状态执行策略
        if failover_status == FailoverStatus.RETRY_IMMEDIATELY:
            if attempt < ctx.effective_max_attempts:
                logger.info(
                    "Task '%s' immediate retry (network error, attempt %d)",
                    task_id_display, attempt,
                )
                return _RetryDecision(action="retry_immediately", result=result)

        elif failover_status == FailoverStatus.NEEDS_HUMAN:
            return self._abort_decision(
                task_id_display=task_id_display,
                task_node=task_node,
                result=result,
                log_message="Task '%s' needs human intervention: %s",
                log_args=(error_msg,),
            )

        elif failover_status == FailoverStatus.ABORT:
            return self._abort_decision(
                task_id_display=task_id_display,
                task_node=task_node,
                result=result,
                log_message="Task '%s' aborted: %s",
                log_args=(failover_reason.value,),
            )

        # RETRYABLE 错误：检查语义重置 + 退避重试
        if attempt < ctx.effective_max_attempts:
            if self._reset_protocol.should_reset(attempt, error_msg):
                logger.info(
                    "Task '%s' triggering semantic reset (attempt %d/%d)",
                    task_id_display, attempt, ctx.effective_max_attempts,
                )
                known_good_state = self._reset_protocol.get_known_good_state(
                    task_node.id, self._store, self._get_run_info().run_id,
                )
                ctx.current_prompt = self._reset_protocol.build_reset_prompt(
                    task_node, ctx.failure_history, ctx.current_prompt,
                )
                if known_good_state:
                    good_state_json = json.dumps(known_good_state, ensure_ascii=False, indent=2)
                    ctx.current_prompt = (
                        f"{ctx.current_prompt}\n\n## 参考：上次成功的输出\n"
                        f"```json\n{good_state_json}\n```"
                    )
                    logger.info("Task '%s' using known good state as reference", task_node.id)

            # 速率限制使用 60s 初始退避 + 每次翻倍，其他错误使用默认退避策略
            if failover_reason.value == 'rate_limit':
                delay = min(60.0 * (2 ** (attempt - 1)), 600.0)
            else:
                delay = task_node.retry_policy.delay_for_attempt(attempt)
            logger.warning(
                "Task '%s' failed (attempt %d/%d), retrying in %.0fs: %s",
                task_id_display, attempt, ctx.effective_max_attempts, delay, result.error,
            )
            # 诊断事件：任务重试（diagnostics 模块缺失时静默跳过）
            if DiagnosticEventType is not None:
                self._diagnostics.record_task_lifecycle(
                    event_type=DiagnosticEventType.TASK_RETRY,
                    run_id=self._get_run_info().run_id if self._get_run_info() else "",
                    task_id=task_node.id,
                    model=model,
                    attempt=attempt,
                    error=result.error or "",
                )
            time.sleep(delay)
            return _RetryDecision(action="retry_with_backoff", result=result)

        # 所有重试用完
        logger.error(
            "Task '%s' failed after %d attempts: %s",
            task_id_display, ctx.effective_max_attempts, result.error,
        )
        if self._dag.hooks and self._dag.hooks.on_task_fail:
            self._execute_hook(self._dag.hooks.on_task_fail, task_node.id, "failed")
        return _RetryDecision(action="abort", result=result)

    def apply_failover(
        self,
        ctx: _TaskExecutionContext,
        attempt: int,
        failover_reason,
        failover_status,
        task_id_display: str,
    ) -> _RetryDecision | None:
        """处理 Pool Runtime 相关的 failover：进程接管和任务级 profile 切换。"""
        attempt_profile = ctx.execution_profile
        profile_name = attempt_profile or self._pool_runtime.active_profile
        self._pool_runtime.record_failure(profile_name, failover_reason.value)

        run_info = self._get_run_info()

        # 检测 process_takeover
        if (
            run_info is not None
            and self._pool_runtime.should_trigger_process_takeover(profile_name, failover_reason.value)
        ):
            next_process_profile = self._pool_runtime.choose_process_profile(current_profile=profile_name)
            if next_process_profile is not None:
                self._store.save_failover_event(
                    execution_id=run_info.run_id,
                    execution_kind="run",
                    scope="process",
                    from_profile=profile_name,
                    to_profile=next_process_profile.name,
                    reason=failover_reason.value,
                    trigger_task_id=ctx.task_node.id,
                    metadata={"attempt": attempt},
                )
                self._pool_runtime.write_request(
                    "takeover",
                    target_profile=next_process_profile.name,
                    reason=failover_reason.value,
                    metadata={"run_id": run_info.run_id, "task_id": ctx.task_node.id},
                )
                self._set_exit_code(PoolRuntime.EXIT_CODE_TAKEOVER)

        # 任务级 profile 切换
        can_task_switch = (
            failover_reason.value in self._pool_runtime.config.task_policy.allowed_reasons
            or failover_reason.value == "auth_expired"
        )
        if can_task_switch and attempt < ctx.effective_max_attempts and run_info is not None:
            next_task_profile = self._pool_runtime.choose_task_profile(
                current_profile=profile_name,
                tried_profiles=ctx.attempted_profiles,
            )
            if next_task_profile is not None:
                self._store.save_failover_event(
                    execution_id=run_info.run_id,
                    execution_kind="run",
                    scope="task",
                    from_profile=profile_name,
                    to_profile=next_task_profile.name,
                    reason=failover_reason.value,
                    trigger_task_id=ctx.task_node.id,
                    metadata={"attempt": attempt},
                )
                logger.warning(
                    "Task '%s' switching supplier profile: %s -> %s (reason=%s)",
                    task_id_display, profile_name, next_task_profile.name, failover_reason.value,
                )
                ctx.execution_profile = next_task_profile.name
                ctx.attempted_profiles.add(next_task_profile.name)
                return _RetryDecision(action="switch_profile", result=None)

        return None

    def select_model_fallback(
        self,
        ctx: _TaskExecutionContext,
        attempt: int,
        failover_status,
        task_id_display: str,
        failover_reason,
        result: TaskResult,
    ) -> _RetryDecision | None:
        """根据 failover 状态决定是否执行模型降级。"""
        if failover_status != FailoverStatus.SWITCH_MODEL:
            return None

        current_model = ctx.effective_task.model or self._config.claude.default_model
        fallback_chain = {"opus": "sonnet", "sonnet": "haiku"}
        next_model = fallback_chain.get(current_model)
        if next_model and attempt < ctx.effective_max_attempts:
            overload_count = self._model_overload_counts.get(current_model, 0)
            logger.warning(
                "Task '%s' switching model: %s -> %s (529 计数=%d, 原因=%s)",
                task_id_display, current_model, next_model, overload_count, failover_reason.value,
            )
            self._model_overload_counts[current_model] = 0
            return _RetryDecision(action="switch_model", model=next_model, result=result)
        return None


class Orchestrator:
    """Drives the DAG execution lifecycle."""

    # 类级别的嵌套深度跟踪（用于测试）
    _nesting_depth: int = 0
    _nesting_lock: threading.Lock = threading.Lock()

    @staticmethod
    def _resolve_audit_log_dir(
        config: Config,
        working_dir: str | None,
        log_file: str | None,
    ) -> Path:
        """解析审计日志目录。

        优先级：
        1. log_file 路径的父级/logs/ → 父级/evidence/audit
        2. config.audit.log_dir（绝对或相对于 working_dir）
        3. working_dir 本身
        """
        if log_file:
            log_path = Path(log_file).resolve()
            logs_dir = log_path.parent
            if logs_dir.name.lower() == "logs":
                return logs_dir.parent / "evidence" / "audit"
            return logs_dir / "audit"

        configured_dir = (getattr(config.audit, "log_dir", "") or "").strip()
        if configured_dir:
            candidate = Path(configured_dir)
            if candidate.is_absolute():
                return candidate
            base = Path(working_dir) if working_dir else Path(".")
            return base / candidate

        return Path(working_dir) if working_dir else Path(".")

    def __init__(
        self,
        dag: DAG,
        config: Config,
        store: Store | None = None,
        working_dir: str | None = None,
        log_file: str | None = None,
        default_error_policy: str | None = None,
        enable_streaming: bool = False,
        pool_runtime: PoolRuntime | None = None,
        on_task_result: Any = None,
    ):
        self._dag = dag
        self._config = config
        self._owns_store = store is None
        self._store = store or Store(config.checkpoint.db_path)
        self._store_closed = False
        self._working_dir = working_dir
        self._pool_runtime = pool_runtime
        self.requested_exit_code: int = 0
        # 每个任务完成后的回调（用于外部状态持久化，如 goal_state.json）
        self._on_task_result = on_task_result

        # 创建冗余检测器
        self._redundancy_detector = RedundancyDetector()

        self._scheduler = Scheduler(
            dag,
            config.orchestrator.max_parallel,
            queue_capacity=config.orchestrator.queue_capacity,
            concurrency_manager=self._build_concurrency_manager(dag),
            redundancy_detector=self._redundancy_detector,
            max_write_parallel=config.orchestrator.max_write_parallel or None,
        )
        # 持久化到 store 数据库同目录
        _budget_persist = str(Path(config.checkpoint.db_path).parent / "budget_tracker.json")
        self._budget = BudgetTracker(
            config.claude.max_budget_usd,
            persist_path=_budget_persist,
            enforcement_mode=config.claude.budget_enforcement_mode,
        )
        self._rate_limiter = RateLimiter(config.rate_limit)
        self._outputs: OrderedDict[str, Any] = OrderedDict()
        self._results: OrderedDict[str, TaskResult] = OrderedDict()
        self._run_info: RunInfo | None = None
        self._monitor: ProgressMonitor | None = None
        self._log_file = log_file
        self._default_error_policy = default_error_policy
        self._enable_streaming = enable_streaming
        # 实例级嵌套深度跟踪（避免类级别锁竞争）
        self._nesting_depth: int = 0
        self._nesting_lock: threading.Lock = threading.Lock()
        # Context cache 用于断点续传时恢复上下文（从 Store.context_data 表加载）
        self._context_cache: OrderedDict[str, Any] = OrderedDict()

        # 从 LimitsConfig 读取 LRU 缓存大小限制
        self._lru_max_outputs: int = config.limits.lru_max_outputs
        self._lru_max_results: int = config.limits.lru_max_results

        # 溢出文件目录：working_dir/.orchestrator_spill/
        spill_cfg = config.spill
        base = Path(working_dir) if working_dir else Path(".")
        self._spill_dir = base / spill_cfg.spill_dir_name

        # 线程池和监控器（在 run/resume 中初始化）
        self._pool: AdaptiveThreadPool | None = None
        self._thread_monitor: ThreadMonitor | None = None

        # 输出缓存
        self._output_cache = OutputCache()

        # 任务缓存（用于幂等任务的结果复用）
        self._task_cache = TaskCache()

        # 上下文隔离（特性开关控制，关闭时使用 Null 对象）
        if is_enabled("context_quarantine"):
            from .context_quarantine import ContextQuarantine
            self._quarantine = ContextQuarantine()
        else:
            from .null_objects import NullQuarantine
            self._quarantine = NullQuarantine()

        # 黑板（特性开关控制，关闭时使用 Null 对象）
        if is_enabled("blackboard"):
            from .blackboard import Blackboard
            self._blackboard = Blackboard()
        else:
            from .null_objects import NullBlackboard
            self._blackboard = NullBlackboard()

        # 验证器（单任务自检和任务间交叉验证）
        self._intra_validator = IntraTaskValidator()
        self._inter_validator = InterTaskValidator()

        # 语义漂移检测器（特性开关控制，关闭时使用 Null 对象）
        if is_enabled("semantic_drift"):
            from .semantic_drift import SemanticDriftDetector
            self._drift_detector = SemanticDriftDetector()
        else:
            from .null_objects import NullDriftDetector
            self._drift_detector = NullDriftDetector()

        # 任务 prompt 缓存（用于漂移检测）
        self._task_prompts: OrderedDict[str, str] = OrderedDict()

        # 角色分配器（用于为任务分配专业化角色）
        self._role_assigner = RoleAssigner()

        # 语义重置协议（用于智能重试）
        self._reset_protocol = SemanticResetProtocol()

        # 上下文摘要器（用于压缩长输出）
        self._summarizer = HierarchicalSummarizer()

        # 风险评估与升级管理器（用于高风险任务的人工审批）
        # 从 config 读取 approval_mode，默认为 'auto'（无人值守模式）
        approval_mode = getattr(config, 'approval_mode', 'auto')
        self._escalation_manager = EscalationManager(approval_mode=approval_mode)

        # 心跳写入器（供 Guardian 检测存活）
        self._heartbeat = Heartbeat()

        # 指标收集器（记录任务执行指标）
        self._metrics_collector = MetricsCollector("metrics.jsonl")

        # 输入输出安全检查（guardrail）
        self._input_guardrail = InputGuardrail(max_length=100_000)
        self._output_guardrail = OutputGuardrail(max_length=500_000)

        # 检查点管理器（用于阶段级状态持久化）
        self._checkpoint_manager = CheckpointManager(config.checkpoint.db_path)
        # Prompt 清洗器（用于防止注入和超长内容）
        self._sanitizer = PromptSanitizer()

        # 上下文压缩器（防止累积输出内存膨胀）
        from .context_compactor import ContextCompactor
        self._compactor = ContextCompactor(
            max_total_chars=self._config.limits.max_output_size_bytes // 10,  # 1MB
            compaction_interval=5,
        )

        # 结构化诊断日志（记录任务生命周期关键事件）
        # 防御性初始化：diagnostics 模块缺失时降级为无操作日志
        if _DiagnosticLogger is not None:
            self._diagnostics = _DiagnosticLogger(
                log_dir=str(self._spill_dir.parent) if self._spill_dir else ".",
            )
        else:
            logger.warning("diagnostics 模块不可用，使用无操作诊断日志")
            self._diagnostics = _NoOpDiagnostics()

        # 审计日志记录器（记录 prompt 和执行结果，使用 _resolve_audit_log_dir 解析目录）
        self._audit_logger = AuditLogger(
            log_dir=self._resolve_audit_log_dir(config, working_dir, log_file),
        )

        # 失败传播器（封装 mark_completed + propagate_failure + 批量生成取消结果的逻辑）
        self._failure_propagator = FailurePropagator(
            scheduler=self._scheduler,
            store=self._store,
            outputs=self._outputs,
            results=self._results,
            lru_set_fn=self._lru_set,
            get_run_info=lambda: self._run_info,
            lru_max_results=self._lru_max_results,
        )

        # 重试策略（封装错误分类、failover、模型降级、语义重置等决策逻辑）
        self._retry_strategy = RetryStrategy(
            config=self._config,
            store=self._store,
            pool_runtime=self._pool_runtime,
            rate_limiter=self._rate_limiter,
            reset_protocol=self._reset_protocol,
            diagnostics=self._diagnostics,
            dag=self._dag,
            execute_hook_fn=self._execute_hook,
            get_run_info_fn=lambda: self._run_info,
            set_exit_code_fn=lambda code: setattr(self, 'requested_exit_code', code),
        )

        # 健康检查 HTTP 端点（延迟到 run/resume 中启动）
        self._health_server: HealthServer | None = None

        # 全局 429 暂停控制：任何任务收到 429 时暂停调度新任务 30 秒
        self._rate_limit_pause_event = threading.Event()
        self._rate_limit_pause_event.set()  # 初始状态：不暂停
        self._rate_limit_pause_lock = threading.Lock()
        self._rate_limit_pause_until: float = 0.0

    def _build_concurrency_manager(self, dag: DAG) -> ConcurrencyGroupManager | None:
        """根据 DAG 中的任务构建并发组管理器，从 config.rate_limit.per_model_limits 读取配置。"""
        # 收集所有任务的并发组
        groups = set()
        for task in dag.tasks.values():
            if task.concurrency_group:
                groups.add(task.concurrency_group)

        if not groups:
            return None

        # 从 config.rate_limit.per_model_limits 读取配置，未定义的组使用默认值（max_parallel 的一半）
        default_limit = max(1, self._config.orchestrator.max_parallel // 2)
        group_limits = {}
        for group in groups:
            group_limits[group] = self._config.rate_limit.per_model_limits.get(group, default_limit)

        logger.info(
            "Created concurrency manager with %d group(s): %s",
            len(groups), {g: group_limits[g] for g in sorted(groups)}
        )

        return ConcurrencyGroupManager(group_limits)

    @staticmethod
    def _lru_set(od: OrderedDict, key: str, value: object, max_size: int) -> None:
        """LRU 淘汰：达到上限时移除最旧条目，确保容量不超过 max_size。"""
        if key in od:
            od.move_to_end(key)
        od[key] = value
        while len(od) > max_size:
            od.popitem(last=False)

    def _create_pool_and_monitor(self) -> None:
        """创建线程池和监控器（提取公共逻辑，避免 run/resume 重复代码）"""
        cfg = self._config.orchestrator

        # 创建线程池
        if cfg.adaptive_enabled:
            self._pool = AdaptiveThreadPool(
                min_workers=cfg.min_parallel,
                max_workers=cfg.max_parallel,
                queue_capacity=cfg.queue_capacity,
            )
        else:
            # 退化为固定大小池（min=max=max_parallel）
            self._pool = AdaptiveThreadPool(
                min_workers=cfg.max_parallel,
                max_workers=cfg.max_parallel,
                queue_capacity=cfg.queue_capacity,
            )

        # 创建并启动线程监控器
        self._thread_monitor = ThreadMonitor(
            pool=self._pool,
            interval=30,  # 每30秒采集一次指标
            starvation_threshold=300,  # 饥饿阈值5分钟
        )
        self._thread_monitor.start()

    def __enter__(self):
        """上下文管理器入口。"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，确保资源清理。"""
        self._close_store()
        return False

    @property
    def run_info(self) -> RunInfo | None:
        return self._run_info

    @property
    def results(self) -> dict[str, TaskResult]:
        return dict(self._results)

    def run(self, dag_hash: str = "") -> RunInfo:
        """Execute the full DAG. Returns RunInfo with final status."""
        # 输入验证：dag_hash 应为空或合法的十六进制哈希
        if dag_hash and not re.fullmatch(r'[0-9a-fA-F]+', dag_hash):
            raise DAGValidationError(f"Invalid dag_hash format (expected hex string): {dag_hash!r}")

        # 工作目录完整性检查：目录不存在时立即抛出结构化异常，而非静默失败
        if self._working_dir:
            work_path = Path(self._working_dir)
            if not work_path.exists():
                raise OrchestratorError(
                    f"工作目录不存在: {self._working_dir}",
                    context={"check": "working_dir", "path": self._working_dir, "exists": False},
                )
            if not work_path.is_dir():
                raise OrchestratorError(
                    f"工作目录路径不是目录: {self._working_dir}",
                    context={"check": "working_dir", "path": self._working_dir, "is_dir": False},
                )

        # 配置完整性检查：必要配置项缺失时立即报错，避免静默产生 phases=0
        cfg_errors: list[str] = []
        if not self._config.claude.cli_path:
            cfg_errors.append("claude.cli_path 为空")
        if not self._config.checkpoint.db_path:
            cfg_errors.append("checkpoint.db_path 为空")
        if self._config.claude.max_budget_usd <= 0:
            cfg_errors.append(f"claude.max_budget_usd 必须 > 0，当前值: {self._config.claude.max_budget_usd}")
        if self._config.orchestrator.max_parallel < 1:
            cfg_errors.append(f"orchestrator.max_parallel 必须 >= 1，当前值: {self._config.orchestrator.max_parallel}")
        if cfg_errors:
            raise OrchestratorError(
                f"配置完整性检查失败: {'; '.join(cfg_errors)}",
                context={"check": "config_integrity", "errors": cfg_errors},
            )

        # DAG 任务非空检查：空 DAG 没有执行意义，立即报错
        if not self._dag.tasks:
            raise DAGValidationError(
                f"DAG '{self._dag.name}' 没有任何任务，无法执行",
                context={"check": "dag_empty", "dag_name": self._dag.name},
            )

        # 嵌套深度检查
        with self._nesting_lock:
            if self._nesting_depth >= self._config.limits.max_nesting_depth:
                raise DAGValidationError("嵌套层级过深，请保持扁平编排")
            self._nesting_depth += 1

        try:
            setup_logging(self._log_file)

            # 启动新运行前清理上次残留的 running 状态
            self._ensure_stuck_runs_cleaned()

            self._run_info = RunInfo(
                dag_name=self._dag.name,
                dag_hash=dag_hash,
                pool_id=self._pool_runtime.pool_id if self._pool_runtime else "",
                active_profile=self._pool_runtime.active_profile if self._pool_runtime else "",
            )
            self._store.create_run(self._run_info)
            if self._pool_runtime:
                self._pool_runtime.mark_execution(
                    execution_id=self._run_info.run_id,
                    execution_kind="run",
                    state_db_path=self._config.checkpoint.db_path,
                    active_profile=self._pool_runtime.active_profile,
                )

            # 设置日志上下文 run_id
            set_run_id(self._run_info.run_id)

            # Initialize all tasks in checkpoint
            for tid, node in self._dag.tasks.items():
                self._store.init_task(self._run_info.run_id, tid, depends_on=list(node.depends_on))

            logger.info("Starting run %s for DAG '%s' (%d tasks)",
                         self._run_info.run_id, self._dag.name, len(self._dag.tasks))

            # 保存初始检查点
            try:
                task_states = {tid: TaskStatus.PENDING.value for tid in self._dag.tasks}
                self._checkpoint_manager.save_checkpoint(
                    self._run_info.run_id,
                    phase="initialization",
                    task_states=task_states,
                    metadata={"dag_name": self._dag.name, "total_tasks": len(self._dag.tasks)}
                )
            except Exception as e:
                logger.warning("Failed to save initial checkpoint: %s", e)


            # 启动心跳后台线程
            self._heartbeat.start_background()

            # 启动健康检查 HTTP 端点
            if self._config.health.health_enabled:
                self._health_server = HealthServer(
                    self._config.health.health_port,
                    bind=self._config.health.health_bind,
                )
                self._health_server.start()

            # 创建线程池和监控器
            self._create_pool_and_monitor()

            # 创建进度监控器（需要在 thread_monitor 之后）
            self._monitor = ProgressMonitor(
                self._dag.name, len(self._dag.tasks),
                interval=self._config.orchestrator.health_check_interval,
                scheduler=self._scheduler,
                budget_tracker=self._budget,
                thread_monitor=self._thread_monitor,
            )
            self._monitor.start()

            try:
                self._execute_loop()
            except KeyboardInterrupt:
                logger.warning("Interrupted by user")
                self._run_info.status = RunStatus.CANCELLED
            except BudgetExhaustedError as e:
                logger.error("Budget exhausted: %s", e)
                self._run_info.status = RunStatus.FAILED
            except OrchestratorError as exc:
                logger.error("Orchestrator error: %s", exc)
                self._run_info.status = RunStatus.FAILED
                get_notifier().critical("编排器异常终止", detail=str(exc))
            except Exception as e:
                logger.exception("Unexpected error: %s", e)
                self._run_info.status = RunStatus.FAILED
            finally:
                try:
                    if self._thread_monitor:
                        self._thread_monitor.stop()
                except Exception as e:
                    logger.warning("Failed to stop thread monitor: %s", e)
                try:
                    self._monitor.stop()
                except Exception as e:
                    logger.warning("Failed to stop progress monitor: %s", e)
                self._finalize()

            return self._run_info
        finally:
            # 退出时递减嵌套深度
            with self._nesting_lock:
                self._nesting_depth -= 1

    def resume(self, run_id: str) -> RunInfo:
        """Resume a previous run from checkpoint."""
        # 输入验证：run_id 应为合法的十六进制字符串（uuid.hex[:12]）
        if not run_id or not re.fullmatch(r'[0-9a-fA-F]{1,32}', run_id):
            raise OrchestratorError(f"Invalid run_id format (expected hex string): {run_id!r}")

        setup_logging(self._log_file)

        info = self._store.get_run(run_id)
        if not info:
            raise OrchestratorError(f"Run '{run_id}' not found")

        self._run_info = info
        self._run_info.status = RunStatus.RUNNING
        if self._pool_runtime:
            self._run_info.pool_id = self._pool_runtime.pool_id
            self._run_info.active_profile = self._pool_runtime.active_profile
            self._store.update_run_pool_info(
                run_id=run_id,
                pool_id=self._pool_runtime.pool_id,
                active_profile=self._pool_runtime.active_profile,
            )
            self._pool_runtime.mark_execution(
                execution_id=run_id,
                execution_kind="run",
                state_db_path=self._config.checkpoint.db_path,
                active_profile=self._pool_runtime.active_profile,
            )

        # DAG 哈希校验：确保 resume 时 DAG 未被修改
        current_hash = self._dag.content_hash()
        if info.dag_hash and current_hash != info.dag_hash:
            raise OrchestratorError(
                f"DAG hash mismatch: saved={info.dag_hash[:12]}… current={current_hash[:12]}…. "
                "DAG was modified since the run was created. Use a new run instead."
            )

        # 设置日志上下文 run_id
        set_run_id(self._run_info.run_id)

        # Restore task states
        saved_results = self._store.get_all_task_results(run_id)
        task_states: dict[str, TaskStatus] = {}
        for tid, result in saved_results.items():
            task_states[tid] = result.status
            self._lru_set(self._results, tid, result, max_size=self._lru_max_results)
            if result.status == TaskStatus.SUCCESS and result.parsed_output is not None:
                self._lru_set(self._outputs, tid, result.parsed_output, max_size=self._lru_max_outputs)

        # Restore budget from saved results
        total_spent = 0.0
        for tid, result in saved_results.items():
            if hasattr(result, 'cost_usd') and result.cost_usd:
                total_spent += float(result.cost_usd)
        if total_spent > 0:
            self._budget.add_cost(total_spent)
            logger.info("恢复已花费预算: $%.4f", total_spent)

        # Restore context data from Store (支持断点续传)
        try:
            context_data = self._store.get_all_context(run_id)
            for key, value_json in context_data.items():
                try:
                    self._lru_set(self._context_cache, key, json.loads(value_json), max_size=self._lru_max_outputs)
                except json.JSONDecodeError:
                    logger.warning("Failed to parse context data for key '%s'", key)
            if self._context_cache:
                logger.info("Restored %d context entries from checkpoint", len(self._context_cache))

            # 恢复 rate_limiter 状态
            if '_rate_limiter_state' in self._context_cache:
                try:
                    self._rate_limiter.restore_state(self._context_cache['_rate_limiter_state'])
                    logger.info("Restored rate limiter state from checkpoint")
                except Exception as e:
                    logger.warning("Failed to restore rate limiter state: %s", e)
        except Exception as e:
            logger.warning("Failed to restore context data: %s", e)

        # 恢复 loop 计数
        try:
            loop_counts = self._store.get_all_loop_counts(run_id)
            if loop_counts:
                self._scheduler.restore_loop_counts(loop_counts)
                logger.info("Restored loop counts for %d tasks from checkpoint", len(loop_counts))
        except Exception as e:
            logger.warning("Failed to restore loop counts: %s", e)

        # Reset any RUNNING tasks to PENDING (crash recovery)
        reset_count = self._store.reset_running_tasks(run_id)
        if reset_count:
            logger.info("Reset %d running tasks to pending (crash recovery)", reset_count)
            for tid in task_states:
                if task_states[tid] == TaskStatus.RUNNING:
                    task_states[tid] = TaskStatus.PENDING


        # 尝试从 CheckpointManager 恢复最新检查点
        try:
            checkpoint = self._checkpoint_manager.restore_checkpoint(run_id)
            if checkpoint:
                logger.info("Restored checkpoint from phase '%s'", checkpoint.phase)
                # 检查点中的 task_states 会覆盖 Store 中的状态（如果更新）
                for tid, status_str in checkpoint.task_states.items():
                    if tid in task_states:
                        task_states[tid] = TaskStatus(status_str)
        except Exception as e:
            logger.warning("Failed to restore checkpoint: %s", e)

        self._scheduler.restore_state(task_states)

        logger.info("Resuming run %s for DAG '%s'", run_id, self._dag.name)

        # 启动心跳后台线程
        self._heartbeat.start_background()

        # 启动健康检查 HTTP 端点
        if self._config.health.health_enabled:
            self._health_server = HealthServer(
                self._config.health.health_port,
                bind=self._config.health.health_bind,
            )
            self._health_server.start()

        # 创建线程池和监控器
        self._create_pool_and_monitor()

        # 创建进度监控器（需要在 thread_monitor 之后）
        self._monitor = ProgressMonitor(
            self._dag.name, len(self._dag.tasks),
            interval=self._config.orchestrator.health_check_interval,
            scheduler=self._scheduler,
            budget_tracker=self._budget,
            thread_monitor=self._thread_monitor,
        )
        self._monitor.start()

        try:
            self._execute_loop()
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            self._run_info.status = RunStatus.CANCELLED
        except BudgetExhaustedError as e:
            logger.error("Budget exhausted: %s", e)
            self._run_info.status = RunStatus.FAILED
        except OrchestratorError as e:
            logger.error("Orchestrator error: %s", e)
            self._run_info.status = RunStatus.FAILED
        except Exception as e:
            logger.exception("Unexpected error during resume: %s", e)
            self._run_info.status = RunStatus.FAILED
        finally:
            try:
                if self._thread_monitor:
                    self._thread_monitor.stop()
            except Exception as e:
                logger.warning("Failed to stop thread monitor: %s", e)
            try:
                if self._monitor:
                    self._monitor.stop()
            except Exception as e:
                logger.warning("Failed to stop progress monitor: %s", e)
            self._finalize()

        return self._run_info

    def retry_failed(self, run_id: str) -> RunInfo:
        """Reset failed tasks and their downstream dependents, then resume."""
        # 输入验证：run_id 应为合法的十六进制字符串
        if not run_id or not re.fullmatch(r'[0-9a-fA-F]{1,32}', run_id):
            raise OrchestratorError(f"Invalid run_id format (expected hex string): {run_id!r}")

        info = self._store.get_run(run_id)
        if not info:
            raise OrchestratorError(f"Run '{run_id}' not found")

        saved_results = self._store.get_all_task_results(run_id)
        failed_ids = {tid for tid, r in saved_results.items() if r.status == TaskStatus.FAILED}

        if not failed_ids:
            logger.info("No failed tasks to retry")
            return info

        all_deps = {tid: self._dag.tasks[tid].depends_on for tid in self._dag.tasks}
        reset_count = self._store.reset_failed_and_downstream(run_id, failed_ids, all_deps)
        logger.info("Reset %d tasks for retry", reset_count)
        try:
            deleted = self._checkpoint_manager.delete_checkpoint(run_id)
            if deleted:
                logger.info("Deleted %d stale checkpoints before retry resume", deleted)
        except Exception as exc:
            logger.warning("Failed to delete stale checkpoints before retry resume: %s", exc)

        return self.resume(run_id)

    def _execute_loop(self) -> None:
        """主执行循环控制器：调度就绪任务、处理完成结果、管理优雅关闭。"""
        assert self._run_info is not None
        assert self._pool is not None

        cfg = self._config.orchestrator

        # 创建优雅关闭管理器
        shutdown_manager = GracefulShutdownManager(
            self._pool, timeout=30,
            store=self._store,
            run_id=self._run_info.run_id if self._run_info else None,
        )

        futures: dict[Future, str] = {}
        pool_shutdown = False
        loop_iteration = 0

        try:
            while self._scheduler.has_work_remaining():
                loop_iteration += 1

                # 记录当前各状态任务计数，便于诊断任务卡死问题
                status_counts = self._scheduler.summary()
                logger.debug(
                    "Loop #%d: pending=%d, running=%d, completed=%d, success=%d, failed=%d, futures=%d | full=%s",
                    loop_iteration,
                    status_counts.get("pending", 0),
                    status_counts.get("running", 0),
                    status_counts.get("completed", 0),
                    status_counts.get("success", 0),
                    status_counts.get("failed", 0),
                    len(futures),
                    status_counts,
                )

                # 上下文压缩检查
                if self._compactor.should_compact(self._outputs):
                    compact_result = self._compactor.compact(self._outputs)
                    if compact_result.compacted_keys:
                        logger.info(
                            "上下文压缩: %d 个输出被压缩 (%d -> %d 字符)",
                            len(compact_result.compacted_keys),
                            compact_result.total_before,
                            compact_result.total_after,
                        )

                # 心跳 touch
                self._heartbeat.touch()

                # Health check
                self._health_check()

                # 每 10 轮清理一次僵尸进程
                if loop_iteration % 10 == 0:
                    try:
                        get_process_cleaner().cleanup_suspended_processes()
                    except Exception as exc:
                        logger.warning("进程清理失败: %s", exc)

                # 检查线程监控器告警
                if self._thread_monitor:
                    alerts = self._thread_monitor.get_alerts()
                    for alert in alerts:
                        logger.warning("ThreadMonitor alert: %s", alert)

                # Update monitor
                if self._monitor:
                    self._monitor.update(
                        self._scheduler.summary(),
                        self._budget.spent,
                    )

                # 安全点 failback 检测：无活跃任务时检查是否需要回切到主 profile
                if (
                    self.requested_exit_code == 0
                    and self._pool_runtime is not None
                    and not futures
                    and self._pool_runtime.should_failback()
                ):
                    primary = self._pool_runtime.config.primary_profile.name
                    logger.info("Safe-point failback requested: %s -> %s", self._pool_runtime.active_profile, primary)
                    self._pool_runtime.write_request(
                        "failback",
                        target_profile=primary,
                        reason="primary_recovered",
                        metadata={"run_id": self._run_info.run_id},
                    )
                    self.requested_exit_code = PoolRuntime.EXIT_CODE_FAILBACK
                    break

                # 全局 429 暂停检查：如果有任务收到 429，暂停调度新任务
                if not self._rate_limit_pause_event.is_set():
                    pause_remaining = max(0.0, self._rate_limit_pause_until - time.monotonic())
                    if pause_remaining > 0:
                        logger.info(
                            "全局 429 暂停中，等待 %.1f 秒后继续调度",
                            pause_remaining,
                        )
                        self._rate_limit_pause_event.wait(timeout=pause_remaining)

                # 调度就绪任务
                self._schedule_ready_tasks(futures, shutdown_manager, cfg)

                if not futures:
                    # No running tasks and no ready tasks
                    if not self._scheduler.has_work_remaining():
                        # All tasks completed
                        break
                    if self._scheduler.running_count > 0:
                        # Scheduler thinks tasks are running but no futures tracked —
                        # this is a state inconsistency. Attempt recovery by resetting orphaned tasks.
                        orphaned_count = self._scheduler.running_count
                        logger.error(
                            "State inconsistency: scheduler.running_count=%d but no futures tracked. "
                            "Attempting recovery by resetting orphaned running tasks.",
                            orphaned_count,
                        )
                        # 重置所有 RUNNING 状态的任务为 PENDING，让它们可以被重新调度
                        reset_ids = self._scheduler.reset_running_to_pending()
                        if reset_ids:
                            logger.warning(
                                "Reset %d orphaned running tasks to pending: %s",
                                len(reset_ids), ", ".join(reset_ids)
                            )
                            # 继续循环，让这些任务被重新调度
                            continue
                        else:
                            # 无法恢复，终止循环
                            logger.error("Recovery failed: no tasks could be reset. Breaking.")
                            break
                    # True deadlock: no running tasks, no ready tasks, but work remains
                    logger.warning("No tasks can be scheduled — possible deadlock")
                    break

                # 等待至少一个任务完成
                done_futures, _ = wait(futures.keys(), return_when=FIRST_COMPLETED)

                # 处理已完成的任务
                self._process_completions(done_futures, futures)
        except KeyboardInterrupt:
            pool_shutdown = self._handle_graceful_shutdown(futures, shutdown_manager)
        finally:
            # 在关闭线程池前，执行任务间交叉验证（inter-task validation）
            self._run_inter_task_validation()

            # 如果未被 shutdown_manager 关闭，则正常关闭线程池
            if not pool_shutdown and self._pool:
                self._pool.shutdown(wait=True)

    def _schedule_ready_tasks(
        self,
        futures: dict[Future, str],
        shutdown_manager: GracefulShutdownManager,
        cfg: Any,
    ) -> None:
        """调度就绪任务：获取就绪列表、构建 prompt、提交线程池、处理背压。"""
        # 获取就绪任务列表
        if shutdown_manager and not shutdown_manager.is_accepting():
            ready = []
        else:
            ready = self._scheduler.get_ready_tasks(self._outputs)
        # TODO: 将 shared_prefix 传递给 run_claude_task 作为 --system-prompt 参数，
        # 利用 Anthropic API 的 Prompt Caching 机制减少重复 token 计算。
        # 当前仅做前缀检测，尚未完成与 claude_cli 的集成。
        if ready:
            ready_prompts = [t.prompt_template for t in ready]
            shared_prefix = extract_shared_prefix(ready_prompts)
            if shared_prefix:
                logger.debug("检测到共享 prompt 前缀: %d 字符（尚未集成到 CLI 调用）", len(shared_prefix))

        for task_node in ready:
            prev_status = self._scheduler.states.get(task_node.id, "unknown")
            logger.info(
                "准备调度任务: task_id='%s', 当前状态=%s, working_dir=%s",
                task_node.id,
                prev_status,
                self._working_dir or ".",
            )

            # 调度前检查 working_dir 是否仍然存在（目录可能在运行期间被外部删除）
            if self._working_dir and not os.path.isdir(self._working_dir):
                logger.error(
                    "Task '%s' 跳过调度: working_dir 已不存在: %s",
                    task_node.id, self._working_dir,
                )
                # 标记为失败而非 running，避免线程池浪费
                self._scheduler.mark_completed(task_node.id, TaskStatus.FAILED)
                self._handle_result(TaskResult(
                    task_id=task_node.id,
                    status=TaskStatus.FAILED,
                    error=f"working_dir does not exist at schedule time: {self._working_dir}",
                    started_at=datetime.now(),
                    finished_at=datetime.now(),
                ))
                continue

            self._scheduler.mark_running(task_node.id)
            logger.info("Scheduling task '%s'", task_node.id)

            # 构建安全且压缩后的上游输出（单次遍历，避免重复字典复制）
            # 合并 context_cache 和 outputs，outputs 优先覆盖 context_cache
            spill_threshold = self._config.spill.spill_threshold_chars
            deps_set = set(task_node.depends_on)
            compressed_outputs: dict[str, Any] = {}

            # 遍历 context_cache（低优先级），跳过被 outputs 覆盖的 key
            for key, value in self._context_cache.items():
                if key in self._outputs:
                    continue  # outputs 中有更新的值，稍后处理
                if key in deps_set:
                    value = self._quarantine.get_safe_output(key, value)
                    if value is None:
                        continue
                if isinstance(value, str) and len(value) > spill_threshold:
                    compressed_outputs[key] = self._summarizer.summarize(value, spill_threshold)
                    logger.debug("Compressed output for '%s': %d -> %d chars", key, len(value), len(compressed_outputs[key]))
                else:
                    compressed_outputs[key] = value

            # 遍历 outputs（高优先级，覆盖 context_cache）
            for key, value in self._outputs.items():
                if key in deps_set:
                    value = self._quarantine.get_safe_output(key, value)
                    if value is None:
                        compressed_outputs.pop(key, None)  # 移除 context_cache 中的旧值
                        continue
                if isinstance(value, str) and len(value) > spill_threshold:
                    compressed_outputs[key] = self._summarizer.summarize(value, spill_threshold)
                    logger.debug("Compressed output for '%s': %d -> %d chars", key, len(value), len(compressed_outputs[key]))
                else:
                    compressed_outputs[key] = value

            # Render prompt
            run_id = self._run_info.run_id if self._run_info else ""
            prompt = render_template(
                task_node.prompt_template,
                compressed_outputs,
                spill_config=self._config.spill,
                spill_dir=self._spill_dir,
                run_id=run_id,
            )

            # 清洗 prompt，防止注入和超长内容
            sanitized = self._sanitizer.sanitize(prompt, max_length=100000)
            if sanitized.warnings:
                logger.debug("任务 %s prompt 清洗: %s", task_node.id, ", ".join(sanitized.warnings))
            prompt = sanitized.cleaned_text

            # 构建下游上下文摘要（收集上游任务的 TaskResult）
            if task_node.depends_on:
                upstream_results = {}
                for dep_id in task_node.depends_on:
                    if dep_id in self._results:
                        upstream_results[dep_id] = self._results[dep_id]

                if upstream_results:
                    context_summary = self._summarizer.build_downstream_context(
                        upstream_results,
                        max_total_chars=8000
                    )
                    prompt = f"{prompt}\n\n## 上游任务摘要\n{context_summary}"

            # 附加黑板中的 facts
            try:
                facts_entries = self._blackboard.query('facts')
                if facts_entries:
                    facts_summary = "\n\n## 共享知识（Facts）\n"
                    for entry in facts_entries:
                        facts_summary += f"- [{entry.source_task}] {entry.key}: {entry.value}\n"
                    prompt = f"{prompt}{facts_summary}"
            except Exception as e:
                logger.warning("Failed to query blackboard facts: %s", e)

            # 集成 link resolver
            if task_node.links:
                resolved = resolve_links(task_node, compressed_outputs)
                prompt = inject_link_context(prompt, resolved)

            # 集成 field transform
            if task_node.transform:
                transformed = apply_transforms(task_node, compressed_outputs)
                if transformed:
                    transform_json = json.dumps(transformed, ensure_ascii=False, indent=2)
                    prompt = f"{prompt}\n\n## 转换数据\n{transform_json}"

            # 缓存 prompt 用于语义漂移检测
            self._lru_set(self._task_prompts, task_node.id, prompt, max_size=self._lru_max_outputs)

            # 背压检查：如果队列已满，采用 CallerRuns 策略
            if self._pool.pending_count >= cfg.queue_capacity:
                logger.warning(
                    "Task '%s' triggered backpressure (pending=%d >= capacity=%d), executing in caller thread",
                    task_node.id, self._pool.pending_count, cfg.queue_capacity
                )
                # 在当前线程同步执行任务
                logger.info(
                    "背压模式同步执行: task_id='%s', status=running, working_dir=%s, prompt长度=%d",
                    task_node.id,
                    self._working_dir or ".",
                    len(prompt),
                )
                try:
                    result = self._run_task_with_retry(task_node, prompt, shutdown_manager)
                except Exception as e:
                    logger.error("背压执行异常: %s", e)
                    result = TaskResult.from_exception(
                        task_node.id,
                        Exception(f"背压执行失败: {e}"),
                    )
                # 直接处理结果
                self._handle_result(result)
                # 背压路径同样调用回调，确保统计完整
                if self._on_task_result is not None:
                    try:
                        self._on_task_result(result)
                    except Exception as e:
                        logger.warning("on_task_result callback (backpressure) failed for task '%s': %s", result.task_id, e)
            else:
                # 正常提交到线程池
                logger.info(
                    "提交任务到线程池: task_id='%s', status=running, working_dir=%s, prompt长度=%d",
                    task_node.id,
                    self._working_dir or ".",
                    len(prompt),
                )
                try:
                    future = self._pool.submit(
                        self._run_task_with_retry,
                        task_node,
                        prompt,
                        shutdown_manager,
                    )
                    futures[future] = task_node.id
                except RuntimeError:
                    logger.warning("任务 '%s' 提交被拒绝（线程池已关闭）", task_node.id)
                    self._scheduler.mark_completed(task_node.id, TaskStatus.PENDING)
                    break

    def _process_completions(
        self,
        done_futures: set[Future],
        futures: dict[Future, str],
    ) -> None:
        """处理已完成的 future：提取结果、调用 _handle_result、保存检查点。"""
        for f in done_futures:
            task_id = futures.pop(f)
            try:
                result = f.result()
            except Exception as e:
                result = TaskResult.from_exception(
                    task_id,
                    e,
                )

            self._handle_result(result)

            # 持久化最新统计到外部状态文件（如 goal_state.json），避免进程异常退出时丢失
            if self._on_task_result is not None:
                try:
                    self._on_task_result(result)
                except Exception as e:
                    logger.warning("on_task_result callback failed for task '%s': %s", task_id, e)

            # 全局 429 暂停触发：如果任务失败且包含 429/限流错误，暂停调度 30 秒
            if result.status != TaskStatus.SUCCESS and result.error:
                error_lower = result.error.lower()
                if '429' in error_lower or 'rate limit' in error_lower or 'too many requests' in error_lower:
                    with self._rate_limit_pause_lock:
                        pause_end = time.monotonic() + 30.0
                        if pause_end > self._rate_limit_pause_until:
                            self._rate_limit_pause_until = pause_end
                            self._rate_limit_pause_event.clear()
                            # 30 秒后自动恢复
                            def _resume_scheduling():
                                time.sleep(30.0)
                                with self._rate_limit_pause_lock:
                                    if time.monotonic() >= self._rate_limit_pause_until - 1.0:
                                        self._rate_limit_pause_event.set()
                            threading.Thread(target=_resume_scheduling, daemon=True).start()
                            logger.warning(
                                "任务 '%s' 收到 429 限流，暂停调度新任务 30 秒",
                                task_id,
                            )

            # 保存检查点（每完成一个任务）
            try:
                task_states = {tid: self._scheduler.get_task_status(tid).value
                               for tid in self._dag.tasks}
                self._checkpoint_manager.save_checkpoint(
                    self._run_info.run_id,
                    phase=f"task_{result.task_id}",
                    task_states=task_states,
                    metadata={"completed_task": result.task_id, "status": result.status.value}
                )
            except Exception as e:
                logger.warning("Failed to save checkpoint: %s", e)

    def _handle_graceful_shutdown(
        self,
        futures: dict[Future, str],
        shutdown_manager: GracefulShutdownManager,
    ) -> bool:
        """处理优雅关闭：等待运行中任务完成、记录未完成任务。

        Returns:
            True 表示线程池已被 shutdown_manager 关闭。
        """
        logger.warning("KeyboardInterrupt detected, initiating graceful shutdown...")
        # 调用优雅关闭管理器，等待三阶段关闭完成
        shutdown_manager.shutdown()

        # 等待所有正在运行的任务完成（最多 shutdown_timeout 秒，避免无限挂起）
        _shutdown_timeout = self._config.orchestrator.shutdown_timeout
        logger.info("Waiting for %d running tasks to complete (max %ds)...", len(futures), _shutdown_timeout)
        shutdown_deadline = time.monotonic() + _shutdown_timeout
        while futures:
            if time.monotonic() >= shutdown_deadline:
                logger.warning(
                    "Shutdown deadline reached, abandoning %d remaining tasks", len(futures)
                )
                for f, task_id in list(futures.items()):
                    result = TaskResult.from_exception(
                        task_id,
                        Exception("Cancelled by user: shutdown deadline exceeded"),
                    )
                    self._handle_result(result)
                futures.clear()
                break

            done_futures = set()
            for f in list(futures):
                if f.done():
                    done_futures.add(f)

            if not done_futures:
                # 使用 wait 替代 sleep 轮询，最多等到 deadline
                remaining = max(0.1, shutdown_deadline - time.monotonic())
                done_futures, _ = wait(futures.keys(), timeout=remaining, return_when=FIRST_COMPLETED)

            if done_futures:
                for f in done_futures:
                    task_id = futures.pop(f)
                    try:
                        result = f.result()
                    except Exception as e:
                        result = TaskResult.from_exception(
                        task_id,
                        e,
                    )
                    self._handle_result(result)
            # wait() 已处理阻塞等待，无需额外 sleep

        logger.info("All running tasks completed, exiting gracefully")
        # 不 raise，让 run() 方法中的 finally 块处理状态设置
        self._run_info.status = RunStatus.CANCELLED
        return True

    def _execute_hook(self, hook_cmd: str, task_id: str, status: str) -> None:
        """执行生命周期钩子命令。

        使用 shlex.split() 参数化命令，避免 shell=True 带来的命令注入风险。
        Windows 下 shlex.split 不适用，退化为 shell=True 但对参数做基本校验。

        Args:
            hook_cmd: 要执行的命令字符串
            task_id: 任务 ID
            status: 任务状态（starting/success/failed）
        """
        # 输入校验：task_id 和 status 只允许安全字符，防止通过环境变量注入
        _SAFE_PATTERN = re.compile(r'^[\w\-.:/ ]+$')
        if not _SAFE_PATTERN.match(task_id):
            logger.warning("Hook skipped: task_id contains unsafe characters: %r", task_id)
            return
        if not _SAFE_PATTERN.match(status):
            logger.warning("Hook skipped: status contains unsafe characters: %r", status)
            return

        try:
            env = os.environ.copy()
            env["TASK_ID"] = task_id
            env["TASK_STATUS"] = status

            normalized_hook_cmd = normalize_python_command(hook_cmd)
            if sys.platform == 'win32':
                parts = shlex.split(normalized_hook_cmd, posix=False)
                normalized_parts = [
                    part[1:-1] if len(part) >= 2 and part[0] == part[-1] and part[0] in {"'", '"'} else part
                    for part in parts
                ]
                executable = sys.executable.replace("/", "\\").lower()
                if normalized_parts and normalized_parts[0].replace("/", "\\").lower() == executable:
                    cmd: str | list[str] = normalized_parts
                    use_shell = False
                else:
                    cmd = normalized_hook_cmd
                    use_shell = True
            else:
                cmd = shlex.split(normalized_hook_cmd)
                use_shell = False

            result = subprocess.run(
                cmd,
                shell=use_shell,
                env=env,
                timeout=10,
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                logger.warning(
                    "Hook '%s' for task '%s' (status=%s) failed with code %d: %s",
                    hook_cmd, task_id, status, result.returncode, result.stderr.strip()
                )
            else:
                logger.debug(
                    "Hook '%s' for task '%s' (status=%s) succeeded",
                    hook_cmd, task_id, status
                )
        except subprocess.TimeoutExpired:
            logger.warning(
                "Hook '%s' for task '%s' (status=%s) timed out after 10s",
                hook_cmd, task_id, status
            )
        except Exception as e:
            logger.warning(
                "Hook '%s' for task '%s' (status=%s) failed: %s",
                hook_cmd, task_id, status, e
            )

    def _check_risk_approval(self, task_node, task_start_time: datetime) -> TaskResult | None:
        """风险评估和审批。返回 TaskResult 表示被拒绝，None 表示通过。"""
        risk_level = self._escalation_manager.assess_risk(task_node)
        logger.info("Task '%s' risk assessment: %s", task_node.id, risk_level.value)

        # 仅对 HIGH 及以上风险触发审批，避免阻塞大多数任务
        if risk_level not in (RiskLevel.HIGH, RiskLevel.CRITICAL):
            return None
        if not self._escalation_manager.should_escalate(risk_level):
            return None

        # 请求人工审批
        risk_reason = f"任务风险等级为 {risk_level.value}，需要人工审批"
        approved = self._escalation_manager.request_approval(
            task_id=task_node.id,
            risk_level=risk_level,
            reason=risk_reason,
            task_node=task_node,
        )
        approval_summary = self._escalation_manager.get_approval_summary(
            task_node.id, approved, risk_level
        )
        logger.info(approval_summary)

        if not approved:
            logger.warning("Task '%s' skipped due to approval rejection", task_node.id)
            return TaskResult.from_exception(
                task_node.id,
                Exception(f"Task skipped: approval rejected (risk level: {risk_level.value})"),
                duration_seconds=(datetime.now() - task_start_time).total_seconds(),
                started_at=task_start_time,
            )
        return None

    def _check_task_caches(self, task_node, prompt: str) -> TaskResult | None:
        """检查 TaskCache 和 OutputCache。返回 TaskResult 表示命中，None 表示未命中。"""
        # TaskCache（仅对 idempotent=True 的任务）
        if task_node.idempotent:
            cache_model = task_node.model
            if cache_model is None and self._pool_runtime is not None:
                cache_model = self._pool_runtime.claude_config_for_profile(self._config.claude).default_model
            if cache_model is None:
                cache_model = self._config.claude.default_model
            cached_result = self._task_cache.get(task_node.id, prompt, cache_model)
            if cached_result is not None:
                logger.info(
                    "Task '%s' hit TaskCache (idempotent), reusing cached result (saved $%.4f)",
                    task_node.id, cached_result.cost_usd,
                )
                cached_result.cost_usd = 0.0
                cached_result.finished_at = datetime.now()
                return cached_result

        # OutputCache（基于 prompt + model + working_dir 的哈希）
        model = task_node.model or self._config.claude.default_model
        working_dir = self._working_dir or "."
        cache_key = self._output_cache.compute_hash(prompt, model, working_dir)
        cached_entry = self._output_cache.get(cache_key)
        if cached_entry is not None:
            logger.info(
                "Task '%s' cache hit, reusing cached result (saved $%.4f)",
                task_node.id, cached_entry.cost_usd,
            )
            try:
                parsed_output = json.loads(cached_entry.output) if cached_entry.output else None
            except (json.JSONDecodeError, TypeError):
                parsed_output = cached_entry.output
            # 缓存路径：确保 parsed_output 不为 None（与 claude_cli.py 行为一致）
            if parsed_output is None and cached_entry.output:
                parsed_output = cached_entry.output
            return TaskResult(
                task_id=task_node.id,
                status=TaskStatus.SUCCESS,
                output=cached_entry.output,
                parsed_output=parsed_output,
                cost_usd=0.0,
                duration_seconds=0.0,
                finished_at=datetime.now(),
            )
        return None

    def _prepare_task_context(self, task_node, prompt: str):
        """准备任务执行上下文：角色分配、模型选择、钩子、流式回调。

        返回 _TaskExecutionContext 包含所有后续重试循环需要的预计算值。
        """
        # 角色分配（仅在缓存未命中时执行，此处已保证未命中）
        assigned_role = self._role_assigner.assign(task_node)
        task_node = apply_role(task_node, assigned_role)
        logger.info(
            "Task '%s' assigned role: %s (model=%s)",
            task_node.id, assigned_role.name, assigned_role.preferred_model,
        )

        # 格式化任务 ID（如果有 color 则使用颜色标签）
        task_id_display = (
            f"[{task_node.color}]{task_node.id}[/{task_node.color}]"
            if task_node.color else task_node.id
        )

        # 执行 on_task_start 钩子
        if self._dag.hooks and self._dag.hooks.on_task_start:
            self._execute_hook(self._dag.hooks.on_task_start, task_node.id, "starting")

        # 基于复杂度自动选择模型
        effective_task = task_node
        if task_node.model is None and task_node.complexity is not None:
            complexity_model_map = {"simple": "haiku", "moderate": "sonnet", "complex": "opus"}
            auto_model = complexity_model_map.get(task_node.complexity)
            if auto_model:
                effective_task = replace(task_node, model=auto_model)
                logger.info(
                    "Task '%s' complexity='%s' → auto-selected model='%s'",
                    task_id_display, task_node.complexity, auto_model,
                )

        # 流式事件持久化回调
        _stream_file_max_bytes = self._config.limits.stream_file_max_bytes
        spill_dir = self._spill_dir
        task_id = task_node.id

        def _on_progress(event_type: str, event: dict) -> None:
            """将流式事件写入状态文件 {spill_dir}/{task_id}_stream.jsonl。"""
            try:
                spill_dir.mkdir(parents=True, exist_ok=True)
                stream_file = spill_dir / f"{task_id}_stream.jsonl"
                if stream_file.exists() and stream_file.stat().st_size >= _stream_file_max_bytes:
                    return
                with open(stream_file, "a", encoding="utf-8") as f:
                    json.dump({"event_type": event_type, "event": event}, f, ensure_ascii=False)
                    f.write("\n")
            except Exception as e:
                logger.debug("Failed to persist stream event for task '%s': %s", task_id, e)

        # OutputCache key（基于原始 prompt 计算，重试期间不变）
        model_for_key = effective_task.model or self._config.claude.default_model
        working_dir = self._working_dir or "."
        cache_key = self._output_cache.compute_hash(prompt, model_for_key, working_dir)

        # 计算有效最大重试次数（辅助任务减半）
        effective_max_attempts = task_node.retry_policy.max_attempts
        if not task_node.is_critical:
            effective_max_attempts = max(1, effective_max_attempts // 2)

        return _TaskExecutionContext(
            task_node=task_node,
            effective_task=effective_task,
            task_id_display=task_id_display,
            on_progress=_on_progress,
            cache_key=cache_key,
            effective_max_attempts=effective_max_attempts,
            execution_profile=self._pool_runtime.active_profile if self._pool_runtime else "",
            attempted_profiles=set(),
            failure_history=[],
            current_prompt=prompt,
        )

    def _execute_single_attempt(self, ctx, attempt: int, attempt_task,
                                 attempt_claude_config, attempt_rate_limiter,
                                 shutdown_manager,
                                 task_start_time: datetime | None = None) -> TaskResult:
        """执行单次 Claude CLI 调用，包含输入/输出 guardrail 检查。"""
        task_node = ctx.task_node
        task_id_display = ctx.task_id_display
        current_prompt = ctx.current_prompt

        # 输入安全检查（guardrail）
        input_check = self._input_guardrail.check(current_prompt)
        if not input_check.passed:
            critical_violations = [v for v in input_check.violations if any(
                keyword in v for keyword in [
                    "API Key", "Password", "Token", "Secret Key", "Private Key", "Access Key",
                ]
            )]
            if critical_violations:
                logger.error(
                    "Task '%s' input guardrail CRITICAL violation, aborting: %s",
                    task_node.id, "; ".join(critical_violations),
                )
                return TaskResult.from_exception(
                    task_node.id,
                    Exception(f"Input guardrail critical violation: {critical_violations[0]}"),
                    duration_seconds=0.0,
                    started_at=task_start_time,
                )
            else:
                logger.warning(
                    "Task '%s' input guardrail warnings: %s",
                    task_node.id, "; ".join(input_check.violations),
                )

        # 检查 working_dir 是否存在，避免子进程在无效目录中启动
        if self._working_dir and not os.path.isdir(self._working_dir):
            logger.error(
                "Task '%s' aborted: working_dir does not exist: %s",
                task_node.id, self._working_dir,
            )
            return TaskResult.from_exception(
                task_node.id,
                FileNotFoundError(f"working_dir does not exist: {self._working_dir}"),
                duration_seconds=0.0,
                started_at=task_start_time,
            )

        # 通过插件系统执行任务
        try:
            executor = PluginRegistry.get_executor(attempt_task.type)
            executor_config = self._config if attempt_task.type == "agent_cli" else attempt_claude_config
            result = executor.execute(
                task=attempt_task,
                prompt=current_prompt,
                claude_config=executor_config,
                limits=self._config.limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
                audit_logger=self._audit_logger,
                rate_limiter=attempt_rate_limiter,
                on_progress=ctx.on_progress,
            )
        except Exception as exc:
            logger.error(
                "Task '%s' run_claude_task raised unexpected exception (attempt %d/%d): %s",
                task_node.id, attempt, ctx.effective_max_attempts, exc,
            )
            result = TaskResult.from_exception(task_node.id, exc, duration_seconds=0.0, started_at=task_start_time)

        result.attempt = attempt

        # 输出安全检查（guardrail）
        if result.status == TaskStatus.SUCCESS and result.output:
            output_check = self._output_guardrail.check(result.output)
            if not output_check.passed:
                critical_violations = [v for v in output_check.violations if any(
                    keyword in v for keyword in [
                        "API Key", "Password", "Token", "Secret Key", "Private Key", "Access Key",
                        "危险的文件删除", "危险的数据库删除", "危险的磁盘格式化", "危险的批量删除",
                        "Fork 炸弹", "危险的 eval", "危险的 exec",
                    ]
                )]
                if critical_violations:
                    logger.error(
                        "Task '%s' output guardrail CRITICAL violation, marking as failed: %s",
                        task_node.id, "; ".join(critical_violations),
                    )
                    result.status = TaskStatus.FAILED
                    result.error = f"Output guardrail critical violation: {critical_violations[0]}"
                else:
                    logger.warning(
                        "Task '%s' output guardrail warnings: %s",
                        task_node.id, "; ".join(output_check.violations),
                    )

        # 注册子进程 PID
        if result.pid > 0:
            shutdown_manager.register_subprocess(result.pid)
            logger.debug("Registered subprocess PID %d for task '%s'", result.pid, task_node.id)

        return result

    def _record_task_metric(
        self,
        *,
        task_id: str,
        task_start_time: datetime,
        end_time: datetime,
        result: TaskResult,
        retry_count: int,
        status: str,
    ) -> None:
        metric = TaskMetrics(
            task_id=task_id,
            start_time=task_start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_ms=(end_time - task_start_time).total_seconds() * 1000,
            retry_count=retry_count,
            cli_duration_ms=result.duration_seconds * 1000 if result.duration_seconds else None,
            token_input=getattr(result, "token_input", None),
            token_output=getattr(result, "token_output", None),
            status=status,
        )
        self._metrics_collector.record(metric)

    def _record_task_lifecycle_diagnostic(
        self,
        *,
        event_type,
        task_id: str,
        model: str,
        attempt: int,
        task_start_time: datetime,
        result: TaskResult,
        include_error: bool = False,
    ) -> None:
        if DiagnosticEventType is None:
            return
        payload = {
            "event_type": event_type,
            "run_id": self._run_info.run_id if self._run_info else "",
            "task_id": task_id,
            "model": model,
            "attempt": attempt,
            "duration_ms": (datetime.now() - task_start_time).total_seconds() * 1000,
            "cost_usd": result.cost_usd,
        }
        if include_error:
            payload["error"] = result.error or ""
        self._diagnostics.record_task_lifecycle(**payload)

    def _cache_successful_result(
        self,
        *,
        task_node: TaskNode,
        cache_key: str,
        prompt: str,
        model: str,
        result: TaskResult,
    ) -> None:
        if result.parsed_output is not None:
            try:
                if isinstance(result.parsed_output, (dict, list)):
                    cache_output = json.dumps(result.parsed_output, ensure_ascii=False)
                else:
                    cache_output = str(result.parsed_output)
                self._output_cache.put(cache_key, cache_output, result.cost_usd)
                logger.debug("Task '%s' result cached", task_node.id)
            except (TypeError, ValueError) as exc:
                logger.warning("Failed to cache task '%s' output: %s", task_node.id, exc)

        if task_node.idempotent:
            try:
                self._task_cache.put(task_node.id, prompt, model, result)
                logger.debug("Task '%s' result cached in TaskCache (idempotent)", task_node.id)
            except Exception as exc:
                logger.warning("Failed to cache task '%s' in TaskCache: %s", task_node.id, exc)

    def _handle_attempt_success(self, ctx, result: TaskResult, attempt: int,
                                 task_start_time: datetime, model: str) -> TaskResult:
        """处理成功的 attempt：缓存、钩子、metrics、诊断记录。"""

        task_node = ctx.task_node
        task_id_display = ctx.task_id_display

        # 通知 rate_limiter 重置 429 退避
        self._rate_limiter.report_success()

        # 执行 on_task_complete 钩子
        if self._dag.hooks and self._dag.hooks.on_task_complete:
            self._execute_hook(self._dag.hooks.on_task_complete, task_node.id, "success")

        self._cache_successful_result(
            task_node=task_node,
            cache_key=ctx.cache_key,
            prompt=ctx.current_prompt,
            model=model,
            result=result,
        )

        # 记录成功的 metrics
        try:
            self._record_task_metric(
                task_id=task_node.id,
                task_start_time=task_start_time,
                end_time=datetime.now(),
                result=result,
                retry_count=attempt - 1,
                status="success",
            )
        except Exception as e:
            logger.warning("Failed to record metrics for task '%s': %s", task_node.id, e)

        # 诊断事件：任务成功（diagnostics 模块缺失时静默跳过）
        try:
            self._record_task_lifecycle_diagnostic(
                event_type=DiagnosticEventType.TASK_COMPLETE if DiagnosticEventType is not None else None,
                task_id=task_node.id,
                model=model,
                attempt=attempt,
                task_start_time=task_start_time,
                result=result,
                include_error=False,
            )
        except Exception as e:
            logger.warning("Failed to record diagnostics for task '%s': %s", task_node.id, e)

        return result

    def _record_final_failure_metrics(self, ctx, result: TaskResult,
                                       task_start_time: datetime, attempt: int) -> None:
        """记录最终失败任务的 metrics 和诊断事件。"""

        task_node = ctx.task_node
        end_time = datetime.now()
        wall_time = (end_time - task_start_time).total_seconds()

        # 确保 duration 反映实际 wall time
        if result.duration_seconds <= 0 and wall_time > 0:
            result.duration_seconds = wall_time
        # 零成本是合法的（任务可能在 API 调用前失败）
        if result.cost_usd <= 0 and wall_time > 0:
            result.cost_usd = 0.0
            logger.info(
                "Task '%s' has zero cost with wall_time=%.1fs (likely failed before API call)",
                task_node.id, wall_time,
            )

        try:
            self._record_task_metric(
                task_id=task_node.id,
                task_start_time=task_start_time,
                end_time=end_time,
                result=result,
                retry_count=ctx.effective_max_attempts - 1,
                status="failed" if result.status == TaskStatus.FAILED else "timeout",
            )
        except Exception as e:
            logger.warning("Failed to record metrics for task '%s': %s", task_node.id, e)

        # 诊断事件：任务最终失败（diagnostics 模块缺失时静默跳过）
        model = ctx.effective_task.model or self._config.claude.default_model
        try:
            self._record_task_lifecycle_diagnostic(
                event_type=DiagnosticEventType.TASK_FAIL if DiagnosticEventType is not None else None,
                task_id=task_node.id,
                model=model,
                attempt=attempt,
                task_start_time=task_start_time,
                result=result,
                include_error=True,
            )
        except Exception as e:
            logger.warning("Failed to record diagnostics for task '%s': %s", task_node.id, e)

    def _run_task_with_retry(self, task_node, prompt: str, shutdown_manager: GracefulShutdownManager) -> TaskResult:
        """Run a task with retry logic — 精简的重试编排器。"""
        assert self._run_info is not None

        task_start_time = datetime.now()

        # 1. 风险审批
        rejected = self._check_risk_approval(task_node, task_start_time)
        if rejected is not None:
            logger.warning(
                "Task '%s' REJECTED by risk approval: status=%s, error=%.200s, decision=abort_risk_rejected",
                task_node.id,
                rejected.status.value if hasattr(rejected.status, 'value') else str(rejected.status),
                rejected.error or "unknown",
            )
            return rejected

        # 2. 缓存检查
        cached = self._check_task_caches(task_node, prompt)
        if cached is not None:
            if cached.started_at is None:
                cached.started_at = task_start_time
            logger.warning(
                "Task '%s' CACHE HIT: status=%s, duration=%.1fs, cost=$%.4f, decision=return_cached_result",
                task_node.id,
                cached.status.value if hasattr(cached.status, 'value') else str(cached.status),
                cached.duration_seconds or 0.0,
                cached.cost_usd or 0.0,
            )
            return cached

        # 3. 准备执行上下文
        ctx = self._prepare_task_context(task_node, prompt)

        # 4. 重试循环
        logger.warning(
            "Task '%s' entering retry loop: max_attempts=%d, model=%s, prompt_len=%d",
            ctx.task_id_display, ctx.effective_max_attempts,
            ctx.effective_task.model or self._config.claude.default_model,
            len(ctx.current_prompt),
        )
        for attempt in range(1, ctx.effective_max_attempts + 1):
            logger.warning(
                "Task '%s' attempt %d/%d starting",
                ctx.task_id_display, attempt, ctx.effective_max_attempts,
            )
            # 重试前检测网络连通性（首次跳过）
            if attempt > 1:
                from .claude_cli import _wait_for_network
                net_ok = _wait_for_network(ctx.task_id_display)
                logger.warning(
                    "Task '%s' retry attempt %d/%d starting, network_ok=%s",
                    ctx.task_id_display, attempt, ctx.effective_max_attempts, net_ok,
                )
                if not net_ok:
                    logger.warning(
                        "Task '%s' 网络探测不可达，但仍尝试第 %d 次重试（claude CLI 可能有独立代理）",
                        ctx.task_id_display, attempt,
                    )

            # 获取当前 attempt 的运行时配置（可能被 Pool overlay 修改）
            attempt_task = ctx.effective_task
            attempt_claude_config = self._config.claude
            attempt_rate_limiter = self._rate_limiter
            attempt_profile = ctx.execution_profile
            if self._pool_runtime is not None:
                attempt_profile = attempt_profile or self._pool_runtime.active_profile
                attempt_task = self._pool_runtime.apply_task_overlay(ctx.effective_task, attempt_profile)
                attempt_claude_config = self._pool_runtime.claude_config_for_profile(self._config.claude, attempt_profile)
                attempt_rate_limiter = self._pool_runtime.rate_limiter_for_profile(self._config.rate_limit, attempt_profile)
                ctx.attempted_profiles.add(attempt_profile)

            model = attempt_task.model or attempt_claude_config.default_model
            attempt_rate_limiter.acquire(model)

            try:
                result = self._execute_single_attempt(
                    ctx, attempt, attempt_task, attempt_claude_config,
                    attempt_rate_limiter, shutdown_manager,
                    task_start_time=task_start_time,
                )

                # 保存 attempt 记录（含失败）
                try:
                    self._store.save_attempt(self._run_info.run_id, result, prompt=ctx.current_prompt)
                except Exception as e:
                    logger.warning(
                        "Failed to save attempt record for task '%s' (attempt %d): %s",
                        ctx.task_id_display, attempt, e,
                    )

                result.attempt = attempt

                if result.status == TaskStatus.SUCCESS:
                    logger.warning(
                        "Task '%s' SUCCEEDED on attempt %d/%d, duration=%.1fs, cost=$%.4f, model=%s",
                        ctx.task_id_display, attempt, ctx.effective_max_attempts,
                        (datetime.now() - task_start_time).total_seconds(),
                        result.cost_usd, model,
                    )
                    success_result = self._handle_attempt_success(
                        ctx, result, attempt, task_start_time, model,
                    )
                    # 确保 started_at 反映任务开始时间（含重试前的开销），而非最后一次 CLI 调用时间
                    success_result.started_at = task_start_time
                    return success_result

                # 失败处理：failover 决策
                decision = self._retry_strategy.decide_retry_action(
                    ctx, result, attempt, task_start_time, model,
                )
                logger.warning(
                    "Task '%s' attempt %d/%d failed: status=%s, error=%.200s, duration=%.1fs, cost=$%.4f, model=%s → decision=%s, next_model=%s",
                    ctx.task_id_display, attempt, ctx.effective_max_attempts,
                    result.status.value if hasattr(result.status, 'value') else str(result.status),
                    result.error or "unknown",
                    result.duration_seconds or 0.0,
                    result.cost_usd or 0.0,
                    model,
                    decision.action,
                    getattr(decision, 'model', None) or "N/A",
                )

                if decision.action == "abort":
                    logger.warning(
                        "Task '%s' ABORT: attempt=%d/%d, error=%.200s, reason=retry_strategy_decided_abort",
                        ctx.task_id_display, attempt, ctx.effective_max_attempts,
                        result.error or "unknown",
                    )
                    decision.result.started_at = task_start_time
                    return decision.result
                elif decision.action == "switch_model":
                    ctx.effective_task = replace(ctx.effective_task, model=decision.model)
                    logger.warning(
                        "Task '%s' SWITCH_MODEL: attempt=%d/%d, old_model=%s → new_model=%s, error=%.200s",
                        ctx.task_id_display, attempt, ctx.effective_max_attempts,
                        model, decision.model, result.error or "unknown",
                    )
                    continue
                elif decision.action == "switch_profile":
                    logger.warning(
                        "Task '%s' SWITCH_PROFILE: attempt=%d/%d, error=%.200s",
                        ctx.task_id_display, attempt, ctx.effective_max_attempts,
                        result.error or "unknown",
                    )
                    continue
                elif decision.action == "retry_immediately":
                    logger.warning(
                        "Task '%s' RETRY_IMMEDIATELY: attempt=%d/%d, error=%.200s",
                        ctx.task_id_display, attempt, ctx.effective_max_attempts,
                        result.error or "unknown",
                    )
                    continue
                elif decision.action == "retry_with_backoff":
                    backoff_info = getattr(decision, 'backoff_seconds', None) or "calculated_in_handler"
                    logger.warning(
                        "Task '%s' RETRY_WITH_BACKOFF: attempt=%d/%d, error=%.200s, backoff=%s",
                        ctx.task_id_display, attempt, ctx.effective_max_attempts,
                        result.error or "unknown", backoff_info,
                    )
                    continue  # 退避已在 _handle_attempt_failure 中完成

            except Exception as exc:
                # _execute_single_attempt 抛异常时 result 未赋值，构造失败结果
                logger.warning(
                    "Task '%s' UNHANDLED EXCEPTION on attempt %d/%d: type=%s, error=%.200s, model=%s, decision=construct_failure_and_continue_or_exhaust",
                    ctx.task_id_display, attempt, ctx.effective_max_attempts,
                    type(exc).__name__, str(exc), model,
                )
                result = TaskResult(
                    task_id=ctx.task_node.id,
                    status=TaskStatus.FAILED,
                    error=f"Unhandled exception on attempt {attempt}: {type(exc).__name__}: {exc}",
                    cost_usd=0.0,
                    duration_seconds=(datetime.now() - task_start_time).total_seconds(),
                    started_at=task_start_time,
                )

            finally:
                _pid = getattr(result, "pid", 0) or 0
                if _pid > 0:
                    shutdown_manager.unregister_subprocess(_pid)
                    logger.debug(
                        "Unregistered subprocess PID %d for task '%s'",
                        _pid, ctx.task_node.id,
                    )

        # 所有重试耗尽，记录最终失败
        logger.warning(
            "Task '%s' ALL RETRIES EXHAUSTED: attempts=%d/%d, final_status=%s, final_error=%.200s, duration=%.1fs, cost=$%.4f, model=%s, tried_profiles=%s",
            ctx.task_id_display, attempt, ctx.effective_max_attempts,
            result.status.value if hasattr(result.status, 'value') else str(result.status),
            result.error or "unknown",
            (datetime.now() - task_start_time).total_seconds(),
            result.cost_usd,
            model,
            ctx.attempted_profiles or set(),
        )
        self._record_final_failure_metrics(ctx, result, task_start_time, attempt)
        # 确保 started_at 反映任务开始时间（含全部重试开销）
        result.started_at = task_start_time
        return result

    def _handle_result(self, result: TaskResult) -> None:
        """Process a completed task result."""
        assert self._run_info is not None

        # 确保时间记录完整：在回调中设置 finished_at，根据 started_at 计算 duration_seconds
        if result.started_at is not None and result.finished_at is None:
            result.finished_at = datetime.now()
        if result.started_at is not None and result.finished_at is not None:
            result.duration_seconds = (
                result.finished_at - result.started_at
            ).total_seconds()

        _dur = result.duration_seconds
        _cost = result.cost_usd
        logger.info(
            "[_handle_result] ENTER task='%s', status=%s, error=%s, duration=%s, cost=%s, parsed_output=%s",
            result.task_id, result.status.value,
            (result.error or "none")[:200],
            f"{_dur:.1f}s" if _dur is not None else "None",
            f"${_cost:.4f}" if _cost is not None else "None",
            type(result.parsed_output).__name__ if result.parsed_output is not None else "None",
        )

        # 防御性检查：task_id 必须在 DAG 中存在
        if result.task_id not in self._dag.tasks:
            logger.error(
                "[_handle_result] task_id='%s' not found in DAG tasks (known=%s), "
                "marking as FAILED to prevent KeyError",
                result.task_id, list(self._dag.tasks.keys())[:10],
            )
            result.status = TaskStatus.FAILED
            result.error = f"Unknown task_id '{result.task_id}' not in DAG definition"
            self._lru_set(self._results, result.task_id, result, max_size=self._lru_max_results)
            self._store.update_task(self._run_info.run_id, result)
            # 必须通知调度器任务已完成，否则任务会永久卡在 running 状态
            self._scheduler.mark_completed(result.task_id, TaskStatus.FAILED, self._outputs)
            return

        self._lru_set(self._results, result.task_id, result, max_size=self._lru_max_results)
        self._store.update_task(self._run_info.run_id, result)

        if result.status == TaskStatus.SUCCESS:
            task_node = self._dag.tasks[result.task_id]
            logger.info(
                "[_handle_result] SUCCESS branch: task='%s', has_parsed_output=%s, output_type=%s",
                result.task_id,
                result.parsed_output is not None,
                type(result.parsed_output).__name__ if result.parsed_output is not None else "N/A",
            )

            # 1. 先执行单任务自检验证（intra-task validation）
            intra_result = self._intra_validator.validate(task_node, result)
            if not intra_result.passed:
                logger.warning(
                    "Task '%s' failed intra-task validation: %s",
                    result.task_id, ", ".join(intra_result.issues)
                )
                # 将任务标记为 FAILED
                result.status = TaskStatus.FAILED
                result.error = f"Intra-task validation failed: {'; '.join(intra_result.issues)}"
                self._lru_set(self._results, result.task_id, result, max_size=self._lru_max_results)
                self._store.update_task(self._run_info.run_id, result)

                # 隔离验证失败的任务输出
                self._quarantine.quarantine(result.task_id, f"Intra-task validation failed: {intra_result.issues[0] if intra_result.issues else 'unknown'}")
                logger.warning("Task '%s' output quarantined due to intra-task validation failure", result.task_id)

                # 通知调度器任务已完成（失败），避免状态残留在 running
                self._scheduler.mark_completed(result.task_id, TaskStatus.FAILED, self._outputs)

                # 传播失败到下游任务
                self._failure_propagator.propagate(
                    result.task_id,
                    cancel_reason_template="Cancelled due to intra-task validation failure of '{task_id}'",
                    log_description="intra-task validation failure",
                )
                return

            # 2. 语义漂移检测（仅记录日志，不阻断任务）
            if result.task_id in self._task_prompts:
                original_prompt = self._task_prompts[result.task_id]
                output = result.output or ""

                drift_result = self._drift_detector.detect(result.task_id, original_prompt, output)

                if drift_result.drifted:
                    logger.warning(
                        "Task '%s' semantic drift detected (similarity=%.2f): %s (non-blocking)",
                        result.task_id, drift_result.similarity, drift_result.detail
                    )
                    get_notifier().warning(
                        "语义漂移",
                        detail=f"任务 {result.task_id} 相似度={drift_result.similarity:.2f}",
                    )

            # 3. 执行验证门禁（如果配置了）
            if task_node.validation_gate is not None:
                validation_passed = self._run_validation_gate(task_node, result)
                result.validation_passed = validation_passed

                if not validation_passed:
                    # 验证失败，将任务标记为 FAILED
                    result.status = TaskStatus.FAILED
                    self._lru_set(self._results, result.task_id, result, max_size=self._lru_max_results)
                    self._store.update_task(self._run_info.run_id, result)

                    # 隔离验证失败的任务输出
                    self._quarantine.quarantine(result.task_id, f"Validation failed: {result.error}")
                    logger.warning("Task '%s' output quarantined due to validation failure", result.task_id)

                    # 通知调度器任务已完成（失败），避免状态残留在 running
                    self._scheduler.mark_completed(result.task_id, TaskStatus.FAILED, self._outputs)

                    # 传播失败到下游任务
                    self._failure_propagator.propagate(
                        result.task_id,
                        cancel_reason_template="Cancelled due to validation failure of '{task_id}'",
                        log_description="validation failure",
                    )
                    return

            # 验证通过或无验证门禁，标记为成功
            self._lru_set(self._outputs, result.task_id, result.parsed_output, max_size=self._lru_max_outputs)
            self._scheduler.mark_completed(result.task_id, TaskStatus.SUCCESS, self._outputs)

            # 发布中间结果到黑板（截断大对象以防内存膨胀）
            if result.parsed_output is not None:
                try:
                    bb_value = result.parsed_output
                    # 对大字符串截断，对大 dict/list 序列化后检查
                    _bb_max = self._config.limits.blackboard_value_max_chars
                    if isinstance(bb_value, str) and len(bb_value) > _bb_max:
                        bb_value = bb_value[:_bb_max] + "...[truncated]"
                    elif isinstance(bb_value, (dict, list)):
                        serialized = json.dumps(bb_value, ensure_ascii=False)
                        if len(serialized) > _bb_max:
                            bb_value = serialized[:_bb_max] + "...[truncated]"
                    self._blackboard.post(
                        category='intermediate_results',
                        key=result.task_id,
                        value=bb_value,
                        source_task=result.task_id
                    )
                except Exception as e:
                    logger.warning("Failed to post result to blackboard for task '%s': %s", result.task_id, e)

            # 保存 loop 状态到 Store（支持断点续传）
            loop_counts = self._scheduler.get_loop_counts()
            if result.task_id in loop_counts:
                try:
                    output_str = json.dumps(result.parsed_output, ensure_ascii=False) if result.parsed_output else ""
                    self._store.save_loop_state(
                        self._run_info.run_id,
                        result.task_id,
                        loop_counts[result.task_id],
                        output_str
                    )
                except Exception as e:
                    logger.warning("Failed to save loop state for task '%s': %s", result.task_id, e)

            # 存储到 context store 支持断点续传
            if result.parsed_output is not None:
                try:
                    value_json = json.dumps(result.parsed_output, ensure_ascii=False)
                    self._store.set_context(self._run_info.run_id, result.task_id, value_json)
                    self._lru_set(self._context_cache, result.task_id, result.parsed_output, max_size=self._lru_max_outputs)
                except Exception as e:
                    logger.warning("Failed to save context for task '%s': %s", result.task_id, e)

            logger.info(
                "Task '%s' succeeded (%.1fs, $%.4f)",
                result.task_id, result.duration_seconds, result.cost_usd,
            )
        else:
            # 任务失败，记录错误分类
            logger.info(
                "[_handle_result] FAILURE branch: task='%s', status=%s",
                result.task_id, result.status.value,
            )
            task_node = self._dag.tasks[result.task_id]
            error_msg = result.error or ""
            error_category = classify_error(error_msg)
            logger.info(
                "Task '%s' failed, error category: %s, error=%.200s",
                result.task_id, error_category.value,
                error_msg[:200],
            )

            # 隔离失败任务的输出
            self._quarantine.quarantine(result.task_id, f"Task failed: {error_msg[:100]}")
            logger.warning("Task '%s' output quarantined due to failure", result.task_id)

            # 根据 error_policy 决定失败处理行为
            # 优先使用任务级别的 error_policy，否则使用 CLI 传入的默认策略
            on_error = None
            if task_node.error_policy is not None:
                on_error = task_node.error_policy.on_error
            elif self._default_error_policy is not None:
                on_error = self._default_error_policy

            if on_error == 'continue-on-error':
                # 不传播失败，标记为完成但不取消下游任务
                logger.warning(
                    "Task '%s' failed but error_policy='continue-on-error', not propagating failure",
                    result.task_id
                )
                self._scheduler.mark_completed(result.task_id, TaskStatus.FAILED, self._outputs)
                return

            elif on_error == 'skip-downstream':
                # 跳过下游任务但标记为 FAILED
                logger.warning(
                    "Task '%s' failed with error_policy='skip-downstream', cancelling downstream tasks",
                    result.task_id
                )
                self._scheduler.mark_completed(result.task_id, TaskStatus.FAILED, self._outputs)
                self._failure_propagator.propagate(
                    result.task_id,
                    cancel_reason_template="Cancelled due to upstream failure of '{task_id}'",
                    log_description="failure",
                )
                return

            # 默认行为：fail-fast，传播失败
            self._scheduler.mark_completed(result.task_id, TaskStatus.FAILED, self._outputs)
            self._failure_propagator.propagate(
                result.task_id,
                cancel_reason_template="Cancelled due to upstream failure of '{task_id}'",
                log_description="failure",
            )

        # EXIT 日志：记录经过所有转换后的最终状态
        _final_in_outputs = result.task_id in self._outputs
        logger.info(
            "[_handle_result] EXIT task='%s', final_status=%s, in_outputs=%s",
            result.task_id, result.status.value, _final_in_outputs,
        )

        # 清理 stream 文件
        stream_file = self._spill_dir / f"{result.task_id}_stream.jsonl" if hasattr(self, '_spill_dir') and self._spill_dir else None
        if stream_file and stream_file.exists():
            try:
                stream_file.unlink()
            except OSError:
                pass

    def _health_check(self) -> None:
        """Check system health (disk space, memory, CPU) and cleanup spill files."""
        min_mb = self._config.limits.min_disk_space_mb
        if min_mb > 0:
            usage = shutil.disk_usage(".")
            free_mb = usage.free // (1024 * 1024)
            if free_mb < min_mb:
                free_gb = free_mb / 1024
                get_notifier().critical("磁盘空间不足", detail=f"可用空间 {free_gb:.1f}GB < 1GB")
                raise HealthCheckError(f"Low disk space: {free_mb}MB free (min {min_mb}MB)")

        # 系统内存检查
        if psutil:
            mem = psutil.virtual_memory()
            if mem.percent > self._config.health.memory_percent_max:
                get_notifier().critical("系统内存不足", detail=f"内存使用 {mem.percent}%")
                raise OrchestratorError(
                    f"系统内存不足: 使用率 {mem.percent}% > 阈值 {self._config.health.memory_percent_max}%"
                )

        # CPU 检查（仅告警，不阻断）
        if psutil:
            cpu = psutil.cpu_percent(interval=0)
            if cpu > self._config.health.cpu_percent_max:
                get_notifier().warning("CPU 使用率过高", detail=f"CPU {cpu}%")

        # 进程级内存检查（仅告警，不阻断）
        if psutil:
            rss = psutil.Process().memory_info().rss
            if rss > 2 * 1024**3:
                get_notifier().warning("进程内存过高", detail=f"RSS {rss / 1024**3:.1f}GB")

        # 清理过期溢出文件
        self._cleanup_spill_files()

        # 所有检查通过，发送恢复通知
        get_notifier().resolve("磁盘空间不足")
        get_notifier().resolve("系统内存不足")

    def _cleanup_spill_files(self) -> None:
        """清理过期的溢出文件。"""
        if not hasattr(self, '_spill_dir') or not self._spill_dir or not self._spill_dir.exists():
            return
        ttl_hours = self._config.health.spill_ttl_hours
        cutoff = time.time() - ttl_hours * 3600
        for f in self._spill_dir.iterdir():
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    logger.debug("清理过期溢出文件: %s", f.name)
            except OSError as exc:
                logger.warning("清理溢出文件失败: %s", exc)

    def _ensure_stuck_runs_cleaned(self) -> None:
        """启动新运行前，将所有 status='running' 但未正确结束的旧运行强制标记为 'failed'。

        防止进程崩溃或被 kill 后遗留的 running 记录永久卡住，
        导致统计中 147 次运行全部 status=running 的问题。
        """
        try:
            # 查找所有 status='running' 且 finished_at IS NULL 的运行
            cur = self._store._execute_write(
                "UPDATE runs SET status = ?, finished_at = ? "
                "WHERE status = ? AND finished_at IS NULL",
                (RunStatus.FAILED.value, datetime.now().isoformat(), RunStatus.RUNNING.value),
                operation="ensure_stuck_runs_cleaned",
            )
            affected = cur.rowcount if cur else 0
            if affected > 0:
                logger.warning(
                    "[_ensure_stuck_runs_cleaned] 强制标记 %d 个残留 running 运行为 FAILED",
                    affected,
                )
        except Exception as e:
            # 清理失败不应阻断新运行启动，仅记录警告
            logger.warning("[_ensure_stuck_runs_cleaned] 清理残留运行失败: %s", e)

    def _close_store(self) -> None:
        """关闭 Store 实例（如果由 Orchestrator 拥有且尚未关闭）。"""
        if self._owns_store and not self._store_closed:
            try:
                self._store.close()
                self._store_closed = True
            except Exception as e:
                logger.warning("Failed to close store: %s", e)

    def _finalize(self) -> None:
        """Finalize the run: set status, update store.

        整个方法体被 try/except 包裹，确保 finalize 永远不会向调用方抛出异常。
        finally 块中的 _finalize() 调用不会因此导致连锁崩溃。
        """
        try:
            logger.info(
                "[_finalize] ENTER: run_info=%s, dag=%s, store_closed=%s",
                self._run_info.run_id if self._run_info else "None",
                self._dag.name,
                self._store_closed,
            )
            if self._run_info:
                # 如果执行循环正常退出且状态仍为 RUNNING，根据任务结果判定最终状态
                if self._run_info.status == RunStatus.RUNNING:
                    task_states = {}
                    for tid in self._dag.tasks:
                        task_states[tid] = self._scheduler.get_task_status(tid)

                    # 交叉校验：用 self._results 修复调度器状态不一致
                    # 场景：任务成功写入 results 但 mark_completed 未被调用（异常导致）
                    # 如果 results 中有终态而调度器仍为非终态，以 results 为准
                    _terminal_statuses = {TaskStatus.SUCCESS, TaskStatus.FAILED,
                                          TaskStatus.SKIPPED, TaskStatus.CANCELLED}
                    reconciled = 0
                    for tid, scheduler_status in list(task_states.items()):
                        if scheduler_status not in _terminal_statuses:
                            result = self._results.get(tid)
                            if result and result.status in _terminal_statuses:
                                logger.warning(
                                    "[_finalize] 调解不一致: task '%s' 调度器=%s 但 results=%s，"
                                    "以 results 为准更新调度器",
                                    tid, scheduler_status.value, result.status.value,
                                )
                                task_states[tid] = result.status
                                # 同步更新调度器，防止后续查询仍返回过期状态
                                self._scheduler.mark_completed(
                                    tid, result.status, self._outputs
                                )
                                reconciled += 1
                    if reconciled > 0:
                        logger.info(
                            "[_finalize] 调解完成: %d 个任务从 results 恢复了终态", reconciled
                        )

                    # 按 terminal/non-terminal 分组计数，覆盖所有 TaskStatus 枚举值
                    terminal_success = {TaskStatus.SUCCESS}
                    terminal_failure = {TaskStatus.FAILED, TaskStatus.SKIPPED, TaskStatus.CANCELLED}
                    non_terminal = {TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.WAITING, TaskStatus.READY}

                    success_count = sum(1 for s in task_states.values() if s in terminal_success)
                    failed_count = sum(1 for s in task_states.values() if s == TaskStatus.FAILED)
                    skipped_count = sum(1 for s in task_states.values() if s in {TaskStatus.SKIPPED, TaskStatus.CANCELLED})
                    stuck_count = sum(1 for s in task_states.values() if s in non_terminal)
                    total_count = len(task_states)

                    # 非终态任务 = 执行异常，强制标记 FAILED
                    if stuck_count > 0:
                        stuck_ids = [tid for tid, s in task_states.items() if s in non_terminal]
                        logger.error(
                            "[_finalize] %d/%d tasks stuck in non-terminal states: %s. "
                            "This indicates a bug in the execution loop. Forcing FAILED.",
                            stuck_count, total_count, stuck_ids[:20],
                        )
                        self._run_info.status = RunStatus.FAILED
                    elif success_count == total_count:
                        self._run_info.status = RunStatus.COMPLETED
                    elif success_count > 0:
                        # 部分成功仍然标记 COMPLETED，但详细记录各类失败
                        logger.warning(
                            "[_finalize] Partial success: %d/%d succeeded, %d failed, %d skipped/cancelled",
                            success_count, total_count, failed_count, skipped_count,
                        )
                        self._run_info.status = RunStatus.COMPLETED
                    else:
                        logger.error(
                            "[_finalize] Run finished with 0 successes: %d failed, %d skipped/cancelled out of %d total. Marking FAILED.",
                            failed_count, skipped_count, total_count,
                        )
                        self._run_info.status = RunStatus.FAILED

                    logger.info(
                        "[_finalize] Run %s final status: %s "
                        "(success=%d, failed=%d, skipped=%d, stuck=%d, total=%d)",
                        self._run_info.run_id, self._run_info.status.value,
                        success_count, failed_count, skipped_count, stuck_count, total_count,
                    )

                # 从所有已完成任务的 TaskResult 中累加成本（三来源 + 交叉校验）
                # 来源1: 内存 results（可能被 LRU 淘汰）
                in_memory_cost = sum(
                    tr.cost_usd for tr in self._results.values() if tr.cost_usd
                )
                in_memory_count = len(self._results)

                # 来源2: Store 持久化数据（LRU 淘汰后补充）
                store_cost = 0.0
                store_count = 0
                try:
                    store_results = self._store.get_all_task_results(self._run_info.run_id)
                    store_cost = sum(
                        tr.cost_usd for tr in store_results.values() if tr.cost_usd
                    )
                    store_count = len(store_results)
                except Exception as e:
                    logger.warning("[_finalize] Store 聚合查询失败: %s", e)

                # 来源3: BudgetTracker（record_usage 实时累加）
                budget_tracker_cost = self._budget.spent

                # 取三个来源的最大值，确保不丢失成本数据
                aggregated_cost = max(in_memory_cost, store_cost, budget_tracker_cost)

                # 交叉校验：多个非零来源差异过大时记录警告
                if aggregated_cost > 0:
                    sources = {
                        "in_memory": in_memory_cost,
                        "store": store_cost,
                        "budget_tracker": budget_tracker_cost,
                    }
                    non_zero = {k: v for k, v in sources.items() if v > 0}
                    if len(non_zero) > 1:
                        vals = list(non_zero.values())
                        spread = max(vals) - min(vals)
                        if spread > 0.01:
                            logger.warning(
                                "[_finalize] 成本交叉校验差异: %s (取最大值 $%.4f)",
                                ", ".join(f"{k}=${v:.4f}" for k, v in non_zero.items()),
                                aggregated_cost,
                            )

                    self._run_info.total_cost_usd = aggregated_cost
                    logger.info(
                        "[_finalize] 聚合成本: $%.4f (内存=%d条/$%.4f, Store=%d条/$%.4f, BudgetTracker=$%.4f)",
                        aggregated_cost, in_memory_count, in_memory_cost,
                        store_count, store_cost, budget_tracker_cost,
                    )
                else:
                    logger.warning(
                        "[_finalize] 所有来源成本均为 0.0 (内存 %d 条, Store %d 条, BudgetTracker $%.4f)，"
                        "CLI 可能未报告成本字段，需检查 _extract_cost_usd 和 token 解析",
                        in_memory_count, store_count, budget_tracker_cost,
                    )
                    self._run_info.total_cost_usd = 0.0
                try:
                    task_states = {tid: self._scheduler.get_task_status(tid).value
                                   for tid in self._dag.tasks}
                    self._checkpoint_manager.save_checkpoint(
                        self._run_info.run_id,
                        phase="finalization",
                        task_states=task_states,
                        metadata={
                            "status": self._run_info.status.value,
                            "completed_tasks": sum(1 for s in task_states.values() if s == TaskStatus.SUCCESS.value),
                            "failed_tasks": sum(1 for s in task_states.values() if s == TaskStatus.FAILED.value)
                        }
                    )
                except Exception as e:
                    logger.warning("Failed to save final checkpoint: %s", e)
                try:
                    logger.info(
                        "[_finalize] 准备更新 run_status: run_id=%s, status=%s, cost=%.4f, store_type=%s",
                        self._run_info.run_id, self._run_info.status.value,
                        self._run_info.total_cost_usd, type(self._store).__name__,
                    )
                    self._store.update_run_status(
                        self._run_info.run_id,
                        self._run_info.status,
                        cost=self._run_info.total_cost_usd,
                    )
                    logger.info("[_finalize] update_run_status 完成: run_id=%s", self._run_info.run_id)
                    if self._pool_runtime is not None:
                        self._store.update_run_pool_info(
                            self._run_info.run_id,
                            pool_id=self._pool_runtime.pool_id,
                            active_profile=self._pool_runtime.active_profile,
                        )
                except Exception as e:
                    logger.warning("Failed to persist final run status: %s", e)

                # 关闭诊断日志
                try:
                    self._diagnostics.close()
                except Exception as e:
                    logger.warning("Failed to close diagnostics: %s", e)
        except Exception as e:
            # _finalize 在 finally 块中被调用，绝不能向调用方抛出异常
            logger.exception("[_finalize] UNEXPECTED ERROR during finalization: %s", e)

    def _run_validation_gate(self, task_node: TaskNode, result: TaskResult) -> bool:
        """执行验证门禁，返回是否通过验证。"""
        assert task_node.validation_gate is not None

        validator_prompt = task_node.validation_gate.get("validator_prompt", "")
        pass_threshold = task_node.validation_gate.get("pass_threshold", 0.7)

        if not validator_prompt:
            logger.warning("Task '%s' has validation_gate but no validator_prompt", task_node.id)
            return True

        # 构造对抗性验证 prompt：要求模型从"挑剔的审查者"角度寻找问题
        adversarial_prefix = """你是一个极其严格的质量审查专家。你的任务是尽可能找出以下任务输出中的缺陷、遗漏、不一致或错误。

请从以下角度逐一审查：
1. **完整性**：输出是否遗漏了关键信息或步骤？
2. **一致性**：输出内部的各部分是否自洽？有无矛盾？
3. **准确性**：数值、引用、事实是否准确？有无编造内容？
4. **格式**：是否严格遵循了要求的输出格式？
5. **安全性**：是否包含敏感信息或危险操作？

对每个维度给出 0-1 分的评分，然后计算加权平均分作为最终分数。

原始验证标准：
"""
        task_output = result.output or ""
        validation_prompt = (
            f"{adversarial_prefix}{validator_prompt}\n\n任务输出：\n{task_output}\n\n"
            "请以 JSON 格式输出评估结果：\n"
            "```json\n"
            '{"score": <0-1>, "dimensions": {"completeness": <0-1>, '
            '"consistency": <0-1>, "accuracy": <0-1>, "format": <0-1>, '
            '"safety": <0-1>}, "issues": ["<发现的问题1>", ...], '
            '"summary": "<一句话总结>"}\n'
            "```"
        )

        logger.info("Running validation gate for task '%s' (threshold=%.2f)", task_node.id, pass_threshold)

        # 创建临时验证任务节点（使用默认模型）
        validator_task = replace(
            task_node,
            id=f"{task_node.id}_validator",
            prompt_template=validation_prompt,
            model=self._config.claude.default_model,
            timeout=300,
            output_format="json",
            retry_policy=RetryPolicy(max_attempts=min(2, self._config.retry.max_attempts)),
            validation_gate=None,  # 防止递归触发验证门禁
        )

        # 执行验证
        try:
            # 通过插件系统执行验证任务
            executor = PluginRegistry.get_executor(validator_task.type)
            validation_result = executor.execute(
                task=validator_task,
                prompt=validation_prompt,
                claude_config=self._config.claude,
                limits=self._config.limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
                on_progress=None,
                audit_logger=self._audit_logger,
                rate_limiter=self._rate_limiter,
            )

            if validation_result.status != TaskStatus.SUCCESS:
                logger.error(
                    "Validation gate execution failed for task '%s': %s",
                    task_node.id, validation_result.error
                )
                result.error = f"Validation gate execution failed: {validation_result.error}"
                return False

            # 解析验证分数
            score = self._parse_validation_score(validation_result.parsed_output)

            if score is None:
                logger.error(
                    "Failed to parse validation score for task '%s', output: %s",
                    task_node.id, validation_result.output
                )
                result.error = "Validation gate failed: could not parse validation score"
                return False

            passed = score >= pass_threshold
            logger.info(
                "Validation gate for task '%s': score=%.2f, threshold=%.2f, passed=%s",
                task_node.id, score, pass_threshold, passed
            )

            if not passed:
                result.error = f"Validation gate failed: score {score:.2f} < threshold {pass_threshold:.2f}"

            return passed

        except Exception as e:
            logger.exception("Unexpected error during validation gate for task '%s': %s", task_node.id, e)
            result.error = f"Validation gate error: {e}"
            return False

    def _parse_validation_score(self, parsed_output: Any) -> float | None:
        """从验证输出中解析分数（支持多种格式，向后兼容）。"""
        if parsed_output is None:
            return None

        # 如果是字典，尝试提取 score 字段
        if isinstance(parsed_output, dict):
            if "score" in parsed_output:
                try:
                    return float(parsed_output["score"])
                except (ValueError, TypeError):
                    return None
            # 尝试从 dimensions 中计算平均分（对抗性验证格式）
            if "dimensions" in parsed_output:
                dims = parsed_output["dimensions"]
                if isinstance(dims, dict) and dims:
                    values = [v for v in dims.values() if isinstance(v, (int, float))]
                    if values:
                        return sum(values) / len(values)

        # 如果是数字，直接返回
        if isinstance(parsed_output, (int, float)):
            return float(parsed_output)

        # 如果是字符串，尝试解析为数字或 JSON
        if isinstance(parsed_output, str):
            try:
                return float(parsed_output)
            except ValueError:
                pass
            # 尝试解析为 JSON，递归处理
            try:
                import json
                parsed = json.loads(parsed_output)
                return self._parse_validation_score(parsed)
            except (json.JSONDecodeError, TypeError):
                pass

        return None

    def _run_inter_task_validation(self) -> None:
        """执行任务间交叉验证，检查有依赖关系的任务组是否存在矛盾或冲突。"""
        if not self._results:
            return

        # 收集所有成功完成的任务
        successful_tasks = {
            tid: result for tid, result in self._results.items()
            if result.status == TaskStatus.SUCCESS
        }

        if len(successful_tasks) < 2:
            # 少于2个成功任务，无需交叉验证
            return

        logger.info("Running inter-task validation for %d successful tasks", len(successful_tasks))

        # 对每个成功任务，检查其与依赖任务之间的一致性
        validation_issues = []
        for task_id, result in successful_tasks.items():
            task_node = self._dag.tasks.get(task_id)
            if not task_node or not task_node.depends_on:
                continue

            # 收集该任务的上游依赖任务结果
            related_results = {}
            for dep_id in task_node.depends_on:
                if dep_id in successful_tasks:
                    related_results[dep_id] = successful_tasks[dep_id]

            if not related_results:
                continue

            # 执行交叉验证
            inter_result = self._inter_validator.validate(task_id, result, related_results)
            if not inter_result.passed:
                validation_issues.append({
                    'task_id': task_id,
                    'issues': inter_result.issues
                })
                logger.warning(
                    "Task '%s' failed inter-task validation: %s",
                    task_id, ", ".join(inter_result.issues)
                )

        # 汇总验证结果
        if validation_issues:
            logger.warning(
                "Inter-task validation found %d task(s) with issues:",
                len(validation_issues)
            )
            for item in validation_issues:
                logger.warning("  - Task '%s': %s", item['task_id'], "; ".join(item['issues']))
        else:
            logger.info("Inter-task validation passed for all task groups")
