"""Data models for the autonomous goal-driven orchestrator.

支持 GoalState 序列化到 JSON 文件，实现中断后恢复。
"""

from __future__ import annotations

import glob
import json
import logging
import os
import shutil
import sys
import time
import uuid
import copy as _copy_module
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Generator, TypedDict


class ExecutionMetrics(TypedDict, total=False):
    """阶段执行指标的强类型约束。

    替代 dict[str, Any] 的弱类型，确保键名在运行时也可校验。
    """
    started_at: str           # ISO8601 时间戳
    finished_at: str          # ISO8601 时间戳
    duration_seconds: float   # 阶段总耗时（秒）
    cli_calls: int            # 实际 CLI 调用次数
    model_used: str           # 执行模型名称
    total_cost_usd: float     # 阶段累计花费
    total_tokens: int         # 阶段累计 token 用量（input + output）

from .config import DEFAULT_CLAUDE_MODEL
from .model import FailureInfo

logger = logging.getLogger(__name__)

# ── 跨平台文件锁 ──

_IS_WINDOWS = sys.platform == "win32"

if _IS_WINDOWS:
    import msvcrt
else:
    import fcntl


@contextmanager
def _file_lock(
    lock_path: Path,
    *,
    exclusive: bool = True,
    timeout: float = 10.0,
) -> Generator[None, None, None]:
    """跨平台文件锁（Windows 用 msvcrt，Unix 用 fcntl）。

    Args:
        lock_path: 锁文件路径（不存在会自动创建）
        exclusive: True=排他锁（写），False=共享锁（读）
        timeout: 获取锁的超时秒数，超时抛出 TimeoutError
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    # 用 "a+b" 模式打开：文件不存在时自动创建，存在时不截断
    # 避免 exists() + touch() 的 TOCTOU 竞态
    fd = open(lock_path, "a+b")  # noqa: SIM115
    try:
        _acquire_lock(fd, exclusive=exclusive, timeout=timeout)
        try:
            yield
        finally:
            _release_lock(fd)
    finally:
        fd.close()


def _acquire_lock(
    fd,
    *,
    exclusive: bool,
    timeout: float,
) -> None:
    """带超时的锁获取，轮询间隔从 10ms 指数退避到 200ms。"""
    deadline = time.monotonic() + timeout
    interval = 0.01  # 初始 10ms
    max_interval = 0.2

    while True:
        try:
            if _IS_WINDOWS:
                # msvcrt.locking 只支持排他锁，对共享读场景也用排他锁
                # （Windows 上 JSON 文件读写频率低，排他锁开销可接受）
                msvcrt.locking(fd.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                flag = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
                fcntl.flock(fd.fileno(), flag | fcntl.LOCK_NB)
            return  # 获取成功
        except (OSError, IOError):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"获取文件锁超时 ({timeout}s): {fd.name}"
                )
            time.sleep(interval)
            interval = min(interval * 2, max_interval)


def _release_lock(fd) -> None:
    """释放文件锁。"""
    try:
        if _IS_WINDOWS:
            msvcrt.locking(fd.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(fd.fileno(), fcntl.LOCK_UN)
    except (OSError, IOError):
        pass  # 释放失败不阻塞主流程


class GoalStatus(Enum):
    INITIALIZING = "initializing"
    GATHERING = "gathering"        # 需求收集阶段
    DECOMPOSING = "decomposing"
    EXECUTING = "executing"
    REVIEWING = "reviewing"
    ITERATING = "iterating"
    CONVERGED = "converged"
    PARTIAL_SUCCESS = "partial_success"
    SAFE_STOP = "safe_stop"
    CATASTROPHIC_STOP = "catastrophic_stop"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class SafeStopReason(Enum):
    """安全停止原因枚举。"""
    BUDGET_EXHAUSTED = "budget_exhausted"
    TIMEOUT = "timeout"
    LOW_SCORE = "low_score"
    MANUAL = "manual"
    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> SafeStopReason | None:
        """向后兼容：JSON 中存储的旧字符串值自动映射到枚举。"""
        if isinstance(value, str):
            for member in cls:
                if member.value == value:
                    return member
        return None


class PhaseStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    NEEDS_REVISION = "needs_revision"
    SKIPPED = "skipped"


class ReviewVerdict(Enum):
    PASS = "pass"
    MINOR_ISSUES = "minor_issues"
    MAJOR_ISSUES = "major_issues"
    CRITICAL = "critical"
    BLOCKED = "blocked"


class FailureCategory(Enum):
    """失败分类：决定重试策略。"""
    TRANSIENT = "transient"          # 临时性失败（超时、网络、CLI 崩溃）→ 原样重试
    LOGIC_ERROR = "logic_error"      # 逻辑/代码错误 → 注入反馈后重试
    QUALITY_GATE = "quality_gate"    # 门禁失败（测试/lint）→ 注入门禁输出后重试
    BUDGET = "budget"                # 历史兼容保留：内部预算控制已停用
    BLOCKED = "blocked"              # 需要人工介入 → 不重试
    INIT_ERROR = "init_error"        # 启动校验失败 → 不重试
    GOAL_PARSE_ERROR = "goal_parse_error"  # 目标解析失败 → 不重试
    ENV_MISSING = "env_missing"      # 环境缺失 → 不重试
    AUTH_EXPIRED = "auth_expired"    # 认证过期 → 尝试恢复后重试
    TIMEOUT = "timeout"              # 整体/阶段超时 → 可重试


@dataclass
class FailureClassification:
    """对一次阶段迭代失败的分类结果。"""
    category: FailureCategory
    retriable: bool
    feedback: str = ""               # 注入下一次迭代的反馈信息
    adjust_timeout: float = 1.0      # 超时倍率（transient 时可放大）


@dataclass
class DiagnosticEntry:
    """诊断记录条目,用于追踪各阶段的执行状态和异常。"""
    stage: str
    entered_at: datetime
    exit_status: str = "ok"
    error_detail: str = ""
    duration_seconds: float = 0.0
    stack_trace: str = ""


@dataclass
class ComplexityEstimate:
    """任务复杂度评估结果。"""
    estimated_subtasks: int
    estimated_hours: float
    tech_stacks: list[str]
    complexity_level: str = "medium"  # low | medium | high | extreme
    should_split: bool = False
    split_suggestion: str = ""

@dataclass
class ReviewIssue:
    severity: str  # "minor" | "major" | "critical"
    category: str  # e.g. "correctness", "security", "performance"
    description: str
    affected_files: list[str] = field(default_factory=list)
    suggested_fix: str = ""


@dataclass
class CorrectiveAction:
    action_id: str
    description: str
    prompt_template: str
    priority: int = 1  # 1=highest
    depends_on_actions: list[str] = field(default_factory=list)
    timeout: int = 1800
    action_type: str = "claude_cli"
    executor_config: dict[str, Any] | None = None


@dataclass
class ReviewResult:
    phase_id: str
    verdict: ReviewVerdict
    score: float  # 0.0 - 1.0
    summary: str
    issues: list[ReviewIssue] = field(default_factory=list)
    corrective_actions: list[CorrectiveAction] = field(default_factory=list)
    reviewed_at: datetime = field(default_factory=datetime.now)
    score_rationale: str = ""  # 评分区间理由，与锚定标准对齐


@dataclass
class Phase:
    id: str
    name: str
    description: str
    order: int
    objectives: list[str] = field(default_factory=list)
    acceptance_criteria: list[str] = field(default_factory=list)
    depends_on_phases: list[str] = field(default_factory=list)
    status: PhaseStatus = PhaseStatus.PENDING
    iteration: int = 0
    max_iterations: int = 3
    review_result: ReviewResult | None = None
    task_outputs: dict[str, Any] = field(default_factory=dict)
    raw_tasks: list[dict] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    last_classification: FailureClassification | None = None
    timeout_multiplier: float = 1.0  # transient 重试时累积放大超时
    strategy_hint: str = ""  # 恶化检测注入的策略提示（switch_strategy / rollback）
    best_score: float = 0.0  # 历史最高分，用于 rollback 判断
    best_outputs: dict[str, Any] = field(default_factory=dict)  # 最高分时的输出快照
    task_result_statuses: dict[str, str] = field(default_factory=dict)  # task_id -> TaskStatus.value，用于统计 success/failed
    execution_metrics: dict[str, Any] = field(default_factory=dict)  # started_at/finished_at/duration_seconds/cli_calls/model_used/total_cost_usd/total_tokens
    started_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass
class IterationRecord:
    iteration: int
    phase_id: str
    score: float
    verdict: ReviewVerdict
    actions_taken: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    # Handoff Protocol 新增字段（向后兼容）
    failure_category: str = ""
    gate_passed: bool | None = None
    regression_detected: bool = False
    task_error_count: int = 0
    deterioration_level: str = ""
    duration_seconds: float = 0.0


@dataclass
class TaskError:
    """任务级错误记录，用于 Handoff 传递。"""
    task_id: str
    error: str
    attempt: int = 1


@dataclass
class IterationHandoff:
    """迭代间结构化上下文传递包。"""
    iteration: int = 0
    review_summary: str = ""
    review_issues: list[ReviewIssue] = field(default_factory=list)
    review_score: float = 0.0
    corrective_actions: list[CorrectiveAction] = field(default_factory=list)
    failure_category: str = ""
    failure_feedback: str = ""
    task_errors: list[TaskError] = field(default_factory=list)
    gate_summary: str = ""
    gate_failed_commands: list[str] = field(default_factory=list)
    regression_detected: bool = False
    regressed_commands: list[str] = field(default_factory=list)
    score_trend: list[float] = field(default_factory=list)
    trend_direction: str = ""  # "improving" | "stable" | "declining"
    architecture_execution_summary: str = ""
    architecture_gate_status: str = ""
    architecture_unmet_cutover_gates: list[str] = field(default_factory=list)
    architecture_missing_evidence_refs: list[str] = field(default_factory=list)
    architecture_missing_rollback_refs: list[str] = field(default_factory=list)
    architecture_report_path: str = ""

    def to_prompt_text(self, max_chars: int = 4000) -> str:
        """序列化为可注入 prompt 的结构化文本。"""
        sections: list[str] = []

        # 1. 迭代趋势
        if self.score_trend:
            trend_str = " → ".join(f"{s:.2f}" for s in self.score_trend)
            sections.append(
                f"## 迭代趋势\n"
                f"分数变化: {trend_str} ({self.trend_direction})\n"
                f"当前迭代: 第 {self.iteration} 次"
            )

        # 2. 审查反馈（含 issues）
        if self.review_summary:
            review_section = f"## 审查反馈 (score={self.review_score:.2f})\n{self.review_summary}"
            if self.review_issues:
                issue_lines = []
                for issue in self.review_issues[:10]:
                    line = f"- [{issue.severity}][{issue.category}] {issue.description}"
                    if issue.affected_files:
                        line += f" (文件: {', '.join(issue.affected_files[:3])})"
                    if issue.suggested_fix:
                        line += f"\n  建议修复: {issue.suggested_fix}"
                    issue_lines.append(line)
                review_section += "\n\n具体问题:\n" + "\n".join(issue_lines)
            sections.append(review_section)

        # 3. 失败分类反馈
        if self.failure_feedback:
            sections.append(
                f"## 失败分类: {self.failure_category}\n{self.failure_feedback}"
            )

        # 4. 任务级错误
        if self.task_errors:
            error_lines = [
                f"- 任务 `{e.task_id}` (attempt {e.attempt}): {e.error[:200]}"
                for e in self.task_errors[:5]
            ]
            sections.append(
                f"## 任务执行错误 ({len(self.task_errors)} 个)\n" + "\n".join(error_lines)
            )

        # 5. 门禁结果
        if self.gate_summary:
            gate_section = f"## 质量门禁\n{self.gate_summary}"
            if self.gate_failed_commands:
                gate_section += "\n失败命令:\n" + "\n".join(
                    f"- `{cmd}`" for cmd in self.gate_failed_commands
                )
            sections.append(gate_section)

        # 6. 回归信息
        if self.regression_detected:
            sections.append(
                "## ⚠ 回归检测\n"
                "以下之前通过的命令现在失败了:\n"
                + "\n".join(f"- `{cmd}`" for cmd in self.regressed_commands)
            )

        text = "\n\n".join(sections)
        # 截断保护
        if len(text) > max_chars:
            text = text[:max_chars - 20] + "\n\n...(已截断)"
        return text


class DeteriorationLevel(Enum):
    """恶化程度等级。"""
    NONE = "none"
    WARNING = "warning"
    SERIOUS = "serious"
    CRITICAL = "critical"


# ── 精确迭代模式（Surgical Mode）数据模型 ──


@dataclass
class NotebookEntry:
    """单次精确迭代的记录条目。

    每条记录包含修复前后的完整验证输出，确保下一轮迭代能精确知道发生了什么。
    """
    iteration: int
    target_file: str
    target_issue: str              # 结构化问题描述
    error_before: str              # 修复前的完整验证输出
    fix_attempted: str             # 修复摘要（从任务输出提取）
    verification_output: str       # 修复后的完整验证输出
    verification_passed: bool
    learned: str = ""              # 一句话总结
    cost_usd: float = 0.0
    duration_seconds: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ContextNotebook:
    """精确迭代模式的上下文笔记本。

    跨迭代积累知识，每轮 prompt 可注入相关历史，避免子 agent 从零开始。
    """
    entries: list[NotebookEntry] = field(default_factory=list)
    goal: str = ""
    verification_commands: list[str] = field(default_factory=list)

    def relevant_entries(self, target_file: str = "", limit: int = 5) -> list[NotebookEntry]:
        """返回与目标文件相关的最近记录，最近的在最后。"""
        if not target_file:
            return self.entries[-limit:]
        matching = [e for e in self.entries if target_file in e.target_file]
        return matching[-limit:] if matching else self.entries[-limit:]

    def to_prompt_text(self, max_chars: int = 8000) -> str:
        """序列化为可注入 prompt 的结构化文本。"""
        if not self.entries:
            return ""
        sections: list[str] = ["## 迭代历史记录"]
        # 从最近的条目开始，控制总长度
        budget = max_chars - 100  # 预留给头部和格式
        for entry in reversed(self.entries):
            status = "✅ 通过" if entry.verification_passed else "❌ 失败"
            block = (
                f"### 第 {entry.iteration} 轮: {entry.target_file} — {status}\n"
                f"- 问题: {entry.target_issue[:200]}\n"
                f"- 修复: {entry.fix_attempted[:200]}\n"
                f"- 验证输出: {entry.verification_output[:500]}\n"
                f"- 经验: {entry.learned[:200]}\n"
            )
            if len(block) > budget:
                break
            sections.append(block)
            budget -= len(block)
        return "\n".join(sections)

    def failed_files(self) -> list[str]:
        """返回验证失败的文件列表（按出现频率排序）。"""
        from collections import Counter
        fails = [e.target_file for e in self.entries if not e.verification_passed]
        return [f for f, _ in Counter(fails).most_common()]


class DeteriorationLevel(Enum):
    """恶化程度等级。"""
    NONE = "none"
    WARNING = "warning"
    SERIOUS = "serious"
    CRITICAL = "critical"


@dataclass
class DeteriorationSignal:
    """恶化检测结果。"""
    level: DeteriorationLevel
    reason: str
    details: dict[str, Any] = field(default_factory=dict)
    recommended_action: str = ""  # "continue" | "switch_strategy" | "rollback" | "escalate"
    correlated_dimensions: list[str] = field(default_factory=list)  # 关联的恶化维度列表


@dataclass
class ConvergenceSignal:
    should_stop: bool
    reason: str
    details: dict[str, Any] = field(default_factory=dict)


# ── 需求收集数据模型 ──


@dataclass
class RequirementQuestion:
    """单个结构化问题。"""
    question_id: str                    # "q_01"
    category: str                       # "scope" | "tech" | "acceptance" | "constraint" | "priority"
    question_text: str
    question_type: str                  # "single_choice" | "multi_choice" | "text" | "yes_no"
    options: list[str] = field(default_factory=list)
    default: str = ""
    context_hint: str = ""             # 基于项目分析的提问依据
    answer: str = ""
    answered: bool = False


@dataclass
class GatheringRound:
    """一轮收集记录。"""
    round_number: int
    questions: list[RequirementQuestion] = field(default_factory=list)
    raw_answers: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class RequirementSpec:
    """结构化需求规格，作为 GoalDecomposer 的增强输入。"""
    original_goal: str
    sufficiency_score: float = 0.0
    sufficiency_verdict: str = ""       # "sufficient" | "needs_gathering" | "ambiguous"
    scope: str = ""
    acceptance_criteria: list[str] = field(default_factory=list)
    technical_constraints: list[str] = field(default_factory=list)
    non_functional_requirements: list[str] = field(default_factory=list)
    priority_order: list[str] = field(default_factory=list)
    excluded_scope: list[str] = field(default_factory=list)
    rounds: list[GatheringRound] = field(default_factory=list)
    total_questions_asked: int = 0
    total_questions_answered: int = 0

    def to_enhanced_goal(self) -> str:
        """拼接为增强版 goal 文本，供 GoalDecomposer 使用。"""
        sections = [f"# 目标\n{self.original_goal}"]
        if self.scope:
            sections.append(f"\n## 范围\n{self.scope}")
        if self.acceptance_criteria:
            sections.append("\n## 验收标准\n" + "\n".join(f"- {c}" for c in self.acceptance_criteria))
        if self.technical_constraints:
            sections.append("\n## 技术约束\n" + "\n".join(f"- {c}" for c in self.technical_constraints))
        if self.non_functional_requirements:
            sections.append("\n## 非功能需求\n" + "\n".join(f"- {r}" for r in self.non_functional_requirements))
        if self.priority_order:
            sections.append("\n## 优先级\n" + "\n".join(f"{i+1}. {p}" for i, p in enumerate(self.priority_order)))
        if self.excluded_scope:
            sections.append("\n## 明确排除\n" + "\n".join(f"- {e}" for e in self.excluded_scope))
        return "\n".join(sections)


@dataclass
class GoalState:
    goal_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    goal_text: str = ""
    status: GoalStatus = GoalStatus.INITIALIZING
    phases: list[Phase] = field(default_factory=list)
    current_phase_index: int = 0
    iteration_history: list[IterationRecord] = field(default_factory=list)
    total_iterations: int = 0  # 全局迭代计数：所有阶段的执行次数总和（含首次和重试）
    deadline: datetime = field(default_factory=lambda: datetime.now() + timedelta(hours=24))
    total_cost_usd: float = 0.0
    project_context: str = ""
    started_at: datetime = field(default_factory=datetime.now)
    diagnostics: list[DiagnosticEntry] = field(default_factory=list)
    failure_categories: dict[str, int] = field(default_factory=dict)
    requirement_spec: RequirementSpec | None = None  # 需求收集结果
    stop_reason: str = ""
    active_profile: str = ""
    architecture_triggered: bool = False
    architecture_contract_path: str = ""
    architecture_summary: str = ""
    architecture_decision_type: str = ""
    pool_state: dict[str, Any] = field(default_factory=dict)
    runtime_dir: str = ""
    workspace_dir: str = ""
    handoff_dir: str = ""
    backup_summary: str = ""
    branch_names: list[str] = field(default_factory=list)
    failure_info: FailureInfo | None = None  # 结构化失败详情（含异常类型、消息、堆栈）
    task_stats: dict = field(default_factory=dict)  # 运行级别任务统计
    deterioration_levels: dict[str, int] = field(default_factory=dict)  # 运行级别恶化统计
    phase_history: list[dict] = field(default_factory=list)  # 阶段切换历史记录
    safe_stop_reason: SafeStopReason = SafeStopReason.UNKNOWN  # 安全停止原因（如预算耗尽、用户中断等）
    task_stats_avg_duration: float = 0.0  # 任务平均执行时长（秒）
    task_stats_max_duration: float = 0.0  # 任务最大执行时长（秒）
    schema_version: str = "1.0"  # goal_state 数据结构版本，resume 时校验兼容性
    notebook: ContextNotebook | None = None  # 精确迭代模式的上下文笔记本

    def create_snapshot(self) -> dict:
        """创建关键字段的深拷贝快照，用于恶化时回滚。

        只快照可变/关键状态字段，不可变字段（goal_id、goal_text 等）不包含。
        phases 使用已有的 _phase_to_dict 序列化以确保完整深拷贝。
        """
        return _copy_module.deepcopy({
            "phases": [_phase_to_dict(p) for p in self.phases],
            "failure_categories": dict(self.failure_categories),
            "total_cost_usd": self.total_cost_usd,
            "iteration_history": [_iteration_record_to_dict(r) for r in self.iteration_history],
            "total_iterations": self.total_iterations,
            "current_phase_index": self.current_phase_index,
            "diagnostics": [_diagnostic_entry_to_dict(d) for d in self.diagnostics],
            "deterioration_levels": dict(self.deterioration_levels),
            "task_stats": dict(self.task_stats),
            "status": self.status.value,
            "stop_reason": self.stop_reason,
        })

    def restore_snapshot(self, data: dict) -> None:
        """从 create_snapshot() 产出的 dict 恢复关键字段。

        用于 CRITICAL 恶化时回滚到最近正常快照。
        """
        if "phases" in data:
            self.phases = [_phase_from_dict(pd) for pd in data["phases"]]
        if "failure_categories" in data:
            self.failure_categories = _copy_module.deepcopy(data["failure_categories"])
        if "total_cost_usd" in data:
            self.total_cost_usd = data["total_cost_usd"]
        if "iteration_history" in data:
            self.iteration_history = [_iteration_record_from_dict(r) for r in data["iteration_history"]]
        if "total_iterations" in data:
            self.total_iterations = data["total_iterations"]
        if "current_phase_index" in data:
            self.current_phase_index = data["current_phase_index"]
        if "diagnostics" in data:
            self.diagnostics = [_diagnostic_entry_from_dict(d) for d in data["diagnostics"]]
        if "deterioration_levels" in data:
            self.deterioration_levels = _copy_module.deepcopy(data["deterioration_levels"])
        if "task_stats" in data:
            self.task_stats = _copy_module.deepcopy(data["task_stats"])
        if "status" in data:
            self.status = GoalStatus(data["status"])
        if "stop_reason" in data:
            self.stop_reason = data["stop_reason"]


@dataclass
class GoalResult:
    """目标执行的最终结果摘要，支持从 GoalState 转换。

    diagnostics 使用 list[dict] 格式，每条包含 stage/message/stack_trace/timestamp，
    便于直接 JSON 序列化和跨进程传递。
    """
    goal_id: str = ""
    goal_text: str = ""
    status: str = ""  # GoalStatus.value
    total_iterations: int = 0  # 全局迭代计数：所有阶段的执行次数总和（含首次和重试）
    total_cost_usd: float = 0.0
    phases_total: int = 0
    phases_completed: int = 0
    failure_categories: dict[str, int] = field(default_factory=dict)
    stop_reason: str = ""
    # 每条 dict 包含: stage(str), message(str), stack_trace(str), timestamp(str/ISO8601)
    diagnostics: list[dict] = field(default_factory=list)
    started_at: str = ""
    finished_at: str = ""
    task_stats: dict = field(default_factory=dict)
    deterioration_levels: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_goal_state(cls, state: GoalState) -> GoalResult:
        """从 GoalState 创建 GoalResult，将 DiagnosticEntry 转为标准 dict 格式。"""
        diagnostics = []
        for d in state.diagnostics:
            diagnostics.append({
                "stage": d.stage,
                "message": d.error_detail,
                "stack_trace": d.stack_trace,
                "timestamp": _dt_to_str(d.entered_at) if d.entered_at else "",
            })
        phases_completed = sum(
            1 for p in state.phases if p.status == PhaseStatus.COMPLETED
        )
        return cls(
            goal_id=state.goal_id,
            goal_text=state.goal_text,
            status=state.status.value,
            total_iterations=state.total_iterations,
            total_cost_usd=state.total_cost_usd,
            phases_total=len(state.phases),
            phases_completed=phases_completed,
            failure_categories=dict(state.failure_categories),
            stop_reason=state.stop_reason,
            diagnostics=diagnostics,
            started_at=_dt_to_str(state.started_at),
            finished_at=_dt_to_str(datetime.now()),
            task_stats=dict(state.task_stats),
            deterioration_levels=dict(state.deterioration_levels),
        )

    def to_dict(self) -> dict:
        """转换为可 JSON 序列化的 dict。"""
        return {
            "goal_id": self.goal_id,
            "goal_text": self.goal_text,
            "status": self.status,
            "total_iterations": self.total_iterations,
            "total_cost_usd": self.total_cost_usd,
            "phases_total": self.phases_total,
            "phases_completed": self.phases_completed,
            "failure_categories": self.failure_categories,
            "stop_reason": self.stop_reason,
            "diagnostics": self.diagnostics,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "task_stats": self.task_stats,
        }


@dataclass
class QualityGate:
    """可配置的硬门禁：在 AI 审查之前执行外部命令，用退出码判定通过/失败。"""
    # 要执行的命令列表，每个命令是一个 shell 字符串
    # 例如: ["pytest -x", "ruff check .", "npm run build"]
    commands: list[str] = field(default_factory=list)
    # 单个命令的超时秒数
    timeout: int = 300
    # 是否在门禁失败时跳过 AI 审查（直接标记为 MAJOR_ISSUES）
    skip_review_on_failure: bool = False
    # 是否启用门禁（方便临时关闭）
    enabled: bool = True


@dataclass
class QualityGateResult:
    """门禁执行结果。"""
    passed: bool
    command_results: list[dict] = field(default_factory=list)
    # 格式: [{"command": "pytest", "exit_code": 0, "stdout": "...", "stderr": "...", "passed": True}]
    summary: str = ""


@dataclass
class RegressionConstraint:
    """回归约束：确保迭代修复不会破坏已通过的检查。

    在阶段首次通过门禁后保存基线快照（通过的命令列表），
    后续迭代中如果基线命令失败，则判定为回归。
    """
    enabled: bool = True
    # 回归时是否阻止标记为通过
    block_on_regression: bool = True


@dataclass
class RegressionBaseline:
    """某阶段的回归基线快照。"""
    phase_id: str = ""
    passed_commands: list[str] = field(default_factory=list)
    score: float = 0.0
    captured_at: datetime = field(default_factory=datetime.now)


@dataclass
class AutoConfig:
    max_hours: float = 24.0
    max_total_iterations: int = 50  # 全局迭代上限（含首次执行和重试），由 ConvergenceDetector 检查
    max_phase_iterations: int = 50
    phase_parallelism: int = 8
    convergence_threshold: float = 0.72
    convergence_window: int = 3
    min_convergence_checks: int = 3  # 收敛判定所需的最少连续高分次数
    score_improvement_min: float = 0.05
    decomposition_model: str = DEFAULT_CLAUDE_MODEL
    review_model: str = DEFAULT_CLAUDE_MODEL
    execution_model: str = DEFAULT_CLAUDE_MODEL
    quality_gate: QualityGate = field(default_factory=QualityGate)
    regression: RegressionConstraint = field(default_factory=RegressionConstraint)
    # Handoff Protocol 配置
    handoff_enabled: bool = True
    handoff_max_chars: int = 4000
    adaptive_tuning_enabled: bool = True
    # 恶化检测配置
    deterioration_enabled: bool = True
    deterioration_window: int = 3
    deterioration_drop_threshold: float = 0.1
    deterioration_error_growth: int = 2
    # 执行进程控制
    manual_overrides: set[str] = field(default_factory=set)
    max_execution_processes: int = 0
    execution_lease_db_path: str = ""
    execution_lease_ttl_seconds: int = 300
    # 降级级别（每次触发恶化降级 +1，用于追踪降级深度）
    degradation_level: int = 0
    # 低分提前终止配置
    low_score_threshold: float = 0.3
    low_score_max_consecutive: int = 3

    def adapt_to_complexity(self, complexity: ComplexityEstimate) -> None:
        """根据复杂度评估自适应调整收敛和恶化参数。

        策略：
        - low:   简单任务，收紧阈值、缩小窗口，快速收敛
        - medium: 保持默认
        - high:  放宽阈值、扩大窗口，给更多迭代空间
        - extreme: 最宽松，允许更多波动和迭代
        """
        if not self.adaptive_tuning_enabled:
            logger.info("已禁用复杂度自适应调参，保留手动配置的收敛参数")
            return

        level = complexity.complexity_level

        # 自适应参数表：(convergence_threshold, convergence_window,
        #   score_improvement_min, max_phase_iterations,
        #   deterioration_window, deterioration_drop_threshold,
        #   deterioration_error_growth, min_convergence_checks)
        profiles = {
            'low':     (0.90, 2, 0.08, 10, 2, 0.08, 1, 3),
            'medium':  (0.85, 3, 0.05, 20, 3, 0.10, 2, 3),
            'high':    (0.80, 4, 0.03, 35, 4, 0.12, 3, 3),
            'extreme': (0.75, 5, 0.02, 50, 5, 0.15, 4, 3),
        }

        profile = profiles.get(level, profiles['medium'])
        (
            self.convergence_threshold,
            self.convergence_window,
            self.score_improvement_min,
            self.max_phase_iterations,
            self.deterioration_window,
            self.deterioration_drop_threshold,
            self.deterioration_error_growth,
            self.min_convergence_checks,
        ) = profile


# ── 配置写入零丢失保护 ──


def backup_file(filepath: str, max_backups: int = 5, dedup_interval: float = 60.0) -> str | None:
    """创建带时间戳的备份文件，去重防抖，自动清理旧备份。

    Args:
        filepath: 要备份的源文件路径
        max_backups: 保留的最大备份数量
        dedup_interval: 去重间隔秒数，距上次备份不足此时间则跳过

    Returns:
        备份文件路径，或 None（文件不存在/被去重）
    """
    src = Path(filepath)
    if not src.exists():
        return None

    # 去重：检查最近的备份是否在 dedup_interval 内
    backup_pattern = str(src) + ".*.bak"
    existing_backups = sorted(glob.glob(backup_pattern))
    if existing_backups:
        latest_backup = Path(existing_backups[-1])
        latest_mtime = latest_backup.stat().st_mtime
        if time.time() - latest_mtime < dedup_interval:
            logger.debug("备份去重: 距上次备份仅 %.1f 秒，跳过", time.time() - latest_mtime)
            return None

    # 创建时间戳备份
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(f"{src}.{timestamp}.bak")
    shutil.copy2(str(src), str(backup_path))

    # 清理超出数量限制的旧备份
    all_backups = sorted(glob.glob(backup_pattern))
    while len(all_backups) > max_backups:
        oldest = all_backups.pop(0)
        try:
            os.remove(oldest)
            logger.debug("清理旧备份: %s", oldest)
        except OSError as e:
            logger.warning("清理旧备份失败 (%s): %s", oldest, e)

    return str(backup_path)


def safe_write_json(filepath: str, data: dict) -> bool:
    """安全写入 JSON 文件：备份 → 写临时文件 → 检测竞态 → 合并或覆盖 → 原子替换。

    写入流程：
    1. 记录当前文件 mtime
    2. 备份原文件（带去重）
    3. 将新数据写入 .tmp 临时文件
    4. 检查 mtime 是否被其他进程修改
    5. 若被修改，读取最新内容并与新数据合并（新数据覆盖旧数据）
    6. 原子 rename 到目标文件

    Args:
        filepath: 目标文件路径
        data: 要写入的数据字典

    Returns:
        是否成功
    """
    target = Path(filepath)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(str(target) + ".tmp")

    try:
        # 1. 记录当前 mtime
        original_mtime = target.stat().st_mtime if target.exists() else None

        # 2. 备份原文件
        if target.exists():
            backup_result = backup_file(str(target))
            if backup_result:
                logger.debug("已创建备份: %s", backup_result)

        # 3. 写入临时文件
        content = json.dumps(data, ensure_ascii=False, indent=2)
        tmp_path.write_text(content, encoding="utf-8")

        # 4. 检查竞态：原文件是否被其他进程修改
        if original_mtime is not None and target.exists():
            current_mtime = target.stat().st_mtime
            if current_mtime != original_mtime:
                logger.warning(
                    "检测到文件竞态写入: %s (mtime %.3f → %.3f)，执行合并",
                    target, original_mtime, current_mtime,
                )
                # 5. 合并：读取最新内容，用新数据覆盖
                try:
                    latest_data = json.loads(target.read_text(encoding="utf-8"))
                    if isinstance(latest_data, dict):
                        merged = {**latest_data, **data}
                        content = json.dumps(merged, ensure_ascii=False, indent=2)
                        tmp_path.write_text(content, encoding="utf-8")
                        logger.info("合并写入完成: %s", target)
                except (json.JSONDecodeError, OSError) as e:
                    logger.warning("合并失败，使用新数据覆盖: %s", e)

        # 6. 原子替换（Windows 重试）
        for attempt in range(4):
            try:
                tmp_path.replace(target)
                return True
            except OSError:
                if attempt >= 3:
                    raise
                time.sleep(0.1 * (2 ** attempt))

    except Exception as e:
        logger.error("safe_write_json 失败 (%s): %s", filepath, e)
        # 清理临时文件
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        return False

    return True


# ── GoalState 序列化/反序列化 ──

def _dt_to_str(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _str_to_dt(s: str | None) -> datetime | None:
    return datetime.fromisoformat(s) if s else None


def _review_issue_to_dict(issue: ReviewIssue) -> dict:
    return {
        "severity": issue.severity,
        "category": issue.category,
        "description": issue.description,
        "affected_files": issue.affected_files,
        "suggested_fix": issue.suggested_fix,
    }


def _corrective_action_to_dict(action: CorrectiveAction) -> dict:
    return {
        "action_id": action.action_id,
        "description": action.description,
        "prompt_template": action.prompt_template,
        "priority": action.priority,
        "depends_on_actions": action.depends_on_actions,
        "timeout": action.timeout,
    }


def _failure_classification_to_dict(fc: FailureClassification) -> dict:
    return {
        "category": fc.category.value,
        "retriable": fc.retriable,
        "feedback": fc.feedback,
        "adjust_timeout": fc.adjust_timeout,
    }


def _review_result_to_dict(r: ReviewResult) -> dict:
    return {
        "phase_id": r.phase_id,
        "verdict": r.verdict.value,
        "score": r.score,
        "summary": r.summary,
        "issues": [_review_issue_to_dict(i) for i in r.issues],
        "corrective_actions": [_corrective_action_to_dict(a) for a in r.corrective_actions],
        "reviewed_at": _dt_to_str(r.reviewed_at),
    }


def _phase_to_dict(p: Phase) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "description": p.description,
        "order": p.order,
        "objectives": p.objectives,
        "acceptance_criteria": p.acceptance_criteria,
        "depends_on_phases": p.depends_on_phases,
        "status": p.status.value,
        "iteration": p.iteration,
        "max_iterations": p.max_iterations,
        "review_result": _review_result_to_dict(p.review_result) if p.review_result else None,
        # task_outputs 可能包含大量数据，只保存摘要
        "task_output_keys": list(p.task_outputs.keys()),
        "raw_tasks": p.raw_tasks,
        "metadata": p.metadata,
        "last_classification": _failure_classification_to_dict(p.last_classification) if p.last_classification else None,
        "timeout_multiplier": p.timeout_multiplier,
        "strategy_hint": p.strategy_hint,
        "best_score": p.best_score,
        "best_outputs": p.best_outputs,
        "execution_metrics": p.execution_metrics,
        "started_at": _dt_to_str(p.started_at) if p.started_at else None,
        "completed_at": _dt_to_str(p.completed_at) if p.completed_at else None,
    }


def _iteration_record_to_dict(r: IterationRecord) -> dict:
    return {
        "iteration": r.iteration,
        "phase_id": r.phase_id,
        "score": r.score,
        "verdict": r.verdict.value,
        "actions_taken": r.actions_taken,
        "timestamp": _dt_to_str(r.timestamp),
        "failure_category": r.failure_category,
        "gate_passed": r.gate_passed,
        "regression_detected": r.regression_detected,
        "task_error_count": r.task_error_count,
        "deterioration_level": r.deterioration_level,
        "duration_seconds": r.duration_seconds,
    }


def _diagnostic_entry_to_dict(d: DiagnosticEntry) -> dict:
    return {
        "stage": d.stage,
        "entered_at": _dt_to_str(d.entered_at),
        "exit_status": d.exit_status,
        "error_detail": d.error_detail,
        "duration_seconds": d.duration_seconds,
        "stack_trace": d.stack_trace,
    }


def _requirement_question_to_dict(q: RequirementQuestion) -> dict:
    return {
        "question_id": q.question_id,
        "category": q.category,
        "question_text": q.question_text,
        "question_type": q.question_type,
        "options": q.options,
        "default": q.default,
        "context_hint": q.context_hint,
        "answer": q.answer,
        "answered": q.answered,
    }


def _gathering_round_to_dict(r: GatheringRound) -> dict:
    return {
        "round_number": r.round_number,
        "questions": [_requirement_question_to_dict(q) for q in r.questions],
        "raw_answers": r.raw_answers,
        "timestamp": _dt_to_str(r.timestamp),
    }


def _requirement_spec_to_dict(spec: RequirementSpec) -> dict:
    return {
        "original_goal": spec.original_goal,
        "sufficiency_score": spec.sufficiency_score,
        "sufficiency_verdict": spec.sufficiency_verdict,
        "scope": spec.scope,
        "acceptance_criteria": spec.acceptance_criteria,
        "technical_constraints": spec.technical_constraints,
        "non_functional_requirements": spec.non_functional_requirements,
        "priority_order": spec.priority_order,
        "excluded_scope": spec.excluded_scope,
        "rounds": [_gathering_round_to_dict(r) for r in spec.rounds],
        "total_questions_asked": spec.total_questions_asked,
        "total_questions_answered": spec.total_questions_answered,
    }


def _notebook_entry_to_dict(e: NotebookEntry) -> dict:
    return {
        "iteration": e.iteration,
        "target_file": e.target_file,
        "target_issue": e.target_issue,
        "error_before": e.error_before,
        "fix_attempted": e.fix_attempted,
        "verification_output": e.verification_output,
        "verification_passed": e.verification_passed,
        "learned": e.learned,
        "cost_usd": e.cost_usd,
        "duration_seconds": e.duration_seconds,
        "timestamp": _dt_to_str(e.timestamp),
    }


def _notebook_to_dict(nb: ContextNotebook) -> dict:
    return {
        "entries": [_notebook_entry_to_dict(e) for e in nb.entries],
        "goal": nb.goal,
        "verification_commands": nb.verification_commands,
    }


def goal_state_to_dict(state: GoalState) -> dict:
    """将 GoalState 序列化为可 JSON 化的 dict。"""
    return {
        "schema_version": state.schema_version,
        "goal_id": state.goal_id,
        "goal_text": state.goal_text,
        "status": state.status.value,
        "phases": [_phase_to_dict(p) for p in state.phases],
        "current_phase_index": state.current_phase_index,
        "iteration_history": [_iteration_record_to_dict(r) for r in state.iteration_history],
        "total_iterations": state.total_iterations,
        "deadline": _dt_to_str(state.deadline),
        "total_cost_usd": state.total_cost_usd,
        "project_context": state.project_context,
        "started_at": _dt_to_str(state.started_at),
        "diagnostics": [_diagnostic_entry_to_dict(d) for d in state.diagnostics],
        "failure_categories": state.failure_categories,
        "requirement_spec": _requirement_spec_to_dict(state.requirement_spec) if state.requirement_spec else None,
        "stop_reason": state.stop_reason,
        "active_profile": state.active_profile,
        "architecture_triggered": state.architecture_triggered,
        "architecture_contract_path": state.architecture_contract_path,
        "architecture_summary": state.architecture_summary,
        "architecture_decision_type": state.architecture_decision_type,
        "pool_state": state.pool_state,
        "runtime_dir": state.runtime_dir,
        "workspace_dir": state.workspace_dir,
        "handoff_dir": state.handoff_dir,
        "backup_summary": state.backup_summary,
        "branch_names": state.branch_names,
        "task_stats": state.task_stats,
        "deterioration_levels": state.deterioration_levels,
        "phase_history": state.phase_history,
        "safe_stop_reason": state.safe_stop_reason.value,
        "task_stats_avg_duration": state.task_stats_avg_duration,
        "task_stats_max_duration": state.task_stats_max_duration,
        "notebook": _notebook_to_dict(state.notebook) if state.notebook else None,
    }


def save_goal_state(state: GoalState, path: str | Path) -> None:
    """将 GoalState 持久化到 JSON 文件（跨进程文件锁保护 + 零丢失写入）。"""
    p = Path(path)
    lock_path = p.with_suffix(".lock")
    data = goal_state_to_dict(state)

    with _file_lock(lock_path, exclusive=True, timeout=10.0):
        # 使用 safe_write_json：备份 → 写临时文件 → 竞态检测 → 原子替换
        success = safe_write_json(str(p), data)
        if not success:
            raise OSError(f"safe_write_json 失败: {p}")


def _review_result_from_dict(d: dict) -> ReviewResult:
    return ReviewResult(
        phase_id=d["phase_id"],
        verdict=ReviewVerdict(d["verdict"]),
        score=d["score"],
        summary=d.get("summary", ""),
        issues=[
            ReviewIssue(
                severity=i["severity"],
                category=i["category"],
                description=i["description"],
                affected_files=i.get("affected_files", []),
                suggested_fix=i.get("suggested_fix", ""),
            )
            for i in d.get("issues", [])
        ],
        corrective_actions=[
            CorrectiveAction(
                action_id=a["action_id"],
                description=a["description"],
                prompt_template=a["prompt_template"],
                priority=a.get("priority", 1),
                depends_on_actions=a.get("depends_on_actions", []),
                timeout=a.get("timeout", 1800),
            )
            for a in d.get("corrective_actions", [])
        ],
        reviewed_at=_str_to_dt(d.get("reviewed_at")) or datetime.now(),
    )


def _failure_classification_from_dict(d: dict) -> FailureClassification:
    return FailureClassification(
        category=FailureCategory(d["category"]),
        retriable=d.get("retriable", True),
        feedback=d.get("feedback", ""),
        adjust_timeout=d.get("adjust_timeout", 1.0),
    )


def _phase_from_dict(d: dict) -> Phase:
    return Phase(
        id=d["id"],
        name=d["name"],
        description=d["description"],
        order=d["order"],
        objectives=d.get("objectives", []),
        acceptance_criteria=d.get("acceptance_criteria", []),
        depends_on_phases=d.get("depends_on_phases", []),
        status=PhaseStatus(d["status"]),
        iteration=d.get("iteration", 0),
        max_iterations=d.get("max_iterations", 3),
        review_result=_review_result_from_dict(d["review_result"]) if d.get("review_result") else None,
        task_outputs={},  # task_outputs 不持久化完整内容，恢复后为空
        raw_tasks=d.get("raw_tasks", []),
        metadata=d.get("metadata", {}),
        last_classification=_failure_classification_from_dict(d["last_classification"]) if d.get("last_classification") else None,
        timeout_multiplier=d.get("timeout_multiplier", 1.0),
        strategy_hint=d.get("strategy_hint", ""),
        best_score=d.get("best_score", 0.0),
        best_outputs=d.get("best_outputs", {}),
        execution_metrics=d.get("execution_metrics", {}),
        started_at=_str_to_dt(d["started_at"]) if d.get("started_at") else None,
        completed_at=_str_to_dt(d["completed_at"]) if d.get("completed_at") else None,
    )


def _iteration_record_from_dict(d: dict) -> IterationRecord:
    return IterationRecord(
        iteration=d["iteration"],
        phase_id=d["phase_id"],
        score=d["score"],
        verdict=ReviewVerdict(d["verdict"]),
        actions_taken=d.get("actions_taken", []),
        timestamp=_str_to_dt(d.get("timestamp")) or datetime.now(),
        failure_category=d.get("failure_category", ""),
        gate_passed=d.get("gate_passed"),
        regression_detected=d.get("regression_detected", False),
        task_error_count=d.get("task_error_count", 0),
        deterioration_level=d.get("deterioration_level", ""),
        duration_seconds=d.get("duration_seconds", 0.0),
    )


def _diagnostic_entry_from_dict(d: dict) -> DiagnosticEntry:
    return DiagnosticEntry(
        stage=d["stage"],
        entered_at=_str_to_dt(d["entered_at"]) or datetime.now(),
        exit_status=d.get("exit_status", "ok"),
        error_detail=d.get("error_detail", ""),
        duration_seconds=d.get("duration_seconds", 0.0),
        stack_trace=d.get("stack_trace", ""),
    )


def _requirement_question_from_dict(d: dict) -> RequirementQuestion:
    return RequirementQuestion(
        question_id=d.get("question_id", ""),
        category=d.get("category", ""),
        question_text=d.get("question_text", ""),
        question_type=d.get("question_type", "text"),
        options=d.get("options", []),
        default=d.get("default", ""),
        context_hint=d.get("context_hint", ""),
        answer=d.get("answer", ""),
        answered=d.get("answered", False),
    )


def _gathering_round_from_dict(d: dict) -> GatheringRound:
    return GatheringRound(
        round_number=d.get("round_number", 0),
        questions=[_requirement_question_from_dict(q) for q in d.get("questions", [])],
        raw_answers=d.get("raw_answers", {}),
        timestamp=_str_to_dt(d.get("timestamp")) or datetime.now(),
    )


def _requirement_spec_from_dict(d: dict) -> RequirementSpec:
    return RequirementSpec(
        original_goal=d.get("original_goal", ""),
        sufficiency_score=d.get("sufficiency_score", 0.0),
        sufficiency_verdict=d.get("sufficiency_verdict", ""),
        scope=d.get("scope", ""),
        acceptance_criteria=d.get("acceptance_criteria", []),
        technical_constraints=d.get("technical_constraints", []),
        non_functional_requirements=d.get("non_functional_requirements", []),
        priority_order=d.get("priority_order", []),
        excluded_scope=d.get("excluded_scope", []),
        rounds=[_gathering_round_from_dict(r) for r in d.get("rounds", [])],
        total_questions_asked=d.get("total_questions_asked", 0),
        total_questions_answered=d.get("total_questions_answered", 0),
    )


def load_goal_state(path: str | Path) -> GoalState:
    """从 JSON 文件恢复 GoalState（跨进程共享锁保护，防止读到写了一半的文件）。

    如果主文件损坏，自动尝试加载 .tmp 文件作为 fallback。
    """
    p = Path(path)
    tmp = p.with_suffix(".tmp")
    lock_path = p.with_suffix(".lock")

    with _file_lock(lock_path, exclusive=False, timeout=10.0):
        # 尝试主文件 → .tmp fallback
        last_error: Exception | None = None
        for candidate in (p, tmp):
            if not candidate.exists():
                continue
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
                if candidate == tmp:
                    logger.warning("主状态文件损坏，已从 .tmp 恢复: %s", tmp)
                break
            except (json.JSONDecodeError, UnicodeDecodeError, OSError) as e:
                last_error = e
                logger.warning("加载状态文件失败 (%s): %s", candidate, e)
                continue
        else:
            raise OSError(
                f"无法加载状态文件 {p} (也无可用的 .tmp fallback): {last_error}"
            )
    # 校验 schema_version 兼容性
    loaded_version = data.get("schema_version", "0.9")  # 旧文件无此字段视为 0.9
    if not loaded_version.startswith("1."):
        logger.warning(
            "goal_state schema_version=%s 与当前版本 1.0 可能不兼容，尽力恢复",
            loaded_version,
        )

    return GoalState(
        schema_version=loaded_version,
        goal_id=data["goal_id"],
        goal_text=data["goal_text"],
        status=GoalStatus(data["status"]),
        phases=[_phase_from_dict(pd) for pd in data.get("phases", [])],
        current_phase_index=data.get("current_phase_index", 0),
        iteration_history=[_iteration_record_from_dict(r) for r in data.get("iteration_history", [])],
        total_iterations=data.get("total_iterations", 0),
        deadline=_str_to_dt(data.get("deadline")) or datetime.now() + timedelta(hours=24),
        total_cost_usd=data.get("total_cost_usd", 0.0),
        project_context=data.get("project_context", ""),
        started_at=_str_to_dt(data.get("started_at")) or datetime.now(),
        diagnostics=[_diagnostic_entry_from_dict(d) for d in data.get("diagnostics", [])],
        failure_categories=data.get("failure_categories", {}),
        requirement_spec=_requirement_spec_from_dict(data["requirement_spec"]) if data.get("requirement_spec") else None,
        stop_reason=data.get("stop_reason", ""),
        active_profile=data.get("active_profile", ""),
        architecture_triggered=bool(data.get("architecture_triggered", False)),
        architecture_contract_path=data.get("architecture_contract_path", ""),
        architecture_summary=data.get("architecture_summary", ""),
        architecture_decision_type=data.get("architecture_decision_type", ""),
        pool_state=data.get("pool_state", {}),
        runtime_dir=data.get("runtime_dir", ""),
        workspace_dir=data.get("workspace_dir", ""),
        handoff_dir=data.get("handoff_dir", ""),
        backup_summary=data.get("backup_summary", ""),
        branch_names=data.get("branch_names", []),
        task_stats=data.get("task_stats", {}),
        deterioration_levels=data.get("deterioration_levels", {}),
        phase_history=data.get("phase_history", []),
        safe_stop_reason=SafeStopReason(data.get("safe_stop_reason", "unknown")) if data.get("safe_stop_reason") else SafeStopReason.UNKNOWN,
        task_stats_avg_duration=data.get("task_stats_avg_duration", 0.0),
        task_stats_max_duration=data.get("task_stats_max_duration", 0.0),
        notebook=_notebook_from_dict(data["notebook"]) if data.get("notebook") else None,
    )

# ── Self-Improve 数据模型 ──


class ImprovementSource(Enum):
    """改进提案的来源。"""
    INTROSPECTION = "introspection"
    EXTERNAL_DOC = "external_doc"
    AUTO_DISCOVER = "auto_discover"
    PLAN_FILE = "plan_file"


class ImprovementPriority(Enum):
    """改进提案的优先级。"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ImprovementStatus(Enum):
    """改进提案的生命周期状态。"""
    PROPOSED = "proposed"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class ImprovementProposal:
    """单条改进提案。"""
    proposal_id: str           # uuid hex[:8]
    title: str
    description: str           # 问题 + 方案
    rationale: str             # 为什么要改
    source: ImprovementSource
    priority: ImprovementPriority
    status: ImprovementStatus = ImprovementStatus.PROPOSED
    affected_files: list[str] = field(default_factory=list)
    estimated_complexity: str = "medium"  # small | medium | large
    evidence: str = ""         # 数据/引用
    source_url: str = ""
    source_provider: str = ""
    evidence_path: str = ""
    source_score: float = 0.0


@dataclass
class SelfImproveState:
    """自我迭代会话的完整状态。"""
    session_id: str
    proposals: list[ImprovementProposal] = field(default_factory=list)
    approved_ids: list[str] = field(default_factory=list)
    rejected_ids: list[str] = field(default_factory=list)
    execution_goal_id: str = ""
    status: str = "analyzing"  # analyzing | awaiting_approval | executing | completed | failed
    total_cost_usd: float = 0.0
    pre_test_passed: bool = False
    post_test_passed: bool = False
    git_branch: str = ""
    pre_commit_hash: str = ""  # 改进前的 commit hash，用于回滚
    runtime_dir: str = ""
    workspace_dir: str = ""
    handoff_dir: str = ""
    branch_names: list[str] = field(default_factory=list)
    goal_history: list[str] = field(default_factory=list)  # 每轮 goal 摘要（前200字符），用于跨轮次去重
    stalled_goal: bool = False  # 连续多轮相似目标均未修复时置 True
    goal_outcomes: list[dict] = field(default_factory=list)  # 每轮目标执行结果 [{"summary": str, "success": bool}]


# ── SelfImproveState 序列化/反序列化 ──


def _proposal_to_dict(p: ImprovementProposal) -> dict:
    return {
        "proposal_id": p.proposal_id,
        "title": p.title,
        "description": p.description,
        "rationale": p.rationale,
        "source": p.source.value,
        "priority": p.priority.value,
        "status": p.status.value,
        "affected_files": p.affected_files,
        "estimated_complexity": p.estimated_complexity,
        "evidence": p.evidence,
        "source_url": p.source_url,
        "source_provider": p.source_provider,
        "evidence_path": p.evidence_path,
        "source_score": p.source_score,
    }


def _proposal_from_dict(d: dict) -> ImprovementProposal:
    return ImprovementProposal(
        proposal_id=d["proposal_id"],
        title=d["title"],
        description=d["description"],
        rationale=d.get("rationale", ""),
        source=ImprovementSource(d.get("source", "introspection")),
        priority=ImprovementPriority(d.get("priority", "medium")),
        status=ImprovementStatus(d.get("status", "proposed")),
        affected_files=d.get("affected_files", []),
        estimated_complexity=d.get("estimated_complexity", "medium"),
        evidence=d.get("evidence", ""),
        source_url=d.get("source_url", ""),
        source_provider=d.get("source_provider", ""),
        evidence_path=d.get("evidence_path", ""),
        source_score=d.get("source_score", 0.0),
    )


def self_improve_state_to_dict(state: SelfImproveState) -> dict:
    return {
        "session_id": state.session_id,
        "proposals": [_proposal_to_dict(p) for p in state.proposals],
        "approved_ids": state.approved_ids,
        "rejected_ids": state.rejected_ids,
        "execution_goal_id": state.execution_goal_id,
        "status": state.status,
        "total_cost_usd": state.total_cost_usd,
        "pre_test_passed": state.pre_test_passed,
        "post_test_passed": state.post_test_passed,
        "git_branch": state.git_branch,
        "pre_commit_hash": state.pre_commit_hash,
        "runtime_dir": state.runtime_dir,
        "workspace_dir": state.workspace_dir,
        "handoff_dir": state.handoff_dir,
        "branch_names": state.branch_names,
        "goal_history": state.goal_history,
        "stalled_goal": state.stalled_goal,
        "goal_outcomes": state.goal_outcomes,
    }


def save_self_improve_state(state: SelfImproveState, path: str | Path) -> None:
    """将 SelfImproveState 持久化到 JSON 文件（跨进程文件锁保护，原子写入）。"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lock_path = p.with_suffix(".lock")
    data = self_improve_state_to_dict(state)

    with _file_lock(lock_path, exclusive=True, timeout=10.0):
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        # Windows 上目标文件可能被其他进程短暂占用，重试几次
        for attempt in range(4):
            try:
                tmp.replace(p)
                return
            except OSError:
                if attempt >= 3:
                    raise
                time.sleep(0.1 * (2 ** attempt))


def load_self_improve_state(path: str | Path) -> SelfImproveState:
    """从 JSON 文件恢复 SelfImproveState（跨进程共享锁保护）。"""
    p = Path(path)
    lock_path = p.with_suffix(".lock")

    with _file_lock(lock_path, exclusive=False, timeout=10.0):
        data = json.loads(p.read_text(encoding="utf-8"))
    return SelfImproveState(
        session_id=data["session_id"],
        proposals=[_proposal_from_dict(d) for d in data.get("proposals", [])],
        approved_ids=data.get("approved_ids", []),
        rejected_ids=data.get("rejected_ids", []),
        execution_goal_id=data.get("execution_goal_id", ""),
        status=data.get("status", "analyzing"),
        total_cost_usd=data.get("total_cost_usd", 0.0),
        pre_test_passed=data.get("pre_test_passed", False),
        post_test_passed=data.get("post_test_passed", False),
        git_branch=data.get("git_branch", ""),
        pre_commit_hash=data.get("pre_commit_hash", ""),
        runtime_dir=data.get("runtime_dir", ""),
        workspace_dir=data.get("workspace_dir", ""),
        handoff_dir=data.get("handoff_dir", ""),
        branch_names=data.get("branch_names", []),
        goal_history=data.get("goal_history", []),
        stalled_goal=data.get("stalled_goal", False),
        goal_outcomes=data.get("goal_outcomes", []),
    )
