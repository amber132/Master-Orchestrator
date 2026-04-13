"""Data models for the Claude Code DAG orchestrator."""

from __future__ import annotations

import hashlib
import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from master_orchestrator.config import Config
    from master_orchestrator.auto_model import AutoConfig
    from master_orchestrator.store import Store
    from master_orchestrator.task_contract import TaskContract
    from master_orchestrator.repo_profile import RepoProfile
    from master_orchestrator.runtime_layout import RuntimeLayout
    from master_orchestrator.verification_planner import VerificationPlan
    from master_orchestrator.architecture_contract import ArchitectureContract
    from master_orchestrator.backup_manifest import BackupManifest
    from master_orchestrator.failover_pool import PoolRuntime


class TaskStatus(Enum):
    PENDING = "pending"
    WAITING = "waiting"
    READY = "ready"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class RunStatus(Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    SAFE_STOP = "safe_stop"


class ErrorCategory(Enum):
    """错误分类：用于智能错误处理和重试策略"""
    RETRYABLE = "retryable"              # 可重试错误（网络超时、临时故障等）
    NON_RETRYABLE = "non_retryable"      # 不可重试错误（语法错误、权限不足等）
    NEEDS_HUMAN = "needs_human"          # 需要人工介入（复杂逻辑错误、设计缺陷等）


class FailoverReason(Enum):
    """故障转移原因：具体的错误类型，用于决定回退策略"""
    RATE_LIMIT = "rate_limit"                # API 速率限制
    AUTH_EXPIRED = "auth_expired"            # 认证过期
    CONTEXT_OVERFLOW = "context_overflow"    # 上下文长度超限
    TIMEOUT = "timeout"                      # 请求超时
    NETWORK_ERROR = "network_error"          # 网络错误
    BUDGET_EXHAUSTED = "budget_exhausted"    # 预算耗尽
    UNKNOWN = "unknown"                      # 未知错误
    # 细粒度错误类型：区分不同恢复策略
    MODEL_OVERLOAD = "model_overload"            # 529 服务器过载（区别于 rate_limit）
    INVALID_MODEL = "invalid_model"              # 模型名不存在
    TOOL_USE_MISMATCH = "tool_use_mismatch"      # tool_use/tool_result 格式不匹配
    CONTENT_TOO_LARGE = "content_too_large"      # 输出内容超长
    CREDIT_EXHAUSTED = "credit_exhausted"        # 信用额度耗尽
    ORGANIZATION_BLOCKED = "organization_blocked"  # 组织被禁用
    PROMPT_TOO_LONG = "prompt_too_long"            # prompt 超出模型 token 上限


class FailoverStatus(Enum):
    """故障转移状态：根据错误原因决定的处理动作"""
    RETRY_IMMEDIATELY = "retry_immediately"      # 立即重试（同模型）
    RETRY_WITH_BACKOFF = "retry_with_backoff"    # 退避重试（同模型）
    SWITCH_MODEL = "switch_model"                # 切换到备用模型
    ABORT = "abort"                              # 终止任务
    NEEDS_HUMAN = "needs_human"                  # 需要人工介入


class CommandLane(Enum):
    """命令泳道：用于隔离不同类型任务的并发资源"""
    MAIN = "main"            # 主任务泳道
    SUBAGENT = "subagent"    # 子代理泳道
    REVIEW = "review"        # 审查任务泳道
    CRON = "cron"            # 定时任务泳道


class PlateauAction(Enum):
    """收敛评分平台期时的应对动作"""
    ESCALATE_REVIEW = "escalate_review"    # 升级审查：切换到更强的模型重新审查
    SHIFT_FOCUS = "shift_focus"            # 转移焦点：切换到尚未充分改进的维度
    INCREASE_DEPTH = "increase_depth"      # 加深分析：要求更细粒度的修改
    RESET_STRATEGY = "reset_strategy"      # 重置策略：清空上下文，从头重新规划


@dataclass
class PlateauSignal:
    """收敛评分平台期信号：检测到评分停滞时生成，驱动 convergence 模块采取应对措施"""
    consecutive_rounds: int                           # 连续评分平台期的轮数
    plateau_score: float                              # 平台期评分值
    suggested_action: PlateauAction                   # 建议的应对动作
    suggested_dimensions: list[str] = field(default_factory=list)  # 建议转移焦点的维度列表


@dataclass
class PhaseRecord:
    """阶段执行记录：记录自主控制器每个阶段的执行信息，用于断点续传和进度追踪。"""
    phase_id: str                                         # 阶段唯一标识
    start_time: float                                     # 阶段开始时间（time.time()）
    end_time: float = 0.0                                 # 阶段结束时间（time.time()）
    task_ids: list[str] = field(default_factory=list)     # 该阶段包含的任务 ID 列表
    score: float = 0.0                                    # 阶段评分（审查引擎输出）
    phase_name: str = ''                                  # 阶段名称（如 "decompose-1"、"review-3"）
    phase_type: str = ''                                  # 阶段类型：decompose/execute/review/analysis/custom


@dataclass
class ModelFallbackChain:
    """模型回退链：定义主模型和备用模型列表"""
    primary: str                          # 主模型（如 "opus"）
    fallbacks: list[str] = field(default_factory=list)  # 备用模型列表（如 ["sonnet", "haiku"]）


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    backoff_base: float = 30.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    max_delay: float = 300.0  # 最大退避延迟（秒），防止指数增长过大

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate backoff delay for a given attempt number (1-based)."""
        # 基础延迟 = min(base * multiplier^(attempt-1), max_delay)
        base_delay = self.backoff_base * (self.backoff_multiplier ** (attempt - 1))
        capped_delay = min(base_delay, self.max_delay)

        # 然后再加抖动
        if self.jitter:
            capped_delay += random.uniform(0, capped_delay * 0.3)
        return capped_delay


@dataclass
class LifecycleHooks:
    """生命周期钩子：在任务状态转换时执行的可选命令"""
    on_task_start: str | None = None      # 任务启动时执行的命令
    on_task_complete: str | None = None   # 任务成功完成时执行的命令
    on_task_fail: str | None = None       # 任务失败时执行的命令


@dataclass
class LoopConfig:
    """循环配置：控制任务的循环执行行为"""
    max_iterations: int = 5              # 最大循环次数
    until_condition: str = ''            # 循环终止条件（表达式字符串）
    retry_on_failure: bool = True        # 失败时是否重试


@dataclass
class FieldTransform:
    """字段转换：从上游任务 JSON 输出提取字段注入下游任务"""
    source_path: str  # JSONPath 表达式，如 "result.user_id" 或 "items[0].name"
    target_key: str   # 注入到下游 prompt 的变量名
    default: Any = None  # 当 source_path 不存在时的默认值


@dataclass
class ErrorPolicy:
    """错误处理策略：控制任务失败时的行为"""
    on_error: str = 'fail-fast'  # 错误处理模式：'fail-fast' | 'continue-on-error' | 'skip-downstream'
    error_handler: str | None = None  # 自定义错误处理器（命令或脚本路径）
    classify_errors: bool = True  # 是否启用智能错误分类（RETRYABLE/NON_RETRYABLE/NEEDS_HUMAN）


@dataclass
class LinkMapping:
    """任务间数据链接映射"""
    upstream_task: str  # 上游任务 ID
    output_path: str    # 输出路径（从上游任务输出中提取）
    input_key: str      # 输入键（注入到当前任务的上下文中）


@dataclass
class TaskNode:
    id: str
    prompt_template: str
    depends_on: list[str] = field(default_factory=list)
    timeout: int = 1800
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    model: str | None = None
    complexity: str | None = None  # 任务复杂度：'simple' | 'moderate' | 'complex'，用于自动选择模型
    output_format: str = "json"
    output_schema: dict | None = None
    working_dir: str | None = None
    allowed_tools: list[str] | None = None  # 工具白名单，None 表示允许所有工具
    system_prompt: str | None = None
    max_budget_usd: float | None = None
    max_turns: int | None = None
    condition: str | None = None
    tags: list[str] = field(default_factory=list)
    preload_skills: list[str] | None = None  # 预加载的 Skills 列表，在 prompt 前自动加载
    validation_gate: dict | None = None  # 验证门禁，包含 validator_prompt (str) 和 pass_threshold (float)
    color: str | None = None  # 日志输出时的颜色标签，用于区分不同 Agent
    loop: LoopConfig | None = None  # 循环配置，控制任务的循环执行行为
    transform: list[FieldTransform] | None = None  # 字段转换规则，从上游输出提取字段注入当前任务
    error_policy: ErrorPolicy | None = None  # 错误处理策略，控制任务失败时的行为
    links: list[LinkMapping] = field(default_factory=list)  # 任务间数据链接
    priority: int = 0  # 任务优先级，值越大优先级越高
    is_sequential: bool = False  # 是否独占执行（串行任务）
    task_type: str = 'io'  # 任务类型：'io' 或 'cpu'，用于区分 I/O 密集型和 CPU 密集型任务
    concurrency_group: str | None = None  # 并发分组名，同组任务受并发限制
    lane: str = 'main'  # 命令泳道，用于隔离不同类型任务的并发资源
    idempotent: bool = False  # 幂等任务标记，启用时会缓存任务结果并在相同输入时复用
    provider: str = "auto"  # provider 选择：auto / claude / codex
    type: str = 'agent_cli'  # 任务执行器类型，默认使用统一 CLI 执行器
    executor_config: dict[str, Any] | None = None  # 执行器专属配置
    env_overrides: dict[str, str] | None = None  # 额外环境变量，用于隔离状态目录等
    extra_args: list[str] | None = None  # 透传给 claude -p 的附加参数
    ephemeral: bool = False  # simple/bulk 等外部持久化场景下禁用 Claude 自身会话落盘
    read_only: bool | None = None  # 向后兼容旧字段名
    is_read_only: bool = False  # 只读任务（扫描、审计等），使用独立的读并发池
    is_critical: bool = False  # 主链路任务，享受更激进的重试和模型降级策略

    def __post_init__(self) -> None:
        if self.read_only is None:
            self.read_only = self.is_read_only
        else:
            self.is_read_only = self.read_only


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    output: str | None = None
    parsed_output: Any = None
    error: str | None = None
    attempt: int = 1
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float = 0.0
    cost_usd: float = 0.0
    model_used: str = ""
    provider_used: str = ""
    pid: int = 0
    validation_passed: bool | None = None  # 验证门禁是否通过，None 表示未执行验证
    token_input: int | None = None
    token_output: int | None = None
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cli_duration_ms: float | None = None
    tool_uses: int | None = None
    turn_started: int | None = None
    turn_completed: int | None = None
    max_turns_exceeded: bool = False

    def to_dict(self) -> dict[str, Any]:
        """转换为字典，处理 datetime 和 Enum 序列化"""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "output": self.output,
            "parsed_output": self.parsed_output,
            "error": self.error,
            "attempt": self.attempt,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
            "cost_usd": self.cost_usd,
            "model_used": self.model_used,
            "provider_used": self.provider_used,
            "pid": self.pid,
            "validation_passed": self.validation_passed,
            "token_input": self.token_input,
            "token_output": self.token_output,
            "cache_read_tokens": self.cache_read_tokens,
            "cache_creation_tokens": self.cache_creation_tokens,
            "cli_duration_ms": self.cli_duration_ms,
            "tool_uses": self.tool_uses,
            "turn_started": self.turn_started,
            "turn_completed": self.turn_completed,
            "max_turns_exceeded": self.max_turns_exceeded,
        }

    @classmethod
    def from_exception(
        cls,
        task_id: str,
        exc: Exception,
        duration_seconds: float = 0.0,
        output: str = "",
        started_at: datetime | None = None,
    ) -> "TaskResult":
        """
        从异常对象创建 TaskResult

        Args:
            task_id: 任务 ID
            exc: 捕获的异常对象
            duration_seconds: 执行耗时（秒）
            output: 已产生的部分输出
            started_at: 任务开始时间，未提供时使用当前时间

        Returns:
            TaskResult 实例
        """
        error_message = str(exc)
        _started_at = started_at or datetime.now()

        return cls(
            task_id=task_id,
            status=TaskStatus.FAILED,
            output=output,
            error=error_message,
            started_at=_started_at,
            finished_at=datetime.now(),
            duration_seconds=duration_seconds,
        )

    @classmethod
    def from_cli_output(
        cls,
        task_id: str,
        stdout: str,
        stderr: str,
        returncode: int,
        duration_seconds: float = 0.0
    ) -> "TaskResult":
        """
        从 CLI 命令输出创建 TaskResult

        Args:
            task_id: 任务 ID
            stdout: 标准输出
            stderr: 标准错误输出
            returncode: 返回码（0 表示成功）
            duration_seconds: 执行耗时（秒）

        Returns:
            TaskResult 实例
        """
        success = (returncode == 0)
        output = stdout.strip()
        error_message = stderr.strip() if stderr.strip() else None

        return cls(
            task_id=task_id,
            status=TaskStatus.SUCCESS if success else TaskStatus.FAILED,
            output=output,
            error=error_message,
            finished_at=datetime.now(),
            duration_seconds=duration_seconds,
        )


@dataclass
class DAG:
    name: str
    tasks: dict[str, TaskNode] = field(default_factory=dict)
    max_parallel: int = 30
    hooks: LifecycleHooks | None = None  # 生命周期钩子配置
    schema_version: str = "2.0.0"  # DAG schema 版本号

    def task(
        self,
        task_id: str,
        prompt: str,
        depends_on: list[str] | None = None,
        **kwargs: Any,
    ) -> TaskNode:
        """Python DSL helper: add a task to the DAG and return it."""
        node = TaskNode(
            id=task_id,
            prompt_template=prompt,
            depends_on=depends_on or [],
            **kwargs,
        )
        self.tasks[task_id] = node
        return node

    def validate(self) -> list[str]:
        """Return a list of validation error messages (empty = valid)."""
        errors: list[str] = []
        all_ids = set(self.tasks)

        for tid, node in self.tasks.items():
            for dep in node.depends_on:
                if dep not in all_ids:
                    errors.append(f"Task '{tid}' depends on unknown task '{dep}'")

        # Cycle detection via DFS
        visited: set[str] = set()
        in_stack: set[str] = set()

        def _dfs(tid: str) -> bool:
            visited.add(tid)
            in_stack.add(tid)
            for dep in self.tasks[tid].depends_on:
                if dep in in_stack:
                    errors.append(f"Cycle detected involving task '{dep}'")
                    return True
                if dep not in visited and dep in all_ids:
                    if _dfs(dep):
                        return True
            in_stack.discard(tid)
            return False

        for tid in all_ids:
            if tid not in visited:
                _dfs(tid)

        return errors

    def content_hash(self) -> str:
        """计算 DAG 内容的 SHA-256 哈希，用于检测 DAG 是否被修改。"""
        data: dict[str, Any] = {
            "name": self.name,
            "max_parallel": self.max_parallel,
            "tasks": {},
        }
        for tid in sorted(self.tasks):
            node = self.tasks[tid]
            data["tasks"][tid] = {
                "prompt_template": node.prompt_template,
                "depends_on": sorted(node.depends_on),
                "model": node.model,
            }
        raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()


@dataclass
class FailureInfo:
    """失败详情：包含异常的结构化信息，用于断点续传和跨进程传递诊断数据。"""
    exception_type: str = ""    # 异常类名，如 "RuntimeError", "TimeoutError"
    message: str = ""           # 异常消息
    stacktrace: str = ""        # 完整堆栈跟踪


@dataclass
class RunInfo:
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    dag_name: str = ""
    dag_hash: str = ""
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: datetime | None = None
    total_cost_usd: float = 0.0
    pool_id: str = ""
    active_profile: str = ""
    failure_info: FailureInfo | None = None  # 结构化失败详情

    def to_dict(self) -> dict[str, Any]:
        """转换为字典，处理 datetime 和 Enum 序列化"""
        result: dict[str, Any] = {
            "run_id": self.run_id,
            "dag_name": self.dag_name,
            "dag_hash": self.dag_hash,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "total_cost_usd": self.total_cost_usd,
            "pool_id": self.pool_id,
            "active_profile": self.active_profile,
        }
        if self.failure_info is not None:
            result["failure_info"] = {
                "exception_type": self.failure_info.exception_type,
                "message": self.failure_info.message,
                "stacktrace": self.failure_info.stacktrace,
            }
        return result


@dataclass
class ControllerConfig:
    """AutonomousController 配置数据类：封装 __init__ 的全部参数为可选字段。
    用于延迟构造、序列化传递或断点续传场景。
    """
    goal: str | None = None
    working_dir: str | None = None
    config: Config | None = None
    auto_config: AutoConfig | None = None
    store: Store | None = None
    log_file: str | None = None
    resume: bool | None = None
    gather_enabled: bool | None = None
    gather_mode: str | None = None
    gather_max_rounds: int | None = None
    gather_file: str | None = None
    task_contract: TaskContract | None = None
    repo_profile: RepoProfile | None = None
    runtime_layout: RuntimeLayout | None = None
    verification_plan: VerificationPlan | None = None
    architecture_contract: ArchitectureContract | None = None
    backup_manifest: BackupManifest | None = None
    pool_runtime: PoolRuntime | None = None
    explicit_mode: str = ""  # "auto" | "surgical" | "simple" — 策略选择器的显式覆盖
    preferred_provider: str = "auto"
    phase_provider_overrides: dict[str, str] = field(default_factory=dict)


class ModelJSONEncoder(json.JSONEncoder):
    """自定义 JSON 编码器，处理 datetime 和 Enum 类型"""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)
