"""Custom exceptions for the Claude Code DAG orchestrator."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from master_orchestrator.model import FailoverReason, FailoverStatus


class OrchestratorError(Exception):
    """Base exception for all orchestrator errors.

    Attributes:
        message: 错误消息
        context: 可选的上下文字典，用于存储额外的诊断信息
    """

    def __init__(self, message: str, context: dict | None = None):
        """初始化异常。

        Args:
            message: 错误消息
            context: 可选的上下文字典，用于存储额外的诊断信息
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}


class DAGValidationError(OrchestratorError):
    """Raised when a DAG definition is invalid (cycles, missing deps, etc.)."""


class DAGLoadError(OrchestratorError):
    """Raised when a DAG file cannot be loaded or parsed."""


class TemplateRenderError(OrchestratorError):
    """Raised when a prompt template cannot be rendered."""


class ClaudeCLIError(OrchestratorError):
    """Raised when the Claude CLI subprocess fails."""


class TaskTimeoutError(ClaudeCLIError):
    """Raised when a task exceeds its timeout."""


class BudgetExhaustedError(OrchestratorError):
    """Raised when the global budget limit is reached."""


class CheckpointError(OrchestratorError):
    """Raised when checkpoint store operations fail."""


class HealthCheckError(OrchestratorError):
    """Raised when a health check fails (disk space, memory, etc.)."""


class TaskConditionError(OrchestratorError):
    """Raised when a task condition expression fails to evaluate."""


class OutputValidationError(OrchestratorError):
    """Raised when task output fails schema validation."""


class PreflightError(OrchestratorError):
    """启动前置校验失败时抛出。"""


class GoalParseError(OrchestratorError):
    """目标解析失败时抛出。"""


class SchemaVersionError(OrchestratorError):
    """Raised when schema version is incompatible or missing."""


class ConfigValidationError(OrchestratorError):
    """Raised when configuration validation fails."""


class DiagnosticError(OrchestratorError):
    """Raised when diagnostic information persistence fails."""


class RateLimitError(OrchestratorError):
    """Raised when rate limit is exceeded."""


class FieldTransformError(OrchestratorError):
    """Raised when field transformation fails."""


class ContextStoreError(OrchestratorError):
    """Raised when context store operations fail."""


class LoopLimitError(OrchestratorError):
    """Raised when loop iteration limit is exceeded."""


class QueueFullError(OrchestratorError):
    """Raised when task queue is full and backpressure policy is ABORT."""


class FailoverError(ClaudeCLIError):
    """Raised when a failover condition is detected during Claude CLI execution.

    Attributes:
        reason: 故障转移原因（FailoverReason 枚举）
        provider: 提供商名称（如 "anthropic"）
        model: 模型名称（如 "claude-opus-4-20250514"）
        status: 故障转移状态（FailoverStatus 枚举）
        original_error: 原始错误信息
    """

    def __init__(
        self,
        reason: "FailoverReason",
        provider: str,
        model: str,
        status: "FailoverStatus",
        original_error: str,
        context: dict | None = None,
    ):
        """初始化 FailoverError。

        Args:
            reason: 故障转移原因
            provider: 提供商名称
            model: 模型名称
            status: 故障转移状态
            original_error: 原始错误信息
            context: 可选的上下文字典
        """
        message = (
            f"Failover triggered: {reason.value} on {provider}/{model} "
            f"-> {status.value}. Original error: {original_error}"
        )
        super().__init__(message, context)
        self.reason = reason
        self.provider = provider
        self.model = model
        self.status = status
        self.original_error = original_error
