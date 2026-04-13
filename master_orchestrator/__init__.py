"""Master Orchestrator — unified DAG orchestration across Claude Code and Codex."""

__version__ = "0.1.0"

from .auto_model import AutoConfig, GoalResult, GoalState, ImprovementProposal, SelfImproveState
from .auto_model import RequirementSpec, RequirementQuestion, GatheringRound
from .auto_model import (
    ComplexityEstimate,
    ConvergenceSignal,
    CorrectiveAction,
    DeteriorationLevel,
    DeteriorationSignal,
    DiagnosticEntry,
    ExecutionMetrics,
    FailureCategory,
    FailureClassification,
    GoalStatus,
    ImprovementPriority,
    ImprovementSource,
    ImprovementStatus,
    IterationHandoff,
    IterationRecord,
    Phase,
    PhaseStatus,
    QualityGate,
    QualityGateResult,
    RegressionBaseline,
    RegressionConstraint,
    ReviewIssue,
    ReviewResult,
    ReviewVerdict,
    SafeStopReason,
    TaskError,
    load_goal_state,
    save_goal_state,
)
from .autonomous import AutonomousController
from .backup_gate import BackupGate, BackupGateError
from .backup_manifest import BackupManifest, BackupEntry, BackupResourceType
from .catastrophic_guard import CatastrophicGuard
from .closure_planner import ClosurePlanner
from .config import Config, RequirementConfig, load_config
from .context_store import ContextStore
from .delivery_manifest import DeliveryManifest
from .diagnostics import DiagnosticEventType, DiagnosticEvent, DiagnosticLogger
from .dag_loader import load_dag
from .error_classifier import classify_error, classify_detailed, should_retry, should_retry_with_priority
from .error_classifier import DetailedErrorInfo, FailoverResult, PromptTooLongInfo, RateLimitInfo, parse_rate_limit_headers
from .execution_preview import ExecutionPreview
from .field_transform import apply_transforms
from .handoff_packager import HandoffPackager
from .link_resolver import inject_link_context, resolve_links
from .llm_schema import LLMTaskSchema, LLMWorkflowSchema, dag_to_llm_schema, llm_schema_to_dag
from .model import (
    DAG,
    ControllerConfig,
    ErrorPolicy,
    FieldTransform,
    LinkMapping,
    LoopConfig,
    ModelFallbackChain,
    PhaseRecord,
    PlateauAction,
    PlateauSignal,
    RetryPolicy,
    RunInfo,
    RunStatus,
    TaskNode,
    TaskResult,
    TaskStatus,
)
from .orchestrator import Orchestrator
from .pagination import Paginator
from .rate_limiter import RateLimiter
from .repo_profile import RepoProfile, RepoProfiler
from .runtime_layout import RuntimeLayout
from .self_improve import SelfImproveController
from .simple_config import SimpleConfig
from .simple_lease import ExecutionLease, SimpleExecutionLeaseManager
from .simple_model import SimpleManifest, SimpleRun, SimpleWorkItem
from .simple_runtime import SimpleTaskRunner
from .store import Store
from .task_classifier import TaskClassification, TaskClassifier
from .task_contract import DataRisk, TaskContract, TaskInputType, TaskType
from .task_intake import TaskIntakeRequest, build_task_contract, normalize_request
from .verification_planner import VerificationCommand, VerificationPlan, VerificationPlanner
from .verification_runner import VerificationRunResult, VerificationRunner
from .workspace_manager import WorkspaceManager, WorkspaceSession

# 新增模块导入
from . import audit_log
from . import checkpoint
from . import error_model
from . import guardrail
from . import metrics
from . import plugin_registry
from . import sanitizer
from . import schema_version
from . import task_cache
from . import validator

__all__ = [
    "AutoConfig",
    "AutonomousController",
    "BackupEntry",
    "BackupGate",
    "BackupGateError",
    "BackupManifest",
    "BackupResourceType",
    "CatastrophicGuard",
    "ClosurePlanner",
    "ComplexityEstimate",
    "Config",
    "ConvergenceSignal",
    "ContextStore",
    "ControllerConfig",
    "CorrectiveAction",
    "DAG",
    "DataRisk",
    "DeliveryManifest",
    "DiagnosticEvent",
    "DiagnosticEventType",
    "DiagnosticLogger",
    "DeteriorationLevel",
    "DeteriorationSignal",
    "DiagnosticEntry",
    "ErrorPolicy",
    "ExecutionMetrics",
    "ExecutionPreview",
    "FailureCategory",
    "FailureClassification",
    "FieldTransform",
    "GatheringRound",
    "GoalResult",
    "GoalState",
    "GoalStatus",
    "HandoffPackager",
    "ImprovementPriority",
    "ImprovementProposal",
    "ImprovementSource",
    "ImprovementStatus",
    "IterationHandoff",
    "IterationRecord",
    "LLMTaskSchema",
    "LLMWorkflowSchema",
    "LinkMapping",
    "LoopConfig",
    "ModelFallbackChain",
    "Orchestrator",
    "Paginator",
    "Phase",
    "PhaseRecord",
    "PhaseStatus",
    "PlateauAction",
    "PlateauSignal",
    "QualityGate",
    "QualityGateResult",
    "RateLimiter",
    "RegressionBaseline",
    "RegressionConstraint",
    "RepoProfile",
    "RepoProfiler",
    "RequirementConfig",
    "RequirementQuestion",
    "RequirementSpec",
    "RetryPolicy",
    "ReviewIssue",
    "ReviewResult",
    "ReviewVerdict",
    "RunInfo",
    "RunStatus",
    "RuntimeLayout",
    "SafeStopReason",
    "SelfImproveController",
    "SelfImproveState",
    "SimpleConfig",
    "ExecutionLease",
    "SimpleExecutionLeaseManager",
    "SimpleManifest",
    "SimpleRun",
    "SimpleTaskRunner",
    "SimpleWorkItem",
    "Store",
    "TaskClassification",
    "TaskClassifier",
    "TaskContract",
    "TaskError",
    "TaskInputType",
    "TaskIntakeRequest",
    "TaskNode",
    "TaskResult",
    "TaskStatus",
    "TaskType",
    "VerificationCommand",
    "VerificationPlan",
    "VerificationPlanner",
    "VerificationRunResult",
    "VerificationRunner",
    "WorkspaceManager",
    "WorkspaceSession",
    "apply_transforms",
    "build_task_contract",
    "classify_error",
    "classify_detailed",
    "DetailedErrorInfo",
    "FailoverResult",
    "PromptTooLongInfo",
    "RateLimitInfo",
    "parse_rate_limit_headers",
    "dag_to_llm_schema",
    "inject_link_context",
    "llm_schema_to_dag",
    "load_config",
    "load_dag",
    "load_goal_state",
    "normalize_request",
    "resolve_links",
    "save_goal_state",
    "should_retry",
    "should_retry_with_priority",
    # 新增模块
    "audit_log",
    "checkpoint",
    "error_model",
    "guardrail",
    "metrics",
    "plugin_registry",
    "sanitizer",
    "schema_version",
    "task_cache",
    "validator",
]
