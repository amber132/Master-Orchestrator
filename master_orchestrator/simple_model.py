"""Domain models for simple task mode."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class SimpleRunStatus(str, Enum):
    QUEUED = "queued"
    SCANNING = "scanning"
    READY = "ready"
    RUNNING = "running"
    DRAINING = "draining"
    COMPLETED = "completed"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SimpleItemStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    PREPARING = "preparing"
    EXECUTING = "executing"
    RUNNING = "running"
    VALIDATING = "validating"
    SUCCEEDED = "succeeded"
    RETRY_WAIT = "retry_wait"
    FAILED = "failed"
    SKIPPED = "skipped"
    BLOCKED = "blocked"


class SimpleItemType(str, Enum):
    FILE = "file"
    DIRECTORY_SHARD = "directory_shard"
    EXTERNAL_TASK = "external_task"


class SimpleErrorCategory(str, Enum):
    NO_CHANGE = "no_change"
    WRONG_FILE_CHANGED = "wrong_file_changed"
    TARGET_MISSING_AFTER_EXEC = "target_missing_after_exec"
    TARGET_PATH_MISMATCH = "target_path_mismatch"
    UNAUTHORIZED_SIDE_FILES = "unauthorized_side_files"
    SEMANTIC_VALIDATION_FAILED = "semantic_validation_failed"
    SYNTAX_ERROR = "syntax_error"
    PATTERN_MISSING = "pattern_missing"
    VERIFY_COMMAND_FAILED = "verify_command_failed"
    TIMEOUT = "timeout"
    MAX_TURNS_EXCEEDED = "max_turns_exceeded"
    RATE_LIMITED = "rate_limited"
    AUTH_EXPIRED = "auth_expired"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    PATH_BUDGET_EXCEEDED = "path_budget_exceeded"
    COPYBACK_CONFLICT = "copyback_conflict"
    NON_RETRYABLE_EXEC_ERROR = "non_retryable_exec_error"
    CONFLICT_DETECTED = "conflict_detected"
    UNKNOWN = "unknown"


class SimpleIsolationMode(str, Enum):
    NONE = "none"
    COPY = "copy"
    WORKTREE = "worktree"


@dataclass
class ValidationStageResult:
    name: str
    passed: bool
    details: str = ""


@dataclass
class ValidationReport:
    passed: bool
    stage_results: list[ValidationStageResult] = field(default_factory=list)
    changed_files: list[str] = field(default_factory=list)
    target_touched: bool = False
    target_exists_after: bool | None = None
    target_content_changed: bool = False
    unauthorized_changes: list[str] = field(default_factory=list)
    syntax_ok: bool | None = None
    pattern_matches: dict[str, bool] = field(default_factory=dict)
    command_results: list[dict[str, Any]] = field(default_factory=list)
    rollback_performed: bool = False
    recovered_unauthorized_changes: list[str] = field(default_factory=list)
    failure_code: str = ""
    failure_reason: str = ""
    warnings: list[str] = field(default_factory=list)
    target_changed_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "stage_results": [asdict(stage) for stage in self.stage_results],
            "changed_files": list(self.changed_files),
            "target_touched": self.target_touched,
            "target_exists_after": self.target_exists_after,
            "target_content_changed": self.target_content_changed,
            "unauthorized_changes": list(self.unauthorized_changes),
            "syntax_ok": self.syntax_ok,
            "pattern_matches": dict(self.pattern_matches),
            "command_results": list(self.command_results),
            "rollback_performed": self.rollback_performed,
            "recovered_unauthorized_changes": list(self.recovered_unauthorized_changes),
            "failure_code": self.failure_code,
            "failure_reason": self.failure_reason,
            "warnings": list(self.warnings),
            "target_changed_files": list(self.target_changed_files),
        }


@dataclass
class SimpleValidationProfile:
    verify_commands: list[str] = field(default_factory=list)
    require_patterns: list[str] = field(default_factory=list)
    allowed_side_files: list[str] = field(default_factory=list)
    semantic_validators: list[str] = field(default_factory=list)


@dataclass
class AttemptState:
    attempt: int = 0
    max_attempts: int = 3
    last_error_category: str = ""
    last_failure_reason: str = ""
    next_retry_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "last_error_category": self.last_error_category,
            "last_failure_reason": self.last_failure_reason,
            "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None,
        }


@dataclass
class SimpleWorkItem:
    item_id: str
    item_type: SimpleItemType
    target: str
    bucket: str
    priority: int
    instruction: str
    attempt_state: AttemptState = field(default_factory=AttemptState)
    validation_profile: SimpleValidationProfile = field(default_factory=SimpleValidationProfile)
    metadata: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 1800
    status: SimpleItemStatus = SimpleItemStatus.PENDING

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "item_type": self.item_type.value,
            "target": self.target,
            "bucket": self.bucket,
            "priority": self.priority,
            "instruction": self.instruction,
            "attempt_state": self.attempt_state.to_dict(),
            "validation_profile": asdict(self.validation_profile),
            "metadata": self.metadata,
            "timeout_seconds": self.timeout_seconds,
            "status": self.status.value,
        }


@dataclass
class SimpleAttempt:
    item_id: str
    attempt: int
    status: SimpleItemStatus
    worker_id: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    error_category: str = ""
    failure_reason: str = ""
    changed_files: list[str] = field(default_factory=list)
    validation_report: ValidationReport | None = None
    output: str = ""
    error: str = ""
    cost_usd: float = 0.0
    model_used: str = ""
    provider_used: str = ""
    pid: int = 0
    token_input: int | None = None
    token_output: int | None = None
    cli_duration_ms: float | None = None
    claude_home_ready_ms: float | None = None
    execution_wall_ms: float | None = None
    tool_uses: int | None = None
    turn_started: int | None = None
    turn_completed: int | None = None
    max_turns_exceeded: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "attempt": self.attempt,
            "status": self.status.value,
            "worker_id": self.worker_id,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "exit_code": self.exit_code,
            "error_category": self.error_category,
            "failure_reason": self.failure_reason,
            "changed_files": list(self.changed_files),
            "validation_report": self.validation_report.to_dict() if self.validation_report else None,
            "output": self.output,
            "error": self.error,
            "cost_usd": self.cost_usd,
            "model_used": self.model_used,
            "provider_used": self.provider_used,
            "pid": self.pid,
            "token_input": self.token_input,
            "token_output": self.token_output,
            "cli_duration_ms": self.cli_duration_ms,
            "claude_home_ready_ms": self.claude_home_ready_ms,
            "execution_wall_ms": self.execution_wall_ms,
            "tool_uses": self.tool_uses,
            "turn_started": self.turn_started,
            "turn_completed": self.turn_completed,
            "max_turns_exceeded": self.max_turns_exceeded,
        }


@dataclass
class BucketStats:
    name: str
    total_items: int = 0
    completed_items: int = 0
    failed_items: int = 0
    running_items: int = 0
    retries: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SimpleManifest:
    run_id: str
    total_items: int = 0
    completed_items: int = 0
    failed_items: int = 0
    retried_success_items: int = 0
    uncovered_targets: list[str] = field(default_factory=list)
    bucket_stats: dict[str, BucketStats] = field(default_factory=dict)
    error_stats: dict[str, int] = field(default_factory=dict)
    validation_stats: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0
    total_cost_usd: float = 0.0
    isolation_mode: str = SimpleIsolationMode.NONE.value
    input_sources: dict[str, int] = field(default_factory=dict)
    throttle_events: list[dict[str, Any]] = field(default_factory=list)
    execution_stats: dict[str, Any] = field(default_factory=dict)
    stage_timing_stats: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["bucket_stats"] = {name: bucket.to_dict() for name, bucket in self.bucket_stats.items()}
        return data


@dataclass
class SimpleRun:
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    run_kind: str = "simple"
    instruction_template: str = ""
    status: SimpleRunStatus = SimpleRunStatus.QUEUED
    source_summary: dict[str, Any] = field(default_factory=dict)
    isolation_mode: str = SimpleIsolationMode.NONE.value
    scheduler_config_snapshot: dict[str, Any] = field(default_factory=dict)
    working_dir: str = "."
    started_at: datetime = field(default_factory=datetime.now)
    last_heartbeat_at: datetime | None = None
    finished_at: datetime | None = None
    manifest_path: str = ""
    pool_id: str = ""
    active_profile: str = ""
    stop_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "run_kind": self.run_kind,
            "instruction_template": self.instruction_template,
            "status": self.status.value,
            "source_summary": self.source_summary,
            "isolation_mode": self.isolation_mode,
            "scheduler_config_snapshot": self.scheduler_config_snapshot,
            "working_dir": self.working_dir,
            "started_at": self.started_at.isoformat(),
            "last_heartbeat_at": self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "manifest_path": self.manifest_path,
            "pool_id": self.pool_id,
            "active_profile": self.active_profile,
            "stop_reason": self.stop_reason,
        }


def stable_item_id(target: str, instruction: str, mode: str) -> str:
    raw = json.dumps({"target": target, "instruction": instruction, "mode": mode}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
