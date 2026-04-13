"""Task contract models for preview-first autonomous execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class TaskInputType(Enum):
    NATURAL_LANGUAGE = "natural_language"
    DOCUMENT_PATH = "document_path"
    MIXED = "mixed"


class TaskType(Enum):
    BUGFIX = "bugfix"
    FEATURE = "feature"
    REFACTOR = "refactor"
    INTEGRATION = "integration"
    UNKNOWN = "unknown"


class DataRisk(Enum):
    NONE = "none"
    FILES = "files"
    DATABASE = "database"
    BOTH = "both"


_DEFAULT_STATE_FILE_PATTERNS = (
    "goal_state.json",
    "orchestrator_state.db",
    "orchestrator_state.db-shm",
    "orchestrator_state.db-wal",
)


@dataclass
class TaskContract:
    source_goal: str
    normalized_goal: str
    input_type: TaskInputType
    task_type: TaskType
    data_risk: DataRisk = DataRisk.NONE
    affected_areas: list[str] = field(default_factory=list)
    document_paths: list[str] = field(default_factory=list)
    document_briefs: list[str] = field(default_factory=list)
    document_context: str = ""
    verification_focus: list[str] = field(default_factory=list)
    inferred_from: list[str] = field(default_factory=list)
    data_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    architecture_mode: str = "auto"
    requires_architecture_council: bool | None = None
    architecture_trigger_reasons: list[str] = field(default_factory=list)
    architecture_contract_path: str = ""
    allow_multi_branch: bool = True
    delivery_local_only: bool = True
    catastrophic_handoff_only: bool = True
    strict_refactor_mode: bool | None = None
    max_service_families_per_phase: int | None = None
    max_prod_files_per_iteration: int | None = None
    forbid_state_file_edits: bool | None = None
    require_guardrail_tests_before_service_moves: bool | None = None
    allowed_refactor_roots: list[str] = field(default_factory=list)
    state_file_patterns: list[str] = field(default_factory=lambda: list(_DEFAULT_STATE_FILE_PATTERNS))
    native_phase_verification: bool | None = None
    native_phase_handoff: bool | None = None

    def __post_init__(self) -> None:
        self.document_paths = [str(Path(p)) for p in self.document_paths]
        self.document_briefs = [str(item).strip() for item in self.document_briefs if str(item).strip()]
        self.document_briefs = list(dict.fromkeys(self.document_briefs))
        self.document_context = str(self.document_context or "").strip()
        self.affected_areas = list(dict.fromkeys(self.affected_areas))
        self.verification_focus = list(dict.fromkeys(self.verification_focus))
        self.inferred_from = list(dict.fromkeys(self.inferred_from))
        self.data_paths = [str(Path(p)) for p in dict.fromkeys(self.data_paths)]
        mode = str(self.architecture_mode or "auto").strip().lower()
        self.architecture_mode = mode if mode in {"auto", "off", "required"} else "auto"
        self.architecture_trigger_reasons = list(dict.fromkeys(self.architecture_trigger_reasons))
        self.architecture_contract_path = str(self.architecture_contract_path or "").strip()
        self.allowed_refactor_roots = [
            _normalize_root(path)
            for path in dict.fromkeys(self.allowed_refactor_roots)
            if _normalize_root(path)
        ]
        self.state_file_patterns = list(dict.fromkeys(self.state_file_patterns or list(_DEFAULT_STATE_FILE_PATTERNS)))

        is_refactor = self.task_type is TaskType.REFACTOR
        if self.strict_refactor_mode is None:
            self.strict_refactor_mode = is_refactor
        if self.max_service_families_per_phase is None:
            self.max_service_families_per_phase = 1 if self.strict_refactor_mode else 3
        if self.max_prod_files_per_iteration is None:
            self.max_prod_files_per_iteration = 8 if self.strict_refactor_mode else 20
        if self.forbid_state_file_edits is None:
            self.forbid_state_file_edits = bool(self.strict_refactor_mode)
        if self.require_guardrail_tests_before_service_moves is None:
            self.require_guardrail_tests_before_service_moves = bool(self.strict_refactor_mode)
        if self.native_phase_verification is None:
            self.native_phase_verification = bool(self.strict_refactor_mode)
        if self.native_phase_handoff is None:
            self.native_phase_handoff = bool(self.strict_refactor_mode)
        if self.requires_architecture_council is None:
            self.requires_architecture_council = self.architecture_mode == "required"

    @property
    def requires_backup(self) -> bool:
        return self.data_risk is not DataRisk.NONE

    @property
    def touches_database(self) -> bool:
        return self.data_risk in (DataRisk.DATABASE, DataRisk.BOTH)

    @property
    def touches_files(self) -> bool:
        return self.data_risk in (DataRisk.FILES, DataRisk.BOTH)

    @property
    def uses_native_phase_closure(self) -> bool:
        return bool(self.strict_refactor_mode and self.native_phase_verification and self.native_phase_handoff)

    @property
    def estimated_branch_count(self) -> int:
        if not self.allow_multi_branch:
            return 1
        if self.task_type in (TaskType.INTEGRATION, TaskType.REFACTOR):
            return 2
        if self.task_type is TaskType.FEATURE and len(self.affected_areas) >= 2:
            return 2
        return 1

    @property
    def task_type_label(self) -> str:
        labels = {
            TaskType.BUGFIX: "Bug 修复",
            TaskType.FEATURE: "新增功能",
            TaskType.REFACTOR: "架构重构",
            TaskType.INTEGRATION: "联调",
            TaskType.UNKNOWN: "未分类",
        }
        return labels[self.task_type]

    @property
    def has_document_context(self) -> bool:
        return bool(self.document_paths or self.document_context or self.document_briefs)



def _normalize_root(path: str) -> str:
    text = str(Path(path)).replace('\\', '/').strip()
    if text in ('.', ''):
        return ''
    return text.rstrip('/')
