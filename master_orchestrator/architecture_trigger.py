"""Policy for deciding whether architecture deliberation is needed."""

from __future__ import annotations

from dataclasses import dataclass, field

from .repo_profile import RepoProfile
from .task_contract import TaskContract, TaskType


@dataclass
class ArchitectureTriggerDecision:
    should_trigger: bool
    reasons: list[str]
    confidence: float
    suggested_roles: list[str] = field(default_factory=list)
    suggested_patterns: list[str] = field(default_factory=list)


class ArchitectureTrigger:
    _ARCHITECTURE_KEYWORDS = (
        "边界", "拆分", "抽离", "迁移", "微服务", "服务化", "数据所有权", "事件", "适配层",
        "模块化", "facade", "adapter", "ownership", "consistency", "migration",
        "boundary", "service", "modular", "strangler", "plugin", "extract",
    )

    def decide(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        project_context: str = "",
    ) -> ArchitectureTriggerDecision:
        mode = (task_contract.architecture_mode or "auto").strip().lower()
        if mode == "off":
            return ArchitectureTriggerDecision(False, [], 0.0)

        text = " ".join(
            [
                task_contract.source_goal,
                task_contract.normalized_goal,
                task_contract.document_context,
                project_context,
                " ".join(task_contract.document_briefs),
            ]
        ).lower()

        reasons: list[str] = []
        if any(keyword.lower() in text for keyword in self._ARCHITECTURE_KEYWORDS):
            reasons.append("architecture_keywords")
        if len(task_contract.affected_areas) >= 2:
            reasons.append("multi_area_change")
        if task_contract.task_type is TaskType.REFACTOR and len(task_contract.affected_areas) >= 2:
            reasons.append("refactor_requires_design")
        if repo_profile and repo_profile.has_backend and repo_profile.has_frontend and any(
            area in task_contract.affected_areas for area in ("backend", "frontend", "integration")
        ):
            reasons.append("cross_layer_change")
        if task_contract.touches_database and len(task_contract.affected_areas) >= 2:
            reasons.append("boundary_change")

        suggested_patterns = self._suggest_patterns(text, task_contract)
        suggested_roles = self._suggest_roles(task_contract, repo_profile)
        if mode == "required":
            reasons.insert(0, "required_mode")

        should_trigger = bool(reasons) or mode == "required"
        confidence = min(0.95, 0.42 + 0.12 * len(set(reasons)))
        if mode == "required" and confidence < 0.8:
            confidence = 0.8

        return ArchitectureTriggerDecision(
            should_trigger=should_trigger,
            reasons=list(dict.fromkeys(reasons)),
            confidence=confidence if should_trigger else 0.0,
            suggested_roles=suggested_roles,
            suggested_patterns=suggested_patterns,
        )

    def _suggest_patterns(self, text: str, task_contract: TaskContract) -> list[str]:
        patterns: list[str] = []
        if any(token in text for token in ("微服务", "服务", "service", "抽离", "extract")):
            patterns.append("service_extraction")
        if any(token in text for token in ("迁移", "strangler", "渐进")):
            patterns.append("strangler_migration")
        if any(token in text for token in ("事件", "event")):
            patterns.append("event_driven_reorganization")
        if any(token in text for token in ("插件", "plugin", "扩展")):
            patterns.append("plugin_extension_extraction")
        if any(token in text for token in ("facade", "门面", "兼容")):
            patterns.append("api_facade")
        if not patterns and task_contract.task_type is TaskType.REFACTOR:
            patterns.append("modular_monolith_boundary_cleanup")
        return list(dict.fromkeys(patterns))

    def _suggest_roles(self, task_contract: TaskContract, repo_profile: RepoProfile | None) -> list[str]:
        roles = ["principal_architect", "skeptical_architect"]
        if task_contract.touches_database:
            roles.append("data_specialist")
        elif repo_profile and repo_profile.has_backend and repo_profile.has_frontend:
            roles.append("platform_specialist")
        if "security" in task_contract.affected_areas:
            roles.append("security_specialist")
        roles.append("judge_scribe")
        return list(dict.fromkeys(roles))
