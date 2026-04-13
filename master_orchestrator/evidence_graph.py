"""Evidence collection for architecture-aware planning."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .repo_profile import RepoProfile
from .task_contract import TaskContract
from .verification_planner import VerificationPlan


@dataclass
class EvidenceItem:
    evidence_id: str
    kind: str
    source: str
    summary: str
    file_refs: list[str] = field(default_factory=list)
    confidence: float = 0.0
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EvidenceGraph:
    items: list[EvidenceItem] = field(default_factory=list)
    relations: list[dict[str, str]] = field(default_factory=list)


class EvidenceGraphBuilder:
    def build(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        project_context: str = "",
        verification_plan: VerificationPlan | None = None,
    ) -> EvidenceGraph:
        items: list[EvidenceItem] = []
        if repo_profile is not None:
            frameworks = ", ".join(repo_profile.detected_frameworks) or "unknown"
            items.append(
                EvidenceItem(
                    evidence_id="repo_profile",
                    kind="repo_structure",
                    source="repo_profile",
                    summary=f"仓库技术栈: {frameworks}; 影响区域: {', '.join(task_contract.affected_areas) or 'unknown'}",
                    confidence=0.85,
                    tags=list(repo_profile.detected_frameworks),
                    metadata={
                        "has_backend": repo_profile.has_backend,
                        "has_frontend": repo_profile.has_frontend,
                        "package_managers": list(repo_profile.package_managers),
                    },
                )
            )
            if repo_profile.docker_compose_file:
                items.append(
                    EvidenceItem(
                        evidence_id="deployment_topology",
                        kind="deployment_topology",
                        source="repo_profile",
                        summary=f"检测到 compose 拓扑: {repo_profile.docker_compose_file}",
                        file_refs=[repo_profile.docker_compose_file],
                        confidence=0.8,
                        tags=["deployment", "compose"],
                    )
                )
            if repo_profile.file_backup_paths or repo_profile.database_backup_commands:
                items.append(
                    EvidenceItem(
                        evidence_id="stateful_assets",
                        kind="stateful_resource",
                        source="repo_profile",
                        summary="检测到文件/数据库备份线索，说明任务可能涉及状态迁移。",
                        confidence=0.78,
                        tags=["stateful", "backup"],
                        metadata={
                            "file_backup_paths": list(repo_profile.file_backup_paths),
                            "database_backup_commands": list(repo_profile.database_backup_commands),
                        },
                    )
                )

        if task_contract.document_paths or task_contract.document_context:
            items.append(
                EvidenceItem(
                    evidence_id="documents",
                    kind="document_requirement",
                    source="task_contract",
                    summary=(task_contract.document_context[:400] or "存在任务文档约束"),
                    file_refs=list(task_contract.document_paths),
                    confidence=0.8,
                    tags=["document_context"],
                )
            )

        if task_contract.architecture_trigger_reasons:
            items.append(
                EvidenceItem(
                    evidence_id="architecture_trigger",
                    kind="user_constraint",
                    source="task_contract",
                    summary=f"触发原因: {', '.join(task_contract.architecture_trigger_reasons)}",
                    confidence=0.9,
                    tags=list(task_contract.architecture_trigger_reasons),
                )
            )

        if verification_plan and verification_plan.commands:
            items.append(
                EvidenceItem(
                    evidence_id="verification_plan",
                    kind="verification_signal",
                    source="verification_planner",
                    summary="; ".join(item.command for item in verification_plan.commands[:4]),
                    confidence=0.75,
                    tags=["verification"],
                    metadata={"command_count": len(verification_plan.commands)},
                )
            )

        if project_context.strip():
            items.append(
                EvidenceItem(
                    evidence_id="project_context",
                    kind="runtime_artifact",
                    source="autonomous.project_context",
                    summary=project_context[:600],
                    confidence=0.7,
                    tags=["project_context"],
                )
            )
            items.extend(self._signal_items_from_context(project_context))

        return EvidenceGraph(items=items, relations=[])

    def _signal_items_from_context(self, project_context: str) -> list[EvidenceItem]:
        text = project_context.lower()
        signals = [
            (
                "signal_shared_db",
                "migration_signal",
                "检测到共享数据库/共享表信号，迁移需要先定义数据所有权。",
                ("共享数据库", "共享表", "shared database", "shared db", "shared table"),
                ["shared-db", "data-boundary"],
            ),
            (
                "signal_sync_call",
                "migration_signal",
                "检测到同步调用链信号，应先收敛入口再切换边界。",
                ("同步调用", "http 调用", "rpc", "sync call", "synchronous"),
                ["sync-call", "boundary"],
            ),
            (
                "signal_rollout",
                "migration_signal",
                "检测到 rollout/canary/shadow 信号，说明需要切流与回滚护栏。",
                ("灰度", "切流", "canary", "shadow", "rollout"),
                ["rollout", "observability"],
            ),
            (
                "signal_event",
                "migration_signal",
                "检测到事件化信号，可以考虑异步边界重组。",
                ("事件", "消息", "event", "queue", "pub/sub"),
                ["event-driven"],
            ),
        ]
        items: list[EvidenceItem] = []
        for evidence_id, kind, summary, keywords, tags in signals:
            if any(keyword.lower() in text for keyword in keywords):
                items.append(
                    EvidenceItem(
                        evidence_id=evidence_id,
                        kind=kind,
                        source="project_context",
                        summary=summary,
                        confidence=0.72,
                        tags=tags,
                    )
                )
        return items


def evidence_graph_to_dict(graph: EvidenceGraph) -> dict[str, Any]:
    return asdict(graph)


def save_evidence_graph(graph: EvidenceGraph, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(evidence_graph_to_dict(graph), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return target
