"""Heuristic task classification for preview-first execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .repo_profile import RepoProfile
from .task_contract import TaskType

if TYPE_CHECKING:
    from .task_intake import TaskIntakeRequest


@dataclass
class TaskClassification:
    task_type: TaskType
    confidence: float
    reasons: list[str] = field(default_factory=list)
    affected_areas: list[str] = field(default_factory=list)


class TaskClassifier:
    def classify(self, request: TaskIntakeRequest, profile: RepoProfile) -> TaskClassification:
        text = " ".join([request.goal, *[str(p) for p in request.document_paths]]).lower()
        reasons: list[str] = []

        if any(token in text for token in ("联调", "integration", "e2e", "end-to-end", "对接")):
            task_type = TaskType.INTEGRATION
            reasons.append("命中联调关键词")
        elif any(token in text for token in ("重构", "refactor", "架构", "技术债", "batch")):
            task_type = TaskType.REFACTOR
            reasons.append("命中重构关键词")
        elif any(token in text for token in ("修复", "fix", "bug", "错误", "异常", "500", "问题")):
            task_type = TaskType.BUGFIX
            reasons.append("命中 bug 修复关键词")
        elif request.goal or request.document_paths:
            task_type = TaskType.FEATURE
            reasons.append("默认归类为新增功能/需求")
        else:
            task_type = TaskType.UNKNOWN

        affected_areas = self._infer_affected_areas(text, profile, task_type)
        confidence = 0.9 if reasons else 0.5
        return TaskClassification(
            task_type=task_type,
            confidence=confidence,
            reasons=reasons,
            affected_areas=affected_areas,
        )

    def _infer_affected_areas(self, text: str, profile: RepoProfile, task_type: TaskType) -> list[str]:
        areas: list[str] = []

        backend_tokens = ("后端", "backend", "api", "接口", "service", "controller", "spring", "mybatis")
        frontend_tokens = ("前端", "frontend", "页面", "ui", "react", "组件", "样式", "vite")
        database_tokens = ("数据库", "database", "sql", "表", "迁移", "schema")
        storage_tokens = ("upload", "上传", "文件", "oss", "storage")

        if profile.has_backend and any(token in text for token in backend_tokens):
            areas.append("backend")
        if profile.has_frontend and any(token in text for token in frontend_tokens):
            areas.append("frontend")
        if any(token in text for token in database_tokens):
            areas.append("database")
        if any(token in text for token in storage_tokens):
            areas.append("storage")

        if task_type is TaskType.INTEGRATION and profile.has_backend and profile.has_frontend:
            areas.extend(["backend", "frontend"])

        if not areas:
            if profile.has_backend and not profile.has_frontend:
                areas.append("backend")
            elif profile.has_frontend and not profile.has_backend:
                areas.append("frontend")
            elif profile.has_backend and profile.has_frontend and task_type in (TaskType.FEATURE, TaskType.INTEGRATION):
                areas.extend(["backend", "frontend"])
            elif profile.has_backend:
                areas.append("backend")

        return list(dict.fromkeys(areas))
