"""Automatic deterministic verification planning."""

from __future__ import annotations

from dataclasses import dataclass, field

from .repo_profile import RepoProfile
from .task_contract import TaskContract, TaskType


@dataclass
class VerificationCommand:
    name: str
    command: str
    cwd: str = ""


@dataclass
class VerificationPlan:
    commands: list[VerificationCommand] = field(default_factory=list)
    summary: str = ""


class VerificationPlanner:
    def plan(self, contract: TaskContract, profile: RepoProfile) -> VerificationPlan:
        commands: list[VerificationCommand] = []

        if any(area in contract.affected_areas for area in ("backend", "database", "storage")) and profile.has_backend:
            for command in profile.backend_commands[:2]:
                commands.append(VerificationCommand(name="backend", command=command, cwd=str(profile.backend_dir or profile.root)))

        if "frontend" in contract.affected_areas and profile.has_frontend:
            for command in profile.frontend_commands[:2]:
                commands.append(VerificationCommand(name="frontend", command=command, cwd=str(profile.frontend_dir or profile.root)))

        if contract.task_type is TaskType.INTEGRATION and profile.docker_compose_file:
            commands.append(VerificationCommand(name="compose", command="docker compose config", cwd=str(profile.root)))

        deduped: list[VerificationCommand] = []
        seen: set[tuple[str, str]] = set()
        for item in commands:
            key = (item.command, item.cwd)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)

        return VerificationPlan(
            commands=deduped,
            summary=f"{len(deduped)} 项自动验证",
        )
