"""Default roles for the architecture council."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ArchitectureRole:
    role_id: str
    title: str
    responsibility: str


class RoleRegistry:
    def __init__(self) -> None:
        self._roles = {
            "principal_architect": ArchitectureRole(
                role_id="principal_architect",
                title="Principal Architect",
                responsibility="提出主方案，并给出阶段化落地建议。",
            ),
            "skeptical_architect": ArchitectureRole(
                role_id="skeptical_architect",
                title="Skeptical Architect",
                responsibility="识别隐藏耦合、回滚风险、迁移风险和过度设计。",
            ),
            "data_specialist": ArchitectureRole(
                role_id="data_specialist",
                title="Data Specialist",
                responsibility="关注数据所有权、一致性、迁移和兼容策略。",
            ),
            "platform_specialist": ArchitectureRole(
                role_id="platform_specialist",
                title="Platform Specialist",
                responsibility="关注部署单元、运行约束、依赖和可运维性。",
            ),
            "security_specialist": ArchitectureRole(
                role_id="security_specialist",
                title="Security Specialist",
                responsibility="关注边界变化带来的权限、审计和暴露面风险。",
            ),
            "judge_scribe": ArchitectureRole(
                role_id="judge_scribe",
                title="Judge / Scribe",
                responsibility="收敛结论，生成结构化 ArchitectureContract。",
            ),
        }

    def resolve(self, role_ids: list[str]) -> list[ArchitectureRole]:
        return [self._roles[role_id] for role_id in role_ids if role_id in self._roles]
