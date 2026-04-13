"""Human-facing one-page execution preview."""

from __future__ import annotations

from dataclasses import dataclass, field

from .task_contract import TaskContract


@dataclass
class ExecutionPreview:
    goal: str
    task_type_label: str
    affected_areas: list[str] = field(default_factory=list)
    document_paths: list[str] = field(default_factory=list)
    data_risk_label: str = "无"
    requires_backup: bool = False
    verification_commands: list[str] = field(default_factory=list)
    estimated_branch_count: int = 1

    @property
    def verification_summary(self) -> str:
        if not self.verification_commands:
            return "未规划验证命令"
        return "；".join(self.verification_commands[:5])

    @classmethod
    def from_contract(
        cls,
        contract: TaskContract,
        verification_commands: list[str] | None = None,
    ) -> "ExecutionPreview":
        risk_label = {
            "none": "无",
            "files": "文件类数据",
            "database": "数据库",
            "both": "数据库 + 文件类数据",
        }[contract.data_risk.value]
        return cls(
            goal=contract.normalized_goal or contract.source_goal,
            task_type_label=contract.task_type_label,
            affected_areas=contract.affected_areas,
            document_paths=contract.document_paths,
            data_risk_label=risk_label,
            requires_backup=contract.requires_backup,
            verification_commands=verification_commands or [],
            estimated_branch_count=contract.estimated_branch_count,
        )

    def render_text(self) -> str:
        lines = [
            "执行摘要",
            f"- 目标: {self.goal}",
            f"- 类型: {self.task_type_label}",
            f"- 影响范围: {', '.join(self.affected_areas) if self.affected_areas else '待自动判断'}",
        ]
        if self.document_paths:
            lines.append(f"- 文档: {', '.join(self.document_paths)}")
        lines.extend([
            f"- 数据风险: {self.data_risk_label}",
            f"- 备份: {'需要备份' if self.requires_backup else '无需备份'}",
            f"- 验证: {self.verification_summary}",
            f"- 本地分支数: {self.estimated_branch_count}",
        ])
        return "\n".join(lines)
