"""Structured artifacts for architecture-aware planning."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CandidateOption:
    option_id: str
    title: str
    summary: str
    pattern_refs: list[str] = field(default_factory=list)
    pros: list[str] = field(default_factory=list)
    cons: list[str] = field(default_factory=list)
    assumptions: list[str] = field(default_factory=list)
    estimated_cost: str = ""
    estimated_risk: str = ""


@dataclass
class WorkPackage:
    package_id: str
    title: str
    objective: str
    scope_in: list[str] = field(default_factory=list)
    scope_out: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)
    preferred_order: int = 0
    planner_hints: dict[str, Any] = field(default_factory=dict)


@dataclass
class VerificationObligation:
    obligation_id: str
    description: str
    category: str
    command_hint: str = ""
    evidence_required: list[str] = field(default_factory=list)
    blocking: bool = True


@dataclass
class RiskItem:
    risk_id: str
    title: str
    severity: str
    likelihood: str
    mitigation: str = ""
    owner: str = ""


@dataclass
class HumanGate:
    gate_id: str
    reason: str
    trigger_condition: str
    required_inputs: list[str] = field(default_factory=list)


@dataclass
class RoleDeliberation:
    role_id: str
    title: str
    stance: str
    summary: str
    key_points: list[str] = field(default_factory=list)
    concerns: list[str] = field(default_factory=list)
    recommended_option_ids: list[str] = field(default_factory=list)
    evidence_refs: list[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class MigrationWave:
    wave_id: str
    title: str
    objective: str
    scope_in: list[str] = field(default_factory=list)
    entry_criteria: list[str] = field(default_factory=list)
    exit_criteria: list[str] = field(default_factory=list)
    rollback_plan: list[str] = field(default_factory=list)
    work_package_ids: list[str] = field(default_factory=list)
    planner_hints: dict[str, Any] = field(default_factory=dict)


@dataclass
class RollbackAction:
    action_id: str
    title: str
    trigger_condition: str
    actions: list[str] = field(default_factory=list)
    command_hints: list[str] = field(default_factory=list)
    evidence_required: list[str] = field(default_factory=list)


@dataclass
class PlaybookStep:
    step_id: str
    wave_id: str
    stage: str
    title: str
    objective: str
    preconditions: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    verification_obligation_ids: list[str] = field(default_factory=list)
    evidence_required: list[str] = field(default_factory=list)
    command_hints: list[str] = field(default_factory=list)
    rollback_action_ids: list[str] = field(default_factory=list)
    blocking: bool = True
    planner_hints: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionPlaybook:
    playbook_id: str
    title: str
    summary: str
    strategy: str = ""
    preconditions: list[str] = field(default_factory=list)
    cutover_gates: list[str] = field(default_factory=list)
    rollback_triggers: list[str] = field(default_factory=list)
    steps: list[PlaybookStep] = field(default_factory=list)
    rollback_actions: list[RollbackAction] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchitectureContract:
    contract_id: str = field(default_factory=lambda: f"arch_{uuid.uuid4().hex[:12]}")
    decision_type: str = ""
    trigger_reasons: list[str] = field(default_factory=list)
    scope_in: list[str] = field(default_factory=list)
    scope_out: list[str] = field(default_factory=list)
    quality_attributes: list[str] = field(default_factory=list)
    candidate_options: list[CandidateOption] = field(default_factory=list)
    selected_option_id: str = ""
    selected_summary: str = ""
    adrs: list[dict[str, Any]] = field(default_factory=list)
    work_packages: list[WorkPackage] = field(default_factory=list)
    verification_obligations: list[VerificationObligation] = field(default_factory=list)
    risk_register: list[RiskItem] = field(default_factory=list)
    human_gates: list[HumanGate] = field(default_factory=list)
    role_deliberations: list[RoleDeliberation] = field(default_factory=list)
    migration_waves: list[MigrationWave] = field(default_factory=list)
    execution_playbook: ExecutionPlaybook | None = None
    planner_hints: dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    dissent_notes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.trigger_reasons = list(dict.fromkeys(self.trigger_reasons))
        self.scope_in = list(dict.fromkeys(self.scope_in))
        self.scope_out = list(dict.fromkeys(self.scope_out))
        self.quality_attributes = list(dict.fromkeys(self.quality_attributes))
        self.dissent_notes = [note.strip() for note in self.dissent_notes if str(note).strip()]


def architecture_contract_to_dict(contract: ArchitectureContract) -> dict[str, Any]:
    return asdict(contract)


def architecture_contract_from_dict(data: dict[str, Any]) -> ArchitectureContract:
    return ArchitectureContract(
        contract_id=data.get("contract_id", ""),
        decision_type=data.get("decision_type", ""),
        trigger_reasons=list(data.get("trigger_reasons", [])),
        scope_in=list(data.get("scope_in", [])),
        scope_out=list(data.get("scope_out", [])),
        quality_attributes=list(data.get("quality_attributes", [])),
        candidate_options=[CandidateOption(**item) for item in data.get("candidate_options", [])],
        selected_option_id=data.get("selected_option_id", ""),
        selected_summary=data.get("selected_summary", ""),
        adrs=list(data.get("adrs", [])),
        work_packages=[WorkPackage(**item) for item in data.get("work_packages", [])],
        verification_obligations=[VerificationObligation(**item) for item in data.get("verification_obligations", [])],
        risk_register=[RiskItem(**item) for item in data.get("risk_register", [])],
        human_gates=[HumanGate(**item) for item in data.get("human_gates", [])],
        role_deliberations=[RoleDeliberation(**item) for item in data.get("role_deliberations", [])],
        migration_waves=[MigrationWave(**item) for item in data.get("migration_waves", [])],
        execution_playbook=_execution_playbook_from_dict(data.get("execution_playbook")),
        planner_hints=dict(data.get("planner_hints", {})),
        confidence=float(data.get("confidence", 0.0) or 0.0),
        dissent_notes=list(data.get("dissent_notes", [])),
        metadata=dict(data.get("metadata", {})),
    )


def save_architecture_contract(contract: ArchitectureContract, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(architecture_contract_to_dict(contract), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return target


def load_architecture_contract(path: str | Path) -> ArchitectureContract:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return architecture_contract_from_dict(data)


def _execution_playbook_from_dict(data: Any) -> ExecutionPlaybook | None:
    if not isinstance(data, dict):
        return None
    return ExecutionPlaybook(
        playbook_id=str(data.get("playbook_id", "") or ""),
        title=str(data.get("title", "") or ""),
        summary=str(data.get("summary", "") or ""),
        strategy=str(data.get("strategy", "") or ""),
        preconditions=list(data.get("preconditions", [])),
        cutover_gates=list(data.get("cutover_gates", [])),
        rollback_triggers=list(data.get("rollback_triggers", [])),
        steps=[PlaybookStep(**item) for item in data.get("steps", [])],
        rollback_actions=[RollbackAction(**item) for item in data.get("rollback_actions", [])],
        metadata=dict(data.get("metadata", {})),
    )


def render_architecture_summary(contract: ArchitectureContract) -> str:
    lines = [
        "# 架构决策摘要",
        "",
        f"- Contract ID: {contract.contract_id}",
        f"- 决策类型: {contract.decision_type or '未声明'}",
        f"- 选定方案: {contract.selected_option_id or '未声明'}",
        f"- 置信度: {contract.confidence:.2f}",
    ]
    if contract.selected_summary:
        lines.extend(["", "## 方案摘要", contract.selected_summary])
    if contract.scope_in:
        lines.extend(["", "## 范围内", *[f"- {item}" for item in contract.scope_in]])
    if contract.scope_out:
        lines.extend(["", "## 范围外", *[f"- {item}" for item in contract.scope_out]])
    if contract.work_packages:
        lines.extend(["", "## 工作包"])
        for item in contract.work_packages:
            lines.append(f"- [{item.package_id}] {item.title}: {item.objective}")
    if contract.migration_waves:
        lines.extend(["", "## 迁移波次"])
        for wave in contract.migration_waves:
            lines.append(f"- [{wave.wave_id}] {wave.title}: {wave.objective}")
    if contract.execution_playbook is not None:
        lines.extend([
            "",
            "## 执行 Playbook",
            f"- 标题: {contract.execution_playbook.title}",
            f"- 策略: {contract.execution_playbook.strategy or '未声明'}",
        ])
        if contract.execution_playbook.cutover_gates:
            lines.append(f"- 切流门槛: {'；'.join(contract.execution_playbook.cutover_gates[:4])}")
        if contract.execution_playbook.rollback_triggers:
            lines.append(f"- 回滚触发: {'；'.join(contract.execution_playbook.rollback_triggers[:4])}")
        if contract.execution_playbook.steps:
            lines.append("- 关键步骤:")
            for step in contract.execution_playbook.steps[:6]:
                lines.append(f"  - [{step.step_id}] {step.title}: {step.objective}")
        if contract.execution_playbook.rollback_actions:
            lines.append("- 回滚动作:")
            for action in contract.execution_playbook.rollback_actions[:4]:
                lines.append(f"  - [{action.action_id}] {action.title}: {action.trigger_condition}")
    if contract.verification_obligations:
        lines.extend(["", "## 验证义务"])
        for item in contract.verification_obligations:
            suffix = f" (`{item.command_hint}`)" if item.command_hint else ""
            lines.append(f"- {item.description}{suffix}")
    if contract.human_gates:
        lines.extend(["", "## 决策门禁"])
        for gate in contract.human_gates:
            lines.append(f"- [{gate.gate_id}] {gate.reason}: {gate.trigger_condition}")
    if contract.role_deliberations:
        lines.extend(["", "## 角色评审"])
        for item in contract.role_deliberations:
            lines.append(f"- [{item.role_id}] {item.title} ({item.stance}): {item.summary}")
    if contract.dissent_notes:
        lines.extend(["", "## 保留意见", *[f"- {item}" for item in contract.dissent_notes]])
    return "\n".join(lines).strip() + "\n"
