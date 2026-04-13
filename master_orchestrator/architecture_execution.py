"""Structured execution evidence for architecture playbooks."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


_NONE_MARKERS = {"", "none", "n/a", "na", "null", "nil", "无", "[]"}


@dataclass
class ArchitectureExecutionReport:
    phase_id: str
    playbook_id: str = ""
    wave_id: str = ""
    step_ids: list[str] = field(default_factory=list)
    status: str = "not_applicable"  # not_applicable | complete | incomplete | blocked
    gate_status: str = "not_applicable"  # not_applicable | ready | blocked
    reported_evidence_refs: list[str] = field(default_factory=list)
    required_evidence_refs: list[str] = field(default_factory=list)
    missing_evidence_refs: list[str] = field(default_factory=list)
    reported_rollback_refs: list[str] = field(default_factory=list)
    required_rollback_refs: list[str] = field(default_factory=list)
    missing_rollback_refs: list[str] = field(default_factory=list)
    cutover_gates: list[str] = field(default_factory=list)
    unmet_cutover_gates: list[str] = field(default_factory=list)
    satisfied_cutover_gates: list[str] = field(default_factory=list)
    report_sources: list[str] = field(default_factory=list)
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


def architecture_execution_report_to_dict(report: ArchitectureExecutionReport) -> dict[str, Any]:
    return asdict(report)


def architecture_execution_report_from_dict(data: dict[str, Any]) -> ArchitectureExecutionReport:
    return ArchitectureExecutionReport(
        phase_id=str(data.get("phase_id", "") or ""),
        playbook_id=str(data.get("playbook_id", "") or ""),
        wave_id=str(data.get("wave_id", "") or ""),
        step_ids=list(data.get("step_ids", [])),
        status=str(data.get("status", "not_applicable") or "not_applicable"),
        gate_status=str(data.get("gate_status", "not_applicable") or "not_applicable"),
        reported_evidence_refs=list(data.get("reported_evidence_refs", [])),
        required_evidence_refs=list(data.get("required_evidence_refs", [])),
        missing_evidence_refs=list(data.get("missing_evidence_refs", [])),
        reported_rollback_refs=list(data.get("reported_rollback_refs", [])),
        required_rollback_refs=list(data.get("required_rollback_refs", [])),
        missing_rollback_refs=list(data.get("missing_rollback_refs", [])),
        cutover_gates=list(data.get("cutover_gates", [])),
        unmet_cutover_gates=list(data.get("unmet_cutover_gates", [])),
        satisfied_cutover_gates=list(data.get("satisfied_cutover_gates", [])),
        report_sources=list(data.get("report_sources", [])),
        summary=str(data.get("summary", "") or ""),
        metadata=dict(data.get("metadata", {})),
    )


def save_architecture_execution_report(report: ArchitectureExecutionReport, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(architecture_execution_report_to_dict(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return target


def load_architecture_execution_report(path: str | Path) -> ArchitectureExecutionReport:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return architecture_execution_report_from_dict(data)


def build_architecture_execution_report(phase: Any, task_outputs: dict[str, Any]) -> ArchitectureExecutionReport | None:
    metadata = getattr(phase, "metadata", {}) or {}
    playbook_steps = list(metadata.get("architecture_playbook_steps") or [])
    if not playbook_steps:
        return None

    gate_scope = str(metadata.get("architecture_gate_scope", "") or "").strip().lower()
    configured_cutover_gates = _dedupe(
        str(item).strip() for item in metadata.get("architecture_cutover_gates", []) if str(item).strip()
    )
    cutover_gates = [] if gate_scope in {"", "none"} else configured_cutover_gates
    required_evidence_refs = _dedupe(
        str(item).strip()
        for step in playbook_steps
        for item in step.get("evidence_required", [])
        if str(item).strip()
    )
    required_rollback_refs = _dedupe(
        str(item).strip()
        for step in playbook_steps
        for item in step.get("rollback_action_ids", [])
        if str(item).strip()
    )

    evidence_refs, evidence_sources = _collect_label_values(task_outputs, "EvidenceRefs")
    rollback_refs, rollback_sources = _collect_label_values(task_outputs, "RollbackRefs")
    unmet_cutover_gates, gate_sources = _collect_label_values(task_outputs, "UnmetCutoverGates")

    if not cutover_gates:
        unmet_cutover_gates = []
    elif not gate_sources:
        unmet_cutover_gates = list(cutover_gates)

    missing_evidence_refs = [item for item in required_evidence_refs if item not in evidence_refs]
    missing_rollback_refs = [item for item in required_rollback_refs if item not in rollback_refs]
    satisfied_cutover_gates = [item for item in cutover_gates if item not in unmet_cutover_gates]

    gate_status = "not_applicable"
    if cutover_gates:
        gate_status = "blocked" if unmet_cutover_gates else "ready"

    if cutover_gates and gate_status == "blocked":
        status = "blocked"
    elif missing_evidence_refs or missing_rollback_refs:
        status = "incomplete"
    else:
        status = "complete"

    report = ArchitectureExecutionReport(
        phase_id=str(getattr(phase, "id", "") or ""),
        playbook_id=str(metadata.get("architecture_execution_playbook_id", "") or ""),
        wave_id=str(metadata.get("architecture_wave_id", "") or ""),
        step_ids=_dedupe(str(step.get("step_id", "")).strip() for step in playbook_steps if str(step.get("step_id", "")).strip()),
        status=status,
        gate_status=gate_status,
        reported_evidence_refs=evidence_refs,
        required_evidence_refs=required_evidence_refs,
        missing_evidence_refs=missing_evidence_refs,
        reported_rollback_refs=rollback_refs,
        required_rollback_refs=required_rollback_refs,
        missing_rollback_refs=missing_rollback_refs,
        cutover_gates=cutover_gates,
        unmet_cutover_gates=unmet_cutover_gates,
        satisfied_cutover_gates=satisfied_cutover_gates,
        report_sources=_dedupe([*evidence_sources, *rollback_sources, *gate_sources]),
        metadata={
            "architecture_gate_scope": gate_scope,
            "playbook_step_count": len(playbook_steps),
        },
    )
    report.summary = render_architecture_execution_summary(report)
    return report


def render_architecture_execution_summary(report: ArchitectureExecutionReport) -> str:
    parts = [f"status={report.status}"]
    if report.gate_status != "not_applicable":
        parts.append(f"gate={report.gate_status}")
    if report.unmet_cutover_gates:
        parts.append("unmet_gates=" + ", ".join(report.unmet_cutover_gates[:4]))
    if report.missing_evidence_refs:
        parts.append("missing_evidence=" + ", ".join(report.missing_evidence_refs[:4]))
    if report.missing_rollback_refs:
        parts.append("missing_rollback=" + ", ".join(report.missing_rollback_refs[:4]))
    if report.report_sources:
        parts.append("sources=" + ", ".join(report.report_sources[:4]))
    return "; ".join(parts)


def _collect_label_values(task_outputs: dict[str, Any], label: str) -> tuple[list[str], list[str]]:
    best_values: list[str] = []
    best_sources: list[str] = []
    best_priority = -1
    best_index = -1
    for index, (task_id, output) in enumerate(task_outputs.items()):
        direct, direct_present = _extract_values_from_object(output, label)
        text = output if isinstance(output, str) else json.dumps(output, ensure_ascii=False, default=str)
        direct.extend(_extract_values_from_text(text, label))
        normalized = _normalize_values(direct)
        if not normalized and not direct_present and not _contains_label(text, label):
            continue
        priority = _label_source_priority(task_id, output)
        if priority > best_priority or (priority == best_priority and index >= best_index):
            best_values = normalized
            best_sources = [str(task_id)]
            best_priority = priority
            best_index = index
    return _dedupe(best_values), _dedupe(best_sources)


def _label_source_priority(task_id: Any, output: Any) -> int:
    task_name = str(task_id or "").strip().lower()
    score = 0

    if task_name.startswith("arch_reconcile_") or "_arch_reconcile_" in task_name:
        score += 400
    if task_name.startswith("task_wave_") or "_task_wave_" in task_name:
        score += 300
    if _is_operation_snapshot(task_name, output):
        score += 200
    if task_name.startswith("playbook_") or "_playbook_" in task_name:
        score += 100

    return score


def _is_operation_snapshot(task_name: str, output: Any) -> bool:
    if isinstance(output, dict) and isinstance(output.get("operation"), dict):
        return True
    return (
        task_name.startswith("arch_refresh_operations_")
        or task_name.endswith("_operation")
        or "_operation_" in task_name
    )


def _extract_values_from_object(output: Any, label: str) -> tuple[list[str], bool]:
    if not isinstance(output, dict):
        return [], False
    results: list[str] = []
    target = label.lower()
    present = False
    for key, value in output.items():
        if str(key).strip().lower() != target:
            continue
        present = True
        if isinstance(value, list):
            results.extend(str(item).strip() for item in value if str(item).strip())
        else:
            results.extend(_split_value_text(str(value)))
    return results, present


def _extract_values_from_text(text: str, label: str) -> list[str]:
    pattern = re.compile(rf"(?im)^\s*{re.escape(label)}\s*[:：]\s*(.+)$")
    results: list[str] = []
    for match in pattern.findall(text or ""):
        results.extend(_split_value_text(match))
    return results


def _contains_label(text: str, label: str) -> bool:
    return bool(re.search(rf"(?im)^\s*{re.escape(label)}\s*[:：]", text or ""))


def _split_value_text(text: str) -> list[str]:
    if not text.strip():
        return []
    return [item.strip() for item in re.split(r"[,;；、\n\r]+", text) if item.strip()]


def _normalize_values(values: list[str]) -> list[str]:
    if not values:
        return []
    lowered = [item.strip() for item in values if item.strip()]
    if lowered and all(item.lower() in _NONE_MARKERS for item in lowered):
        return []
    return _dedupe(lowered)


def _dedupe(values: list[str] | tuple[str, ...] | Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result
