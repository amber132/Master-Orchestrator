"""Expand phase raw tasks into an enforced closed-loop task sequence."""

from __future__ import annotations

from dataclasses import replace

from .auto_model import Phase
from .task_contract import TaskContract
from .task_templates import build_contract_hint, build_refactor_execution_rules


class ClosurePlanner:
    def plan_phase(self, phase: Phase, contract: TaskContract) -> Phase:
        tasks: list[dict] = []
        analyze_id = f"analyze_{phase.id}"
        scope_id = f"scope_{phase.id}"
        tasks.append({
            "id": analyze_id,
            "prompt": f"分析阶段目标、已有上下文和风险。{build_contract_hint(contract)}",
            "depends_on": [],
            "timeout": 300,
            "tags": ["phase_analyze", "drift_blocking"],
        })
        scope_prompt = "整理本阶段影响范围、依赖关系、不可改动边界，并明确 out-of-scope。"
        refactor_rules = build_refactor_execution_rules(contract)
        if refactor_rules:
            scope_prompt = f"{scope_prompt}\n\n{refactor_rules}"
        tasks.append({
            "id": scope_id,
            "prompt": scope_prompt,
            "depends_on": [analyze_id],
            "timeout": 300,
            "tags": ["phase_scope", "drift_blocking"],
        })

        anchor_id = scope_id
        if contract.requires_backup:
            backup_id = f"backup_{phase.id}"
            tasks.append({
                "id": backup_id,
                "prompt": "确认备份门禁已完成，并根据备份证据继续实施。",
                "depends_on": [scope_id],
                "timeout": 180,
                "tags": ["phase_backup"],
            })
            anchor_id = backup_id

        implementation_tasks = phase.raw_tasks or [{
            "id": f"implement_{phase.id}_1",
            "prompt": phase.description or phase.name,
            "depends_on": [],
            "timeout": 1800,
            "tags": ["phase_implement"],
        }]

        implementation_ids: list[str] = []
        for raw_task in implementation_tasks:
            task_id = raw_task.get("id") or f"implement_{phase.id}_{len(implementation_ids) + 1}"
            depends_on = list(dict.fromkeys([anchor_id, *raw_task.get("depends_on", [])]))
            prompt = raw_task.get("prompt", phase.description or phase.name)
            raw_tags = [str(tag) for tag in raw_task.get("tags", [])]
            if contract.strict_refactor_mode and refactor_rules:
                prompt = f"{prompt}\n\n{refactor_rules}"
                raw_tags.extend(["strict_refactor", "bounded_slice"])
            tasks.append({
                "id": task_id,
                "prompt": prompt,
                "depends_on": depends_on,
                "timeout": raw_task.get("timeout", 1800),
                "tags": list(dict.fromkeys(raw_tags or ["phase_implement"])),
            })
            implementation_ids.append(task_id)

        if contract.uses_native_phase_closure:
            acceptance = "；".join(phase.acceptance_criteria[:2]).strip()
            if not acceptance:
                acceptance = phase.description or phase.name
            tasks.append({
                "id": f"native_verify_{phase.id}",
                "prompt": (
                    "执行确定性验证，确认当前阶段结果满足验收标准。"
                    f"重点检查：{acceptance}。"
                    "优先使用文件检查、精确匹配和最小语法/测试命令；不要输出交接总结。"
                ),
                "depends_on": implementation_ids,
                "timeout": 600,
                "tags": ["phase_native_verify", "strict_refactor", "drift_blocking"],
            })
            return replace(phase, raw_tasks=tasks)

        verify_id = f"verify_{phase.id}"
        handoff_id = f"handoff_{phase.id}"
        tasks.append({
            "id": verify_id,
            "prompt": "总结当前实现结果、未解决问题和建议运行的验证步骤。",
            "depends_on": implementation_ids,
            "timeout": 300,
            "tags": ["phase_verify", "drift_blocking"],
        })
        tasks.append({
            "id": handoff_id,
            "prompt": "输出本阶段交付摘要、关键改动和后续建议。",
            "depends_on": [verify_id],
            "timeout": 300,
            "tags": ["phase_handoff", "drift_blocking"],
        })
        return replace(phase, raw_tasks=tasks)

    def plan_phases(self, phases: list[Phase], contract: TaskContract) -> list[Phase]:
        return [self.plan_phase(phase, contract) for phase in phases]
