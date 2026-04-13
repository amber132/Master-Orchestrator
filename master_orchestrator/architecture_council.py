"""Deterministic architecture council synthesis."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from .architecture_contract import (
    ArchitectureContract,
    CandidateOption,
    ExecutionPlaybook,
    HumanGate,
    MigrationWave,
    PlaybookStep,
    RiskItem,
    RollbackAction,
    RoleDeliberation,
    VerificationObligation,
    WorkPackage,
)
from .architecture_trigger import ArchitectureTriggerDecision
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig
from .evidence_graph import EvidenceGraph
from .json_utils import robust_parse_json
from .model import TaskNode, TaskResult, TaskStatus
from .pattern_registry import ArchitecturePattern, PatternRegistry
from .repo_profile import RepoProfile
from .role_registry import ArchitectureRole, RoleRegistry
from .task_contract import TaskContract


class ArchitectureCouncil:
    def __init__(
        self,
        claude_config: ClaudeConfig | None = None,
        limits_config: LimitsConfig | None = None,
        budget_tracker: BudgetTracker | None = None,
        working_dir: str | None = None,
    ) -> None:
        self._patterns = PatternRegistry()
        self._roles = RoleRegistry()
        self._claude_config = claude_config
        self._limits = limits_config
        self._budget = budget_tracker
        self._working_dir = working_dir

    def deliberate(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        evidence_graph: EvidenceGraph,
        trigger_decision: ArchitectureTriggerDecision,
        project_context: str = "",
    ) -> ArchitectureContract:
        pattern = self._select_pattern(task_contract, trigger_decision)
        fallback = self._patterns.get("modular_monolith_boundary_cleanup")
        candidate_patterns = [item for item in [pattern, fallback] if item is not None]
        deduped_patterns: list[ArchitecturePattern] = []
        seen_pattern_ids: set[str] = set()
        for item in candidate_patterns:
            if item.pattern_id in seen_pattern_ids:
                continue
            seen_pattern_ids.add(item.pattern_id)
            deduped_patterns.append(item)
        candidate_patterns = deduped_patterns
        evidence_text = self._build_evidence_text(task_contract, evidence_graph, project_context)
        evidence_tags = self._collect_evidence_tags(evidence_graph, evidence_text)
        candidate_options = [
            self._candidate_for_pattern(item, task_contract, evidence_tags) for item in candidate_patterns
        ]
        selected_option = self._select_option(candidate_options, evidence_tags)
        selected_pattern = self._pattern_for_option(selected_option, candidate_patterns) or pattern or fallback
        role_specs = self._roles.resolve(trigger_decision.suggested_roles)
        advisory_metadata: dict[str, Any] = {"deliberation_mode": "deterministic", "board_used": False}
        advisory_role_deliberations = self._run_advisory_board(
            task_contract,
            role_specs,
            candidate_options,
            evidence_graph,
            project_context,
        )
        if advisory_role_deliberations:
            advisory_metadata["deliberation_mode"] = "advisory"
            advisory_metadata["board_used"] = True
            selected_option = self._select_option_from_role_votes(candidate_options, advisory_role_deliberations, selected_option)
            selected_pattern = self._pattern_for_option(selected_option, candidate_patterns) or selected_pattern

        risks = self._build_risks(task_contract, selected_pattern, evidence_tags)
        role_deliberations = advisory_role_deliberations or self._build_role_deliberations(
            role_specs,
            selected_option,
            selected_pattern,
            evidence_graph,
            evidence_tags,
            risks,
        )
        human_gates = self._build_human_gates(task_contract, selected_pattern, evidence_tags)
        migration_waves = self._build_migration_waves(task_contract, selected_pattern, evidence_tags)
        work_packages = self._build_work_packages(task_contract, migration_waves)
        obligations = self._build_verification_obligations(
            task_contract,
            repo_profile,
            selected_pattern,
            evidence_tags,
        )
        execution_playbook = self._build_execution_playbook(
            task_contract,
            repo_profile,
            selected_pattern,
            evidence_tags,
            migration_waves,
            obligations,
            human_gates,
        )

        return ArchitectureContract(
            decision_type="boundary_redefinition",
            trigger_reasons=list(trigger_decision.reasons),
            scope_in=list(task_contract.affected_areas),
            scope_out=["超出当前工作包的跨域发布与生产切换"],
            quality_attributes=list(selected_pattern.quality_attributes if selected_pattern else ["change_isolation", "rollback"]),
            candidate_options=candidate_options,
            selected_option_id=selected_option.option_id,
            selected_summary=selected_option.summary,
            adrs=[
                {
                    "title": f"选择 {selected_pattern.title if selected_pattern else 'Incremental Boundary Cleanup'}",
                    "context": task_contract.normalized_goal,
                    "decision": selected_option.summary,
                }
            ],
            work_packages=work_packages,
            verification_obligations=obligations,
            risk_register=risks,
            human_gates=human_gates,
            role_deliberations=role_deliberations,
            migration_waves=migration_waves,
            execution_playbook=execution_playbook,
            planner_hints={
                "deterministic_phase_builder": True,
                "phase_source": "migration_waves" if migration_waves else "work_packages",
                "execution_playbook_present": execution_playbook is not None,
            },
            confidence=max(trigger_decision.confidence, min(0.98, selected_option.metadata_score)),
            dissent_notes=self._build_dissent_notes(selected_pattern, task_contract, evidence_tags),
            metadata={
                "selected_pattern": selected_pattern.pattern_id if selected_pattern else "modular_monolith_boundary_cleanup",
                "evidence_count": len(evidence_graph.items),
                "evidence_tags": sorted(evidence_tags),
                "roles": [asdict(item) for item in role_specs],
                "project_context_excerpt": project_context[:240],
                **advisory_metadata,
            },
        )

    def _select_pattern(
        self,
        task_contract: TaskContract,
        trigger_decision: ArchitectureTriggerDecision,
    ) -> ArchitecturePattern | None:
        preferred = str(task_contract.metadata.get("preferred_architecture_pattern", "") or "").strip()
        if preferred:
            return self._patterns.get(preferred)
        for pattern_id in trigger_decision.suggested_patterns:
            pattern = self._patterns.get(pattern_id)
            if pattern is not None:
                return pattern
        return self._patterns.get("modular_monolith_boundary_cleanup")

    def _build_evidence_text(
        self,
        task_contract: TaskContract,
        evidence_graph: EvidenceGraph,
        project_context: str,
    ) -> str:
        parts = [
            task_contract.source_goal,
            task_contract.normalized_goal,
            task_contract.document_context,
            project_context,
        ]
        parts.extend(task_contract.document_briefs)
        parts.extend(item.summary for item in evidence_graph.items)
        return " ".join(part for part in parts if part).lower()

    def _collect_evidence_tags(self, evidence_graph: EvidenceGraph, evidence_text: str) -> set[str]:
        tags = {tag for item in evidence_graph.items for tag in item.tags}
        signals = {
            "shared-db": ("共享数据库", "共享表", "shared database", "shared db", "shared table"),
            "shared-library": ("共享库", "shared library", "shared module"),
            "sync-call": ("同步调用", "http 调用", "rpc", "synchronous", "sync call"),
            "event-driven": ("事件", "消息", "event", "queue", "pub/sub"),
            "compatibility": ("兼容", "facade", "adapter", "双路径", "dual path"),
            "rollout": ("灰度", "切流", "canary", "shadow", "rollout"),
            "observability": ("监控", "观测", "trace", "metrics", "slo"),
        }
        for tag, keywords in signals.items():
            if any(keyword in evidence_text for keyword in keywords):
                tags.add(tag)
        return tags

    def _candidate_for_pattern(
        self,
        pattern: ArchitecturePattern,
        task_contract: TaskContract,
        evidence_tags: set[str],
    ) -> CandidateOption:
        score = 0.56
        if pattern.pattern_id == "service_extraction":
            score += 0.12
            if "shared-db" in evidence_tags:
                score -= 0.03
        if pattern.pattern_id == "strangler_migration":
            score += 0.1
            if {"shared-db", "compatibility"} & evidence_tags:
                score += 0.05
        if pattern.pattern_id == "api_facade" and "compatibility" in evidence_tags:
            score += 0.1
        if pattern.pattern_id == "event_driven_reorganization" and "event-driven" in evidence_tags:
            score += 0.12
        if task_contract.touches_database and pattern.pattern_id in {"strangler_migration", "api_facade"}:
            score += 0.03
        if pattern.pattern_id == "modular_monolith_boundary_cleanup" and task_contract.task_type.value == "refactor":
            score += 0.05

        option = CandidateOption(
            option_id=f"opt_{pattern.pattern_id}",
            title=pattern.title,
            summary=f"{pattern.summary} 当前目标优先关注 `{task_contract.normalized_goal}` 的增量落地与验证闭环。",
            pattern_refs=[pattern.pattern_id],
            pros=list(pattern.quality_attributes),
            cons=list(pattern.default_risks),
            assumptions=["现有验证入口可作为回归基线", "先收敛边界，再扩大改动范围"],
            estimated_cost="medium",
            estimated_risk="medium",
        )
        option.metadata_score = round(min(0.98, score), 3)  # type: ignore[attr-defined]
        return option

    def _select_option(
        self,
        candidate_options: list[CandidateOption],
        evidence_tags: set[str],
    ) -> CandidateOption:
        if not candidate_options:
            option = CandidateOption(
                option_id="opt_default",
                title="Default Incremental Boundary Cleanup",
                summary="先缩小边界，再逐步推进实现。",
            )
            option.metadata_score = 0.6  # type: ignore[attr-defined]
            return option

        ranked = sorted(
            candidate_options,
            key=lambda item: (
                -float(getattr(item, "metadata_score", 0.0)),
                item.option_id,
            ),
        )
        return ranked[0]

    def _select_option_from_role_votes(
        self,
        candidate_options: list[CandidateOption],
        role_deliberations: list[RoleDeliberation],
        fallback: CandidateOption,
    ) -> CandidateOption:
        option_map = {item.option_id: item for item in candidate_options}
        vote_scores: dict[str, float] = {}
        for item in role_deliberations:
            for option_id in item.recommended_option_ids:
                if option_id not in option_map:
                    continue
                vote_scores[option_id] = vote_scores.get(option_id, 0.0) + max(item.confidence, 0.1)
        if not vote_scores:
            return fallback
        ranked = sorted(
            vote_scores.items(),
            key=lambda entry: (
                -entry[1],
                -float(getattr(option_map[entry[0]], "metadata_score", 0.0)),
                entry[0],
            ),
        )
        return option_map.get(ranked[0][0], fallback)

    def _pattern_for_option(
        self,
        option: CandidateOption,
        candidates: list[ArchitecturePattern],
    ) -> ArchitecturePattern | None:
        pattern_ids = set(option.pattern_refs)
        for pattern in candidates:
            if pattern.pattern_id in pattern_ids:
                return pattern
        return candidates[0] if candidates else None

    def _build_role_deliberations(
        self,
        role_specs: list[ArchitectureRole],
        selected_option: CandidateOption,
        selected_pattern: ArchitecturePattern | None,
        evidence_graph: EvidenceGraph,
        evidence_tags: set[str],
        risks: list[RiskItem],
    ) -> list[RoleDeliberation]:
        pattern_id = selected_pattern.pattern_id if selected_pattern else "modular_monolith_boundary_cleanup"
        risk_titles = [risk.title for risk in risks[:3]]
        evidence_refs = [item.evidence_id for item in evidence_graph.items[:4]]
        result: list[RoleDeliberation] = []
        for role in role_specs:
            stance = "support"
            summary = f"支持 `{pattern_id}`，要求按最小切片推进。"
            key_points = [role.responsibility]
            concerns: list[str] = []
            if role.role_id == "skeptical_architect":
                stance = "conditional"
                summary = "接受增量方案，但只接受先边界收敛、后实现迁移。"
                concerns.extend(risk_titles[:2])
            elif role.role_id == "data_specialist":
                stance = "conditional"
                summary = "只有在数据所有权、写路径和回填策略明确后才支持继续推进。"
                concerns.extend(
                    [item for item in ["共享数据库/共享表需要先解耦", "迁移验证必须覆盖回滚与兼容路径"] if item]
                )
                if "shared-db" in evidence_tags:
                    key_points.append("检测到 shared-db 信号，优先采用渐进迁移而不是一步切换。")
            elif role.role_id == "platform_specialist":
                stance = "conditional"
                summary = "支持方案，但 rollout、观测和回滚演练必须前置。"
                concerns.extend(["缺少 rollout 护栏时不得直接切流"])
            elif role.role_id == "security_specialist":
                stance = "conditional"
                summary = "边界变化会扩大暴露面，权限和审计边界必须同步收敛。"
                concerns.extend(["新边界的认证、授权和审计策略需要同步定义"])
            elif role.role_id == "judge_scribe":
                summary = f"裁决选用 `{selected_option.option_id}`，并要求沿迁移波次逐步交付。"
                key_points.append("判定工作包以 migration waves 为唯一执行顺序。")

            result.append(
                RoleDeliberation(
                    role_id=role.role_id,
                    title=role.title,
                    stance=stance,
                    summary=summary,
                    key_points=key_points[:4],
                    concerns=concerns[:4],
                    recommended_option_ids=[selected_option.option_id],
                    evidence_refs=evidence_refs,
                    confidence=0.74 if stance == "conditional" else 0.82,
                )
            )
        return result

    def _run_advisory_board(
        self,
        task_contract: TaskContract,
        role_specs: list[ArchitectureRole],
        candidate_options: list[CandidateOption],
        evidence_graph: EvidenceGraph,
        project_context: str,
    ) -> list[RoleDeliberation]:
        mode = str(task_contract.metadata.get("architecture_deliberation_mode", "") or "").strip().lower()
        if mode not in {"advisory", "multi_agent"}:
            return []
        if self._claude_config is None or self._limits is None:
            return []

        result: list[RoleDeliberation] = []
        transcript: list[str] = []
        advisory_model = self._select_advisory_model()
        for role in role_specs:
            prompt = self._build_role_prompt(role, task_contract, candidate_options, evidence_graph, project_context, transcript)
            task = TaskNode(
                id=f"_architecture_role_{role.role_id}",
                prompt_template=prompt,
                timeout=300,
                model=advisory_model,
                max_turns=1,
                tags=["architecture_board", role.role_id],
            )
            task_result = run_claude_task(
                task=task,
                prompt=prompt,
                claude_config=self._claude_config,
                limits=self._limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
            )
            deliberation = self._parse_role_deliberation(task_result, role, candidate_options)
            if deliberation is None:
                continue
            result.append(deliberation)
            transcript.append(f"{deliberation.title}({deliberation.stance}): {deliberation.summary}")
        return result

    def _select_advisory_model(self) -> str:
        default_model = str(self._claude_config.default_model)
        if default_model == "opus":
            return "sonnet"
        return default_model

    def _build_role_prompt(
        self,
        role: ArchitectureRole,
        task_contract: TaskContract,
        candidate_options: list[CandidateOption],
        evidence_graph: EvidenceGraph,
        project_context: str,
        transcript: list[str],
    ) -> str:
        evidence_lines = [
            f"- [{item.evidence_id}] {item.summary}"
            for item in evidence_graph.items[:8]
        ] or ["- 无结构化证据"]
        option_lines = [
            f"- {item.option_id}: {item.title} | pros={', '.join(item.pros[:3]) or 'n/a'} | cons={', '.join(item.cons[:3]) or 'n/a'}"
            for item in candidate_options
        ] or ["- opt_default: Default Incremental Boundary Cleanup"]
        transcript_block = "\n".join(f"- {line}" for line in transcript[-4:]) if transcript else "- 暂无前序意见"
        return (
            "你是架构评审委员会的一员。请只输出 JSON。\n"
            "JSON schema:\n"
            "{"
            '"stance":"support|conditional|oppose",'
            '"summary":"...",'
            '"key_points":["..."],'
            '"concerns":["..."],'
            '"recommended_option_ids":["opt_x"],'
            '"confidence":0.0'
            "}\n\n"
            "# 评审约束\n"
            "- 只允许基于下方提供的项目上下文、候选方案、证据和已有评审意见做判断。\n"
            "- 默认不要重新扫描仓库、不要递归列目录、不要运行构建/测试、不要做额外环境探测。\n"
            "- 如果证据不足，直接在 concerns 中写明缺口，并基于保守假设给出结论。\n"
            "- 目标是在单轮内给出 JSON 结论，不要输出解释性前后文。\n\n"
            f"# 角色\n- {role.title}\n- 职责: {role.responsibility}\n\n"
            f"# 目标\n{task_contract.normalized_goal}\n\n"
            f"# 项目上下文\n{project_context[:1600] or '无'}\n\n"
            "# 候选方案\n"
            + "\n".join(option_lines)
            + "\n\n# 证据\n"
            + "\n".join(evidence_lines)
            + "\n\n# 已有评审意见\n"
            + transcript_block
            + "\n\n请从该角色立场给出推荐。"
        )

    def _parse_role_deliberation(
        self,
        task_result: TaskResult,
        role: ArchitectureRole,
        candidate_options: list[CandidateOption],
    ) -> RoleDeliberation | None:
        if task_result.status is not TaskStatus.SUCCESS:
            return None
        payload = task_result.parsed_output
        if not isinstance(payload, dict):
            try:
                payload = robust_parse_json(task_result.output or "")
            except Exception:
                return None
        if not isinstance(payload, dict):
            return None

        valid_option_ids = {item.option_id for item in candidate_options}
        recommended_option_ids = [
            str(item).strip()
            for item in payload.get("recommended_option_ids", [])
            if str(item).strip() in valid_option_ids
        ]
        return RoleDeliberation(
            role_id=role.role_id,
            title=role.title,
            stance=str(payload.get("stance", "conditional") or "conditional").strip().lower(),
            summary=str(payload.get("summary", "") or "").strip() or role.responsibility,
            key_points=[str(item).strip() for item in payload.get("key_points", []) if str(item).strip()][:5],
            concerns=[str(item).strip() for item in payload.get("concerns", []) if str(item).strip()][:5],
            recommended_option_ids=recommended_option_ids,
            evidence_refs=[],
            confidence=float(payload.get("confidence", 0.65) or 0.65),
        )

    def _build_migration_waves(
        self,
        task_contract: TaskContract,
        pattern: ArchitecturePattern | None,
        evidence_tags: set[str],
    ) -> list[MigrationWave]:
        pattern_id = pattern.pattern_id if pattern else "modular_monolith_boundary_cleanup"
        affected = list(task_contract.affected_areas)
        waves: list[MigrationWave] = [
            MigrationWave(
                wave_id="wave_boundary",
                title="Boundary And Entry Convergence",
                objective="先收敛入口、门面和依赖方向，不直接做一次性拆分。",
                scope_in=affected,
                entry_criteria=["基线验证可执行", "明确最小迁移切片"],
                exit_criteria=["新增单一入口或 facade/adapter", "禁止新增跨边界直连"],
                rollback_plan=["保留旧入口调用路径", "适配层可回指旧实现"],
                work_package_ids=["wp_wave_boundary"],
                planner_hints={
                    "tasks": [
                        {
                            "id": "task_wave_boundary_entry",
                            "prompt": "建立单一入口或 facade/adapter，收敛跨模块调用边界。",
                            "timeout": 1800,
                            "tags": ["architecture_wave", "boundary"],
                        }
                    ]
                },
            )
        ]

        if pattern_id in {"service_extraction", "strangler_migration", "api_facade"}:
            waves.append(
                MigrationWave(
                    wave_id="wave_contract",
                    title="Compatibility Contract",
                    objective="补齐兼容契约、双路径护栏或 shadow 校验，保证旧调用方仍能工作。",
                    scope_in=["integration", *affected],
                    entry_criteria=["边界入口已收敛"],
                    exit_criteria=["契约验证通过", "兼容层覆盖旧入口"],
                    rollback_plan=["保持旧协议可用", "切换失败时回退到旧路由"],
                    work_package_ids=["wp_wave_contract"],
                    planner_hints={
                        "tasks": [
                            {
                                "id": "task_wave_contract_guardrail",
                                "prompt": "补齐契约验证与双路径护栏，确保旧调用方和新边界同时可用。",
                                "timeout": 1800,
                                "tags": ["architecture_wave", "contract"],
                            }
                        ]
                    },
                )
            )

        if task_contract.touches_database or "shared-db" in evidence_tags:
            waves.append(
                MigrationWave(
                    wave_id="wave_data",
                    title="Data Ownership Split",
                    objective="明确写路径、数据所有权和回填/回滚策略，避免共享库表直接扩散。",
                    scope_in=["database", *affected],
                    entry_criteria=["兼容层和契约护栏已就绪"],
                    exit_criteria=["写路径归属明确", "数据边界验证通过"],
                    rollback_plan=["保留旧写路径兜底", "迁移失败可停止切换并回退"],
                    work_package_ids=["wp_wave_data"],
                    planner_hints={
                        "tasks": [
                            {
                                "id": "task_wave_data_ownership",
                                "prompt": "梳理写路径和数据所有权，补齐最小迁移与回滚护栏。",
                                "timeout": 1800,
                                "tags": ["architecture_wave", "data-boundary"],
                            }
                        ]
                    },
                )
            )

        waves.append(
            MigrationWave(
                wave_id="wave_cutover",
                title="Cutover Readiness",
                objective="只在 rollout、观测和回滚条件就绪后推进最小切流。",
                scope_in=affected,
                entry_criteria=["前置波次全部通过", "验证义务齐备"],
                exit_criteria=["完成最小切流准备", "保留回滚与 smoke 验证脚本"],
                rollback_plan=["保留旧流量入口", "切流失败直接回退到旧路径"],
                work_package_ids=["wp_wave_cutover"],
                planner_hints={
                    "tasks": [
                        {
                            "id": "task_wave_cutover_readiness",
                            "prompt": "补齐 rollout、观测和回滚清单，只为最小切片准备切换，不扩大战线。",
                            "timeout": 1800,
                            "tags": ["architecture_wave", "cutover"],
                        }
                    ]
                },
            )
        )

        return waves

    def _build_work_packages(
        self,
        task_contract: TaskContract,
        migration_waves: list[MigrationWave],
    ) -> list[WorkPackage]:
        packages: list[WorkPackage] = []
        previous_package_id = ""
        for index, wave in enumerate(migration_waves, start=1):
            package_id = wave.work_package_ids[0] if wave.work_package_ids else f"wp_{wave.wave_id}"
            dependencies = [previous_package_id] if previous_package_id else []
            packages.append(
                WorkPackage(
                    package_id=package_id,
                    title=wave.title,
                    objective=wave.objective,
                    scope_in=list(wave.scope_in or task_contract.affected_areas),
                    scope_out=["一次性大规模迁移", "同时推进多个服务族"],
                    dependencies=dependencies,
                    preferred_order=index,
                    planner_hints=dict(wave.planner_hints),
                )
            )
            previous_package_id = package_id
        return packages

    def _build_execution_playbook(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        pattern: ArchitecturePattern | None,
        evidence_tags: set[str],
        migration_waves: list[MigrationWave],
        obligations: list[VerificationObligation],
        human_gates: list[HumanGate],
    ) -> ExecutionPlaybook | None:
        if not migration_waves:
            return None

        rollback_actions = self._build_rollback_actions(task_contract, repo_profile, pattern, evidence_tags)
        obligation_map = {item.obligation_id: item for item in obligations}
        rollback_map = {item.action_id: item for item in rollback_actions}
        baseline_commands = self._baseline_commands(repo_profile)
        backup_commands = self._backup_commands(task_contract, repo_profile)
        cutover_gates = self._cutover_gates(task_contract, repo_profile, evidence_tags, obligations, human_gates)
        playbook_steps: list[PlaybookStep] = []

        for wave in migration_waves:
            stage = self._wave_stage(wave.wave_id)
            obligation_ids = self._step_obligation_ids(stage, obligation_map, evidence_tags)
            rollback_ids = self._step_rollback_action_ids(stage, rollback_map)
            gate_inputs = self._wave_gate_inputs(wave, human_gates)

            actions = self._step_actions(stage, evidence_tags, pattern, repo_profile)
            command_hints = self._step_command_hints(
                stage,
                repo_profile,
                baseline_commands=baseline_commands,
                backup_commands=backup_commands,
            )
            evidence_required = list(
                dict.fromkeys(
                    [*gate_inputs, *wave.exit_criteria, *self._obligation_evidence(obligation_ids, obligation_map)]
                )
            )
            operation_task = self._build_operation_task(
                wave=wave,
                stage=stage,
                rollback_ids=rollback_ids,
                baseline_commands=baseline_commands,
                backup_commands=backup_commands,
                repo_profile=repo_profile,
                cutover_gates=cutover_gates,
            )
            cli_task = {
                "id": f"playbook_{wave.wave_id}",
                "prompt": self._render_playbook_task_prompt(
                    wave=wave,
                    stage=stage,
                    actions=actions,
                    command_hints=command_hints,
                    gate_inputs=gate_inputs,
                    rollback_ids=rollback_ids,
                ),
                "timeout": 1800,
                "tags": ["architecture_playbook", stage],
            }
            if operation_task is not None:
                cli_task["depends_on"] = [operation_task["id"]]
            planner_tasks = [item for item in [operation_task, cli_task] if item is not None]
            playbook_steps.append(
                PlaybookStep(
                    step_id=f"step_{wave.wave_id}",
                    wave_id=wave.wave_id,
                    stage=stage,
                    title=wave.title,
                    objective=wave.objective,
                    preconditions=list(dict.fromkeys([*wave.entry_criteria, *gate_inputs])),
                    actions=actions,
                    verification_obligation_ids=obligation_ids,
                    evidence_required=evidence_required,
                    command_hints=command_hints,
                    rollback_action_ids=rollback_ids,
                    planner_hints={"tasks": planner_tasks},
                )
            )

        selected_pattern = pattern.pattern_id if pattern else "modular_monolith_boundary_cleanup"
        rollback_triggers = self._rollback_triggers(task_contract, evidence_tags)
        return ExecutionPlaybook(
            playbook_id=f"playbook_{selected_pattern}",
            title="Incremental Migration Execution Playbook",
            summary="把边界收敛、兼容验证、数据迁移、切流与回滚护栏编排成按波次执行的闭环。",
            strategy=selected_pattern,
            preconditions=self._playbook_preconditions(task_contract, repo_profile, human_gates),
            cutover_gates=cutover_gates,
            rollback_triggers=rollback_triggers,
            steps=playbook_steps,
            rollback_actions=rollback_actions,
            metadata={
                "uses_backup": task_contract.requires_backup,
                "has_compose_topology": bool(repo_profile and repo_profile.docker_compose_file),
                "evidence_tags": sorted(evidence_tags),
            },
        )

    def _build_operation_task(
        self,
        wave: MigrationWave,
        stage: str,
        rollback_ids: list[str],
        baseline_commands: list[str],
        backup_commands: list[str],
        repo_profile: RepoProfile | None,
        cutover_gates: list[str],
    ) -> dict[str, Any] | None:
        commands = self._build_operation_commands(
            wave=wave,
            stage=stage,
            baseline_commands=baseline_commands,
            backup_commands=backup_commands,
            repo_profile=repo_profile,
        )
        if not commands:
            return None
        return {
            "id": f"playbook_{wave.wave_id}_operation",
            "prompt": f"执行 `{wave.title}` 的结构化操作检查并产出证据。",
            "type": "operation",
            "output_format": "json",
            "timeout": 1200 if stage == "data" else 900,
            "tags": ["architecture_playbook", stage, "operation"],
            "executor_config": {
                "mode": "architecture_playbook",
                "wave_id": wave.wave_id,
                "rollback_refs": list(rollback_ids),
                "cutover_gates": list(cutover_gates) if stage == "cutover" else [],
                "commands": commands,
            },
        }

    def _build_operation_commands(
        self,
        wave: MigrationWave,
        stage: str,
        baseline_commands: list[str],
        backup_commands: list[str],
        repo_profile: RepoProfile | None,
    ) -> list[dict[str, Any]]:
        commands: list[dict[str, Any]] = []
        seen_commands: set[str] = set()

        def _append(command: str, payload: dict[str, Any]) -> None:
            normalized = str(command).strip()
            if not normalized or normalized in seen_commands:
                return
            seen_commands.add(normalized)
            item = dict(payload)
            item["command"] = normalized
            commands.append(item)

        if baseline_commands:
            payload: dict[str, Any] = {
                "id": "baseline",
                "name": "Baseline Regression",
                "evidence_refs": ["baseline_pass"],
            }
            if stage == "cutover":
                payload["satisfies_gates"] = ["基线回归验证通过。"]
            _append(baseline_commands[0], payload)

        if stage in {"contract", "cutover"} and len(baseline_commands) > 1:
            payload = {
                "id": "contract_guard",
                "name": "Contract Compatibility",
                "evidence_refs": ["contract_compatibility"],
            }
            if stage == "cutover":
                payload["satisfies_gates"] = ["兼容契约/双路径验证通过。"]
            _append(baseline_commands[1], payload)

        if stage == "data" and backup_commands:
            _append(
                backup_commands[0],
                {
                    "id": "backup_snapshot",
                    "name": "Backup Snapshot",
                    "output_file": "evidence/operation_backups/{task_id}_{command_id}.artifact",
                    "evidence_refs": ["backup_snapshot"],
                },
            )

        if stage == "cutover" and repo_profile and repo_profile.docker_compose_file:
            _append(
                "docker compose config",
                {
                    "id": "compose_config",
                    "name": "Compose Config Dry Run",
                    "evidence_refs": ["rollout_checklist"],
                    "satisfies_gates": ["部署配置 dry-run 通过。"],
                },
            )

        if stage == "cutover" and len(baseline_commands) > 2:
            _append(
                baseline_commands[2],
                {
                    "id": "smoke_check",
                    "name": "Smoke Check",
                    "evidence_refs": ["smoke_checks"],
                },
            )

        return commands

    def _build_rollback_actions(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        pattern: ArchitecturePattern | None,
        evidence_tags: set[str],
    ) -> list[RollbackAction]:
        actions = [
            RollbackAction(
                action_id="rollback_boundary",
                title="恢复旧入口和旧实现路径",
                trigger_condition="边界收敛后回归失败，或新 facade/adapter 破坏旧调用路径",
                actions=[
                    "保留并重新启用旧入口路由。",
                    "让适配层回指旧实现，停止继续扩大改动面。",
                ],
                command_hints=self._baseline_commands(repo_profile)[:1],
                evidence_required=["baseline_pass", "legacy_route_enabled"],
            )
        ]
        if pattern and pattern.pattern_id in {"service_extraction", "strangler_migration", "api_facade"}:
            actions.append(
                RollbackAction(
                    action_id="rollback_contract",
                    title="撤回兼容层切换",
                    trigger_condition="契约校验、shadow 验证或调用方兼容性检查失败",
                    actions=[
                        "恢复旧协议或旧路由优先级。",
                        "停止双路径切换，保留旧调用链作为主路径。",
                    ],
                    command_hints=self._baseline_commands(repo_profile)[:2],
                    evidence_required=["contract_compatibility", "rollback_ready"],
                )
            )
        if task_contract.touches_database or "shared-db" in evidence_tags:
            actions.append(
                RollbackAction(
                    action_id="rollback_data_path",
                    title="回退数据写路径和迁移切片",
                    trigger_condition="双写、回填、读写归属验证失败，或发现数据一致性风险",
                    actions=[
                        "停止新写路径或双写开关。",
                        "恢复旧写路径为唯一事实来源。",
                        "必要时使用最近一次备份证据进行人工恢复。",
                    ],
                    command_hints=self._backup_commands(task_contract, repo_profile),
                    evidence_required=["ownership_map", "rollback_strategy", "backup_snapshot"],
                )
            )
        if {"rollout", "compose", "deployment"} & evidence_tags or (repo_profile and repo_profile.docker_compose_file):
            actions.append(
                RollbackAction(
                    action_id="rollback_cutover",
                    title="撤回最小切流",
                    trigger_condition="smoke、指标、trace 或 canary 结果恶化",
                    actions=[
                        "将流量切回旧入口或旧部署单元。",
                        "保留问题样本与观测证据，暂停继续 rollout。",
                    ],
                    command_hints=self._step_command_hints(
                        "cutover",
                        repo_profile,
                        baseline_commands=self._baseline_commands(repo_profile),
                        backup_commands=self._backup_commands(task_contract, repo_profile),
                    ),
                    evidence_required=["smoke_checks", "smoke_metrics", "rollback_ready"],
                )
            )
        return actions

    def _baseline_commands(self, repo_profile: RepoProfile | None) -> list[str]:
        if repo_profile is None:
            return []
        commands: list[str] = []
        commands.extend(repo_profile.backend_commands[:2])
        commands.extend(command for command in repo_profile.frontend_commands[:1] if command not in commands)
        commands.extend(command for command in repo_profile.verification_commands[:2] if command not in commands)
        return commands

    def _backup_commands(self, task_contract: TaskContract, repo_profile: RepoProfile | None) -> list[str]:
        commands: list[str] = []
        if repo_profile is not None:
            commands.extend(repo_profile.database_backup_commands[:1])
        if task_contract.requires_backup and not commands:
            commands.append("准备可回放的数据库或文件快照备份证据")
        return commands

    def _wave_stage(self, wave_id: str) -> str:
        normalized = wave_id.lower()
        if "boundary" in normalized:
            return "boundary"
        if "contract" in normalized:
            return "contract"
        if "data" in normalized:
            return "data"
        if "cutover" in normalized:
            return "cutover"
        return "migration"

    def _step_obligation_ids(
        self,
        stage: str,
        obligation_map: dict[str, VerificationObligation],
        evidence_tags: set[str],
    ) -> list[str]:
        order: list[str] = ["obl_regression"]
        if stage in {"contract", "cutover"} and "obl_contract" in obligation_map:
            order.append("obl_contract")
        if stage == "data" and "obl_data_boundary" in obligation_map:
            order.append("obl_data_boundary")
        if stage == "cutover" and "obl_cutover" in obligation_map:
            order.append("obl_cutover")
        if stage == "cutover" and "obl_observability" in obligation_map and {"rollout", "observability"} & evidence_tags:
            order.append("obl_observability")
        return [item for item in dict.fromkeys(order) if item in obligation_map]

    def _step_rollback_action_ids(
        self,
        stage: str,
        rollback_map: dict[str, RollbackAction],
    ) -> list[str]:
        mapping = {
            "boundary": ["rollback_boundary"],
            "contract": ["rollback_boundary", "rollback_contract"],
            "data": ["rollback_contract", "rollback_data_path"],
            "cutover": ["rollback_cutover", "rollback_contract", "rollback_data_path"],
            "migration": ["rollback_boundary"],
        }
        return [item for item in mapping.get(stage, ["rollback_boundary"]) if item in rollback_map]

    def _wave_gate_inputs(self, wave: MigrationWave, human_gates: list[HumanGate]) -> list[str]:
        gate_inputs: list[str] = []
        for gate in human_gates:
            if "data" in wave.wave_id and gate.gate_id == "gate_data_ownership":
                gate_inputs.extend(gate.required_inputs or [gate.reason])
            elif "cutover" in wave.wave_id and gate.gate_id == "gate_cutover_readiness":
                gate_inputs.extend(gate.required_inputs or [gate.reason])
            elif "boundary" in wave.wave_id and gate.gate_id == "gate_security_boundary":
                gate_inputs.extend(gate.required_inputs or [gate.reason])
        return list(dict.fromkeys(gate_inputs))

    def _step_actions(
        self,
        stage: str,
        evidence_tags: set[str],
        pattern: ArchitecturePattern | None,
        repo_profile: RepoProfile | None,
    ) -> list[str]:
        actions: list[str] = []
        if stage == "boundary":
            actions.extend(
                [
                    "梳理跨边界调用点，并把入口收敛到单一 facade/adapter。",
                    "冻结新增跨边界直连，先让新旧路径共存。",
                ]
            )
            if "sync-call" in evidence_tags:
                actions.append("把同步调用链先挂到兼容层，而不是直接拆成跨服务硬依赖。")
        elif stage == "contract":
            actions.extend(
                [
                    "补齐兼容契约、双路径护栏或 shadow 校验。",
                    "确保旧调用方在切换前后都能继续工作。",
                ]
            )
        elif stage == "data":
            actions.extend(
                [
                    "定义数据所有权、写路径归属和最小回填范围。",
                    "在切换前验证双写、回填或影子读结果。",
                ]
            )
            if repo_profile and repo_profile.database_backup_commands:
                actions.append("生成最新备份证据并绑定到迁移批次。")
        elif stage == "cutover":
            actions.extend(
                [
                    "只针对最小切片准备切流，不一次性扩大范围。",
                    "准备 smoke、指标、trace 和回滚脚本，再执行 canary/shadow 验证。",
                ]
            )
            if repo_profile and repo_profile.docker_compose_file:
                actions.append("先验证 compose/deployment 配置，再推进最小发布切片。")
        else:
            actions.append("按最小切片推进增量迁移，并保留回退入口。")

        if pattern and pattern.pattern_id == "event_driven_reorganization" and "event-driven" in evidence_tags:
            actions.append("确认事件契约与消费幂等性，再替换同步耦合。")
        return list(dict.fromkeys(actions))

    def _step_command_hints(
        self,
        stage: str,
        repo_profile: RepoProfile | None,
        *,
        baseline_commands: list[str],
        backup_commands: list[str],
    ) -> list[str]:
        hints: list[str] = []
        hints.extend(baseline_commands[:1])
        if stage == "data":
            hints.extend(backup_commands[:1])
        if stage == "cutover" and repo_profile and repo_profile.docker_compose_file:
            hints.append("docker compose config")
        if stage == "cutover":
            hints.extend(command for command in baseline_commands[1:3] if command not in hints)
        return [item for item in dict.fromkeys(hints) if item]

    def _obligation_evidence(
        self,
        obligation_ids: list[str],
        obligation_map: dict[str, VerificationObligation],
    ) -> list[str]:
        evidence: list[str] = []
        for obligation_id in obligation_ids:
            obligation = obligation_map.get(obligation_id)
            if obligation is None:
                continue
            evidence.extend(obligation.evidence_required)
        return list(dict.fromkeys(evidence))

    def _render_playbook_task_prompt(
        self,
        wave: MigrationWave,
        stage: str,
        actions: list[str],
        command_hints: list[str],
        gate_inputs: list[str],
        rollback_ids: list[str],
    ) -> str:
        lines = [
            f"按执行 playbook 推进 `{wave.title}`，仅覆盖当前波次 `{wave.wave_id}`。",
            "落实以下动作：",
        ]
        lines.extend(f"- {item}" for item in actions[:4])
        if gate_inputs:
            lines.append("进入本波次前先确认这些输入/门禁：")
            lines.extend(f"- {item}" for item in gate_inputs[:4])
        if command_hints:
            lines.append("优先结合这些验证或演练命令：")
            lines.extend(f"- {item}" for item in command_hints[:3])
        if rollback_ids:
            lines.append(f"输出本波次对应的回滚动作引用：{', '.join(rollback_ids)}")
        if stage == "cutover":
            lines.append("若切流条件未满足，只产出准备物，不执行扩大范围的真实切换。")
        lines.extend(
            [
                "结果中必须原样输出以下前缀，便于后续硬验证：",
                "EvidenceRefs: 列出本波次已覆盖的 evidence_required 条目。",
                "RollbackRefs: 列出本波次已引用的 rollback_action_ids。",
                "UnmetCutoverGates: 列出仍未满足的切流门槛，没有则写 none。",
            ]
        )
        return "\n".join(lines)

    def _playbook_preconditions(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        human_gates: list[HumanGate],
    ) -> list[str]:
        items = ["基线验证入口可运行并可复用。"]
        if task_contract.requires_backup:
            items.append("在涉及状态迁移前保留最近一次可恢复的备份证据。")
        if repo_profile and repo_profile.docker_compose_file:
            items.append("部署拓扑文件可解析，能用于发布前检查。")
        if human_gates:
            items.append("需要人工确认的门禁输入已形成可审阅材料。")
        return list(dict.fromkeys(items))

    def _cutover_gates(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        evidence_tags: set[str],
        obligations: list[VerificationObligation],
        human_gates: list[HumanGate],
    ) -> list[str]:
        gates = ["基线回归验证通过。", "兼容契约/双路径验证通过。", "回滚入口已演练或已验证可用。"]
        if task_contract.touches_database or "shared-db" in evidence_tags:
            gates.append("数据所有权、写路径和回填策略已确认。")
        if repo_profile and repo_profile.docker_compose_file:
            gates.append("部署配置 dry-run 通过。")
        if any(item.gate_id == "gate_cutover_readiness" for item in human_gates):
            gates.append("切流 readiness 门禁已被人工确认。")
        if any(item.obligation_id == "obl_observability" for item in obligations):
            gates.append("最小指标、日志和 trace 观测路径可用。")
        return list(dict.fromkeys(gates))

    def _rollback_triggers(
        self,
        task_contract: TaskContract,
        evidence_tags: set[str],
    ) -> list[str]:
        triggers = [
            "契约或 smoke 验证失败。",
            "切换后错误率、延迟或核心指标明显恶化。",
        ]
        if task_contract.touches_database or "shared-db" in evidence_tags:
            triggers.append("双写、回填或读写归属验证出现一致性风险。")
        if "sync-call" in evidence_tags:
            triggers.append("同步调用链出现级联失败或超时放大。")
        return list(dict.fromkeys(triggers))

    def _build_verification_obligations(
        self,
        task_contract: TaskContract,
        repo_profile: RepoProfile | None,
        pattern: ArchitecturePattern | None,
        evidence_tags: set[str],
    ) -> list[VerificationObligation]:
        baseline_commands: list[str] = []
        if repo_profile is not None:
            baseline_commands.extend(repo_profile.backend_commands[:2])
            baseline_commands.extend(
                command for command in repo_profile.frontend_commands[:1] if command not in baseline_commands
            )
            baseline_commands.extend(
                command for command in repo_profile.verification_commands[:2] if command not in baseline_commands
            )

        first_command = baseline_commands[0] if baseline_commands else ""
        second_command = baseline_commands[1] if len(baseline_commands) > 1 else first_command
        rollout_command = "docker compose config" if repo_profile and repo_profile.docker_compose_file else second_command

        obligations = [
            VerificationObligation(
                obligation_id="obl_regression",
                description="保持基线验证入口通过，避免架构调整破坏现有能力。",
                category="regression",
                command_hint=first_command,
                evidence_required=["baseline_pass"],
            )
        ]
        if pattern and pattern.pattern_id in {"service_extraction", "strangler_migration", "api_facade"}:
            obligations.append(
                VerificationObligation(
                    obligation_id="obl_contract",
                    description="补齐兼容契约、适配层或双路径验证。",
                    category="contract",
                    command_hint=second_command,
                    evidence_required=["contract_compatibility"],
                )
            )
            obligations.append(
                VerificationObligation(
                    obligation_id="obl_cutover",
                    description="验证 rollout/cutover 前置条件与回滚入口仍然成立。",
                    category="rollout",
                    command_hint=rollout_command,
                    evidence_required=["rollout_checklist", "rollback_ready"],
                )
            )
        if task_contract.touches_database or "shared-db" in evidence_tags:
            obligations.append(
                VerificationObligation(
                    obligation_id="obl_data_boundary",
                    description="验证数据边界变化不会破坏现有持久化路径。",
                    category="data",
                    command_hint=first_command,
                    evidence_required=["data_boundary_review"],
                )
            )
        if {"rollout", "observability"} & evidence_tags:
            obligations.append(
                VerificationObligation(
                    obligation_id="obl_observability",
                    description="确认切换后最小观测能力仍然可用。",
                    category="observability",
                    command_hint=rollout_command or first_command,
                    evidence_required=["smoke_metrics", "smoke_trace"],
                    blocking=False,
                )
            )
        return obligations

    def _build_risks(
        self,
        task_contract: TaskContract,
        pattern: ArchitecturePattern | None,
        evidence_tags: set[str],
    ) -> list[RiskItem]:
        risks = [
            RiskItem(
                risk_id="risk_boundary_drift",
                title="边界收敛不彻底",
                severity="major",
                likelihood="medium",
                mitigation="先建立单一入口，再做实现迁移。",
                owner="principal_architect",
            )
        ]
        if pattern is not None:
            for index, item in enumerate(pattern.default_risks, start=1):
                risks.append(
                    RiskItem(
                        risk_id=f"risk_pattern_{index}",
                        title=item,
                        severity="major",
                        likelihood="medium",
                        mitigation="通过工作包拆分、契约验证和回滚护栏降低风险。",
                        owner="skeptical_architect",
                    )
                )
        if task_contract.touches_database or "shared-db" in evidence_tags:
            risks.append(
                RiskItem(
                    risk_id="risk_data_ownership",
                    title="数据所有权与一致性处理不充分",
                    severity="critical",
                    likelihood="medium",
                    mitigation="明确写路径归属，并在迁移阶段保留兼容验证和回滚路径。",
                    owner="data_specialist",
                )
            )
        if "sync-call" in evidence_tags:
            risks.append(
                RiskItem(
                    risk_id="risk_sync_coupling",
                    title="同步调用链放大切换风险",
                    severity="major",
                    likelihood="medium",
                    mitigation="先收敛同步入口，再引入异步或兼容层。",
                    owner="platform_specialist",
                )
            )
        return risks

    def _build_human_gates(
        self,
        task_contract: TaskContract,
        pattern: ArchitecturePattern | None,
        evidence_tags: set[str],
    ) -> list[HumanGate]:
        gates: list[HumanGate] = []
        if task_contract.touches_database or "shared-db" in evidence_tags:
            gates.append(
                HumanGate(
                    gate_id="gate_data_ownership",
                    reason="需要确认最终数据所有权与写路径归属",
                    trigger_condition="进入数据边界或切流波次前",
                    required_inputs=["ownership_map", "rollback_strategy"],
                )
            )
        if pattern and pattern.pattern_id in {"service_extraction", "strangler_migration"}:
            gates.append(
                HumanGate(
                    gate_id="gate_cutover_readiness",
                    reason="需要确认切流、观测和回滚条件齐备",
                    trigger_condition="进入 cutover 波次前",
                    required_inputs=["rollout_plan", "smoke_checks", "rollback_plan"],
                )
            )
        if "security" in task_contract.affected_areas:
            gates.append(
                HumanGate(
                    gate_id="gate_security_boundary",
                    reason="需要确认新边界的认证、授权与审计策略",
                    trigger_condition="引入新入口或新服务暴露面前",
                    required_inputs=["auth_boundary", "audit_plan"],
                )
            )
        return gates

    def _build_dissent_notes(
        self,
        pattern: ArchitecturePattern | None,
        task_contract: TaskContract,
        evidence_tags: set[str],
    ) -> list[str]:
        notes = ["不要把第一阶段做成一次性全面迁移，应当先缩小切片。"]
        if pattern and pattern.pattern_id == "service_extraction":
            notes.append("抽离服务前必须先收敛入口与兼容层，否则会放大耦合和回滚风险。")
        if task_contract.touches_database or "shared-db" in evidence_tags:
            notes.append("涉及数据库边界时，功能通过不等于迁移安全。")
        if "sync-call" in evidence_tags:
            notes.append("同步调用链未收敛前，不应直接推进跨服务切流。")
        return notes
