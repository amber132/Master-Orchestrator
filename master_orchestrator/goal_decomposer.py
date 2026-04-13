"""Goal decomposition: call Claude to break a goal into phases and tasks."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from .auto_model import AutoConfig, Phase, PhaseStatus
from .agent_cli import run_agent_task
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig
from .json_utils import repair_truncated_json, robust_parse_json
from .model import TaskNode
from .architecture_contract import ArchitectureContract
from .task_contract import TaskContract
from .task_templates import build_document_execution_rules

logger = logging.getLogger(__name__)
_RECENT_DECOMPOSITION_FILE_PREFIXES = (
    "decompose",
    "_decompose",
    "goal_decompose",
    "goal_decomposition",
)

_DECOMPOSE_SYSTEM_PROMPT = """\
你是一个项目规划专家。你的任务是将用户的目标分解为可执行的阶段(Phase)和任务(Task)。

规则：
1. 默认生成 3-6 个阶段，每个阶段包含尽可能多的并行任务（最多 30 个）
2. 【关键】任务拆分要足够细！每个任务应该是一个小的、独立的工作单元，控制在 5-10 分钟内可完成
3. 例如：解析 20 个模块的报告，应该拆成 20 个独立任务（每个模块一个），而不是 4 个任务（每个处理 5 个模块）
4. 默认可利用并行能力，但必须尊重后续附加约束
5. 阶段之间有明确的先后顺序
6. 每个任务的 prompt 必须是具体、可执行的指令
7. 任务的 depends_on 只能引用同一阶段内的其他任务 ID，尽量减少依赖
8. objectives 和 acceptance_criteria 各写 1 条，保持简洁
9. 任务 prompt 要简洁精炼，控制在 100 字以内，只写核心指令
10. 【禁止大型汇总任务】不要创建一个任务依赖所有其他任务并汇总全部结果
11. 汇总/排序/统计类任务的 timeout 设为 600 秒（10分钟），如果超过说明任务太大需要拆分

【极其重要】禁止使用 Write 工具！直接在对话中输出 JSON 内容。不要写入任何文件！

JSON 格式：
{"phases":[{"id":"phase_01_xxx","name":"名","description":"描述","objectives":["目标"],"acceptance_criteria":["标准"],"depends_on_phases":[],"tasks":[{"id":"task_id","prompt":"指令","depends_on":[],"timeout":1800}]}]}
"""

_STRICT_REFACTOR_APPENDIX = """\
# 严格重构模式（以下规则覆盖默认拆分策略）
1. 仅生成 1-4 个阶段。
2. 每个阶段只允许 1 个受限服务族/控制器族，不得在同一阶段覆盖多个服务族。
3. 每个阶段最多 2 个实现任务，且必须围绕同一个生产切片。
4. 每个阶段必须显式给出 `target_service_family` 和 `out_of_scope`。
5. tasks 只能生成实现类任务，不要生成 analyze/scope/verify/handoff/summary 之类的元任务。
6. 不要要求编辑编排器状态文件、运行时状态文件或交付清单文件。
7. 默认最小并行、优先顺序执行，先缩小范围再扩展范围。
8. 如果某一阶段需要触达多个模块，请拆成多个阶段，而不是在同一阶段并行推进。
"""


class GoalDecomposer:
    """Calls Claude to decompose a goal into a list of phases."""

    def __init__(
        self,
        claude_config: ClaudeConfig,
        limits_config: LimitsConfig,
        auto_config: AutoConfig,
        budget_tracker: BudgetTracker | None = None,
        working_dir: str | None = None,
        provider_config: Any | None = None,
        preferred_provider: str = "auto",
        phase_provider_overrides: dict[str, str] | None = None,
    ):
        self._claude_config = claude_config
        self._limits = limits_config
        self._auto_config = auto_config
        self._budget = budget_tracker
        self._working_dir = working_dir
        self._provider_config = provider_config
        self._preferred_provider = preferred_provider
        self._phase_provider_overrides = dict(phase_provider_overrides or {})

    def decompose(
        self,
        goal: str,
        project_context: str,
        task_contract: TaskContract | None = None,
        architecture_contract: ArchitectureContract | None = None,
    ) -> list[Phase]:
        """Decompose a goal into a list of Phases with raw task definitions."""
        max_retries = 3
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                prompt = self._build_prompt(goal, project_context, task_contract, architecture_contract)

                task_node = TaskNode(
                    id="_decompose",
                    prompt_template=prompt,
                    timeout=1800,
                    model=self._auto_config.decomposition_model,
                    output_format="text",
                    system_prompt=None,
                    max_turns=100,
                    provider=self._preferred_provider,
                    type="agent_cli",
                    executor_config={"phase": "decompose"},
                )

                if self._provider_config is not None:
                    result = run_agent_task(
                        task=task_node,
                        prompt=prompt,
                        config=self._provider_config,
                        limits=self._limits,
                        budget_tracker=self._budget,
                        working_dir=self._working_dir,
                        on_progress=None,
                        cli_provider=self._preferred_provider if self._preferred_provider in {"claude", "codex"} else None,
                        phase_provider_overrides=self._phase_provider_overrides,
                    )
                else:
                    result = run_claude_task(
                        task=task_node,
                        prompt=prompt,
                        claude_config=self._claude_config,
                        limits=self._limits,
                        budget_tracker=self._budget,
                        working_dir=self._working_dir,
                    )

                if result.status.value != "success":
                    raise RuntimeError(f"目标分解失败: {result.error}")

                raw_output = result.output or ""
                logger.debug("Decompose raw output length: %d chars", len(raw_output))
                logger.debug("Decompose raw output (first 500): %s", raw_output[:500])

                data = self._extract_json_from_output(raw_output)
                if data is None:
                    logger.warning(
                        "目标分解第 %d 次：LLM 输出非 JSON（%d chars），启动格式转换任务",
                        attempt,
                        len(raw_output),
                    )
                    data = self._convert_text_to_json(raw_output, goal, task_contract)

                return self._build_phases(data, task_contract)
            except (RuntimeError, json.JSONDecodeError, KeyError, ValueError) as exc:
                last_error = exc
                logger.warning("目标分解第 %d/%d 次失败: %s", attempt, max_retries, exc)
                if attempt < max_retries:
                    import time as _time

                    _time.sleep(min(5 * attempt, 15))
                    continue
        raise RuntimeError(f"目标分解 {max_retries} 次尝试后失败: {last_error}")

    def _extract_json_from_output(self, raw_output: str) -> Any | None:
        """从 LLM 输出中提取 JSON。"""
        data = None
        done_match = re.search(r'DONE[:\s]+(.+\.json)', raw_output)
        if done_match:
            file_path = done_match.group(1).strip().strip('"\'')
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as handle:
                        candidate = json.load(handle)
                    if self._is_valid_decomposition_payload(candidate):
                        return candidate
                except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                    logger.debug("忽略无法解析的 DONE JSON 文件: %s", file_path)

        if data is None and self._working_dir and os.path.isdir(self._working_dir):
            recent_jsons: list[tuple[float, str]] = []
            now = time.time()
            try:
                for root, _, files in os.walk(self._working_dir):
                    for fname in files:
                        if not fname.endswith('.json'):
                            continue
                        if fname in {"goal_state.json", "delivery_manifest.json"}:
                            continue
                        if not self._is_safe_recent_decomposition_filename(fname):
                            continue
                        fpath = os.path.join(root, fname)
                        try:
                            mtime = os.path.getmtime(fpath)
                            if now - mtime < 300:
                                recent_jsons.append((mtime, fpath))
                        except OSError:
                            continue
            except OSError:
                recent_jsons = []

            recent_jsons.sort(reverse=True)
            for _, fpath in recent_jsons:
                try:
                    with open(fpath, 'r', encoding='utf-8') as handle:
                        candidate = json.load(handle)
                    if self._is_valid_decomposition_payload(candidate):
                        data = candidate
                        logger.info("从最近修改的文件读取分解结果: %s", fpath)
                        break
                except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                    continue

        if data is None:
            try:
                data = robust_parse_json(raw_output)
                if not self._is_valid_decomposition_payload(data):
                    data = None
            except ValueError:
                repaired = repair_truncated_json(raw_output)
                if repaired:
                    try:
                        data = json.loads(repaired)
                        if not self._is_valid_decomposition_payload(data):
                            data = None
                    except json.JSONDecodeError:
                        data = None

        return data

    def _is_safe_recent_decomposition_filename(self, fname: str) -> bool:
        stem = os.path.splitext(os.path.basename(fname))[0].lower()
        return any(stem.startswith(prefix) for prefix in _RECENT_DECOMPOSITION_FILE_PREFIXES)

    def _is_valid_decomposition_payload(self, candidate: Any) -> bool:
        if not isinstance(candidate, dict):
            return False

        raw_phases = candidate.get("phases")
        if not isinstance(raw_phases, list) or not raw_phases:
            return False

        for raw_phase in raw_phases:
            if not isinstance(raw_phase, dict):
                return False
            tasks = raw_phase.get("tasks")
            if not isinstance(tasks, list) or not tasks:
                return False
            for raw_task in tasks:
                if not isinstance(raw_task, dict):
                    return False
                if not str(raw_task.get("prompt") or "").strip():
                    return False
        return True

    def _convert_text_to_json(
        self,
        analysis_text: str,
        goal: str,
        task_contract: TaskContract | None = None,
    ) -> Any:
        """机械兜底：把自然语言分析文本转换为结构化 JSON。"""
        max_ctx = 15000
        truncated = analysis_text[:max_ctx]
        if len(analysis_text) > max_ctx:
            truncated += f"\n\n... (截断，原文共 {len(analysis_text)} 字符)"

        strict_rules = self._build_strict_refactor_rules(task_contract)
        convert_prompt = f"""你之前对以下目标做了详细分析，现在请将分析结果转换为指定的 JSON 格式。

# 原始目标
{goal}

# 你之前的分析
{truncated}

# 要求
将上述分析转换为以下 JSON 格式，不要做新的分析，不要使用任何工具，直接输出 JSON：
{{"phases":[{{"id":"phase_01_xxx","name":"名","description":"描述","objectives":["目标"],"acceptance_criteria":["标准"],"depends_on_phases":[],"target_service_family":"服务族","out_of_scope":["不在本阶段处理的内容"],"tasks":[{{"id":"task_id","prompt":"指令","depends_on":[],"timeout":1800}}]}}]}}

规则：默认 3-6 个阶段，任务拆分要细（每个 5-10 分钟），prompt 控制在 100 字以内。{strict_rules}"""

        task_node = TaskNode(
            id="_decompose_convert",
            prompt_template=convert_prompt,
            timeout=300,
            model=self._auto_config.decomposition_model,
            output_format="text",
            system_prompt=None,
            max_turns=2,
            provider=self._preferred_provider,
            type="agent_cli",
            executor_config={"phase": "decompose"},
        )

        if self._provider_config is not None:
            result = run_agent_task(
                task=task_node,
                prompt=convert_prompt,
                config=self._provider_config,
                limits=self._limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
                on_progress=None,
                cli_provider=self._preferred_provider if self._preferred_provider in {"claude", "codex"} else None,
                phase_provider_overrides=self._phase_provider_overrides,
            )
        else:
            result = run_claude_task(
                task=task_node,
                prompt=convert_prompt,
                claude_config=self._claude_config,
                limits=self._limits,
                budget_tracker=self._budget,
                working_dir=self._working_dir,
            )

        if result.status.value != "success":
            raise RuntimeError(f"格式转换任务失败: {result.error}")

        convert_output = result.output or ""
        logger.info("格式转换任务输出长度: %d chars", len(convert_output))
        try:
            return robust_parse_json(convert_output)
        except ValueError as exc:
            raise RuntimeError(
                f"格式转换任务输出仍非 JSON（{len(convert_output)} chars）: {convert_output[:200]}"
            ) from exc

    def _build_prompt(
        self,
        goal: str,
        project_context: str,
        task_contract: TaskContract | None = None,
        architecture_contract: ArchitectureContract | None = None,
    ) -> str:
        ctx = project_context if project_context else "（无项目上下文，请根据目标自行规划）"
        strict_rules = self._build_strict_refactor_rules(task_contract)
        document_rules = build_document_execution_rules(task_contract)
        arch_section = self._build_architecture_section(architecture_contract)
        return f"""{_DECOMPOSE_SYSTEM_PROMPT}

{strict_rules}
{document_rules}
{arch_section}

# 目标
{goal}

# 项目上下文
{ctx[:12000]}

直接输出 JSON，第一个字符必须是 {{。任务 prompt 要精简（100字以内）。"""

    def _build_strict_refactor_rules(self, task_contract: TaskContract | None) -> str:
        if not task_contract or not task_contract.strict_refactor_mode:
            return ""

        allowed_roots = "、".join(task_contract.allowed_refactor_roots[:8]) or "后端受影响根目录"
        forbidden = "、".join(task_contract.state_file_patterns)
        return (
            f"{_STRICT_REFACTOR_APPENDIX}\n"
            f"9. 本次每个阶段最多 {task_contract.max_service_families_per_phase} 个服务族，"
            f"单次迭代最多 {task_contract.max_prod_files_per_iteration} 个生产文件。\n"
            f"10. 允许的根路径：{allowed_roots}\n"
            f"11. 禁止编辑这些状态文件：{forbidden}\n"
        )

    def _build_architecture_section(self, contract: ArchitectureContract | None) -> str:
        """将架构决策注入分解 prompt，让任务拆分遵循已确定的架构方向。"""
        if not contract:
            return ""
        lines = ["\n# 架构决策约束（必须遵循）"]
        if contract.selected_summary:
            lines.append(f"- 选定方案：{contract.selected_summary}")
        if contract.scope_in:
            lines.append(f"- 范围内：{', '.join(contract.scope_in)}")
        if contract.scope_out:
            lines.append(f"- 范围外：{', '.join(contract.scope_out)}")
        if contract.quality_attributes:
            lines.append(f"- 质量属性：{', '.join(contract.quality_attributes)}")
        if contract.work_packages:
            lines.append("- 工作包：")
            for wp in contract.work_packages[:5]:
                lines.append(f"  - {wp.name}: {wp.description}")
        if contract.verification_obligations:
            lines.append("- 验证义务：")
            for vo in contract.verification_obligations[:5]:
                lines.append(f"  - {vo.description}")
        return "\n".join(lines)

    def _build_phases(
        self,
        data: dict[str, Any] | list | None,
        task_contract: TaskContract | None = None,
    ) -> list[Phase]:
        """Convert parsed JSON into Phase objects."""
        if data is None:
            raise RuntimeError("无法解析目标分解结果：JSON 解析失败")

        raw_phases = data if isinstance(data, list) else data.get("phases", [])
        if not raw_phases:
            raise RuntimeError("目标分解结果中没有 phases")

        phases: list[Phase] = []
        for index, raw_phase in enumerate(raw_phases):
            phase_id = raw_phase.get("id", f"phase_{index + 1:02d}")
            raw_tasks = self._normalize_raw_tasks(raw_phase.get("tasks", []), phase_id, task_contract)
            phase = Phase(
                id=phase_id,
                name=raw_phase.get("name", f"阶段 {index + 1}"),
                description=raw_phase.get("description", ""),
                order=index,
                objectives=raw_phase.get("objectives", []),
                acceptance_criteria=raw_phase.get("acceptance_criteria", []),
                depends_on_phases=raw_phase.get("depends_on_phases", []),
                status=PhaseStatus.PENDING,
                max_iterations=self._auto_config.max_phase_iterations,
                raw_tasks=raw_tasks,
                metadata=self._extract_phase_metadata(raw_phase, task_contract),
            )
            phases.append(phase)

        logger.info("目标分解完成: %d 个阶段", len(phases))
        for phase in phases:
            logger.info("  阶段 %s: %s (%d 个任务)", phase.id, phase.name, len(phase.raw_tasks))

        return phases

    def _normalize_raw_tasks(
        self,
        raw_tasks: list[dict[str, Any]],
        phase_id: str,
        task_contract: TaskContract | None,
    ) -> list[dict[str, Any]]:
        tasks = list(raw_tasks or [])
        if task_contract and task_contract.strict_refactor_mode:
            filtered_tasks: list[dict[str, Any]] = []
            for task in tasks:
                task_id = str(task.get("id", ""))
                prompt = str(task.get("prompt", "")).lower()
                if task_id.startswith(("analyze_", "scope_", "verify_", "handoff_")):
                    continue
                if any(token in prompt for token in ("总结", "汇总", "交付", "handoff", "summary", "verify", "验证步骤")):
                    continue
                filtered_tasks.append(task)
            tasks = filtered_tasks
            if len(tasks) > 2:
                logger.warning(
                    "严格重构模式下阶段 %s 返回了 %d 个实现任务，自动截断到前 2 个",
                    phase_id,
                    len(tasks),
                )
                tasks = tasks[:2]

        normalized: list[dict[str, Any]] = []
        for index, raw_task in enumerate(tasks, start=1):
            task_id = raw_task.get("id") or f"implement_{phase_id}_{index}"
            prompt = raw_task.get("prompt") or f"围绕阶段 {phase_id} 实施最小闭环修改"
            tags = [str(tag) for tag in raw_task.get("tags", [])]
            tags.append("phase_implement")
            if task_contract and task_contract.strict_refactor_mode:
                tags.extend(["strict_refactor", "bounded_slice"])
            normalized.append({
                "id": task_id,
                "prompt": prompt,
                "depends_on": raw_task.get("depends_on", []),
                "timeout": raw_task.get("timeout", 1800),
                "tags": list(dict.fromkeys(tags)),
            })
        return normalized

    def _extract_phase_metadata(
        self,
        raw_phase: dict[str, Any],
        task_contract: TaskContract | None,
    ) -> dict[str, Any]:
        metadata = {
            key: raw_phase[key]
            for key in ("target_service_family", "out_of_scope", "guardrails", "refactor_slice", "touched_roots")
            if key in raw_phase
        }
        if task_contract and task_contract.strict_refactor_mode:
            metadata.setdefault("strict_refactor_mode", True)
            metadata.setdefault("out_of_scope", [])
            metadata.setdefault("allowed_refactor_roots", list(task_contract.allowed_refactor_roots))
            metadata.setdefault("max_prod_files_per_iteration", task_contract.max_prod_files_per_iteration)
            inferred_family = self._infer_target_service_family(raw_phase)
            if inferred_family:
                metadata.setdefault("target_service_family", inferred_family)
        return metadata

    def _infer_target_service_family(self, raw_phase: dict[str, Any]) -> str:
        candidates: list[str] = []
        for key in ("target_service_family", "name", "description"):
            value = raw_phase.get(key)
            if value:
                candidates.append(str(value))
        for task in raw_phase.get("tasks", []) or []:
            if task.get("prompt"):
                candidates.append(str(task["prompt"]))

        for text in candidates:
            match = re.search(r'\b([A-Z][A-Za-z0-9]+(?:Service|Controller))\b', text)
            if match:
                return match.group(1)
        return ""
