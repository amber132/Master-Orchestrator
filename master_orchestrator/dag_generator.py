"""DAG generator: convert Phase task definitions into DAG objects.

Responsibilities:
- Convert Phase.raw_tasks into DAG + TaskNode objects
- Inject context prefix into each task prompt (project context, phase info, prior outputs, review feedback)
- Validate the generated DAG
- Generate correction DAGs from ReviewResult.corrective_actions
"""

from __future__ import annotations

import logging
import re

from .auto_model import AutoConfig, Phase, ReviewResult
from .exceptions import DAGValidationError
from .model import DAG, RetryPolicy, TaskNode
from .provider_router import normalize_task_executor
from .task_contract import TaskContract
from .task_templates import build_document_execution_rules, build_refactor_execution_rules

logger = logging.getLogger(__name__)


def _goal_slug(text: str, max_len: int = 40) -> str:
    """从目标文本中提取简短可读的标识符。

    例: "修复 login API 返回 500 错误" → "fix-login-api-500"
    """
    if not text:
        return ""
    # 取前 max_len 个字符，去除多余空白
    slug = text.strip()[:max_len]
    # 中文：取前 20 字符作为摘要
    if re.search(r'[一-鿿]', slug):
        slug = re.sub(r'\s+', '-', slug)
        slug = re.sub(r'[^\w一-鿿-]', '', slug)
        return slug[:20] if slug else ""
    # 英文：转小写，非字母数字替换为连字符，去重连字符
    slug = slug.lower()
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    # 取前 5 个有意义的词
    parts = [p for p in slug.split('-') if p][:5]
    return '-'.join(parts) if parts else ""

# 注入到每个任务 prompt 末尾的效率约束，防止 Claude 过度探索
_TASK_EFFICIENCY_CONSTRAINT = r"""

# 执行约束
- 直接修改目标文件，不要花时间探索无关代码
- 如果不确定某个文件的位置，用 Glob/Grep 搜索而非逐目录浏览
- 每个文件最多读取一次，避免重复读取同一文件
- 完成修改后立即结束，不要做额外的"验证性"探索
- 不要编辑 `goal_state.json`、`orchestrator_state.db`、交付清单等编排器状态文件

# 文件写入规范（严格遵守，违反此规则会导致任务无限循环）
Bash 工具在传递命令时会处理反斜杠转义（`\\` → `\`），即使单引号 heredoc 也无法避免。
写文件方法按可靠性排序（优先使用排名靠前的方法）：
1. **Write/Edit 工具**（100% 可靠）— 永远首选，无转义问题
2. **base64 编解码**（100% 可靠）— 仅当 Write 工具不可用时：`echo '<base64>' | base64 -d > file`
3. **Heredoc / printf / python -c / node -e** — 仅限不含 `\\`、`${}`、`\n\t` 字面量的简单内容，否则必定出错
4. **绝对禁止**：通过 Bash 调用 `pwsh -Command "..."` 写文件（双重转义 100% 失败）
- 规则：优先用 Write/Edit，第一次遇到转义困难时立即切换 base64，**禁止反复尝试不同 shell 转义方案**
- **大文件拆分**：超过 200 行的文件，先用 Write 写入前 200 行，再用 Edit 工具分批追加剩余内容（每次不超过 200 行）
- Bash 工具仅用于：运行命令（npm/pip/git/make）、检查状态、执行测试等系统操作
"""


class DAGGenerator:
    """Generates DAG objects from Phase definitions."""

    def __init__(self, auto_config: AutoConfig, max_parallel: int = 200):
        self._auto_config = auto_config
        self._max_parallel = max_parallel

    def generate(
        self,
        phase: Phase,
        project_context: str,
        prior_phase_summaries: list[str] | None = None,
        review_feedback: str | None = None,
        task_contract: TaskContract | None = None,
    ) -> DAG:
        """Convert a Phase's raw_tasks into a validated DAG."""
        goal = _goal_slug(task_contract.normalized_goal) if task_contract else ""
        dag_name = f"{goal}__{phase.id}_iter{phase.iteration}" if goal else f"{phase.id}_iter{phase.iteration}"
        dag = DAG(name=dag_name, max_parallel=self._max_parallel)
        context_prefix = self._build_context_prefix(
            phase,
            project_context,
            prior_phase_summaries,
            review_feedback,
            task_contract,
        )

        for raw_task in phase.raw_tasks:
            task_id = raw_task.get("id", "")
            if not task_id:
                logger.warning("跳过无 ID 的任务定义: %s", raw_task)
                continue

            raw_prompt = raw_task.get("prompt", "")
            full_prompt = f"{context_prefix}\n\n# 当前任务\n{raw_prompt}{_TASK_EFFICIENCY_CONSTRAINT}"
            task_type = str(raw_task.get("type", "agent_cli") or "agent_cli")
            output_format = str(raw_task.get("output_format", "json" if task_type == "operation" else "text") or "text")
            executor_config = dict(raw_task.get("executor_config") or {})
            executor_config.setdefault("phase", getattr(phase, "phase_type", "") or phase.name)

            node = normalize_task_executor(TaskNode(
                id=task_id,
                prompt_template=full_prompt,
                depends_on=raw_task.get("depends_on", []),
                timeout=raw_task.get("timeout", 1800),
                retry_policy=RetryPolicy(max_attempts=2, backoff_base=15.0),
                model=self._auto_config.execution_model,
                output_format=output_format,
                tags=list(dict.fromkeys([str(tag) for tag in raw_task.get("tags", [])])),
                provider=str(raw_task.get("provider", "auto") or "auto"),
                type=task_type,
                executor_config=executor_config,
            ))
            dag.tasks[task_id] = node

        errors = dag.validate()
        if errors:
            logger.warning("DAG 验证发现问题，尝试自动修复: %s", errors)
            self._auto_fix_deps(dag)
            errors = dag.validate()
            if errors:
                raise DAGValidationError(
                    f"阶段 '{phase.id}' 生成的 DAG 无效:\n" + "\n".join(f"  - {e}" for e in errors)
                )

        logger.info("为阶段 '%s' 生成 DAG: %d 个任务", phase.id, len(dag.tasks))
        return dag

    def generate_correction_dag(
        self,
        phase: Phase,
        review: ReviewResult,
        project_context: str,
        task_contract: TaskContract | None = None,
    ) -> DAG:
        """Generate a DAG from corrective actions in a review result."""
        goal = _goal_slug(task_contract.normalized_goal) if task_contract else ""
        dag_name = f"{goal}__{phase.id}_fix_iter{phase.iteration}" if goal else f"{phase.id}_fix_iter{phase.iteration}"
        dag = DAG(name=dag_name, max_parallel=self._max_parallel)
        if not review.corrective_actions:
            return dag

        refactor_rules = build_refactor_execution_rules(task_contract) if task_contract else ""
        document_rules = build_document_execution_rules(task_contract) if task_contract else ""
        context_prefix = (
            f"# 项目上下文\n{project_context[:2000]}\n\n"
            f"# 修正背景\n"
            f"阶段 '{phase.name}' 审查发现以下问题需要修正:\n"
            f"{review.summary}\n"
        )
        if document_rules:
            context_prefix += f"\n# 文档执行约束\n{document_rules}\n"
        if refactor_rules:
            context_prefix += f"\n# 严格重构护栏\n{refactor_rules}\n"

        for action in review.corrective_actions:
            full_prompt = f"{context_prefix}\n# 修正任务\n{action.prompt_template}{_TASK_EFFICIENCY_CONSTRAINT}"
            deps = [d for d in action.depends_on_actions if d != action.action_id]
            action_type = str(action.action_type or "agent_cli")
            output_format = "json" if action_type == "operation" else "text"
            node = normalize_task_executor(TaskNode(
                id=action.action_id,
                prompt_template=full_prompt,
                depends_on=deps,
                timeout=action.timeout,
                retry_policy=RetryPolicy(max_attempts=2, backoff_base=10.0),
                model=self._auto_config.execution_model,
                output_format=output_format,
                tags=["phase_correction", "strict_refactor"] if task_contract and task_contract.strict_refactor_mode else ["phase_correction"],
                provider="auto",
                type=action_type,
                executor_config={
                    **dict(action.executor_config or {}),
                    "phase": dict(action.executor_config or {}).get("phase", getattr(phase, "phase_type", "") or phase.name),
                },
            ))
            dag.tasks[action.action_id] = node

        self._auto_fix_deps(dag)
        errors = dag.validate()
        if errors:
            raise DAGValidationError("修正 DAG 无效:\n" + "\n".join(f"  - {e}" for e in errors))

        logger.info("为阶段 '%s' 生成修正 DAG: %d 个任务", phase.id, len(dag.tasks))
        return dag

    def _build_context_prefix(
        self,
        phase: Phase,
        project_context: str,
        prior_summaries: list[str] | None,
        review_feedback: str | None,
        task_contract: TaskContract | None = None,
    ) -> str:
        sections: list[str] = []

        if project_context:
            sections.append(f"# 项目上下文\n{project_context[:3000]}")
        if task_contract:
            document_rules = build_document_execution_rules(task_contract)
            if document_rules:
                sections.append(f"# 文档执行约束\n{document_rules}")

        phase_info = (
            f"# 当前阶段: {phase.name}\n"
            f"描述: {phase.description}\n"
            f"目标: {', '.join(phase.objectives)}\n"
            f"验收标准: {', '.join(phase.acceptance_criteria)}"
        )
        sections.append(phase_info)

        target_family = phase.metadata.get("target_service_family") if phase.metadata else None
        if target_family:
            sections.append(f"# 本阶段唯一目标服务族\n{target_family}")
        out_of_scope = phase.metadata.get("out_of_scope") if phase.metadata else None
        if out_of_scope:
            sections.append("# 本阶段明确不处理\n" + "\n".join(f"- {item}" for item in out_of_scope))

        if task_contract and task_contract.strict_refactor_mode:
            sections.append(f"# 严格重构护栏\n{build_refactor_execution_rules(task_contract)}")

        if prior_summaries:
            summaries_text = "\n".join(prior_summaries[-3:])
            sections.append(f"# 前序阶段完成情况（不要重复已完成的工作）\n{summaries_text}")

        if review_feedback:
            sections.append(f"# 上次迭代诊断（请重点关注并修正）\n{review_feedback}")

        if phase.iteration > 0:
            sections.append(f"# 注意: 这是第 {phase.iteration + 1} 次迭代，请根据反馈改进")

        return "\n\n".join(sections)

    def _auto_fix_deps(self, dag: DAG) -> None:
        """Remove references to non-existent task IDs from depends_on."""
        all_ids = set(dag.tasks)
        for node in dag.tasks.values():
            invalid = [d for d in node.depends_on if d not in all_ids]
            if invalid:
                logger.warning("任务 '%s' 移除无效依赖: %s", node.id, invalid)
                node.depends_on = [d for d in node.depends_on if d in all_ids]
