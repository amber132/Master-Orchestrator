"""Closed-loop task template definitions."""

from __future__ import annotations

from .task_contract import TaskContract, TaskType


TASK_TYPE_HINTS: dict[TaskType, str] = {
    TaskType.BUGFIX: "这是一个 bug 修复任务，优先复现、定位和回归验证。",
    TaskType.FEATURE: "这是一个新增功能任务，优先明确范围、接口和验收标准。",
    TaskType.REFACTOR: "这是一个严格重构任务，必须冻结业务语义、缩小范围、先验证再扩展。",
    TaskType.INTEGRATION: "这是一个联调任务，优先打通前后端与运行时验证。",
    TaskType.UNKNOWN: "这是一个通用任务，请优先保证闭环和验证。",
}


REQUIRED_STAGE_ORDER = ["analyze", "scope", "implement", "verify", "handoff"]


def build_contract_hint(contract: TaskContract) -> str:
    hint = TASK_TYPE_HINTS[contract.task_type]
    if not contract.strict_refactor_mode:
        return hint

    return (
        f"{hint} "
        f"单阶段最多 {contract.max_service_families_per_phase} 个服务族/控制器族，"
        f"单次迭代最多 {contract.max_prod_files_per_iteration} 个生产文件。"
        "不得修改业务语义，不得修改编排器状态文件。"
    )


def build_refactor_execution_rules(contract: TaskContract) -> str:
    if not contract.strict_refactor_mode:
        return ""

    allowed_roots = "、".join(contract.allowed_refactor_roots[:8]) if contract.allowed_refactor_roots else "当前受影响的业务目录"
    state_files = "、".join(contract.state_file_patterns)
    return (
        "严格重构护栏：\n"
        f"- 单阶段只允许 {contract.max_service_families_per_phase} 个服务族或控制器族\n"
        f"- 单次迭代最多触达 {contract.max_prod_files_per_iteration} 个生产代码文件\n"
        "- 不新增业务能力，不改变业务语义，不扩大范围\n"
        f"- 禁止编辑 {state_files}\n"
        f"- 改动必须限制在这些根路径内：{allowed_roots}\n"
        "- 先完成最小可验证闭环，再决定是否继续扩大范围"
    )


def build_document_execution_rules(contract: TaskContract | None) -> str:
    if not contract or not contract.has_document_context:
        return ""

    doc_lines = "\n".join(f"- {path}" for path in contract.document_paths[:12])
    sections = [
        "文档驱动执行约束：",
        "- 分解阶段和执行阶段都必须优先遵守任务文档，而不是只依赖自然语言 goal。",
        "- 文档摘要只用于压缩上下文；真正修改文件前必须打开原文。",
        "- 若项目仓库内 guide 与中央宪章冲突，以项目仓库内 guide 为准。",
    ]
    if doc_lines:
        sections.append("任务文档列表：\n" + doc_lines)
    if contract.document_context:
        sections.append("任务文档摘要：\n" + contract.document_context[:5000])
    return "\n".join(sections)
