"""角色专业化：为任务分配专业化的 Agent 角色，提升执行质量。"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from master_orchestrator.model import TaskNode


@dataclass
class AgentRole:
    """Agent 角色定义"""
    name: str
    system_prompt_prefix: str
    allowed_tools: list[str] | None = None  # None 表示允许所有工具
    preferred_model: str = "sonnet"
    tags: list[str] = field(default_factory=list)


# 预定义的内置角色
BUILTIN_ROLES: dict[str, AgentRole] = {
    "data_extractor": AgentRole(
        name="data_extractor",
        system_prompt_prefix=(
            "你是一个数据提取专家。你的任务是从各种来源（文件、API、数据库等）"
            "精确提取结构化数据。专注于数据完整性、格式规范和错误处理。"
        ),
        allowed_tools=["Read", "Write", "Edit", "Grep", "Glob", "Bash", "WebFetch"],
        preferred_model="haiku",
        tags=["extract", "data", "parse", "fetch", "scrape"],
    ),
    "analyzer": AgentRole(
        name="analyzer",
        system_prompt_prefix=(
            "你是一个分析专家。你的任务是深入分析数据、代码或系统行为，"
            "识别模式、问题和改进机会。提供清晰的洞察和可操作的建议。"
        ),
        allowed_tools=["Read", "Write", "Edit", "Grep", "Glob", "Bash"],
        preferred_model="sonnet",
        tags=["analyze", "review", "audit", "inspect", "investigate"],
    ),
    "validator": AgentRole(
        name="validator",
        system_prompt_prefix=(
            "你是一个验证专家。你的任务是验证数据、代码或系统的正确性、"
            "完整性和合规性。执行严格的检查，报告所有不符合要求的项。"
        ),
        allowed_tools=["Read", "Write", "Edit", "Grep", "Bash"],
        preferred_model="sonnet",
        tags=["validate", "verify", "check", "test", "quality"],
    ),
    "reporter": AgentRole(
        name="reporter",
        system_prompt_prefix=(
            "你是一个报告生成专家。你的任务是将分析结果、执行状态或数据"
            "整理成清晰、结构化的报告。注重可读性、完整性和专业性。"
        ),
        allowed_tools=["Read", "Write", "Edit", "Grep", "Glob"],
        preferred_model="sonnet",
        tags=["report", "summary", "document", "format", "present"],
    ),
    "coordinator": AgentRole(
        name="coordinator",
        system_prompt_prefix=(
            "你是一个协调者。你的任务是整合多个任务的输出，协调工作流，"
            "确保各部分协同工作。关注全局视角和依赖关系管理。"
        ),
        allowed_tools=None,  # 允许所有工具
        preferred_model="sonnet",
        tags=["coordinate", "integrate", "merge", "orchestrate", "combine"],
    ),
    "specialist": AgentRole(
        name="specialist",
        system_prompt_prefix=(
            "你是一个通用专家。你具备广泛的技能，能够处理各种复杂任务。"
            "根据任务需求灵活调整策略，确保高质量的执行结果。"
        ),
        allowed_tools=None,  # 允许所有工具
        preferred_model="opus",
        tags=["complex", "advanced", "expert", "comprehensive"],
    ),
}


class RoleAssigner:
    """角色分配器：根据任务特征自动匹配最合适的角色"""

    def __init__(self, custom_roles: dict[str, AgentRole] | None = None):
        """
        初始化角色分配器

        Args:
            custom_roles: 自定义角色字典，会与内置角色合并
        """
        self.roles = BUILTIN_ROLES.copy()
        if custom_roles:
            self.roles.update(custom_roles)

    def assign(self, task_node: TaskNode) -> AgentRole:
        """
        根据任务特征自动分配角色

        Args:
            task_node: 任务节点

        Returns:
            匹配的 AgentRole
        """
        # 1. 优先匹配任务的 tags
        if task_node.tags:
            for role_name, role in self.roles.items():
                # 检查任务 tags 是否与角色 tags 有交集
                if any(tag in role.tags for tag in task_node.tags):
                    return role

        # 2. 根据 prompt 关键词匹配
        prompt_lower = task_node.prompt_template.lower()

        # 关键词匹配规则
        keyword_rules = [
            (["extract", "fetch", "scrape", "parse", "获取", "提取"], "data_extractor"),
            (["analyze", "review", "audit", "inspect", "分析", "审查", "检查"], "analyzer"),
            (["validate", "verify", "test", "check", "验证", "测试"], "validator"),
            (["report", "summary", "document", "总结", "报告", "文档"], "reporter"),
            (["coordinate", "integrate", "merge", "combine", "协调", "整合", "合并"], "coordinator"),
        ]

        for keywords, role_name in keyword_rules:
            if any(kw in prompt_lower for kw in keywords):
                return self.roles[role_name]

        # 3. 根据复杂度选择角色
        if task_node.complexity == "complex":
            return self.roles["specialist"]

        # 4. 默认返回 specialist
        return self.roles["specialist"]


def apply_role(task_node: TaskNode, role: AgentRole) -> TaskNode:
    """
    将角色应用到任务节点，返回新的 TaskNode

    Args:
        task_node: 原始任务节点
        role: 要应用的角色

    Returns:
        应用角色后的新 TaskNode
    """
    # 构建新的 system_prompt
    new_system_prompt = role.system_prompt_prefix
    if task_node.system_prompt:
        new_system_prompt = f"{role.system_prompt_prefix}\n\n{task_node.system_prompt}"

    # 合并 allowed_tools（如果角色有限制且任务没有指定）
    new_allowed_tools = task_node.allowed_tools
    if new_allowed_tools is None and role.allowed_tools is not None:
        new_allowed_tools = role.allowed_tools

    # 使用 dataclass 的 replace 创建新实例
    return replace(
        task_node,
        system_prompt=new_system_prompt,
        allowed_tools=new_allowed_tools,
        model=task_node.model or role.preferred_model,  # 如果任务未指定模型，使用角色的首选模型
    )
