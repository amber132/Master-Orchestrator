"""风险评估与人工审批升级模块"""

from __future__ import annotations

import logging
import sys
from enum import Enum
from pathlib import Path
from typing import Any

from master_orchestrator.model import TaskNode

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """任务风险等级"""
    LOW = "low"           # 低风险：只读操作、信息查询等
    MEDIUM = "medium"     # 中风险：常规修改、非关键配置变更
    HIGH = "high"         # 高风险：批量操作、关键配置修改
    CRITICAL = "critical" # 严重风险：生产环境操作、破坏性操作


class EscalationManager:
    """风险评估与人工审批管理器"""

    # 高风险关键词（出现在 tags 或 prompt 中）
    HIGH_RISK_KEYWORDS = {
        'production', 'prod', 'deploy', 'release', 'publish',
        'delete', 'drop', 'remove', 'truncate', 'destroy',
        'modify_config', 'change_config', 'update_config',
        'database', 'db_migration', 'schema_change',
        'critical', 'destructive', 'irreversible'
    }

    # 中风险关键词
    MEDIUM_RISK_KEYWORDS = {
        'refactor', 'rename', 'move', 'restructure',
        'config', 'settings', 'environment',
        'batch', 'bulk', 'mass_update'
    }

    # 敏感路径模式
    SENSITIVE_PATHS = {
        '/etc', '/usr', '/var', '/sys', '/boot',
        'C:\\Windows', 'C:\\Program Files',
        '.git', '.env', 'config', 'secrets'
    }

    def __init__(self, approval_mode: str = 'interactive'):
        """
        初始化升级管理器

        Args:
            approval_mode: 审批模式
                - 'interactive': 交互式审批（stdin）
                - 'file': 文件审批（写入待审批文件，等待外部确认）
                - 'auto': 自动审批（LOW/MEDIUM 自动通过，HIGH/CRITICAL 拒绝）
        """
        self.approval_mode = approval_mode

    def assess_risk(self, task_node: TaskNode, context: dict[str, Any] | None = None) -> RiskLevel:
        """
        评估任务风险等级

        Args:
            task_node: 任务节点
            context: 上下文信息（可选，包含运行时状态）

        Returns:
            RiskLevel: 风险等级
        """
        risk_score = 0
        reasons = []

        # 1. 检查 tags 中的关键词
        tags_lower = [tag.lower() for tag in task_node.tags]
        for tag in tags_lower:
            if any(keyword in tag for keyword in self.HIGH_RISK_KEYWORDS):
                risk_score += 3
                reasons.append(f"高风险标签: {tag}")
            elif any(keyword in tag for keyword in self.MEDIUM_RISK_KEYWORDS):
                risk_score += 1
                reasons.append(f"中风险标签: {tag}")

        # 2. 检查 prompt 中的关键词
        prompt_lower = task_node.prompt_template.lower()
        high_risk_matches = [kw for kw in self.HIGH_RISK_KEYWORDS if kw in prompt_lower]
        medium_risk_matches = [kw for kw in self.MEDIUM_RISK_KEYWORDS if kw in prompt_lower]

        if high_risk_matches:
            risk_score += 2
            reasons.append(f"Prompt 包含高风险关键词: {', '.join(high_risk_matches[:3])}")
        if medium_risk_matches:
            risk_score += 1
            reasons.append(f"Prompt 包含中风险关键词: {', '.join(medium_risk_matches[:3])}")

        # 3. 检查工作目录是否敏感
        if task_node.working_dir:
            wd = task_node.working_dir.lower()
            if any(sensitive in wd for sensitive in self.SENSITIVE_PATHS):
                risk_score += 2
                reasons.append(f"敏感工作目录: {task_node.working_dir}")

        # 4. 检查是否独占执行（可能影响较大）
        if task_node.is_sequential:
            risk_score += 1
            reasons.append("独占执行模式")

        # 5. 检查错误处理策略
        if task_node.error_policy and task_node.error_policy.on_error == 'continue-on-error':
            risk_score += 1
            reasons.append("错误继续执行策略")

        # 6. 检查是否有工具限制（无限制可能风险更高）
        if task_node.allowed_tools is None:
            risk_score += 1
            reasons.append("无工具限制")

        # 7. 根据上下文调整（如果提供）
        if context:
            # 如果是生产环境
            if context.get('environment') == 'production':
                risk_score += 2
                reasons.append("生产环境")

            # 如果影响多个下游任务
            downstream_count = context.get('downstream_count', 0)
            if downstream_count > 5:
                risk_score += 1
                reasons.append(f"影响 {downstream_count} 个下游任务")

        # 根据总分判定风险等级
        if risk_score >= 5:
            return RiskLevel.CRITICAL
        elif risk_score >= 3:
            return RiskLevel.HIGH
        elif risk_score >= 1:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW

    def should_escalate(self, risk_level: RiskLevel, auto_config: dict[str, Any] | None = None) -> bool:
        """
        判断是否需要人工审批

        Args:
            risk_level: 风险等级
            auto_config: 自动审批配置（可选）
                - auto_approve_threshold: 自动通过的最高风险等级

        Returns:
            bool: True 表示需要人工审批
        """
        # auto 模式：全部自动通过，不需要审批流程
        if self.approval_mode == 'auto':
            return False

        # 如果提供了自定义配置
        if auto_config and 'auto_approve_threshold' in auto_config:
            threshold = auto_config['auto_approve_threshold']
            threshold_map = {
                'low': RiskLevel.LOW,
                'medium': RiskLevel.MEDIUM,
                'high': RiskLevel.HIGH,
                'critical': RiskLevel.CRITICAL
            }
            threshold_level = threshold_map.get(threshold.lower(), RiskLevel.LOW)

            # 风险等级高于阈值时需要审批
            risk_order = [RiskLevel.LOW, RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL]
            return risk_order.index(risk_level) > risk_order.index(threshold_level)

        # 默认：MEDIUM 及以上需要审批
        return risk_level in (RiskLevel.MEDIUM, RiskLevel.HIGH, RiskLevel.CRITICAL)

    def request_approval(
        self,
        task_id: str,
        risk_level: RiskLevel,
        reason: str,
        task_node: TaskNode | None = None
    ) -> bool:
        """
        请求人工审批

        Args:
            task_id: 任务 ID
            risk_level: 风险等级
            reason: 风险原因说明
            task_node: 任务节点（可选，用于显示更多信息）

        Returns:
            bool: True 表示批准，False 表示拒绝
        """
        if self.approval_mode == 'auto':
            # auto 模式：全部自动通过（无人值守场景）
            # CRITICAL 级别记录警告但仍然通过
            if risk_level == RiskLevel.CRITICAL:
                logger.warning("[自动审批] 任务 %s 风险等级为 CRITICAL，自动批准但请注意", task_id)
            logger.info("[自动审批] 任务 %s - 风险等级: %s - 自动批准", task_id, risk_level.value)
            return True

        elif self.approval_mode == 'file':
            # 文件审批模式：写入待审批文件
            approval_file = Path(f".escalation_approval_{task_id}.txt")
            with approval_file.open('w', encoding='utf-8') as f:
                f.write(f"任务 ID: {task_id}\n")
                f.write(f"风险等级: {risk_level.value}\n")
                f.write(f"风险原因: {reason}\n")
                if task_node:
                    f.write(f"\n任务详情:\n")
                    f.write(f"  Prompt: {task_node.prompt_template[:200]}...\n")
                    f.write(f"  Tags: {', '.join(task_node.tags)}\n")
                    f.write(f"  Working Dir: {task_node.working_dir}\n")
                f.write(f"\n请在此文件中添加一行 'APPROVED' 或 'REJECTED' 来确认审批结果\n")

            print(f"\n[人工审批] 任务 {task_id} 需要审批")
            print(f"  风险等级: {risk_level.value}")
            print(f"  审批文件: {approval_file}")
            print(f"  请编辑文件并添加 'APPROVED' 或 'REJECTED'，然后按 Enter 继续...")
            input()

            # 读取审批结果
            if approval_file.exists():
                content = approval_file.read_text(encoding='utf-8').upper()
                approved = 'APPROVED' in content
                approval_file.unlink()  # 删除审批文件
                return approved
            else:
                print(f"[警告] 未找到审批文件，默认拒绝")
                return False

        else:  # interactive 模式
            # 交互式审批
            print(f"\n{'='*60}")
            print(f"[人工审批请求] 任务: {task_id}")
            print(f"{'='*60}")
            print(f"风险等级: {risk_level.value.upper()}")
            print(f"风险原因: {reason}")

            if task_node:
                print(f"\n任务详情:")
                print(f"  Prompt: {task_node.prompt_template[:200]}...")
                if task_node.tags:
                    print(f"  Tags: {', '.join(task_node.tags)}")
                if task_node.working_dir:
                    print(f"  Working Dir: {task_node.working_dir}")

            print(f"\n是否批准执行此任务？")
            print(f"  [y] 批准")
            print(f"  [n] 拒绝")
            print(f"  [s] 跳过（标记为 SKIPPED）")
            print(f"{'='*60}")

            while True:
                try:
                    response = input("请选择 [y/n/s]: ").strip().lower()
                    if response in ('y', 'yes'):
                        print(f"[✓] 任务 {task_id} 已批准")
                        return True
                    elif response in ('n', 'no'):
                        print(f"[✗] 任务 {task_id} 已拒绝")
                        return False
                    elif response in ('s', 'skip'):
                        print(f"[→] 任务 {task_id} 已跳过")
                        return False  # 跳过也返回 False，由调用方处理
                    else:
                        print("无效输入，请输入 y/n/s")
                except (EOFError, KeyboardInterrupt):
                    print(f"\n[✗] 审批被中断，默认拒绝")
                    return False

    def get_approval_summary(self, task_id: str, approved: bool, risk_level: RiskLevel) -> str:
        """
        生成审批结果摘要

        Args:
            task_id: 任务 ID
            approved: 是否批准
            risk_level: 风险等级

        Returns:
            str: 审批结果摘要
        """
        status = "✓ 已批准" if approved else "✗ 已拒绝"
        return f"[审批] 任务 {task_id} ({risk_level.value}) - {status}"
