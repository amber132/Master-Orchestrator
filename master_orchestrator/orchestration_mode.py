"""编排模式选择器：根据 DAG 特征自动选择最优编排模式。

编排模式：
- CENTRALIZED：中心化编排，适合高依赖密度的 DAG，统一调度和审查
- DECENTRALIZED：去中心化编排，适合无依赖的创意/设计任务，各任务自主执行
- HYBRID：混合模式，适合中等依赖密度或混合类型任务
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from master_orchestrator.model import DAG


class OrchestrationMode(Enum):
    """编排模式枚举"""
    CENTRALIZED = "centralized"      # 中心化编排：高依赖密度，统一调度
    DECENTRALIZED = "decentralized"  # 去中心化编排：无依赖，自主执行
    HYBRID = "hybrid"                # 混合模式：中等依赖或混合类型


class OrchestrationModeSelector:
    """编排模式选择器：根据 DAG 特征自动选择最优编排模式"""

    def __init__(self, density_threshold: float = 0.5):
        """
        初始化选择器。

        Args:
            density_threshold: 依赖密度阈值，超过此值选择 CENTRALIZED 模式
        """
        self.density_threshold = density_threshold

    def select(self, dag: DAG) -> OrchestrationMode:
        """
        根据 DAG 特征选择编排模式。

        判断逻辑：
        1. 任务间依赖密度 > threshold → CENTRALIZED
        2. 无依赖且有 creativity/design 标签 → DECENTRALIZED
        3. 其他 → HYBRID

        Args:
            dag: 待分析的 DAG

        Returns:
            选择的编排模式
        """
        if not dag.tasks:
            return OrchestrationMode.HYBRID

        # 计算依赖密度
        density = self._calculate_dependency_density(dag)

        # 规则 1：高依赖密度 → CENTRALIZED
        if density > self.density_threshold:
            return OrchestrationMode.CENTRALIZED

        # 规则 2：无依赖且有创意/设计标签 → DECENTRALIZED
        if density == 0.0 and self._has_creative_tasks(dag):
            return OrchestrationMode.DECENTRALIZED

        # 规则 3：其他情况 → HYBRID
        return OrchestrationMode.HYBRID

    def _calculate_dependency_density(self, dag: DAG) -> float:
        """
        计算 DAG 的依赖密度。

        依赖密度 = 实际依赖数 / 最大可能依赖数
        最大可能依赖数 = n * (n - 1) / 2（完全图）

        Args:
            dag: 待分析的 DAG

        Returns:
            依赖密度（0.0 ~ 1.0）
        """
        n = len(dag.tasks)
        if n <= 1:
            return 0.0

        # 统计实际依赖数
        total_deps = sum(len(task.depends_on) for task in dag.tasks.values())

        # 计算最大可能依赖数（完全图）
        max_possible_deps = n * (n - 1) / 2

        return total_deps / max_possible_deps if max_possible_deps > 0 else 0.0

    def _has_creative_tasks(self, dag: DAG) -> bool:
        """
        检查 DAG 是否包含创意/设计类任务。

        Args:
            dag: 待分析的 DAG

        Returns:
            是否包含创意/设计标签
        """
        creative_tags = {"creativity", "design", "creative", "brainstorm", "ideation"}
        for task in dag.tasks.values():
            if any(tag.lower() in creative_tags for tag in task.tags):
                return True
        return False

    def get_config_overrides(self, mode: OrchestrationMode) -> dict:
        """
        获取指定编排模式的配置覆盖。

        Args:
            mode: 编排模式

        Returns:
            配置覆盖字典，包含并发数、审查频率等参数
        """
        if mode == OrchestrationMode.CENTRALIZED:
            return {
                "max_parallel": 10,           # 降低并发，便于集中控制
                "review_frequency": "high",   # 高频审查
                "coordination": "strict",     # 严格协调
                "retry_strategy": "aggressive",  # 激进重试
            }
        elif mode == OrchestrationMode.DECENTRALIZED:
            return {
                "max_parallel": 50,           # 提高并发，充分利用并行性
                "review_frequency": "low",    # 低频审查，减少干预
                "coordination": "loose",      # 松散协调
                "retry_strategy": "conservative",  # 保守重试
            }
        else:  # HYBRID
            return {
                "max_parallel": 30,           # 中等并发
                "review_frequency": "medium", # 中等审查频率
                "coordination": "balanced",   # 平衡协调
                "retry_strategy": "balanced", # 平衡重试
            }
