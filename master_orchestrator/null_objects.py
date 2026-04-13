"""空操作代理，替代被特性开关禁用的组件。

Null Object 模式——当特性开关关闭时，用这些空操作对象替代真实组件，
避免 orchestrator 中到处做 if self._xxx is not None 检查。
"""
from __future__ import annotations

from typing import Any

from .semantic_drift import DriftResult


class NullDriftDetector:
    """语义漂移检测器的空操作替代。始终报告无漂移。"""

    def detect(
        self,
        task_id: str,
        original_prompt: str,
        task_output: str,
        threshold: float = 0.15,
        task_tags: list[str] | None = None,
    ) -> DriftResult:
        return DriftResult(
            task_id=task_id,
            similarity=1.0,
            drifted=False,
            detail="Feature disabled: semantic drift detection skipped",
            blocking=False,
            severity="info",
        )


class NullBlackboard:
    """黑板的空操作替代。所有操作静默吸收，查询返回空列表。"""

    def post(self, category: str, key: str, value: Any, source_task: str) -> None:
        pass

    def query(self, category: str | None = None, key: str | None = None) -> list:
        return []

    def subscribe(self, category: str, callback: Any) -> None:
        pass

    def get_snapshot(self) -> dict:
        return {"facts": [], "hypotheses": [], "intermediate_results": []}


class NullQuarantine:
    """上下文隔离的空操作替代。get_safe_output 直接透传，不隔离任何内容。"""

    def get_safe_output(self, task_id: str, outputs: Any) -> Any:
        return outputs  # 不过滤

    def quarantine(self, task_id: str, reason: str) -> None:
        pass

    def is_quarantined(self, task_id: str) -> bool:
        return False

    def release(self, task_id: str) -> None:
        pass

    def get_quarantine_reason(self, task_id: str) -> str | None:
        return None

    def get_all_quarantined(self) -> dict[str, str]:
        return {}

    def clear(self) -> None:
        pass
