"""特性开关系统——借鉴 Claude Code 的 feature() 门控模式。

运行时开关，通过 config.toml [features] 段或环境变量控制。
未启用的特性对应的重型模块不会被导入（懒加载）。
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import ClassVar

logger = logging.getLogger(__name__)

# 所有已注册的特性名及其默认值
_KNOWN_FEATURES: dict[str, bool] = {
    "semantic_drift": False,
    "context_quarantine": False,
    "validation_gate": False,
    "semantic_reset": False,
    "blackboard": False,
    "redundancy_detector": False,
    "convergence": False,
}


@dataclass
class FeatureFlags:
    """特性开关集合。默认全部关闭，显式启用才会激活。"""

    semantic_drift: bool = False
    context_quarantine: bool = False
    validation_gate: bool = False
    semantic_reset: bool = False
    blackboard: bool = False
    redundancy_detector: bool = False
    convergence: bool = False

    # 类级别单例持有者（非 dataclass 字段），initialize() 设置
    _instance: ClassVar[FeatureFlags | None] = None

    @classmethod
    def from_dict(cls, data: dict[str, bool]) -> FeatureFlags:
        """从字典构建，忽略未知键。"""
        known_keys = set(_KNOWN_FEATURES)
        filtered = {k: bool(v) for k, v in data.items() if k in known_keys}
        return cls(**filtered)

    @classmethod
    def initialize(cls, flags: FeatureFlags) -> None:
        """设置全局单例。"""
        cls._instance = flags
        enabled = [k for k in _KNOWN_FEATURES if getattr(flags, k, False)]
        if enabled:
            logger.info("特性开关已启用: %s", ", ".join(enabled))

    @classmethod
    def reset(cls) -> None:
        """重置全局单例（测试用）。"""
        cls._instance = None


def is_enabled(feature_name: str) -> bool:
    """检查特性是否启用。

    优先级：环境变量 > 全局单例 > 默认值(False)
    环境变量格式：ORCHESTRATOR_FEATURE_<UPPER_NAME>=true/false
    """
    env_key = f"ORCHESTRATOR_FEATURE_{feature_name.upper()}"
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return env_val.lower() in ("true", "1", "yes")

    flags = FeatureFlags._instance
    if flags is not None:
        return getattr(flags, feature_name, False)

    if feature_name not in _KNOWN_FEATURES:
        logger.warning("未知的特性名: '%s'（已知特性: %s）", feature_name, ", ".join(sorted(_KNOWN_FEATURES)))

    return _KNOWN_FEATURES.get(feature_name, False)
