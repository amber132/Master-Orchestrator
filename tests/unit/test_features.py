from __future__ import annotations

from claude_orchestrator.features import FeatureFlags, is_enabled


def test_feature_flags_defaults():
    """所有特性默认关闭"""
    flags = FeatureFlags()
    assert flags.semantic_drift is False
    assert flags.context_quarantine is False
    assert flags.validation_gate is False


def test_feature_flags_from_dict():
    """从字典加载特性开关"""
    flags = FeatureFlags.from_dict({
        "semantic_drift": True,
        "context_quarantine": True,
    })
    assert flags.semantic_drift is True
    assert flags.context_quarantine is True
    assert flags.validation_gate is False


def test_is_enabled_without_init():
    """未初始化时所有特性返回 False"""
    FeatureFlags.reset()
    assert is_enabled("semantic_drift") is False


def test_is_enabled_with_init():
    """初始化后按配置返回"""
    flags = FeatureFlags()
    flags.semantic_drift = True
    FeatureFlags.initialize(flags)
    assert is_enabled("semantic_drift") is True
    FeatureFlags.reset()
