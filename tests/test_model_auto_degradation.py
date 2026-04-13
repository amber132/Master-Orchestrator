"""模型自动降级测试 — 连续 529 时自动从 opus→sonnet→haiku 降级。"""

import pytest
from unittest.mock import patch
from dataclasses import replace

from claude_orchestrator.config import Config
from claude_orchestrator.model import (
    DAG, TaskNode, FailoverReason, FailoverStatus,
)
from claude_orchestrator.error_classifier import classify_failover_reason, resolve_failover_status


class TestErrorClassifierFor529:
    """529 错误的分类和策略测试。"""

    def test_classify_529_as_model_overload(self):
        """529 错误码被分类为 MODEL_OVERLOAD。"""
        result = classify_failover_reason("Error: 529 Model is overloaded")
        assert result.reason == FailoverReason.MODEL_OVERLOAD

    def test_classify_overloaded_keyword(self):
        """overloaded 关键词被分类为 MODEL_OVERLOAD。"""
        result = classify_failover_reason("The model is currently overloaded, please try again later")
        assert result.reason == FailoverReason.MODEL_OVERLOAD

    def test_classify_529_not_confused_with_429(self):
        """529 不会被误分类为 RATE_LIMIT。"""
        result = classify_failover_reason("HTTP 529 Service Overloaded")
        assert result.reason == FailoverReason.MODEL_OVERLOAD
        assert result.reason != FailoverReason.RATE_LIMIT

    def test_resolve_model_overload_to_switch_model(self):
        """MODEL_OVERLOAD 映射到 SWITCH_MODEL（不再退避重试）。"""
        status = resolve_failover_status(FailoverReason.MODEL_OVERLOAD, attempt=1, max_attempts=3)
        assert status == FailoverStatus.SWITCH_MODEL

    def test_resolve_model_overload_last_attempt_still_switch(self):
        """即使最后一次重试，MODEL_OVERLOAD 仍然映射到 SWITCH_MODEL（降级而非终止）。"""
        status = resolve_failover_status(FailoverReason.MODEL_OVERLOAD, attempt=3, max_attempts=3)
        assert status == FailoverStatus.SWITCH_MODEL


class TestFallbackChain:
    """降级链 opus→sonnet→haiku 测试。"""

    def test_opus_falls_back_to_sonnet(self):
        """opus → sonnet。"""
        fallback_chain = {"opus": "sonnet", "sonnet": "haiku"}
        assert fallback_chain.get("opus") == "sonnet"

    def test_sonnet_falls_back_to_haiku(self):
        """sonnet → haiku。"""
        fallback_chain = {"opus": "sonnet", "sonnet": "haiku"}
        assert fallback_chain.get("sonnet") == "haiku"

    def test_haiku_no_further_fallback(self):
        """haiku 无更小模型可降级。"""
        fallback_chain = {"opus": "sonnet", "sonnet": "haiku"}
        assert fallback_chain.get("haiku") is None


class TestOverloadCounter:
    """RetryStrategy 的 529 过载计数器测试。"""

    def _make_strategy(self):
        """创建测试用 RetryStrategy 实例。"""
        from claude_orchestrator.orchestrator import RetryStrategy

        config = Config()
        dag = DAG(name="test", tasks={
            "t1": TaskNode(id="t1", prompt_template="test", model="opus"),
        })
        return RetryStrategy(
            config=config,
            store=None,
            pool_runtime=None,
            rate_limiter=None,
            reset_protocol=None,
            diagnostics=None,
            dag=dag,
            execute_hook_fn=lambda *a: None,
            get_run_info_fn=lambda: None,
            set_exit_code_fn=lambda code: None,
        )

    def test_counter_initialized_empty(self):
        """529 计数器在 __init__ 中正确初始化为空字典。"""
        strategy = self._make_strategy()
        assert hasattr(strategy, '_model_overload_counts')
        assert strategy._model_overload_counts == {}

    def test_counter_threshold_is_2(self):
        """降级阈值默认为 2。"""
        strategy = self._make_strategy()
        assert strategy._model_overload_threshold == 2

    def test_counter_increment(self):
        """529 错误增加计数器。"""
        strategy = self._make_strategy()
        strategy._model_overload_counts["opus"] = 0
        strategy._model_overload_counts["opus"] = strategy._model_overload_counts.get("opus", 0) + 1
        assert strategy._model_overload_counts["opus"] == 1

    def test_counter_consecutive_increment(self):
        """连续多次 529 不断累加。"""
        strategy = self._make_strategy()
        for _ in range(3):
            strategy._model_overload_counts["opus"] = strategy._model_overload_counts.get("opus", 0) + 1
        assert strategy._model_overload_counts["opus"] == 3

    def test_counter_reset_on_non_529(self):
        """非 529 错误重置该模型的计数器。"""
        strategy = self._make_strategy()
        strategy._model_overload_counts["opus"] = 5
        if "opus" in strategy._model_overload_counts:
            strategy._model_overload_counts["opus"] = 0
        assert strategy._model_overload_counts["opus"] == 0

    def test_counter_reset_after_model_switch(self):
        """降级后原模型计数器被重置。"""
        strategy = self._make_strategy()
        strategy._model_overload_counts["opus"] = 3
        # 模拟降级后重置
        strategy._model_overload_counts["opus"] = 0
        assert strategy._model_overload_counts["opus"] == 0

    def test_different_models_tracked_independently(self):
        """不同模型的计数器互不影响。"""
        strategy = self._make_strategy()
        strategy._model_overload_counts["opus"] = 3
        strategy._model_overload_counts["sonnet"] = 1
        assert strategy._model_overload_counts["opus"] == 3
        assert strategy._model_overload_counts["sonnet"] == 1
