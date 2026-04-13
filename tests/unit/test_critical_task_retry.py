from __future__ import annotations

from claude_orchestrator.model import ErrorCategory, ErrorPolicy, TaskNode
from claude_orchestrator.error_classifier import should_retry_with_priority


def test_critical_task_gets_aggressive_retry():
    """主链路任务遇到 RETRYABLE 错误时激进重试。"""
    policy = ErrorPolicy(on_error="fail-fast", classify_errors=True)
    assert should_retry_with_priority(
        ErrorCategory.RETRYABLE, policy, is_critical=True
    ) is True


def test_critical_task_retries_non_retryable():
    """主链路任务遇到 NON_RETRYABLE 仍然尝试重试。"""
    policy = ErrorPolicy(on_error="fail-fast", classify_errors=True)
    assert should_retry_with_priority(
        ErrorCategory.NON_RETRYABLE, policy, is_critical=True
    ) is True


def test_critical_task_stops_on_needs_human():
    """主链路任务遇到 NEEDS_HUMAN 不重试。"""
    policy = ErrorPolicy(on_error="fail-fast", classify_errors=True)
    assert should_retry_with_priority(
        ErrorCategory.NEEDS_HUMAN, policy, is_critical=True
    ) is False


def test_non_critical_task_fails_fast_on_non_retryable():
    """辅助任务遇到 NON_RETRYABLE 立即失败。"""
    policy = ErrorPolicy(on_error="fail-fast", classify_errors=True)
    assert should_retry_with_priority(
        ErrorCategory.NON_RETRYABLE, policy, is_critical=False
    ) is False


def test_non_critical_task_retries_retryable():
    """辅助任务遇到 RETRYABLE 仍然重试。"""
    policy = ErrorPolicy(on_error="fail-fast", classify_errors=True)
    assert should_retry_with_priority(
        ErrorCategory.RETRYABLE, policy, is_critical=False
    ) is True


def test_task_node_default_not_critical():
    """TaskNode 默认 is_critical=False。"""
    node = TaskNode(id="x", prompt_template="test")
    assert node.is_critical is False
