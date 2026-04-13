"""细粒度错误分类测试：验证新增 FailoverReason 和 classify_detailed 函数。"""

from __future__ import annotations

from claude_orchestrator.error_classifier import classify_detailed, classify_failover_reason
from claude_orchestrator.model import FailoverReason


# ── classify_failover_reason 新增类型检测 ──


def test_classify_model_overload():
    """529 过载应识别为 MODEL_OVERLOAD 而非 RATE_LIMIT。"""
    reason = classify_failover_reason("Error: 529 Overloaded", 1)
    assert reason == FailoverReason.MODEL_OVERLOAD


def test_classify_model_overload_capacity():
    """capacity 关键词应识别为 MODEL_OVERLOAD。"""
    reason = classify_failover_reason("server at capacity", 1)
    assert reason == FailoverReason.MODEL_OVERLOAD


def test_classify_invalid_model():
    """model_not_found 应识别为 INVALID_MODEL。"""
    reason = classify_failover_reason("Error: model_not_found: invalid-model", 1)
    assert reason == FailoverReason.INVALID_MODEL


def test_classify_invalid_model_does_not_exist():
    """does not exist 应识别为 INVALID_MODEL。"""
    reason = classify_failover_reason("model does not exist", 1)
    assert reason == FailoverReason.INVALID_MODEL


def test_classify_tool_use_mismatch():
    """tool_use/tool_result 不匹配应识别为 TOOL_USE_MISMATCH。"""
    reason = classify_failover_reason("tool_use does not match tool_result", 1)
    assert reason == FailoverReason.TOOL_USE_MISMATCH


def test_classify_content_too_large():
    """content too large 应识别为 CONTENT_TOO_LARGE。"""
    reason = classify_failover_reason("content too large for response", 1)
    assert reason == FailoverReason.CONTENT_TOO_LARGE


def test_classify_credit_exhausted():
    """credit balance 关键词应识别为 CREDIT_EXHAUSTED。"""
    reason = classify_failover_reason("credit balance is low", 1)
    assert reason == FailoverReason.CREDIT_EXHAUSTED


def test_classify_organization_blocked():
    """organization disabled 应识别为 ORGANIZATION_BLOCKED。"""
    reason = classify_failover_reason("organization has been disabled", 1)
    assert reason == FailoverReason.ORGANIZATION_BLOCKED


# ── 新旧类型区分 ──


def test_429_still_rate_limit():
    """429 不应被 MODEL_OVERLOAD 拦截，仍然是 RATE_LIMIT。"""
    reason = classify_failover_reason("Error 429: too many requests", 1)
    assert reason == FailoverReason.RATE_LIMIT


def test_budget_not_credit():
    """budget 关键词应仍为 BUDGET_EXHAUSTED，不被 CREDIT_EXHAUSTED 拦截。"""
    reason = classify_failover_reason("budget limit reached", 1)
    assert reason == FailoverReason.BUDGET_EXHAUSTED


# ── classify_detailed 函数测试 ──


def test_detailed_info_invalid_model():
    """INVALID_MODEL 应为不可恢复、高严重级别、有恢复建议。"""
    info = classify_detailed("model_not_found: foo", 1)
    assert info.reason == FailoverReason.INVALID_MODEL
    assert info.recoverable is False
    assert info.severity == "high"
    assert info.suggested_action != ""


def test_detailed_info_rate_limit_recoverable():
    """RATE_LIMIT 应为可恢复、低严重级别。"""
    info = classify_detailed("rate limit exceeded", 1)
    assert info.recoverable is True
    assert info.severity == "low"


def test_detailed_info_budget_critical():
    """BUDGET_EXHAUSTED 应为 critical 严重级别且不可恢复。"""
    info = classify_detailed("budget exhausted", 1)
    assert info.severity == "critical"
    assert info.recoverable is False


def test_detailed_info_credit_critical():
    """CREDIT_EXHAUSTED 应为 critical 严重级别且不可恢复。"""
    info = classify_detailed("credit exhausted", 1)
    assert info.severity == "critical"
    assert info.recoverable is False


def test_detailed_info_org_blocked_needs_human():
    """ORGANIZATION_BLOCKED 应为不可恢复且高严重级别。"""
    info = classify_detailed("organization disabled", 1)
    assert info.recoverable is False
    assert info.severity == "critical"


def test_detailed_info_tool_mismatch_abort():
    """TOOL_USE_MISMATCH 应为不可恢复。"""
    info = classify_detailed("tool_use mismatch error", 1)
    assert info.reason == FailoverReason.TOOL_USE_MISMATCH
    assert info.recoverable is False
    assert info.severity == "high"


def test_detailed_info_model_overload_recoverable():
    """MODEL_OVERLOAD 应为可恢复、低严重级别。"""
    info = classify_detailed("529 overloaded", 1)
    assert info.recoverable is True
    assert info.severity == "low"


def test_detailed_info_content_too_large():
    """CONTENT_TOO_LARGE 应为可恢复、中等严重级别。"""
    info = classify_detailed("content too large", 1)
    assert info.recoverable is True
    assert info.severity == "medium"
