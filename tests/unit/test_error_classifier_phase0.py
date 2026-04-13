from __future__ import annotations

from claude_orchestrator.error_classifier import classify_error
from claude_orchestrator.model import ErrorCategory


def test_classify_error_treats_model_overload_as_retryable() -> None:
    assert classify_error("server overloaded with 529", 1) is ErrorCategory.RETRYABLE


def test_classify_error_treats_context_length_overflow_as_retryable() -> None:
    assert classify_error("context_length_exceeded for current model", 1) is ErrorCategory.RETRYABLE


def test_classify_error_treats_invalid_api_key_as_needs_human() -> None:
    assert classify_error("invalid_api_key provided", 1) is ErrorCategory.NEEDS_HUMAN


def test_classify_error_uses_timeout_exit_code() -> None:
    assert classify_error("", 124) is ErrorCategory.RETRYABLE


def test_classify_error_uses_sigint_exit_code() -> None:
    assert classify_error("", 130) is ErrorCategory.NEEDS_HUMAN
