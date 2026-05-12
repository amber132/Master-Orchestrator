"""claude_cli.py 中纯函数和近纯函数的单元测试。

覆盖目标：8 个关键方法，确保文件拆分前有安全网。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pytest

from master_orchestrator.claude_cli import (
    CLIResponse,
    StreamProgress,
    _build_model_fallback_candidates,
    _build_task_result,
    _estimate_cost_from_tokens,
    _extract_cost_usd,
    _get_model_pricing,
    _recursive_find_cost,
    _should_use_model_fallback,
    _task_result_metrics,
    _try_float,
)
from master_orchestrator.model import TaskResult, TaskStatus


# ── _try_float ──

class TestTryFloat:
    def test_float_value(self):
        assert _try_float(3.14) == 3.14

    def test_int_value(self):
        assert _try_float(42) == 42.0

    def test_string_number(self):
        assert _try_float("2.5") == 2.5

    def test_string_non_numeric(self):
        assert _try_float("abc") == 0.0

    def test_dict_returns_zero(self):
        assert _try_float({"key": "value"}) == 0.0

    def test_list_returns_zero(self):
        assert _try_float([1, 2, 3]) == 0.0

    def test_tuple_returns_zero(self):
        assert _try_float((1, 2)) == 0.0

    def test_set_returns_zero(self):
        assert _try_float({1, 2}) == 0.0

    def test_none_returns_zero(self):
        assert _try_float(None) == 0.0

    def test_bool_value(self):
        # bool 是 int 子类，True → 1.0
        assert _try_float(True) == 1.0

    def test_zero(self):
        assert _try_float(0) == 0.0

    def test_negative(self):
        assert _try_float(-5.5) == -5.5

    def test_string_zero(self):
        assert _try_float("0") == 0.0


# ── _build_model_fallback_candidates ──

class TestBuildModelFallbackCandidates:
    def test_pro_model(self):
        result = _build_model_fallback_candidates("claude-sonnet-4-pro")
        assert result == ["claude-sonnet-4"]

    def test_non_pro_model(self):
        result = _build_model_fallback_candidates("claude-sonnet-4")
        assert result == []

    def test_empty_string(self):
        result = _build_model_fallback_candidates("")
        assert result == []

    def test_just_pro(self):
        # "-pro" → strip → "" → filtered out
        result = _build_model_fallback_candidates("-pro")
        assert result == []

    def test_pro_pro(self):
        # "pro-pro" → strip "-pro" → "pro"
        result = _build_model_fallback_candidates("pro-pro")
        assert result == ["pro"]

    def test_whitespace(self):
        result = _build_model_fallback_candidates("  ")
        assert result == []

    def test_none_input(self):
        result = _build_model_fallback_candidates(None)
        assert result == []


# ── _get_model_pricing ──

class TestGetModelPricing:
    def test_opus_model(self):
        pricing = _get_model_pricing("claude-opus-4")
        assert pricing == (15.0, 75.0, 1.5, 18.75)

    def test_sonnet_model(self):
        pricing = _get_model_pricing("claude-sonnet-4")
        assert pricing == (3.0, 15.0, 0.3, 3.75)

    def test_haiku_model(self):
        pricing = _get_model_pricing("claude-3-5-haiku")
        assert pricing == (0.80, 4.0, 0.08, 1.0)

    def test_unknown_model_returns_default(self):
        pricing = _get_model_pricing("gpt-4")
        assert pricing == (3.0, 15.0, 0.3, 3.75)

    def test_empty_string_returns_default(self):
        pricing = _get_model_pricing("")
        assert pricing == (3.0, 15.0, 0.3, 3.75)

    def test_partial_match(self):
        # "claude-sonnet-4-pro" 包含 "claude-sonnet-4"
        pricing = _get_model_pricing("claude-sonnet-4-pro")
        assert pricing == (3.0, 15.0, 0.3, 3.75)

    def test_case_insensitive(self):
        pricing = _get_model_pricing("Claude-Opus-4")
        assert pricing == (15.0, 75.0, 1.5, 18.75)


# ── _recursive_find_cost ──

class TestRecursiveFindCost:
    def test_top_level_cost_usd(self):
        assert _recursive_find_cost({"cost_usd": 0.05}) == 0.05

    def test_top_level_total_cost(self):
        assert _recursive_find_cost({"totalCost": 1.5}) == 1.5

    def test_nested_cost(self):
        event = {"usage": {"cost_usd": 0.03}}
        assert _recursive_find_cost(event) == 0.03

    def test_deeply_nested(self):
        event = {"a": {"b": {"c": {"cost": 2.0}}}}
        assert _recursive_find_cost(event) == 2.0

    def test_no_cost_field(self):
        assert _recursive_find_cost({"key": "value", "num": 42}) == 0.0

    def test_skip_session_prefix(self):
        assert _recursive_find_cost({"session_cost_id": 5.0}) == 0.0

    def test_skip_request_prefix(self):
        assert _recursive_find_cost({"request_cost": 5.0}) == 0.0

    def test_max_depth_exceeded(self):
        event = {"a": {"b": {"c": {"d": {"e": {"cost": 1.0}}}}}}
        # depth 5 > max_depth 4
        assert _recursive_find_cost(event, max_depth=4) == 0.0

    def test_string_value_cost(self):
        assert _recursive_find_cost({"cost_usd": "0.07"}) == 0.07

    def test_zero_value_ignored(self):
        # val > 0 才匹配，0 不匹配
        assert _recursive_find_cost({"cost_usd": 0}) == 0.0

    def test_negative_value_ignored(self):
        assert _recursive_find_cost({"cost_usd": -1.0}) == 0.0

    def test_non_dict_input(self):
        assert _recursive_find_cost("not a dict") == 0.0

    def test_usd_key(self):
        assert _recursive_find_cost({"usd": 0.5}) == 0.5

    def test_camel_case_cost_usd(self):
        assert _recursive_find_cost({"costUSD": 0.1}) == 0.1


# ── _should_use_model_fallback ──

class TestShouldUseModelFallback:
    def _make_result(self, status: TaskStatus, error: str = "", output: str = "") -> TaskResult:
        return TaskResult(
            task_id="t1",
            status=status,
            error=error,
            output=output,
            started_at=datetime.now(),
            finished_at=datetime.now(),
            model_used="test",
        )

    def test_pro_model_with_rate_limit_error(self):
        result = self._make_result(
            TaskStatus.FAILED,
            error="429 Too Many Requests - rate limit exceeded"
        )
        assert _should_use_model_fallback(result, "claude-sonnet-4-pro") is True

    def test_pro_model_with_network_error(self):
        result = self._make_result(
            TaskStatus.FAILED,
            error="ECONNREFUSED connection failed"
        )
        assert _should_use_model_fallback(result, "claude-sonnet-4-pro") is True

    def test_pro_model_success_no_fallback(self):
        result = self._make_result(TaskStatus.SUCCESS)
        assert _should_use_model_fallback(result, "claude-sonnet-4-pro") is False

    def test_non_pro_model_no_fallback(self):
        result = self._make_result(
            TaskStatus.FAILED,
            error="429 rate limit"
        )
        assert _should_use_model_fallback(result, "claude-sonnet-4") is False

    def test_pro_model_failed_no_error_text(self):
        result = self._make_result(TaskStatus.FAILED, error="", output="")
        assert _should_use_model_fallback(result, "claude-sonnet-4-pro") is False

    def test_pro_model_with_error_in_output(self):
        result = self._make_result(
            TaskStatus.FAILED,
            error="",
            output="rate limit exceeded please retry"
        )
        assert _should_use_model_fallback(result, "claude-opus-4-pro") is True


# ── _extract_cost_usd ──

class TestExtractCostUsd:
    def test_total_cost_usd(self):
        assert _extract_cost_usd({"total_cost_usd": 0.05}) == 0.05

    def test_cost_usd(self):
        assert _extract_cost_usd({"cost_usd": 0.03}) == 0.03

    def test_cost_camel_case(self):
        assert _extract_cost_usd({"costUSD": 0.02}) == 0.02

    def test_total_cost(self):
        assert _extract_cost_usd({"total_cost": 0.04}) == 0.04

    def test_split_costs(self):
        event = {"input_cost_usd": 0.01, "output_cost_usd": 0.02}
        assert _extract_cost_usd(event) == pytest.approx(0.03)

    def test_cost_in_usage(self):
        event = {"usage": {"cost_usd": 0.06}}
        assert _extract_cost_usd(event) == 0.06

    def test_cost_in_model_usage(self):
        event = {"modelUsage": {
            "claude-sonnet-4": {"costUSD": 0.01},
            "claude-haiku": {"costUSD": 0.005},
        }}
        assert _extract_cost_usd(event) == pytest.approx(0.015)

    def test_fallback_to_token_estimation(self):
        event = {
            "model": "claude-sonnet-4",
            "usage": {
                "input_tokens": 1_000_000,
                "output_tokens": 1_000_000,
            },
        }
        # sonnet: input=3.0, output=15.0 per million
        expected = 3.0 + 15.0
        assert _extract_cost_usd(event) == pytest.approx(expected)

    def test_empty_event(self):
        assert _extract_cost_usd({}) == 0.0

    def test_cost_field_as_dict_safely_ignored(self):
        # cost_usd 是 dict 时，_try_float 返回 0.0，不会崩溃
        assert _extract_cost_usd({"cost_usd": {"nested": "value"}}) == 0.0


# ── _estimate_cost_from_tokens ──

class TestEstimateCostFromTokens:
    def test_snake_case_tokens(self):
        event = {
            "model": "claude-sonnet-4",
            "input_tokens": 1_000_000,
            "output_tokens": 500_000,
        }
        # sonnet: input=3.0, output=15.0
        expected = 3.0 + 7.5
        assert _estimate_cost_from_tokens(event) == pytest.approx(expected)

    def test_camel_case_tokens(self):
        event = {
            "model": "claude-sonnet-4",
            "inputTokens": 1_000_000,
            "outputTokens": 500_000,
        }
        expected = 3.0 + 7.5
        assert _estimate_cost_from_tokens(event) == pytest.approx(expected)

    def test_usage_nested(self):
        event = {
            "model": "claude-sonnet-4",
            "usage": {
                "input_tokens": 1_000_000,
                "output_tokens": 1_000_000,
            },
        }
        assert _estimate_cost_from_tokens(event) == pytest.approx(18.0)

    def test_model_usage_fallback(self):
        event = {
            "modelUsage": {
                "claude-sonnet-4": {
                    "inputTokens": 1_000_000,
                    "outputTokens": 1_000_000,
                },
            },
        }
        assert _estimate_cost_from_tokens(event) == pytest.approx(18.0)

    def test_cache_tokens(self):
        event = {
            "model": "claude-sonnet-4",
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_input_tokens": 1_000_000,
            "cache_creation_input_tokens": 500_000,
        }
        # sonnet: cache_read=0.3, cache_creation=3.75
        expected = 0.3 + 1.875
        assert _estimate_cost_from_tokens(event) == pytest.approx(expected)

    def test_empty_event(self):
        assert _estimate_cost_from_tokens({}) == 0.0

    def test_no_tokens(self):
        event = {"model": "claude-sonnet-4"}
        assert _estimate_cost_from_tokens(event) == 0.0


# ── _task_result_metrics ──

class TestTaskResultMetrics:
    def test_empty_args(self):
        assert _task_result_metrics() == {}

    def test_pid_only(self):
        result = _task_result_metrics(pid=1234)
        assert result == {"pid": 1234}

    def test_with_response(self):
        resp = CLIResponse(
            raw="{}", result="ok", is_error=False,
            cost_usd=0.01, model="test",
            token_input=100, token_output=50, cli_duration_ms=1500.0,
        )
        result = _task_result_metrics(response=resp)
        assert result["token_input"] == 100
        assert result["token_output"] == 50
        assert result["cli_duration_ms"] == 1500.0

    def test_with_progress(self):
        progress = StreamProgress()
        progress.token_input = 200
        progress.token_output = 100
        progress.cli_duration_ms = 2000.0
        progress.tool_uses = 5
        progress.turn_started = 3
        progress.turn_completed = 2
        progress.max_turns_exceeded = False
        progress.cache_read_tokens = 50
        progress.cache_creation_tokens = 25

        result = _task_result_metrics(progress=progress)
        assert result["token_input"] == 200
        assert result["token_output"] == 100
        assert result["tool_uses"] == 5
        assert result["turn_started"] == 3
        assert result["cache_read_tokens"] == 50
        assert result["cache_creation_tokens"] == 25

    def test_response_takes_priority_over_progress(self):
        resp = CLIResponse(
            raw="{}", result="ok", is_error=False,
            cost_usd=0.0, model="",
            token_input=100, token_output=50, cli_duration_ms=1000.0,
        )
        progress = StreamProgress()
        progress.token_input = 999
        progress.token_output = 999
        progress.cli_duration_ms = 9999.0

        result = _task_result_metrics(response=resp, progress=progress)
        # response 的 token 数据优先
        assert result["token_input"] == 100
        assert result["token_output"] == 50
        assert result["cli_duration_ms"] == 1000.0
        # progress 的独有字段仍然存在
        assert "tool_uses" in result


# ── _build_task_result ──

class TestBuildTaskResult:
    def test_basic_construction(self):
        now = datetime.now()
        result = _build_task_result(
            task_id="t1",
            status=TaskStatus.SUCCESS,
            started_at=now,
            finished_at=now,
            model_used="claude-sonnet-4",
            output="done",
        )
        assert result.task_id == "t1"
        assert result.status == TaskStatus.SUCCESS
        assert result.model_used == "claude-sonnet-4"
        assert result.output == "done"

    def test_with_error(self):
        now = datetime.now()
        result = _build_task_result(
            task_id="t2",
            status=TaskStatus.FAILED,
            started_at=now,
            finished_at=now,
            model_used="test",
            error="something went wrong",
        )
        assert result.error == "something went wrong"

    def test_with_cost(self):
        now = datetime.now()
        result = _build_task_result(
            task_id="t3",
            status=TaskStatus.SUCCESS,
            started_at=now,
            finished_at=now,
            model_used="test",
            cost_usd=0.05,
        )
        assert result.cost_usd == 0.05

    def test_with_response_metrics(self):
        resp = CLIResponse(
            raw="{}", result="ok", is_error=False,
            cost_usd=0.01, model="test",
            token_input=100, token_output=50, cli_duration_ms=1500.0,
        )
        now = datetime.now()
        result = _build_task_result(
            task_id="t4",
            status=TaskStatus.SUCCESS,
            started_at=now,
            finished_at=now,
            model_used="test",
            response=resp,
        )
        assert result.token_input == 100
        assert result.token_output == 50
