"""Tests for autonomous.py helper methods.

验证 3 个零阶段辅助方法的正确性：
- _diagnose_zero_phase_failure
- _auto_recover_zero_phase
- _estimate_goal_complexity
"""

import pytest

from master_orchestrator.autonomous import AutonomousController


# ── _diagnose_zero_phase_failure 测试 ────────────────────────────


class TestDiagnoseZeroPhaseFailure:
    """测试零阶段失败诊断方法。"""

    _make_ctrl = staticmethod(lambda: type("_T", (), {})())

    def test_transient_timeout(self):
        """TimeoutError 应被归类为临时性错误。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, TimeoutError("connection timed out"), "test_step",
        )
        assert result["category"] == "transient"
        assert result["recoverable"] is True
        assert "test_step" in result["detail"]

    def test_transient_network(self):
        """网络相关错误应被归类为临时性。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, ConnectionError("connection refused"), "decompose",
        )
        assert result["category"] == "transient"
        assert result["recoverable"] is True

    def test_environment_permission(self):
        """PermissionError 应被归类为环境错误。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, PermissionError("access denied"), "execute",
        )
        assert result["category"] == "environment"
        assert result["recoverable"] is False

    def test_environment_filenotfound(self):
        """FileNotFoundError 应被归类为环境错误。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, FileNotFoundError("no such file or directory"), "preflight",
        )
        assert result["category"] == "environment"
        assert result["recoverable"] is False

    def test_logic_json(self):
        """JSON 解析错误应被归类为逻辑错误（可重试）。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, ValueError("json parse error"), "decompose",
        )
        assert result["category"] == "logic"
        assert result["recoverable"] is True

    def test_fatal_unknown(self):
        """未知错误应被归类为致命错误。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, RuntimeError("something unexpected"), "unknown_step",
        )
        assert result["category"] == "fatal"
        assert result["recoverable"] is False

    def test_rate_limit_is_transient(self):
        """429 rate limit 应被归类为临时性错误。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, Exception("rate limit exceeded 429"), "api_call",
        )
        assert result["category"] == "transient"
        assert result["recoverable"] is True

    def test_result_structure(self):
        """返回字典应包含所有必需字段。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._diagnose_zero_phase_failure(
            ctrl, Exception("test"), "step",
        )
        assert "category" in result
        assert "recoverable" in result
        assert "suggested_action" in result
        assert "detail" in result


# ── _estimate_goal_complexity 测试 ───────────────────────────────


class TestEstimateGoalComplexity:
    """测试目标复杂度估算方法。"""

    _make_ctrl = staticmethod(lambda: type("_T", (), {})())

    def test_simple_short(self):
        """短目标应判为 simple。"""
        ctrl = self._make_ctrl()
        assert AutonomousController._estimate_goal_complexity(ctrl, "fix bug") == "simple"

    def test_simple_under_50_chars(self):
        """50 字符以内的目标应判为 simple。"""
        ctrl = self._make_ctrl()
        assert AutonomousController._estimate_goal_complexity(ctrl, "update the readme file") == "simple"

    def test_complex_multi_tech(self):
        """多技术栈 + 多步骤的目标应判为 complex。"""
        ctrl = self._make_ctrl()
        goal = (
            "refactor the codebase and migrate from django to fastapi, "
            "then deploy to kubernetes with redis caching and websocket support"
        )
        result = AutonomousController._estimate_goal_complexity(ctrl, goal)
        assert result == "complex"

    def test_returns_valid_values(self):
        """应只返回 simple / normal / complex。"""
        ctrl = self._make_ctrl()
        valid = {"simple", "normal", "complex"}
        for goal in ["fix typo", "add tests and refactor module", "redesign entire system"]:
            result = AutonomousController._estimate_goal_complexity(ctrl, goal)
            assert result in valid, f"Invalid result '{result}' for goal '{goal}'"


# ── _auto_recover_zero_phase 测试 ────────────────────────────────


class TestAutoRecoverZeroPhase:
    """测试零阶段自动恢复方法。

    由于 _auto_recover_zero_phase 依赖 self._decomposer（需要完整初始化），
    这里只测试无 decomposer 时的降级行为（应返回 False）。
    """

    _make_ctrl = staticmethod(lambda: type("_T", (), {})())

    def test_returns_false_without_decomposer(self):
        """无 decomposer 时应返回 False（所有策略失败）。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._auto_recover_zero_phase(ctrl, "test goal", "context")
        assert result is False

    def test_does_not_crash_with_empty_goal(self):
        """空 goal 不应导致崩溃。"""
        ctrl = self._make_ctrl()
        result = AutonomousController._auto_recover_zero_phase(ctrl, "", "")
        assert result is False

    def test_does_not_crash_with_long_goal(self):
        """超长 goal 不应导致崩溃。"""
        ctrl = self._make_ctrl()
        long_goal = "x" * 10000
        result = AutonomousController._auto_recover_zero_phase(ctrl, long_goal, "context")
        assert result is False
