"""autonomous.py 中纯函数和近纯函数的单元测试。

覆盖目标：7 个关键方法，确保文件拆分前有安全网。
"""
from __future__ import annotations

import hashlib
import re

import pytest

from master_orchestrator.autonomous import AutonomousController, validate_goal_length


# ── validate_goal_length ──

class TestValidateGoalLength:
    def test_valid_goal(self):
        ok, msg = validate_goal_length("fix the login bug in auth module")
        assert ok is True
        assert msg == ""

    def test_empty_string(self):
        ok, msg = validate_goal_length("")
        assert ok is False
        assert msg == "Goal too short"

    def test_whitespace_only(self):
        ok, msg = validate_goal_length("   ")
        assert ok is False
        assert msg == "Goal too short"

    def test_exactly_at_threshold(self):
        ok, msg = validate_goal_length("1234567890")
        assert ok is True

    def test_one_below_threshold(self):
        ok, msg = validate_goal_length("123456789")
        assert ok is False

    def test_custom_min_length(self):
        ok, msg = validate_goal_length("hello", min_length=3)
        assert ok is True

    def test_custom_min_length_too_short(self):
        ok, msg = validate_goal_length("hi", min_length=3)
        assert ok is False


# ── _is_simple_goal ──

class TestIsSimpleGoal:
    @pytest.fixture
    def ctrl(self, tmp_path):
        """创建最小化的 AutonomousController 实例用于测试纯方法。"""
        from master_orchestrator.auto_model import AutoConfig, GoalState
        from master_orchestrator.config import load_config

        # 使用 monkeypatch 绕过 __init__ 的复杂依赖
        obj = object.__new__(AutonomousController)
        obj._SIMPLE_GOAL_MAX_LENGTH = 120
        obj._SIMPLE_GOAL_COMPILED = re.compile(
            '|'.join(AutonomousController._SIMPLE_GOAL_PATTERNS), re.IGNORECASE
        )
        return obj

    def test_simple_create(self, ctrl):
        assert ctrl._is_simple_goal("create a README.md file") is True

    def test_simple_fix(self, ctrl):
        assert ctrl._is_simple_goal("fix the login bug") is True

    def test_simple_delete(self, ctrl):
        assert ctrl._is_simple_goal("delete temp files") is True

    def test_empty_string(self, ctrl):
        assert ctrl._is_simple_goal("") is False

    def test_too_long(self, ctrl):
        assert ctrl._is_simple_goal("a" * 121) is False

    def test_exactly_max_length(self, ctrl):
        # 120 chars starting with "create " — should match
        goal = "create " + "x" * 113
        assert len(goal) == 120
        assert ctrl._is_simple_goal(goal) is True

    def test_multi_step_and(self, ctrl):
        assert ctrl._is_simple_goal("create a file and add tests") is False

    def test_multi_step_then(self, ctrl):
        assert ctrl._is_simple_goal("create a file then delete it") is False

    def test_multi_step_chinese(self, ctrl):
        assert ctrl._is_simple_goal("创建文件然后删除") is False

    def test_multi_step_phase(self, ctrl):
        assert ctrl._is_simple_goal("create phase 1 module") is False

    def test_non_simple_verb(self, ctrl):
        # "refactor" is not in the simple patterns
        assert ctrl._is_simple_goal("refactor the auth module") is False


# ── _estimate_goal_complexity ──

class TestEstimateGoalComplexity:
    @pytest.fixture
    def ctrl(self):
        obj = object.__new__(AutonomousController)
        return obj

    def test_very_short_is_simple(self, ctrl):
        assert ctrl._estimate_goal_complexity("fix bug") == "simple"

    def test_exactly_50_chars_is_simple(self, ctrl):
        assert ctrl._estimate_goal_complexity("x" * 50) == "simple"

    def test_just_over_50_no_keywords(self, ctrl):
        # 51 chars, no keywords: length score = 51/100 = 0.51, total = 0.51
        assert ctrl._estimate_goal_complexity("x" * 51) == "simple"

    def test_normal_with_some_keywords(self, ctrl):
        goal = "add react and typescript support to the express api with websocket"
        result = ctrl._estimate_goal_complexity(goal)
        assert result in ("normal", "complex")

    def test_complex_with_many_keywords(self, ctrl):
        goal = (
            "refactor the react and vue components to use typescript, "
            "then migrate the express api to fastapi with postgresql and redis, "
            "after that add docker and kubernetes deployment with aws integration"
        )
        assert ctrl._estimate_goal_complexity(goal) == "complex"

    def test_empty_string(self, ctrl):
        assert ctrl._estimate_goal_complexity("") == "simple"

    def test_whitespace_only(self, ctrl):
        assert ctrl._estimate_goal_complexity("   ") == "simple"


# ── _compute_pattern_hash ──

class TestComputePatternHash:
    def test_basic_hash(self):
        h = AutonomousController._compute_pattern_hash("phase1", "SyntaxError on line 42")
        assert isinstance(h, str)
        assert len(h) == 32  # MD5 hex digest

    def test_normalizes_line_numbers(self):
        h1 = AutonomousController._compute_pattern_hash("p", "error on line 10")
        h2 = AutonomousController._compute_pattern_hash("p", "error on line 999")
        assert h1 == h2  # line numbers should be normalized

    def test_normalizes_windows_paths(self):
        h1 = AutonomousController._compute_pattern_hash("p", "error in C:\\Users\\foo\\bar.py")
        h2 = AutonomousController._compute_pattern_hash("p", "error in D:\\other\\path.py")
        assert h1 == h2

    def test_normalizes_unix_paths(self):
        # 目录部分被规范化为 <path>/，但文件名保留
        h1 = AutonomousController._compute_pattern_hash("p", "error in /home/user/foo.py")
        h2 = AutonomousController._compute_pattern_hash("p", "error in /tmp/other/foo.py")
        assert h1 == h2

    def test_different_unix_filename_different_hash(self):
        h1 = AutonomousController._compute_pattern_hash("p", "error in /home/user/foo.py")
        h2 = AutonomousController._compute_pattern_hash("p", "error in /home/user/bar.py")
        assert h1 != h2

    def test_different_phase_different_hash(self):
        h1 = AutonomousController._compute_pattern_hash("phase1", "same error")
        h2 = AutonomousController._compute_pattern_hash("phase2", "same error")
        assert h1 != h2

    def test_truncates_long_text(self):
        long_error = "x" * 500
        h = AutonomousController._compute_pattern_hash("p", long_error)
        assert isinstance(h, str)
        assert len(h) == 32


# ── _normalize_failure_text ──

class TestNormalizeFailureText:
    def test_basic_normalization(self):
        result = AutonomousController._normalize_failure_text("  Error in  Module  ")
        assert result == "error in module"

    def test_replaces_hex_addresses(self):
        result = AutonomousController._normalize_failure_text("segfault at 0x7fff5fbff8ac")
        assert "0xaddr" in result
        assert "0x7fff5fbff8ac" not in result

    def test_replaces_numbers(self):
        result = AutonomousController._normalize_failure_text("failed at line 42 with code 500")
        assert "42" not in result
        assert "500" not in result
        assert "#" in result

    def test_empty_string(self):
        assert AutonomousController._normalize_failure_text("") == ""

    def test_none_input(self):
        assert AutonomousController._normalize_failure_text(None) == ""

    def test_truncates_to_400(self):
        result = AutonomousController._normalize_failure_text("x" * 1000)
        assert len(result) == 400

    def test_collapses_whitespace(self):
        result = AutonomousController._normalize_failure_text("a   b\n\tc")
        assert result == "a b c"


# ── _canonical_phase_lineage ──

class TestCanonicalPhaseLineage:
    @pytest.fixture
    def ctrl(self):
        obj = object.__new__(AutonomousController)
        return obj

    def test_no_replan_prefix(self, ctrl):
        assert ctrl._canonical_phase_lineage("phase_1") == "phase_1"

    def test_single_replan(self, ctrl):
        assert ctrl._canonical_phase_lineage("replan_phase_1_2") == "phase_1"

    def test_nested_replans(self, ctrl):
        assert ctrl._canonical_phase_lineage("replan_replan_phase_1_2_3") == "phase_1"

    def test_replan_without_numeric_suffix(self, ctrl):
        # "replan_phase_abc" — "abc" is not digit, so stop stripping
        assert ctrl._canonical_phase_lineage("replan_phase_abc") == "phase_abc"

    def test_just_replan(self, ctrl):
        # "replan_" with nothing after — should handle gracefully
        result = ctrl._canonical_phase_lineage("replan_")
        assert isinstance(result, str)

    def test_plain_id_unchanged(self, ctrl):
        assert ctrl._canonical_phase_lineage("execute") == "execute"


# ── _is_downstream_cancellation_error ──

class TestIsDownstreamCancellationError:
    def test_british_spelling(self):
        assert AutonomousController._is_downstream_cancellation_error(
            "cancelled due to upstream failure"
        ) is True

    def test_american_spelling(self):
        assert AutonomousController._is_downstream_cancellation_error(
            "canceled due to upstream failure"
        ) is True

    def test_case_insensitive(self):
        assert AutonomousController._is_downstream_cancellation_error(
            "CANCELLED DUE TO timeout"
        ) is True

    def test_not_cancellation(self):
        assert AutonomousController._is_downstream_cancellation_error(
            "task failed with exit code 1"
        ) is False

    def test_empty_string(self):
        assert AutonomousController._is_downstream_cancellation_error("") is False

    def test_none_input(self):
        assert AutonomousController._is_downstream_cancellation_error(None) is False

    def test_partial_match(self):
        # "cancelled" without "due to" should not match
        assert AutonomousController._is_downstream_cancellation_error(
            "cancelled by user"
        ) is False
