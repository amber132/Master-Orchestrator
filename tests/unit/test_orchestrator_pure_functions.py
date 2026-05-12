"""orchestrator.py 中纯函数和近纯函数的单元测试。

覆盖目标：3 个关键方法，确保文件拆分前有安全网。
"""
from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from master_orchestrator.orchestrator import Orchestrator


# ── _resolve_audit_log_dir ──

class TestResolveAuditLogDir:
    """Orchestrator._resolve_audit_log_dir 静态方法测试。"""

    def _make_config(self, log_dir: str = "") -> MagicMock:
        config = MagicMock()
        config.audit.log_dir = log_dir
        return config

    def test_log_file_in_logs_dir(self, tmp_path):
        """log_file 在 logs/ 目录下 → 返回 父级/evidence/audit"""
        log_file = str(tmp_path / "logs" / "run.log")
        result = Orchestrator._resolve_audit_log_dir(
            self._make_config(), None, log_file
        )
        assert result == tmp_path / "evidence" / "audit"

    def test_log_file_not_in_logs_dir(self, tmp_path):
        """log_file 不在 logs/ 目录下 → 返回 log_file 同级/audit"""
        log_file = str(tmp_path / "output" / "run.log")
        result = Orchestrator._resolve_audit_log_dir(
            self._make_config(), None, log_file
        )
        assert result == tmp_path / "output" / "audit"

    def test_config_absolute_log_dir(self):
        """config.audit.log_dir 是绝对路径 → 直接返回"""
        config = self._make_config("/var/log/audit")
        result = Orchestrator._resolve_audit_log_dir(config, None, None)
        assert result == Path("/var/log/audit")

    def test_config_relative_log_dir_with_working_dir(self, tmp_path):
        """config.audit.log_dir 是相对路径 + working_dir → 基于 working_dir 拼接"""
        config = self._make_config("my_audit")
        result = Orchestrator._resolve_audit_log_dir(
            config, str(tmp_path), None
        )
        assert result == tmp_path / "my_audit"

    def test_config_relative_log_dir_no_working_dir(self):
        """config.audit.log_dir 是相对路径 + 无 working_dir → 基于 '.' 拼接"""
        config = self._make_config("my_audit")
        result = Orchestrator._resolve_audit_log_dir(config, None, None)
        assert result == Path(".") / "my_audit"

    def test_fallback_to_working_dir(self, tmp_path):
        """无 log_file、无 config log_dir → 返回 working_dir"""
        config = self._make_config("")
        result = Orchestrator._resolve_audit_log_dir(
            config, str(tmp_path), None
        )
        assert result == tmp_path

    def test_fallback_to_dot(self):
        """无 log_file、无 config log_dir、无 working_dir → 返回 Path('.')"""
        config = self._make_config("")
        result = Orchestrator._resolve_audit_log_dir(config, None, None)
        assert result == Path(".")

    def test_log_file_takes_priority_over_config(self, tmp_path):
        """log_file 优先级高于 config.audit.log_dir"""
        log_file = str(tmp_path / "logs" / "run.log")
        config = self._make_config("/ignored/path")
        result = Orchestrator._resolve_audit_log_dir(
            config, None, log_file
        )
        assert result == tmp_path / "evidence" / "audit"


# ── _lru_set ──

class TestLruSet:
    """Orchestrator._lru_set 静态方法测试。"""

    def test_basic_insert(self):
        od = OrderedDict()
        Orchestrator._lru_set(od, "a", 1, max_size=3)
        assert od == OrderedDict({"a": 1})

    def test_multiple_inserts_within_limit(self):
        od = OrderedDict()
        Orchestrator._lru_set(od, "a", 1, 3)
        Orchestrator._lru_set(od, "b", 2, 3)
        Orchestrator._lru_set(od, "c", 3, 3)
        assert list(od.keys()) == ["a", "b", "c"]

    def test_eviction_at_max_size(self):
        od = OrderedDict()
        Orchestrator._lru_set(od, "a", 1, 2)
        Orchestrator._lru_set(od, "b", 2, 2)
        Orchestrator._lru_set(od, "c", 3, 2)
        # "a" 应被淘汰
        assert "a" not in od
        assert list(od.keys()) == ["b", "c"]

    def test_update_existing_key_moves_to_end(self):
        od = OrderedDict({"a": 1, "b": 2, "c": 3})
        Orchestrator._lru_set(od, "a", 10, max_size=3)
        # "a" 被移到末尾
        assert list(od.keys()) == ["b", "c", "a"]
        assert od["a"] == 10

    def test_max_size_one(self):
        od = OrderedDict()
        Orchestrator._lru_set(od, "a", 1, 1)
        Orchestrator._lru_set(od, "b", 2, 1)
        assert list(od.keys()) == ["b"]
        assert len(od) == 1

    def test_eviction_preserves_order(self):
        od = OrderedDict()
        for i in range(5):
            Orchestrator._lru_set(od, f"k{i}", i, 3)
        # 只保留最后 3 个
        assert list(od.keys()) == ["k2", "k3", "k4"]


# ── _parse_validation_score ──

class TestParseValidationScore:
    """Orchestrator._parse_validation_score 方法测试。

    虽然是实例方法，但完全不访问 self 属性，
    可以安全地用 object.__new__ 绕过 __init__。
    """

    @pytest.fixture
    def ctrl(self):
        obj = object.__new__(Orchestrator)
        return obj

    def test_none_input(self, ctrl):
        assert ctrl._parse_validation_score(None) is None

    def test_dict_with_score_float(self, ctrl):
        assert ctrl._parse_validation_score({"score": 0.85}) == 0.85

    def test_dict_with_score_string(self, ctrl):
        assert ctrl._parse_validation_score({"score": "0.75"}) == 0.75

    def test_dict_with_score_invalid(self, ctrl):
        assert ctrl._parse_validation_score({"score": "abc"}) is None

    def test_dict_with_dimensions_average(self, ctrl):
        parsed = {"dimensions": {"accuracy": 8, "completeness": 6}}
        assert ctrl._parse_validation_score(parsed) == 7.0

    def test_dict_with_dimensions_empty(self, ctrl):
        assert ctrl._parse_validation_score({"dimensions": {}}) is None

    def test_dict_with_dimensions_non_numeric(self, ctrl):
        assert ctrl._parse_validation_score(
            {"dimensions": {"a": "x", "b": "y"}}
        ) is None

    def test_dict_no_score_no_dimensions(self, ctrl):
        assert ctrl._parse_validation_score({"other": "data"}) is None

    def test_int_input(self, ctrl):
        assert ctrl._parse_validation_score(42) == 42.0

    def test_float_input(self, ctrl):
        assert ctrl._parse_validation_score(3.14) == 3.14

    def test_string_number(self, ctrl):
        assert ctrl._parse_validation_score("0.95") == 0.95

    def test_string_json_dict(self, ctrl):
        assert ctrl._parse_validation_score('{"score": 0.6}') == 0.6

    def test_string_json_invalid(self, ctrl):
        assert ctrl._parse_validation_score("not json") is None

    def test_string_empty(self, ctrl):
        assert ctrl._parse_validation_score("") is None

    def test_bool_input(self, ctrl):
        # bool 是 int 子类，True → 1.0
        assert ctrl._parse_validation_score(True) == 1.0

    def test_list_input(self, ctrl):
        assert ctrl._parse_validation_score([1, 2, 3]) is None

    def test_dict_score_none_value(self, ctrl):
        assert ctrl._parse_validation_score({"score": None}) is None
