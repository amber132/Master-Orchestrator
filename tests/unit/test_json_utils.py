from __future__ import annotations

import pytest

from claude_orchestrator.json_utils import repair_truncated_json, robust_parse_json


def test_robust_parse_json_parses_direct_json() -> None:
    result = robust_parse_json('{"status": "ok", "count": 2}')

    assert result == {"status": "ok", "count": 2}


def test_robust_parse_json_parses_json_code_block() -> None:
    text = 'Here is the payload:\n```json\n{"items": [{"id": 1}]}\n```'

    result = robust_parse_json(text)

    assert result == {"items": [{"id": 1}]}


def test_robust_parse_json_repairs_truncated_code_block() -> None:
    text = '```json\n{"items": [1, 2, 3]\n'

    result = robust_parse_json(text)

    assert result == {"items": [1, 2, 3]}


def test_robust_parse_json_extracts_embedded_array() -> None:
    text = "prefix text\n[1, 2, 3]\nsuffix text"

    result = robust_parse_json(text)

    assert result == [1, 2, 3]


def test_repair_truncated_json_adds_missing_closers() -> None:
    repaired = repair_truncated_json('{"task": {"id": 1, "tags": ["a", "b"]')

    assert repaired == '{"task": {"id": 1, "tags": ["a", "b"]}}'


def test_robust_parse_json_raises_for_invalid_input() -> None:
    with pytest.raises(ValueError, match="无法解析 JSON"):
        robust_parse_json("not-json")
