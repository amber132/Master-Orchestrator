from __future__ import annotations

import pytest

from claude_orchestrator.exceptions import FieldTransformError
from claude_orchestrator.field_transform import _extract_by_path, apply_transforms
from claude_orchestrator.model import FieldTransform, TaskNode


def test_apply_transforms_returns_empty_dict_when_no_rules() -> None:
    task = TaskNode(id="scan", prompt_template="noop")

    assert apply_transforms(task, {"scan": {"ok": True}}) == {}


def test_apply_transforms_extracts_values_and_uses_defaults() -> None:
    task = TaskNode(
        id="process",
        prompt_template="noop",
        transform=[
            FieldTransform(source_path="scan.output.files", target_key="files"),
            FieldTransform(source_path="config.timeout", target_key="timeout", default=300),
        ],
    )

    outputs = {
        "scan": {"output": {"files": ["a.py", "b.py"]}},
        "config": {},
    }

    assert apply_transforms(task, outputs) == {
        "files": ["a.py", "b.py"],
        "timeout": 300,
    }


def test_apply_transforms_raises_with_context_when_required_path_missing() -> None:
    task = TaskNode(
        id="process",
        prompt_template="noop",
        transform=[FieldTransform(source_path="scan.output.files", target_key="files")],
    )

    with pytest.raises(FieldTransformError) as exc_info:
        apply_transforms(task, {"scan": {"result": {}}})

    exc = exc_info.value
    assert exc.context["task_id"] == "process"
    assert exc.context["source_path"] == "scan.output.files"
    assert exc.context["target_key"] == "files"


def test_extract_by_path_raises_type_error_for_non_dict_intermediate_value() -> None:
    with pytest.raises(TypeError, match="expected dict, got list"):
        _extract_by_path({"scan": []}, "scan.output")
