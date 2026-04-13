from __future__ import annotations

from claude_orchestrator.link_resolver import _extract_value, inject_link_context, resolve_links
from claude_orchestrator.model import LinkMapping, TaskNode


class _NonJsonValue:
    def __repr__(self) -> str:
        return "<NonJsonValue>"


def test_extract_value_supports_nested_dicts_and_arrays() -> None:
    data = {"data": {"users": [{"profile": {"name": "alice"}}]}}

    assert _extract_value(data, "data.users[0].profile.name") == "alice"


def test_resolve_links_skips_missing_or_invalid_links() -> None:
    task = TaskNode(
        id="consumer",
        prompt_template="noop",
        links=[
            LinkMapping(upstream_task="producer", output_path="result.user_id", input_key="user_id"),
            LinkMapping(upstream_task="missing", output_path="result.user_id", input_key="missing_user_id"),
            LinkMapping(upstream_task="producer", output_path="result.profile.name", input_key="user_name"),
        ],
    )
    outputs = {"producer": {"result": {"user_id": 123}}}

    resolved = resolve_links(task, outputs)

    assert resolved == {"user_id": 123}


def test_inject_link_context_serializes_json_payload() -> None:
    prompt = inject_link_context("Process user", {"user_id": 123, "user_name": "alice"})

    assert prompt.startswith("Process user\n\n## 上游数据\n")
    assert '"user_id": 123' in prompt
    assert '"user_name": "alice"' in prompt


def test_inject_link_context_falls_back_to_repr_for_non_json_values() -> None:
    prompt = inject_link_context("Process user", {"payload": _NonJsonValue()})

    assert "## 上游数据" in prompt
    assert "<NonJsonValue>" in prompt


def test_inject_link_context_returns_original_prompt_for_empty_payload() -> None:
    assert inject_link_context("Process user", {}) == "Process user"
