from __future__ import annotations

from claude_orchestrator.prompt_prefix import extract_shared_prefix


def test_extract_shared_prefix_returns_empty_string_for_no_prompts() -> None:
    assert extract_shared_prefix([]) == ""


def test_extract_shared_prefix_returns_single_prompt_verbatim() -> None:
    assert extract_shared_prefix(["short"]) == "short"


def test_extract_shared_prefix_truncates_to_separator_boundary() -> None:
    prompts = [
        "Intro line\nShared section\nTask A",
        "Intro line\nShared section\nTask B",
    ]

    result = extract_shared_prefix(prompts, min_length=5)

    assert result == "Intro line\nShared section\n"


def test_extract_shared_prefix_requires_minimum_length() -> None:
    prompts = ["abc1", "abc2"]

    assert extract_shared_prefix(prompts, min_length=4, separator="") == ""
