from __future__ import annotations

from types import SimpleNamespace

from master_orchestrator.model import TaskNode
from master_orchestrator.provider_router import (
    ProviderDecision,
    normalize_task_executor,
    parse_phase_provider_overrides,
    select_provider,
)


def _config() -> object:
    return SimpleNamespace(
        routing=SimpleNamespace(
            default_provider="auto",
            auto_fallback=True,
            phase_defaults={
                "decompose": "claude",
                "review": "claude",
                "discover": "claude",
                "execute": "codex",
                "simple": "codex",
                "self_improve": "claude",
            },
        )
    )


def test_parse_phase_provider_overrides_returns_mapping() -> None:
    parsed = parse_phase_provider_overrides(["execute=codex", "review=claude"])

    assert parsed == {"execute": "codex", "review": "claude"}


def test_select_provider_prefers_cli_override() -> None:
    decision = select_provider(
        config=_config(),
        task=TaskNode(id="t1", prompt_template="x"),
        phase="review",
        cli_provider="codex",
        phase_provider_overrides={"review": "claude"},
    )

    assert decision == ProviderDecision(provider="codex", fallback_allowed=False, source="cli")


def test_select_provider_uses_phase_default_for_auto_task() -> None:
    decision = select_provider(
        config=_config(),
        task=TaskNode(id="t1", prompt_template="x"),
        phase="execute",
        cli_provider=None,
        phase_provider_overrides={},
    )

    assert decision == ProviderDecision(provider="codex", fallback_allowed=True, source="phase_default")


def test_normalize_task_executor_maps_legacy_executor_types() -> None:
    task = TaskNode(id="t1", prompt_template="x", type="codex_cli", provider="auto")

    normalized = normalize_task_executor(task)

    assert normalized.type == "agent_cli"
    assert normalized.provider == "codex"
