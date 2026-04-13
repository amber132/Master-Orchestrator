"""Provider routing helpers for unified Claude/Codex execution."""

from __future__ import annotations

from dataclasses import dataclass, replace

from .model import TaskNode

_PROVIDERS = {"auto", "claude", "codex"}
_LEGACY_EXECUTOR_TO_PROVIDER = {
    "claude_cli": "claude",
    "codex_cli": "codex",
}


@dataclass(frozen=True)
class ProviderDecision:
    provider: str
    fallback_allowed: bool
    source: str


def parse_phase_provider_overrides(entries: list[str] | None) -> dict[str, str]:
    overrides: dict[str, str] = {}
    for entry in entries or []:
        key, sep, value = entry.partition("=")
        phase = key.strip()
        provider = value.strip()
        if not sep or not phase or provider not in {"claude", "codex", "auto"}:
            raise ValueError(f"Invalid --phase-provider value: {entry!r}")
        overrides[phase] = provider
    return overrides


def normalize_task_executor(task: TaskNode) -> TaskNode:
    task_type = (task.type or "agent_cli").strip() or "agent_cli"
    provider = (task.provider or "auto").strip() or "auto"
    executor_config = dict(task.executor_config or {})

    if task_type in _LEGACY_EXECUTOR_TO_PROVIDER:
        provider = _LEGACY_EXECUTOR_TO_PROVIDER[task_type]
        task_type = "agent_cli"
    elif task_type == "agent_cli" and provider not in _PROVIDERS:
        provider = "auto"

    return replace(task, type=task_type, provider=provider, executor_config=executor_config)


def select_provider(
    *,
    config,
    task: TaskNode,
    phase: str,
    cli_provider: str | None = None,
    phase_provider_overrides: dict[str, str] | None = None,
) -> ProviderDecision:
    normalized = normalize_task_executor(task)
    provider_overrides = phase_provider_overrides or {}

    if cli_provider and cli_provider != "auto":
        return ProviderDecision(provider=cli_provider, fallback_allowed=False, source="cli")
    if normalized.provider in {"claude", "codex"}:
        return ProviderDecision(provider=normalized.provider, fallback_allowed=False, source="task")

    phase_override = provider_overrides.get(phase)
    if phase_override and phase_override != "auto":
        return ProviderDecision(provider=phase_override, fallback_allowed=False, source="phase_override")

    phase_default = (config.routing.phase_defaults or {}).get(phase)
    if phase_default and phase_default != "auto":
        return ProviderDecision(
            provider=phase_default,
            fallback_allowed=bool(config.routing.auto_fallback),
            source="phase_default",
        )

    default_provider = getattr(config.routing, "default_provider", "auto")
    if default_provider in {"claude", "codex"}:
        return ProviderDecision(
            provider=default_provider,
            fallback_allowed=False,
            source="config_default",
        )

    heuristic_provider = "codex" if phase in {"execute", "simple"} else "claude"
    return ProviderDecision(
        provider=heuristic_provider,
        fallback_allowed=bool(config.routing.auto_fallback),
        source="heuristic",
    )
