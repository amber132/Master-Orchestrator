"""Unified task execution entrypoint for Claude and Codex providers."""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from .claude_cli import run_claude_task
from .codex_cli import run_codex_task
from .model import TaskNode, TaskResult
from .provider_router import normalize_task_executor, select_provider

logger = logging.getLogger(__name__)


def _phase_for_task(task: TaskNode) -> str:
    executor_config = task.executor_config or {}
    phase = str(executor_config.get("phase", "") or "").strip()
    if phase:
        return phase
    if task.task_type == "cpu":
        return "execute"
    return "execute"


def run_agent_task(
    *,
    task: TaskNode,
    prompt: str,
    config: Any,
    limits: Any,
    budget_tracker: Any,
    working_dir: str | None,
    on_progress: Any,
    audit_logger: Any = None,
    rate_limiter: Any = None,
    cli_provider: str | None = None,
    phase_provider_overrides: dict[str, str] | None = None,
) -> TaskResult:
    normalized = normalize_task_executor(task)
    if not hasattr(config, "claude") or not hasattr(config, "codex") or not hasattr(config, "routing"):
        result = run_claude_task(
            task=replace(normalized, provider="claude"),
            prompt=prompt,
            claude_config=getattr(config, "claude", config),
            limits=limits,
            budget_tracker=budget_tracker,
            working_dir=working_dir,
            on_progress=on_progress,
            audit_logger=audit_logger,
            rate_limiter=rate_limiter,
        )
        result.provider_used = "claude"
        return result

    phase = _phase_for_task(normalized)
    decision = select_provider(
        config=config,
        task=normalized,
        phase=phase,
        cli_provider=cli_provider,
        phase_provider_overrides=phase_provider_overrides,
    )
    logger.info(
        "[agent_cli] task=%s phase=%s selected_provider=%s source=%s fallback_allowed=%s task_provider=%s task_type=%s",
        normalized.id,
        phase,
        decision.provider,
        decision.source,
        decision.fallback_allowed,
        normalized.provider,
        normalized.type,
    )

    primary = decision.provider
    providers = [primary]
    if normalized.provider == "auto" and decision.fallback_allowed:
        providers.append("codex" if primary == "claude" else "claude")

    last_result: TaskResult | None = None
    for provider in providers:
        if provider == "codex":
            attempt_task = replace(normalized, provider="codex")
            last_result = run_codex_task(
                task=attempt_task,
                prompt=prompt,
                codex_config=config.codex,
                limits=limits,
                budget_tracker=budget_tracker,
                working_dir=working_dir,
                on_progress=on_progress,
                audit_logger=audit_logger,
                rate_limiter=rate_limiter,
            )
            last_result.provider_used = "codex"
        else:
            attempt_task = replace(normalized, provider="claude")
            last_result = run_claude_task(
                task=attempt_task,
                prompt=prompt,
                claude_config=config.claude,
                limits=limits,
                budget_tracker=budget_tracker,
                working_dir=working_dir,
                on_progress=on_progress,
                audit_logger=audit_logger,
                rate_limiter=rate_limiter,
            )
            last_result.provider_used = "claude"

        if last_result.status.value == "success":
            return last_result
        if normalized.provider != "auto":
            return last_result

    assert last_result is not None
    return last_result
