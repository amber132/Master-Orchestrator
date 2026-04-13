from __future__ import annotations

import argparse
from types import SimpleNamespace

from master_orchestrator.auto_model import AutoConfig
from master_orchestrator.cli import _resolve_auto_config
from master_orchestrator.goal_decomposer import GoalDecomposer
from master_orchestrator.model import TaskStatus


def _config():
    return SimpleNamespace(
        claude=SimpleNamespace(default_model="sonnet", max_budget_usd=1000.0),
        codex=SimpleNamespace(default_model="gpt-5.4", max_budget_usd=0.0),
        limits=SimpleNamespace(),
        auto=SimpleNamespace(
            max_hours=24.0,
            max_total_iterations=50,
            max_phase_iterations=50,
            phase_parallelism=8,
            convergence_threshold=0.72,
            convergence_window=3,
            min_convergence_checks=2,
            score_improvement_min=0.05,
            adaptive_tuning_enabled=True,
            max_execution_processes=0,
            execution_lease_db_path="",
            execution_lease_ttl_seconds=300,
        ),
        routing=SimpleNamespace(
            default_provider="auto",
            phase_defaults={
                "decompose": "claude",
                "review": "claude",
                "discover": "claude",
                "execute": "codex",
                "simple": "codex",
                "self_improve": "claude",
                "requirement": "claude",
            },
        ),
    )


def test_resolve_auto_config_uses_codex_models_when_provider_is_codex() -> None:
    args = argparse.Namespace(
        quality_gate=None,
        max_hours=None,
        max_iterations=None,
        max_phase_iterations=None,
        phase_parallelism=None,
        convergence_threshold=None,
        convergence_window=None,
        score_improvement_min=None,
        disable_adaptive_tuning=False,
        max_execution_processes=None,
        provider="codex",
        phase_provider=[],
    )

    auto_cfg = _resolve_auto_config(args, _config())

    assert auto_cfg.decomposition_model == "gpt-5.4"
    assert auto_cfg.review_model == "gpt-5.4"
    assert auto_cfg.execution_model == "gpt-5.4"


def test_goal_decomposer_uses_agent_task_for_provider_aware_config(monkeypatch) -> None:
    captured = {}

    def fake_run_agent_task(**kwargs):
        captured["provider"] = kwargs["cli_provider"]
        task = kwargs["task"]
        captured["task_provider"] = task.provider
        captured["phase"] = task.executor_config["phase"]
        return SimpleNamespace(
            status=TaskStatus.SUCCESS,
            output='{"phases":[{"id":"p1","name":"phase","description":"d","objectives":["o"],"acceptance_criteria":["a"],"tasks":[{"id":"t1","prompt":"do","depends_on":[]}]}]}',
        )

    monkeypatch.setattr("master_orchestrator.goal_decomposer.run_agent_task", fake_run_agent_task)

    decomposer = GoalDecomposer(
        claude_config=SimpleNamespace(default_model="sonnet"),
        limits_config=SimpleNamespace(),
        auto_config=AutoConfig(decomposition_model="gpt-5.4"),
        provider_config=_config(),
        preferred_provider="codex",
    )

    phases = decomposer.decompose("analyze repo", "ctx")

    assert len(phases) == 1
    assert captured["provider"] == "codex"
    assert captured["task_provider"] == "codex"
    assert captured["phase"] == "decompose"
