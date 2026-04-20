from __future__ import annotations

from pathlib import Path

from master_orchestrator.auto_model import AutoConfig, GoalStatus
from master_orchestrator.config import Config
from master_orchestrator.self_improve import SelfImproveController
from master_orchestrator.store import Store


def test_self_improve_preflight_provider_prefers_explicit_provider(tmp_path: Path) -> None:
    cfg = Config()
    controller = SelfImproveController(
        config=cfg,
        auto_config=AutoConfig(),
        working_dir=tmp_path / "work",
        orchestrator_dir=tmp_path,
        preferred_provider="codex",
    )

    assert controller._preflight_provider() == "codex"


def test_self_improve_preflight_provider_uses_phase_override(tmp_path: Path) -> None:
    cfg = Config()
    controller = SelfImproveController(
        config=cfg,
        auto_config=AutoConfig(),
        working_dir=tmp_path / "work",
        orchestrator_dir=tmp_path,
        preferred_provider="claude",
        phase_provider_overrides={"self_improve": "codex"},
    )

    assert controller._preflight_provider() == "codex"


def test_self_improve_phase_execute_passes_provider_preferences(tmp_path: Path, monkeypatch) -> None:
    working_dir = tmp_path / "work"
    orchestrator_dir = tmp_path / "repo"
    working_dir.mkdir()
    orchestrator_dir.mkdir()
    (orchestrator_dir / "config.toml").write_text("", encoding="utf-8")

    cfg = Config()
    cfg.checkpoint.db_path = str(tmp_path / "state.db")
    captured = {}

    class FakeController:
        def __init__(self, cfg_obj):
            captured["preferred_provider"] = cfg_obj.preferred_provider
            captured["phase_provider_overrides"] = dict(cfg_obj.phase_provider_overrides)
            captured["decomposition_model"] = cfg_obj.auto_config.decomposition_model
            captured["review_model"] = cfg_obj.auto_config.review_model
            captured["execution_model"] = cfg_obj.auto_config.execution_model
            self._rate_limiter = type("RateLimiter", (), {"get_state": lambda self: None})()
            self._budget = type("Budget", (), {"spent": 0.0})()

        def execute(self):
            return type(
                "GoalState",
                (),
                {
                    "goal_id": "goal1",
                    "status": GoalStatus.CONVERGED,
                    "total_cost_usd": 0.0,
                    "failure_categories": {},
                },
            )()

    monkeypatch.setattr("master_orchestrator.self_improve.AutonomousController", FakeController)

    with Store(cfg.checkpoint.db_path) as store:
        controller = SelfImproveController(
            config=cfg,
            auto_config=AutoConfig(
                decomposition_model="gpt-5.4",
                review_model="gpt-5.4",
                execution_model="gpt-5.4",
            ),
            working_dir=working_dir,
            orchestrator_dir=orchestrator_dir,
            store=store,
            preferred_provider="codex",
            phase_provider_overrides={"discover": "codex", "review": "codex"},
        )
        controller._execution_repo_dir = orchestrator_dir
        controller._state.goal_history = []
        controller._state.goal_outcomes = []
        assert controller._phase_execute([]) is True

    assert captured["preferred_provider"] == "codex"
    assert captured["phase_provider_overrides"]["discover"] == "codex"
    assert captured["decomposition_model"] == "gpt-5.4"
