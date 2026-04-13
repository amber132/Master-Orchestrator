from __future__ import annotations

from pathlib import Path

from claude_orchestrator.autonomous import AutonomousController
from claude_orchestrator.auto_model import GoalStatus, SafeStopReason
from claude_orchestrator.config import Config
from claude_orchestrator.exceptions import BudgetExhaustedError
from claude_orchestrator.store import Store


def test_execute_marks_safe_stop_on_budget_exhaustion(tmp_path: Path, monkeypatch) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    cfg = Config()
    cfg.checkpoint.db_path = str(tmp_path / "state.db")

    with Store(cfg.checkpoint.db_path) as store:
        controller = AutonomousController(
            goal="修复预算耗尽路径",
            working_dir=str(workdir),
            config=cfg,
            store=store,
        )

        monkeypatch.setattr(controller, "_save_state", lambda: None)
        monkeypatch.setattr(controller, "_print_final_summary", lambda: None)

        def fake_run_pipeline_stage(stage_fn, *, stage, **kwargs):
            if stage == "analysis":
                raise BudgetExhaustedError("hard limit reached")
            return None

        monkeypatch.setattr(controller, "_run_pipeline_stage", fake_run_pipeline_stage)

        state = controller.execute()

    assert state.status is GoalStatus.SAFE_STOP
    assert state.safe_stop_reason is SafeStopReason.BUDGET_EXHAUSTED
    assert state.failure_categories.get("budget_exhausted") == 1
