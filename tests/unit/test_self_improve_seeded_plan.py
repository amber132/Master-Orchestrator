from __future__ import annotations

from pathlib import Path

from master_orchestrator.auto_model import AutoConfig, ImprovementPriority, ImprovementProposal, ImprovementSource
from master_orchestrator.config import Config
from master_orchestrator.self_improve import SelfImproveController
from master_orchestrator.store import Store


def test_phase_discover_returns_seed_proposals_when_other_sources_disabled(tmp_path: Path) -> None:
    working_dir = tmp_path / "work"
    orchestrator_dir = tmp_path / "repo"
    working_dir.mkdir()
    orchestrator_dir.mkdir()
    (orchestrator_dir / "config.toml").write_text("", encoding="utf-8")

    cfg = Config()
    cfg.checkpoint.db_path = str(tmp_path / "state.db")
    seed = [
        ImprovementProposal(
            proposal_id="seed001",
            title="Seed proposal",
            description="Use the roadmap directly.",
            rationale="Execute the plan through self-improve.",
            source=ImprovementSource.PLAN_FILE,
            priority=ImprovementPriority.HIGH,
        )
    ]

    with Store(cfg.checkpoint.db_path) as store:
        controller = SelfImproveController(
            config=cfg,
            auto_config=AutoConfig(),
            working_dir=working_dir,
            orchestrator_dir=orchestrator_dir,
            store=store,
            skip_introspection=True,
            skip_external=True,
            seed_proposals=seed,
        )

        proposals = controller._phase_discover()

    assert len(proposals) == 1
    assert proposals[0].proposal_id == "seed001"
    assert proposals[0].source is ImprovementSource.PLAN_FILE
