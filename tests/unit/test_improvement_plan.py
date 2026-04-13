from __future__ import annotations

from pathlib import Path

from master_orchestrator.auto_model import ImprovementPriority, ImprovementSource
from master_orchestrator.improvement_plan import load_improvement_plan


def test_load_improvement_plan_from_json(tmp_path: Path) -> None:
    plan_path = tmp_path / "phase0.json"
    plan_path.write_text(
        """
{
  "title": "Phase 0",
  "proposals": [
    {
      "title": "收紧 review score 一致性",
      "description": "让 verdict 和 score 保持一致。",
      "rationale": "避免误判收敛。",
      "priority": "critical",
      "affected_files": ["master_orchestrator/review_engine.py"],
      "estimated_complexity": "small"
    },
    {
      "title": "启用 required flow gate",
      "description": "把 required flow 作为发布门槛。",
      "rationale": "持续监控核心回归。",
      "priority": "high",
      "affected_files": ["master_orchestrator/flow_matrix.py"],
      "estimated_complexity": "medium"
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )

    proposals = load_improvement_plan(plan_path)

    assert len(proposals) == 2
    assert proposals[0].title == "收紧 review score 一致性"
    assert proposals[0].priority is ImprovementPriority.CRITICAL
    assert proposals[0].source is ImprovementSource.PLAN_FILE
    assert proposals[1].affected_files == ["master_orchestrator/flow_matrix.py"]


def test_load_improvement_plan_filters_by_phase_tag(tmp_path: Path) -> None:
    plan_path = tmp_path / "phase-filter.json"
    plan_path.write_text(
        """
{
  "proposals": [
    {
      "title": "Phase 0 item",
      "description": "desc",
      "rationale": "why",
      "priority": "high",
      "phase": "phase0"
    },
    {
      "title": "Phase 1 item",
      "description": "desc",
      "rationale": "why",
      "priority": "medium",
      "phase": "phase1"
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )

    proposals = load_improvement_plan(plan_path, phase_filter="phase0")

    assert len(proposals) == 1
    assert proposals[0].title == "Phase 0 item"
