"""Load structured self-improvement plans from files."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from .auto_model import ImprovementPriority, ImprovementProposal, ImprovementSource


def load_improvement_plan(path: str | Path, *, phase_filter: str | None = None) -> list[ImprovementProposal]:
    plan_path = Path(path)
    raw = json.loads(plan_path.read_text(encoding="utf-8"))

    if isinstance(raw, dict):
        proposal_items = raw.get("proposals", [])
    elif isinstance(raw, list):
        proposal_items = raw
    else:
        raise ValueError("Improvement plan must be a JSON object or array")

    proposals: list[ImprovementProposal] = []
    normalized_filter = (phase_filter or "").strip().lower()
    for item in proposal_items:
        if not isinstance(item, dict):
            continue
        item_phase = str(item.get("phase", "")).strip().lower()
        if normalized_filter and item_phase != normalized_filter:
            continue

        proposals.append(
            ImprovementProposal(
                proposal_id=str(item.get("proposal_id") or uuid.uuid4().hex[:8]),
                title=str(item.get("title") or "").strip(),
                description=str(item.get("description") or "").strip(),
                rationale=str(item.get("rationale") or "").strip(),
                source=ImprovementSource.PLAN_FILE,
                priority=ImprovementPriority(str(item.get("priority", "medium")).lower()),
                affected_files=[str(path) for path in item.get("affected_files", [])],
                estimated_complexity=str(item.get("estimated_complexity", "medium") or "medium"),
                evidence=str(item.get("evidence") or ""),
                source_url=str(plan_path),
                source_provider="plan_file",
                evidence_path=str(plan_path),
            )
        )

    return proposals
