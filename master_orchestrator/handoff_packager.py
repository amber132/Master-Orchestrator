"""Create human-readable and machine-readable local handoff artifacts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .auto_model import GoalState
from .delivery_manifest import DeliveryManifest
from .runtime_layout import RuntimeLayout


@dataclass
class HandoffWriteResult:
    json_path: Path
    markdown_path: Path


class HandoffPackager:
    def write(
        self,
        layout: RuntimeLayout,
        state: GoalState | None,
        manifest: DeliveryManifest,
    ) -> HandoffWriteResult:
        layout.handoff.mkdir(parents=True, exist_ok=True)
        json_path = layout.handoff / "delivery_manifest.json"
        markdown_path = layout.handoff / "handoff_summary.md"

        payload = manifest.to_dict()
        if state is not None:
            payload["goal"] = state.goal_text
            payload["goal_status"] = state.status.value

        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        lines = [
            "# 本地交付摘要",
            "",
            f"- Run ID: {manifest.run_id}",
            f"- 状态: {manifest.status}",
            f"- 分支: {', '.join(manifest.branch_names) if manifest.branch_names else '无'}",
            f"- Worktree: {', '.join(manifest.worktree_paths) if manifest.worktree_paths else '无'}",
            f"- 备份: {manifest.backup_summary or '无'}",
            f"- 验证: {manifest.verification_summary or '无'}",
        ]
        if manifest.recommended_merge_order:
            lines.append(f"- 推荐合并顺序: {', '.join(manifest.recommended_merge_order)}")
        if state is not None and state.goal_text:
            lines.append(f"- 目标: {state.goal_text}")
        markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return HandoffWriteResult(json_path=json_path, markdown_path=markdown_path)
