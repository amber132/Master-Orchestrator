"""Manifest generation for simple mode."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from .simple_model import SimpleManifest, SimpleRun, SimpleWorkItem

if TYPE_CHECKING:
    from .simple_runtime import SimpleRuntimeLayout


def write_simple_manifest(
    layout: SimpleRuntimeLayout,
    run: SimpleRun,
    manifest: SimpleManifest,
    items: list[SimpleWorkItem],
) -> Path:
    manifest_path = layout.manifests / "simple_manifest.json"
    failed_items = [item for item in items if item.status.value in {"failed", "blocked"}]
    uncovered = sorted(item.target for item in items if item.status.value != "succeeded")
    manifest.uncovered_targets = uncovered
    manifest_path.write_text(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")

    (layout.root / "failed_items.jsonl").write_text(
        "\n".join(json.dumps(item.to_dict(), ensure_ascii=False) for item in failed_items),
        encoding="utf-8",
    )
    (layout.root / "uncovered_targets.txt").write_text("\n".join(uncovered), encoding="utf-8")
    (layout.root / "bucket_stats.json").write_text(
        json.dumps({name: stats.to_dict() for name, stats in manifest.bucket_stats.items()}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    summary_lines = [
        f"# Simple Run {run.run_id}",
        "",
        f"- Status: {run.status.value}",
        f"- Isolation: {run.isolation_mode}",
        f"- Total items: {manifest.total_items}",
        f"- Completed: {manifest.completed_items}",
        f"- Failed: {manifest.failed_items}",
        f"- Retried success: {manifest.retried_success_items}",
        f"- Total cost: ${manifest.total_cost_usd:.4f}",
    ]
    execution_stats = manifest.execution_stats or {}
    stage_timing_stats = manifest.stage_timing_stats or {}
    if execution_stats:
        summary_lines.extend([
            "",
            "## Execution Stats",
            "",
            f"- Attempts total: {execution_stats.get('attempts_total', 0)}",
            f"- Tool uses total: {execution_stats.get('tool_uses_total', 0)}",
            f"- Token input total: {execution_stats.get('token_input_total', 0)}",
            f"- Token output total: {execution_stats.get('token_output_total', 0)}",
            f"- CLI duration avg ms: {execution_stats.get('cli_duration_ms_avg', 0.0)}",
            f"- CLI duration max ms: {execution_stats.get('cli_duration_ms_max', 0.0)}",
            f"- CLAUDE_HOME ready avg ms: {execution_stats.get('claude_home_ready_ms_avg', 0.0)}",
            f"- CLAUDE_HOME ready max ms: {execution_stats.get('claude_home_ready_ms_max', 0.0)}",
            f"- Execution wall avg ms: {execution_stats.get('execution_wall_ms_avg', 0.0)}",
            f"- Execution wall max ms: {execution_stats.get('execution_wall_ms_max', 0.0)}",
            f"- Max-turn watchdog hits: {execution_stats.get('max_turns_exceeded_attempts', 0)}",
        ])
    if stage_timing_stats:
        summary_lines.extend(["", "## Stage Timing", ""])
        for stage in ("prepare", "execute", "validate"):
            stats = stage_timing_stats.get(stage) or {}
            summary_lines.append(
                f"- {stage}: count={stats.get('count', 0)} avg_ms={stats.get('avg_ms', 0.0)} max_ms={stats.get('max_ms', 0.0)}"
            )
    (layout.root / "simple_summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return manifest_path
