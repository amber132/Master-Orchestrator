"""Status rendering for simple mode."""

from __future__ import annotations

import json
from collections import Counter

from .simple_model import SimpleManifest, SimpleRun, SimpleWorkItem


def build_simple_status_payload(
    run: SimpleRun,
    manifest: dict | None,
    items: list[SimpleWorkItem],
    events: list[dict],
) -> dict:
    counts = Counter(item.status.value for item in items)
    active_counts = {
        key: counts.get(key, 0)
        for key in ("ready", "preparing", "executing", "validating", "retry_wait")
    }
    terminal_counts = {
        key: counts.get(key, 0)
        for key in ("succeeded", "failed", "blocked", "skipped")
    }
    payload = run.to_dict()
    payload["items"] = {
        item.item_id: {
            "target": item.target,
            "bucket": item.bucket,
            "status": item.status.value,
            "attempt_state": item.attempt_state.to_dict(),
            "priority": item.priority,
        }
        for item in items
    }
    payload["counts"] = dict(counts)
    payload["active_counts"] = active_counts
    payload["terminal_counts"] = terminal_counts
    payload["manifest"] = manifest or {}
    payload["recent_events"] = events[:20]
    return payload


def render_simple_status_text(run: SimpleRun, manifest: dict | None, items: list[SimpleWorkItem], events: list[dict]) -> str:
    payload = build_simple_status_payload(run, manifest, items, events)
    lines = [
        f"Run:     {run.run_id}",
        "Mode:    simple",
        f"Status:  {run.status.value}",
        f"Started: {run.started_at}",
    ]
    if run.finished_at:
        lines.append(f"Ended:   {run.finished_at}")
    lines.extend([
        f"Dir:     {run.working_dir}",
        f"Isolation: {run.isolation_mode}",
        "",
        "Items:",
    ])
    for status, count in sorted(payload["counts"].items()):
        lines.append(f"  {status}: {count}")
    lines.extend([
        "",
        "Active pipeline:",
    ])
    for status, count in payload["active_counts"].items():
        lines.append(f"  {status}: {count}")
    if manifest:
        lines.extend([
            "",
            f"Coverage: {manifest.get('completed_items', 0)}/{manifest.get('total_items', 0)}",
            f"Retries succeeded: {manifest.get('retried_success_items', 0)}",
        ])
        execution_stats = manifest.get("execution_stats") or {}
        stage_timing_stats = manifest.get("stage_timing_stats") or {}
        if execution_stats:
            lines.extend([
                "",
                "Execution stats:",
                f"  attempts_total: {execution_stats.get('attempts_total', 0)}",
                f"  tool_uses_total: {execution_stats.get('tool_uses_total', 0)}",
                f"  token_input_total: {execution_stats.get('token_input_total', 0)}",
                f"  token_output_total: {execution_stats.get('token_output_total', 0)}",
                f"  cli_duration_ms_avg: {execution_stats.get('cli_duration_ms_avg', 0.0)}",
                f"  cli_duration_ms_max: {execution_stats.get('cli_duration_ms_max', 0.0)}",
                f"  claude_home_ready_ms_avg: {execution_stats.get('claude_home_ready_ms_avg', 0.0)}",
                f"  claude_home_ready_ms_max: {execution_stats.get('claude_home_ready_ms_max', 0.0)}",
                f"  execution_wall_ms_avg: {execution_stats.get('execution_wall_ms_avg', 0.0)}",
                f"  execution_wall_ms_max: {execution_stats.get('execution_wall_ms_max', 0.0)}",
                f"  max_turns_exceeded_attempts: {execution_stats.get('max_turns_exceeded_attempts', 0)}",
            ])
        if stage_timing_stats:
            lines.extend(["", "Stage timing stats:"])
            for stage in ("prepare", "execute", "validate"):
                stats = stage_timing_stats.get(stage) or {}
                lines.append(
                    f"  {stage}: count={stats.get('count', 0)} avg_ms={stats.get('avg_ms', 0.0)} max_ms={stats.get('max_ms', 0.0)}"
                )
    if events:
        lines.extend(["", "Recent events:"])
        for event in events[:10]:
            bucket = f" [{event['bucket']}]" if event.get("bucket") else ""
            item = f" ({event['item_id']})" if event.get("item_id") else ""
            lines.append(f"  - {event['event_type']}{bucket}{item}: {event['data']}")
    return "\n".join(lines)


def render_simple_status_json(run: SimpleRun, manifest: dict | None, items: list[SimpleWorkItem], events: list[dict]) -> str:
    return json.dumps(build_simple_status_payload(run, manifest, items, events), indent=2, ensure_ascii=False)
