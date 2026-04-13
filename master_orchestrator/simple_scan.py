"""Scan reporting helpers for simple mode."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .simple_loader import SimpleLoadResult


@dataclass
class SimpleScanReport:
    total_items: int
    buckets: dict[str, int]
    samples: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_items": self.total_items,
            "buckets": self.buckets,
            "samples": self.samples,
            "warnings": self.warnings,
            "source_summary": self.source_summary,
        }

    def render_text(self) -> str:
        lines = [
            "Simple Scan Preview",
            f"Total items: {self.total_items}",
            "",
            "Buckets:",
        ]
        for bucket, count in sorted(self.buckets.items()):
            lines.append(f"  {bucket}: {count}")
        if self.samples:
            lines.extend(["", "Samples:"])
            for sample in self.samples:
                lines.append(f"  - [{sample['item_type']}] {sample['target']} (bucket={sample['bucket']}, priority={sample['priority']})")
        if self.warnings:
            lines.extend(["", "Warnings:"])
            for warning in self.warnings:
                lines.append(f"  - {warning}")
        return "\n".join(lines)


def build_scan_report(load_result: SimpleLoadResult, sample_size: int = 10) -> SimpleScanReport:
    buckets = dict(load_result.source_summary.get("buckets", {}))
    samples = [
        {
            "item_id": item.item_id,
            "item_type": item.item_type.value,
            "target": item.target,
            "bucket": item.bucket,
            "priority": item.priority,
        }
        for item in load_result.items[:sample_size]
    ]
    return SimpleScanReport(
        total_items=len(load_result.items),
        buckets=buckets,
        samples=samples,
        warnings=list(load_result.warnings),
        source_summary=json.loads(json.dumps(load_result.source_summary, ensure_ascii=False)),
    )
