"""Disaster-stop detection for autonomous runs."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field

from .config import LimitsConfig
from .runtime_layout import RuntimeLayout


@dataclass
class CatastrophicCheckResult:
    is_catastrophic: bool
    reason: str = ""
    details: dict = field(default_factory=dict)


class CatastrophicGuard:
    def __init__(self, limits: LimitsConfig):
        self._limits = limits

    def check(self, layout: RuntimeLayout) -> CatastrophicCheckResult:
        if not layout.workspace.exists():
            return CatastrophicCheckResult(True, "workspace is missing")

        try:
            usage = shutil.disk_usage(layout.root)
            free_mb = usage.free // (1024 * 1024)
            if free_mb < self._limits.min_disk_space_mb:
                return CatastrophicCheckResult(True, f"disk free below threshold: {free_mb}MB")
        except OSError as exc:
            return CatastrophicCheckResult(True, f"disk usage check failed: {exc}")

        return CatastrophicCheckResult(False)
