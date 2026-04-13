"""Retry helpers for simple mode."""

from __future__ import annotations

from .simple_model import SimpleItemStatus
from .simple_store import SimpleStore


def prepare_simple_resume(simple_store: SimpleStore, run_id: str) -> int:
    return simple_store.reset_for_resume(run_id)


def prepare_simple_retry(simple_store: SimpleStore, run_id: str) -> int:
    return simple_store.reset_failed_for_retry(run_id)
