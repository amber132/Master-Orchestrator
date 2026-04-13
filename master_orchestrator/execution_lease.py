"""Generic cross-process execution lease helpers."""

from __future__ import annotations

from .simple_lease import ExecutionLease, SimpleExecutionLeaseManager


class ExecutionLeaseManager(SimpleExecutionLeaseManager):
    @property
    def max_leases(self) -> int:
        return self._max_leases

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds


__all__ = ["ExecutionLease", "ExecutionLeaseManager"]
