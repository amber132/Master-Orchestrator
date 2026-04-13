"""Operational controls for simple-mode runs."""

from __future__ import annotations

import os
import signal
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[assignment]

from .model import RunStatus
from .simple_model import SimpleItemStatus, SimpleRun, SimpleRunStatus
from .store import Store


_RUNNER_MARKERS = (" simple run ", " simple resume ", " simple retry ", " simple watch ")
_EXEC_MARKERS = ("claude exec", "claude-exec")


@dataclass
class MatchedProcess:
    pid: int
    kind: str
    cmdline: str
    cwd: str = ""


@dataclass
class SimpleProcessSnapshot:
    runner_processes: list[MatchedProcess]
    exec_processes: list[MatchedProcess]

    @property
    def runner_pids(self) -> list[int]:
        return [proc.pid for proc in self.runner_processes]

    @property
    def exec_pids(self) -> list[int]:
        return [proc.pid for proc in self.exec_processes]

    @property
    def all_processes(self) -> list[MatchedProcess]:
        processes = {proc.pid: proc for proc in self.exec_processes}
        for proc in self.runner_processes:
            processes.setdefault(proc.pid, proc)
        return sorted(processes.values(), key=lambda proc: (proc.kind, proc.pid))

    def to_dict(self) -> dict[str, Any]:
        return {
            "runner_pids": self.runner_pids,
            "exec_pids": self.exec_pids,
            "runner_processes": [asdict(proc) for proc in self.runner_processes],
            "exec_processes": [asdict(proc) for proc in self.exec_processes],
        }


class SimpleRunController:
    def __init__(self, store: Store, *, config_path: str | None = None):
        self._store = store
        self._config_path = str(Path(config_path).resolve()) if config_path else ""
        self._excluded_pids = self._build_excluded_pids()

    def cancel(
        self,
        run_id: str,
        *,
        reason: str = "",
        kill_processes: bool = True,
        force_close: bool = False,
        grace_seconds: float = 5.0,
        kill_timeout_seconds: float = 2.0,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        run = self._require_run(run_id)
        process_snapshot = self.inspect_processes(run)
        before_counts = self._store.get_simple_item_counts(run_id)
        termination: dict[str, Any] = {
            "attempted": False,
            "terminated_pids": [],
            "killed_pids": [],
            "survivor_pids": process_snapshot.runner_pids + process_snapshot.exec_pids,
        }

        if not dry_run:
            if process_snapshot.all_processes and kill_processes:
                termination = self._terminate_processes(
                    process_snapshot,
                    grace_seconds=grace_seconds,
                    kill_timeout_seconds=kill_timeout_seconds,
                )
            survivors = termination["survivor_pids"] if kill_processes else (
                process_snapshot.runner_pids + process_snapshot.exec_pids
            )
            if survivors and not force_close:
                raise RuntimeError(
                    f"simple cancel refused to close run '{run_id}' because live processes remain: {survivors}"
                )
            closed = self._store.close_simple_run_state(
                run_id,
                simple_status=SimpleRunStatus.CANCELLED,
                run_status=RunStatus.CANCELLED,
                item_from_statuses=[
                    SimpleItemStatus.PENDING,
                    SimpleItemStatus.READY,
                    SimpleItemStatus.PREPARING,
                    SimpleItemStatus.EXECUTING,
                    SimpleItemStatus.RUNNING,
                    SimpleItemStatus.VALIDATING,
                    SimpleItemStatus.RETRY_WAIT,
                ],
                item_to_status=SimpleItemStatus.BLOCKED,
            )
            after_counts = self._store.get_simple_item_counts(run_id)
            self._store.save_simple_event(
                run_id,
                "manual_cancel",
                {
                    "reason": reason,
                    "kill_processes": kill_processes,
                    "force_close": force_close,
                    "processes": process_snapshot.to_dict(),
                    "termination": termination,
                    "items_updated": closed["items_updated"],
                    "before_counts": before_counts,
                    "after_counts": after_counts,
                },
                level="warning",
            )
        else:
            after_counts = before_counts

        return {
            "run_id": run_id,
            "action": "cancel",
            "dry_run": dry_run,
            "reason": reason,
            "before_counts": before_counts,
            "after_counts": after_counts,
            "processes": process_snapshot.to_dict(),
            "termination": termination,
        }

    def reconcile(
        self,
        run_id: str,
        *,
        item_status: SimpleItemStatus = SimpleItemStatus.READY,
        run_status: SimpleRunStatus = SimpleRunStatus.FAILED,
        reason: str = "",
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        if run_status not in (SimpleRunStatus.FAILED, SimpleRunStatus.CANCELLED):
            raise ValueError("simple reconcile only supports failed/cancelled run status")
        run = self._require_run(run_id)
        process_snapshot = self.inspect_processes(run)
        if process_snapshot.all_processes and not force:
            raise RuntimeError(
                f"simple reconcile refused to close run '{run_id}' because live processes remain: "
                f"{process_snapshot.runner_pids + process_snapshot.exec_pids}"
            )

        before_counts = self._store.get_simple_item_counts(run_id)
        if not dry_run:
            closed = self._store.close_simple_run_state(
                run_id,
                simple_status=run_status,
                run_status=RunStatus.CANCELLED if run_status == SimpleRunStatus.CANCELLED else RunStatus.FAILED,
                item_from_statuses=[
                    SimpleItemStatus.PREPARING,
                    SimpleItemStatus.EXECUTING,
                    SimpleItemStatus.RUNNING,
                    SimpleItemStatus.VALIDATING,
                    SimpleItemStatus.RETRY_WAIT,
                ],
                item_to_status=item_status,
            )
            after_counts = self._store.get_simple_item_counts(run_id)
            self._store.save_simple_event(
                run_id,
                "manual_reconcile",
                {
                    "reason": reason,
                    "force": force,
                    "requested_item_status": item_status.value,
                    "requested_run_status": run_status.value,
                    "processes": process_snapshot.to_dict(),
                    "items_updated": closed["items_updated"],
                    "before_counts": before_counts,
                    "after_counts": after_counts,
                },
                level="warning",
            )
        else:
            after_counts = before_counts

        return {
            "run_id": run_id,
            "action": "reconcile",
            "dry_run": dry_run,
            "reason": reason,
            "before_counts": before_counts,
            "after_counts": after_counts,
            "requested_item_status": item_status.value,
            "requested_run_status": run_status.value,
            "processes": process_snapshot.to_dict(),
        }

    def inspect_processes(self, run: SimpleRun) -> SimpleProcessSnapshot:
        runner_processes: dict[int, MatchedProcess] = {}
        exec_processes: dict[int, MatchedProcess] = {}
        tokens = self._run_tokens(run)
        for proc in self._iter_processes():
            if proc.pid in self._excluded_pids:
                continue
            padded = f" {proc.cmdline} "
            if any(marker in padded for marker in _RUNNER_MARKERS) and self._matches_scope(proc, tokens):
                runner_processes[proc.pid] = proc

        for runner in list(runner_processes.values()):
            for child in self._children_recursive(runner.pid):
                if child.pid in self._excluded_pids:
                    continue
                if any(marker in child.cmdline for marker in _EXEC_MARKERS):
                    exec_processes[child.pid] = child

        if not exec_processes:
            for proc in self._iter_processes():
                if proc.pid in self._excluded_pids:
                    continue
                if any(marker in proc.cmdline for marker in _EXEC_MARKERS) and self._matches_scope(proc, tokens):
                    exec_processes[proc.pid] = proc

        return SimpleProcessSnapshot(
            runner_processes=sorted(runner_processes.values(), key=lambda proc: proc.pid),
            exec_processes=sorted(exec_processes.values(), key=lambda proc: proc.pid),
        )

    def _terminate_processes(
        self,
        snapshot: SimpleProcessSnapshot,
        *,
        grace_seconds: float,
        kill_timeout_seconds: float,
    ) -> dict[str, Any]:
        ordered = [*snapshot.exec_processes, *snapshot.runner_processes]
        terminated: list[int] = []
        killed: list[int] = []
        survivors: list[int] = []
        if not ordered:
            return {
                "attempted": False,
                "terminated_pids": terminated,
                "killed_pids": killed,
                "survivor_pids": survivors,
            }
        if psutil is not None:
            processes = []
            for proc in ordered:
                try:
                    processes.append(psutil.Process(proc.pid))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            for proc in processes:
                try:
                    proc.terminate()
                    terminated.append(proc.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            _gone, alive = psutil.wait_procs(processes, timeout=max(0.1, grace_seconds))
            for proc in alive:
                try:
                    proc.kill()
                    killed.append(proc.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            _gone, alive = psutil.wait_procs(alive, timeout=max(0.1, kill_timeout_seconds))
            survivors = sorted(proc.pid for proc in alive if proc.is_running())
        else:
            for proc in ordered:
                try:
                    os.kill(proc.pid, signal.SIGTERM)
                    terminated.append(proc.pid)
                except OSError:
                    continue
            if not self._procfs_available():
                return {
                    "attempted": True,
                    "terminated_pids": sorted(set(terminated)),
                    "killed_pids": sorted(set(killed)),
                    "survivor_pids": survivors,
                }
            deadline = time.time() + max(0.1, grace_seconds)
            while time.time() < deadline:
                alive = [proc.pid for proc in ordered if Path(f"/proc/{proc.pid}").exists()]
                if not alive:
                    break
                time.sleep(0.1)
            for pid in [proc.pid for proc in ordered if Path(f"/proc/{proc.pid}").exists()]:
                try:
                    os.kill(pid, signal.SIGKILL)
                    killed.append(pid)
                except OSError:
                    continue
            deadline = time.time() + max(0.1, kill_timeout_seconds)
            while time.time() < deadline:
                alive = [proc.pid for proc in ordered if Path(f"/proc/{proc.pid}").exists()]
                if not alive:
                    break
                time.sleep(0.1)
            survivors = sorted(proc.pid for proc in ordered if Path(f"/proc/{proc.pid}").exists())
        return {
            "attempted": True,
            "terminated_pids": sorted(set(terminated)),
            "killed_pids": sorted(set(killed)),
            "survivor_pids": survivors,
        }

    def _require_run(self, run_id: str) -> SimpleRun:
        run = self._store.get_simple_run(run_id)
        if run is None:
            raise ValueError(f"simple run '{run_id}' not found")
        return run

    def _run_tokens(self, run: SimpleRun) -> tuple[str, str, str]:
        working_dir = str(Path(run.working_dir).resolve()) if run.working_dir else ""
        return run.run_id, working_dir, self._config_path

    @staticmethod
    def _matches_scope(proc: MatchedProcess, tokens: tuple[str, str, str]) -> bool:
        run_id, working_dir, config_path = tokens
        if run_id and run_id in proc.cmdline:
            return True
        if config_path and config_path in proc.cmdline:
            return True
        if working_dir and (working_dir in proc.cmdline or proc.cwd == working_dir):
            return True
        return False

    def _build_excluded_pids(self) -> set[int]:
        excluded = {os.getpid()}
        if psutil is not None:
            try:
                proc = psutil.Process(os.getpid())
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                return excluded
            for parent in proc.parents():
                excluded.add(parent.pid)
            return excluded
        if not self._procfs_available():
            return excluded
        pid = os.getpid()
        while True:
            status_path = Path(f"/proc/{pid}/status")
            try:
                content = status_path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                break
            ppid = 0
            for line in content.splitlines():
                if line.startswith("PPid:"):
                    try:
                        ppid = int(line.split()[1])
                    except (IndexError, ValueError):
                        ppid = 0
                    break
            if ppid <= 1 or ppid in excluded:
                break
            excluded.add(ppid)
            pid = ppid
        return excluded

    def _iter_processes(self) -> list[MatchedProcess]:
        if psutil is not None:
            processes: list[MatchedProcess] = []
            for proc in psutil.process_iter(["pid", "cmdline", "cwd"]):
                try:
                    cmdline_list = proc.info.get("cmdline") or []
                    cmdline = " ".join(str(part) for part in cmdline_list).strip()
                    cwd = str(proc.info.get("cwd") or "")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                if not cmdline:
                    continue
                processes.append(MatchedProcess(pid=proc.pid, kind=self._classify_kind(cmdline), cmdline=cmdline, cwd=cwd))
            return processes

        if not self._procfs_available():
            return []
        processes = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            try:
                data = (entry / "cmdline").read_bytes()
            except OSError:
                continue
            if not data:
                continue
            cmdline = data.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()
            if not cmdline:
                continue
            cwd = ""
            try:
                cwd = str((entry / "cwd").resolve())
            except OSError:
                cwd = ""
            processes.append(MatchedProcess(pid=pid, kind=self._classify_kind(cmdline), cmdline=cmdline, cwd=cwd))
        return processes

    def _children_recursive(self, pid: int) -> list[MatchedProcess]:
        if psutil is not None:
            try:
                proc = psutil.Process(pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                return []
            result: list[MatchedProcess] = []
            for child in proc.children(recursive=True):
                try:
                    cmdline = " ".join(str(part) for part in (child.cmdline() or [])).strip()
                    cwd = child.cwd() if hasattr(child, "cwd") else ""
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                if not cmdline:
                    continue
                result.append(MatchedProcess(pid=child.pid, kind=self._classify_kind(cmdline), cmdline=cmdline, cwd=str(cwd or "")))
            return result

        if not self._procfs_available():
            return []
        children: list[MatchedProcess] = []
        frontier = [pid]
        seen = {pid}
        while frontier:
            parent = frontier.pop()
            for child_pid in self._direct_children(parent):
                if child_pid in seen:
                    continue
                seen.add(child_pid)
                try:
                    data = Path(f"/proc/{child_pid}/cmdline").read_bytes()
                except OSError:
                    continue
                cmdline = data.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()
                if not cmdline:
                    continue
                cwd = ""
                try:
                    cwd = str(Path(f"/proc/{child_pid}/cwd").resolve())
                except OSError:
                    cwd = ""
                children.append(MatchedProcess(pid=child_pid, kind=self._classify_kind(cmdline), cmdline=cmdline, cwd=cwd))
                frontier.append(child_pid)
        return children

    @staticmethod
    def _direct_children(pid: int) -> list[int]:
        if not SimpleRunController._procfs_available():
            return []
        children: list[int] = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                content = (entry / "status").read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for line in content.splitlines():
                if not line.startswith("PPid:"):
                    continue
                try:
                    ppid = int(line.split()[1])
                except (IndexError, ValueError):
                    ppid = -1
                if ppid == pid:
                    children.append(int(entry.name))
                break
        return children

    @staticmethod
    def _classify_kind(cmdline: str) -> str:
        if any(marker in cmdline for marker in _EXEC_MARKERS):
            return "exec"
        return "runner"

    @staticmethod
    def _procfs_available() -> bool:
        return os.name != "nt" and Path("/proc").exists()
