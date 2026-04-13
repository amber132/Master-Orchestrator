"""Pool-aware process supervision helpers."""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .config import Config, load_config
from .failover_pool import PoolRuntime, load_failover_pool_config, load_pool_state
from .heartbeat import ENV_HEARTBEAT_FILE

logger = logging.getLogger(__name__)

ENV_POOL_CHILD = "ORCH_POOL_CHILD"
ENV_POOL_STATE_FILE = "ORCH_POOL_STATE_FILE"
ENV_POOL_REQUEST_FILE = "ORCH_POOL_REQUEST_FILE"
ENV_POOL_ACTIVE_PROFILE = "ORCH_POOL_ACTIVE_PROFILE"
ENV_POOL_FIXED_PROFILE = "ORCH_POOL_FIXED_PROFILE"

_HEARTBEAT_POLL_SECONDS = 2.0


def pool_supervisor_required(args: argparse.Namespace) -> bool:
    if not getattr(args, "pool_config", None):
        return False
    if os.environ.get(ENV_POOL_CHILD) == "1":
        return False
    command = getattr(args, "command", "")
    if command in {"run", "resume", "retry-failed", "auto"}:
        return True
    if command == "simple" and getattr(args, "simple_command", "") in {"run", "resume", "retry"}:
        return True
    return False


def build_pool_runtime(args: argparse.Namespace, config: Config) -> PoolRuntime | None:
    pool_config_path = getattr(args, "pool_config", None)
    if not pool_config_path:
        return None
    pool_config = load_failover_pool_config(pool_config_path)
    active_profile = (
        os.environ.get(ENV_POOL_ACTIVE_PROFILE)
        or getattr(args, "pool_profile", None)
        or None
    )
    fixed_profile = (
        os.environ.get(ENV_POOL_FIXED_PROFILE)
        or (
            getattr(args, "pool_profile", None)
            if os.environ.get(ENV_POOL_CHILD) != "1"
            else None
        )
        or None
    )
    runtime = PoolRuntime(
        pool_config,
        active_profile=active_profile,
        state_path=os.environ.get(ENV_POOL_STATE_FILE),
        request_path=os.environ.get(ENV_POOL_REQUEST_FILE),
        fixed_profile=fixed_profile,
    )
    if runtime.state.state_db_path:
        config.checkpoint.db_path = runtime.state.state_db_path
    return runtime


class PoolSupervisor:
    def __init__(self, args: argparse.Namespace, raw_argv: list[str]):
        self._args = args
        self._raw_argv = list(raw_argv)
        self._pool_config_path = Path(args.pool_config).resolve()
        self._pool_config = load_failover_pool_config(self._pool_config_path)
        self._fixed_profile = (getattr(args, "pool_profile", "") or "").strip()
        state_root = self._state_root_dir()
        self._workspace = Path(
            tempfile.mkdtemp(prefix=f"pool_{self._pool_config.pool_id}_", dir=str(state_root))
        )
        self._state_file = self._workspace / "pool_state.json"
        self._request_file = self._workspace / "pool_request.json"
        self._runtime = PoolRuntime(
            self._pool_config,
            active_profile=self._fixed_profile or self._pool_config.primary_profile.name,
            state_path=self._state_file,
            request_path=self._request_file,
            fixed_profile=self._fixed_profile or None,
        )
        self._heartbeat_file = self._workspace / "child_heartbeat"

    def run(self) -> int:
        active_profile = self._runtime.active_profile
        argv = self._initial_child_argv(active_profile)

        while True:
            exit_code = self._run_child(argv, active_profile)
            self._reload_runtime()
            request = self._runtime.read_request()

            if request is not None:
                target_profile = str(request.get("target_profile", "")).strip() or active_profile
                if self._fixed_profile:
                    target_profile = self._fixed_profile
                self._runtime.clear_request()
                self._runtime.activate_profile(target_profile)
                active_profile = target_profile
                argv = self._resume_child_argv(active_profile)
                continue

            if exit_code == 0:
                return 0

            state = self._runtime.state
            execution_id = state.execution_id.strip()
            if not execution_id:
                return exit_code

            if not self._should_failover_for_exit(exit_code):
                return exit_code

            if self._fixed_profile:
                return exit_code

            next_profile = self._choose_takeover_profile(active_profile)
            if next_profile is None:
                return exit_code

            self._runtime.mark_profile_cooldown(active_profile)
            self._runtime.activate_profile(next_profile.name)
            active_profile = next_profile.name
            argv = self._resume_child_argv(active_profile)

    def cleanup(self) -> None:
        shutil.rmtree(self._workspace, ignore_errors=True)

    def _reload_runtime(self) -> None:
        saved = load_pool_state(self._state_file)
        active_profile = saved.active_profile if saved else self._runtime.active_profile
        self._runtime = PoolRuntime(
            self._pool_config,
            active_profile=active_profile,
            state_path=self._state_file,
            request_path=self._request_file,
            fixed_profile=self._fixed_profile or None,
        )

    def _state_root_dir(self) -> Path:
        try:
            base_config = load_config(self._pool_config.primary_profile.config_path)
            state_root = Path(base_config.checkpoint.db_path).resolve().parent
        except Exception:
            state_root = Path.cwd()
        state_root.mkdir(parents=True, exist_ok=True)
        return state_root

    def _child_env(self, active_profile: str) -> dict[str, str]:
        env = os.environ.copy()
        env[ENV_POOL_CHILD] = "1"
        env[ENV_POOL_STATE_FILE] = str(self._state_file)
        env[ENV_POOL_REQUEST_FILE] = str(self._request_file)
        env[ENV_POOL_ACTIVE_PROFILE] = active_profile
        env[ENV_HEARTBEAT_FILE] = str(self._heartbeat_file)
        if self._fixed_profile:
            env[ENV_POOL_FIXED_PROFILE] = self._fixed_profile
        else:
            env.pop(ENV_POOL_FIXED_PROFILE, None)
        return env

    def _profile_config_path(self, profile_name: str) -> str:
        return self._pool_config.get_profile(profile_name).config_path

    def _initial_child_argv(self, active_profile: str) -> list[str]:
        argv = _replace_config_arg(self._raw_argv, self._profile_config_path(active_profile))
        argv = _ensure_option(argv, "--pool-config", str(self._pool_config_path))
        if self._fixed_profile:
            argv = _replace_or_append_option(argv, "--pool-profile", self._fixed_profile)
        else:
            argv = _remove_option(argv, "--pool-profile")
        return argv

    def _resume_child_argv(self, active_profile: str) -> list[str]:
        config_path = self._profile_config_path(active_profile)
        state = self._runtime.state
        common: list[str] = ["-c", config_path]
        if getattr(self._args, "log_file", None):
            common.extend(["--log-file", self._args.log_file])
        if getattr(self._args, "log_dir", None):
            common.extend(["--log-dir", self._args.log_dir])

        if self._args.command in {"run", "resume", "retry-failed"}:
            common.extend(["resume", self._args.dag, "--run-id", state.execution_id])
            if getattr(self._args, "dir", None):
                common.extend(["-d", self._args.dir])
        elif self._args.command == "simple":
            common.extend(["simple", "resume", "--run-id", state.execution_id])
        elif self._args.command == "auto":
            common.extend(["auto", "--resume"])
            if state.runtime_dir:
                common.extend(["--runtime-dir", state.runtime_dir])
            if state.workspace_dir:
                common.extend(["-d", state.workspace_dir])
        else:
            raise ValueError(f"unsupported supervised command: {self._args.command}")

        common.extend(["--pool-config", str(self._pool_config_path)])
        if self._fixed_profile:
            common.extend(["--pool-profile", self._fixed_profile])
        return common

    def _run_child(self, argv: list[str], active_profile: str) -> int:
        cmd = [sys.executable, "-m", "claude_orchestrator.cli", *argv]
        env = self._child_env(active_profile)
        config = load_config(self._profile_config_path(active_profile))
        heartbeat_timeout = max(30, int(config.health.heartbeat_timeout_seconds or 300))
        self._heartbeat_file.unlink(missing_ok=True)
        process = subprocess.Popen(cmd, env=env)
        started_at = time.time()

        while True:
            exit_code = process.poll()
            if exit_code is not None:
                return exit_code
            if self._heartbeat_timed_out(heartbeat_timeout, started_at=started_at):
                logger.warning(
                    "pool child heartbeat stalled for profile=%s timeout=%ss; killing process",
                    active_profile,
                    heartbeat_timeout,
                )
                process.kill()
                process.wait()
                return PoolRuntime.EXIT_CODE_TAKEOVER
            time.sleep(_HEARTBEAT_POLL_SECONDS)

    def _heartbeat_timed_out(self, timeout_seconds: int, *, started_at: float) -> bool:
        try:
            last_beat = self._heartbeat_file.stat().st_mtime
        except OSError:
            return time.time() - started_at > timeout_seconds
        return time.time() - last_beat > timeout_seconds

    def _choose_takeover_profile(self, active_profile: str):
        prefer_primary = self._runtime.state.failback_pending
        return self._runtime.choose_process_profile(
            current_profile=active_profile,
            prefer_primary=prefer_primary,
        )

    @staticmethod
    def _should_failover_for_exit(exit_code: int) -> bool:
        return exit_code in {PoolRuntime.EXIT_CODE_TAKEOVER, PoolRuntime.EXIT_CODE_FAILBACK} or exit_code < 0


def run_pool_supervisor(args: argparse.Namespace, raw_argv: list[str]) -> int:
    supervisor = PoolSupervisor(args, raw_argv)
    try:
        return supervisor.run()
    finally:
        supervisor.cleanup()


def _replace_config_arg(argv: list[str], config_path: str) -> list[str]:
    updated = list(argv)
    for flag in ("-c", "--config"):
        if flag in updated:
            index = updated.index(flag)
            if index + 1 < len(updated):
                updated[index + 1] = config_path
                return updated
    return ["-c", config_path, *updated]


def _ensure_option(argv: list[str], flag: str, value: str) -> list[str]:
    if flag in argv:
        return _replace_or_append_option(argv, flag, value)
    return [*argv, flag, value]


def _replace_or_append_option(argv: list[str], flag: str, value: str) -> list[str]:
    updated = list(argv)
    if flag in updated:
        index = updated.index(flag)
        if index + 1 < len(updated):
            updated[index + 1] = value
        else:
            updated.append(value)
        return updated
    updated.extend([flag, value])
    return updated


def _remove_option(argv: list[str], flag: str) -> list[str]:
    updated = list(argv)
    if flag not in updated:
        return updated
    index = updated.index(flag)
    del updated[index:index + 2]
    return updated
