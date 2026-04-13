"""Failover pool configuration and runtime state helpers."""

from __future__ import annotations

import json
import logging
import tomllib
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import ClaudeConfig, RateLimitConfig, load_config
from .model import TaskNode
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionOverlay:
    default_model: str
    cli_path: str
    requests_per_minute: int
    burst_size: int
    per_model_limits: dict[str, int] = field(default_factory=dict)
    task_env_overrides: dict[str, str] = field(default_factory=dict)
    task_extra_args: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PoolProfile:
    name: str
    priority: int
    config_path: str
    task_switchable: bool = True
    process_switchable: bool = True
    overlay: ExecutionOverlay = field(
        default_factory=lambda: ExecutionOverlay(
            default_model="",
            cli_path="claude",
            requests_per_minute=60,
            burst_size=10,
        )
    )


@dataclass(frozen=True)
class TaskFailoverPolicy:
    allowed_reasons: list[str] = field(default_factory=list)
    switch_after_attempt: int = 1


@dataclass(frozen=True)
class ProcessFailoverPolicy:
    hard_failure_reasons: list[str] = field(default_factory=list)
    hard_failure_threshold: int = 3
    failure_window_seconds: int = 300
    network_failure_threshold: int = 5
    network_failure_window_seconds: int = 600


@dataclass(frozen=True)
class FailoverPoolConfig:
    pool_id: str
    profiles: list[PoolProfile]
    failback_policy: str = "safe-point"
    health_window_seconds: int = 600
    cooldown_seconds: int = 300
    task_policy: TaskFailoverPolicy = field(default_factory=TaskFailoverPolicy)
    process_policy: ProcessFailoverPolicy = field(default_factory=ProcessFailoverPolicy)

    @property
    def primary_profile(self) -> PoolProfile:
        return self.profiles[0]

    def get_profile(self, name: str) -> PoolProfile:
        for profile in self.profiles:
            if profile.name == name:
                return profile
        raise KeyError(name)


@dataclass(frozen=True)
class FailoverEvent:
    execution_id: str
    execution_kind: str
    scope: str
    from_profile: str
    to_profile: str
    reason: str
    trigger_task_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


@dataclass
class PoolState:
    pool_id: str
    active_profile: str
    execution_id: str = ""
    execution_kind: str = ""
    runtime_dir: str = ""
    workspace_dir: str = ""
    state_db_path: str = ""
    failback_pending: bool = False
    last_reason: str = ""
    profile_health: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pool_id": self.pool_id,
            "active_profile": self.active_profile,
            "execution_id": self.execution_id,
            "execution_kind": self.execution_kind,
            "runtime_dir": self.runtime_dir,
            "workspace_dir": self.workspace_dir,
            "state_db_path": self.state_db_path,
            "failback_pending": self.failback_pending,
            "last_reason": self.last_reason,
            "profile_health": self.profile_health,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PoolState":
        return cls(
            pool_id=str(data.get("pool_id", "")),
            active_profile=str(data.get("active_profile", "")),
            execution_id=str(data.get("execution_id", "")),
            execution_kind=str(data.get("execution_kind", "")),
            runtime_dir=str(data.get("runtime_dir", "")),
            workspace_dir=str(data.get("workspace_dir", "")),
            state_db_path=str(data.get("state_db_path", "")),
            failback_pending=bool(data.get("failback_pending", False)),
            last_reason=str(data.get("last_reason", "")),
            profile_health=dict(data.get("profile_health", {})),
        )


class PoolRuntime:
    EXIT_CODE_TAKEOVER = 80
    EXIT_CODE_FAILBACK = 81

    def __init__(
        self,
        config: FailoverPoolConfig,
        *,
        active_profile: str | None = None,
        state_path: str | Path | None = None,
        request_path: str | Path | None = None,
        fixed_profile: str | None = None,
    ):
        self.config = config
        self._state_path = Path(state_path).resolve() if state_path else None
        self._request_path = Path(request_path).resolve() if request_path else None
        self._fixed_profile = (fixed_profile or "").strip()
        saved_state = load_pool_state(self._state_path) if self._state_path else None
        self._state = saved_state or PoolState(
            pool_id=config.pool_id,
            active_profile=self._fixed_profile or active_profile or config.primary_profile.name,
        )
        if self._fixed_profile:
            self._state.active_profile = self._fixed_profile
        elif active_profile:
            self._state.active_profile = active_profile
        self._rate_limiters: dict[str, RateLimiter] = {}
        self._persist_state()

    @property
    def pool_id(self) -> str:
        return self.config.pool_id

    @property
    def active_profile(self) -> str:
        return self._state.active_profile

    @property
    def state(self) -> PoolState:
        return self._state

    def mark_execution(
        self,
        *,
        execution_id: str = "",
        execution_kind: str = "",
        runtime_dir: str = "",
        workspace_dir: str = "",
        state_db_path: str = "",
        active_profile: str | None = None,
    ) -> None:
        if execution_id:
            self._state.execution_id = execution_id
        if execution_kind:
            self._state.execution_kind = execution_kind
        if runtime_dir:
            self._state.runtime_dir = runtime_dir
        if workspace_dir:
            self._state.workspace_dir = workspace_dir
        if state_db_path:
            self._state.state_db_path = state_db_path
        if active_profile:
            self._state.active_profile = active_profile
        self._persist_state()

    def activate_profile(self, profile_name: str) -> None:
        self._state.active_profile = self._fixed_profile or profile_name
        self._state.failback_pending = False
        self._persist_state()

    def claude_config_for_profile(self, base: ClaudeConfig, profile_name: str | None = None) -> ClaudeConfig:
        overlay = self.config.get_profile(profile_name or self.active_profile).overlay
        return ClaudeConfig(
            default_model=overlay.default_model,
            default_timeout=base.default_timeout,
            max_budget_usd=base.max_budget_usd,
            cli_path=overlay.cli_path,
        )

    def rate_limiter_for_profile(self, base: RateLimitConfig, profile_name: str | None = None) -> RateLimiter:
        name = profile_name or self.active_profile
        if name in self._rate_limiters:
            return self._rate_limiters[name]
        overlay = self.config.get_profile(name).overlay
        cfg = RateLimitConfig(
            requests_per_minute=overlay.requests_per_minute,
            per_model_limits=dict(overlay.per_model_limits),
            burst_size=overlay.burst_size,
            rpm_limit=overlay.requests_per_minute,
        )
        limiter = RateLimiter(cfg)
        self._rate_limiters[name] = limiter
        return limiter

    def apply_task_overlay(self, task: TaskNode, profile_name: str | None = None) -> TaskNode:
        overlay = self.config.get_profile(profile_name or self.active_profile).overlay
        env_overrides = dict(task.env_overrides or {})
        env_overrides.update(overlay.task_env_overrides)
        extra_args = list(task.extra_args or [])
        extra_args.extend(arg for arg in overlay.task_extra_args if arg not in extra_args)
        model = task.model or overlay.default_model
        return replace(
            task,
            model=model,
            env_overrides=env_overrides or None,
            extra_args=extra_args or None,
        )

    def choose_task_profile(
        self,
        *,
        current_profile: str | None = None,
        tried_profiles: set[str] | None = None,
        at: datetime | None = None,
    ) -> PoolProfile | None:
        if self._fixed_profile:
            return None
        current = current_profile or self.active_profile
        tried = tried_profiles or set()
        for profile in self.config.profiles:
            if not profile.task_switchable or profile.name == current or profile.name in tried:
                continue
            if not self._is_profile_available(profile.name, at=at):
                continue
            return profile
        return None

    def choose_process_profile(
        self,
        *,
        current_profile: str | None = None,
        prefer_primary: bool = False,
        at: datetime | None = None,
    ) -> PoolProfile | None:
        if self._fixed_profile:
            return None
        current = current_profile or self.active_profile
        if prefer_primary:
            primary = self.config.primary_profile
            if (
                primary.name != current
                and primary.process_switchable
                and self._is_profile_available(primary.name, at=at)
            ):
                return primary
        for profile in self.config.profiles:
            if not profile.process_switchable or profile.name == current:
                continue
            if not self._is_profile_available(profile.name, at=at):
                continue
            return profile
        return None

    def record_failure(self, profile_name: str, reason: str, *, at: datetime | None = None) -> None:
        now = at or datetime.now()
        profile_state = self._state.profile_health.setdefault(profile_name, {})
        failures = profile_state.setdefault("failures", {})
        timestamps = failures.setdefault(reason, [])
        timestamps.append(now.isoformat())
        profile_state["last_failure_at"] = now.isoformat()
        if reason == "auth_expired":
            profile_state["blocked"] = True
        self._prune_failures(profile_state, now)
        self._persist_state()

    def should_trigger_process_takeover(self, profile_name: str, reason: str, *, at: datetime | None = None) -> bool:
        now = at or datetime.now()
        profile_state = self._state.profile_health.setdefault(profile_name, {})
        self._prune_failures(profile_state, now)
        failures = profile_state.get("failures", {})
        window = self.config.process_policy.failure_window_seconds
        count = len(failures.get(reason, []))
        if reason in self.config.process_policy.hard_failure_reasons:
            return count >= self.config.process_policy.hard_failure_threshold
        if reason in {"network_error", "timeout"}:
            network_count = sum(len(failures.get(name, [])) for name in ("network_error", "timeout"))
            return network_count >= self.config.process_policy.network_failure_threshold
        return False

    def mark_profile_cooldown(
        self,
        profile_name: str,
        *,
        at: datetime | None = None,
        seconds: int | None = None,
    ) -> None:
        duration = self.config.cooldown_seconds if seconds is None else seconds
        profile_state = self._state.profile_health.setdefault(profile_name, {})
        if duration <= 0:
            profile_state.pop("cooldown_until", None)
            self._persist_state()
            return

        now = at or datetime.now()
        cooldown_until = now + timedelta(seconds=duration)
        existing = profile_state.get("cooldown_until")
        if existing:
            existing_until = datetime.fromisoformat(existing)
            if existing_until > cooldown_until:
                cooldown_until = existing_until
        profile_state["cooldown_until"] = cooldown_until.isoformat()
        self._persist_state()

    def should_failback(self, *, at: datetime | None = None) -> bool:
        if self._fixed_profile:
            return False
        if self.active_profile == self.config.primary_profile.name:
            return False
        now = at or datetime.now()
        primary_name = self.config.primary_profile.name
        primary_state = self._state.profile_health.get(primary_name, {})
        if not self._is_profile_available(primary_name, at=now):
            return False
        last_failure_at = primary_state.get("last_failure_at")
        if not last_failure_at:
            return True
        return now - datetime.fromisoformat(last_failure_at) >= timedelta(seconds=self.config.health_window_seconds)

    def write_request(self, action: str, *, target_profile: str, reason: str, metadata: dict[str, Any] | None = None) -> None:
        if self._request_path is None:
            return
        now = datetime.now()
        resolved_target = self._fixed_profile or target_profile
        if action == "takeover" and resolved_target and resolved_target != self.active_profile:
            self.mark_profile_cooldown(self.active_profile, at=now)
        self._state.failback_pending = action == "failback"
        self._state.last_reason = reason
        self._persist_state()
        self._request_path.parent.mkdir(parents=True, exist_ok=True)
        self._request_path.write_text(
            json.dumps(
                {
                    "action": action,
                    "target_profile": resolved_target,
                    "reason": reason,
                    "metadata": metadata or {},
                    "created_at": now.isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def clear_request(self) -> None:
        if self._request_path is not None:
            self._request_path.unlink(missing_ok=True)
        self._state.failback_pending = False
        self._persist_state()

    def read_request(self) -> dict[str, Any] | None:
        if self._request_path is None or not self._request_path.exists():
            return None
        return json.loads(self._request_path.read_text(encoding="utf-8"))

    def _is_profile_blocked(self, profile_name: str) -> bool:
        return bool(self._state.profile_health.get(profile_name, {}).get("blocked", False))

    def _is_profile_available(self, profile_name: str, *, at: datetime | None = None) -> bool:
        return not self._is_profile_blocked(profile_name) and not self._is_profile_in_cooldown(profile_name, at=at)

    def _is_profile_in_cooldown(self, profile_name: str, *, at: datetime | None = None) -> bool:
        cooldown_until = self._state.profile_health.get(profile_name, {}).get("cooldown_until")
        if not cooldown_until:
            return False
        now = at or datetime.now()
        return now < datetime.fromisoformat(cooldown_until)

    def _persist_state(self) -> None:
        if self._state_path is not None:
            save_pool_state(self._state_path, self._state)

    def _prune_failures(self, profile_state: dict[str, Any], now: datetime) -> None:
        failures = profile_state.setdefault("failures", {})
        hard_window = timedelta(seconds=self.config.process_policy.failure_window_seconds)
        network_window = timedelta(seconds=self.config.process_policy.network_failure_window_seconds)
        for reason, timestamps in list(failures.items()):
            kept: list[str] = []
            for raw in timestamps:
                ts = datetime.fromisoformat(raw)
                window = network_window if reason in {"network_error", "timeout"} else hard_window
                if now - ts <= window:
                    kept.append(raw)
            failures[reason] = kept


def save_pool_state(path: str | Path, state: PoolState) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(state.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def load_pool_state(path: str | Path) -> PoolState | None:
    target = Path(path)
    if not target.exists():
        return None
    data = json.loads(target.read_text(encoding="utf-8"))
    return PoolState.from_dict(data)


def _load_overlay(profile_data: dict[str, Any], base_dir: Path) -> tuple[str, ExecutionOverlay]:
    config_path = Path(str(profile_data["config_path"]))
    if not config_path.is_absolute():
        config_path = (base_dir / config_path).resolve()
    if not config_path.exists():
        raise ValueError(f"profile config_path does not exist: {config_path}")
    cfg = load_config(config_path)
    overlay = ExecutionOverlay(
        default_model=cfg.claude.default_model,
        cli_path=cfg.claude.cli_path,
        requests_per_minute=cfg.rate_limit.requests_per_minute,
        burst_size=cfg.rate_limit.burst_size,
        per_model_limits=dict(cfg.rate_limit.per_model_limits),
        task_env_overrides=dict(profile_data.get("task_env_overrides", {})),
        task_extra_args=list(profile_data.get("task_extra_args", [])),
    )
    return str(config_path), overlay


def load_failover_pool_config(path: str | Path) -> FailoverPoolConfig:
    target = Path(path).resolve()
    raw = tomllib.loads(target.read_text(encoding="utf-8"))
    profile_rows = list(raw.get("profiles", []))
    if not profile_rows:
        raise ValueError("failover pool requires at least one profile")

    names: set[str] = set()
    priorities: set[int] = set()
    profiles: list[PoolProfile] = []
    base_dir = target.parent
    for row in profile_rows:
        name = str(row.get("name", "")).strip()
        priority = int(row.get("priority", 0))
        if not name:
            raise ValueError("profile name is required")
        if name in names:
            raise ValueError(f"duplicate profile name: {name}")
        if priority in priorities:
            raise ValueError(f"duplicate profile priority: {priority}")
        names.add(name)
        priorities.add(priority)
        config_path, overlay = _load_overlay(row, base_dir)
        profiles.append(
            PoolProfile(
                name=name,
                priority=priority,
                config_path=config_path,
                task_switchable=bool(row.get("task_switchable", True)),
                process_switchable=bool(row.get("process_switchable", True)),
                overlay=overlay,
            )
        )

    profiles.sort(key=lambda item: item.priority)
    task_policy_data = raw.get("task_policy", {})
    process_policy_data = raw.get("process_policy", {})
    return FailoverPoolConfig(
        pool_id=str(raw.get("pool_id", "")).strip() or target.stem,
        profiles=profiles,
        failback_policy=str(raw.get("failback_policy", "safe-point")),
        health_window_seconds=int(raw.get("health_window_seconds", raw.get("health_window", 600))),
        cooldown_seconds=int(raw.get("cooldown_seconds", 300)),
        task_policy=TaskFailoverPolicy(
            allowed_reasons=list(task_policy_data.get("allowed_reasons", [])),
            switch_after_attempt=int(task_policy_data.get("switch_after_attempt", 1)),
        ),
        process_policy=ProcessFailoverPolicy(
            hard_failure_reasons=list(process_policy_data.get("hard_failure_reasons", [])),
            hard_failure_threshold=int(process_policy_data.get("hard_failure_threshold", 3)),
            failure_window_seconds=int(process_policy_data.get("failure_window_seconds", 300)),
            network_failure_threshold=int(process_policy_data.get("network_failure_threshold", 5)),
            network_failure_window_seconds=int(process_policy_data.get("network_failure_window_seconds", 600)),
        ),
    )
