"""Configuration models for simple task mode."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SimpleConfig:
    enabled: bool = True
    default_isolation: str = "none"
    none_file_conflict_strategy: str = "copy"
    max_pending_tasks: int = 100_000
    max_running_processes: int = 16
    initial_execution_slots: int = 0
    execution_slot_batch_size: int = 0
    execution_slot_ramp_interval_seconds: float = 0.0
    global_max_running_processes: int = 0
    global_fair_share_enabled: bool = True
    prepare_workers: int = 0
    validate_workers: int = 1
    max_prepared_items: int = 0
    default_timeout_seconds: int = 1800
    default_max_attempts: int = 3
    bucket_strategy: str = "dir"
    fair_scheduling: bool = True
    dynamic_throttle_enabled: bool = True
    copy_root_dir: str = "./simple_runs/copies"
    manifest_dir: str = "./simple_runs"
    event_log_enabled: bool = True
    claude_exec_ephemeral: bool = True
    claude_home_isolation: str = "worker"
    codex_exec_ephemeral: bool = True
    codex_home_isolation: str = "worker"
    default_max_turns: int = 12
    prompt_inline_file_bytes: int = 12_000
    prompt_inline_file_chars: int = 8_000
    prompt_inline_directory_entries: int = 20
    retry_feedback_in_prompt_enabled: bool = True
    syntax_checkers: dict[str, str] = field(default_factory=lambda: {
        ".py": "{python} -m py_compile {target}",
    })
    pattern_checks_enabled: bool = True
    default_semantic_validators: list[str] = field(default_factory=list)
    conflict_detection_enabled: bool = True
    auto_recover_unauthorized_changes: bool = True
    rollback_on_validation_failure: bool = True
    resume_reset_running_to_pending: bool = True
    windows_path_budget: int = 220
    cleanup_policy: str = "preserve-failures"
    starvation_seconds: int = 300
    max_bucket_alerts: int = 50
    verify_command_timeout_seconds: int = 600
    cpu_percent_max: float = 95.0
    memory_percent_max: float = 90.0
    disk_free_mb_min: int = 1024
    cache_repo_status_baseline: bool = True
    execution_lease_db_path: str = ""
    execution_lease_ttl_seconds: int = 300
    run_heartbeat_interval_seconds: int = 5
    stale_run_timeout_seconds: int = 180
    validate_task_file_targets: bool = False
    completion_until_clean_enabled: bool = True
    completion_retry_cancelled_runs: bool = False
    completion_max_retry_waves: int = 0
    completion_max_stagnant_waves: int = 4
    completion_max_identical_failure_waves: int = 3
