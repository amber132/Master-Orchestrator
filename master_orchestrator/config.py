"""Configuration loading from TOML files and environment variables."""

from __future__ import annotations

import logging
import os
import tomllib
import dataclasses
from dataclasses import dataclass, field
from pathlib import Path

from .exceptions import ConfigValidationError
from .simple_config import SimpleConfig

logger = logging.getLogger(__name__)
from .model import RetryPolicy
from .notification import NotificationConfig

DEFAULT_CLAUDE_MODEL = "sonnet"
LEGACY_CLAUDE_MODELS = ("opus",)
SUPPORTED_CLAUDE_MODELS = (DEFAULT_CLAUDE_MODEL, *LEGACY_CLAUDE_MODELS)
DEFAULT_CODEX_MODEL = "gpt-5.4"
LEGACY_CODEX_MODELS = ("gpt-5.3-codex",)
SUPPORTED_CODEX_MODELS = (DEFAULT_CODEX_MODEL, *LEGACY_CODEX_MODELS)


@dataclass
class ClaudeConfig:
    default_model: str = "sonnet"
    default_timeout: int = 600
    max_budget_usd: float = 50.0
    cli_path: str = "claude"
    budget_enforcement_mode: str = "accounting"


@dataclass
class CodexConfig:
    default_model: str = DEFAULT_CODEX_MODEL
    default_timeout: int = 600
    max_budget_usd: float = 0.0
    cli_path: str = "codex"
    budget_enforcement_mode: str = "accounting"
    execution_security_mode: str = "restricted"


@dataclass
class OrchestratorConfig:
    max_parallel: int = 4
    health_check_interval: int = 60
    min_parallel: int = 2
    adaptive_enabled: bool = True
    queue_capacity: int = 1000
    shutdown_timeout: int = 30
    concurrency_groups: dict[str, int] = field(default_factory=dict)
    max_write_parallel: int = 0  # 写操作并发上限，0 表示与 max_parallel 相同


@dataclass
class CheckpointConfig:
    db_path: str = "./orchestrator_state.db"


@dataclass
class LimitsConfig:
    max_output_size_bytes: int = 10_485_760
    max_prompt_size_chars: int = 200_000
    min_disk_space_mb: int = 500
    max_memory_percent: int = 90  # 内存使用率上限
    max_process_rss_mb: int = 4096  # 单进程 RSS 上限（4GB）
    max_nesting_depth: int = 2
    stream_file_max_bytes: int = 10 * 1024 * 1024  # 10 MB per task
    blackboard_value_max_chars: int = 50_000
    lru_max_outputs: int = 500
    lru_max_results: int = 1000


@dataclass
class SpillConfig:
    """长输出溢出到文件的配置，避免模板截断导致下游任务信息丢失。"""
    # 单个任务输出超过此字符数时，自动溢出到文件
    spill_threshold_chars: int = 8_000
    # 溢出后注入到模板中的摘要长度（取输出头尾各一半）
    summary_chars: int = 2_000
    # 溢出文件存放子目录名
    spill_dir_name: str = ".orchestrator_spill"


@dataclass
class PaginationConfig:
    """分页配置，用于控制大规模数据的分页处理。"""
    page_size: int = 50
    max_items: int = 1000


@dataclass
class DiscoveryConfig:
    """多来源搜索发现配置。"""
    enabled_providers: list[str] = field(
        default_factory=lambda: [
            "duckduckgo_html",
            "brave_web",
            "bing_web",
            "github_search",
            "sogou_wechat",
            "rss",
        ]
    )
    disabled_providers: list[str] = field(default_factory=list)
    min_source_score: float = 0.55
    max_hits_per_provider: int = 10
    max_queries: int = 5
    research_enabled: bool = True
    research_iterations: int = 1
    research_probe_budget: int = 4
    research_max_leads: int = 8
    sogou_cookie_header: str = ""
    sogou_cookie_file: str = ""
    sogou_storage_state_path: str = ""


@dataclass
class RateLimitConfig:
    """API 调用速率限制配置，防止触发 Claude API 限流。"""
    # 每分钟最大请求数（全局限制）
    requests_per_minute: int = 60
    # 每个模型的独立限制（如 {"opus": 20, "sonnet": 40}）
    per_model_limits: dict[str, int] = field(default_factory=dict)
    # 突发流量缓冲区大小（令牌桶算法）
    burst_size: int = 10
    rpm_limit: int = 60  # 每分钟最大请求数（与 requests_per_minute 一致）


@dataclass
class MonitorConfig:
    """监控配置，用于线程池和任务执行的监控指标。"""
    # 指标收集间隔（秒）
    metrics_interval: int = 30
    # 是否启用饥饿检测
    enable_starvation_detection: bool = True
    # 饥饿阈值（秒），任务等待超过此时间视为饥饿
    starvation_threshold_seconds: int = 300


@dataclass
class HealthConfig:
    """健康检查配置，用于资源监控和心跳检测。"""
    memory_percent_max: float = 85
    cpu_percent_max: float = 95
    heartbeat_timeout_seconds: int = 300
    spill_ttl_hours: int = 24
    health_port: int = 9100
    health_enabled: bool = False
    health_bind: str = "127.0.0.1"  # 默认仅本地访问，设为 "0.0.0.0" 可外部访问


@dataclass
class AlertConfig:
    """告警配置，用于去重和自动恢复。"""
    dedup_window_seconds: int = 300
    recovery_enabled: bool = True


@dataclass
class CacheConfig:
    """任务缓存配置。"""
    cache_ttl_seconds: int = 3600  # 缓存过期时间（秒），默认 1 小时


@dataclass
class GuardrailConfig:
    """护栏配置。"""
    guardrail_enabled: bool = True
    max_prompt_length: int = 200_000  # 最大 prompt 长度（字符数）


@dataclass
class MetricsConfig:
    """指标收集配置。"""
    metrics_path: str = "./metrics"  # 指标文件存储路径

@dataclass
class RequirementConfig:
    """需求收集配置。"""
    enabled: bool = False
    sufficiency_threshold: float = 0.75
    max_rounds: int = 3
    max_questions_per_round: int = 8
    gather_mode: str = "interactive"
    assessment_model: str = DEFAULT_CLAUDE_MODEL
    question_gen_model: str = DEFAULT_CLAUDE_MODEL
    synthesis_model: str = DEFAULT_CLAUDE_MODEL


@dataclass
class AutoRuntimeConfig:
    """自主模式运行时配置。"""
    max_hours: float = 24.0
    max_total_iterations: int = 50
    max_phase_iterations: int = 50
    phase_parallelism: int = 8
    convergence_threshold: float = 0.72
    convergence_window: int = 3
    min_convergence_checks: int = 2  # 收敛判定所需的最少连续高分次数
    score_improvement_min: float = 0.05
    adaptive_tuning_enabled: bool = True
    max_execution_processes: int = 0
    execution_lease_db_path: str = ""
    execution_lease_ttl_seconds: int = 300


@dataclass
class PreviewConfig:
    """执行前摘要预览配置。"""
    show_summary: bool = True
    require_confirmation: bool = True
    auto_confirm: bool = False
    max_doc_excerpt_chars: int = 2000


@dataclass
class WorkspaceConfig:
    """隔离工作区配置。"""
    enabled: bool = True
    root_dir: str = "./orchestrator_runs"
    branch_prefix: str = "orchestrator"
    keep_artifacts: bool = True


@dataclass
class BackupConfig:
    """涉数据任务备份配置。"""
    enabled: bool = True
    file_paths: list[str] = field(default_factory=list)
    database_backup_commands: list[str] = field(default_factory=list)
    metadata_paths: list[str] = field(default_factory=list)


@dataclass
class VerificationConfig:
    """自动验证规划配置。"""
    auto_plan: bool = True
    allow_service_start: bool = True
    allow_docker_compose: bool = True
    command_timeout: int = 600
    max_commands: int = 8


@dataclass
class DeliveryConfig:
    """本地交付配置。"""
    local_only: bool = True
    allow_multi_branch: bool = True
    write_pr_drafts: bool = True


@dataclass
class AuditConfig:
    """审计日志配置。"""
    enabled: bool = True
    log_dir: str = "./audit_logs"
    audit_log_path: str = "./audit_logs/audit.log"  # 审计日志文件路径


@dataclass
class FeaturesConfig:
    """特性开关配置。"""
    semantic_drift: bool = False
    context_quarantine: bool = False
    validation_gate: bool = False
    semantic_reset: bool = False
    blackboard: bool = False
    redundancy_detector: bool = False
    convergence: bool = False


@dataclass
class RoutingConfig:
    default_provider: str = "auto"
    auto_fallback: bool = True
    phase_defaults: dict[str, str] = field(
        default_factory=lambda: {
            "decompose": "claude",
            "review": "claude",
            "discover": "claude",
            "execute": "codex",
            "simple": "codex",
            "self_improve": "claude",
            "requirement": "claude",
        }
    )


@dataclass
class Config:
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)
    claude: ClaudeConfig = field(default_factory=ClaudeConfig)
    codex: CodexConfig = field(default_factory=CodexConfig)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    spill: SpillConfig = field(default_factory=SpillConfig)
    pagination: PaginationConfig = field(default_factory=PaginationConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    alert: AlertConfig = field(default_factory=AlertConfig)
    audit: AuditConfig = field(default_factory=AuditConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    guardrail: GuardrailConfig = field(default_factory=GuardrailConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    requirement: RequirementConfig = field(default_factory=RequirementConfig)
    auto: AutoRuntimeConfig = field(default_factory=AutoRuntimeConfig)
    preview: PreviewConfig = field(default_factory=PreviewConfig)
    workspace: WorkspaceConfig = field(default_factory=WorkspaceConfig)
    backup: BackupConfig = field(default_factory=BackupConfig)
    verification: VerificationConfig = field(default_factory=VerificationConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    features: FeaturesConfig = field(default_factory=FeaturesConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    simple: SimpleConfig = field(default_factory=SimpleConfig)


def _apply_env_overrides(cfg: Config) -> None:
    """Override config values from environment variables."""
    env_map = {
        "ORCHESTRATOR_MAX_PARALLEL": ("orchestrator", "max_parallel", int),
        "CLAUDE_DEFAULT_MODEL": ("claude", "default_model", str),
        "CLAUDE_DEFAULT_TIMEOUT": ("claude", "default_timeout", int),
        "CLAUDE_MAX_BUDGET_USD": ("claude", "max_budget_usd", float),
        "CLAUDE_CLI_PATH": ("claude", "cli_path", str),
        "CODEX_DEFAULT_MODEL": ("codex", "default_model", str),
        "CODEX_DEFAULT_TIMEOUT": ("codex", "default_timeout", int),
        "CODEX_MAX_BUDGET_USD": ("codex", "max_budget_usd", float),
        "CODEX_CLI_PATH": ("codex", "cli_path", str),
        "CODEX_EXECUTION_SECURITY_MODE": ("codex", "execution_security_mode", str),
        "CHECKPOINT_DB_PATH": ("checkpoint", "db_path", str),
        "NOTIFICATION_WEBHOOK_URL": ("notification", "webhook_url", str),
        "AUDIT_LOG_DIR": ("audit", "log_dir", str),
        "SOGOU_WECHAT_COOKIE_HEADER": ("discovery", "sogou_cookie_header", str),
        "SOGOU_WECHAT_COOKIE_FILE": ("discovery", "sogou_cookie_file", str),
        "SOGOU_WECHAT_STORAGE_STATE_PATH": ("discovery", "sogou_storage_state_path", str),
        "DISCOVERY_RESEARCH_ENABLED": ("discovery", "research_enabled", lambda value: value.lower() in {"1", "true", "yes", "on"}),
        "DISCOVERY_RESEARCH_ITERATIONS": ("discovery", "research_iterations", int),
        "DISCOVERY_RESEARCH_PROBE_BUDGET": ("discovery", "research_probe_budget", int),
        "DISCOVERY_RESEARCH_MAX_LEADS": ("discovery", "research_max_leads", int),
    }
    for env_key, (section, attr, typ) in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            try:
                setattr(getattr(cfg, section), attr, typ(val))
            except (ValueError, TypeError) as e:
                raise ConfigValidationError(
                    f"Invalid environment variable {env_key}={val!r}: cannot convert to {typ.__name__}",
                    context={"env_key": env_key, "value": val, "expected_type": typ.__name__, "error": str(e)}
                )


def _handle_unknown_config_keys(location: str, unknown_keys: list[str], mode: str) -> None:
    if not unknown_keys:
        return

    message = f"Unknown config keys under {location}: {', '.join(sorted(unknown_keys))}"
    if mode == "strict":
        raise ConfigValidationError(
            message,
            context={"location": location, "unknown_keys": sorted(unknown_keys)},
        )
    if mode == "warn":
        logger.warning(message)


def _dict_to_dataclass(
    data: dict,
    cls: type,
    defaults: object | None = None,
    *,
    section_name: str = "",
    unknown_key_mode: str = "ignore",
):
    """Map a dict to a dataclass, ignoring unknown keys."""
    valid_fields = {f.name for f in dataclasses.fields(cls)}
    unknown_keys = sorted(k for k in data.keys() if k not in valid_fields)
    _handle_unknown_config_keys(section_name or cls.__name__, unknown_keys, unknown_key_mode)
    filtered = {k: v for k, v in data.items() if k in valid_fields}
    if defaults:
        return cls(**{**{f.name: getattr(defaults, f.name) for f in dataclasses.fields(cls)}, **filtered})
    return cls(**filtered)


def load_config(
    path: str | Path | None = None,
    project_dir: str | Path | None = None,
    unknown_key_mode: str | None = None,
) -> Config:
    """Load configuration from a TOML file, with env var overrides.

    优先级（从高到低）：
      1. 显式指定的 path 参数（-c/--config）
      2. ORCHESTRATOR_CONFIG 环境变量
      3. 项目级配置 <project_dir>/.claude-orchestrator/config.toml
      4. 全局配置 ./config.toml 或 <包目录>/../config.toml
      5. 环境变量覆盖（ORCHESTRATOR_MAX_PARALLEL 等，在所有文件加载后应用）
    """
    cfg = Config()
    effective_unknown_key_mode = (
        unknown_key_mode
        or os.environ.get("ORCHESTRATOR_CONFIG_UNKNOWN_KEY_MODE", "warn").strip().lower()
    )
    if effective_unknown_key_mode not in {"ignore", "warn", "strict"}:
        raise ConfigValidationError(
            f"unknown_key_mode must be ignore/warn/strict, got {effective_unknown_key_mode!r}",
            context={"field": "unknown_key_mode", "value": effective_unknown_key_mode},
        )

    # 自动发现配置文件
    if path is None:
        candidates = [
            Path("config.toml"),
            Path(__file__).parent.parent / "config.toml",
        ]
        env_path = os.environ.get("ORCHESTRATOR_CONFIG")
        if env_path:
            candidates.insert(0, Path(env_path))

        # 项目级配置：-d 指定目录下的 .claude-orchestrator/config.toml
        if project_dir is not None:
            project_cfg = Path(project_dir) / ".claude-orchestrator" / "config.toml"
            if project_cfg.exists():
                candidates.insert(0, project_cfg)
                logger.info("发现项目级配置: %s", project_cfg)

        for candidate in candidates:
            try:
                if candidate.exists():
                    path = candidate
                    logger.info("自动发现配置文件: %s", candidate.resolve())
                    break
            except OSError:
                continue

    if path is not None:
        p = Path(path)
        if p.exists():
            # 文件 I/O 和 TOML 解析异常处理
            try:
                with open(p, "rb") as f:
                    raw = tomllib.load(f)
            except (OSError, PermissionError) as e:
                raise ConfigValidationError(
                    f"Failed to read config file: {e}",
                    context={"path": str(p), "error": str(e)}
                )
            except tomllib.TOMLDecodeError as e:
                raise ConfigValidationError(
                    f"Invalid TOML syntax: {e}",
                    context={"path": str(p), "error": str(e)}
                )

            # TOML 数据转换异常处理
            try:
                known_sections = {field.name for field in dataclasses.fields(Config)}
                unknown_sections = sorted(key for key in raw.keys() if key not in known_sections)
                _handle_unknown_config_keys("root", unknown_sections, effective_unknown_key_mode)
                if "orchestrator" in raw:
                    cfg.orchestrator = _dict_to_dataclass(
                        raw["orchestrator"],
                        OrchestratorConfig,
                        section_name="orchestrator",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "claude" in raw:
                    cfg.claude = _dict_to_dataclass(
                        raw["claude"],
                        ClaudeConfig,
                        section_name="claude",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "codex" in raw:
                    cfg.codex = _dict_to_dataclass(
                        raw["codex"],
                        CodexConfig,
                        section_name="codex",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "retry" in raw:
                    cfg.retry = _dict_to_dataclass(
                        raw["retry"],
                        RetryPolicy,
                        section_name="retry",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "checkpoint" in raw:
                    cfg.checkpoint = _dict_to_dataclass(
                        raw["checkpoint"],
                        CheckpointConfig,
                        section_name="checkpoint",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "limits" in raw:
                    cfg.limits = _dict_to_dataclass(
                        raw["limits"],
                        LimitsConfig,
                        section_name="limits",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "spill" in raw:
                    cfg.spill = _dict_to_dataclass(
                        raw["spill"],
                        SpillConfig,
                        section_name="spill",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "pagination" in raw:
                    cfg.pagination = _dict_to_dataclass(
                        raw["pagination"],
                        PaginationConfig,
                        section_name="pagination",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "discovery" in raw:
                    cfg.discovery = _dict_to_dataclass(
                        raw["discovery"],
                        DiscoveryConfig,
                        section_name="discovery",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "rate_limit" in raw:
                    cfg.rate_limit = _dict_to_dataclass(
                        raw["rate_limit"],
                        RateLimitConfig,
                        section_name="rate_limit",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "monitor" in raw:
                    cfg.monitor = _dict_to_dataclass(
                        raw["monitor"],
                        MonitorConfig,
                        section_name="monitor",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "notification" in raw:
                    cfg.notification = _dict_to_dataclass(
                        raw["notification"],
                        NotificationConfig,
                        section_name="notification",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "health" in raw:
                    cfg.health = _dict_to_dataclass(
                        raw["health"],
                        HealthConfig,
                        section_name="health",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "alert" in raw:
                    cfg.alert = _dict_to_dataclass(
                        raw["alert"],
                        AlertConfig,
                        section_name="alert",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "audit" in raw:
                    cfg.audit = _dict_to_dataclass(
                        raw["audit"],
                        AuditConfig,
                        section_name="audit",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "cache" in raw:
                    cfg.cache = _dict_to_dataclass(
                        raw["cache"],
                        CacheConfig,
                        section_name="cache",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "guardrail" in raw:
                    cfg.guardrail = _dict_to_dataclass(
                        raw["guardrail"],
                        GuardrailConfig,
                        section_name="guardrail",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "metrics" in raw:
                    cfg.metrics = _dict_to_dataclass(
                        raw["metrics"],
                        MetricsConfig,
                        section_name="metrics",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "requirement" in raw:
                    cfg.requirement = _dict_to_dataclass(
                        raw["requirement"],
                        RequirementConfig,
                        section_name="requirement",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "auto" in raw:
                    cfg.auto = _dict_to_dataclass(
                        raw["auto"],
                        AutoRuntimeConfig,
                        section_name="auto",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "preview" in raw:
                    cfg.preview = _dict_to_dataclass(
                        raw["preview"],
                        PreviewConfig,
                        section_name="preview",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "workspace" in raw:
                    cfg.workspace = _dict_to_dataclass(
                        raw["workspace"],
                        WorkspaceConfig,
                        section_name="workspace",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "backup" in raw:
                    cfg.backup = _dict_to_dataclass(
                        raw["backup"],
                        BackupConfig,
                        section_name="backup",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "verification" in raw:
                    cfg.verification = _dict_to_dataclass(
                        raw["verification"],
                        VerificationConfig,
                        section_name="verification",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "delivery" in raw:
                    cfg.delivery = _dict_to_dataclass(
                        raw["delivery"],
                        DeliveryConfig,
                        section_name="delivery",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "routing" in raw:
                    cfg.routing = _dict_to_dataclass(
                        raw["routing"],
                        RoutingConfig,
                        defaults=cfg.routing,
                        section_name="routing",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "simple" in raw:
                    cfg.simple = _dict_to_dataclass(
                        raw["simple"],
                        SimpleConfig,
                        section_name="simple",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
                if "features" in raw:
                    cfg.features = _dict_to_dataclass(
                        raw["features"],
                        FeaturesConfig,
                        section_name="features",
                        unknown_key_mode=effective_unknown_key_mode,
                    )
            except (TypeError, ValueError) as e:
                raise ConfigValidationError(
                    f"Invalid type in config file: {e}",
                    context={"path": str(p), "error": str(e)}
                )


    _apply_env_overrides(cfg)

    # 配置校验
    if cfg.orchestrator.max_parallel <= 0:
        raise ConfigValidationError(
            f"max_parallel must be > 0, got {cfg.orchestrator.max_parallel}",
            context={"field": "orchestrator.max_parallel", "value": cfg.orchestrator.max_parallel}
        )

    if cfg.claude.max_budget_usd < 0:
        raise ConfigValidationError(
            f"max_budget_usd must be >= 0, got {cfg.claude.max_budget_usd}",
            context={"field": "claude.max_budget_usd", "value": cfg.claude.max_budget_usd}
        )
    if cfg.claude.budget_enforcement_mode not in {"accounting", "hard_limit"}:
        raise ConfigValidationError(
            (
                "claude.budget_enforcement_mode must be one of "
                f"accounting/hard_limit, got {cfg.claude.budget_enforcement_mode!r}"
            ),
            context={
                "field": "claude.budget_enforcement_mode",
                "value": cfg.claude.budget_enforcement_mode,
            },
        )
    if cfg.codex.max_budget_usd < 0:
        raise ConfigValidationError(
            f"max_budget_usd must be >= 0, got {cfg.codex.max_budget_usd}",
            context={"field": "codex.max_budget_usd", "value": cfg.codex.max_budget_usd}
        )
    if cfg.codex.budget_enforcement_mode not in {"accounting", "hard_limit"}:
        raise ConfigValidationError(
            (
                "codex.budget_enforcement_mode must be one of "
                f"accounting/hard_limit, got {cfg.codex.budget_enforcement_mode!r}"
            ),
            context={
                "field": "codex.budget_enforcement_mode",
                "value": cfg.codex.budget_enforcement_mode,
            },
        )

    if not 0.0 <= cfg.discovery.min_source_score <= 1.0:
        raise ConfigValidationError(
            f"discovery.min_source_score must be between 0 and 1, got {cfg.discovery.min_source_score}",
            context={"field": "discovery.min_source_score", "value": cfg.discovery.min_source_score},
        )

    if cfg.discovery.max_hits_per_provider <= 0:
        raise ConfigValidationError(
            f"discovery.max_hits_per_provider must be > 0, got {cfg.discovery.max_hits_per_provider}",
            context={
                "field": "discovery.max_hits_per_provider",
                "value": cfg.discovery.max_hits_per_provider,
            },
        )

    if cfg.discovery.research_iterations < 0:
        raise ConfigValidationError(
            f"discovery.research_iterations must be >= 0, got {cfg.discovery.research_iterations}",
            context={
                "field": "discovery.research_iterations",
                "value": cfg.discovery.research_iterations,
            },
        )

    if cfg.discovery.research_probe_budget <= 0:
        raise ConfigValidationError(
            f"discovery.research_probe_budget must be > 0, got {cfg.discovery.research_probe_budget}",
            context={
                "field": "discovery.research_probe_budget",
                "value": cfg.discovery.research_probe_budget,
            },
        )

    if cfg.discovery.research_max_leads <= 0:
        raise ConfigValidationError(
            f"discovery.research_max_leads must be > 0, got {cfg.discovery.research_max_leads}",
            context={
                "field": "discovery.research_max_leads",
                "value": cfg.discovery.research_max_leads,
            },
        )

    if not cfg.claude.cli_path or not cfg.claude.cli_path.strip():
        raise ConfigValidationError(
            f"cli_path must not be empty, got {cfg.claude.cli_path!r}",
            context={"field": "claude.cli_path", "value": cfg.claude.cli_path}
        )
    if not cfg.codex.cli_path or not cfg.codex.cli_path.strip():
        raise ConfigValidationError(
            f"cli_path must not be empty, got {cfg.codex.cli_path!r}",
            context={"field": "codex.cli_path", "value": cfg.codex.cli_path}
        )
    if cfg.codex.execution_security_mode not in {"restricted", "trusted_local"}:
        raise ConfigValidationError(
            (
                "codex.execution_security_mode must be one of "
                f"restricted/trusted_local, got {cfg.codex.execution_security_mode!r}"
            ),
            context={
                "field": "codex.execution_security_mode",
                "value": cfg.codex.execution_security_mode,
            },
        )
    if cfg.routing.default_provider not in {"auto", "claude", "codex"}:
        raise ConfigValidationError(
            f"routing.default_provider must be auto/claude/codex, got {cfg.routing.default_provider}",
            context={"field": "routing.default_provider", "value": cfg.routing.default_provider},
        )
    invalid_phase_providers = {
        phase: provider
        for phase, provider in cfg.routing.phase_defaults.items()
        if provider not in {"claude", "codex", "auto"}
    }
    if invalid_phase_providers:
        raise ConfigValidationError(
            f"routing.phase_defaults contains invalid providers: {invalid_phase_providers}",
            context={"field": "routing.phase_defaults", "value": invalid_phase_providers},
        )

    if cfg.retry.max_attempts <= 0 or cfg.retry.max_attempts > 1000:
        raise ConfigValidationError(
            f"max_attempts must be > 0 and <= 1000, got {cfg.retry.max_attempts}",
            context={"field": "retry.max_attempts", "value": cfg.retry.max_attempts}
        )

    if cfg.auto.max_hours <= 0:
        raise ConfigValidationError(
            f"auto.max_hours must be > 0, got {cfg.auto.max_hours}",
            context={"field": "auto.max_hours", "value": cfg.auto.max_hours},
        )
    if cfg.auto.max_total_iterations <= 0:
        raise ConfigValidationError(
            f"auto.max_total_iterations must be > 0, got {cfg.auto.max_total_iterations}",
            context={"field": "auto.max_total_iterations", "value": cfg.auto.max_total_iterations},
        )
    if cfg.auto.max_phase_iterations <= 0:
        raise ConfigValidationError(
            f"auto.max_phase_iterations must be > 0, got {cfg.auto.max_phase_iterations}",
            context={"field": "auto.max_phase_iterations", "value": cfg.auto.max_phase_iterations},
        )
    if cfg.auto.phase_parallelism <= 0:
        raise ConfigValidationError(
            f"auto.phase_parallelism must be > 0, got {cfg.auto.phase_parallelism}",
            context={"field": "auto.phase_parallelism", "value": cfg.auto.phase_parallelism},
        )
    if cfg.auto.convergence_window <= 0:
        raise ConfigValidationError(
            f"auto.convergence_window must be > 0, got {cfg.auto.convergence_window}",
            context={"field": "auto.convergence_window", "value": cfg.auto.convergence_window},
        )
    if cfg.auto.max_execution_processes < 0:
        raise ConfigValidationError(
            f"auto.max_execution_processes must be >= 0, got {cfg.auto.max_execution_processes}",
            context={"field": "auto.max_execution_processes", "value": cfg.auto.max_execution_processes},
        )
    if cfg.auto.execution_lease_ttl_seconds <= 0:
        raise ConfigValidationError(
            f"auto.execution_lease_ttl_seconds must be > 0, got {cfg.auto.execution_lease_ttl_seconds}",
            context={
                "field": "auto.execution_lease_ttl_seconds",
                "value": cfg.auto.execution_lease_ttl_seconds,
            },
        )

    if cfg.simple.max_pending_tasks <= 0:
        raise ConfigValidationError(
            f"simple.max_pending_tasks must be > 0, got {cfg.simple.max_pending_tasks}",
            context={"field": "simple.max_pending_tasks", "value": cfg.simple.max_pending_tasks},
        )
    if cfg.simple.max_running_processes <= 0:
        raise ConfigValidationError(
            f"simple.max_running_processes must be > 0, got {cfg.simple.max_running_processes}",
            context={"field": "simple.max_running_processes", "value": cfg.simple.max_running_processes},
        )
    if cfg.simple.global_max_running_processes < 0:
        raise ConfigValidationError(
            f"simple.global_max_running_processes must be >= 0, got {cfg.simple.global_max_running_processes}",
            context={"field": "simple.global_max_running_processes", "value": cfg.simple.global_max_running_processes},
        )
    if cfg.simple.run_heartbeat_interval_seconds <= 0:
        raise ConfigValidationError(
            f"simple.run_heartbeat_interval_seconds must be > 0, got {cfg.simple.run_heartbeat_interval_seconds}",
            context={"field": "simple.run_heartbeat_interval_seconds", "value": cfg.simple.run_heartbeat_interval_seconds},
        )
    if cfg.simple.stale_run_timeout_seconds <= 0:
        raise ConfigValidationError(
            f"simple.stale_run_timeout_seconds must be > 0, got {cfg.simple.stale_run_timeout_seconds}",
            context={"field": "simple.stale_run_timeout_seconds", "value": cfg.simple.stale_run_timeout_seconds},
        )
    if cfg.simple.prepare_workers < 0:
        raise ConfigValidationError(
            f"simple.prepare_workers must be >= 0, got {cfg.simple.prepare_workers}",
            context={"field": "simple.prepare_workers", "value": cfg.simple.prepare_workers},
        )
    if cfg.simple.validate_workers <= 0:
        raise ConfigValidationError(
            f"simple.validate_workers must be > 0, got {cfg.simple.validate_workers}",
            context={"field": "simple.validate_workers", "value": cfg.simple.validate_workers},
        )
    if cfg.simple.max_prepared_items < 0:
        raise ConfigValidationError(
            f"simple.max_prepared_items must be >= 0, got {cfg.simple.max_prepared_items}",
            context={"field": "simple.max_prepared_items", "value": cfg.simple.max_prepared_items},
        )
    if cfg.simple.execution_lease_ttl_seconds <= 0:
        raise ConfigValidationError(
            f"simple.execution_lease_ttl_seconds must be > 0, got {cfg.simple.execution_lease_ttl_seconds}",
            context={"field": "simple.execution_lease_ttl_seconds", "value": cfg.simple.execution_lease_ttl_seconds},
        )
    if cfg.simple.default_isolation not in {"none", "copy", "worktree"}:
        raise ConfigValidationError(
            f"simple.default_isolation must be one of none/copy/worktree, got {cfg.simple.default_isolation}",
            context={"field": "simple.default_isolation", "value": cfg.simple.default_isolation},
        )

    # 初始化特性开关全局单例
    from .features import FeatureFlags, _KNOWN_FEATURES
    FeatureFlags.initialize(FeatureFlags.from_dict({
        k: getattr(cfg.features, k)
        for k in _KNOWN_FEATURES
    }))

    return cfg
