"""Execution helpers for simple mode."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from .audit_log import AuditLogger
from .agent_cli import run_agent_task
from .claude_cli import BudgetTracker, run_claude_task
from .config import Config
from .error_classifier import classify_failover_reason
from .failover_pool import PoolRuntime
from .file_lock import FileLock
from .model import RetryPolicy, TaskNode, TaskResult
from .rate_limiter import RateLimiter
from .simple_isolation import PreparedItemWorkspace
from .simple_model import SimpleAttempt, SimpleItemStatus
from .simple_semantic_validation import resolve_semantic_validators, semantic_prompt_hints

if TYPE_CHECKING:
    from .store import Store


@dataclass
class ExecutionOutcome:
    result: TaskResult
    attempt: SimpleAttempt
    changed_files: list[str]
    prompt: str


def build_simple_prompt(prepared: PreparedItemWorkspace) -> str:
    return _build_simple_prompt(prepared, None)


def _decode_prompt_text(data: bytes | None, byte_limit: int, char_limit: int) -> tuple[str, bool]:
    if not data:
        return "", False
    if b"\x00" in data:
        return "", True
    truncated = len(data) > byte_limit
    if truncated:
        data = data[:byte_limit]
    text = data.decode("utf-8", errors="replace")
    truncated = truncated or len(text) > char_limit
    if truncated:
        text = text[:char_limit].rstrip() + "\n...<truncated>"
    return text, truncated


def _render_target_snapshot(prepared: PreparedItemWorkspace, config: Config | None) -> str:
    simple_cfg = config.simple if config is not None else Config().simple
    item = prepared.item
    if item.item_type.value == "directory_shard":
        root = prepared.target_path
        entries: list[str] = []
        if root.exists() and root.is_dir():
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                rel = str(path.relative_to(prepared.cwd)).replace("\\", "/")
                entries.append(f"- {rel} ({path.stat().st_size} bytes)")
                if len(entries) >= simple_cfg.prompt_inline_directory_entries:
                    break
        if not entries:
            entries.append("- <empty directory shard>")
        return "\n".join([
            "[Target Snapshot]",
            f"- directory entries sampled: {len(entries)}",
            *entries,
        ])

    source_bytes = prepared.source_baseline_bytes
    if source_bytes is None and prepared.source_target_path.exists() and prepared.source_target_path.is_file():
        source_bytes = prepared.source_target_path.read_bytes()
    size_bytes = len(source_bytes) if source_bytes is not None else 0
    text, is_binary = _decode_prompt_text(
        source_bytes,
        simple_cfg.prompt_inline_file_bytes,
        simple_cfg.prompt_inline_file_chars,
    )
    line_count = text.count("\n") + (1 if text else 0)
    lines = [
        "[Target Snapshot]",
        f"- path validated: yes",
        f"- file size bytes: {size_bytes}",
        f"- file suffix: {prepared.target_path.suffix or '<no-ext>'}",
        f"- line count (approx): {line_count}",
        f"- empty file: {'yes' if size_bytes == 0 else 'no'}",
        f"- binary-like file: {'yes' if is_binary else 'no'}",
    ]
    if text:
        lines.extend([
            "",
            "[Target File Content]",
            "```text",
            text,
            "```",
        ])
    elif is_binary:
        lines.append("- file content omitted because it appears binary")
    else:
        lines.append("- file content omitted because file is empty")
    return "\n".join(lines)


def build_simple_system_prompt(prepared: PreparedItemWorkspace) -> str:
    item = prepared.item
    return (
        "这是 simple task mode 的批处理 work-item。\n"
        "目标路径已经由编排器验证，优先直接在目标文件上完成修改。\n"
        "不要先做仓库级扫描，不要先读取 README、技能文档或无关目录。\n"
        "除非为理解当前目标文件内符号所必需，否则不要读取额外文件。\n"
        "如果 prompt 里已经包含目标文件快照，优先基于该快照工作。\n"
        "只允许修改目标范围内文件；如果目标与工单不一致，停止扩展探索，不要改其他文件。\n"
        "严禁顺手修改同目录兄弟文件、父包 __init__.py、示例入口或邻近模块来补上下文。\n"
        "如果你判断其他文件更适合加注释，也不要去改它；保持非目标文件完全不变。\n"
        "禁止创建、删除、重命名任何非目标文件。\n"
        f"当前目标: {item.target}"
    )


def _retry_feedback_lines(prepared: PreparedItemWorkspace, config: Config | None) -> list[str]:
    simple_cfg = config.simple if config is not None else Config().simple
    if not simple_cfg.retry_feedback_in_prompt_enabled:
        return []
    item = prepared.item
    if item.attempt_state.attempt <= 0 and not item.attempt_state.last_error_category and not item.attempt_state.last_failure_reason:
        return []
    lines = [
        f"- 历史尝试次数: {item.attempt_state.attempt}",
    ]
    if item.attempt_state.last_error_category:
        lines.append(f"- 上次失败类别: {item.attempt_state.last_error_category}")
    if item.attempt_state.last_failure_reason:
        lines.append(f"- 上次失败原因: {item.attempt_state.last_failure_reason}")

    category = item.attempt_state.last_error_category
    guidance = {
        "target_path_mismatch": "这次必须严格只修改目标路径；如果需要其他文件上下文，只读不改。",
        "wrong_file_changed": "不要改兄弟文件或旁路文件，只保留目标文件改动。",
        "unauthorized_side_files": "不要再触碰未授权文件；宁可少改，也不要扩展修改范围。",
        "no_change": "这次必须在目标文件中落下实际源码改动，不能只分析不改。",
        "syntax_error": "先保证目标文件语法正确，再补充注释内容。",
        "pattern_missing": "必须显式满足 require_patterns 中的模式要求。",
        "semantic_validation_failed": "这次优先补齐语义校验器要求的缺失点，不要只做表面注释。",
        "verify_command_failed": "修改后要兼顾 verify command 通过，不要只追求文件有变更。",
    }.get(category)
    if guidance:
        lines.append(f"- 本轮修正重点: {guidance}")
    return lines


def _build_simple_prompt(prepared: PreparedItemWorkspace, config: Config | None) -> str:
    item = prepared.item
    is_directory = item.item_type.value == "directory_shard"
    simple_cfg = config.simple if config is not None else Config().simple
    semantic_validators = resolve_semantic_validators(simple_cfg.default_semantic_validators, item)
    scope_lines = [
        f"- 目标类型: {item.item_type.value}",
        f"- 目标路径: {item.target}",
        f"- 执行目录: {prepared.cwd}",
        f"- 隔离模式: {prepared.effective_mode}",
        "- 编排器已确认目标存在且位于工作目录内",
    ]
    if item.validation_profile.allowed_side_files:
        scope_lines.append(f"- 允许额外修改: {', '.join(item.validation_profile.allowed_side_files)}")
    elif is_directory:
        scope_lines.append("- 只允许修改目标目录内文件；不要触碰目录外文件")
    else:
        scope_lines.append("- 只允许修改目标文件；不要触碰其他文件")
    checks = []
    if item.validation_profile.require_patterns:
        checks.append(f"必须命中的模式: {', '.join(item.validation_profile.require_patterns)}")
    if item.validation_profile.verify_commands:
        checks.append(f"执行后会验证命令: {' | '.join(item.validation_profile.verify_commands)}")
    if semantic_validators:
        checks.append(f"启用语义校验器: {', '.join(semantic_validators)}")
    metadata_lines = []
    if item.metadata:
        metadata_lines = [f"- {key}: {value}" for key, value in sorted(item.metadata.items())]
    scope = "\n".join(scope_lines)
    constraints = "\n".join(f"- {line}" for line in checks) if checks else "- 无额外显式校验要求"
    metadata = "\n".join(metadata_lines) if metadata_lines else "- 无额外 metadata"
    snapshot = _render_target_snapshot(prepared, config)
    semantic_hint_lines = semantic_prompt_hints(semantic_validators)
    semantic_hint_block = ""
    if semantic_hint_lines:
        semantic_hint_block = "[Semantic Quality Gates]\n" + "\n".join(f"- {line}" for line in semantic_hint_lines) + "\n\n"
    retry_feedback = _retry_feedback_lines(prepared, config)
    retry_feedback_block = ""
    if retry_feedback:
        retry_feedback_block = "[Previous Attempt Feedback]\n" + "\n".join(retry_feedback) + "\n\n"
    return (
        "[Task]\n"
        f"{item.instruction.strip()}\n\n"
        "[Execution Rules]\n"
        "- 直接进入目标文件编辑，不要为确认路径再做仓库级扫描。\n"
        "- 不要读取 README、技能文档、handoff 文档或无关目录，除非对当前目标文件是必需的。\n"
        "- 如果下面已经提供目标内容快照，优先使用快照，只有在需要精确上下文时才回读目标文件本身。\n"
        "- 不要输出总结，不要生成额外文档，只交付源码改动。\n"
        "- 验证只做最小必要检查，避免把时间花在无关探索上。\n"
        "- 不要为了补当前文件注释去改父目录 __init__.py、同包兄弟文件、示例入口或测试夹具。\n"
        "- 如果无法只在当前目标文件完成，请停止扩展修改范围，保持其他文件不变。\n"
        "- 如果是在补中文学习注释，类说明里的中文字符数至少要达到英文字符的 3 倍。\n"
        "- 类说明必须像给小学生讲解一样白话：先说「这是做什么的」，再说「什么时候会用到」，最好顺手给一个「比如 / 就像」的例子。\n"
        "- 出现英文术语、协议名或类名时，必须立刻补中文解释，不能只把英文名原样堆进注释里。\n\n"
        "- 类说明尽量分成几层来讲：它是什么、什么时候会用、上下游怎么配合、一个具体例子，避免只写口号式短句。\n"
        "- 英文类名、函数名、模块名最多点名一次；点名以后，后文尽量改成「这个类 / 这个命令层 / 这个输入输出前台」之类的中文代称。\n"
        "- 不要反复用反引号包裹英文标识，也不要把一长串英文调用链原样抄进注释里；优先把调用关系改写成中文描述。\n\n"
        "[Work Item]\n"
        f"{scope}\n\n"
        "[Metadata]\n"
        f"{metadata}\n\n"
        f"{snapshot}\n\n"
        f"{semantic_hint_block}"
        f"{retry_feedback_block}"
        "[Constraints]\n"
        "- 优先做最小必要改动。\n"
        "- 不要生成总结文档，不要修改无关文件。\n"
        f"- {'可以修改目标目录中的多个文件' if is_directory else '默认只修改目标文件'}。\n"
        f"{constraints}\n"
    )


class SimpleExecutor:
    _WORKER_HOME_REQUIRED_DIR_NAMES = (
        "sessions",
        "shell_snapshots",
        "guardian",
        "tmp",
        "log",
        "sqlite",
        "xdg_state",
        "xdg_cache",
        "xdg_data",
        "xdg_config",
        "xdg_runtime",
        "skills",
    )
    _SHARED_SKILL_DIR_NAMES = frozenset({
        "node_modules",
    })
    _PRIVATE_SKILL_DIR_NAMES = frozenset({
        ".system",
    })
    _DESKTOP_ONLY_HOME_FILE_NAMES = frozenset({
        ".claude-global-state.json",
        "cap_sid",
    })

    def __init__(
        self,
        config: Config,
        budget: BudgetTracker,
        rate_limiter: RateLimiter,
        audit_logger: AuditLogger | None = None,
        *,
        state_root: Path | None = None,
        pool_runtime: PoolRuntime | None = None,
        store: "Store | None" = None,
        run_id: str = "",
        on_process_request: Callable[[int], None] | None = None,
        preferred_provider: str = "auto",
    ):
        self._config = config
        self._budget = budget
        self._rate_limiter = rate_limiter
        self._audit_logger = audit_logger
        self._state_root = state_root
        self._pool_runtime = pool_runtime
        self._store = store
        self._run_id = run_id
        self._on_process_request = on_process_request
        self._preferred_provider = preferred_provider
        self._claude_home_lock = threading.Lock()
        self._seed_home_lock = threading.Lock()
        self._prepared_claude_homes: set[Path] = set()
        self._preparing_claude_homes: dict[Path, threading.Event] = {}
        self._seed_home: Path | None = None
        self._source_claude_home: Path | None = None

    def _claude_home_ready_marker(self, home: Path) -> Path:
        return home / ".simple-home-ready"

    def _existing_claude_home_is_ready(self, home: Path) -> bool:
        if not home.exists() or not home.is_dir():
            return False
        if any(not (home / name).exists() for name in self._WORKER_HOME_REQUIRED_DIR_NAMES):
            return False
        source_home = self._resolve_source_claude_home()
        if source_home is not None and not self._seeded_home_matches_source(home, source_home):
            return False
        marker = self._claude_home_ready_marker(home)
        if marker.exists():
            return True
        # 兼容旧版本 simple mode 创建的 worker home：没有 marker 也允许直接复用。
        return any((home / name).exists() for name in ("config.toml", "auth.json", "version.json"))

    def _required_seed_files(self, source_home: Path) -> tuple[str, ...]:
        required: list[str] = []
        for filename in ("config.toml", "auth.json", "version.json"):
            if (source_home / filename).exists():
                required.append(filename)
        return tuple(required)

    def _hash_bytes(self, payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def _hash_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _expected_seed_hashes(self, source_home: Path) -> dict[str, str]:
        expected: dict[str, str] = {}
        config_src = source_home / "config.toml"
        if config_src.exists():
            worker_config = self._build_worker_config_text(config_src)
            expected["config.toml"] = self._hash_bytes(worker_config.encode("utf-8"))
        for filename in ("auth.json", "version.json", "AGENTS.md"):
            src = source_home / filename
            if src.exists():
                expected[filename] = self._hash_file(src)
        return expected

    def _seeded_home_matches_source(self, home: Path, source_home: Path) -> bool:
        for filename in self._DESKTOP_ONLY_HOME_FILE_NAMES:
            if (home / filename).exists():
                return False
        expected = self._expected_seed_hashes(source_home)
        if not expected:
            return True
        for filename, expected_hash in expected.items():
            candidate = home / filename
            if not candidate.exists() or not candidate.is_file():
                return False
            try:
                if self._hash_file(candidate) != expected_hash:
                    return False
            except OSError:
                return False
        return True

    def _resolve_source_claude_home(self) -> Path | None:
        if self._source_claude_home is not None:
            return self._source_claude_home

        candidates: list[Path] = []
        source_home_env = os.environ.get("CLAUDE_HOME")
        if source_home_env:
            candidates.append(Path(source_home_env).expanduser())

        home_dir = Path.home()
        candidates.extend(
            [
                home_dir / ".claude",
                home_dir / ".config" / "claude",
            ]
        )
        appdata = os.environ.get("APPDATA")
        if appdata:
            candidates.append(Path(appdata) / "claude")

        seen: set[str] = set()
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            key = str(resolved).lower() if os.name == "nt" else str(resolved)
            if key in seen:
                continue
            seen.add(key)
            if not resolved.exists() or not resolved.is_dir():
                continue
            if any((resolved / name).exists() for name in ("config.toml", "auth.json", "version.json", "skills")):
                self._source_claude_home = resolved
                return resolved
        return None

    def _claude_home_for_worker(self, worker_id: str) -> Path | None:
        isolation_mode = self._config.simple.claude_home_isolation
        if isolation_mode == "none" or self._state_root is None:
            return None
        slug = "run" if isolation_mode == "run" else worker_id
        home = (self._state_root / "claude_home" / slug).resolve()
        self._ensure_claude_home(home)
        return home

    def warm_worker_home(self, worker_id: str) -> Path | None:
        return self._claude_home_for_worker(worker_id)

    def _ensure_claude_home(self, home: Path) -> None:
        wait_event: threading.Event | None = None
        should_prepare = False
        with self._claude_home_lock:
            if home in self._prepared_claude_homes:
                return
            wait_event = self._preparing_claude_homes.get(home)
            if wait_event is None:
                wait_event = threading.Event()
                self._preparing_claude_homes[home] = wait_event
                should_prepare = True

        if not should_prepare:
            wait_event.wait()
            return

        try:
            if self._existing_claude_home_is_ready(home):
                with self._claude_home_lock:
                    self._prepared_claude_homes.add(home)
                return
            home.mkdir(parents=True, exist_ok=True)
            for name in self._WORKER_HOME_REQUIRED_DIR_NAMES:
                (home / name).mkdir(parents=True, exist_ok=True)

            source_home = self._resolve_source_claude_home()
            if source_home is not None:
                self._seed_claude_home(home, self._materialize_seed_home(source_home))

            self._claude_home_ready_marker(home).write_text("ready\n", encoding="utf-8")

            with self._claude_home_lock:
                self._prepared_claude_homes.add(home)
        finally:
            with self._claude_home_lock:
                event = self._preparing_claude_homes.pop(home, None)
            if event is not None:
                event.set()

    def _seed_claude_home(self, home: Path, source_home: Path) -> None:
        # worker home 必须具备独立且完整的最小配置；否则 claude 会回退到桌面端全局 home，
        # 在高并发下共享同一批会话状态、MCP 启动和桌面线程环境，导致请求链路不稳定。
        for filename in self._DESKTOP_ONLY_HOME_FILE_NAMES:
            stale = home / filename
            if stale.exists():
                if stale.is_dir():
                    shutil.rmtree(stale, ignore_errors=True)
                else:
                    stale.unlink(missing_ok=True)

        config_src = source_home / "config.toml"
        config_dst = home / "config.toml"
        if config_src.exists():
            config_dst.write_text(self._build_worker_config_text(config_src), encoding="utf-8")

        for filename in ("auth.json", "version.json", "AGENTS.md"):
            src = source_home / filename
            dst = home / filename
            if src.exists():
                shutil.copy2(src, dst)

        skills_src = source_home / "skills"
        skills_dst = home / "skills"
        if skills_src.exists():
            self._copy_skill_tree(skills_src, skills_dst)

        for dirname in ("vendor_imports", "bin"):
            src = source_home / dirname
            dst = home / dirname
            if not src.exists() or dst.exists():
                continue
            try:
                dst.symlink_to(src, target_is_directory=True)
            except OSError:
                shutil.copytree(src, dst, dirs_exist_ok=True)

    def _build_worker_config_text(self, source_config_path: Path) -> str:
        source_text = source_config_path.read_text(encoding="utf-8")
        try:
            raw = tomllib.loads(source_text)
        except tomllib.TOMLDecodeError:
            return source_text

        top_level_keys = (
            "sandbox_mode",
            "approval_policy",
            "model",
            "model_provider",
            "model_reasoning_effort",
            "disable_response_storage",
            "web_search",
            "service_tier",
            "personality",
        )
        lines: list[str] = []
        for key in top_level_keys:
            if key in raw:
                lines.append(f"{key} = {self._toml_value(raw[key])}")

        provider_name = raw.get("model_provider")
        providers = raw.get("model_providers")
        if isinstance(provider_name, str) and isinstance(providers, dict):
            provider_cfg = providers.get(provider_name)
            if isinstance(provider_cfg, dict):
                self._append_toml_table(lines, ["model_providers", provider_name], provider_cfg)

        source_features = raw.get("features")
        features: dict[str, object] = {}
        if isinstance(source_features, dict):
            for key, value in source_features.items():
                if isinstance(value, dict):
                    continue
                if key == "apps_mcp_gateway":
                    continue
                features[key] = value
        # simple worker 是纯后台子进程，不应该再接入桌面 app-server；
        # 保留源配置里其他显式 feature 覆盖，避免回退到默认值后重新打开交互式通道。
        features["apps"] = False
        features["realtime_conversation"] = False
        features.setdefault("responses_websockets", False)
        features.setdefault("responses_websockets_v2", False)
        if features:
            self._append_toml_table(lines, ["features"], features)

        return "\n".join(lines).rstrip() + "\n"

    def _build_worker_env_overrides(self, claude_home: Path) -> dict[str, str]:
        resolved_home = claude_home.resolve()
        user_home = resolved_home / "user_home"
        appdata_roaming = user_home / "AppData" / "Roaming"
        appdata_local = user_home / "AppData" / "Local"
        temp_dir = appdata_local / "Temp"
        xdg_state = resolved_home / "xdg_state"
        xdg_cache = resolved_home / "xdg_cache"
        xdg_data = resolved_home / "xdg_data"
        xdg_config = resolved_home / "xdg_config"
        xdg_runtime = resolved_home / "xdg_runtime"
        for path in (
            user_home,
            appdata_roaming,
            appdata_local,
            temp_dir,
            xdg_state,
            xdg_cache,
            xdg_data,
            xdg_config,
            xdg_runtime,
        ):
            path.mkdir(parents=True, exist_ok=True)

        env = {
            "CLAUDE_HOME": str(resolved_home),
            "XDG_STATE_HOME": str(xdg_state),
            "XDG_CACHE_HOME": str(xdg_cache),
            "XDG_DATA_HOME": str(xdg_data),
            "XDG_CONFIG_HOME": str(xdg_config),
            "XDG_RUNTIME_DIR": str(xdg_runtime),
            "TMPDIR": str(temp_dir),
        }
        if os.name == "nt":
            user_home_str = str(user_home)
            env.update(
                {
                    "HOME": user_home_str,
                    "USERPROFILE": user_home_str,
                    "APPDATA": str(appdata_roaming),
                    "LOCALAPPDATA": str(appdata_local),
                    "TEMP": str(temp_dir),
                    "TMP": str(temp_dir),
                }
            )
            if user_home.drive:
                env["HOMEDRIVE"] = user_home.drive
                home_path = user_home_str[len(user_home.drive):]
                env["HOMEPATH"] = home_path or "\\"
        else:
            env["HOME"] = str(user_home)
        return env

    def _append_toml_table(self, lines: list[str], path: list[str], values: dict[str, object]) -> None:
        scalar_items: list[tuple[str, object]] = []
        nested_items: list[tuple[str, dict[str, object]]] = []
        for key, value in values.items():
            if value is None:
                continue
            if isinstance(value, dict):
                nested_items.append((key, value))
            else:
                scalar_items.append((key, value))

        if scalar_items:
            if lines:
                lines.append("")
            lines.append(f"[{'.'.join(self._toml_key(part) for part in path)}]")
            for key, value in scalar_items:
                lines.append(f"{self._toml_key(key)} = {self._toml_value(value)}")

        for key, nested in nested_items:
            self._append_toml_table(lines, [*path, key], nested)

    def _toml_key(self, key: str) -> str:
        normalized = key.replace("-", "_").replace(".", "_")
        if normalized and normalized.replace("_", "").isalnum() and not normalized[:1].isdigit():
            return key
        return json.dumps(key, ensure_ascii=False)

    def _toml_value(self, value: object) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, (int, float)):
            return repr(value)
        if isinstance(value, list):
            return "[" + ", ".join(self._toml_value(item) for item in value) + "]"
        if isinstance(value, dict):
            inline = ", ".join(f"{self._toml_key(k)} = {self._toml_value(v)}" for k, v in value.items())
            return "{ " + inline + " }"
        raise TypeError(f"Unsupported TOML value type: {type(value).__name__}")

    def _link_or_copy_dir(self, src: Path, dst: Path) -> None:
        if dst.exists():
            return
        linked_src = src.resolve()
        try:
            dst.symlink_to(linked_src, target_is_directory=True)
        except OSError:
            shutil.copytree(linked_src, dst, dirs_exist_ok=True)

    def _copy_skill_tree(self, src: Path, dst: Path) -> None:
        dst.mkdir(parents=True, exist_ok=True)
        for entry in src.iterdir():
            target = dst / entry.name
            if target.exists():
                continue
            if entry.is_dir():
                if entry.name in self._PRIVATE_SKILL_DIR_NAMES:
                    self._copy_skill_tree(entry, target)
                    continue
                if entry.name in self._SHARED_SKILL_DIR_NAMES or entry.name not in self._PRIVATE_SKILL_DIR_NAMES:
                    self._link_or_copy_dir(entry, target)
                    continue
            if entry.is_file():
                shutil.copy2(entry, target)

    def _merge_existing_system_skills(self, seed_home: Path) -> None:
        if self._state_root is None:
            return
        runs_root = self._state_root.parent.parent
        if not runs_root.exists():
            return
        seed_system = seed_home / "skills" / ".system"
        seed_system.mkdir(parents=True, exist_ok=True)
        for system_dir in runs_root.glob("*/state/claude_home/*/skills/.system"):
            if not system_dir.exists() or system_dir.resolve() == seed_system.resolve():
                continue
            for source in system_dir.rglob("*"):
                relative = source.relative_to(system_dir)
                target = seed_system / relative
                if target.exists():
                    continue
                if source.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)

    def _materialize_seed_home(self, source_home: Path) -> Path:
        if self._state_root is None:
            return source_home
        with self._seed_home_lock:
            if self._seed_home is not None:
                return self._seed_home
            seed_root = (self._state_root.parent.parent / "_seed_cache").resolve()
            seed_root.mkdir(parents=True, exist_ok=True)
            seed_home = seed_root / "claude_home"
            ready_marker = seed_home / ".ready"
            lock_path = seed_root / ".seed.lock"
            with FileLock(lock_path):
                if not ready_marker.exists() or not self._seeded_home_matches_source(seed_home, source_home):
                    seed_home.mkdir(parents=True, exist_ok=True)
                    self._seed_claude_home(seed_home, source_home)
                    ready_marker.write_text(str(source_home), encoding="utf-8")
                self._merge_existing_system_skills(seed_home)
            self._seed_home = seed_home
            return seed_home

    def execute(self, prepared: PreparedItemWorkspace, worker_id: str) -> ExecutionOutcome:
        item = prepared.item
        prompt = _build_simple_prompt(prepared, self._config)
        selected_provider = self._preferred_provider
        if selected_provider == "auto":
            selected_provider = self._config.routing.phase_defaults.get("simple", self._config.routing.default_provider)
        selected_provider = selected_provider if selected_provider in {"claude", "codex"} else "claude"

        if selected_provider == "codex":
            task = TaskNode(
                id=item.item_id,
                prompt_template="{prompt}",
                timeout=item.timeout_seconds,
                retry_policy=RetryPolicy(max_attempts=1),
                working_dir=str(prepared.cwd),
                output_format="text",
                system_prompt=build_simple_system_prompt(prepared),
                max_turns=self._config.simple.default_max_turns or None,
                ephemeral=self._config.simple.codex_exec_ephemeral,
                provider="codex",
                type="agent_cli",
                executor_config={"phase": "simple"},
            )
            started_at = datetime.now()
            execution_started_at = time.monotonic()
            result = run_agent_task(
                task=task,
                prompt=prompt,
                config=self._config,
                limits=self._config.limits,
                budget_tracker=self._budget,
                working_dir=str(prepared.cwd),
                on_progress=None,
                audit_logger=self._audit_logger,
                rate_limiter=self._rate_limiter,
                cli_provider="codex",
                phase_provider_overrides={"simple": "codex"},
            )
            execution_wall_ms = round((time.monotonic() - execution_started_at) * 1000, 1)
            changed_files = prepared.collect_changed_files()
            attempt = SimpleAttempt(
                item_id=item.item_id,
                attempt=item.attempt_state.attempt,
                status=SimpleItemStatus.EXECUTING,
                worker_id=worker_id,
                started_at=started_at,
                finished_at=result.finished_at,
                exit_code=0 if result.status.value == "success" else 1,
                changed_files=changed_files,
                output=result.output or "",
                error=result.error or "",
                cost_usd=result.cost_usd,
                model_used=result.model_used,
                provider_used=result.provider_used,
                pid=result.pid,
                token_input=result.token_input,
                token_output=result.token_output,
                cli_duration_ms=result.cli_duration_ms,
                execution_wall_ms=execution_wall_ms,
                tool_uses=result.tool_uses,
                turn_started=result.turn_started,
                turn_completed=result.turn_completed,
                max_turns_exceeded=result.max_turns_exceeded,
            )
            return ExecutionOutcome(result=result, attempt=attempt, changed_files=changed_files, prompt=prompt)

        env_overrides: dict[str, str] | None = None
        claude_home_ready_started_at = time.monotonic()
        claude_home = self._claude_home_for_worker(worker_id)
        claude_home_ready_ms = round((time.monotonic() - claude_home_ready_started_at) * 1000, 1)
        if claude_home is not None:
            env_overrides = self._build_worker_env_overrides(claude_home)
        task = TaskNode(
            id=item.item_id,
            prompt_template="{prompt}",
            timeout=item.timeout_seconds,
            retry_policy=RetryPolicy(max_attempts=1),
            working_dir=str(prepared.cwd),
            output_format="text",
            env_overrides=env_overrides,
            system_prompt=build_simple_system_prompt(prepared),
            max_turns=self._config.simple.default_max_turns or None,
            ephemeral=self._config.simple.claude_exec_ephemeral,
        )
        task_profile = self._pool_runtime.active_profile if self._pool_runtime else ""
        attempted_profiles: set[str] = set()
        started_at = datetime.now()
        execution_started_at = time.monotonic()
        while True:
            attempt_task = task
            claude_config = self._config.claude
            rate_limiter = self._rate_limiter
            if self._pool_runtime is not None:
                task_profile = task_profile or self._pool_runtime.active_profile
                attempt_task = self._pool_runtime.apply_task_overlay(task, task_profile)
                claude_config = self._pool_runtime.claude_config_for_profile(self._config.claude, task_profile)
                rate_limiter = self._pool_runtime.rate_limiter_for_profile(self._config.rate_limit, task_profile)
                attempted_profiles.add(task_profile)

            result = run_claude_task(
                attempt_task,
                prompt,
                claude_config,
                self._config.limits,
                budget_tracker=self._budget,
                working_dir=str(prepared.cwd),
                audit_logger=self._audit_logger,
                rate_limiter=rate_limiter,
            )
            if self._pool_runtime is None or result.status.value == "success":
                break

            failover_reason = classify_failover_reason(
                result.error or "",
                exit_code=getattr(result, "exit_code", 1),
                stderr=getattr(result, "stderr", ""),
            )
            profile_name = task_profile or self._pool_runtime.active_profile
            self._pool_runtime.record_failure(profile_name, failover_reason.value)

            if (
                self._store is not None
                and self._run_id
                and self._pool_runtime.should_trigger_process_takeover(profile_name, failover_reason.value)
            ):
                next_process_profile = self._pool_runtime.choose_process_profile(current_profile=profile_name)
                if next_process_profile is not None:
                    self._store.save_failover_event(
                        execution_id=self._run_id,
                        execution_kind="simple",
                        scope="process",
                        from_profile=profile_name,
                        to_profile=next_process_profile.name,
                        reason=failover_reason.value,
                        trigger_task_id=item.item_id,
                        metadata={"worker_id": worker_id},
                    )
                    self._pool_runtime.write_request(
                        "takeover",
                        target_profile=next_process_profile.name,
                        reason=failover_reason.value,
                        metadata={"run_id": self._run_id, "item_id": item.item_id},
                    )
                    if self._on_process_request is not None:
                        self._on_process_request(PoolRuntime.EXIT_CODE_TAKEOVER)

            can_task_switch = (
                failover_reason.value in self._pool_runtime.config.task_policy.allowed_reasons
                or failover_reason.value == "auth_expired"
            )
            if not can_task_switch:
                break
            next_task_profile = self._pool_runtime.choose_task_profile(
                current_profile=profile_name,
                tried_profiles=attempted_profiles,
            )
            if next_task_profile is None:
                break
            if self._store is not None and self._run_id:
                self._store.save_failover_event(
                    execution_id=self._run_id,
                    execution_kind="simple",
                    scope="task",
                    from_profile=profile_name,
                    to_profile=next_task_profile.name,
                    reason=failover_reason.value,
                    trigger_task_id=item.item_id,
                    metadata={"worker_id": worker_id},
                )
            task_profile = next_task_profile.name
        execution_wall_ms = round((time.monotonic() - execution_started_at) * 1000, 1)
        changed_files = prepared.collect_changed_files()
        attempt = SimpleAttempt(
            item_id=item.item_id,
            attempt=item.attempt_state.attempt,
            status=SimpleItemStatus.EXECUTING,
            worker_id=worker_id,
            started_at=started_at,
            finished_at=result.finished_at,
            exit_code=0 if result.status.value == "success" else 1,
            changed_files=changed_files,
            output=result.output or "",
            error=result.error or "",
            cost_usd=result.cost_usd,
            model_used=result.model_used,
            provider_used=result.provider_used or "claude",
            pid=result.pid,
            token_input=result.token_input,
            token_output=result.token_output,
            cli_duration_ms=result.cli_duration_ms,
            claude_home_ready_ms=claude_home_ready_ms,
            execution_wall_ms=execution_wall_ms,
            tool_uses=result.tool_uses,
            turn_started=result.turn_started,
            turn_completed=result.turn_completed,
            max_turns_exceeded=result.max_turns_exceeded,
        )
        return ExecutionOutcome(result=result, attempt=attempt, changed_files=changed_files, prompt=prompt)
