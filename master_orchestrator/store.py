"""SQLite checkpoint storage with WAL mode for crash safety."""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import threading
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, TypeVar

from .exceptions import CheckpointError, SchemaVersionError
from .file_lock import FileLock
from .model import RunInfo, RunStatus, TaskResult, TaskStatus
from .simple_model import (
    AttemptState,
    BucketStats,
    SimpleAttempt,
    SimpleItemType,
    SimpleItemStatus,
    SimpleManifest,
    SimpleRun,
    SimpleRunStatus,
    SimpleValidationProfile,
    SimpleWorkItem,
    ValidationReport,
    ValidationStageResult,
)

_logger = logging.getLogger(__name__)
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_SQLITE_LOCK_RETRIES = 8
_SQLITE_LOCK_BACKOFF_BASE_SECONDS = 0.05
_TxnResult = TypeVar("_TxnResult")

# Schema 迁移脚本注册表（版本号 -> SQL 脚本）
_MIGRATIONS: dict[int, str] = {}


def _decode_json_like_value(value: object) -> object:
    if isinstance(value, str) and value[:1] in {"{", "["}:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _simple_event_from_row(row: tuple | sqlite3.Row) -> dict:
    return {
        "event_id": row[0],
        "item_id": row[1],
        "bucket": row[2],
        "event_type": row[3],
        "level": row[4],
        "data": _decode_json_like_value(row[5]),
        "ts": row[6],
    }


def _register_migration(version: int, sql: str) -> None:
    """注册 schema 迁移脚本。

    Args:
        version: 迁移版本号（必须递增）
        sql: SQL 迁移脚本（可包含多条语句）
    """
    if version in _MIGRATIONS:
        raise ValueError(f"Migration version {version} already registered")
    _MIGRATIONS[version] = sql


# Version 1: 初始 schema（三张核心表）
_register_migration(1, """
CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    dag_name     TEXT NOT NULL,
    dag_hash     TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'running',
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    total_cost_usd REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS task_results (
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    attempt      INTEGER NOT NULL DEFAULT 0,
    output       TEXT,
    parsed_output TEXT,
    error        TEXT,
    cost_usd     REAL NOT NULL DEFAULT 0.0,
    model_used   TEXT NOT NULL DEFAULT '',
    started_at   TEXT,
    finished_at  TEXT,
    duration_seconds REAL NOT NULL DEFAULT 0.0,
    pid          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, task_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS task_attempts (
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    attempt      INTEGER NOT NULL,
    status       TEXT NOT NULL,
    prompt       TEXT,
    output       TEXT,
    error        TEXT,
    cost_usd     REAL NOT NULL DEFAULT 0.0,
    started_at   TEXT,
    finished_at  TEXT,
    PRIMARY KEY (run_id, task_id, attempt),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
""")

# Version 2: 新增 context_data 表（用于存储运行时上下文数据）
_register_migration(2, """
CREATE TABLE IF NOT EXISTS context_data (
    run_id       TEXT NOT NULL,
    key          TEXT NOT NULL,
    value        TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (run_id, key),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
""")

# Version 3: 新增 loop_state 表（用于存储循环任务的迭代状态）
_register_migration(3, """
CREATE TABLE IF NOT EXISTS loop_state (
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    iteration    INTEGER NOT NULL,
    last_output  TEXT,
    PRIMARY KEY (run_id, task_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
""")

# Version 4: 新增 stream_events 表（用于存储流式输出事件）
_register_migration(4, """
CREATE TABLE IF NOT EXISTS stream_events (
    run_id       TEXT NOT NULL,
    task_id      TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    event_type   TEXT NOT NULL,
    event_data   TEXT NOT NULL,
    ts           TEXT NOT NULL,
    PRIMARY KEY (run_id, task_id, seq),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
""")

# Version 5: 新增 handoff_data 表（用于持久化迭代间 Handoff 包）
_register_migration(5, """
CREATE TABLE IF NOT EXISTS handoff_data (
    phase_id     TEXT PRIMARY KEY,
    data         TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
""")

# Version 6: 新增 learning_memory 表（用于跨迭代学习记忆）
_register_migration(6, """
CREATE TABLE IF NOT EXISTS learning_memory (
    pattern_hash  TEXT PRIMARY KEY,
    pattern       TEXT NOT NULL,
    resolution    TEXT NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    fail_count    INTEGER NOT NULL DEFAULT 0,
    last_seen     TEXT NOT NULL
);
""")

# Version 7: simple mode tables
_register_migration(7, """
CREATE TABLE IF NOT EXISTS simple_runs (
    run_id       TEXT PRIMARY KEY,
    run_kind     TEXT NOT NULL DEFAULT 'simple',
    instruction_template TEXT NOT NULL DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'queued',
    source_summary TEXT NOT NULL DEFAULT '{}',
    isolation_mode TEXT NOT NULL DEFAULT 'none',
    scheduler_config_snapshot TEXT NOT NULL DEFAULT '{}',
    working_dir  TEXT NOT NULL DEFAULT '.',
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    manifest_path TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS simple_items (
    run_id       TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    item_type    TEXT NOT NULL,
    target       TEXT NOT NULL,
    bucket       TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    instruction  TEXT NOT NULL DEFAULT '',
    attempt_state TEXT NOT NULL DEFAULT '{}',
    validation_profile TEXT NOT NULL DEFAULT '{}',
    metadata     TEXT NOT NULL DEFAULT '{}',
    timeout_seconds INTEGER NOT NULL DEFAULT 1800,
    status       TEXT NOT NULL DEFAULT 'pending',
    last_error_category TEXT NOT NULL DEFAULT '',
    last_failure_reason TEXT NOT NULL DEFAULT '',
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (run_id, item_id),
    FOREIGN KEY (run_id) REFERENCES simple_runs(run_id)
);

CREATE TABLE IF NOT EXISTS simple_attempts (
    run_id       TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    attempt      INTEGER NOT NULL,
    status       TEXT NOT NULL,
    worker_id    TEXT NOT NULL DEFAULT '',
    started_at   TEXT,
    finished_at  TEXT,
    exit_code    INTEGER,
    error_category TEXT NOT NULL DEFAULT '',
    failure_reason TEXT NOT NULL DEFAULT '',
    changed_files TEXT NOT NULL DEFAULT '[]',
    validation_report TEXT,
    output       TEXT NOT NULL DEFAULT '',
    error        TEXT NOT NULL DEFAULT '',
    cost_usd     REAL NOT NULL DEFAULT 0.0,
    model_used   TEXT NOT NULL DEFAULT '',
    pid          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, item_id, attempt),
    FOREIGN KEY (run_id) REFERENCES simple_runs(run_id)
);

CREATE TABLE IF NOT EXISTS simple_events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    item_id      TEXT NOT NULL DEFAULT '',
    bucket       TEXT NOT NULL DEFAULT '',
    event_type   TEXT NOT NULL,
    level        TEXT NOT NULL DEFAULT 'info',
    data         TEXT NOT NULL DEFAULT '{}',
    ts           TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES simple_runs(run_id)
);

CREATE TABLE IF NOT EXISTS simple_manifests (
    run_id       TEXT PRIMARY KEY,
    data         TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES simple_runs(run_id)
);

CREATE TABLE IF NOT EXISTS simple_buckets (
    run_id       TEXT NOT NULL,
    bucket       TEXT NOT NULL,
    total_items  INTEGER NOT NULL DEFAULT 0,
    completed_items INTEGER NOT NULL DEFAULT 0,
    failed_items INTEGER NOT NULL DEFAULT 0,
    running_items INTEGER NOT NULL DEFAULT 0,
    retries      INTEGER NOT NULL DEFAULT 0,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (run_id, bucket),
    FOREIGN KEY (run_id) REFERENCES simple_runs(run_id)
);

CREATE TABLE IF NOT EXISTS simple_artifacts (
    run_id       TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    path         TEXT NOT NULL,
    metadata     TEXT NOT NULL DEFAULT '{}',
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (run_id, item_id, artifact_type, path),
    FOREIGN KEY (run_id) REFERENCES simple_runs(run_id)
);
""")

# Version 8: extend simple_attempts telemetry
_register_migration(8, """
ALTER TABLE simple_attempts ADD COLUMN token_input INTEGER;
ALTER TABLE simple_attempts ADD COLUMN token_output INTEGER;
ALTER TABLE simple_attempts ADD COLUMN cli_duration_ms REAL;
ALTER TABLE simple_attempts ADD COLUMN tool_uses INTEGER;
ALTER TABLE simple_attempts ADD COLUMN turn_started INTEGER;
ALTER TABLE simple_attempts ADD COLUMN turn_completed INTEGER;
ALTER TABLE simple_attempts ADD COLUMN max_turns_exceeded INTEGER NOT NULL DEFAULT 0;
""")

# Version 9: simple run heartbeat for stale-run recovery
_register_migration(9, """
ALTER TABLE simple_runs ADD COLUMN last_heartbeat_at TEXT;
UPDATE simple_runs
SET last_heartbeat_at = COALESCE(last_heartbeat_at, started_at)
WHERE last_heartbeat_at IS NULL OR last_heartbeat_at = '';
""")

# Version 10: pool metadata and failover events
_register_migration(10, """
ALTER TABLE runs ADD COLUMN pool_id TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN active_profile TEXT NOT NULL DEFAULT '';
ALTER TABLE simple_runs ADD COLUMN pool_id TEXT NOT NULL DEFAULT '';
ALTER TABLE simple_runs ADD COLUMN active_profile TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS failover_events (
    event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id  TEXT NOT NULL,
    execution_kind TEXT NOT NULL DEFAULT 'run',
    scope         TEXT NOT NULL DEFAULT 'task',
    from_profile  TEXT NOT NULL DEFAULT '',
    to_profile    TEXT NOT NULL DEFAULT '',
    reason        TEXT NOT NULL DEFAULT '',
    trigger_task_id TEXT NOT NULL DEFAULT '',
    metadata      TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL
);
""")

# Version 11: stop_reason for safe_stop support
_register_migration(11, """
ALTER TABLE simple_runs ADD COLUMN stop_reason TEXT NOT NULL DEFAULT '';
""")

_register_migration(12, """
ALTER TABLE task_results ADD COLUMN provider_used TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN provider_used TEXT NOT NULL DEFAULT '';
ALTER TABLE simple_attempts ADD COLUMN provider_used TEXT NOT NULL DEFAULT '';
""")

_register_migration(13, """
ALTER TABLE task_results ADD COLUMN depends_on_json TEXT NOT NULL DEFAULT '[]';
""")


class Store:
    """SQLite-backed checkpoint store with schema version management."""

    def __init__(self, db_path: str | Path = "./orchestrator_state.db"):
        self._db_path = str(db_path)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False, timeout=30)
        # 写路径会在 `_execute_write()` 内部触发 `_maybe_checkpoint()`，
        # 后者可能进一步调用 `cleanup_old_data()`；这里必须允许同线程重入，
        # 否则会在高并发长跑时把自己锁死。
        self._lock = threading.RLock()
        self._write_count = 0
        self._cleanup_counter = 0

        # 调试日志：记录 db_path 的绝对路径，便于排查多实例写入不同数据库的问题
        _abs_db_path = str(Path(self._db_path).resolve())
        _logger.info(
            "[Store.__init__] db_path=%s (resolved=%s), thread=%s",
            self._db_path, _abs_db_path, threading.current_thread().name,
        )

        # 磁盘空间预检：确保有足够空间进行写入操作
        try:
            usage = shutil.disk_usage(str(Path(self._db_path).parent or "."))
            free_mb = usage.free // (1024 * 1024)
            if free_mb < 100:  # 最低 100MB
                _logger.critical(
                    "磁盘空间严重不足: %dMB 可用 (最低 100MB)，数据库写入可能失败", free_mb
                )
        except Exception:
            pass  # 磁盘检查失败不阻断启动

        init_lock_path = Path(f"{self._db_path}.init.lock")
        try:
            with FileLock(init_lock_path):
                # 启用 WAL 模式，提升并发读写性能
                wal_result = self._execute_startup_sql_with_retry("PRAGMA journal_mode=WAL").fetchone()
                if wal_result and wal_result[0].lower() != "wal":
                    import logging
                    logging.getLogger(__name__).warning(
                        "SQLite WAL 模式启用失败 (当前: %s)，可能影响并发性能", wal_result[0],
                    )
                self._execute_startup_sql_with_retry("PRAGMA synchronous=NORMAL")
                self._execute_startup_sql_with_retry(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")

                # 确保 schema_version 表存在
                self._execute_startup_sql_with_retry("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TEXT NOT NULL
                    )
                """)
                self._conn.commit()

                # 执行增量迁移（失败时关闭连接避免泄漏）
                self._migrate_schema()
        except Exception:
            self._conn.close()
            raise

        # 调试日志：记录迁移后的 schema 版本和表状态
        try:
            _schema_ver = self._get_current_version()
            _table_count = self._conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
            ).fetchone()[0]
            _logger.info(
                "[Store.__init__] schema_version=%d, table_count=%d, db_path=%s",
                _schema_ver, _table_count, self._db_path,
            )
        except Exception as _e:
            _logger.warning("[Store.__init__] 无法读取 schema 版本: %s", _e)

        # 数据库完整性快速检查
        try:
            result = self._conn.execute("PRAGMA quick_check").fetchone()
            if result and result[0] != "ok":
                _logger.critical(
                    "数据库完整性检查失败: %s. 建议删除 %s 并重新运行。",
                    result[0], self._db_path
                )
        except Exception as e:
            _logger.warning("数据库完整性检查异常: %s", e)

    def _execute_startup_sql_with_retry(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        for attempt in range(8):
            try:
                return self._conn.execute(sql, params)
            except sqlite3.OperationalError as e:
                if "database is locked" not in str(e).lower() or attempt >= 7:
                    raise
                time.sleep(0.05 * (2 ** attempt))
        return self._conn.execute(sql, params)

    def _maybe_checkpoint(self) -> None:
        """定期执行 WAL checkpoint 防止 WAL 文件过大。

        高并发时 checkpoint 可能因数据库锁定而失败，最多重试 3 次，
        间隔指数退避（0.1s → 0.2s → 0.4s）。
        """
        self._write_count += 1
        if self._write_count >= 300:
            self._write_count = 0
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                    break
                except Exception as e:
                    if attempt >= max_retries - 1:
                        _logger.warning(
                            "WAL checkpoint 在 %d 次重试后仍失败: %s", max_retries, e,
                        )
                    else:
                        import time
                        time.sleep(0.1 * (2 ** attempt))

            # 每 3000 次写入触发一次历史数据清理
            self._cleanup_counter += 1
            if self._cleanup_counter >= 10:
                self._cleanup_counter = 0
                try:
                    self.cleanup_old_data()
                except Exception as e:
                    _logger.warning("自动清理失败: %s", e)

    def _execute_write(self, sql: str, params: tuple, operation: str = "write") -> sqlite3.Cursor:
        """执行写入操作，统一处理磁盘满和数据库锁定错误。

        Args:
            sql: SQL 语句
            params: SQL 参数
            operation: 操作描述（用于错误消息）

        Returns:
            执行后的 Cursor 对象

        Raises:
            CheckpointError: 磁盘满、数据库锁定或其他数据库错误
        """
        try:
            with self._lock:
                for attempt in range(_SQLITE_LOCK_RETRIES):
                    try:
                        cur = self._conn.execute(sql, params)
                        self._conn.commit()
                        self._maybe_checkpoint()
                        return cur
                    except sqlite3.OperationalError as e:
                        err_msg = str(e).lower()
                        try:
                            self._conn.rollback()
                        except sqlite3.Error:
                            pass
                        if "database is locked" not in err_msg or attempt >= _SQLITE_LOCK_RETRIES - 1:
                            raise
                        time.sleep(_SQLITE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
                cur = self._conn.execute(sql, params)
                self._conn.commit()
                self._maybe_checkpoint()
                return cur
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "disk" in err_msg or "full" in err_msg or "no space" in err_msg:
                raise CheckpointError(
                    f"磁盘空间不足，{operation}失败: {e}. "
                    "请清理磁盘空间后重试。"
                ) from e
            if "database is locked" in err_msg:
                raise CheckpointError(
                    f"数据库锁定，{operation}失败: {e}. "
                    "可能有其他进程正在访问数据库。"
                ) from e
            raise CheckpointError(f"{operation}失败: {e}") from e
        except sqlite3.Error as e:
            raise CheckpointError(f"{operation}失败: {e}") from e

    def _execute_transaction(
        self,
        operation: str,
        action: Callable[[sqlite3.Connection], _TxnResult],
    ) -> _TxnResult:
        try:
            with self._lock:
                for attempt in range(_SQLITE_LOCK_RETRIES):
                    try:
                        result = action(self._conn)
                        self._conn.commit()
                        self._maybe_checkpoint()
                        return result
                    except sqlite3.OperationalError as e:
                        err_msg = str(e).lower()
                        try:
                            self._conn.rollback()
                        except sqlite3.Error:
                            pass
                        if "database is locked" not in err_msg or attempt >= _SQLITE_LOCK_RETRIES - 1:
                            raise
                        time.sleep(_SQLITE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
                result = action(self._conn)
                self._conn.commit()
                self._maybe_checkpoint()
                return result
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "disk" in err_msg or "full" in err_msg or "no space" in err_msg:
                raise CheckpointError(
                    f"磁盘空间不足，{operation}失败: {e}. "
                    "请清理磁盘空间后重试。"
                ) from e
            if "database is locked" in err_msg:
                raise CheckpointError(
                    f"数据库锁定，{operation}失败: {e}. "
                    "可能有其他进程正在访问数据库。"
                ) from e
            raise CheckpointError(f"{operation}失败: {e}") from e
        except sqlite3.Error as e:
            raise CheckpointError(f"{operation}失败: {e}") from e

    def close(self) -> None:
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        self._conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False  # 不吞掉异常

    def _get_current_version(self) -> int:
        """获取当前 schema 版本号。

        Returns:
            当前版本号，如果未应用任何迁移则返回 0
        """
        row = self._conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        return row[0] if row and row[0] is not None else 0

    def _migrate_schema(self) -> None:
        """执行增量 schema 迁移，每个迁移在独立事务中执行。

        Raises:
            SchemaVersionError: 迁移失败时抛出
        """
        for version in sorted(_MIGRATIONS.keys()):
            migration_sql = _MIGRATIONS[version]
            for attempt in range(_SQLITE_LOCK_RETRIES):
                try:
                    # 用 IMMEDIATE 锁串行化首次启动阶段的 schema 迁移，避免多个 simple run
                    # 同时初始化共享 DB 时出现 database is locked / duplicate version 竞争。
                    self._conn.execute("BEGIN IMMEDIATE")

                    existing = self._conn.execute(
                        "SELECT 1 FROM schema_version WHERE version = ?",
                        (version,),
                    ).fetchone()
                    if existing:
                        self._conn.commit()
                        break

                    # 将 SQL 脚本拆分为单条语句逐条执行（避免 executescript 的隐式 COMMIT）
                    statements = [
                        stmt.strip()
                        for stmt in migration_sql.split(';')
                        if stmt.strip() and not stmt.strip().startswith('--')
                    ]
                    for stmt in statements:
                        self._conn.execute(stmt)

                    # 记录迁移版本
                    self._conn.execute(
                        "INSERT OR IGNORE INTO schema_version (version, applied_at) VALUES (?, ?)",
                        (version, datetime.now().isoformat())
                    )
                    self._conn.commit()
                    break
                except sqlite3.Error as e:
                    err_msg = str(e).lower()
                    self._conn.rollback()
                    if "database is locked" in err_msg and attempt < _SQLITE_LOCK_RETRIES - 1:
                        time.sleep(_SQLITE_LOCK_BACKOFF_BASE_SECONDS * (2 ** attempt))
                        continue
                    raise SchemaVersionError(
                        f"Failed to apply migration version {version}: {e}",
                        context={"version": version, "sql": migration_sql}
                    ) from e

    # ── Run operations ──

    def create_run(self, info: RunInfo) -> None:
        """创建新的运行记录。"""
        self._execute_write(
            "INSERT INTO runs (run_id, dag_name, dag_hash, status, started_at, total_cost_usd, pool_id, active_profile) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                info.run_id,
                info.dag_name,
                info.dag_hash,
                info.status.value,
                info.started_at.isoformat(),
                info.total_cost_usd,
                info.pool_id,
                info.active_profile,
            ),
            operation="create_run",
        )

    def update_run_status(self, run_id: str, status: RunStatus, cost: float | None = None) -> None:
        """更新运行状态。"""
        fields = ["status = ?"]
        params: list = [status.value]
        if status == RunStatus.RUNNING:
            fields.append("finished_at = ?")
            params.append(None)
        if status in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
            fields.append("finished_at = ?")
            params.append(datetime.now().isoformat())
        if cost is not None:
            fields.append("total_cost_usd = ?")
            params.append(cost)
        params.append(run_id)
        cur = self._execute_write(
            f"UPDATE runs SET {', '.join(fields)} WHERE run_id = ?",
            tuple(params),
            operation="update_run_status",
        )
        _logger.info(
            "[update_run_status] run_id=%s, status=%s, cost=%s, affected_rows=%s",
            run_id, status.value, cost, cur.rowcount if cur else "None",
        )
        if cur and cur.rowcount == 0:
            _logger.warning(
                "[update_run_status] UPDATE 影响行数为 0！run_id=%s, status=%s — "
                "可能是 create_run 未被调用或 run_id 不匹配",
                run_id, status.value,
            )

    def update_run_pool_info(
        self,
        run_id: str,
        *,
        pool_id: str | None = None,
        active_profile: str | None = None,
    ) -> None:
        fields: list[str] = []
        params: list[object] = []
        if pool_id is not None:
            fields.append("pool_id = ?")
            params.append(pool_id)
        if active_profile is not None:
            fields.append("active_profile = ?")
            params.append(active_profile)
        if not fields:
            return
        params.append(run_id)
        self._execute_write(
            f"UPDATE runs SET {', '.join(fields)} WHERE run_id = ?",
            tuple(params),
            operation="update_run_pool_info",
        )

    def get_run(self, run_id: str) -> RunInfo | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id, dag_name, dag_hash, status, started_at, finished_at, total_cost_usd, pool_id, active_profile "
                "FROM runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        if not row:
            return None
        return RunInfo(
            run_id=row[0], dag_name=row[1], dag_hash=row[2],
            status=RunStatus(row[3]),
            started_at=datetime.fromisoformat(row[4]),
            finished_at=datetime.fromisoformat(row[5]) if row[5] else None,
            total_cost_usd=row[6],
            pool_id=row[7] or "",
            active_profile=row[8] or "",
        )

    def get_latest_run(self, dag_name: str | None = None) -> RunInfo | None:
        with self._lock:
            if dag_name:
                row = self._conn.execute(
                    "SELECT run_id FROM runs WHERE dag_name = ? ORDER BY started_at DESC LIMIT 1",
                    (dag_name,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1"
                ).fetchone()
        return self.get_run(row[0]) if row else None

    def list_runs(self, limit: int = 50) -> list[RunInfo]:
        """查询最近 N 条 DAG 运行记录。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [r for r in (self.get_run(row[0]) for row in rows) if r is not None]

    # ── Task result operations ──

    def init_task(self, run_id: str, task_id: str, depends_on: list[str] | None = None) -> None:
        """初始化任务记录（如果不存在）。"""
        _logger.debug("[init_task] run_id=%s, task_id=%s", run_id, task_id)
        deps_json = json.dumps(depends_on or [], ensure_ascii=False)
        self._execute_write(
            "INSERT OR IGNORE INTO task_results (run_id, task_id, status, depends_on_json) VALUES (?, ?, ?, ?)",
            (run_id, task_id, TaskStatus.PENDING.value, deps_json),
            operation="init_task",
        )

    def update_task(self, run_id: str, result: TaskResult) -> None:
        """更新任务结果。"""
        try:
            parsed = json.dumps(result.parsed_output) if result.parsed_output is not None else None
        except (TypeError, ValueError) as e:
            raise CheckpointError(f"Failed to serialize parsed_output: {e}") from e
        cur = self._execute_write(
            "UPDATE task_results SET status=?, attempt=?, output=?, parsed_output=?, "
            "error=?, cost_usd=?, model_used=?, provider_used=?, started_at=?, finished_at=?, "
            "duration_seconds=?, pid=? WHERE run_id=? AND task_id=?",
            (
                result.status.value, result.attempt, result.output, parsed,
                result.error, result.cost_usd, result.model_used, result.provider_used,
                result.started_at.isoformat() if result.started_at else None,
                result.finished_at.isoformat() if result.finished_at else None,
                result.duration_seconds, result.pid,
                run_id, result.task_id,
            ),
            operation="update_task",
        )
        _logger.debug(
            "[update_task] run_id=%s, task_id=%s, status=%s, cost=%.4f, affected_rows=%s",
            run_id, result.task_id, result.status.value, result.cost_usd,
            cur.rowcount if cur else "None",
        )
        if cur and cur.rowcount == 0:
            _logger.warning(
                "[update_task] UPDATE 影响行数为 0！run_id=%s, task_id=%s — "
                "可能是 init_task 未被调用或 run_id 不匹配",
                run_id, result.task_id,
            )

    def save_attempt(self, run_id: str, result: TaskResult, prompt: str | None = None) -> None:
        """保存任务尝试记录。"""
        self._execute_write(
            "INSERT OR REPLACE INTO task_attempts "
            "(run_id, task_id, attempt, status, prompt, output, error, cost_usd, provider_used, started_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id, result.task_id, result.attempt, result.status.value,
                prompt, result.output, result.error, result.cost_usd, result.provider_used,
                result.started_at.isoformat() if result.started_at else None,
                result.finished_at.isoformat() if result.finished_at else None,
            ),
            operation="save_attempt",
        )

    def get_task_result(self, run_id: str, task_id: str) -> TaskResult | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT task_id, status, attempt, output, parsed_output, error, cost_usd, "
                "model_used, provider_used, started_at, finished_at, duration_seconds, pid "
                "FROM task_results WHERE run_id=? AND task_id=?",
                (run_id, task_id),
            ).fetchone()
        if not row:
            return None
        try:
            parsed_output = json.loads(row[4]) if row[4] else None
        except json.JSONDecodeError as e:
            raise CheckpointError(f"Corrupted JSON in parsed_output for task {row[0]}: {e}") from e
        
        return TaskResult(
            task_id=row[0],
            status=TaskStatus(row[1]),
            attempt=row[2],
            output=row[3],
            parsed_output=parsed_output,
            error=row[5],
            cost_usd=row[6],
            model_used=row[7],
            provider_used=row[8],
            started_at=datetime.fromisoformat(row[9]) if row[9] else None,
            finished_at=datetime.fromisoformat(row[10]) if row[10] else None,
            duration_seconds=row[11],
            pid=row[12],
        )

    def get_all_task_results(self, run_id: str) -> dict[str, TaskResult]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_id, status, attempt, output, parsed_output, error, cost_usd, "
                "model_used, provider_used, started_at, finished_at, duration_seconds, pid "
                "FROM task_results WHERE run_id=?",
                (run_id,),
            ).fetchall()
        results = {}
        for row in rows:
            try:
                parsed_output = json.loads(row[4]) if row[4] else None
            except json.JSONDecodeError as e:
                raise CheckpointError(f"Corrupted JSON in parsed_output for task {row[0]}: {e}") from e

            results[row[0]] = TaskResult(
                task_id=row[0],
                status=TaskStatus(row[1]),
                attempt=row[2],
                output=row[3],
                parsed_output=parsed_output,
                error=row[5],
                cost_usd=row[6],
                model_used=row[7],
                provider_used=row[8],
                started_at=datetime.fromisoformat(row[9]) if row[9] else None,
                finished_at=datetime.fromisoformat(row[10]) if row[10] else None,
                duration_seconds=row[11],
                pid=row[12],
            )
        return results

    def get_task_dependencies(self, run_id: str) -> dict[str, list[str]]:
        """获取运行中每个任务的依赖关系。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_id, depends_on_json FROM task_results WHERE run_id=?",
                (run_id,),
            ).fetchall()
        deps = {}
        for row in rows:
            try:
                deps[row[0]] = json.loads(row[1]) if row[1] else []
            except json.JSONDecodeError:
                deps[row[0]] = []
        return deps

    def reset_running_tasks(self, run_id: str) -> int:
        """将 RUNNING 状态的任务重置为 PENDING（崩溃恢复）。返回重置数量。"""
        try:
            with self._lock:
                cur = self._conn.execute(
                    "UPDATE task_results SET status=? WHERE run_id=? AND status=?",
                    (TaskStatus.PENDING.value, run_id, TaskStatus.RUNNING.value),
                )
                self._conn.commit()
                self._maybe_checkpoint()
                return cur.rowcount
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "disk" in err_msg or "full" in err_msg or "no space" in err_msg:
                raise CheckpointError(
                    f"磁盘空间不足，reset_running_tasks失败: {e}. "
                    "请清理磁盘空间后重试。"
                ) from e
            if "database is locked" in err_msg:
                raise CheckpointError(
                    f"数据库锁定，reset_running_tasks失败: {e}. "
                    "可能有其他进程正在访问数据库。"
                ) from e
            raise CheckpointError(f"reset_running_tasks失败: {e}") from e
        except sqlite3.Error as e:
            raise CheckpointError(f"reset_running_tasks失败: {e}") from e

    def reset_failed_and_downstream(self, run_id: str, failed_ids: set[str], all_task_ids: dict[str, list[str]]) -> int:
        """将失败任务及其下游依赖重置为 PENDING。
        all_task_ids 映射 task_id -> depends_on 列表。"""
        # 查找所有下游任务
        to_reset = set(failed_ids)
        changed = True
        while changed:
            changed = False
            for tid, deps in all_task_ids.items():
                if tid not in to_reset and any(d in to_reset for d in deps):
                    to_reset.add(tid)
                    changed = True

        # 重置任务状态
        try:
            with self._lock:
                for tid in to_reset:
                    self._conn.execute(
                        "UPDATE task_results SET status=?, error=NULL WHERE run_id=? AND task_id=?",
                        (TaskStatus.PENDING.value, run_id, tid),
                    )
                self._conn.commit()
                self._maybe_checkpoint()
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "disk" in err_msg or "full" in err_msg or "no space" in err_msg:
                raise CheckpointError(
                    f"磁盘空间不足，reset_failed_and_downstream失败: {e}. "
                    "请清理磁盘空间后重试。"
                ) from e
            if "database is locked" in err_msg:
                raise CheckpointError(
                    f"数据库锁定，reset_failed_and_downstream失败: {e}. "
                    "可能有其他进程正在访问数据库。"
                ) from e
            raise CheckpointError(f"reset_failed_and_downstream失败: {e}") from e
        except sqlite3.Error as e:
            raise CheckpointError(f"reset_failed_and_downstream失败: {e}") from e
        return len(to_reset)

    def get_total_cost(self, run_id: str) -> float:
        with self._lock:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(cost_usd), 0) FROM task_results WHERE run_id=?",
                (run_id,),
            ).fetchone()
        return row[0] if row else 0.0

    # ── Simple mode operations ──

    def create_simple_run(self, run: SimpleRun) -> None:
        self._execute_write(
            "INSERT OR REPLACE INTO simple_runs "
            "(run_id, run_kind, instruction_template, status, source_summary, isolation_mode, "
            "scheduler_config_snapshot, working_dir, started_at, last_heartbeat_at, finished_at, manifest_path, pool_id, active_profile, stop_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run.run_id,
                run.run_kind,
                run.instruction_template,
                run.status.value,
                json.dumps(run.source_summary, ensure_ascii=False),
                run.isolation_mode,
                json.dumps(run.scheduler_config_snapshot, ensure_ascii=False),
                run.working_dir,
                run.started_at.isoformat(),
                (run.last_heartbeat_at or run.started_at).isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                run.manifest_path,
                run.pool_id,
                run.active_profile,
                run.stop_reason,
            ),
            operation="create_simple_run",
        )

    def create_simple_run_bundle(
        self,
        info: RunInfo,
        run: SimpleRun,
        items: list[SimpleWorkItem],
        bucket_stats: list[BucketStats],
    ) -> None:
        run_row = (
            run.run_id,
            run.run_kind,
            run.instruction_template,
            run.status.value,
            json.dumps(run.source_summary, ensure_ascii=False),
            run.isolation_mode,
            json.dumps(run.scheduler_config_snapshot, ensure_ascii=False),
            run.working_dir,
            run.started_at.isoformat(),
            (run.last_heartbeat_at or run.started_at).isoformat(),
            run.finished_at.isoformat() if run.finished_at else None,
            run.manifest_path,
            run.pool_id,
            run.active_profile,
            run.stop_reason,
        )
        items_updated_at = datetime.now().isoformat()
        item_rows = [
            (
                run.run_id,
                item.item_id,
                item.item_type.value,
                item.target,
                item.bucket,
                item.priority,
                item.instruction,
                json.dumps(item.attempt_state.to_dict(), ensure_ascii=False),
                json.dumps(asdict(item.validation_profile), ensure_ascii=False),
                json.dumps(item.metadata, ensure_ascii=False),
                item.timeout_seconds,
                item.status.value,
                item.attempt_state.last_error_category,
                item.attempt_state.last_failure_reason,
                items_updated_at,
            )
            for item in items
        ]
        bucket_updated_at = datetime.now().isoformat()
        bucket_rows = [
            (
                run.run_id,
                stats.name,
                stats.total_items,
                stats.completed_items,
                stats.failed_items,
                stats.running_items,
                stats.retries,
                bucket_updated_at,
            )
            for stats in bucket_stats
        ]

        def _action(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT INTO runs (run_id, dag_name, dag_hash, status, started_at, total_cost_usd, pool_id, active_profile) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    info.run_id,
                    info.dag_name,
                    info.dag_hash,
                    info.status.value,
                    info.started_at.isoformat(),
                    info.total_cost_usd,
                    info.pool_id,
                    info.active_profile,
                ),
            )
            conn.execute(
                "INSERT OR REPLACE INTO simple_runs "
                "(run_id, run_kind, instruction_template, status, source_summary, isolation_mode, "
                "scheduler_config_snapshot, working_dir, started_at, last_heartbeat_at, finished_at, manifest_path, pool_id, active_profile, stop_reason) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                run_row,
            )
            if item_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO simple_items "
                    "(run_id, item_id, item_type, target, bucket, priority, instruction, attempt_state, "
                    "validation_profile, metadata, timeout_seconds, status, last_error_category, "
                    "last_failure_reason, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    item_rows,
                )
            if bucket_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO simple_buckets "
                    "(run_id, bucket, total_items, completed_items, failed_items, running_items, retries, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    bucket_rows,
                )

        self._execute_transaction("create_simple_run_bundle", _action)

    def update_simple_run(
        self,
        run_id: str,
        *,
        status: SimpleRunStatus | None = None,
        manifest_path: str | None = None,
        source_summary: dict | None = None,
        scheduler_config_snapshot: dict | None = None,
        last_heartbeat_at: datetime | None = None,
        finished_at: datetime | None = None,
        clear_finished_at: bool = False,
        pool_id: str | None = None,
        active_profile: str | None = None,
        stop_reason: str | None = None,
    ) -> None:
        fields: list[str] = []
        params: list[object] = []
        if status is not None:
            fields.append("status = ?")
            params.append(status.value)
        if manifest_path is not None:
            fields.append("manifest_path = ?")
            params.append(manifest_path)
        if source_summary is not None:
            fields.append("source_summary = ?")
            params.append(json.dumps(source_summary, ensure_ascii=False))
        if scheduler_config_snapshot is not None:
            fields.append("scheduler_config_snapshot = ?")
            params.append(json.dumps(scheduler_config_snapshot, ensure_ascii=False))
        if last_heartbeat_at is not None:
            fields.append("last_heartbeat_at = ?")
            params.append(last_heartbeat_at.isoformat())
        if clear_finished_at:
            fields.append("finished_at = ?")
            params.append(None)
        if finished_at is not None:
            fields.append("finished_at = ?")
            params.append(finished_at.isoformat())
        if pool_id is not None:
            fields.append("pool_id = ?")
            params.append(pool_id)
        if active_profile is not None:
            fields.append("active_profile = ?")
            params.append(active_profile)
        if stop_reason is not None:
            fields.append("stop_reason = ?")
            params.append(stop_reason)
        if not fields:
            return
        params.append(run_id)
        self._execute_write(
            f"UPDATE simple_runs SET {', '.join(fields)} WHERE run_id = ?",
            tuple(params),
            operation="update_simple_run",
        )

    def get_simple_run(self, run_id: str) -> SimpleRun | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id, run_kind, instruction_template, status, source_summary, "
                "isolation_mode, scheduler_config_snapshot, working_dir, started_at, "
                "last_heartbeat_at, finished_at, manifest_path, pool_id, active_profile, stop_reason "
                "FROM simple_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return SimpleRun(
            run_id=row[0],
            run_kind=row[1],
            instruction_template=row[2],
            status=SimpleRunStatus(row[3]),
            source_summary=json.loads(row[4] or "{}"),
            isolation_mode=row[5],
            scheduler_config_snapshot=json.loads(row[6] or "{}"),
            working_dir=row[7],
            started_at=datetime.fromisoformat(row[8]),
            last_heartbeat_at=datetime.fromisoformat(row[9]) if row[9] else None,
            finished_at=datetime.fromisoformat(row[10]) if row[10] else None,
            manifest_path=row[11],
            pool_id=row[12] or "",
            active_profile=row[13] or "",
            stop_reason=row[14] or "",
        )

    def get_latest_simple_run(self) -> SimpleRun | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id FROM simple_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        return self.get_simple_run(row[0]) if row else None

    def get_latest_simple_run_for_working_dir(self, working_dir: str) -> SimpleRun | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT run_id FROM simple_runs WHERE working_dir = ? ORDER BY started_at DESC LIMIT 1",
                (working_dir,),
            ).fetchone()
        return self.get_simple_run(row[0]) if row else None

    def list_simple_runs(self, statuses: list[SimpleRunStatus] | None = None) -> list[SimpleRun]:
        sql = (
            "SELECT run_id, run_kind, instruction_template, status, source_summary, "
            "isolation_mode, scheduler_config_snapshot, working_dir, started_at, "
            "last_heartbeat_at, finished_at, manifest_path, pool_id, active_profile, stop_reason "
            "FROM simple_runs"
        )
        params: list[object] = []
        if statuses:
            sql += f" WHERE status IN ({', '.join('?' for _ in statuses)})"
            params.extend(status.value for status in statuses)
        sql += " ORDER BY started_at DESC"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        runs: list[SimpleRun] = []
        for row in rows:
            runs.append(
                SimpleRun(
                    run_id=row[0],
                    run_kind=row[1],
                    instruction_template=row[2],
                    status=SimpleRunStatus(row[3]),
                    source_summary=json.loads(row[4] or "{}"),
                    isolation_mode=row[5],
                    scheduler_config_snapshot=json.loads(row[6] or "{}"),
                    working_dir=row[7],
                    started_at=datetime.fromisoformat(row[8]),
                    last_heartbeat_at=datetime.fromisoformat(row[9]) if row[9] else None,
                    finished_at=datetime.fromisoformat(row[10]) if row[10] else None,
                    manifest_path=row[11],
                    pool_id=row[12] or "",
                    active_profile=row[13] or "",
                    stop_reason=row[14] or "",
                )
            )
        return runs

    def touch_simple_run_heartbeat(self, run_id: str, at: datetime | None = None) -> None:
        heartbeat_at = at or datetime.now()
        self._execute_write(
            "UPDATE simple_runs SET last_heartbeat_at = ? WHERE run_id = ?",
            (heartbeat_at.isoformat(), run_id),
            operation="touch_simple_run_heartbeat",
        )

    def find_stale_simple_runs(
        self,
        stale_before: datetime,
        *,
        statuses: list[SimpleRunStatus] | None = None,
    ) -> list[SimpleRun]:
        sql = (
            "SELECT run_id, run_kind, instruction_template, status, source_summary, "
            "isolation_mode, scheduler_config_snapshot, working_dir, started_at, "
            "last_heartbeat_at, finished_at, manifest_path, "
            "pool_id, active_profile, stop_reason "
            "FROM simple_runs "
            "WHERE finished_at IS NULL AND COALESCE(last_heartbeat_at, started_at) <= ?"
        )
        params: list[object] = [stale_before.isoformat()]
        if statuses:
            sql += f" AND status IN ({', '.join('?' for _ in statuses)})"
            params.extend(status.value for status in statuses)
        sql += " ORDER BY started_at DESC"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        result: list[SimpleRun] = []
        for row in rows:
            result.append(
                SimpleRun(
                    run_id=row[0],
                    run_kind=row[1],
                    instruction_template=row[2],
                    status=SimpleRunStatus(row[3]),
                    source_summary=json.loads(row[4] or "{}"),
                    isolation_mode=row[5],
                    scheduler_config_snapshot=json.loads(row[6] or "{}"),
                    working_dir=row[7],
                    started_at=datetime.fromisoformat(row[8]),
                    last_heartbeat_at=datetime.fromisoformat(row[9]) if row[9] else None,
                    finished_at=datetime.fromisoformat(row[10]) if row[10] else None,
                    manifest_path=row[11],
                    pool_id=row[12] or "",
                    active_profile=row[13] or "",
                    stop_reason=row[14] or "",
                )
            )
        return result

    def count_live_simple_runs(
        self,
        stale_before: datetime,
        *,
        statuses: list[SimpleRunStatus] | None = None,
    ) -> int:
        sql = (
            "SELECT COUNT(*) FROM simple_runs "
            "WHERE finished_at IS NULL AND COALESCE(last_heartbeat_at, started_at) > ?"
        )
        params: list[object] = [stale_before.isoformat()]
        if statuses:
            sql += f" AND status IN ({', '.join('?' for _ in statuses)})"
            params.extend(status.value for status in statuses)
        with self._lock:
            row = self._conn.execute(sql, tuple(params)).fetchone()
        return int(row[0]) if row else 0

    def upsert_simple_item(self, run_id: str, item: SimpleWorkItem) -> None:
        self._execute_write(
            "INSERT OR REPLACE INTO simple_items "
            "(run_id, item_id, item_type, target, bucket, priority, instruction, attempt_state, "
            "validation_profile, metadata, timeout_seconds, status, last_error_category, "
            "last_failure_reason, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                item.item_id,
                item.item_type.value,
                item.target,
                item.bucket,
                item.priority,
                item.instruction,
                json.dumps(item.attempt_state.to_dict(), ensure_ascii=False),
                json.dumps(asdict(item.validation_profile), ensure_ascii=False),
                json.dumps(item.metadata, ensure_ascii=False),
                item.timeout_seconds,
                item.status.value,
                item.attempt_state.last_error_category,
                item.attempt_state.last_failure_reason,
                datetime.now().isoformat(),
            ),
            operation="upsert_simple_item",
        )

    def get_simple_item(self, run_id: str, item_id: str) -> SimpleWorkItem | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT item_id, item_type, target, bucket, priority, instruction, attempt_state, "
                "validation_profile, metadata, timeout_seconds, status "
                "FROM simple_items WHERE run_id = ? AND item_id = ?",
                (run_id, item_id),
            ).fetchone()
        if not row:
            return None
        attempt_state = json.loads(row[6] or "{}")
        validation_profile = json.loads(row[7] or "{}")
        metadata = json.loads(row[8] or "{}")
        next_retry_at = attempt_state.get("next_retry_at")
        return SimpleWorkItem(
            item_id=row[0],
            item_type=SimpleItemType(row[1]),
            target=row[2],
            bucket=row[3],
            priority=row[4],
            instruction=row[5],
            attempt_state=AttemptState(
                attempt=attempt_state.get("attempt", 0),
                max_attempts=attempt_state.get("max_attempts", 3),
                last_error_category=attempt_state.get("last_error_category", ""),
                last_failure_reason=attempt_state.get("last_failure_reason", ""),
                next_retry_at=datetime.fromisoformat(next_retry_at) if next_retry_at else None,
            ),
            validation_profile=SimpleValidationProfile(
                verify_commands=list(validation_profile.get("verify_commands", [])),
                require_patterns=list(validation_profile.get("require_patterns", [])),
                allowed_side_files=list(validation_profile.get("allowed_side_files", [])),
            ),
            metadata=metadata,
            timeout_seconds=row[9],
            status=SimpleItemStatus(row[10]),
        )

    def get_simple_items(self, run_id: str, statuses: list[SimpleItemStatus] | None = None) -> list[SimpleWorkItem]:
        sql = (
            "SELECT item_id, item_type, target, bucket, priority, instruction, attempt_state, "
            "validation_profile, metadata, timeout_seconds, status "
            "FROM simple_items WHERE run_id = ?"
        )
        params: list[object] = [run_id]
        if statuses:
            sql += f" AND status IN ({', '.join('?' for _ in statuses)})"
            params.extend(status.value for status in statuses)
        sql += " ORDER BY bucket, priority DESC, item_id"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        items: list[SimpleWorkItem] = []
        for row in rows:
            attempt_state = json.loads(row[6] or "{}")
            validation_profile = json.loads(row[7] or "{}")
            metadata = json.loads(row[8] or "{}")
            next_retry_at = attempt_state.get("next_retry_at")
            items.append(SimpleWorkItem(
                item_id=row[0],
                item_type=SimpleItemType(row[1]),
                target=row[2],
                bucket=row[3],
                priority=row[4],
                instruction=row[5],
                attempt_state=AttemptState(
                    attempt=attempt_state.get("attempt", 0),
                    max_attempts=attempt_state.get("max_attempts", 3),
                    last_error_category=attempt_state.get("last_error_category", ""),
                    last_failure_reason=attempt_state.get("last_failure_reason", ""),
                    next_retry_at=datetime.fromisoformat(next_retry_at) if next_retry_at else None,
                ),
                validation_profile=SimpleValidationProfile(
                    verify_commands=list(validation_profile.get("verify_commands", [])),
                    require_patterns=list(validation_profile.get("require_patterns", [])),
                    allowed_side_files=list(validation_profile.get("allowed_side_files", [])),
                ),
                metadata=metadata,
                timeout_seconds=row[9],
                status=SimpleItemStatus(row[10]),
            ))
        return items

    def get_simple_item_counts(self, run_id: str) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) FROM simple_items WHERE run_id = ? GROUP BY status",
                (run_id,),
            ).fetchall()
        return {str(status): int(count) for status, count in rows}

    def update_simple_item_status(
        self,
        run_id: str,
        item_id: str,
        status: SimpleItemStatus,
        *,
        attempt_state: dict | None = None,
        last_error_category: str | None = None,
        last_failure_reason: str | None = None,
    ) -> None:
        fields = ["status = ?", "updated_at = ?"]
        params: list[object] = [status.value, datetime.now().isoformat()]
        if attempt_state is not None:
            fields.append("attempt_state = ?")
            params.append(json.dumps(attempt_state, ensure_ascii=False))
        if last_error_category is not None:
            fields.append("last_error_category = ?")
            params.append(last_error_category)
        if last_failure_reason is not None:
            fields.append("last_failure_reason = ?")
            params.append(last_failure_reason)
        params.extend([run_id, item_id])
        self._execute_write(
            f"UPDATE simple_items SET {', '.join(fields)} WHERE run_id = ? AND item_id = ?",
            tuple(params),
            operation="update_simple_item_status",
        )

    def reset_simple_items(
        self,
        run_id: str,
        from_statuses: list[SimpleItemStatus],
        to_status: SimpleItemStatus,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE simple_items SET status = ?, updated_at = ? "
                f"WHERE run_id = ? AND status IN ({', '.join('?' for _ in from_statuses)})",
                (
                    to_status.value,
                    datetime.now().isoformat(),
                    run_id,
                    *(status.value for status in from_statuses),
                ),
            )
            self._conn.commit()
            self._maybe_checkpoint()
            return cur.rowcount

    def close_simple_run_state(
        self,
        run_id: str,
        *,
        simple_status: SimpleRunStatus,
        run_status: RunStatus,
        item_from_statuses: list[SimpleItemStatus],
        item_to_status: SimpleItemStatus,
        finished_at: datetime | None = None,
        last_heartbeat_at: datetime | None = None,
    ) -> dict[str, int | str]:
        closed_at = finished_at or datetime.now()
        heartbeat_at = last_heartbeat_at or closed_at

        def _action(conn: sqlite3.Connection) -> dict[str, int | str]:
            item_cur = conn.execute(
                f"UPDATE simple_items SET status = ?, updated_at = ? "
                f"WHERE run_id = ? AND status IN ({', '.join('?' for _ in item_from_statuses)})",
                (
                    item_to_status.value,
                    closed_at.isoformat(),
                    run_id,
                    *(status.value for status in item_from_statuses),
                ),
            )
            conn.execute(
                "UPDATE simple_runs SET status = ?, finished_at = ?, last_heartbeat_at = ? WHERE run_id = ?",
                (simple_status.value, closed_at.isoformat(), heartbeat_at.isoformat(), run_id),
            )
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                (run_status.value, closed_at.isoformat(), run_id),
            )
            return {
                "run_id": run_id,
                "items_updated": int(item_cur.rowcount),
                "simple_status": simple_status.value,
                "run_status": run_status.value,
            }

        return self._execute_transaction("close_simple_run_state", _action)

    def save_simple_attempt(self, run_id: str, attempt: SimpleAttempt) -> None:
        self._execute_write(
            "INSERT OR REPLACE INTO simple_attempts "
            "(run_id, item_id, attempt, status, worker_id, started_at, finished_at, exit_code, "
            "error_category, failure_reason, changed_files, validation_report, output, error, "
            "cost_usd, model_used, provider_used, pid, token_input, token_output, cli_duration_ms, tool_uses, "
            "turn_started, turn_completed, max_turns_exceeded) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                attempt.item_id,
                attempt.attempt,
                attempt.status.value,
                attempt.worker_id,
                attempt.started_at.isoformat() if attempt.started_at else None,
                attempt.finished_at.isoformat() if attempt.finished_at else None,
                attempt.exit_code,
                attempt.error_category,
                attempt.failure_reason,
                json.dumps(attempt.changed_files, ensure_ascii=False),
                json.dumps(attempt.validation_report.to_dict(), ensure_ascii=False) if attempt.validation_report else None,
                attempt.output,
                attempt.error,
                attempt.cost_usd,
                attempt.model_used,
                attempt.provider_used,
                attempt.pid,
                attempt.token_input,
                attempt.token_output,
                attempt.cli_duration_ms,
                attempt.tool_uses,
                attempt.turn_started,
                attempt.turn_completed,
                1 if attempt.max_turns_exceeded else 0,
            ),
            operation="save_simple_attempt",
        )

    def get_simple_attempts(self, run_id: str, item_id: str | None = None) -> list[SimpleAttempt]:
        sql = (
            "SELECT item_id, attempt, status, worker_id, started_at, finished_at, exit_code, "
            "error_category, failure_reason, changed_files, validation_report, output, error, "
            "cost_usd, model_used, provider_used, pid, token_input, token_output, cli_duration_ms, tool_uses, "
            "turn_started, turn_completed, max_turns_exceeded "
            "FROM simple_attempts WHERE run_id = ?"
        )
        params: list[object] = [run_id]
        if item_id:
            sql += " AND item_id = ?"
            params.append(item_id)
        sql += " ORDER BY item_id, attempt"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        attempts: list[SimpleAttempt] = []
        for row in rows:
            validation_report = None
            if row[10]:
                vr = json.loads(row[10])
                validation_report = ValidationReport(
                    passed=vr.get("passed", False),
                    stage_results=[
                        ValidationStageResult(
                            name=str(stage.get("name", "")),
                            passed=bool(stage.get("passed", False)),
                            details=str(stage.get("details", "")),
                        )
                        for stage in vr.get("stage_results", [])
                    ],
                    changed_files=list(vr.get("changed_files", [])),
                    target_touched=vr.get("target_touched", False),
                    target_exists_after=vr.get("target_exists_after"),
                    target_content_changed=vr.get("target_content_changed", False),
                    unauthorized_changes=list(vr.get("unauthorized_changes", [])),
                    syntax_ok=vr.get("syntax_ok"),
                    pattern_matches=dict(vr.get("pattern_matches", {})),
                    command_results=list(vr.get("command_results", [])),
                    rollback_performed=vr.get("rollback_performed", False),
                    recovered_unauthorized_changes=list(vr.get("recovered_unauthorized_changes", [])),
                    failure_code=vr.get("failure_code", ""),
                    failure_reason=vr.get("failure_reason", ""),
                    warnings=list(vr.get("warnings", [])),
                    target_changed_files=list(vr.get("target_changed_files", [])),
                )
            attempts.append(SimpleAttempt(
                item_id=row[0],
                attempt=row[1],
                status=SimpleItemStatus(row[2]),
                worker_id=row[3],
                started_at=datetime.fromisoformat(row[4]) if row[4] else None,
                finished_at=datetime.fromisoformat(row[5]) if row[5] else None,
                exit_code=row[6],
                error_category=row[7],
                failure_reason=row[8],
                changed_files=json.loads(row[9] or "[]"),
                validation_report=validation_report,
                output=row[11],
                error=row[12],
                cost_usd=row[13],
                model_used=row[14],
                provider_used=row[15],
                pid=row[16],
                token_input=row[17],
                token_output=row[18],
                cli_duration_ms=row[19],
                tool_uses=row[20],
                turn_started=row[21],
                turn_completed=row[22],
                max_turns_exceeded=bool(row[23]),
            ))
        return attempts

    def save_simple_event(
        self,
        run_id: str,
        event_type: str,
        data: dict | list | str,
        *,
        item_id: str = "",
        bucket: str = "",
        level: str = "info",
    ) -> None:
        payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
        self._execute_write(
            "INSERT INTO simple_events (run_id, item_id, bucket, event_type, level, data, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, item_id, bucket, event_type, level, payload, datetime.now().isoformat()),
            operation="save_simple_event",
        )

    def save_failover_event(
        self,
        *,
        execution_id: str,
        execution_kind: str,
        scope: str,
        from_profile: str,
        to_profile: str,
        reason: str,
        trigger_task_id: str = "",
        metadata: dict | None = None,
        created_at: datetime | None = None,
    ) -> None:
        event_time = created_at or datetime.now()
        self._execute_write(
            "INSERT INTO failover_events "
            "(execution_id, execution_kind, scope, from_profile, to_profile, reason, trigger_task_id, metadata, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                execution_id,
                execution_kind,
                scope,
                from_profile,
                to_profile,
                reason,
                trigger_task_id,
                json.dumps(metadata or {}, ensure_ascii=False),
                event_time.isoformat(),
            ),
            operation="save_failover_event",
        )

    def get_failover_events(self, execution_id: str, limit: int | None = None) -> list[dict]:
        sql = (
            "SELECT event_id, execution_id, execution_kind, scope, from_profile, to_profile, "
            "reason, trigger_task_id, metadata, created_at "
            "FROM failover_events WHERE execution_id = ? ORDER BY event_id DESC"
        )
        params: list[object] = [execution_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "event_id": row[0],
                "execution_id": row[1],
                "execution_kind": row[2],
                "scope": row[3],
                "from_profile": row[4],
                "to_profile": row[5],
                "reason": row[6],
                "trigger_task_id": row[7],
                "metadata": json.loads(row[8] or "{}"),
                "created_at": row[9],
            }
            for row in rows
        ]

    def get_simple_events(self, run_id: str, limit: int | None = None) -> list[dict]:
        sql = (
            "SELECT event_id, item_id, bucket, event_type, level, data, ts "
            "FROM simple_events WHERE run_id = ? ORDER BY event_id DESC"
        )
        params: list[object] = [run_id]
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
            if limit and rows and not any(row[3] == "run_started" for row in rows):
                anchor = self._conn.execute(
                    "SELECT event_id, item_id, bucket, event_type, level, data, ts "
                    "FROM simple_events WHERE run_id = ? AND event_type = 'run_started' "
                    "ORDER BY event_id ASC LIMIT 1",
                    (run_id,),
                ).fetchone()
                if anchor is not None:
                    rows = rows[:-1] + [anchor]
        return [_simple_event_from_row(row) for row in rows]

    def get_latest_simple_event(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT event_id, item_id, bucket, event_type, level, data, ts "
                "FROM simple_events WHERE run_id = ? ORDER BY event_id DESC LIMIT 1",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return _simple_event_from_row(row)

    def save_simple_manifest(self, run_id: str, manifest: SimpleManifest) -> None:
        payload = json.dumps(manifest.to_dict(), ensure_ascii=False)
        self._execute_write(
            "INSERT OR REPLACE INTO simple_manifests (run_id, data, updated_at) VALUES (?, ?, ?)",
            (run_id, payload, datetime.now().isoformat()),
            operation="save_simple_manifest",
        )

    def get_simple_manifest(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM simple_manifests WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def save_simple_bucket(self, run_id: str, stats: BucketStats) -> None:
        self._execute_write(
            "INSERT OR REPLACE INTO simple_buckets "
            "(run_id, bucket, total_items, completed_items, failed_items, running_items, retries, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                stats.name,
                stats.total_items,
                stats.completed_items,
                stats.failed_items,
                stats.running_items,
                stats.retries,
                datetime.now().isoformat(),
            ),
            operation="save_simple_bucket",
        )

    def get_simple_buckets(self, run_id: str) -> dict[str, BucketStats]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT bucket, total_items, completed_items, failed_items, running_items, retries "
                "FROM simple_buckets WHERE run_id = ? ORDER BY bucket",
                (run_id,),
            ).fetchall()
        return {
            row[0]: BucketStats(
                name=row[0],
                total_items=row[1],
                completed_items=row[2],
                failed_items=row[3],
                running_items=row[4],
                retries=row[5],
            )
            for row in rows
        }

    def save_simple_artifact(
        self,
        run_id: str,
        item_id: str,
        artifact_type: str,
        path: str,
        metadata: dict | None = None,
    ) -> None:
        self._execute_write(
            "INSERT OR REPLACE INTO simple_artifacts "
            "(run_id, item_id, artifact_type, path, metadata, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (
                run_id,
                item_id,
                artifact_type,
                path,
                json.dumps(metadata or {}, ensure_ascii=False),
                datetime.now().isoformat(),
            ),
            operation="save_simple_artifact",
        )

    def get_simple_artifacts(self, run_id: str, item_id: str | None = None) -> list[dict]:
        sql = (
            "SELECT item_id, artifact_type, path, metadata, updated_at FROM simple_artifacts WHERE run_id = ?"
        )
        params: list[object] = [run_id]
        if item_id:
            sql += " AND item_id = ?"
            params.append(item_id)
        sql += " ORDER BY item_id, artifact_type, path"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "item_id": row[0],
                "artifact_type": row[1],
                "path": row[2],
                "metadata": json.loads(row[3] or "{}"),
                "updated_at": row[4],
            }
            for row in rows
        ]

    # ── Context data operations ──

    def set_context(self, run_id: str, key: str, value: str) -> None:
        """设置运行时上下文数据。

        Args:
            run_id: 运行 ID
            key: 上下文键
            value: 上下文值（JSON 字符串）

        Raises:
            CheckpointError: 数据库操作失败时抛出
        """
        self._execute_write(
            "INSERT OR REPLACE INTO context_data (run_id, key, value, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (run_id, key, value, datetime.now().isoformat()),
            operation="set_context",
        )

    def get_context(self, run_id: str, key: str) -> str | None:
        """获取单个上下文数据。

        Args:
            run_id: 运行 ID
            key: 上下文键

        Returns:
            上下文值（JSON 字符串），如果不存在则返回 None
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM context_data WHERE run_id = ? AND key = ?",
                (run_id, key),
            ).fetchone()
        return row[0] if row else None

    def get_all_context(self, run_id: str) -> dict[str, str]:
        """获取某个 run_id 的所有上下文数据。

        Args:
            run_id: 运行 ID

        Returns:
            字典，键为上下文键，值为上下文值（JSON 字符串）
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM context_data WHERE run_id = ?",
                (run_id,),
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    # ── Stream events operations ──

    def save_stream_event(
        self,
        run_id: str,
        task_id: str,
        seq: int,
        event_type: str,
        data: dict | str
    ) -> None:
        """保存流式输出事件。

        Args:
            run_id: 运行 ID
            task_id: 任务 ID
            seq: 事件序号（从 0 开始递增）
            event_type: 事件类型（如 "chunk", "done", "error"）
            data: 事件数据（dict 会自动序列化为 JSON）

        Raises:
            CheckpointError: 数据库操作失败时抛出
        """
        if isinstance(data, dict):
            try:
                event_data = json.dumps(data)
            except (TypeError, ValueError) as e:
                raise CheckpointError(f"Failed to serialize event data: {e}") from e
        else:
            event_data = data
        self._execute_write(
            "INSERT OR REPLACE INTO stream_events "
            "(run_id, task_id, seq, event_type, event_data, ts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, task_id, seq, event_type, event_data, datetime.now().isoformat()),
            operation="save_stream_event",
        )

    def get_stream_events(self, run_id: str, task_id: str) -> list[dict]:
        """获取某个任务的所有流式事件，按序号排序。

        Args:
            run_id: 运行 ID
            task_id: 任务 ID

        Returns:
            事件列表，每个事件包含 seq, event_type, event_data, ts 字段
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT seq, event_type, event_data, ts FROM stream_events "
                "WHERE run_id = ? AND task_id = ? ORDER BY seq",
                (run_id, task_id),
            ).fetchall()
        result = []
        for row in rows:
            event_data_str = row[2]
            if event_data_str.startswith('{') or event_data_str.startswith('['):
                try:
                    event_data = json.loads(event_data_str)
                except json.JSONDecodeError as e:
                    raise CheckpointError(f"Corrupted JSON in stream event (seq={row[0]}): {e}") from e
            else:
                event_data = event_data_str
            
            result.append({
                "seq": row[0],
                "event_type": row[1],
                "event_data": event_data,
                "ts": row[3],
            })
        return result

    # ── Loop state operations ──

    def save_loop_state(self, run_id: str, task_id: str, iteration: int, output: str) -> None:
        """保存循环任务的迭代状态。

        Args:
            run_id: 运行 ID
            task_id: 任务 ID
            iteration: 当前迭代次数
            output: 最后一次迭代的输出

        Raises:
            CheckpointError: 数据库操作失败时抛出
        """
        self._execute_write(
            "INSERT OR REPLACE INTO loop_state (run_id, task_id, iteration, last_output) "
            "VALUES (?, ?, ?, ?)",
            (run_id, task_id, iteration, output),
            operation="save_loop_state",
        )

    def get_loop_state(self, run_id: str, task_id: str) -> tuple[int, str] | None:
        """获取循环任务的迭代状态。

        Args:
            run_id: 运行 ID
            task_id: 任务 ID

        Returns:
            (iteration, last_output) 元组，如果不存在则返回 None
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT iteration, last_output FROM loop_state WHERE run_id = ? AND task_id = ?",
                (run_id, task_id),
            ).fetchone()
        return (row[0], row[1]) if row else None

    def get_all_loop_counts(self, run_id: str) -> dict[str, int]:
        """获取某个运行的所有循环计数。

        Args:
            run_id: 运行 ID

        Returns:
            {task_id: iteration} 字典
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT task_id, iteration FROM loop_state WHERE run_id = ?",
                (run_id,),
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    # ── Handoff data operations ──

    def save_handoff(self, phase_id: str, data_json: str) -> None:
        """持久化迭代间 Handoff 包。

        Args:
            phase_id: 阶段 ID
            data_json: Handoff 数据的 JSON 字符串
        """
        self._execute_write(
            "INSERT OR REPLACE INTO handoff_data (phase_id, data, updated_at) "
            "VALUES (?, ?, ?)",
            (phase_id, data_json, datetime.now().isoformat()),
            operation="save_handoff",
        )

    def get_handoff(self, phase_id: str) -> str | None:
        """获取单个阶段的 Handoff 数据。

        Args:
            phase_id: 阶段 ID

        Returns:
            Handoff JSON 字符串，不存在则返回 None
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM handoff_data WHERE phase_id = ?",
                (phase_id,),
            ).fetchone()
        return row[0] if row else None

    def get_all_handoffs(self) -> dict[str, str]:
        """获取所有阶段的 Handoff 数据。

        Returns:
            {phase_id: data_json} 字典
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT phase_id, data FROM handoff_data"
            ).fetchall()
        return {row[0]: row[1] for row in rows}

    # ── Learning memory operations ──

    def save_learning(
        self, pattern_hash: str, pattern: str, resolution: str, success: bool,
    ) -> None:
        """保存或更新学习记忆条目。

        Args:
            pattern_hash: 错误模式的哈希值
            pattern: 错误模式文本
            resolution: 解决方案文本
            success: 本次是否成功
        """
        try:
            with self._lock:
                existing = self._conn.execute(
                    "SELECT success_count, fail_count FROM learning_memory WHERE pattern_hash = ?",
                    (pattern_hash,),
                ).fetchone()
                if existing:
                    sc, fc = existing
                    if success:
                        sc += 1
                    else:
                        fc += 1
                    self._conn.execute(
                        "UPDATE learning_memory SET resolution=?, success_count=?, "
                        "fail_count=?, last_seen=? WHERE pattern_hash=?",
                        (resolution, sc, fc, datetime.now().isoformat(), pattern_hash),
                    )
                else:
                    self._conn.execute(
                        "INSERT INTO learning_memory "
                        "(pattern_hash, pattern, resolution, success_count, fail_count, last_seen) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            pattern_hash, pattern, resolution,
                            1 if success else 0,
                            0 if success else 1,
                            datetime.now().isoformat(),
                        ),
                    )
                self._conn.commit()
                self._maybe_checkpoint()
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "disk" in err_msg or "full" in err_msg or "no space" in err_msg:
                raise CheckpointError(
                    f"磁盘空间不足，save_learning失败: {e}. "
                    "请清理磁盘空间后重试。"
                ) from e
            if "database is locked" in err_msg:
                raise CheckpointError(
                    f"数据库锁定，save_learning失败: {e}. "
                    "可能有其他进程正在访问数据库。"
                ) from e
            raise CheckpointError(f"save_learning失败: {e}") from e
        except sqlite3.Error as e:
            raise CheckpointError(f"save_learning失败: {e}") from e

    def get_learning(self, pattern_hash: str) -> dict | None:
        """获取单条学习记忆。

        Returns:
            包含 pattern, resolution, success_count, fail_count 的字典，不存在返回 None
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT pattern, resolution, success_count, fail_count, last_seen "
                "FROM learning_memory WHERE pattern_hash = ?",
                (pattern_hash,),
            ).fetchone()
        if not row:
            return None
        return {
            "pattern": row[0],
            "resolution": row[1],
            "success_count": row[2],
            "fail_count": row[3],
            "last_seen": row[4],
        }

    def get_best_resolution(self, pattern_hash: str) -> str | None:
        """获取成功率最高的解决方案。

        只返回成功次数 > 失败次数的方案。

        Returns:
            解决方案文本，不存在或成功率不够则返回 None
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT resolution, success_count, fail_count FROM learning_memory "
                "WHERE pattern_hash = ? AND success_count > fail_count",
                (pattern_hash,),
            ).fetchone()
        return row[0] if row else None

    def cleanup_old_data(self, max_age_days: int = 7, max_rows_per_table: int = 50000) -> dict[str, int]:
        """清理过期的历史数据，防止数据库无限增长。

        Args:
            max_age_days: 保留最近 N 天的数据
            max_rows_per_table: 每张表最大行数（超过时删除最旧的）

        Returns:
            {表名: 删除行数} 字典
        """
        cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()
        cleaned: dict[str, int] = {}

        # 清理 task_attempts（最大的表）
        try:
            with self._lock:
                # 按时间清理
                cur = self._conn.execute(
                    "DELETE FROM task_attempts WHERE finished_at IS NOT NULL AND finished_at < ?",
                    (cutoff,)
                )
                cleaned["task_attempts"] = cur.rowcount

                # 按行数限制清理（保留最新的 max_rows_per_table 行）
                cur2 = self._conn.execute(
                    "DELETE FROM task_attempts WHERE rowid NOT IN "
                    "(SELECT rowid FROM task_attempts ORDER BY rowid DESC LIMIT ?)",
                    (max_rows_per_table,)
                )
                cleaned["task_attempts"] += cur2.rowcount
                self._conn.commit()
        except Exception as e:
            _logger.warning("清理 task_attempts 失败: %s", e)

        # 清理 stream_events
        try:
            with self._lock:
                cur = self._conn.execute(
                    "DELETE FROM stream_events WHERE ts < ?",
                    (cutoff,)
                )
                cleaned["stream_events"] = cur.rowcount

                cur2 = self._conn.execute(
                    "DELETE FROM stream_events WHERE rowid NOT IN "
                    "(SELECT rowid FROM stream_events ORDER BY rowid DESC LIMIT ?)",
                    (max_rows_per_table,)
                )
                cleaned["stream_events"] += cur2.rowcount
                self._conn.commit()
        except Exception as e:
            _logger.warning("清理 stream_events 失败: %s", e)

        # 清理已完成的旧 runs（保留最近 max_age_days 天的）
        try:
            with self._lock:
                cur = self._conn.execute(
                    "DELETE FROM runs WHERE status IN ('completed', 'failed', 'cancelled') "
                    "AND finished_at IS NOT NULL AND finished_at < ?",
                    (cutoff,)
                )
                cleaned["runs"] = cur.rowcount
                self._conn.commit()
        except Exception as e:
            _logger.warning("清理 runs 失败: %s", e)

        # 清理孤立的 task_results（对应的 run 已被删除）
        try:
            with self._lock:
                cur = self._conn.execute(
                    "DELETE FROM task_results WHERE run_id NOT IN (SELECT run_id FROM runs)"
                )
                cleaned["task_results_orphaned"] = cur.rowcount
                self._conn.commit()
        except Exception as e:
            _logger.warning("清理孤立 task_results 失败: %s", e)

        # 执行 VACUUM 回收空间（仅在删除了大量数据时）
        # VACUUM 需要排他锁，在 self._lock 内执行防止并发写入冲突
        total_cleaned = sum(cleaned.values())
        if total_cleaned > 1000:
            try:
                with self._lock:
                    self._conn.execute("VACUUM")
                _logger.info("VACUUM 完成，回收磁盘空间")
            except Exception as e:
                _logger.warning("VACUUM 失败（可能有并发访问）: %s", e)

        if total_cleaned > 0:
            _logger.info("历史数据清理完成: %s", cleaned)
        return cleaned
