"""动态日志级别 — 后台线程监控文件变化，实时调整日志级别。"""

import logging
import os
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_WATCH_FILE = Path.home() / ".claude" / "log_level"

_VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


class LogLevelWatcher:
    """监控 ``~/.claude/log_level`` 文件，检测到变化时动态调整日志级别。"""

    def __init__(
        self,
        watch_file: Path | str | None = None,
        check_interval: float = 10,
    ) -> None:
        self._file = Path(watch_file) if watch_file else _DEFAULT_WATCH_FILE
        self._interval = check_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_mtime: float = 0.0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="log-level-watcher"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._check()
            except Exception as exc:
                logger.debug("日志级别检查异常: %s", exc)
            self._stop_event.wait(self._interval)

    def _check(self) -> None:
        if not self._file.exists():
            return
        mtime = os.path.getmtime(self._file)
        if mtime == self._last_mtime:
            return
        self._last_mtime = mtime
        content = self._file.read_text().strip().upper()
        if content not in _VALID_LEVELS:
            logger.warning("无效的日志级别: %r (有效值: %s)", content, _VALID_LEVELS)
            return
        target_logger = logging.getLogger("claude_orchestrator")
        target_logger.setLevel(getattr(logging, content))
        logger.info("日志级别已动态调整为: %s", content)
