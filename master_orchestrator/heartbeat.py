"""子进程心跳写入器 — 定期更新文件 mtime 供 Guardian 检测存活。"""

import logging
import os
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_DIR = Path.home() / ".claude" / "guardian"

ENV_HEARTBEAT_FILE = "ORCH_HEARTBEAT_FILE"


class Heartbeat:
    """周期性写入心跳文件，供 Guardian 检测子进程是否卡死。"""

    def __init__(self, heartbeat_dir: Path | str | None = None) -> None:
        self._dir = Path(heartbeat_dir) if heartbeat_dir else _DEFAULT_DIR
        self._file = self._dir / "child_heartbeat"
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # -- 公开 API ----------------------------------------------------------

    def touch(self) -> None:
        """立即更新心跳文件 mtime。"""
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            self._file.write_text(str(time.time()))
        except OSError as exc:
            logger.warning("心跳写入失败: %s", exc)

    def start_background(self, interval: float = 30) -> None:
        """启动后台 daemon 线程，每 *interval* 秒写一次心跳。"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, args=(interval,), daemon=True, name="heartbeat"
        )
        self._thread.start()
        logger.debug("心跳后台线程已启动 (间隔 %ss)", interval)

    def stop(self) -> None:
        """停止后台线程。"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        # 清理心跳文件
        try:
            self._file.unlink(missing_ok=True)
        except OSError:
            pass

    # -- 内部 --------------------------------------------------------------

    def _loop(self, interval: float) -> None:
        while not self._stop_event.is_set():
            self.touch()
            self._stop_event.wait(interval)
