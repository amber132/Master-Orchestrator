"""Webhook notification module for critical events during unattended execution.

Supports sending alerts to Webhook endpoints (Slack, Discord, 飞书, 钉钉, etc.)
when critical events occur: budget exhaustion, deterioration, auth failure, etc.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """告警级别。"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class NotificationConfig:
    """通知配置。"""
    # Webhook URL（留空则禁用通知）
    webhook_url: str = ""
    # 最低告警级别（低于此级别的不发送）
    min_level: str = "warning"
    # 请求超时（秒）
    timeout: int = 10
    # 连续发送失败后静默的次数（防止告警风暴）
    max_consecutive_failures: int = 3
    # 自定义 HTTP headers（如 Authorization）
    headers: dict[str, str] = field(default_factory=dict)
    # 心跳汇报间隔（秒），0 表示禁用
    heartbeat_interval: int = 600  # 默认 10 分钟
    # 本地降级日志路径（Webhook 失败时写入此文件）
    fallback_log_path: str = ""


class NotificationManager:
    """管理告警通知的发送。

    线程安全，可在多线程环境中使用。

    Usage:
        notifier = NotificationManager(config)
        notifier.critical("预算耗尽", budget_spent=50.0, budget_max=50.0)
        notifier.warning("恶化检测触发", score_delta=-0.3)
    """

    def __init__(self, config: NotificationConfig | None = None, dedup_window: float | None = None):
        self._config = config or NotificationConfig()
        self._lock = threading.Lock()
        self._consecutive_failures = 0
        self._total_sent = 0
        self._total_failed = 0
        self._silenced = False
        # 告警去重和恢复通知
        self._recent_alerts: dict[str, float] = {}  # title -> last_sent_time
        self._active_alerts: set[str] = set()
        self._dedup_window: float = dedup_window if dedup_window is not None else 300
        # 心跳线程控制
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        # 本地文件降级写入锁
        self._file_lock = threading.Lock()
        # 创建时间戳，用于计算运行时长
        self._start_time = time.monotonic()
        # 限制并发发送线程数（防止告警风暴时创建过多线程）
        self._send_semaphore = threading.Semaphore(5)

    @property
    def enabled(self) -> bool:
        return bool(self._config.webhook_url)

    def _should_dedup_unlocked(self, title: str) -> bool:
        """检查是否应该去重（假设调用者已持有锁）。

        同时清理过期条目，防止 _recent_alerts 无限增长。
        """
        now = time.time()
        # 定期清理过期条目（每次检查时顺便清理，O(n) 但 n 通常很小）
        if len(self._recent_alerts) > 200:
            expired = [k for k, t in self._recent_alerts.items() if (now - t) >= self._dedup_window]
            for k in expired:
                del self._recent_alerts[k]
                self._active_alerts.discard(k)

        last_sent = self._recent_alerts.get(title)
        if last_sent and (now - last_sent) < self._dedup_window:
            return True
        return False

    def info(self, title: str, **details: Any) -> None:
        # INFO 不去重
        self._send(AlertLevel.INFO, title, details)

    def warning(self, title: str, **details: Any) -> None:
        with self._lock:
            if self._should_dedup_unlocked(title):
                return
            self._recent_alerts[title] = time.time()
            self._active_alerts.add(title)
        self._send(AlertLevel.WARNING, title, details)

    def critical(self, title: str, **details: Any) -> None:
        with self._lock:
            if self._should_dedup_unlocked(title):
                return
            self._recent_alerts[title] = time.time()
            self._active_alerts.add(title)
        self._send(AlertLevel.CRITICAL, title, details)

    def resolve(self, title: str) -> None:
        """标记告警已恢复，发送恢复通知。"""
        with self._lock:
            if title not in self._active_alerts:
                return
            self._active_alerts.discard(title)
            self._recent_alerts.pop(title, None)
        self.info(f"{title} - 已恢复", message="告警已自动恢复")

    def _should_send(self, level: AlertLevel) -> bool:
        """判断是否应该发送此级别的告警。"""
        if not self.enabled:
            return False

        level_order = {AlertLevel.INFO: 0, AlertLevel.WARNING: 1, AlertLevel.CRITICAL: 2}
        min_level = AlertLevel(self._config.min_level) if self._config.min_level in [e.value for e in AlertLevel] else AlertLevel.WARNING
        if level_order.get(level, 0) < level_order.get(min_level, 1):
            return False

        with self._lock:
            if self._silenced and level != AlertLevel.CRITICAL:
                # CRITICAL 级别永远不静默
                return False
            return True

    def _send(self, level: AlertLevel, title: str, details: dict[str, Any]) -> None:
        """异步发送告警（不阻塞调用方）。"""
        if not self._should_send(level):
            return

        # 检查是否有可用的发送槽位，避免创建过多线程
        if not self._send_semaphore.acquire(blocking=False):
            logger.warning("告警发送并发已满，丢弃: %s", title)
            return

        payload = self._build_payload(level, title, details)
        # 在后台线程发送，避免阻塞主流程
        t = threading.Thread(
            target=self._do_send_with_semaphore,
            args=(payload,),
            daemon=True,
            name=f"notify-{level.value}",
        )
        t.start()

    def _do_send_with_semaphore(self, payload: dict) -> None:
        """包装 _do_send，确保 semaphore 释放。"""
        try:
            self._do_send(payload)
        finally:
            self._send_semaphore.release()

    def _build_payload(self, level: AlertLevel, title: str, details: dict[str, Any]) -> dict:
        """构建 Webhook payload（兼容 Slack/飞书/通用格式）。"""
        icon = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(level.value, "📢")
        timestamp = datetime.now().isoformat()

        # 构建详情文本
        detail_lines = [f"  {k}: {v}" for k, v in details.items()] if details else []
        detail_text = "\n".join(detail_lines)

        text = f"{icon} [{level.value.upper()}] {title}\n时间: {timestamp}"
        if detail_text:
            text += f"\n{detail_text}"

        return {
            # Slack 格式
            "text": text,
            # 通用字段（飞书/钉钉/自定义 Webhook 可读取）
            "level": level.value,
            "title": title,
            "timestamp": timestamp,
            "details": details,
        }

    def _do_send(self, payload: dict) -> None:
        """实际发送 HTTP POST 请求，失败时最多重试 2 次（指数退避）。"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                headers = {"Content-Type": "application/json", **self._config.headers}
                req = urllib.request.Request(
                    self._config.webhook_url,
                    data=data,
                    headers=headers,
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=self._config.timeout)

                with self._lock:
                    self._consecutive_failures = 0
                    self._silenced = False
                    self._total_sent += 1

                logger.debug("告警发送成功: %s", payload.get("title", ""))
                return  # 成功，退出

            except Exception as e:
                if attempt < max_retries - 1:
                    # 指数退避重试：1s, 2s
                    backoff = (attempt + 1)
                    logger.debug("告警发送失败 (attempt %d/%d)，%ds 后重试: %s",
                                 attempt + 1, max_retries, backoff, e)
                    time.sleep(backoff)
                    continue

                # 最后一次重试也失败
                with self._lock:
                    self._consecutive_failures += 1
                    self._total_failed += 1
                    if self._consecutive_failures >= self._config.max_consecutive_failures:
                        self._silenced = True
                        logger.warning(
                            "告警发送连续失败 %d 次，暂时静默非 CRITICAL 告警: %s",
                            self._consecutive_failures, e,
                        )
                    else:
                        logger.warning("告警发送失败 (%d/%d): %s",
                                       self._consecutive_failures,
                                       self._config.max_consecutive_failures, e)
                # Webhook 失败时降级写入本地文件
                self._fallback_to_file(payload)

    def get_uptime(self) -> float:
        """返回 NotificationManager 创建以来的运行时长（秒）。"""
        return time.monotonic() - self._start_time

    def start_heartbeat(self) -> None:
        """启动心跳汇报 daemon 线程。

        每 heartbeat_interval 秒发送一次 INFO 级别心跳，
        内容包含运行时长、已发送告警数、连续失败数。
        心跳不受去重限制（每次都发）。
        """
        interval = self._config.heartbeat_interval
        if interval <= 0:
            logger.info("心跳汇报已禁用（heartbeat_interval=%d）", interval)
            return

        # 防止重复启动
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            logger.warning("心跳线程已在运行，跳过重复启动")
            return

        self._heartbeat_stop_event.clear()

        def _heartbeat_loop() -> None:
            """心跳循环，使用 Event.wait 实现可中断的定时等待。"""
            logger.info("心跳线程启动，间隔 %d 秒", interval)
            while not self._heartbeat_stop_event.wait(timeout=interval):
                uptime = self.get_uptime()
                with self._lock:
                    sent = self._total_sent
                    failed = self._total_failed
                    consecutive_failures = self._consecutive_failures

                # 格式化运行时长为可读字符串
                hours, remainder = divmod(int(uptime), 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}h{minutes}m{seconds}s"

                # 直接调用 _send，绕过去重逻辑
                self._send(
                    AlertLevel.INFO,
                    "心跳汇报",
                    {
                        "uptime": uptime_str,
                        "uptime_seconds": round(uptime, 1),
                        "total_sent": sent,
                        "total_failed": failed,
                        "consecutive_failures": consecutive_failures,
                    },
                )
            logger.info("心跳线程已停止")

        self._heartbeat_thread = threading.Thread(
            target=_heartbeat_loop,
            daemon=True,
            name="heartbeat",
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        """停止心跳汇报线程。"""
        self._heartbeat_stop_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None
            logger.info("心跳线程已终止")

    # ── 本地文件降级 ──

    _FALLBACK_MAX_BYTES = 10 * 1024 * 1024  # 10MB 轮转阈值

    def _fallback_to_file(self, payload: dict) -> None:
        """Webhook 失败时将告警写入本地 JSONL 文件。

        文件超过 10MB 时自动轮转（重命名为 .old）。
        """
        fallback_path = self._config.fallback_log_path
        if not fallback_path:
            return

        record = {
            "timestamp": payload.get("timestamp", datetime.now().isoformat()),
            "level": payload.get("level", "unknown"),
            "title": payload.get("title", ""),
            "details": payload.get("details", {}),
        }
        line = json.dumps(record, ensure_ascii=False) + "\n"

        with self._file_lock:
            try:
                p = Path(fallback_path)
                # 确保父目录存在
                p.parent.mkdir(parents=True, exist_ok=True)
                # 文件大小超过阈值时轮转
                if p.exists() and p.stat().st_size >= self._FALLBACK_MAX_BYTES:
                    old_path = p.with_suffix(p.suffix + ".old")
                    # 原子替换：直接 rename 覆盖旧 .old 文件（跨平台安全）
                    try:
                        p.replace(old_path)
                    except OSError as rename_err:
                        logger.warning("降级日志轮转失败: %s", rename_err)
                    else:
                        logger.info("降级日志已轮转: %s -> %s", p, old_path)
                # 追加写入
                with open(p, "a", encoding="utf-8") as f:
                    f.write(line)
                logger.debug("告警已写入降级日志: %s", fallback_path)
            except Exception as e:
                logger.error("写入降级日志失败: %s", e)

    def get_stats(self) -> dict[str, Any]:
        """返回通知统计信息。"""
        with self._lock:
            return {
                "enabled": self.enabled,
                "total_sent": self._total_sent,
                "total_failed": self._total_failed,
                "consecutive_failures": self._consecutive_failures,
                "silenced": self._silenced,
            }


# ── 全局单例 ──

_global_notifier: NotificationManager | None = None
_global_lock = threading.Lock()


def init_notifier(config: NotificationConfig | None = None, dedup_window: float | None = None) -> NotificationManager:
    """初始化全局通知管理器。"""
    global _global_notifier
    with _global_lock:
        _global_notifier = NotificationManager(config, dedup_window=dedup_window)
    return _global_notifier


def get_notifier() -> NotificationManager:
    """获取全局通知管理器（未初始化时返回禁用的实例）。"""
    global _global_notifier
    with _global_lock:
        if _global_notifier is None:
            _global_notifier = NotificationManager()
        return _global_notifier
