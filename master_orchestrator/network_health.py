"""网络健康探针与分级等待策略。

从 claude_cli.py 提取的网络可达性检测和恢复等待逻辑。
"""
from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger(__name__)


# 网络健康探针配置
_NETWORK_PROBE_TIMEOUT = 5  # 单次探测超时秒数
_NETWORK_PROBE_MAX_WAIT = 60  # 最长等待网络恢复秒数（1 分钟，避免长时间阻塞重试）
_NETWORK_PROBE_INTERVAL = 30  # 探测间隔秒数（默认值，实际由分级策略覆盖）
_NETWORK_PROBE_ENDPOINTS = [
    "https://api.anthropic.com",
    "https://1.1.1.1",
]

# 分级等待策略阈值（秒）
_PROBE_TIER1_LIMIT = 300   # 前 5 分钟：每 10 秒探测
_PROBE_TIER1_INTERVAL = 10
_PROBE_TIER2_LIMIT = 1800  # 5-30 分钟：每 30 秒探测
_PROBE_TIER2_INTERVAL = 30
_PROBE_TIER3_INTERVAL = 60  # 30-60 分钟：每 60 秒探测

# 通知触发阈值（秒）
_PROBE_NOTIFY_WARNING = 120   # 等待超过 2 分钟发送 WARNING
_PROBE_NOTIFY_CRITICAL = 600  # 等待超过 10 分钟发送 CRITICAL


def _probe_single_endpoint(endpoint: str) -> tuple[bool, float]:
    """探测单个端点的可达性和延迟。

    自动检测本地代理（环境变量 > 常见本地代理端口）。

    Args:
        endpoint: 要探测的 URL。

    Returns:
        (reachable, latency_ms) — reachable 为 True 表示可达，
        latency_ms 为往返延迟毫秒数（不可达时为 -1）。
    """
    import urllib.request
    import urllib.error

    # 构建代理配置：优先环境变量，其次尝试常见本地代理端口
    proxy_url = (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
    )
    if not proxy_url:
        # 尝试常见本地代理端口
        import socket
        for port in (7897, 7890, 1080, 8080):
            try:
                s = socket.create_connection(("127.0.0.1", port), timeout=1)
                s.close()
                proxy_url = f"http://127.0.0.1:{port}"
                break
            except OSError:
                continue

    start = time.monotonic()
    try:
        if proxy_url:
            proxy_handler = urllib.request.ProxyHandler({
                "http": proxy_url,
                "https": proxy_url,
            })
        else:
            proxy_handler = urllib.request.ProxyHandler()
        opener = urllib.request.build_opener(proxy_handler)
        req = urllib.request.Request(endpoint, method="HEAD")
        opener.open(req, timeout=_NETWORK_PROBE_TIMEOUT)
        latency_ms = (time.monotonic() - start) * 1000
        return True, latency_ms
    except Exception:
        return False, -1.0


def _wait_for_network(task_id: str = "") -> bool:
    """等待网络恢复。

    在重试前调用，避免在网络不通时浪费重试次数。
    先快速探测一次，如果通则立即返回。
    如果不通，使用分级等待策略循环探测直到恢复或超时：
      - 前 5 分钟：每 10 秒探测一次（快速恢复场景）
      - 5-30 分钟：每 30 秒探测一次（中等故障）
      - 30-60 分钟：每 60 秒探测一次（长时间宕机）

    在关键时间点通过 NotificationManager 发送告警：
      - 等待超过 2 分钟：WARNING
      - 等待超过 10 分钟：CRITICAL
      - 网络恢复时：恢复通知

    Returns:
        True 表示网络可用，False 表示等待超时仍不可用。
    """
    from .notification import get_notifier

    def _probe_once() -> bool:
        """探测所有端点，任一可达即返回 True。"""
        for endpoint in _NETWORK_PROBE_ENDPOINTS:
            reachable, _ = _probe_single_endpoint(endpoint)
            if reachable:
                return True
        return False

    def _get_interval(elapsed: float) -> float:
        """根据已等待时间返回当前探测间隔（分级策略）。"""
        if elapsed < _PROBE_TIER1_LIMIT:
            return _PROBE_TIER1_INTERVAL
        elif elapsed < _PROBE_TIER2_LIMIT:
            return _PROBE_TIER2_INTERVAL
        else:
            return _PROBE_TIER3_INTERVAL

    # 快速探测：网络正常则立即返回
    if _probe_once():
        return True

    # 网络不通，进入等待循环
    notifier = get_notifier()
    logger.warning(
        "[%s] 网络不可达，进入分级等待 (最长 %ds)...", task_id, _NETWORK_PROBE_MAX_WAIT
    )

    start_time = time.monotonic()
    warning_sent = False   # 是否已发送 WARNING 通知
    critical_sent = False  # 是否已发送 CRITICAL 通知
    alert_title = "网络中断"

    while True:
        waited = time.monotonic() - start_time
        if waited >= _NETWORK_PROBE_MAX_WAIT:
            break

        interval = _get_interval(waited)
        # 确保不会 sleep 超过剩余等待时间
        remaining = _NETWORK_PROBE_MAX_WAIT - waited
        time.sleep(min(interval, remaining))
        waited = time.monotonic() - start_time

        # 发送分级通知（仅在首次越过阈值时触发）
        if not warning_sent and waited >= _PROBE_NOTIFY_WARNING:
            logger.warning("[%s] 网络中断已超过 %d 秒，发送 WARNING 通知", task_id, int(waited))
            notifier.warning(
                alert_title,
                task_id=task_id,
                waited_seconds=int(waited),
                message=f"网络已中断超过 {int(waited)} 秒，正在等待恢复",
            )
            warning_sent = True

        if not critical_sent and waited >= _PROBE_NOTIFY_CRITICAL:
            logger.error("[%s] 网络中断已超过 %d 秒，发送 CRITICAL 通知", task_id, int(waited))
            notifier.critical(
                alert_title,
                task_id=task_id,
                waited_seconds=int(waited),
                message=f"网络已中断超过 {int(waited)} 秒，可能需要人工介入",
            )
            critical_sent = True

        # 探测网络
        if _probe_once():
            logger.info("[%s] 网络已恢复 (等待了 %ds)", task_id, int(waited))
            # 如果之前发过告警，发送恢复通知
            if warning_sent or critical_sent:
                notifier.resolve(alert_title)
            return True

        # 记录当前等待状态（使用当前层级对应的日志级别）
        if waited >= _PROBE_NOTIFY_CRITICAL:
            logger.error("[%s] 网络仍不可达，已等待 %ds / %ds", task_id, int(waited), _NETWORK_PROBE_MAX_WAIT)
        elif waited >= _PROBE_NOTIFY_WARNING:
            logger.warning("[%s] 网络仍不可达，已等待 %ds / %ds", task_id, int(waited), _NETWORK_PROBE_MAX_WAIT)
        else:
            logger.debug("[%s] 网络仍不可达，已等待 %ds / %ds", task_id, int(waited), _NETWORK_PROBE_MAX_WAIT)

    # 超时，发送最终通知
    logger.error("[%s] 网络等待超时 (%ds)，放弃", task_id, _NETWORK_PROBE_MAX_WAIT)
    if not critical_sent:
        notifier.critical(
            alert_title,
            task_id=task_id,
            waited_seconds=int(waited),
            message=f"网络等待超时 ({_NETWORK_PROBE_MAX_WAIT}s)，任务将失败",
        )
    return False
