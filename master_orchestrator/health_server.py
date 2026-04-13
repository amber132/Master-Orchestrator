"""轻量 HTTP 健康检查端点 — 供外部监控探测编排器状态。

包含:
- /health  — JSON 健康检查
- /metrics — Prometheus 格式指标
- /api/status — 编排器运行状态 JSON
- /dashboard — 自包含实时监控 HTML 页面
"""

import json
import logging
import os
import tempfile
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore[assignment]


# 持久化路径：~/.claude/guardian/dashboard_state.json
_DASHBOARD_STATE_FILE = Path.home() / ".claude" / "guardian" / "dashboard_state.json"

# 全局共享状态，由 AutonomousController 定期更新
_dashboard_state: dict[str, Any] = {}
_dashboard_lock = threading.Lock()

# 持久化写入锁和频率控制（最多每 10 秒写一次文件）
_persist_lock = threading.Lock()
_last_persist_time: float = 0.0
_PERSIST_INTERVAL: float = 10.0


def _persist_dashboard_state_sync() -> None:
    """将内存中的 dashboard 状态原子写入文件（tmp → rename）。"""
    global _last_persist_time
    with _persist_lock:
        now = time.time()
        # 写入频率限制：距上次写入不足 10 秒则跳过
        if now - _last_persist_time < _PERSIST_INTERVAL:
            return
        _last_persist_time = now

        with _dashboard_lock:
            snapshot = dict(_dashboard_state)

        try:
            _DASHBOARD_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            # 原子写入：先写临时文件，再 rename 覆盖目标文件
            fd, tmp_path = tempfile.mkstemp(
                dir=str(_DASHBOARD_STATE_FILE.parent),
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, ensure_ascii=False)
                os.replace(tmp_path, str(_DASHBOARD_STATE_FILE))
            except BaseException:
                # 写入失败时清理临时文件
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as exc:  # noqa: BLE001
            logger.debug("Dashboard 状态持久化失败: %s", exc)


def update_dashboard_state(data: dict[str, Any]) -> None:
    """线程安全地更新 dashboard 数据源，并异步持久化到文件。"""
    with _dashboard_lock:
        _dashboard_state.update(data)
        _dashboard_state["last_updated"] = time.time()

    # 使用 daemon 线程异步写入文件，不阻塞调用方
    t = threading.Thread(
        target=_persist_dashboard_state_sync,
        daemon=True,
        name="dashboard-persist",
    )
    t.start()


def get_dashboard_state() -> dict[str, Any]:
    """线程安全地读取 dashboard 数据快照。"""
    with _dashboard_lock:
        return dict(_dashboard_state)


def load_dashboard_state() -> dict[str, Any]:
    """从持久化文件加载 dashboard 状态到内存。

    文件不存在或内容损坏时返回空 dict，不抛异常。
    """
    global _dashboard_state
    try:
        raw = _DASHBOARD_STATE_FILE.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except OSError as exc:
        logger.warning("读取 Dashboard 持久化文件失败: %s", exc)
        return {}
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        with _dashboard_lock:
            _dashboard_state.update(data)
        logger.info("已从 %s 恢复 Dashboard 状态", _DASHBOARD_STATE_FILE)
        return data
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("Dashboard 持久化文件内容损坏: %s", exc)
        return {}


_DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Claude Orchestrator Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
.header{background:#1e293b;padding:16px 24px;border-bottom:1px solid #334155;display:flex;align-items:center;gap:12px}
.header h1{font-size:18px;font-weight:600}
.dot{width:10px;height:10px;border-radius:50%;background:#22c55e;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;padding:24px}
.card{background:#1e293b;border-radius:12px;padding:20px;border:1px solid #334155}
.card h2{font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:#94a3b8;margin-bottom:12px}
.metric{font-size:28px;font-weight:700;color:#f8fafc}
.metric small{font-size:14px;color:#94a3b8;font-weight:400}
.phase-list{list-style:none}
.phase-list li{padding:8px 0;border-bottom:1px solid #334155;display:flex;justify-content:space-between;align-items:center}
.phase-list li:last-child{border-bottom:none}
.badge{padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600}
.badge-completed{background:#166534;color:#86efac}
.badge-running{background:#854d0e;color:#fde047}
.badge-pending{background:#334155;color:#94a3b8}
.badge-failed{background:#991b1b;color:#fca5a5}
.score-bar{height:6px;background:#334155;border-radius:3px;margin-top:6px;overflow:hidden}
.score-fill{height:100%;border-radius:3px;transition:width .5s}
.trend{display:flex;gap:4px;align-items:flex-end;height:40px;margin-top:8px}
.trend-bar{width:16px;border-radius:2px 2px 0 0;transition:height .3s}
.error-box{background:#1c1917;border:1px solid #44403c;border-radius:8px;padding:12px;margin-top:8px;font-family:monospace;font-size:12px;color:#fbbf24;max-height:120px;overflow-y:auto;white-space:pre-wrap}
.footer{text-align:center;padding:16px;color:#475569;font-size:12px}
</style>
</head>
<body>
<div class="header">
  <div class="dot" id="pulse"></div>
  <h1>Claude Orchestrator</h1>
  <span id="status-text" style="margin-left:auto;font-size:13px;color:#94a3b8">连接中...</span>
</div>
<div class="grid" id="grid">
  <div class="card"><h2>目标</h2><div class="metric" id="goal">-</div></div>
  <div class="card"><h2>运行状态</h2><div class="metric" id="run-status">-</div></div>
  <div class="card"><h2>总花费</h2><div class="metric" id="cost">-</div></div>
  <div class="card"><h2>运行时间</h2><div class="metric" id="uptime">-</div></div>
  <div class="card" style="grid-column:1/-1"><h2>阶段进度</h2><ul class="phase-list" id="phases"></ul></div>
  <div class="card"><h2>当前迭代</h2><div class="metric" id="iteration">-</div></div>
  <div class="card"><h2>分数趋势</h2><div class="trend" id="trend"></div></div>
  <div class="card" style="grid-column:1/-1"><h2>最近错误</h2><div class="error-box" id="errors">无</div></div>
</div>
<div class="footer">自动刷新间隔 3 秒</div>
<script>
function fmt(s){return s<10?'0'+s:''+s}
function duration(sec){
  if(!sec)return'-';
  var h=Math.floor(sec/3600),m=Math.floor(sec%3600/60),s=Math.floor(sec%60);
  return h?h+'h'+fmt(m)+'m':m?m+'m'+fmt(s)+'s':s+'s';
}
function badgeCls(s){
  if(s==='COMPLETED')return'badge-completed';
  if(s==='RUNNING'||s==='IN_PROGRESS')return'badge-running';
  if(s==='FAILED')return'badge-failed';
  return'badge-pending';
}
function scoreColor(v){
  if(v>=0.8)return'#22c55e';if(v>=0.5)return'#eab308';return'#ef4444';
}
function refresh(){
  fetch('/api/status').then(r=>r.json()).then(d=>{
    document.getElementById('status-text').textContent='最后更新: '+new Date(d.last_updated*1000).toLocaleTimeString();
    document.getElementById('goal').textContent=d.goal||'-';
    document.getElementById('run-status').textContent=d.run_status||'-';
    document.getElementById('cost').innerHTML=(d.cost_usd||0).toFixed(4)+'<small> USD</small>';
    document.getElementById('uptime').textContent=duration(d.uptime_seconds);
    document.getElementById('iteration').textContent=''+(d.current_iteration||0);
    // phases
    var pl=document.getElementById('phases');pl.innerHTML='';
    (d.phases||[]).forEach(function(p){
      var li=document.createElement('li');
      var left=document.createElement('span');left.textContent=p.name;
      var right=document.createElement('span');
      right.className='badge '+badgeCls(p.status);right.textContent=p.status;
      li.appendChild(left);li.appendChild(right);
      if(typeof p.score==='number'){
        var bar=document.createElement('div');bar.className='score-bar';bar.style.width='120px';
        var fill=document.createElement('div');fill.className='score-fill';
        fill.style.width=Math.round(p.score*100)+'%';fill.style.background=scoreColor(p.score);
        bar.appendChild(fill);li.appendChild(bar);
      }
      pl.appendChild(li);
    });
    // trend
    var tr=document.getElementById('trend');tr.innerHTML='';
    (d.score_trend||[]).forEach(function(v){
      var b=document.createElement('div');b.className='trend-bar';
      b.style.height=Math.max(4,v*40)+'px';b.style.background=scoreColor(v);
      tr.appendChild(b);
    });
    // errors
    var eb=document.getElementById('errors');
    eb.textContent=(d.recent_errors&&d.recent_errors.length)?d.recent_errors.join('\n'):'无';
    // pulse color
    var dot=document.getElementById('pulse');
    dot.style.background=d.run_status==='RUNNING'?'#22c55e':d.run_status==='FAILED'?'#ef4444':'#94a3b8';
  }).catch(function(){
    document.getElementById('status-text').textContent='连接失败';
    document.getElementById('pulse').style.background='#ef4444';
  });
}
refresh();setInterval(refresh,3000);
</script>
</body>
</html>"""


class _HealthHandler(BaseHTTPRequestHandler):
    """处理 /health, /metrics, /api/status, /dashboard 请求。"""

    # 由 HealthServer 注入
    _start_time: float = 0.0

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._respond_health()
        elif self.path == "/metrics":
            self._respond_metrics()
        elif self.path == "/api/status":
            self._respond_api_status()
        elif self.path == "/dashboard":
            self._respond_dashboard()
        else:
            self.send_error(404)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        # 静默 HTTP 日志，避免刷屏
        pass

    def _send_json(self, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _respond_health(self) -> None:
        uptime = time.time() - self._start_time
        data: dict = {"status": "ok", "uptime": round(uptime, 1)}
        if psutil:
            proc = psutil.Process()
            data["memory_mb"] = round(proc.memory_info().rss / 1024 / 1024, 1)
            data["cpu_percent"] = psutil.cpu_percent(interval=0)
        self._send_json(data)

    def _respond_metrics(self) -> None:
        uptime = time.time() - self._start_time
        lines = [f"orchestrator_uptime_seconds {uptime:.1f}"]
        if psutil:
            proc = psutil.Process()
            lines.append(f"orchestrator_memory_bytes {proc.memory_info().rss}")
            lines.append(f"orchestrator_cpu_percent {psutil.cpu_percent(interval=0)}")
        body = "\n".join(lines).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _respond_api_status(self) -> None:
        state = get_dashboard_state()
        state["uptime_seconds"] = round(time.time() - self._start_time, 1)
        if psutil:
            proc = psutil.Process()
            state["memory_mb"] = round(proc.memory_info().rss / 1024 / 1024, 1)
        self._send_json(state)

    def _respond_dashboard(self) -> None:
        body = _DASHBOARD_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class HealthServer:
    """后台线程运行的轻量 HTTP 健康检查服务器。"""

    def __init__(self, port: int = 9100, bind: str = "127.0.0.1") -> None:
        self._port = port
        self._bind = bind
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        # 启动前从持久化文件恢复上次的 Dashboard 状态
        load_dashboard_state()
        _HealthHandler._start_time = time.time()
        try:
            self._server = HTTPServer((self._bind, self._port), _HealthHandler)
        except OSError as exc:
            logger.warning("健康检查端口 %d 绑定失败: %s", self._port, exc)
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True, name="health-server"
        )
        self._thread.start()
        logger.info(
            "健康检查端点已启动: http://%s:%d/health | Dashboard: http://%s:%d/dashboard",
            self._bind, self._port, self._bind, self._port,
        )

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
