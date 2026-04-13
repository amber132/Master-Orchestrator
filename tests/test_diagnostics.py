"""结构化诊断日志测试。"""
import json
import tempfile
from pathlib import Path

from claude_orchestrator.diagnostics import (
    DiagnosticEvent,
    DiagnosticEventType,
    DiagnosticLogger,
)


class TestDiagnosticLogger:
    """DiagnosticLogger 核心功能测试。"""

    def test_record_creates_jsonl_file(self, tmp_path):
        """记录事件后生成 JSONL 文件。"""
        diag = DiagnosticLogger(log_dir=str(tmp_path))
        event = DiagnosticEvent(
            event_type="task_complete",
            run_id="abc123",
            task_id="t1",
            model="sonnet",
            duration_ms=1500.0,
            cost_usd=0.05,
        )
        diag.record(event)

        log_file = tmp_path / "diagnostics.jsonl"
        assert log_file.exists()

        lines = log_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["event_type"] == "task_complete"
        assert data["run_id"] == "abc123"
        assert data["task_id"] == "t1"

    def test_record_strips_empty_values(self, tmp_path):
        """空值被移除以减少日志体积。"""
        diag = DiagnosticLogger(log_dir=str(tmp_path))
        event = DiagnosticEvent(event_type="health_check")
        diag.record(event)

        log_file = tmp_path / "diagnostics.jsonl"
        data = json.loads(log_file.read_text(encoding="utf-8").strip())
        assert "error" not in data  # 空字符串被移除
        assert "attempt" not in data  # 0 被移除
        assert "event_type" in data

    def test_record_task_lifecycle(self, tmp_path):
        """便捷方法记录任务生命周期。"""
        diag = DiagnosticLogger(log_dir=str(tmp_path))
        diag.record_task_lifecycle(
            event_type=DiagnosticEventType.TASK_START,
            run_id="run1",
            task_id="task1",
            model="opus",
        )

        log_file = tmp_path / "diagnostics.jsonl"
        data = json.loads(log_file.read_text(encoding="utf-8").strip())
        assert data["event_type"] == "task_start"
        assert data["model"] == "opus"

    def test_multiple_events(self, tmp_path):
        """多次记录写入多行。"""
        diag = DiagnosticLogger(log_dir=str(tmp_path))
        for i in range(5):
            diag.record(DiagnosticEvent(
                event_type="task_complete",
                task_id=f"t{i}",
            ))

        log_file = tmp_path / "diagnostics.jsonl"
        lines = log_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 5
        assert diag.event_count == 5

    def test_summary(self, tmp_path):
        """摘要统计正确。"""
        diag = DiagnosticLogger(log_dir=str(tmp_path))
        diag.record(DiagnosticEvent(event_type="test"))
        diag.record(DiagnosticEvent(event_type="test"))

        summary = diag.summary()
        assert summary["event_count"] == 2
        assert "events_per_minute" in summary

    def test_close_writes_summary(self, tmp_path):
        """关闭时写入会话摘要。"""
        diag = DiagnosticLogger(log_dir=str(tmp_path))
        diag.record(DiagnosticEvent(event_type="test"))
        diag.close()

        log_file = tmp_path / "diagnostics.jsonl"
        lines = log_file.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2  # 1 event + 1 summary
        summary_data = json.loads(lines[-1])
        assert summary_data["event_type"] == "session_summary"

    def test_event_auto_timestamp(self):
        """事件自动填充时间戳。"""
        event = DiagnosticEvent(event_type="test")
        assert event.timestamp != ""
        # ISO 8601 格式
        assert "T" in event.timestamp or "-" in event.timestamp

    def test_thread_safety(self, tmp_path):
        """多线程并发写入不出错。"""
        import threading

        diag = DiagnosticLogger(log_dir=str(tmp_path))
        errors = []

        def write_events():
            try:
                for i in range(20):
                    diag.record(DiagnosticEvent(
                        event_type="test",
                        task_id=f"t_{threading.current_thread().name}_{i}",
                    ))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_events, name=f"w{i}") for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert diag.event_count == 100  # 5 threads * 20 events
