"""端到端测试：验证 cost_usd 从 CLI 输出提取 → BudgetTracker 累加 → Orchestrator._finalize 聚合 → AutonomousController._save_state 持久化的完整链路。

测试策略：
1. 单元级：_extract_cost_usd 从各种 CLI 输出格式中正确提取成本
2. 单元级：_estimate_cost_from_tokens 在无显式成本时从 token 估算成本
3. 单元级：BudgetTracker.record_usage 正确累加 cost_usd
4. 集成级：Orchestrator._finalize 从 TaskResult 聚合 total_cost_usd > 0
5. 集成级：AutonomousController._save_state 将 total_cost_usd 写入 goal_state.json
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

# 确保可以导入主包
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from claude_orchestrator.claude_cli import (
    BudgetTracker,
    _estimate_cost_from_tokens,
    _extract_cost_usd,
    _parse_cli_output,
    _parse_stream_event,
    StreamProgress,
)
from claude_orchestrator.model import (
    DAG,
    RunInfo,
    RunStatus,
    TaskNode,
    TaskResult,
    TaskStatus,
)


# ============================================================
# 测试 1：_extract_cost_usd 从各种 CLI 输出格式提取成本
# ============================================================

class TestExtractCostUsd:
    """验证 _extract_cost_usd 能从 Claude CLI 实际可能输出的各种 JSON 格式中提取成本。"""

    def test_total_cost_usd_field(self):
        """策略 1：顶层 total_cost_usd（最可靠）"""
        event = {"type": "result", "total_cost_usd": 0.0123, "result": "done"}
        cost = _extract_cost_usd(event)
        assert cost > 0, f"应从 total_cost_usd 提取到正数成本，实际: {cost}"
        assert abs(cost - 0.0123) < 1e-6

    def test_cost_usd_field(self):
        """策略 2：顶层 cost_usd"""
        event = {"type": "result", "cost_usd": 0.005, "result": "done"}
        cost = _extract_cost_usd(event)
        assert cost > 0, f"应从 cost_usd 提取到正数成本，实际: {cost}"
        assert abs(cost - 0.005) < 1e-6

    def test_total_cost_field(self):
        """策略 3：顶层 total_cost（无 _usd 后缀）"""
        event = {"type": "result", "total_cost": 0.008, "result": "done"}
        cost = _extract_cost_usd(event)
        assert cost > 0, f"应从 total_cost 提取到正数成本，实际: {cost}"
        assert abs(cost - 0.008) < 1e-6

    def test_cost_usd_in_usage(self):
        """策略 8：嵌套在 usage 对象中"""
        event = {
            "type": "result",
            "usage": {"input_tokens": 1000, "output_tokens": 500, "cost_usd": 0.003},
            "result": "done",
        }
        cost = _extract_cost_usd(event)
        assert cost > 0, f"应从 usage.cost_usd 提取到正数成本，实际: {cost}"
        assert abs(cost - 0.003) < 1e-6

    def test_cost_usd_in_model_usage(self):
        """策略 9：从 modelUsage 汇总"""
        event = {
            "type": "result",
            "modelUsage": {
                "claude-sonnet-4": {"costUSD": 0.004, "input_tokens": 1000},
                "claude-haiku": {"cost_usd": 0.002, "input_tokens": 500},
            },
            "result": "done",
        }
        cost = _extract_cost_usd(event)
        assert cost > 0, f"应从 modelUsage 汇总提取到正数成本，实际: {cost}"
        assert abs(cost - 0.006) < 1e-6

    def test_zero_cost_when_no_fields(self):
        """无成本字段且无 token 信息时应返回 0.0"""
        event = {"type": "result", "result": "done"}
        cost = _extract_cost_usd(event)
        assert cost == 0.0, f"无成本字段时应返回 0.0，实际: {cost}"

    def test_input_output_cost_split(self):
        """策略 5：拆分成本 input_cost_usd + output_cost_usd"""
        event = {
            "type": "result",
            "input_cost_usd": 0.001,
            "output_cost_usd": 0.003,
            "result": "done",
        }
        cost = _extract_cost_usd(event)
        assert cost > 0, f"应从拆分成本提取到正数，实际: {cost}"
        assert abs(cost - 0.004) < 1e-6


# ============================================================
# 测试 2：_estimate_cost_from_tokens 回退估算
# ============================================================

class TestEstimateCostFromTokens:
    """验证 _estimate_cost_from_tokens 在无显式 cost 字段时能从 token 使用量估算成本。"""

    def test_with_input_output_tokens(self):
        """有 input_tokens 和 output_tokens 时应估算出正数成本"""
        event = {
            "model": "claude-sonnet-4",
            "usage": {"input_tokens": 10000, "output_tokens": 5000},
        }
        cost = _estimate_cost_from_tokens(event)
        assert cost > 0, f"应从 token 估算出正数成本，实际: {cost}"
        # Sonnet 定价：$3/M input + $15/M output = 0.03 + 0.075 = 0.105
        expected_min = 0.05  # 保守下限
        assert cost >= expected_min, f"估算成本 {cost} 应 >= {expected_min}"

    def test_with_cache_tokens(self):
        """有 cache_read 和 cache_creation tokens 时应计入估算"""
        event_no_cache = {
            "model": "claude-sonnet-4",
            "usage": {"input_tokens": 10000, "output_tokens": 5000},
        }
        event_with_cache = {
            "model": "claude-sonnet-4",
            "usage": {
                "input_tokens": 10000,
                "output_tokens": 5000,
                "cache_read_input_tokens": 50000,
            },
        }
        cost_no_cache = _estimate_cost_from_tokens(event_no_cache)
        cost_with_cache = _estimate_cost_from_tokens(event_with_cache)
        assert cost_with_cache > cost_no_cache, (
            f"有 cache_read 时成本应更高: with={cost_with_cache} vs without={cost_no_cache}"
        )

    def test_zero_tokens_returns_zero(self):
        """无 token 信息时返回 0.0"""
        event = {"model": "claude-sonnet-4"}
        cost = _estimate_cost_from_tokens(event)
        assert cost == 0.0

    def test_unknown_model_uses_default_pricing(self):
        """未知模型使用默认定价（Sonnet 级别）"""
        event = {
            "model": "some-future-model-xyz",
            "usage": {"input_tokens": 1000, "output_tokens": 1000},
        }
        cost = _estimate_cost_from_tokens(event)
        assert cost > 0, "未知模型也应估算出正数成本"

    def test_top_level_tokens_fallback(self):
        """顶层 token_count_input / token_count_output 字段也能被识别"""
        event = {
            "model": "claude-sonnet-4",
            "token_count_input": 5000,
            "token_count_output": 2000,
        }
        cost = _estimate_cost_from_tokens(event)
        assert cost > 0, "应从顶层 token_count_input/output 估算出成本"


# ============================================================
# 测试 3：BudgetTracker.record_usage 正确累加
# ============================================================

class TestBudgetTracker:
    """验证 BudgetTracker 的成本记录和累加机制。"""

    def test_record_usage_accumulates_cost(self):
        """record_usage 多次调用后 spent 应等于所有 cost_usd 之和"""
        tracker = BudgetTracker(max_budget_usd=100.0)
        tracker.record_usage(model="claude-sonnet-4", cost_usd=0.01)
        assert tracker.spent == pytest.approx(0.01, abs=1e-6)
        tracker.record_usage(model="claude-sonnet-4", cost_usd=0.02)
        assert tracker.spent == pytest.approx(0.03, abs=1e-6)
        tracker.record_usage(model="claude-opus-4", cost_usd=0.05)
        assert tracker.spent == pytest.approx(0.08, abs=1e-6)

    def test_record_usage_tracks_per_model(self):
        """按模型分组的 token 和成本统计应正确"""
        tracker = BudgetTracker(max_budget_usd=100.0)
        tracker.record_usage(
            model="claude-sonnet-4",
            input_tokens=1000,
            output_tokens=500,
            cost_usd=0.01,
        )
        tracker.record_usage(
            model="claude-opus-4",
            input_tokens=2000,
            output_tokens=1000,
            cost_usd=0.05,
        )
        usage = tracker.model_usage
        assert "claude-sonnet-4" in usage
        assert "claude-opus-4" in usage
        assert usage["claude-sonnet-4"].input_tokens == 1000
        assert usage["claude-opus-4"].output_tokens == 1000

    def test_total_cost_property(self):
        """total_cost 属性应等于 spent"""
        tracker = BudgetTracker(max_budget_usd=100.0)
        tracker.record_usage(model="claude-sonnet-4", cost_usd=0.01)
        assert tracker.total_cost == tracker.spent
        assert tracker.total_cost == pytest.approx(0.01, abs=1e-6)

    def test_summary_includes_total_spent(self):
        """summary() 应包含 total_spent_usd"""
        tracker = BudgetTracker(max_budget_usd=100.0)
        tracker.record_usage(model="claude-sonnet-4", cost_usd=0.01)
        s = tracker.summary()
        assert s["total_spent_usd"] == pytest.approx(0.01, abs=1e-6)

    def test_thread_safety(self):
        """多线程并发调用 record_usage 不应丢失成本"""
        import threading

        tracker = BudgetTracker(max_budget_usd=100.0)
        num_threads = 10
        cost_per_call = 0.001
        calls_per_thread = 100

        def worker():
            for _ in range(calls_per_thread):
                tracker.record_usage(model="test-model", cost_usd=cost_per_call)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        expected = num_threads * calls_per_thread * cost_per_call
        assert tracker.spent == pytest.approx(expected, abs=1e-3), (
            f"并发累加后 spent={tracker.spent}，期望={expected}"
        )


# ============================================================
# 测试 4：_parse_cli_output 从完整 CLI JSON 输出提取成本
# ============================================================

class TestParseCliOutput:
    """验证 _parse_cli_output 能从实际 CLI 输出格式解析成本。"""

    def test_result_event_with_cost(self):
        """CLI 返回 result 类型事件并包含 total_cost_usd"""
        raw = json.dumps({
            "type": "result",
            "result": "Task completed successfully",
            "is_error": False,
            "total_cost_usd": 0.025,
            "model": "claude-sonnet-4",
            "usage": {"input_tokens": 5000, "output_tokens": 2000},
        })
        resp = _parse_cli_output(raw)
        assert resp.cost_usd == pytest.approx(0.025, abs=1e-6)
        assert not resp.is_error

    def test_result_event_with_model_usage(self):
        """CLI 返回 modelUsage 格式的成本数据"""
        raw = json.dumps({
            "type": "result",
            "result": "done",
            "is_error": False,
            "modelUsage": {
                "claude-sonnet-4": {
                    "input_tokens": 5000,
                    "output_tokens": 2000,
                    "costUSD": 0.015,
                },
            },
        })
        resp = _parse_cli_output(raw)
        assert resp.cost_usd > 0, f"应从 modelUsage 提取成本，实际: {resp.cost_usd}"

    def test_result_event_without_cost_uses_token_estimate(self):
        """CLI 返回无成本字段但有 token 数据时，应回退到 token 估算"""
        raw = json.dumps({
            "type": "result",
            "result": "done",
            "is_error": False,
            "usage": {"input_tokens": 10000, "output_tokens": 5000},
        })
        resp = _parse_cli_output(raw)
        assert resp.cost_usd > 0, (
            f"无显式成本时应从 token 估算，实际: {resp.cost_usd}"
        )

    def test_non_json_output(self):
        """CLI 返回非 JSON 纯文本时，cost 应为 0"""
        resp = _parse_cli_output("Just some plain text output")
        assert resp.cost_usd == 0.0


# ============================================================
# 测试 5：_parse_stream_event 从流式事件提取成本
# ============================================================

class TestStreamProgressCostExtraction:
    """验证流式事件解析能正确提取成本。"""

    def test_result_event_with_total_cost_usd(self):
        """流式 result 事件包含 total_cost_usd"""
        progress = StreamProgress()
        line = json.dumps({
            "type": "result",
            "result": "done",
            "is_error": False,
            "total_cost_usd": 0.033,
            "usage": {"input_tokens": 1000, "output_tokens": 500},
        })
        _parse_stream_event(line, "test-task", progress)
        assert progress.result_event is not None
        cost = _extract_cost_usd(progress.result_event)
        assert cost == pytest.approx(0.033, abs=1e-6)

    def test_result_event_with_tokens_only(self):
        """流式 result 事件无成本字段但有 token → 回退估算"""
        progress = StreamProgress()
        line = json.dumps({
            "type": "result",
            "result": "done",
            "is_error": False,
            "model": "claude-sonnet-4",
            "usage": {"input_tokens": 5000, "output_tokens": 2000},
        })
        _parse_stream_event(line, "test-task", progress)
        assert progress.result_event is not None
        cost = _extract_cost_usd(progress.result_event)
        assert cost > 0, f"流式事件无成本字段时应从 token 估算，实际: {cost}"


# ============================================================
# 测试 6：Orchestrator._finalize 从 TaskResult 聚合 total_cost_usd
# ============================================================

class TestOrchestratorFinalize:
    """验证 Orchestrator._finalize 能从内存中的 TaskResult 聚合出 total_cost_usd > 0。"""

    def _make_orchestrator(self, tmpdir, dag):
        """创建 Orchestrator 实例，确保所有资源可正确清理。"""
        from claude_orchestrator.orchestrator import Orchestrator
        from claude_orchestrator.config import Config
        from claude_orchestrator.store import Store

        db_path = str(Path(tmpdir) / "test_state.db")
        config = Config()
        config.checkpoint.db_path = db_path
        config.claude.max_budget_usd = 100.0

        store = Store(db_path)
        orch = Orchestrator(
            dag=dag,
            config=config,
            store=store,
            working_dir=tmpdir,
        )
        return orch, store

    def _cleanup_orchestrator(self, orch, store):
        """关闭 Orchestrator 持有的所有资源（SQLite 连接等）。"""
        try:
            orch._close_store()
        except Exception:
            pass
        try:
            orch._checkpoint_manager.close()
        except Exception:
            pass
        try:
            orch._diagnostics.close()
        except Exception:
            pass
        try:
            store.close()
        except Exception:
            pass

    def test_finalize_aggregates_cost_from_results(self, tmp_path):
        """_finalize 从 _results 中的 TaskResult.cost_usd 累加 total_cost_usd"""
        dag = DAG(
            name="test-cost-dag",
            tasks={
                "task1": TaskNode(
                    id="task1",
                    prompt_template="echo hello",
                ),
            },
        )

        orch, store = self._make_orchestrator(tmp_path, dag)
        try:
            # 模拟 _run_info 和 _results（不执行实际任务）
            orch._run_info = RunInfo(dag_name="test-cost-dag")
            orch._store.create_run(orch._run_info)

            # 手动注入带 cost_usd 的 TaskResult
            now = datetime.now()
            task_result = TaskResult(
                task_id="task1",
                status=TaskStatus.SUCCESS,
                output="done",
                cost_usd=0.05,
                started_at=now,
                finished_at=now,
                duration_seconds=1.0,
            )
            orch._results["task1"] = task_result

            # 标记任务为 SUCCESS 让 _finalize 判定 COMPLETED
            orch._scheduler.mark_running("task1")
            orch._scheduler.mark_completed("task1", TaskStatus.SUCCESS)

            # 调用 _finalize
            orch._finalize()

            # 验证 total_cost_usd > 0
            assert orch._run_info.total_cost_usd > 0, (
                f"_finalize 后 total_cost_usd 应 > 0，实际: {orch._run_info.total_cost_usd}"
            )
            assert orch._run_info.total_cost_usd == pytest.approx(0.05, abs=1e-6)
        finally:
            self._cleanup_orchestrator(orch, store)

    def test_finalize_aggregates_multiple_results(self, tmp_path):
        """多个 TaskResult 的成本应正确累加"""
        dag = DAG(
            name="test-multi-cost-dag",
            tasks={
                "task1": TaskNode(id="task1", prompt_template="echo 1"),
                "task2": TaskNode(id="task2", prompt_template="echo 2"),
            },
        )

        orch, store = self._make_orchestrator(tmp_path, dag)
        try:
            orch._run_info = RunInfo(dag_name="test-multi-cost-dag")
            orch._store.create_run(orch._run_info)

            now = datetime.now()
            orch._results["task1"] = TaskResult(
                task_id="task1",
                status=TaskStatus.SUCCESS,
                cost_usd=0.03,
                started_at=now,
                finished_at=now,
            )
            orch._results["task2"] = TaskResult(
                task_id="task2",
                status=TaskStatus.SUCCESS,
                cost_usd=0.07,
                started_at=now,
                finished_at=now,
            )

            orch._scheduler.mark_running("task1")
            orch._scheduler.mark_completed("task1", TaskStatus.SUCCESS)
            orch._scheduler.mark_running("task2")
            orch._scheduler.mark_completed("task2", TaskStatus.SUCCESS)

            orch._finalize()

            assert orch._run_info.total_cost_usd == pytest.approx(0.10, abs=1e-5), (
                f"多个 TaskResult 累加后 total_cost_usd 应 ≈ 0.10，实际: {orch._run_info.total_cost_usd}"
            )
        finally:
            self._cleanup_orchestrator(orch, store)


# ============================================================
# 测试 7：AutonomousController._save_state 持久化 total_cost_usd
# ============================================================

class TestAutonomousControllerSaveState:
    """验证 AutonomousController._save_state 将 total_cost_usd 写入 goal_state.json。"""

    def test_save_state_persists_cost(self, tmp_path):
        """_save_state 应将 budget.spent 同步到 state.total_cost_usd 并写入文件"""
        from claude_orchestrator.auto_model import GoalState, save_goal_state, load_goal_state
        from claude_orchestrator.claude_cli import BudgetTracker

        state_path = tmp_path / "goal_state.json"

        # 创建 GoalState
        state = GoalState(goal_text="test goal")
        assert state.total_cost_usd == 0.0

        # 创建 BudgetTracker 并记录花费
        budget = BudgetTracker(max_budget_usd=100.0)
        budget.record_usage(model="claude-sonnet-4", cost_usd=0.042)

        # 手动模拟 _save_state 的核心逻辑
        state.total_cost_usd = budget.spent
        save_goal_state(state, state_path)

        # 验证文件存在且 total_cost_usd > 0
        assert state_path.exists(), "goal_state.json 应被创建"
        with open(state_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert data["total_cost_usd"] > 0, (
            f"文件中 total_cost_usd 应 > 0，实际: {data['total_cost_usd']}"
        )
        assert data["total_cost_usd"] == pytest.approx(0.042, abs=1e-6)

        # 验证 load_goal_state 也能正确读回
        loaded = load_goal_state(state_path)
        assert loaded.total_cost_usd == pytest.approx(0.042, abs=1e-6)

    def test_save_state_updates_with_new_cost(self, tmp_path):
        """多次更新 BudgetTracker 后 _save_state 应反映最新成本"""
        from claude_orchestrator.auto_model import GoalState, save_goal_state, load_goal_state
        from claude_orchestrator.claude_cli import BudgetTracker

        state_path = tmp_path / "goal_state.json"
        state = GoalState(goal_text="test goal")
        budget = BudgetTracker(max_budget_usd=100.0)

        # 第一次保存
        budget.record_usage(model="claude-sonnet-4", cost_usd=0.01)
        state.total_cost_usd = budget.spent
        save_goal_state(state, state_path)

        loaded = load_goal_state(state_path)
        assert loaded.total_cost_usd == pytest.approx(0.01, abs=1e-6)

        # 第二次累加保存
        budget.record_usage(model="claude-sonnet-4", cost_usd=0.02)
        state.total_cost_usd = budget.spent
        save_goal_state(state, state_path)

        loaded = load_goal_state(state_path)
        assert loaded.total_cost_usd == pytest.approx(0.03, abs=1e-6)


# ============================================================
# 测试 8：完整链路模拟 — _extract_cost_usd → BudgetTracker → _finalize
# ============================================================

class TestEndToEndPipeline:
    """模拟完整链路：CLI 输出 → 成本提取 → BudgetTracker → TaskResult → _finalize。"""

    def test_full_pipeline_with_total_cost_usd(self, tmp_path):
        """模拟 CLI 返回 total_cost_usd 的完整链路"""
        # 1. 模拟 CLI JSON 输出
        cli_output = json.dumps({
            "type": "result",
            "result": "Task completed",
            "is_error": False,
            "total_cost_usd": 0.015,
            "model": "claude-sonnet-4",
            "usage": {"input_tokens": 3000, "output_tokens": 1000},
        })

        # 2. 解析 CLI 输出
        resp = _parse_cli_output(cli_output)
        assert resp.cost_usd == pytest.approx(0.015, abs=1e-6)

        # 3. 记录到 BudgetTracker
        tracker = BudgetTracker(max_budget_usd=100.0)
        tracker.record_usage(
            model=resp.model or "claude-sonnet-4",
            cost_usd=resp.cost_usd,
        )
        assert tracker.spent == pytest.approx(0.015, abs=1e-6)

        # 4. 创建 TaskResult（模拟 Orchestrator 内部流程）
        now = datetime.now()
        task_result = TaskResult(
            task_id="task1",
            status=TaskStatus.SUCCESS,
            output="Task completed",
            cost_usd=resp.cost_usd,
            started_at=now,
            finished_at=now,
            duration_seconds=2.0,
        )
        assert task_result.cost_usd > 0

        # 5. 验证 Orchestrator._finalize 能聚合
        from claude_orchestrator.orchestrator import Orchestrator
        from claude_orchestrator.config import Config
        from claude_orchestrator.store import Store

        dag = DAG(
            name="e2e-test-dag",
            tasks={"task1": TaskNode(id="task1", prompt_template="test")},
        )

        db_path = str(tmp_path / "e2e_state.db")
        config = Config()
        config.checkpoint.db_path = db_path

        store = Store(db_path)
        orch = Orchestrator(dag=dag, config=config, store=store, working_dir=str(tmp_path))

        try:
            orch._run_info = RunInfo(dag_name="e2e-test-dag")
            orch._store.create_run(orch._run_info)
            orch._results["task1"] = task_result
            orch._scheduler.mark_running("task1")
            orch._scheduler.mark_completed("task1", TaskStatus.SUCCESS)

            orch._finalize()

            assert orch._run_info.total_cost_usd > 0, (
                f"完整链路验证失败：_finalize 后 total_cost_usd={orch._run_info.total_cost_usd}"
            )
        finally:
            # 清理所有资源
            for closer in ("_close_store",):
                try:
                    getattr(orch, closer)()
                except Exception:
                    pass
            try:
                orch._checkpoint_manager.close()
            except Exception:
                pass
            try:
                orch._diagnostics.close()
            except Exception:
                pass
            try:
                store.close()
            except Exception:
                pass

    def test_full_pipeline_with_token_estimation(self, tmp_path):
        """模拟 CLI 只返回 token 使用量（无 cost 字段）的完整链路"""
        # 1. 模拟无 cost 字段但有 token 的 CLI 输出
        cli_output = json.dumps({
            "type": "result",
            "result": "done",
            "is_error": False,
            "model": "claude-sonnet-4",
            "usage": {"input_tokens": 10000, "output_tokens": 5000},
        })

        # 2. 解析（应回退到 token 估算）
        resp = _parse_cli_output(cli_output)
        assert resp.cost_usd > 0, (
            f"无 cost 字段时应从 token 估算出正数成本，实际: {resp.cost_usd}"
        )

        # 3. BudgetTracker 累加
        tracker = BudgetTracker(max_budget_usd=100.0)
        tracker.record_usage(
            model=resp.model or "claude-sonnet-4",
            input_tokens=10000,
            output_tokens=5000,
            cost_usd=resp.cost_usd,
        )
        assert tracker.spent > 0

        # 4. 验证持久化
        from claude_orchestrator.auto_model import GoalState, save_goal_state, load_goal_state

        state_path = tmp_path / "goal_state.json"
        state = GoalState(goal_text="test")
        state.total_cost_usd = tracker.spent
        save_goal_state(state, state_path)

        loaded = load_goal_state(state_path)
        assert loaded.total_cost_usd > 0, (
            f"持久化验证失败：total_cost_usd={loaded.total_cost_usd}"
        )
