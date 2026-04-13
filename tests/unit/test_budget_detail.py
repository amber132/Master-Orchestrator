from __future__ import annotations

from claude_orchestrator.claude_cli import BudgetTracker, ModelTokenUsage


def test_record_usage_tracks_per_model():
    tracker = BudgetTracker(max_budget_usd=100.0)
    tracker.record_usage("opus", input_tokens=1000, output_tokens=500, cost_usd=0.05)
    tracker.record_usage("opus", input_tokens=2000, output_tokens=1000, cost_usd=0.10)
    tracker.record_usage("sonnet", input_tokens=500, output_tokens=200, cost_usd=0.01)

    usage = tracker.model_usage
    assert usage["opus"].input_tokens == 3000
    assert usage["opus"].output_tokens == 1500
    assert usage["opus"].request_count == 2
    assert usage["sonnet"].input_tokens == 500
    assert round(tracker.spent, 6) == 0.16


def test_cache_hit_rate():
    tracker = BudgetTracker(max_budget_usd=100.0)
    tracker.record_usage("opus", input_tokens=500, cache_read_tokens=500)
    assert tracker.cache_hit_rate() == 0.5  # 500/(500+500)


def test_cache_hit_rate_no_data():
    tracker = BudgetTracker(max_budget_usd=100.0)
    assert tracker.cache_hit_rate() == 0.0


def test_summary_structure():
    tracker = BudgetTracker(max_budget_usd=100.0)
    tracker.record_usage("opus", input_tokens=100, output_tokens=50,
                         cache_read_tokens=200, cache_creation_tokens=300, cost_usd=0.05)
    s = tracker.summary()
    assert "models" in s
    assert "cache_hit_rate" in s
    assert s["models"]["opus"]["cache_read_tokens"] == 200
    assert s["models"]["opus"]["cache_creation_tokens"] == 300


def test_model_token_usage_defaults():
    u = ModelTokenUsage()
    assert u.input_tokens == 0
    assert u.cost_usd == 0.0
    assert u.request_count == 0


def test_record_usage_increments_spent():
    """record_usage 应同时增加 _spent（通过 check_and_add 的方式）"""
    tracker = BudgetTracker(max_budget_usd=100.0)
    tracker.record_usage("opus", cost_usd=1.5)
    tracker.record_usage("sonnet", cost_usd=0.5)
    assert tracker.spent == 2.0
    assert tracker.model_usage["opus"].cost_usd == 1.5
    assert tracker.model_usage["sonnet"].cost_usd == 0.5


def test_summary_remaining_usd():
    tracker = BudgetTracker(max_budget_usd=100.0)
    tracker.record_usage("opus", cost_usd=30.0)
    s = tracker.summary()
    assert s["remaining_usd"] == 70.0


def test_backward_compatibility_check_and_add():
    """现有代码调用 check_and_add 不应受 record_usage 影响"""
    tracker = BudgetTracker(max_budget_usd=100.0)
    tracker.check_and_add(5.0)
    assert tracker.spent == 5.0
    # record_usage 再加
    tracker.record_usage("opus", cost_usd=3.0)
    assert tracker.spent == 8.0
