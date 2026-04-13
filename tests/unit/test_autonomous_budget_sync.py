from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from claude_orchestrator.autonomous_helpers import sync_budget_from_orchestrator
from claude_orchestrator.claude_cli import BudgetTracker


def _build_budget_state(initial_spent: float, initial_state_cost: float) -> tuple[BudgetTracker, object, threading.Lock]:
    budget = BudgetTracker(max_budget_usd=100.0)
    budget.spent = initial_spent
    state = SimpleNamespace(total_cost_usd=initial_state_cost)
    return budget, state, threading.Lock()


def test_sync_budget_from_orchestrator_prefers_higher_run_info_cost():
    budget, state, state_lock = _build_budget_state(initial_spent=1.2, initial_state_cost=1.2)
    orch = SimpleNamespace(_budget=SimpleNamespace(spent=0.4))
    run_info = SimpleNamespace(total_cost_usd=0.6)

    synced_cost = sync_budget_from_orchestrator(
        budget,
        orch,
        run_info=run_info,
        state=state,
        state_lock=state_lock,
    )

    assert synced_cost == pytest.approx(0.6, abs=1e-6)
    assert budget.spent == pytest.approx(1.8, abs=1e-6)
    assert state.total_cost_usd == pytest.approx(1.2, abs=1e-6)


def test_sync_budget_from_orchestrator_can_update_goal_state():
    budget, state, state_lock = _build_budget_state(initial_spent=2.0, initial_state_cost=1.5)
    orch = SimpleNamespace(_budget=SimpleNamespace(spent=0.25))

    synced_cost = sync_budget_from_orchestrator(
        budget,
        orch,
        state=state,
        state_lock=state_lock,
        sync_state=True,
    )

    assert synced_cost == pytest.approx(0.25, abs=1e-6)
    assert budget.spent == pytest.approx(2.25, abs=1e-6)
    assert state.total_cost_usd == pytest.approx(2.25, abs=1e-6)
