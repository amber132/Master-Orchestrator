from __future__ import annotations

import pytest

from claude_orchestrator.claude_cli import BudgetTracker
from claude_orchestrator.config import load_config
from claude_orchestrator.exceptions import BudgetExhaustedError
from codex_orchestrator.codex_cli import BudgetTracker as CodexBudgetTracker


def test_budget_tracker_hard_limit_blocks_check_and_add() -> None:
    tracker = BudgetTracker(1.0, enforcement_mode="hard_limit")

    tracker.check_and_add(0.4)
    assert tracker.spent == 0.4

    with pytest.raises(BudgetExhaustedError):
        tracker.check_and_add(0.7)

    assert tracker.spent == pytest.approx(0.4, abs=1e-6)


def test_budget_tracker_hard_limit_blocks_record_usage_without_mutating_usage() -> None:
    tracker = BudgetTracker(1.0, enforcement_mode="hard_limit")

    tracker.record_usage("sonnet", input_tokens=10, output_tokens=5, cost_usd=0.6)
    assert tracker.spent == pytest.approx(0.6, abs=1e-6)
    assert tracker.model_usage["sonnet"].request_count == 1
    assert tracker.can_afford(0.5) is False
    assert tracker.remaining == pytest.approx(0.4, abs=1e-6)

    with pytest.raises(BudgetExhaustedError):
        tracker.record_usage("sonnet", input_tokens=20, output_tokens=10, cost_usd=0.5)

    assert tracker.spent == pytest.approx(0.6, abs=1e-6)
    assert tracker.model_usage["sonnet"].request_count == 1


def test_load_config_reads_budget_enforcement_mode_from_claude_section(tmp_path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[claude]
budget_enforcement_mode = "hard_limit"
max_budget_usd = 12.5
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.claude.budget_enforcement_mode == "hard_limit"
    assert cfg.claude.max_budget_usd == 12.5


def test_codex_budget_tracker_hard_limit_blocks() -> None:
    tracker = CodexBudgetTracker(1.0, enforcement_mode="hard_limit")

    tracker.check_and_add(0.5)
    assert tracker.spent == pytest.approx(0.5, abs=1e-6)
    assert tracker.can_afford(0.4) is True
    assert tracker.can_afford(0.6) is False

    with pytest.raises(BudgetExhaustedError):
        tracker.check_and_add(0.6)

    assert tracker.spent == pytest.approx(0.5, abs=1e-6)


def test_load_config_reads_budget_enforcement_mode_from_codex_section(tmp_path) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[codex]
budget_enforcement_mode = "hard_limit"
max_budget_usd = 4.2
""".strip(),
        encoding="utf-8",
    )

    cfg = load_config(config_path)

    assert cfg.codex.budget_enforcement_mode == "hard_limit"
    assert cfg.codex.max_budget_usd == 4.2
