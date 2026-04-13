from pathlib import Path

from claude_orchestrator.claude_cli import BudgetTracker
from claude_orchestrator.config import load_config


def test_budget_tracker_never_blocks_even_when_spent_exceeds_limit() -> None:
    tracker = BudgetTracker(1.0)

    assert tracker.can_afford() is True
    tracker.check_and_add(2.5)

    assert tracker.spent == 2.5
    assert tracker.can_afford(999999.0) is True


def test_load_config_allows_zero_budget_for_unlimited_mode(tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text("[claude]\nmax_budget_usd = 0\n", encoding="utf-8")

    cfg = load_config(config_file)

    assert cfg.claude.max_budget_usd == 0
