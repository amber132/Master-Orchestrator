from __future__ import annotations

from claude_orchestrator.auto_model import AutoConfig, ReviewVerdict
from claude_orchestrator.config import ClaudeConfig, LimitsConfig
from claude_orchestrator.review_engine import ReviewEngine


def _engine() -> ReviewEngine:
    return ReviewEngine(ClaudeConfig(), LimitsConfig(), AutoConfig())


def test_parse_review_coerces_score_to_match_pass_verdict() -> None:
    review = _engine()._parse_review(
        "phase_1",
        '{"verdict":"pass","score":0.42,"summary":"looks good"}',
    )

    assert review.verdict is ReviewVerdict.PASS
    assert review.score == 0.9


def test_parse_review_does_not_treat_plain_decimal_as_percent_scale() -> None:
    review = _engine()._parse_review(
        "phase_1",
        '{"verdict":"major_issues","score":1.5,"summary":"needs work"}',
    )

    assert review.verdict is ReviewVerdict.MAJOR_ISSUES
    assert review.score == 0.45
