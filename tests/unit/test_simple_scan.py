from __future__ import annotations

from claude_orchestrator.simple_loader import SimpleLoadResult
from claude_orchestrator.simple_model import AttemptState, SimpleItemType, SimpleValidationProfile, SimpleWorkItem
from claude_orchestrator.simple_scan import build_scan_report


def _make_item(item_id: str, target: str, bucket: str, priority: int) -> SimpleWorkItem:
    return SimpleWorkItem(
        item_id=item_id,
        item_type=SimpleItemType.FILE,
        target=target,
        bucket=bucket,
        priority=priority,
        instruction="annotate",
        attempt_state=AttemptState(),
        validation_profile=SimpleValidationProfile(),
    )


def test_build_scan_report_limits_samples_and_copies_source_summary() -> None:
    source_summary = {"files": 2, "globs": 0, "task_file": 0, "buckets": {"src": 2}}
    load_result = SimpleLoadResult(
        items=[
            _make_item("item-1", "src/a.py", "src", 10),
            _make_item("item-2", "src/b.py", "src", 5),
        ],
        source_summary=source_summary,
        warnings=["missing target skipped"],
    )

    report = build_scan_report(load_result, sample_size=1)
    source_summary["buckets"]["src"] = 99

    assert report.total_items == 2
    assert report.buckets == {"src": 2}
    assert len(report.samples) == 1
    assert report.samples[0]["target"] == "src/a.py"
    assert report.warnings == ["missing target skipped"]
    assert report.source_summary["buckets"]["src"] == 2


def test_simple_scan_report_render_text_includes_buckets_samples_and_warnings() -> None:
    load_result = SimpleLoadResult(
        items=[_make_item("item-1", "src/a.py", "src", 10)],
        source_summary={"files": 1, "globs": 0, "task_file": 0, "buckets": {"src": 1}},
        warnings=["warn-1"],
    )

    text = build_scan_report(load_result).render_text()

    assert "Simple Scan Preview" in text
    assert "Total items: 1" in text
    assert "src: 1" in text
    assert "[file] src/a.py" in text
    assert "warn-1" in text
