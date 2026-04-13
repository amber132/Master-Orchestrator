from __future__ import annotations

from pathlib import Path

from claude_orchestrator.config import SpillConfig
from claude_orchestrator.template import (
    BlockType,
    StructuralPreserver,
    StructureBlock,
    extract_dependencies,
    handle_prompt_too_long,
    hierarchical_compress,
    render_template,
)


def test_render_template_substitutes_nested_output_values() -> None:
    rendered = render_template(
        "User: ${scan.output.user.name}",
        {"scan": {"user": {"name": "alice"}}},
    )

    assert rendered == "User: alice"


def test_render_template_spills_large_outputs_to_file(tmp_path: Path) -> None:
    spill_dir = tmp_path / "spill"
    long_text = "X" * 64

    rendered = render_template(
        "Output:\n${scan}",
        {"scan": long_text},
        spill_config=SpillConfig(spill_threshold_chars=10, summary_chars=12, spill_dir_name="spill"),
        spill_dir=spill_dir,
        run_id="run123",
    )

    spill_file = spill_dir / "run123_scan.txt"
    assert spill_file.exists()
    assert spill_file.read_text(encoding="utf-8") == long_text
    assert "run123_scan.txt" in rendered
    assert "完整内容已保存到文件" in rendered


def test_extract_dependencies_returns_unique_task_ids() -> None:
    deps = extract_dependencies("A ${scan.output} B ${fix.result} C ${scan.extra}")

    assert deps == {"scan", "fix"}


def test_structural_preserver_detects_multiple_block_types() -> None:
    preserver = StructuralPreserver()
    text = (
        "# Heading\n"
        "score: 95%\n"
        "We decided to ship this change.\n"
        "```python\nprint('hi')\n```\n"
        "plain details"
    )

    blocks = preserver.extract_structure(text)
    block_types = {block.block_type for block in blocks}

    assert BlockType.HEADING in block_types
    assert BlockType.METRIC in block_types
    assert BlockType.DECISION in block_types
    assert BlockType.CODE in block_types
    assert BlockType.DETAIL in block_types


def test_hierarchical_compress_preserves_high_priority_blocks() -> None:
    blocks = [
        StructureBlock(BlockType.HEADING, "# Heading"),
        StructureBlock(BlockType.DETAIL, "detail " * 40),
        StructureBlock(BlockType.METRIC, "score: 95%"),
    ]

    compressed = hierarchical_compress(blocks, max_chars=40, max_detail_length=20)

    assert "# Heading" in compressed
    assert "score: 95%" in compressed
    assert len(compressed) <= 40


def test_handle_prompt_too_long_reduces_context_size() -> None:
    context = "# Heading\nscore: 95%\n" + ("detail line\n" * 80)

    compressed = handle_prompt_too_long(
        "prompt is 30 tokens too long",
        context,
        max_detail_length=60,
    )

    assert len(compressed) < len(context)
    assert "# Heading" in compressed
    assert "score: 95%" in compressed
