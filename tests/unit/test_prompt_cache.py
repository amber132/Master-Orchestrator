from __future__ import annotations

from claude_orchestrator.prompt_prefix import extract_shared_prefix


def test_extract_shared_prefix_from_tasks():
    """从多个任务的 prompt 中提取共享前缀。"""
    shared = "你是代码审计专家。请按照以下规范审查代码：检查所有安全漏洞、性能问题和代码规范。"
    prompts = [
        shared + "\n请审计模块A的安全问题。",
        shared + "\n请审计模块B的安全问题。",
        shared + "\n请审计模块C的安全问题。",
    ]
    prefix = extract_shared_prefix(prompts, separator="\n")
    assert len(prefix) > 0
    assert "你是代码审计专家" in prefix


def test_no_shared_prefix():
    """没有共享前缀时返回空字符串。"""
    prompts = [
        "完全不同的第一个prompt",
        "完全不同的第二个prompt",
        "完全不同的第三个prompt",
    ]
    prefix = extract_shared_prefix(prompts)
    assert prefix == ""


def test_empty_input():
    """空输入返回空字符串。"""
    assert extract_shared_prefix([]) == ""


def test_single_input():
    """单个输入返回整个字符串。"""
    assert extract_shared_prefix(["single prompt text here"]) == "single prompt text here"
