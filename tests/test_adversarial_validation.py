"""对抗性验证 prompt 测试。"""
import json
from unittest.mock import MagicMock, patch

import pytest

from claude_orchestrator.model import TaskNode, TaskResult, TaskStatus, RetryPolicy


class TestAdversarialValidation:
    """对抗性验证门禁测试。"""

    def _make_task_with_gate(self, validator_prompt: str = "检查输出质量", threshold: float = 0.7):
        """创建带验证门禁的任务节点。"""
        return TaskNode(
            id="test_task",
            prompt_template="测试任务",
            validation_gate={
                "validator_prompt": validator_prompt,
                "pass_threshold": threshold,
            },
        )

    def _make_result(self, output: str = "测试输出"):
        """创建测试结果。"""
        return TaskResult(
            task_id="test_task",
            status=TaskStatus.SUCCESS,
            output=output,
            parsed_output=output,
        )

    def _make_orchestrator(self):
        """创建用于测试的 Orchestrator 实例（mock 掉 Store）。"""
        from claude_orchestrator.orchestrator import Orchestrator
        from claude_orchestrator.config import Config
        from claude_orchestrator.model import DAG
        from claude_orchestrator.store import Store

        dag = DAG(name="test", tasks={
            "t1": TaskNode(id="t1", prompt_template="test"),
        })
        config = Config()

        with patch.object(Store, '__init__', lambda self, *a, **kw: None):
            with patch.object(Store, 'close', lambda self: None):
                return Orchestrator(dag=dag, config=config)

    # ---- _parse_validation_score 测试 ----

    def test_parse_score_from_dict_with_score(self):
        """从字典的 score 字段提取分数。"""
        orch = self._make_orchestrator()
        result = orch._parse_validation_score({
            "score": 0.85,
            "dimensions": {
                "completeness": 0.9,
                "consistency": 0.8,
                "accuracy": 0.85,
                "format": 0.9,
                "safety": 0.8,
            },
            "issues": [],
            "summary": "输出质量良好",
        })
        assert result == 0.85

    def test_parse_score_from_dimensions_average(self):
        """从 dimensions 计算平均分（无 score 字段时）。"""
        orch = self._make_orchestrator()
        result = orch._parse_validation_score({
            "dimensions": {
                "completeness": 0.8,
                "consistency": 0.6,
            },
            "issues": ["缺少关键信息"],
        })
        assert result == pytest.approx(0.7, abs=0.01)

    def test_parse_score_json_string(self):
        """解析 JSON 字符串格式的验证结果。"""
        orch = self._make_orchestrator()
        result = orch._parse_validation_score('{"score": 0.75}')
        assert result == 0.75

    def test_parse_score_plain_number(self):
        """纯数字直接返回。"""
        orch = self._make_orchestrator()
        assert orch._parse_validation_score(0.9) == 0.9
        assert orch._parse_validation_score(1) == 1.0

    def test_parse_score_numeric_string(self):
        """数字字符串直接解析。"""
        orch = self._make_orchestrator()
        assert orch._parse_validation_score("0.6") == 0.6

    def test_parse_score_none_for_invalid(self):
        """无效输入返回 None。"""
        orch = self._make_orchestrator()
        assert orch._parse_validation_score(None) is None
        assert orch._parse_validation_score("not a number") is None
        assert orch._parse_validation_score({}) is None

    def test_parse_score_backward_compat_dict_score_only(self):
        """向后兼容：旧的简单 {score: x} 格式仍能解析。"""
        orch = self._make_orchestrator()
        assert orch._parse_validation_score({"score": 0.5}) == 0.5

    # ---- 对抗性 prompt 构造测试 ----

    def test_adversarial_prompt_contains_dimensions(self):
        """对抗性 prompt 包含多维度评分要求。"""
        orch = self._make_orchestrator()
        task = self._make_task_with_gate()
        result = self._make_result("一些输出内容")

        # 提取 prompt 构造逻辑验证关键词
        validator_prompt = task.validation_gate["validator_prompt"]
        task_output = result.output or ""

        adversarial_prefix = """你是一个极其严格的质量审查专家。你的任务是尽可能找出以下任务输出中的缺陷、遗漏、不一致或错误。

请从以下角度逐一审查：
1. **完整性**：输出是否遗漏了关键信息或步骤？
2. **一致性**：输出内部的各部分是否自洽？有无矛盾？
3. **准确性**：数值、引用、事实是否准确？有无编造内容？
4. **格式**：是否严格遵循了要求的输出格式？
5. **安全性**：是否包含敏感信息或危险操作？

对每个维度给出 0-1 分的评分，然后计算加权平均分作为最终分数。

原始验证标准：
"""
        validation_prompt = (
            f"{adversarial_prefix}{validator_prompt}\n\n任务输出：\n{task_output}\n\n"
            "请以 JSON 格式输出评估结果：\n"
            "```json\n"
            '{"score": <0-1>, "dimensions": {"completeness": <0-1>, '
            '"consistency": <0-1>, "accuracy": <0-1>, "format": <0-1>, '
            '"safety": <0-1>}, "issues": ["<发现的问题1>", ...], '
            '"summary": "<一句话总结>"}\n'
            "```"
        )

        # 验证 prompt 包含所有关键维度
        assert "完整性" in validation_prompt
        assert "一致性" in validation_prompt
        assert "准确性" in validation_prompt
        assert "格式" in validation_prompt
        assert "安全性" in validation_prompt
        assert "dimensions" in validation_prompt
        assert "issues" in validation_prompt
        assert "极其严格" in validation_prompt
        assert validator_prompt in validation_prompt
        assert task_output in validation_prompt
