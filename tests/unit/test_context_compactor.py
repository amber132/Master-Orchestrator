"""上下文压缩器测试。"""
import json
from collections import OrderedDict

from claude_orchestrator.context_compactor import (
    CompactResult,
    CompactStrategy,
    ContextCompactor,
)


class TestContextCompactor:
    """ContextCompactor 核心逻辑测试。"""

    def test_no_compaction_needed(self):
        """总大小未超阈值时不压缩。"""
        compactor = ContextCompactor(max_total_chars=10_000, compaction_interval=1)
        outputs = OrderedDict([("t1", "short"), ("t2", "also short")])
        assert not compactor.should_compact(outputs)

    def test_compaction_triggered_when_over_threshold(self):
        """总大小超过阈值时触发压缩。"""
        compactor = ContextCompactor(
            max_total_chars=100,
            max_single_chars=50,
            compaction_interval=1,
        )
        outputs = OrderedDict([
            ("t1", "x" * 80),
            ("t2", "y" * 80),
        ])
        assert compactor.should_compact(outputs)
        result = compactor.compact(outputs)
        assert result.compacted_keys
        assert result.total_before > result.total_after
        assert result.total_after < result.total_before  # 确保有效压缩

    def test_truncate_strategy(self):
        """truncate 策略保留头尾。"""
        compactor = ContextCompactor(
            max_total_chars=100,
            max_single_chars=30,
            strategy=CompactStrategy.TRUNCATE,
            compaction_interval=1,
        )
        text = "A" * 200  # 总量必须超过 max_total 才会触发压缩
        outputs = OrderedDict([("t1", text)])
        result = compactor.compact(outputs)
        assert "t1" in result.compacted_keys
        # 压缩后的输出应该比原始短
        assert len(str(outputs["t1"])) < len(text)

    def test_compaction_interval(self):
        """只在指定间隔检查。"""
        compactor = ContextCompactor(
            max_total_chars=10,
            max_single_chars=5,
            compaction_interval=3,
        )
        outputs = OrderedDict([("t1", "x" * 100)])
        # 第 1、2 次不检查
        assert not compactor.should_compact(outputs)
        assert not compactor.should_compact(outputs)
        # 第 3 次检查
        assert compactor.should_compact(outputs)

    def test_dict_output_compression(self):
        """dict 类型输出也能被压缩。"""
        compactor = ContextCompactor(
            max_total_chars=200,
            max_single_chars=100,
            compaction_interval=1,
        )
        big_dict = {"data": "x" * 200, "more": "y" * 200}
        outputs = OrderedDict([("t1", big_dict)])
        assert compactor.should_compact(outputs)
        result = compactor.compact(outputs)
        assert result.compacted_keys
        assert result.total_after < result.total_before

    def test_measure_outputs_mixed_types(self):
        """测量函数支持混合类型。"""
        compactor = ContextCompactor()
        outputs = OrderedDict([
            ("str_val", "hello"),
            ("dict_val", {"key": "value"}),
            ("list_val", [1, 2, 3]),
            ("int_val", 42),
        ])
        total = compactor._measure_outputs(outputs)
        assert total > 0

    def test_compact_below_threshold_returns_early(self):
        """总量低于阈值时 compact 直接返回，不做处理。"""
        compactor = ContextCompactor(max_total_chars=10_000, compaction_interval=1)
        outputs = OrderedDict([("t1", "small output")])
        result = compactor.compact(outputs)
        assert result.compacted_keys == []
        assert result.total_before == result.total_after

    def test_truncate_preserves_head_and_tail(self):
        """truncate 策略的截断包含头尾内容和标记信息。"""
        compactor = ContextCompactor(
            max_total_chars=100,
            max_single_chars=20,
            strategy=CompactStrategy.TRUNCATE,
            compaction_interval=1,
        )
        text = "ABCDEFGHIJ" * 10  # 100 字符，恰好等于 max_total 但需要超过
        # 加一个短 key 使总量超过阈值
        outputs = OrderedDict([("t1", text), ("t2", "extra")])
        result = compactor.compact(outputs)
        compressed = outputs["t1"]
        # 应包含压缩标记
        assert "压缩" in compressed
        # 应包含头部和尾部内容
        assert compressed.startswith("ABCDEFGHIJ")
