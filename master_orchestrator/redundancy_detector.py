"""任务冗余检测器：识别语义重叠的任务并生成合并建议"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from master_orchestrator.model import TaskNode


@dataclass
class RedundancyGroup:
    """冗余任务组：包含相似度超过阈值的任务集合"""
    task_ids: list[str]
    similarity_score: float


@dataclass
class MergeSuggestion:
    """合并建议：为冗余任务组生成的合并方案"""
    task_ids: list[str]
    merged_prompt: str


class RedundancyDetector:
    """
    任务冗余检测器

    使用词袋模型 + Jaccard 相似度检测 prompt 语义重叠的任务。
    """

    # 常见停用词（英文）
    STOP_WORDS = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
        'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
        'to', 'was', 'will', 'with', 'this', 'these', 'those', 'or', 'but',
        'if', 'then', 'else', 'when', 'where', 'why', 'how', 'all', 'each',
        'every', 'both', 'few', 'more', 'most', 'other', 'some', 'such',
    }

    def __init__(self, remove_stopwords: bool = True):
        """
        初始化冗余检测器

        Args:
            remove_stopwords: 是否移除停用词（默认 True）
        """
        self.remove_stopwords = remove_stopwords

    def _tokenize(self, text: str) -> set[str]:
        """
        将文本分词并转为词袋（集合）

        Args:
            text: 输入文本

        Returns:
            词的集合（小写，去除标点）
        """
        # 转小写，提取单词（字母数字组合）
        words = re.findall(r'\b\w+\b', text.lower())

        # 去除停用词
        if self.remove_stopwords:
            words = [w for w in words if w not in self.STOP_WORDS]

        return set(words)

    def _jaccard_similarity(self, set_a: set[str], set_b: set[str]) -> float:
        """
        计算两个集合的 Jaccard 相似度

        Args:
            set_a: 集合 A
            set_b: 集合 B

        Returns:
            相似度分数 [0, 1]，1 表示完全相同
        """
        if not set_a and not set_b:
            return 1.0  # 两个空集合视为相同

        intersection = set_a & set_b
        union = set_a | set_b

        if not union:
            return 0.0

        return len(intersection) / len(union)

    def detect(
        self,
        tasks: dict[str, TaskNode],
        threshold: float = 0.85,
    ) -> list[RedundancyGroup]:
        """
        检测 prompt 语义重叠的任务组

        Args:
            tasks: 任务字典 {task_id: TaskNode}
            threshold: 相似度阈值（默认 0.6），超过此值视为冗余

        Returns:
            冗余任务组列表
        """
        if not tasks:
            return []

        # 预处理：为每个任务生成词袋
        task_ids = list(tasks.keys())
        token_sets = {
            tid: self._tokenize(tasks[tid].prompt_template)
            for tid in task_ids
        }

        # 存储已分组的任务（避免重复）
        grouped_tasks: set[str] = set()
        redundancy_groups: list[RedundancyGroup] = []

        # 遍历所有任务对，计算相似度
        for i, tid_a in enumerate(task_ids):
            if tid_a in grouped_tasks:
                continue

            # 当前组：以 tid_a 为起点
            current_group = [tid_a]
            similarities = []

            for tid_b in task_ids[i + 1:]:
                if tid_b in grouped_tasks:
                    continue

                # 计算 Jaccard 相似度
                similarity = self._jaccard_similarity(
                    token_sets[tid_a],
                    token_sets[tid_b],
                )

                # 如果超过阈值，加入当前组
                if similarity >= threshold:
                    current_group.append(tid_b)
                    similarities.append(similarity)

            # 如果找到冗余任务（组大小 > 1），记录
            if len(current_group) > 1:
                # 计算组内平均相似度
                avg_similarity = sum(similarities) / len(similarities) if similarities else threshold

                redundancy_groups.append(
                    RedundancyGroup(
                        task_ids=current_group,
                        similarity_score=avg_similarity,
                    )
                )

                # 标记已分组
                grouped_tasks.update(current_group)

        return redundancy_groups

    def merge_suggestions(
        self,
        groups: list[RedundancyGroup],
        tasks: dict[str, TaskNode],
    ) -> list[MergeSuggestion]:
        """
        为冗余任务组生成合并建议

        Args:
            groups: 冗余任务组列表
            tasks: 任务字典 {task_id: TaskNode}

        Returns:
            合并建议列表
        """
        suggestions = []

        for group in groups:
            # 收集组内所有 prompt
            prompts = [tasks[tid].prompt_template for tid in group.task_ids]

            # 合并策略：选择最长的 prompt 作为基础
            # （假设最长的 prompt 包含最多信息）
            merged_prompt = max(prompts, key=len)

            # 可选：提取所有 prompt 的关键词并合并
            # 这里简化处理，直接使用最长的 prompt

            suggestions.append(
                MergeSuggestion(
                    task_ids=group.task_ids,
                    merged_prompt=merged_prompt,
                )
            )

        return suggestions
