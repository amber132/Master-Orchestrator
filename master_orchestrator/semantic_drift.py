"""
语义漂移检测器

检测任务输出是否偏离原始提示的语义意图。
使用关键词重叠率和 Jaccard 相似度量化语义距离。
"""

import re
from dataclasses import dataclass
from typing import Set, List


@dataclass
class DriftResult:
    """语义漂移检测结果"""
    task_id: str
    similarity: float  # 相似度分数 [0, 1]
    drifted: bool      # 是否发生漂移
    detail: str        # 详细说明
    blocking: bool = False
    severity: str = "info"


class SemanticDriftDetector:
    """
    语义漂移检测器

    使用纯 Python 实现，不依赖外部 embedding 库。
    通过关键词重叠率和 Jaccard 相似度量化语义距离。
    """

    # 常见英文停用词
    STOP_WORDS = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
        'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the',
        'to', 'was', 'will', 'with', 'this', 'but', 'they', 'have', 'had',
        'what', 'when', 'where', 'who', 'which', 'why', 'how', 'all', 'each',
        'every', 'both', 'few', 'more', 'most', 'other', 'some', 'such',
        'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too',
        'very', 'can', 'just', 'should', 'now'
    }

    def __init__(self):
        """初始化检测器"""
        pass

    def _preprocess_text(self, text: str) -> str:
        """
        预处理文本

        Args:
            text: 原始文本

        Returns:
            预处理后的文本（小写、去除特殊字符）
        """
        # 转小写
        text = text.lower()
        # 保留字母、数字、中文字符、空格
        text = re.sub(r'[^a-z0-9\u4e00-\u9fff\s]', ' ', text)
        # 合并多个空格
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _extract_keywords(self, text: str, min_length: int = 3) -> Set[str]:
        """
        提取关键词

        Args:
            text: 文本
            min_length: 最小词长（英文）

        Returns:
            关键词集合
        """
        processed = self._preprocess_text(text)

        # 提取英文词
        english_words = re.findall(r'[a-z0-9]+', processed)
        keywords = {
            word for word in english_words
            if word not in self.STOP_WORDS and len(word) >= min_length
        }

        # 提取中文词组（连续中文字符，至少2个字）
        chinese_phrases = re.findall(r'[\u4e00-\u9fff]{2,}', processed)
        keywords.update(chinese_phrases)

        return keywords

    def _calculate_jaccard_similarity(self, set1: Set[str], set2: Set[str]) -> float:
        """
        计算 Jaccard 相似度

        Args:
            set1: 集合1
            set2: 集合2

        Returns:
            Jaccard 相似度 [0, 1]
        """
        if not set1 and not set2:
            return 1.0

        if not set1 or not set2:
            return 0.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    def _calculate_keyword_overlap(self, set1: Set[str], set2: Set[str]) -> float:
        """
        计算关键词重叠率

        Args:
            set1: 集合1
            set2: 集合2

        Returns:
            重叠率 [0, 1]
        """
        if not set1 and not set2:
            return 1.0

        if not set1 or not set2:
            return 0.0

        intersection = len(set1 & set2)
        total = len(set1) + len(set2)

        return (2 * intersection) / total if total > 0 else 0.0

    def _calculate_similarity(self, prompt_keywords: Set[str], output_keywords: Set[str]) -> float:
        """
        计算综合相似度

        结合 Jaccard 相似度和关键词重叠率。

        Args:
            prompt_keywords: 提示关键词
            output_keywords: 输出关键词

        Returns:
            综合相似度 [0, 1]
        """
        jaccard = self._calculate_jaccard_similarity(prompt_keywords, output_keywords)
        overlap = self._calculate_keyword_overlap(prompt_keywords, output_keywords)

        # 加权平均：Jaccard 60%，重叠率 40%
        similarity = 0.6 * jaccard + 0.4 * overlap

        return similarity

    def detect(
        self,
        task_id: str,
        original_prompt: str,
        task_output: str,
        threshold: float = 0.15,
        task_tags: List[str] | None = None,
    ) -> DriftResult:
        """
        检测语义漂移

        Args:
            task_id: 任务 ID
            original_prompt: 原始提示
            task_output: 任务输出
            threshold: 漂移阈值（相似度低于此值判定为漂移）

        Returns:
            DriftResult: 检测结果
        """
        # 提取关键词
        prompt_keywords = self._extract_keywords(original_prompt)
        output_keywords = self._extract_keywords(task_output)

        # 计算相似度
        similarity = self._calculate_similarity(prompt_keywords, output_keywords)

        # 判断是否漂移
        drifted = similarity < threshold

        # 生成详细说明
        common_keywords = prompt_keywords & output_keywords
        prompt_only = prompt_keywords - output_keywords
        output_only = output_keywords - prompt_keywords

        detail_parts = [
            f"相似度: {similarity:.3f}",
            f"提示关键词数: {len(prompt_keywords)}",
            f"输出关键词数: {len(output_keywords)}",
            f"共同关键词数: {len(common_keywords)}"
        ]

        if drifted:
            detail_parts.append(f"检测到语义漂移（阈值: {threshold:.3f}）")
            if prompt_only:
                sample = list(prompt_only)[:5]
                detail_parts.append(f"提示中缺失的关键词示例: {', '.join(sample)}")
            if output_only:
                sample = list(output_only)[:5]
                detail_parts.append(f"输出中新增的关键词示例: {', '.join(sample)}")
        else:
            detail_parts.append("语义保持一致")

        detail = "; ".join(detail_parts)

        tags = set(task_tags or [])
        blocking = drifted and (
            "drift_blocking" in tags
            or "phase_scope" in tags
            or task_id.startswith("scope_")
        )
        severity = "critical" if blocking else ("warning" if drifted else "info")

        return DriftResult(
            task_id=task_id,
            similarity=similarity,
            drifted=drifted,
            detail=detail,
            blocking=blocking,
            severity=severity,
        )
