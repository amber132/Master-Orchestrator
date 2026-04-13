"""上下文摘要器：将长任务输出压缩为结构化摘要，供下游任务使用。"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from master_orchestrator.model import TaskResult, TaskStatus


class HierarchicalSummarizer:
    """层次化摘要器：提取关键信息，压缩长输出为精简上下文。"""

    def __init__(self) -> None:
        # 文件路径模式（支持 Windows 和 Unix 路径）
        self.file_pattern = re.compile(
            r'(?:^|\s)(?:[A-Za-z]:[/\\]|\.{0,2}/|/)?'
            r'(?:[\w\-\.]+[/\\])*[\w\-\.]+\.\w{1,10}(?:\:\d+)?',
            re.MULTILINE
        )
        # 错误关键词
        self.error_keywords = [
            'error', 'exception', 'failed', 'failure', 'traceback',
            'fatal', 'critical', 'warning', 'invalid', 'cannot', 'unable'
        ]
        # 结论性关键词
        self.conclusion_keywords = [
            'summary', 'conclusion', 'result', 'completed', 'finished',
            'total', 'found', 'fixed', 'created', 'updated', 'deleted'
        ]

    def summarize(self, task_output: str, max_chars: int = 2000) -> str:
        """
        将长任务输出压缩为结构化摘要。

        提取策略：
        1. 错误信息（最高优先级）
        2. 修改的文件列表
        3. 关键结论性语句
        4. 如果仍超长，截断并添加省略标记

        Args:
            task_output: 原始任务输出
            max_chars: 最大字符数

        Returns:
            压缩后的摘要字符串
        """
        if not task_output:
            return "[空输出]"

        if len(task_output) <= max_chars:
            return task_output

        # 分行处理
        lines = task_output.split('\n')

        # 1. 提取错误信息
        error_lines = self._extract_error_lines(lines)

        # 2. 提取文件列表
        files = self._extract_files(task_output)

        # 3. 提取结论性语句
        conclusion_lines = self._extract_conclusion_lines(lines)

        # 4. 组装摘要
        summary_parts = []

        if error_lines:
            summary_parts.append("【错误信息】")
            summary_parts.extend(error_lines[:10])  # 最多10行错误

        if files:
            summary_parts.append("\n【修改文件】")
            summary_parts.extend(files[:20])  # 最多20个文件

        if conclusion_lines:
            summary_parts.append("\n【关键结论】")
            summary_parts.extend(conclusion_lines[:10])  # 最多10行结论

        summary = '\n'.join(summary_parts)

        # 5. 如果仍超长，截断
        if len(summary) > max_chars:
            summary = summary[:max_chars - 50] + "\n\n[... 输出过长，已截断 ...]"

        return summary

    def build_downstream_context(
        self,
        upstream_results: dict[str, TaskResult],
        max_total_chars: int = 8000
    ) -> str:
        """
        将多个上游任务结果合并为下游可用的精简上下文。

        优先级策略：
        1. 失败的任务（需要知道失败原因）
        2. 有错误的任务（可能影响下游）
        3. 成功的任务（提供正常上下文）

        Args:
            upstream_results: 上游任务结果字典 {task_id: TaskResult}
            max_total_chars: 总字符数上限

        Returns:
            合并后的精简上下文字符串
        """
        if not upstream_results:
            return "[无上游任务]"

        # 按优先级分组
        failed_tasks = []
        error_tasks = []
        success_tasks = []

        for task_id, result in upstream_results.items():
            if result.status.value == 'failed':
                failed_tasks.append((task_id, result))
            elif result.error:
                error_tasks.append((task_id, result))
            else:
                success_tasks.append((task_id, result))

        # 计算每个任务的配额
        total_tasks = len(upstream_results)
        base_quota = max_total_chars // total_tasks if total_tasks > 0 else max_total_chars

        # 失败任务获得更多配额
        failed_quota = min(base_quota * 2, 3000)
        error_quota = min(base_quota * 1.5, 2000)
        success_quota = base_quota

        context_parts = []
        remaining_chars = max_total_chars

        # 1. 处理失败任务
        for task_id, result in failed_tasks:
            if remaining_chars <= 0:
                break
            quota = min(failed_quota, remaining_chars)
            summary = self._summarize_task_result(task_id, result, quota)
            context_parts.append(summary)
            remaining_chars -= len(summary)

        # 2. 处理有错误的任务
        for task_id, result in error_tasks:
            if remaining_chars <= 0:
                break
            quota = min(error_quota, remaining_chars)
            summary = self._summarize_task_result(task_id, result, quota)
            context_parts.append(summary)
            remaining_chars -= len(summary)

        # 3. 处理成功任务
        for task_id, result in success_tasks:
            if remaining_chars <= 0:
                break
            quota = min(success_quota, remaining_chars)
            summary = self._summarize_task_result(task_id, result, quota)
            context_parts.append(summary)
            remaining_chars -= len(summary)

        # 组装最终上下文
        context = "\n\n" + "="*60 + "\n\n".join(context_parts)

        # 最终截断保护
        if len(context) > max_total_chars:
            context = context[:max_total_chars - 50] + "\n\n[... 上下文过长，已截断 ...]"

        return context

    def _summarize_task_result(
        self,
        task_id: str,
        result: TaskResult,
        max_chars: int
    ) -> str:
        """为单个任务结果生成摘要。"""
        header = f"【任务 {task_id}】状态: {result.status.value}"

        if result.error:
            header += f" | 错误: {result.error[:200]}"

        if result.duration_seconds > 0:
            header += f" | 耗时: {result.duration_seconds:.1f}s"

        # 摘要输出
        output_summary = ""
        if result.output:
            # 为输出留出空间（减去 header 长度）
            output_quota = max_chars - len(header) - 50
            if output_quota > 0:
                output_summary = self.summarize(result.output, output_quota)

        return f"{header}\n{output_summary}"

    def _extract_error_lines(self, lines: list[str]) -> list[str]:
        """提取包含错误关键词的行。"""
        error_lines = []
        for line in lines:
            line_lower = line.lower()
            if any(kw in line_lower for kw in self.error_keywords):
                error_lines.append(line.strip())
        return error_lines

    def _extract_files(self, text: str) -> list[str]:
        """提取文件路径。"""
        matches = self.file_pattern.findall(text)
        # 去重并保持顺序
        seen = set()
        files = []
        for match in matches:
            match = match.strip()
            if match and match not in seen:
                seen.add(match)
                files.append(match)
        return files

    def _extract_conclusion_lines(self, lines: list[str]) -> list[str]:
        """提取结论性语句。"""
        conclusion_lines = []
        for line in lines:
            line_lower = line.lower()
            # 结论性语句通常在行首或包含特定关键词
            if any(kw in line_lower for kw in self.conclusion_keywords):
                conclusion_lines.append(line.strip())
            # 或者是短句（可能是总结）
            elif len(line.strip()) < 100 and line.strip().endswith(('.', '!', '。', '！')):
                conclusion_lines.append(line.strip())
        return conclusion_lines
