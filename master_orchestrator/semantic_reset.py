"""语义重置协议：根据失败模式动态修正任务 prompt，实现智能重试。"""

from __future__ import annotations

from typing import TYPE_CHECKING

from master_orchestrator.error_classifier import classify_error
from master_orchestrator.model import ErrorCategory, TaskStatus

if TYPE_CHECKING:
    from master_orchestrator.model import TaskNode, TaskResult
    from master_orchestrator.store import Store


class SemanticResetProtocol:
    """语义重置协议：分析失败历史，动态修正 prompt 以提高重试成功率。

    核心策略：
    - 超时错误 → 精简指令，减少任务范围
    - 逻辑错误 → 添加约束条件，明确边界
    - 格式错误 → 强化输出格式要求，提供示例
    """

    def build_reset_prompt(
        self,
        task_node: TaskNode,
        failure_history: list[TaskResult],
        original_prompt: str,
    ) -> str:
        """根据失败模式动态修正 prompt。

        Args:
            task_node: 任务节点配置
            failure_history: 失败历史记录列表
            original_prompt: 原始 prompt 模板

        Returns:
            str: 修正后的 prompt
        """
        if not failure_history:
            return original_prompt

        # 分析失败模式
        error_patterns = self._analyze_failure_patterns(failure_history)

        # 根据主要失败模式修正 prompt
        if error_patterns['timeout_count'] > 0:
            return self._build_timeout_reset_prompt(original_prompt, error_patterns)
        elif error_patterns['format_error_count'] > 0:
            return self._build_format_reset_prompt(original_prompt, task_node, error_patterns)
        elif error_patterns['logic_error_count'] > 0:
            return self._build_logic_reset_prompt(original_prompt, error_patterns)
        else:
            # 通用重置：添加失败上下文
            return self._build_generic_reset_prompt(original_prompt, failure_history)

    def get_known_good_state(
        self,
        task_id: str,
        store: Store,
        run_id: str,
    ) -> dict | None:
        """从 store 获取最近一次成功的输出。

        Args:
            task_id: 任务 ID
            store: 状态存储实例
            run_id: 当前运行 ID

        Returns:
            dict | None: 成功输出的 parsed_output，如果没有则返回 None
        """
        # 查询该任务在当前 run 中的所有结果
        result = store.get_task_result(run_id, task_id)

        # 如果当前 run 有成功记录，返回
        if result and result.status == TaskStatus.SUCCESS and result.parsed_output:
            return result.parsed_output

        # 否则查询历史 run 中的成功记录
        # 注意：这需要跨 run 查询，当前 Store 接口不支持
        # 这里返回 None，表示没有已知的良好状态
        return None

    def should_reset(
        self,
        failure_count: int,
        error_pattern: str,
    ) -> bool:
        """判断是否需要语义重置而非简单重试。

        Args:
            failure_count: 失败次数
            error_pattern: 错误模式（从 error_msg 中提取的关键特征）

        Returns:
            bool: True 表示需要语义重置，False 表示简单重试即可

        决策规则：
        - 失败次数 >= 2 且错误模式相同 → 需要语义重置
        - 失败次数 >= 3 → 强制语义重置
        - 错误类型为 NON_RETRYABLE → 需要语义重置
        """
        # 失败 3 次以上，强制语义重置
        if failure_count >= 3:
            return True

        # 失败 2 次且错误模式相同，需要语义重置
        if failure_count >= 2:
            # 分类错误
            category = classify_error(error_pattern)
            # 如果是不可重试错误，需要语义重置
            if category == ErrorCategory.NON_RETRYABLE:
                return True

        # 其他情况，简单重试
        return False

    # ── 私有辅助方法 ──

    def _analyze_failure_patterns(
        self,
        failure_history: list[TaskResult],
    ) -> dict:
        """分析失败历史，统计各类错误模式。

        Returns:
            dict: 包含各类错误计数和错误消息列表
        """
        patterns = {
            'timeout_count': 0,
            'format_error_count': 0,
            'logic_error_count': 0,
            'other_count': 0,
            'error_messages': [],
        }

        for result in failure_history:
            if not result.error:
                continue

            error_msg = result.error.lower()
            patterns['error_messages'].append(result.error)

            # 超时错误
            if 'timeout' in error_msg or 'timed out' in error_msg:
                patterns['timeout_count'] += 1
            # 格式错误（JSON、输出格式等）
            elif any(kw in error_msg for kw in ['json', 'format', 'parse', 'invalid', 'schema']):
                patterns['format_error_count'] += 1
            # 逻辑错误（断言失败、验证失败等）
            elif any(kw in error_msg for kw in ['assert', 'validation', 'constraint', 'logic']):
                patterns['logic_error_count'] += 1
            else:
                patterns['other_count'] += 1

        return patterns

    def _build_timeout_reset_prompt(
        self,
        original_prompt: str,
        error_patterns: dict,
    ) -> str:
        """构建超时重置 prompt：精简指令，减少任务范围。"""
        reset_instructions = """
⚠️ 前次执行超时，请精简任务范围：
1. 优先完成核心功能，暂时跳过次要细节
2. 减少输出内容，只返回关键信息
3. 避免深度递归或大规模遍历
4. 如果任务可分解，先完成第一阶段

"""
        return reset_instructions + original_prompt

    def _build_format_reset_prompt(
        self,
        original_prompt: str,
        task_node: TaskNode,
        error_patterns: dict,
    ) -> str:
        """构建格式重置 prompt：强化输出格式要求。"""
        format_instructions = """
⚠️ 前次执行输出格式错误，请严格遵守以下格式要求：
1. 输出必须是有效的 JSON 格式
2. 不要在 JSON 前后添加任何说明文字或 markdown 标记
3. 确保所有字符串正确转义（引号、换行符等）
4. 使用双引号而非单引号

"""
        # 如果有 output_schema，添加 schema 示例
        if task_node.output_schema:
            format_instructions += f"预期输出结构：\n```json\n{task_node.output_schema}\n```\n\n"

        return format_instructions + original_prompt

    def _build_logic_reset_prompt(
        self,
        original_prompt: str,
        error_patterns: dict,
    ) -> str:
        """构建逻辑重置 prompt：添加约束条件，明确边界。"""
        logic_instructions = """
⚠️ 前次执行逻辑错误，请注意以下约束：
1. 仔细检查所有边界条件（空值、零值、边界索引等）
2. 确保所有假设都有明确验证
3. 对于不确定的情况，采用保守策略
4. 添加必要的错误处理和防御性检查

"""
        # 附加历史错误信息作为参考
        if error_patterns['error_messages']:
            logic_instructions += "历史错误参考：\n"
            for i, msg in enumerate(error_patterns['error_messages'][-3:], 1):  # 最多显示最近 3 条
                logic_instructions += f"{i}. {msg[:200]}\n"  # 截断过长的错误消息
            logic_instructions += "\n"

        return logic_instructions + original_prompt

    def _build_generic_reset_prompt(
        self,
        original_prompt: str,
        failure_history: list[TaskResult],
    ) -> str:
        """构建通用重置 prompt：添加失败上下文。"""
        reset_instructions = f"""
⚠️ 前次执行失败（共 {len(failure_history)} 次尝试），请重新审视任务：
1. 检查任务目标是否明确
2. 确认所有依赖条件是否满足
3. 验证输入数据是否有效
4. 考虑是否需要调整实现策略

"""
        # 附加最近一次的错误信息
        if failure_history and failure_history[-1].error:
            last_error = failure_history[-1].error[:300]  # 截断过长的错误
            reset_instructions += f"最近错误：{last_error}\n\n"

        return reset_instructions + original_prompt
