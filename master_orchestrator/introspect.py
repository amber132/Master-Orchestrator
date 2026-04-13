"""内省引擎：收集运行历史数据 + 源码结构，调用 Claude 分析生成改进提案。"""

from __future__ import annotations

import ast
import json
import logging
import re
import uuid
from pathlib import Path
from typing import Any

from .auto_model import (
    ImprovementPriority,
    ImprovementProposal,
    ImprovementSource,
    load_goal_state,
)
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig
from .json_utils import repair_truncated_json, robust_parse_json
from .model import TaskNode, TaskStatus
from .store import Store

logger = logging.getLogger(__name__)

# 最多扫描的 goal_state.json 文件数
_MAX_GOAL_STATES = 5

_ANALYSIS_SYSTEM_PROMPT = """\
你是一位资深软件架构分析师。你的任务是分析一个 DAG 编排器项目的运行数据和源码结构，
找出可以改进的地方并生成具体的改进提案。

【极其重要】禁止使用 Write 工具！禁止使用 Task 工具！不要启动子代理！直接在对话中输出 JSON 数组即可。不要写入任何文件。

分析维度：
1. 性能瓶颈（基于 duration, timeout_count, max_duration_seconds 数据）
2. 可靠性问题（基于 failure_categories, regression_count, deterioration_levels 数据）
3. 收敛效率（基于 score_trend, total_iterations, phases 数据）
4. 成本优化（基于 cost_usd, avg_cost_per_task, model 选择数据）
5. 代码复杂度热点（基于函数行数、try_except_count、long_functions preview）

输出要求：
- 直接输出一个 JSON 数组，不要使用 Write 工具
- 每条包含以下字段：
  title, description, rationale, priority (critical/high/medium/low),
  affected_files (文件名列表), complexity (small/medium/large), evidence (数据/引用)
- 只输出 JSON 数组，不要包含其他文本
- 每条提案必须具体可执行，不要泛泛而谈
- evidence 必须引用具体数据字段和数值（如 "timeout_count=3", "函数 _execute_loop 有 120 行"）
- affected_files 必须从源码结构中选择实际存在的文件名
- 优先级判定标准：
  - critical: 影响正确性或数据安全
  - high: 影响可靠性或显著影响效率
  - medium: 改善代码质量或用户体验
  - low: 优化建议，非必须

示例输出：
[
  {
    "title": "拆分 _execute_loop 长函数",
    "description": "将 orchestrator.py 中 120 行的 _execute_loop 拆分为任务调度、结果收集、状态更新三个子函数",
    "rationale": "长函数难以测试和维护，且包含 5 个 try-except 块，错误处理逻辑交织",
    "priority": "medium",
    "affected_files": ["orchestrator.py"],
    "complexity": "medium",
    "evidence": "_execute_loop: 120 行, try_except_count=5, 超过 50 行阈值"
  }
]
"""



def _safe_mtime(p):
    """安全获取文件修改时间，处理文件不存在或权限错误的情况。"""
    try:
        return p.stat().st_mtime
    except (FileNotFoundError, PermissionError, OSError):
        return 0


class IntrospectionEngine:
    """内省引擎：从运行历史和源码中发现改进机会。"""

    def __init__(
        self,
        claude_config: ClaudeConfig,
        limits_config: LimitsConfig,
        budget_tracker: BudgetTracker | None,
        orchestrator_dir: str | Path,
        working_dir: str | Path,
        store: Store | None = None,
    ):
        self._claude_config = claude_config
        self._limits = limits_config
        self._budget = budget_tracker
        self._orchestrator_dir = Path(orchestrator_dir)
        self._working_dir = Path(working_dir)
        self._store = store

    def analyze(self) -> list[ImprovementProposal]:
        """收集数据并调用 Claude 生成改进提案。"""
        logger.info("内省引擎启动：收集运行数据和源码结构")

        # 收集三个来源的数据
        goal_state_data = self._collect_goal_states()
        store_data = self._collect_store_stats()
        source_structure = self._collect_source_structure()

        # 组装分析 prompt
        prompt = self._build_analysis_prompt(goal_state_data, store_data, source_structure)

        # 调用 Claude 分析
        task_node = TaskNode(
            id="_introspect_analyze",
            prompt_template=prompt,
            timeout=1200,
            model="opus",
            output_format="text",
            system_prompt=_ANALYSIS_SYSTEM_PROMPT,
            allowed_tools=["Read", "Glob", "Grep"],
        )

        result = run_claude_task(
            task=task_node,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits,
            budget_tracker=self._budget,
            working_dir=str(self._working_dir),
        )

        if result.status != TaskStatus.SUCCESS:
            logger.error("内省分析失败: %s", result.error)
            return []

        # 解析提案
        return self._parse_proposals(result.output or "")

    def _collect_goal_states(self) -> list[dict]:
        """扫描 goal_state.json 文件，提取运行指标。"""
        summaries: list[dict] = []

        # 在工作目录下搜索 goal_state.json
        candidates = sorted(
            self._working_dir.rglob("goal_state.json"),
            key=_safe_mtime,
            reverse=True,
        )[:_MAX_GOAL_STATES]

        for gs_path in candidates:
            try:
                state = load_goal_state(gs_path)
                # 提取关键指标
                phase_count = len(state.phases)
                total_iters = state.total_iterations
                cost = state.total_cost_usd

                # 迭代历史分析
                scores = [r.score for r in state.iteration_history]
                failure_cats = {}
                regression_count = 0
                deterioration_levels = {}
                for r in state.iteration_history:
                    if r.failure_category:
                        failure_cats[r.failure_category] = failure_cats.get(r.failure_category, 0) + 1
                    if r.regression_detected:
                        regression_count += 1
                    # 空字符串统一视为 "none" 级别，确保 deterioration_levels 始终被填充
                    level = r.deterioration_level or "none"
                    deterioration_levels[level] = deterioration_levels.get(level, 0) + 1

                summaries.append({
                    "file": str(gs_path),
                    "goal": state.goal_text[:200],
                    "status": state.status.value,
                    "phases": phase_count,
                    "total_iterations": total_iters,
                    "cost_usd": cost,
                    "score_trend": scores,
                    "failure_categories": failure_cats,
                    "regression_count": regression_count,
                    "deterioration_levels": deterioration_levels,
                })
            except Exception as e:
                logger.warning("读取 %s 失败: %s", gs_path, e)

        logger.info("收集了 %d 个 goal_state.json 的运行数据", len(summaries))
        return summaries

    def _collect_store_stats(self) -> dict:
        """从 Store SQLite 提取任务统计。"""
        if not self._store:
            return {}

        try:
            # 获取最近的运行记录
            latest = self._store.get_latest_run()
            if not latest:
                return {}

            results = self._store.get_all_task_results(latest.run_id)
            if not results:
                return {}

            total = len(results)
            success = sum(1 for r in results.values() if r.status == TaskStatus.SUCCESS)
            failed = sum(1 for r in results.values() if r.status == TaskStatus.FAILED)
            durations = [r.duration_seconds for r in results.values() if r.duration_seconds > 0]
            costs = [r.cost_usd for r in results.values() if r.cost_usd > 0]

            # 超时检测（duration 接近或超过默认 timeout）
            timeout_count = sum(1 for d in durations if d >= 550)  # 接近 600s 默认超时

            stats = {
                "latest_run_id": latest.run_id,
                "latest_run_status": latest.status.value,
                "total_tasks": total,
                "success_count": success,
                "failed_count": failed,
                "success_rate": success / total if total > 0 else 0,
                "avg_duration_seconds": sum(durations) / len(durations) if durations else 0,
                "max_duration_seconds": max(durations) if durations else 0,
                "timeout_count": timeout_count,
                "total_cost_usd": sum(costs),
                "avg_cost_per_task": sum(costs) / len(costs) if costs else 0,
            }
            logger.info("Store 统计: %d 任务, 成功率 %.1f%%", total, stats["success_rate"] * 100)
            return stats
        except Exception as e:
            logger.warning("Store 统计收集失败: %s", e)
            return {}

    def _collect_source_structure(self) -> list[dict]:
        """扫描编排器源码，用 ast 模块提取丰富的结构信息。"""
        structure: list[dict] = []

        # 递归扫描所有 .py 文件，排除特定目录
        exclude_dirs = {'__pycache__', '.venv', '.git', '.idea', 'venv'}
        for py_file in sorted(self._orchestrator_dir.rglob("*.py")):
            # 过滤掉不需要的目录
            if any(part in exclude_dirs or part.startswith('test-') for part in py_file.parts):
                continue
            try:
                content = py_file.read_text(encoding="utf-8")
                lines = content.split("\n")
                line_count = len(lines)

                # 用 ast 解析
                try:
                    tree = ast.parse(content, filename=py_file.name)
                except (SyntaxError, ValueError, RecursionError) as e:
                    # ast 解析失败时回退到正则
                    logger.warning("AST 解析 %s 失败: %s，回退到正则", py_file.name, e)
                    classes = re.findall(r"^\s*class\s+(\w+)", content, re.MULTILINE)
                    functions = re.findall(r"^\s*def\s+(\w+)", content, re.MULTILINE)
                    structure.append({
                        "file": py_file.name,
                        "lines": line_count,
                        "classes": classes,
                        "functions": [{"name": f} for f in functions[:30]],
                        "imports": [],
                        "long_functions": [],
                    })
                    continue

                # 一次遍历 AST 树，提取所有信息
                imports: list[str] = []
                classes: list[str] = []
                func_infos: list[dict] = []
                long_functions: list[dict] = []

                for node in ast.walk(tree):
                    # 提取 import
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            imports.append(alias.name)
                    elif isinstance(node, ast.ImportFrom):
                        if node.module:
                            imports.append(node.module)
                    # 提取类名
                    elif isinstance(node, ast.ClassDef):
                        classes.append(node.name)
                    # 提取函数/方法详细信息
                    elif not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue

                    # 函数参数签名
                    args_list: list[str] = []
                    for arg in node.args.args:
                        arg_str = arg.arg
                        if arg.annotation:
                            try:
                                arg_str += ": " + ast.unparse(arg.annotation)
                            except Exception:
                                pass
                        args_list.append(arg_str)

                    # 函数行数
                    func_lines = 0
                    if hasattr(node, "end_lineno") and node.end_lineno and node.lineno:
                        func_lines = node.end_lineno - node.lineno + 1

                    # 是否有 docstring
                    has_docstring = (
                        node.body
                        and isinstance(node.body[0], ast.Expr)
                        and isinstance(node.body[0].value, ast.Constant)
                        and isinstance(node.body[0].value.value, str)
                    )

                    # 统计 try-except 块数量
                    try_except_count = sum(
                        1 for child in ast.walk(node) if isinstance(child, ast.Try)
                    )

                    func_info: dict = {
                        "name": node.name,
                        "args": args_list,
                        "lines": func_lines,
                        "has_docstring": bool(has_docstring),
                        "try_except_count": try_except_count,
                    }
                    func_infos.append(func_info)

                    # >50 行的函数提取前 30 行代码作为 preview
                    if func_lines > 50:
                        start = node.lineno - 1  # 0-indexed
                        end = min(start + 30, len(lines))
                        preview = "\n".join(lines[start:end])
                        long_functions.append({
                            "name": node.name,
                            "lines": func_lines,
                            "try_except_count": try_except_count,
                            "preview": preview,
                        })

                structure.append({
                    "file": py_file.name,
                    "lines": line_count,
                    "classes": classes,
                    "functions": func_infos[:30],  # 限制数量
                    "imports": imports,
                    "long_functions": long_functions,
                })
            except Exception as e:
                logger.warning("读取 %s 失败: %s", py_file, e)

        logger.info("扫描了 %d 个源码文件", len(structure))
        return structure

    def _build_analysis_prompt(
        self,
        goal_states: list[dict],
        store_stats: dict,
        source_structure: list[dict],
    ) -> str:
        """组装发送给 Claude 的分析 prompt。

        源码结构部分只传 long_functions 的 preview（控制 prompt 长度），
        并提供文件名列表供 affected_files 引用。
        """
        sections: list[str] = []

        sections.append("# 编排器自我分析请求\n")
        sections.append("请分析以下运行数据和源码结构，生成具体的改进提案。\n")

        # 运行历史
        if goal_states:
            sections.append("## 运行历史数据\n")
            sections.append(json.dumps(goal_states, ensure_ascii=False, indent=2))

        # Store 统计
        if store_stats:
            sections.append("\n## 任务执行统计\n")
            sections.append(json.dumps(store_stats, ensure_ascii=False, indent=2))

        # 源码结构：精简版（不含完整函数列表，只含摘要和长函数 preview）
        if source_structure:
            # 文件名列表（供 affected_files 引用）
            file_names = [s["file"] for s in source_structure]
            sections.append("\n## 可用文件名列表（affected_files 必须从此列表选择）\n")
            sections.append(json.dumps(file_names, ensure_ascii=False))

            # 每个文件的摘要 + 长函数 preview
            sections.append("\n## 源码结构摘要\n")
            for s in source_structure:
                summary: dict = {
                    "file": s["file"],
                    "lines": s["lines"],
                    "classes": s.get("classes", []),
                    "function_count": len(s.get("functions", [])),
                    "imports": s.get("imports", []),
                }
                # 只传函数名和行数的简要列表
                funcs = s.get("functions", [])
                if funcs and isinstance(funcs[0], dict):
                    summary["functions"] = [
                        {"name": f["name"], "lines": f.get("lines", 0), "try_except_count": f.get("try_except_count", 0)}
                        for f in funcs
                    ]
                sections.append(json.dumps(summary, ensure_ascii=False))

            # 长函数 preview（代码复杂度热点的关键数据）
            long_funcs = []
            for s in source_structure:
                for lf in s.get("long_functions", []):
                    long_funcs.append({
                        "file": s["file"],
                        "name": lf["name"],
                        "lines": lf["lines"],
                        "try_except_count": lf.get("try_except_count", 0),
                        "preview": lf["preview"],
                    })
            if long_funcs:
                sections.append("\n## 长函数代码预览（>50 行，前 30 行）\n")
                sections.append(json.dumps(long_funcs, ensure_ascii=False, indent=2))

        sections.append(
            "\n## 输出要求\n"
            "输出一个 JSON 数组，每条包含: title, description, rationale, priority, "
            "affected_files, complexity, evidence。\n"
            "evidence 必须引用具体数据字段和数值。\n"
            "affected_files 必须从上方文件名列表中选择。\n"
            "只输出 JSON 数组，第一个字符必须是 [。"
        )

        return "\n".join(sections)

    def _parse_proposals(self, raw_output: str) -> list[ImprovementProposal]:
        """解析 Claude 返回的改进提案 JSON。"""
        try:
            data = robust_parse_json(raw_output)
        except ValueError as e:
            logger.error("解析改进提案失败: %s", e)
            return []

        # 确保是列表
        if isinstance(data, dict):
            data = data.get("proposals", data.get("improvements", [data]))
        if not isinstance(data, list):
            logger.error("改进提案格式错误: 期望数组，得到 %s", type(data).__name__)
            return []

        proposals: list[ImprovementProposal] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            try:
                priority_str = item.get("priority", "medium").lower()
                if priority_str not in ("critical", "high", "medium", "low"):
                    priority_str = "medium"

                proposal = ImprovementProposal(
                    proposal_id=uuid.uuid4().hex[:8],
                    title=item.get("title", "未命名提案"),
                    description=item.get("description", ""),
                    rationale=item.get("rationale", ""),
                    source=ImprovementSource.INTROSPECTION,
                    priority=ImprovementPriority(priority_str),
                    affected_files=item.get("affected_files", []),
                    estimated_complexity=item.get("complexity", "medium"),
                    evidence=item.get("evidence", ""),
                )
                proposals.append(proposal)
            except Exception as e:
                logger.warning("解析单条提案失败: %s", e)

        logger.info("内省引擎生成了 %d 条改进提案", len(proposals))
        return proposals
