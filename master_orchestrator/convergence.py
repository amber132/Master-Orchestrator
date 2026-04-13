"""Convergence detection for the autonomous orchestrator.

Determines when to stop iterating based on:
1. Score threshold reached
2. Total iteration limit
3. Deadline (time limit)
4. Budget exhaustion
5. Score plateau (no improvement over N iterations)
6. Diminishing returns (improvements getting progressively smaller)
7. All phases passed
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import TypedDict

from .auto_model import (
    AutoConfig,
    ConvergenceSignal,
    DeteriorationLevel,
    DeteriorationSignal,
    GoalState,
    GoalStatus,
    Phase,
    PhaseStatus,
    ReviewVerdict,
)

logger = logging.getLogger(__name__)


class RootCauseDiagnosis(TypedDict):
    """从 failure_categories 提取的单条根因诊断数据。"""
    category: str           # 失败类别名称
    count: int              # 出现次数
    severity: str           # 严重程度：critical / major / minor
    related_categories: list[str]  # 同族关联类别


class PlateauSignal(TypedDict):
    """分数平台期信号的数据结构。

    当连续多轮迭代的评分完全相同时，生成此信号，
    包含打破平台期的策略建议。
    """
    consecutive_count: int      # 连续相同分数的轮数
    plateau_score: float        # 重复出现的分数
    strategy_adjustment: dict   # {'action': 'escalate_review', 'suggested_dimensions': [...]}


class ConvergenceDetector:
    """Checks whether the autonomous loop should stop."""

    # 最小总迭代次数：低于此值时 score/plateau/diminishing 相关检查不具备统计意义，不判定收敛。
    # 值为 3 是因为 score_trend 少于 3 个采样点时不具备统计意义，不应判定收敛。
    MIN_ITERATIONS = 3

    def __init__(self, auto_config: AutoConfig):
        self._cfg = auto_config

    def check(self, state: GoalState, current_phase: Phase | None = None) -> ConvergenceSignal:
        """Run all convergence checks. Returns first triggered signal."""
        # 优先检查：空阶段列表 + FAILED 状态，说明分解失败且无法恢复
        empty_signal = self._check_empty_phases(state)
        if empty_signal is not None:
            return empty_signal

        # 总迭代次数（含首次执行）不足时，只做结构性检查（deadline/total_iterations/all_phases_passed），
        # 跳过 score/plateau/diminishing_returns 等基于统计的检查
        skip_statistical = state.total_iterations < self.MIN_ITERATIONS

        checks = [
            self._check_all_phases_passed,
            self._check_deadline,
            self._check_total_iterations,
            self._check_phase_iterations,
        ]

        # 低分提前终止不受 MIN_ITERATIONS 限制（严重情况应立即停止）
        low_signal = self._check_low_score_early_stop(state, current_phase)
        if low_signal.should_stop:
            logger.info("收敛检测触发: %s", low_signal.reason)
            return low_signal

        # 崩塌检测：分数从 >0.8 骤跌至 <0.2，不受 MIN_ITERATIONS 限制
        collapse_signal = self._check_score_collapse(state, current_phase)
        if collapse_signal.should_stop:
            logger.info("收敛检测触发: %s", collapse_signal.reason)
            return collapse_signal

        # 失败模式收敛：检测 critical 级别根因，不受 MIN_ITERATIONS 限制
        failure_signal = self._check_failure_pattern_convergence(state, current_phase)
        if failure_signal.should_stop:
            logger.info("收敛检测触发: %s", failure_signal.reason)
            return failure_signal

        if not skip_statistical:
            # 先检查震荡，若检测到震荡则跳过其他统计检查以防误判收敛
            osc = self._check_oscillation(state, current_phase)
            if osc.reason == "oscillation_detected":
                cv_val = osc.details.get("cv")
                cv_str = f", CV={cv_val:.3f}" if cv_val is not None else ""
                logger.info(
                    "震荡检测: 分数波动剧烈 (std=%.3f%s, 骤降次数=%d)，跳过收敛判定",
                    osc.details["amplitude"], cv_str, osc.details["drop_count"],
                )
            else:
                checks.extend([
                    self._check_score_threshold,
                    self._check_plateau,
                    self._check_diminishing_returns,
                ])
        for check_fn in checks:
            signal = check_fn(state, current_phase)
            if signal.should_stop:
                logger.info("收敛检测触发: %s", signal.reason)
                return signal

        return ConvergenceSignal(should_stop=False, reason="")

    def _check_empty_phases(self, state: GoalState) -> ConvergenceSignal | None:
        """检查阶段列表为空且状态为 FAILED 的情况（分解失败，无法恢复）。"""
        if not state.phases and state.status == GoalStatus.FAILED:
            return ConvergenceSignal(
                should_stop=True,
                reason="empty_phases_no_recovery",
                details={"trigger": "empty_phases_no_recovery"},
            )
        return None

    def _check_all_phases_passed(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        if not state.phases:
            return ConvergenceSignal(should_stop=False, reason="")
        all_done = all(p.status == PhaseStatus.COMPLETED for p in state.phases)
        if all_done:
            return ConvergenceSignal(
                should_stop=True,
                reason="所有阶段已完成",
                details={"trigger": "all_phases_passed"},
            )
        return ConvergenceSignal(should_stop=False, reason="")

    def _check_deadline(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        if datetime.now() >= state.deadline:
            return ConvergenceSignal(
                should_stop=True,
                reason=f"已超过截止时间 ({self._cfg.max_hours}h)",
                details={"trigger": "deadline", "deadline": state.deadline.isoformat()},
            )
        return ConvergenceSignal(should_stop=False, reason="")

    def _check_total_iterations(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        if state.total_iterations >= self._cfg.max_total_iterations:
            return ConvergenceSignal(
                should_stop=True,
                reason=f"全局迭代次数达到上限 ({state.total_iterations}/{self._cfg.max_total_iterations})",
                details={"trigger": "max_iterations", "count": state.total_iterations},
            )
        return ConvergenceSignal(should_stop=False, reason="")

    def _check_phase_iterations(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        if phase and phase.iteration >= phase.max_iterations:
            return ConvergenceSignal(
                should_stop=True,
                reason=f"阶段 '{phase.name}' 迭代次数达到上限 ({phase.max_iterations})",
                details={"trigger": "phase_max_iterations", "phase_id": phase.id},
            )
        return ConvergenceSignal(should_stop=False, reason="")

    def _check_low_score_early_stop(
        self,
        state: GoalState,
        phase: Phase | None,
    ) -> ConvergenceSignal:
        """连续低分提前终止：最近 N 个采样点分数均低于阈值时停止迭代。"""
        if not state.iteration_history:
            return ConvergenceSignal(should_stop=False, reason="")

        threshold = self._cfg.low_score_threshold
        max_consecutive = self._cfg.low_score_max_consecutive

        # 从 iteration_history 尾部扫描连续低分
        consecutive = 0
        for record in reversed(state.iteration_history):
            if record.score <= threshold:
                consecutive += 1
            else:
                break

        if consecutive >= max_consecutive:
            return ConvergenceSignal(
                should_stop=True,
                reason=f"连续 {consecutive} 次迭代分数低于 {threshold}，提前终止",
                details={
                    "trigger": "consecutive_low_scores",
                    "consecutive_count": consecutive,
                    "threshold": threshold,
                    "recent_scores": [r.score for r in state.iteration_history[-max_consecutive * 2:]],
                },
            )

        return ConvergenceSignal(should_stop=False, reason="")

    def _check_score_collapse(
        self,
        state: GoalState,
        phase: Phase | None,
    ) -> ConvergenceSignal:
        """崩塌检测：遍历 score_trend，若任意相邻两次从 >0.8 跌至 <0.2，标记崩塌。

        崩塌意味着某次迭代引入了严重退化，分数断崖式下跌。
        检测到崩塌时应停止迭代并触发回滚，避免在错误基础上继续迭代。
        不受 MIN_ITERATIONS 限制，因为崩塌是灾难性信号。
        """
        if not state.iteration_history:
            return ConvergenceSignal(should_stop=False, reason="")

        scores = [r.score for r in state.iteration_history]

        # 遍历所有相邻分数对，检测 >0.8 → <0.2 的崩塌
        for i in range(len(scores) - 1):
            if scores[i] > 0.8 and scores[i + 1] < 0.2:
                return ConvergenceSignal(
                    should_stop=True,
                    reason=(
                        f"分数崩塌检测: 第 {i} 轮 {scores[i]:.2f} → "
                        f"第 {i + 1} 轮 {scores[i + 1]:.2f}，"
                        f"跌幅 {scores[i] - scores[i + 1]:.2f}"
                    ),
                    details={
                        "trigger": "score_collapse",
                        "is_collapsed": True,
                        "collapse_index": i,
                        "from_score": scores[i],
                        "to_score": scores[i + 1],
                        "drop": scores[i] - scores[i + 1],
                    },
                )

        return ConvergenceSignal(
            should_stop=False,
            reason="",
            details={"is_collapsed": False},
        )

    def _check_score_threshold(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        """检查分数是否达到收敛阈值，且连续达标次数 >= min_convergence_checks。

        仅当最近 N 次迭代的分数都 >= convergence_threshold 时才判定收敛，
        防止单次偶然高分导致假收敛。
        """
        if not phase:
            return ConvergenceSignal(should_stop=False, reason="")

        min_checks = self._cfg.min_convergence_checks
        threshold = self._cfg.convergence_threshold

        # 获取当前阶段的迭代记录
        phase_records = [r for r in state.iteration_history if r.phase_id == phase.id]

        # 至少需要 min_checks 条记录才能判断连续达标
        if len(phase_records) < min_checks:
            return ConvergenceSignal(should_stop=False, reason="")

        # 检查最近 min_checks 次的分数是否都 >= 阈值
        recent_scores = [r.score for r in phase_records[-min_checks:]]
        all_above = all(s >= threshold for s in recent_scores)

        if all_above:
            latest_score = recent_scores[-1]
            return ConvergenceSignal(
                should_stop=True,
                reason=(
                    f"阶段 '{phase.name}' 连续 {min_checks} 次分数达标 "
                    f"(最近 {min_checks} 次: {', '.join(f'{s:.2f}' for s in recent_scores)}, "
                    f"阈值 {threshold})"
                ),
                details={
                    "trigger": "score_threshold",
                    "consecutive_checks": min_checks,
                    "scores": recent_scores,
                    "latest_score": latest_score,
                },
            )
        return ConvergenceSignal(should_stop=False, reason="")

    def _check_oscillation(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        """检测分数震荡：CV > 0.3 视为未收敛，阻止误判收敛。

        优先使用变异系数（CV = std/mean）判断震荡：
        - CV > 0.3 → 震荡（分数相对波动过大，未收敛）
        - CV <= 0.3 且 std > 0.2 → 辅助快速路径：仍视为震荡（绝对波动大）
        - 否则 → 未检测到震荡

        骤降次数和分数区间分布作为附加诊断信息。
        """
        if not phase:
            return ConvergenceSignal(should_stop=False, reason="")

        window = self._cfg.convergence_window
        phase_records = [r for r in state.iteration_history if r.phase_id == phase.id]
        if len(phase_records) < window:
            return ConvergenceSignal(should_stop=False, reason="")

        recent = phase_records[-window:]
        scores = [r.score for r in recent]

        # 计算变异系数（核心指标）
        cv = self._calc_cv(scores)

        # 计算标准差（辅助快速路径）
        mean_score = sum(scores) / len(scores)
        variance = sum((s - mean_score) ** 2 for s in scores) / len(scores)
        std = math.sqrt(variance)

        # 核心判定：CV > 0.3 视为未收敛
        cv_oscillation = cv is not None and cv > 0.3
        # 辅助判定：std > 0.2（当 CV 不可用时回退到绝对阈值）
        std_oscillation = std > 0.2

        if not cv_oscillation and not std_oscillation:
            return ConvergenceSignal(should_stop=False, reason="")

        # 统计骤降次数（相邻分数降幅 > 0.3），作为附加诊断信息
        drop_count = sum(
            1 for i in range(len(scores) - 1)
            if scores[i] - scores[i + 1] > 0.3
        )

        # 分数区间分布，增强诊断能力
        distribution = self.compute_score_distribution(scores)

        # 确定震荡原因标签
        if cv_oscillation:
            oscillation_reason = f"CV={cv:.3f} > 0.3 阈值（分数相对波动过大）"
        else:
            oscillation_reason = f"std={std:.3f} > 0.2 阈值（绝对波动大）"

        return ConvergenceSignal(
            should_stop=False,
            reason="oscillation_detected",
            details={
                "amplitude": std,
                "cv": cv,
                "cv_threshold": 0.3,
                "cv_oscillation": cv_oscillation,
                "oscillation_reason": oscillation_reason,
                "drop_count": drop_count,
                "scores": scores,
                "distribution": distribution,
                "trigger": "oscillation_detected",
            },
        )

    @staticmethod
    def _calc_cv(score_trend: list[float]) -> float | None:
        """计算分数序列的变异系数 CV = std / mean。

        变异系数衡量分数的相对离散程度，不受评分绝对值影响。
        用于区分真实收敛（CV 低，分数稳定）与震荡（CV 高，分数波动大）。

        Args:
            score_trend: 分数序列（按时间顺序排列）。

        Returns:
            float: 变异系数 CV，非负。
            None: 输入为空列表或仅含单个元素时（统计无意义）。
        """
        if len(score_trend) < 2:
            return None

        mean = sum(score_trend) / len(score_trend)
        if mean == 0:
            # 均值为零时 CV 无意义（所有分数均为 0），返回 None 避免除零
            return None

        variance = sum((s - mean) ** 2 for s in score_trend) / len(score_trend)
        std = math.sqrt(variance)
        return std / mean

    @staticmethod
    def compute_score_distribution(score_trend: list[float]) -> dict[str, float]:
        """统计 score_trend 的区间分布，返回各区间占比。

        区间定义：
        - "0.0-0.3": score < 0.3（低分区）
        - "0.3-0.6": 0.3 <= score < 0.6（中低区）
        - "0.6-0.8": 0.6 <= score < 0.8（中高区）
        - "0.8-1.0": 0.8 <= score <= 1.0（达标区）

        Args:
            score_trend: 分数序列（按时间顺序排列）。

        Returns:
            dict: 各区间占比（0.0~1.0），key 为区间字符串。
                  空输入时所有区间返回 0.0。
        """
        bins: dict[str, int] = {
            "0.0-0.3": 0,
            "0.3-0.6": 0,
            "0.6-0.8": 0,
            "0.8-1.0": 0,
        }

        if not score_trend:
            return {k: 0.0 for k in bins}

        total = len(score_trend)
        for score in score_trend:
            if score >= 0.8:
                bins["0.8-1.0"] += 1
            elif score >= 0.6:
                bins["0.6-0.8"] += 1
            elif score >= 0.3:
                bins["0.3-0.6"] += 1
            else:
                bins["0.0-0.3"] += 1

        return {k: round(v / total, 4) for k, v in bins.items()}

    def _check_plateau(self, state: GoalState, phase: Phase | None) -> ConvergenceSignal:
        """检测分数是否在连续 N 次迭代中无明显改善。

        同时检查两种模式：
        1. 整体波动小（max - min < threshold）—— 原有逻辑
        2. 每步变化都小（所有相邻差值 < threshold）—— 捕获边缘震荡
        """
        if not phase:
            return ConvergenceSignal(should_stop=False, reason="")

        window = self._cfg.convergence_window
        phase_records = [r for r in state.iteration_history if r.phase_id == phase.id]
        if len(phase_records) < window:
            return ConvergenceSignal(should_stop=False, reason="")

        recent = phase_records[-window:]
        scores = [r.score for r in recent]
        improvement = max(scores) - min(scores)
        threshold = self._cfg.score_improvement_min
        net_change = scores[-1] - scores[0]
        diffs = [scores[i + 1] - scores[i] for i in range(len(scores) - 1)]
        non_zero_diffs = [diff for diff in diffs if abs(diff) > 1e-9]
        sign_changes = sum(
            1
            for i in range(len(non_zero_diffs) - 1)
            if non_zero_diffs[i] * non_zero_diffs[i + 1] < 0
        )
        slope = self._linear_regression_slope(scores)

        # 模式 1：整体波动小
        range_plateau = improvement < threshold

        # 模式 2：每步变化都小（捕获 0.70→0.75→0.70 这类震荡）
        step_plateau = all(
            abs(scores[i + 1] - scores[i]) < threshold
            for i in range(len(scores) - 1)
        )

        # 模式 3：末尾回到原地且中间存在方向反转，说明在原地打转而非持续改善。
        trend_plateau = (
            abs(net_change) < threshold
            and sign_changes >= 1
            and abs(slope) < max(threshold / max(1, len(scores) - 1), 0.01)
        )

        if range_plateau or step_plateau or trend_plateau:
            if range_plateau:
                trigger = "range_plateau"
            elif step_plateau:
                trigger = "step_plateau"
            else:
                trigger = "trend_plateau"

            # 检测精确分数平台期（连续 >=2 轮 score 完全相同）
            plateau_hint = self._detect_exact_plateau(scores)

            details: dict = {
                "trigger": "plateau",
                "sub_trigger": trigger,
                "scores": scores,
                "improvement": improvement,
                "net_change": net_change,
                "sign_changes": sign_changes,
                "slope": slope,
            }
            if plateau_hint is not None:
                details["plateau_hint"] = plateau_hint

            return ConvergenceSignal(
                should_stop=True,
                reason=(
                    f"阶段 '{phase.name}' 连续 {window} 次迭代分数无明显改善 "
                    f"(波动 {improvement:.3f}, 触发: {trigger})"
                ),
                details=details,
            )
        return ConvergenceSignal(should_stop=False, reason="")

    @staticmethod
    def _linear_regression_slope(scores: list[float]) -> float:
        if len(scores) < 2:
            return 0.0

        xs = list(range(len(scores)))
        mean_x = sum(xs) / len(xs)
        mean_y = sum(scores) / len(scores)
        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, scores))
        denominator = sum((x - mean_x) ** 2 for x in xs)
        if denominator == 0:
            return 0.0
        return numerator / denominator

    @staticmethod
    def _detect_exact_plateau(scores: list[float]) -> PlateauSignal | None:
        """检测精确分数平台期：连续 >=2 轮 score 完全相同。

        扫描分数序列，找到最长连续相同分数段，
        若长度 >= 2 则返回 PlateauSignal，包含打破平台期的策略建议。
        """
        if len(scores) < 2:
            return None

        max_run = 1
        current_run = 1
        plateau_score = scores[0]

        for i in range(1, len(scores)):
            if abs(scores[i] - scores[i - 1]) < 1e-9:
                current_run += 1
                if current_run > max_run:
                    max_run = current_run
                    plateau_score = scores[i]
            else:
                current_run = 1

        if max_run >= 2:
            return PlateauSignal(
                consecutive_count=max_run,
                plateau_score=plateau_score,
                strategy_adjustment={
                    "action": "escalate_review",
                    "suggested_dimensions": [
                        "code_structure",
                        "error_handling",
                        "test_coverage",
                        "performance",
                        "security",
                    ],
                },
            )
        return None

    def _check_diminishing_returns(
        self, state: GoalState, phase: Phase | None
    ) -> ConvergenceSignal:
        """检测边际递减：改善幅度逐渐减小，投入产出比低。

        计算最近 N 轮迭代的分数改善差值序列，
        如果差值呈单调递减趋势（每轮比上一轮改善更少），
        则认为进入边际递减状态。
        """
        if not phase:
            return ConvergenceSignal(should_stop=False, reason="")

        window = min(self._cfg.convergence_window, len(state.iteration_history))
        if window < 3:
            return ConvergenceSignal(should_stop=False, reason="")

        # 获取当前阶段的迭代记录
        phase_records = [r for r in state.iteration_history if r.phase_id == phase.id]
        if len(phase_records) < window:
            return ConvergenceSignal(should_stop=False, reason="")

        recent = phase_records[-window:]
        scores = [r.score for r in recent]

        if len(scores) < 3:
            return ConvergenceSignal(should_stop=False, reason="")

        # 计算相邻分数的改善量（差值）
        improvements = [scores[i + 1] - scores[i] for i in range(len(scores) - 1)]

        # 检查是否单调递减（每轮改善越来越少）
        # 允许最多 1 个例外（小波动）
        violations = 0
        for i in range(len(improvements) - 1):
            if improvements[i + 1] > improvements[i]:
                violations += 1

        is_diminishing = (
            violations <= 1
            and all(0 < imp < self._cfg.convergence_threshold for imp in improvements)
        )

        if is_diminishing:
            avg_improvement = sum(improvements) / len(improvements)
            return ConvergenceSignal(
                should_stop=True,
                reason=(
                    f"边际递减：阶段 '{phase.name}' 最近 {window} 轮改善量单调递减 "
                    f"(平均改善 {avg_improvement:.4f} < 阈值 {self._cfg.convergence_threshold})"
                ),
                details={
                    "trigger": "diminishing_returns",
                    "scores": scores,
                    "improvements": improvements,
                    "avg_improvement": avg_improvement,
                },
            )

        return ConvergenceSignal(should_stop=False, reason="")

    def _check_failure_pattern_convergence(
        self, state: GoalState, phase: Phase | None
    ) -> ConvergenceSignal:
        """基于 failure_categories 的根因模式检测收敛信号。

        当检测到 2+ 个 critical 级别的根因时，认为无法通过迭代改善，应停止执行。
        不受 MIN_ITERATIONS 限制，因为 critical 模式意味着系统性问题。

        Returns:
            ConvergenceSignal: should_stop=True 表示应停止迭代
        """
        diagnoses = ConvergenceDetector.extract_root_causes(state)
        if not diagnoses:
            return ConvergenceSignal(should_stop=False, reason="")

        critical_diagnoses = [d for d in diagnoses if d["severity"] == "critical"]
        if len(critical_diagnoses) >= 2:
            critical_summary = "; ".join(
                f"{d['category']}(x{d['count']})" for d in critical_diagnoses[:3]
            )
            return ConvergenceSignal(
                should_stop=True,
                reason=(
                    f"检测到 {len(critical_diagnoses)} 个 critical 级别失败根因，"
                    f"迭代无法改善: {critical_summary}"
                ),
                details={
                    "trigger": "failure_pattern_critical",
                    "critical_count": len(critical_diagnoses),
                    "diagnoses": [
                        {"category": d["category"], "count": d["count"], "severity": d["severity"]}
                        for d in critical_diagnoses
                    ],
                },
            )

        return ConvergenceSignal(should_stop=False, reason="")

    @staticmethod
    def extract_root_causes(state: GoalState) -> list[RootCauseDiagnosis]:
        """从 goal_state.failure_categories 提取根因诊断数据，供收敛判定使用。

        若 failure_categories 为空或不存在，返回空列表不报错。

        严重程度分级规则：
        - critical: 单类别计数 >= 5，或属于 catastrophic/repeated_failure 等硬性类别
        - major: 单类别计数 >= 2，或属于 timeout/transient 等可恢复类别
        - minor: 单类别计数 == 1 且不匹配以上规则
        """
        # 安全访问：兼容 failure_categories 缺失或为 None 的情况
        raw_categories = getattr(state, "failure_categories", None) or {}
        if not raw_categories:
            return []

        # 类型防御：过滤非 int 值，防止上游误写 dict/str 导致排序崩溃
        categories: dict[str, int] = {
            k: v for k, v in raw_categories.items() if isinstance(v, int)
        }
        if not categories:
            return []

        # 硬性失败类别（一旦出现即为 critical）
        CRITICAL_KEYWORDS = {
            "catastrophic_stop", "repeated_failure", "escalation",
            "budget_exhausted", "max_iterations", "empty_phases",
        }
        # 可恢复类别（计数 >= 2 时为 major）
        RECOVERABLE_KEYWORDS = {
            "timeout", "transient", "retry", "preflight",
        }

        # 按计数降序排列，优先关注高频失败
        sorted_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)

        diagnoses: list[RootCauseDiagnosis] = []
        for cat, count in sorted_cats:
            # 判定严重程度
            if count >= 5 or any(kw in cat for kw in CRITICAL_KEYWORDS):
                severity = "critical"
            elif count >= 2 or any(kw in cat for kw in RECOVERABLE_KEYWORDS):
                severity = "major"
            else:
                severity = "minor"

            # 同族关联类别：按冒号前的前缀分组（如 "zero_phase:parse" 和 "zero_phase:execute" 同族）
            prefix = cat.split(":")[0] if ":" in cat else cat
            related = [
                other for other, _ in categories.items()
                if other != cat and other.split(":")[0] == prefix
            ]

            diagnoses.append(RootCauseDiagnosis(
                category=cat,
                count=count,
                severity=severity,
                related_categories=related,
            ))

        return diagnoses


class DeteriorationDetector:
    """检测迭代过程中的恶化趋势。

    4 种检查：
    1. 分数连续下降
    2. 失败任务数增长
    3. 回归加剧
    4. verdict 严重程度升级
    """

    def __init__(self, auto_config: AutoConfig):
        self._cfg = auto_config

    def check(self, state: GoalState, phase: Phase | None = None) -> DeteriorationSignal:
        """运行所有恶化检查，返回最严重的信号。支持跨维度关联分析。"""
        if not self._cfg.deterioration_enabled or not phase:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        phase_records = [r for r in state.iteration_history if r.phase_id == phase.id]
        if len(phase_records) < 2:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        # 维度名称映射
        dimension_names = {
            0: "score_decline",
            1: "error_growth",
            2: "regression_escalation",
            3: "verdict_degradation",
        }

        # 骤降检测：捕获锯齿形震荡中的单步大幅下降
        sharp_drop_signal = self._check_sharp_drops(phase_records)

        checks = [
            self._check_score_decline(phase_records),
            self._check_error_growth(phase_records),
            self._check_regression_escalation(phase_records),
            self._check_verdict_degradation(phase_records),
        ]

        # 将骤降检测结果纳入综合判定
        if sharp_drop_signal.level != DeteriorationLevel.NONE:
            checks.append(sharp_drop_signal)

        # 收集所有检测到恶化的维度
        deteriorated_dimensions = [
            dimension_names[i]
            for i, signal in enumerate(checks)
            if signal.level != DeteriorationLevel.NONE
        ]

        # 返回最严重的信号
        worst = max(checks, key=lambda s: list(DeteriorationLevel).index(s.level))

        # 跨维度关联分析：若同时检测到 2+ 维度恶化且包含核心指标(score_decline)，提升严重级别
        if len(deteriorated_dimensions) >= 2 and worst.level != DeteriorationLevel.NONE:
            # 只有当分数下降参与时才提升——非核心指标的多维度恶化仅记录
            if "score_decline" in deteriorated_dimensions:
                level_order = [DeteriorationLevel.NONE, DeteriorationLevel.WARNING,
                              DeteriorationLevel.SERIOUS, DeteriorationLevel.CRITICAL]
                current_index = level_order.index(worst.level)

                # 提升一级（但不超过 CRITICAL）
                if current_index < len(level_order) - 1:
                    new_level = level_order[current_index + 1]
                    logger.warning(
                        "跨维度关联分析: 检测到 %d 个维度同时恶化 %s，严重级别从 %s 提升至 %s",
                        len(deteriorated_dimensions), deteriorated_dimensions,
                        worst.level.value, new_level.value,
                    )
                    worst.level = new_level
                    worst.reason = f"[跨维度恶化] {worst.reason}"
                    worst.correlated_dimensions = deteriorated_dimensions
                else:
                    worst.correlated_dimensions = deteriorated_dimensions
            else:
                logger.info(
                    "跨维度关联分析: %d 个维度恶化 %s 但不含核心指标 score_decline，不提升级别",
                    len(deteriorated_dimensions), deteriorated_dimensions,
                )
                worst.correlated_dimensions = deteriorated_dimensions

        if worst.level != DeteriorationLevel.NONE:
            logger.warning(
                "恶化检测 [%s]: %s → 建议: %s",
                worst.level.value, worst.reason, worst.recommended_action,
            )
        return worst

    def _check_score_decline(self, records: list) -> DeteriorationSignal:
        """分数连续下降检测。window 内严格递减则触发。"""
        window = self._cfg.deterioration_window
        if len(records) < window:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        recent = records[-window:]
        scores = [r.score for r in recent]

        # 检查是否严格递减
        strictly_declining = all(scores[i] > scores[i + 1] for i in range(len(scores) - 1))
        if not strictly_declining:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        total_drop = scores[0] - scores[-1]
        avg_drop = total_drop / (len(scores) - 1)

        # 按平均下降幅度分级
        if avg_drop >= self._cfg.deterioration_drop_threshold * 2:
            level = DeteriorationLevel.CRITICAL
            action = "escalate"
        elif avg_drop >= self._cfg.deterioration_drop_threshold:
            level = DeteriorationLevel.SERIOUS
            action = "switch_strategy"
        else:
            level = DeteriorationLevel.WARNING
            action = "continue"

        return DeteriorationSignal(
            level=level,
            reason=f"分数连续 {window} 次下降: {' → '.join(f'{s:.2f}' for s in scores)} (平均降幅 {avg_drop:.3f})",
            details={"scores": scores, "total_drop": total_drop, "avg_drop": avg_drop},
            recommended_action=action,
        )

    def _check_error_growth(self, records: list) -> DeteriorationSignal:
        """失败任务数增长检测。错误数单调递增且增长 >= 阈值则触发。"""
        window = self._cfg.deterioration_window
        if len(records) < window:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        recent = records[-window:]
        error_counts = [r.task_error_count for r in recent]

        # 检查单调递增
        monotonic_increasing = all(
            error_counts[i] < error_counts[i + 1] for i in range(len(error_counts) - 1)
        )
        if not monotonic_increasing:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        growth = error_counts[-1] - error_counts[0]
        if growth < self._cfg.deterioration_error_growth:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        return DeteriorationSignal(
            level=DeteriorationLevel.SERIOUS,
            reason=f"失败任务数连续增长: {' → '.join(str(c) for c in error_counts)} (增长 {growth})",
            details={"error_counts": error_counts, "growth": growth},
            recommended_action="switch_strategy",
        )

    def _check_regression_escalation(self, records: list) -> DeteriorationSignal:
        """回归加剧检测。连续多次回归则升级严重程度。"""
        # 取最近的记录检查 regression_detected
        recent = records[-4:] if len(records) >= 4 else records
        regression_streak = 0
        for r in reversed(recent):
            if r.regression_detected:
                regression_streak += 1
            else:
                break

        if regression_streak >= 3:
            return DeteriorationSignal(
                level=DeteriorationLevel.CRITICAL,
                reason=f"连续 {regression_streak} 次迭代检测到回归",
                details={"regression_streak": regression_streak},
                recommended_action="escalate",
            )
        elif regression_streak >= 2:
            return DeteriorationSignal(
                level=DeteriorationLevel.SERIOUS,
                reason=f"连续 {regression_streak} 次迭代检测到回归",
                details={"regression_streak": regression_streak},
                recommended_action="rollback",
            )

        return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

    def _check_verdict_degradation(self, records: list) -> DeteriorationSignal:
        """verdict 严重程度升级检测。minor → major → critical 连续升级则触发。"""
        # verdict 严重程度排序
        severity_order = {
            ReviewVerdict.PASS: 0,
            ReviewVerdict.MINOR_ISSUES: 1,
            ReviewVerdict.MAJOR_ISSUES: 2,
            ReviewVerdict.CRITICAL: 3,
            ReviewVerdict.BLOCKED: 4,
        }

        if len(records) < 2:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        recent = records[-3:] if len(records) >= 3 else records[-2:]
        severities = [severity_order.get(r.verdict, 0) for r in recent]

        # 检查严格递增
        strictly_increasing = all(
            severities[i] < severities[i + 1] for i in range(len(severities) - 1)
        )
        if not strictly_increasing:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        verdicts_str = " → ".join(r.verdict.value for r in recent)

        if len(recent) >= 3 and severities[-1] >= 3:
            return DeteriorationSignal(
                level=DeteriorationLevel.CRITICAL,
                reason=f"verdict 连续升级至危急: {verdicts_str}",
                details={"verdicts": [r.verdict.value for r in recent]},
                recommended_action="escalate",
            )

        return DeteriorationSignal(
            level=DeteriorationLevel.WARNING,
            reason=f"verdict 严重程度持续升级: {verdicts_str}",
            details={"verdicts": [r.verdict.value for r in recent]},
            recommended_action="switch_strategy",
        )

    def _check_sharp_drops(self, records: list) -> DeteriorationSignal:
        """检测分数骤降：捕获锯齿形震荡中的单步大幅下降。

        两种模式：
        1. 任意单步降幅 >= 0.3 → CRITICAL
        2. 2+ 个单步降幅 >= 0.15 但 < 0.3 → SERIOUS
        """
        if len(records) < 2:
            return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")

        scores = [r.score for r in records]

        # 计算所有相邻分数差值（scores[i] - scores[i+1]，正值表示下降）
        drops = [(scores[i] - scores[i + 1], i) for i in range(len(scores) - 1)]

        # 检查单步骤降 >= 0.3（CRITICAL）
        for drop, idx in drops:
            if drop >= 0.3:
                return DeteriorationSignal(
                    level=DeteriorationLevel.CRITICAL,
                    reason=f"sharp_score_drop: {scores[idx]:.2f}->{scores[idx + 1]:.2f}",
                    details={
                        "trigger": "sharp_drop",
                        "drop_magnitude": drop,
                        "from_score": scores[idx],
                        "to_score": scores[idx + 1],
                        "all_drops": [(d, i) for d, i in drops if d > 0],
                    },
                    recommended_action="escalate",
                )

        # 检查 2+ 个单步降幅 >= 0.15（SERIOUS）
        moderate_drops = [(d, i) for d, i in drops if 0.15 <= d < 0.3]
        if len(moderate_drops) >= 2:
            drop_descriptions = ", ".join(
                f"{scores[i]:.2f}->{scores[i + 1]:.2f}({d:.2f})"
                for d, i in moderate_drops
            )
            return DeteriorationSignal(
                level=DeteriorationLevel.SERIOUS,
                reason=f"multiple_moderate_drops: {drop_descriptions}",
                details={
                    "trigger": "moderate_drops",
                    "drop_count": len(moderate_drops),
                    "drops": [(d, i) for d, i in moderate_drops],
                },
                recommended_action="switch_strategy",
            )

        return DeteriorationSignal(level=DeteriorationLevel.NONE, reason="")
