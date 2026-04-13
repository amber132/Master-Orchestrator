"""
Dashboard 数据收集器

提供实时监控数据的收集、序列化和格式化功能。
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
from datetime import datetime


@dataclass
class DashboardSnapshot:
    """Dashboard 快照数据"""

    # 任务状态统计
    task_status_counts: Dict[str, int] = field(default_factory=dict)

    # 成本汇总
    cost_summary: Dict[str, float] = field(default_factory=dict)

    # 漂移告警
    drift_alerts: List[Dict[str, Any]] = field(default_factory=list)

    # 隔离任务
    quarantined_tasks: List[Dict[str, Any]] = field(default_factory=list)

    # 收敛趋势
    convergence_trend: List[Dict[str, Any]] = field(default_factory=list)

    # 吞吐量（任务/分钟）
    throughput: float = 0.0

    # 活跃模型
    active_models: List[str] = field(default_factory=list)

    # 快照时间戳
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """序列化为 JSON 兼容的字典格式"""
        return asdict(self)

    def format_text_report(self) -> str:
        """生成终端友好的文本报告"""
        lines = []
        lines.append("=" * 80)
        lines.append("Dashboard Snapshot Report")
        lines.append("=" * 80)
        lines.append(f"Timestamp: {self.timestamp}")
        lines.append("")

        # 任务状态统计
        lines.append("Task Status:")
        lines.append("-" * 40)
        if self.task_status_counts:
            for status, count in sorted(self.task_status_counts.items()):
                lines.append(f"  {status:20s}: {count:5d}")
        else:
            lines.append("  No tasks")
        lines.append("")

        # 成本汇总
        lines.append("Cost Summary:")
        lines.append("-" * 40)
        if self.cost_summary:
            for key, value in self.cost_summary.items():
                if isinstance(value, float):
                    lines.append(f"  {key:20s}: ${value:10.4f}")
                else:
                    lines.append(f"  {key:20s}: {value}")
        else:
            lines.append("  No cost data")
        lines.append("")

        # 吞吐量
        lines.append("Performance:")
        lines.append("-" * 40)
        lines.append(f"  Throughput: {self.throughput:.2f} tasks/min")
        lines.append("")

        # 活跃模型
        lines.append("Active Models:")
        lines.append("-" * 40)
        if self.active_models:
            for model in self.active_models:
                lines.append(f"  - {model}")
        else:
            lines.append("  No active models")
        lines.append("")

        # 漂移告警
        lines.append("Drift Alerts:")
        lines.append("-" * 40)
        if self.drift_alerts:
            for i, alert in enumerate(self.drift_alerts, 1):
                lines.append(f"  [{i}] {alert.get('message', 'Unknown alert')}")
                if 'severity' in alert:
                    lines.append(f"      Severity: {alert['severity']}")
        else:
            lines.append("  No drift alerts")
        lines.append("")

        # 隔离任务
        lines.append("Quarantined Tasks:")
        lines.append("-" * 40)
        if self.quarantined_tasks:
            for i, task in enumerate(self.quarantined_tasks, 1):
                task_id = task.get('task_id', 'unknown')
                reason = task.get('reason', 'No reason provided')
                lines.append(f"  [{i}] Task {task_id}: {reason}")
        else:
            lines.append("  No quarantined tasks")
        lines.append("")

        # 收敛趋势
        lines.append("Convergence Trend:")
        lines.append("-" * 40)
        if self.convergence_trend:
            lines.append(f"  Total data points: {len(self.convergence_trend)}")
            # 显示最近 5 个数据点
            recent = self.convergence_trend[-5:]
            for point in recent:
                iteration = point.get('iteration', '?')
                score = point.get('score', 0.0)
                lines.append(f"  Iteration {iteration}: {score:.4f}")
        else:
            lines.append("  No convergence data")
        lines.append("")

        lines.append("=" * 80)
        return "\n".join(lines)


class DashboardDataCollector:
    """Dashboard 数据收集器

    从各个组件收集运行时数据，生成 Dashboard 快照。
    """

    def collect(
        self,
        scheduler: Any,
        budget_tracker: Any,
        convergence_history: Optional[List[Dict[str, Any]]] = None,
        quarantine: Any = None
    ) -> DashboardSnapshot:
        """收集 Dashboard 数据

        Args:
            scheduler: 任务调度器实例
            budget_tracker: 预算追踪器实例
            convergence_history: 收敛历史数据（可选）
            quarantine: 隔离区实例（可选）

        Returns:
            DashboardSnapshot: Dashboard 快照数据
        """
        snapshot = DashboardSnapshot()

        # 收集任务状态统计
        snapshot.task_status_counts = self._collect_task_status(scheduler)

        # 收集成本汇总
        snapshot.cost_summary = self._collect_cost_summary(budget_tracker)

        # 收集收敛趋势
        if convergence_history:
            snapshot.convergence_trend = convergence_history

        # 收集隔离任务
        if quarantine:
            snapshot.quarantined_tasks = self._collect_quarantined_tasks(quarantine)

        # 收集漂移告警
        snapshot.drift_alerts = self._collect_drift_alerts(convergence_history)

        # 计算吞吐量
        snapshot.throughput = self._calculate_throughput(scheduler)

        # 收集活跃模型
        snapshot.active_models = self._collect_active_models(scheduler)

        return snapshot

    def _collect_task_status(self, scheduler: Any) -> Dict[str, int]:
        """收集任务状态统计"""
        status_counts = {}

        try:
            # 尝试从 scheduler 获取任务状态
            if hasattr(scheduler, 'get_task_status_counts'):
                status_counts = scheduler.get_task_status_counts()
            elif hasattr(scheduler, 'tasks'):
                # 手动统计任务状态
                for task in scheduler.tasks.values():
                    status = getattr(task, 'status', 'unknown')
                    status_counts[status] = status_counts.get(status, 0) + 1
            elif hasattr(scheduler, 'store'):
                # 从 store 获取任务状态
                store = scheduler.store
                if hasattr(store, 'get_all_tasks'):
                    tasks = store.get_all_tasks()
                    for task in tasks:
                        status = task.get('status', 'unknown')
                        status_counts[status] = status_counts.get(status, 0) + 1
        except Exception as e:
            # 静默失败，返回空字典
            pass

        return status_counts

    def _collect_cost_summary(self, budget_tracker: Any) -> Dict[str, float]:
        """收集成本汇总"""
        cost_summary = {}

        try:
            # 尝试从 budget_tracker 获取成本信息
            if hasattr(budget_tracker, 'get_total_cost'):
                cost_summary['total_cost'] = budget_tracker.get_total_cost()

            if hasattr(budget_tracker, 'get_cost_by_model'):
                cost_by_model = budget_tracker.get_cost_by_model()
                cost_summary.update(cost_by_model)

            if hasattr(budget_tracker, 'total_cost'):
                cost_summary['total_cost'] = budget_tracker.total_cost

            if hasattr(budget_tracker, 'remaining_budget'):
                cost_summary['remaining_budget'] = budget_tracker.remaining_budget
        except Exception as e:
            # 静默失败，返回空字典
            pass

        return cost_summary

    def _collect_quarantined_tasks(self, quarantine: Any) -> List[Dict[str, Any]]:
        """收集隔离任务"""
        quarantined = []

        try:
            if hasattr(quarantine, 'get_quarantined_tasks'):
                quarantined = quarantine.get_quarantined_tasks()
            elif hasattr(quarantine, 'quarantined_tasks'):
                tasks = quarantine.quarantined_tasks
                if isinstance(tasks, dict):
                    quarantined = [
                        {'task_id': task_id, 'reason': info.get('reason', 'Unknown')}
                        for task_id, info in tasks.items()
                    ]
                elif isinstance(tasks, list):
                    quarantined = tasks
        except Exception as e:
            # 静默失败，返回空列表
            pass

        return quarantined

    def _collect_drift_alerts(
        self,
        convergence_history: Optional[List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """收集漂移告警"""
        alerts = []

        if not convergence_history or len(convergence_history) < 2:
            return alerts

        try:
            # 检查最近的收敛趋势
            recent = convergence_history[-5:]

            # 检测恶化趋势
            if len(recent) >= 2:
                last_score = recent[-1].get('score', 0.0)
                prev_score = recent[-2].get('score', 0.0)

                if last_score < prev_score - 0.1:
                    alerts.append({
                        'message': 'Convergence score degradation detected',
                        'severity': 'warning',
                        'current_score': last_score,
                        'previous_score': prev_score
                    })

            # 检测停滞
            if len(recent) >= 3:
                scores = [p.get('score', 0.0) for p in recent[-3:]]
                if max(scores) - min(scores) < 0.01:
                    alerts.append({
                        'message': 'Convergence stagnation detected',
                        'severity': 'info',
                        'scores': scores
                    })
        except Exception as e:
            # 静默失败，返回空列表
            pass

        return alerts

    def _calculate_throughput(self, scheduler: Any) -> float:
        """计算吞吐量（任务/分钟）"""
        throughput = 0.0

        try:
            if hasattr(scheduler, 'get_throughput'):
                throughput = scheduler.get_throughput()
            elif hasattr(scheduler, 'start_time') and hasattr(scheduler, 'completed_count'):
                # 手动计算吞吐量
                start_time = scheduler.start_time
                completed = scheduler.completed_count

                if start_time and completed > 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed > 0:
                        throughput = (completed / elapsed) * 60  # 转换为任务/分钟
        except Exception as e:
            # 静默失败，返回 0.0
            pass

        return throughput

    def _collect_active_models(self, scheduler: Any) -> List[str]:
        """收集活跃模型列表"""
        models = []

        try:
            if hasattr(scheduler, 'get_active_models'):
                models = scheduler.get_active_models()
            elif hasattr(scheduler, 'tasks'):
                # 从任务中提取模型信息
                model_set = set()
                for task in scheduler.tasks.values():
                    if hasattr(task, 'model'):
                        model_set.add(task.model)
                models = sorted(list(model_set))
        except Exception as e:
            # 静默失败，返回空列表
            pass

        return models
