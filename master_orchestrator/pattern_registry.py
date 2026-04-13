"""Reusable architecture pattern packs."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ArchitecturePattern:
    pattern_id: str
    title: str
    summary: str
    default_risks: list[str] = field(default_factory=list)
    quality_attributes: list[str] = field(default_factory=list)


class PatternRegistry:
    def __init__(self) -> None:
        self._patterns = {
            "modular_monolith_boundary_cleanup": ArchitecturePattern(
                pattern_id="modular_monolith_boundary_cleanup",
                title="Modular Monolith Boundary Cleanup",
                summary="在保留单体部署形态下，先收敛模块边界、门面和依赖方向。",
                default_risks=["边界名义化但调用链未真正收敛"],
                quality_attributes=["modularity", "change_isolation"],
            ),
            "strangler_migration": ArchitecturePattern(
                pattern_id="strangler_migration",
                title="Strangler Migration",
                summary="通过适配层和分流策略逐步迁移旧路径，而不是一次性切换。",
                default_risks=["双路径长期并存导致复杂度上升"],
                quality_attributes=["rollback", "deployability"],
            ),
            "service_extraction": ArchitecturePattern(
                pattern_id="service_extraction",
                title="Service Extraction",
                summary="先收敛边界与接口，再逐步抽离实现和部署单元。",
                default_risks=["数据边界不清、契约漂移、回滚困难"],
                quality_attributes=["deployability", "boundary_clarity"],
            ),
            "event_driven_reorganization": ArchitecturePattern(
                pattern_id="event_driven_reorganization",
                title="Event Driven Reorganization",
                summary="围绕事件和异步边界重组系统职责。",
                default_risks=["最终一致性处理不充分"],
                quality_attributes=["loose_coupling", "scalability"],
            ),
            "api_facade": ArchitecturePattern(
                pattern_id="api_facade",
                title="API Facade",
                summary="引入统一 facade 隔离调用方和后端内部重构。",
                default_risks=["facade 成为新的耦合热点"],
                quality_attributes=["compatibility", "incremental_delivery"],
            ),
            "plugin_extension_extraction": ArchitecturePattern(
                pattern_id="plugin_extension_extraction",
                title="Plugin Extension Extraction",
                summary="把不稳定扩展点抽成插件边界，降低核心系统耦合。",
                default_risks=["扩展协议不稳定"],
                quality_attributes=["extensibility", "separation_of_concerns"],
            ),
        }

    def get(self, pattern_id: str) -> ArchitecturePattern | None:
        return self._patterns.get(pattern_id)

    def resolve(self, pattern_ids: list[str]) -> list[ArchitecturePattern]:
        return [self._patterns[item] for item in pattern_ids if item in self._patterns]
