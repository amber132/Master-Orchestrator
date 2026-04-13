"""Research-oriented iteration helpers for discovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import re
import urllib.parse
import uuid
from typing import Callable

from .config import DiscoveryConfig
from .search_provider import DiscoveredSource, SearchHit

_MODEL_ID_RE = re.compile(r"\b[a-z0-9]+(?:-[a-z0-9]+){1,}-\d+(?:\.\d+)*(?:-[a-z0-9]+)*\b", re.IGNORECASE)
_API_ACTION_RE = re.compile(
    r"\b(?:Create|Get|List|Update|Delete|Batch|Describe|Query|Invoke|Submit|Run|Generate|Fetch|Retrieve|Cancel)"
    r"(?:[A-Z][A-Za-z0-9]+){1,}\b"
)
_API_PATH_RE = re.compile(r"/api/[a-z0-9/_-]+", re.IGNORECASE)
_DOC_ID_RE = re.compile(r"/docs/\d+/\d+", re.IGNORECASE)
_SIGNAL_QUERY_KEYS = frozenset({
    "action",
    "api",
    "doc",
    "docid",
    "document",
    "documentid",
    "endpoint",
    "model",
    "modelid",
    "model_id",
    "path",
    "route",
})


@dataclass
class SourceProfile:
    source_url: str
    domain: str
    tier: str
    source_kind: str
    source_score: float
    providers: list[str] = field(default_factory=list)


@dataclass
class ResearchLead:
    anchor: str
    lead_type: str
    confidence: float
    supporting_urls: list[str] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)


@dataclass
class ResearchClaim:
    statement: str
    status: str
    confidence: float
    evidence_urls: list[str] = field(default_factory=list)


@dataclass
class ResearchContradiction:
    topic: str
    summary: str
    evidence_urls: list[str] = field(default_factory=list)
    severity: str = "medium"


@dataclass
class NextProbe:
    query: str
    question: str
    rationale: str
    priority: str = "medium"


@dataclass
class ResearchDossier:
    topic: str
    keywords: list[str] = field(default_factory=list)
    profiles: list[SourceProfile] = field(default_factory=list)
    leads: list[ResearchLead] = field(default_factory=list)
    claims: list[ResearchClaim] = field(default_factory=list)
    contradictions: list[ResearchContradiction] = field(default_factory=list)
    next_probes: list[NextProbe] = field(default_factory=list)
    executed_probes: list[NextProbe] = field(default_factory=list)
    artifact_path: str = ""
    summary: str = ""


@dataclass
class ResearchRunResult:
    sources: list[DiscoveredSource]
    dossier: ResearchDossier


def _domain_for_source(source: DiscoveredSource) -> str:
    raw_url = source.resolved_url or source.canonical_url or source.url
    parsed = urllib.parse.urlparse(raw_url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        return domain[4:]
    return domain


def _text_for_source(source: DiscoveredSource) -> str:
    url_signals = [
        _signal_text_from_url(url)
        for url in [source.resolved_url, source.canonical_url, source.url]
        if url
    ]
    return " ".join(
        part
        for part in [
            source.title,
            source.snippet,
            *url_signals,
        ]
        if part
    )


def _topic_text_for_source(source: DiscoveredSource) -> str:
    return " ".join(
        part
        for part in [
            source.title,
            source.snippet,
            _signal_text_from_url(source.resolved_url or source.canonical_url or source.url),
        ]
        if part
    )


def _source_matches_topic(source: DiscoveredSource, keywords: list[str]) -> bool:
    if not keywords:
        return True
    text = _topic_text_for_source(source).lower()
    compact_text = re.sub(r"[^a-z0-9]+", "", text)
    for keyword in keywords:
        normalized = keyword.strip().lower()
        if not normalized:
            continue
        if normalized in text:
            return True
        compact_keyword = re.sub(r"[^a-z0-9]+", "", normalized)
        if compact_keyword and len(compact_keyword) >= 8 and compact_keyword in compact_text:
            return True
    return False


def _signal_text_from_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return ""

    parts: list[str] = []
    path = urllib.parse.unquote(parsed.path or "")
    if path:
        parts.append(path)

    for key, values in urllib.parse.parse_qs(parsed.query, keep_blank_values=False).items():
        if key.lower() not in _SIGNAL_QUERY_KEYS:
            continue
        for value in values:
            decoded = urllib.parse.unquote(value or "").strip()
            if _looks_like_signal_value(decoded):
                parts.append(decoded)

    return " ".join(parts)


def _looks_like_signal_value(value: str) -> bool:
    if not value or len(value) > 160:
        return False
    return any(
        pattern.search(value)
        for pattern in (_MODEL_ID_RE, _API_ACTION_RE, _API_PATH_RE, _DOC_ID_RE)
    )


def _is_high_signal_model_id(anchor: str) -> bool:
    digit_segments = [segment for segment in anchor.split("-") if segment.isdigit()]
    if any(len(segment) >= 4 for segment in digit_segments):
        return True
    return sum(ch.isdigit() for ch in anchor) >= 6


def classify_source_profile(source: DiscoveredSource) -> SourceProfile:
    raw_url = (source.resolved_url or source.canonical_url or source.url).lower()
    domain = _domain_for_source(source)
    if domain == "weixin.sogou.com" and "/link" in raw_url:
        tier = "L4"
        source_kind = "aggregator_wrapper"
    elif domain.startswith("api.") or "/docs/" in raw_url or "api-explorer" in raw_url:
        tier = "L1"
        source_kind = "official_structured"
    elif domain.startswith(("developer.", "blog.", "news.")):
        tier = "L2"
        source_kind = "official_narrative"
    elif len(source.providers) > 1 and domain not in {"wikipedia.org"}:
        tier = "L3"
        source_kind = "corroborated_external"
    else:
        tier = "L4"
        source_kind = "weak_signal"
    return SourceProfile(
        source_url=source.resolved_url or source.url,
        domain=domain,
        tier=tier,
        source_kind=source_kind,
        source_score=source.source_score,
        providers=list(source.providers),
    )


def _anchor_queries(anchor: str, lead_type: str) -> list[str]:
    if lead_type == "api_action":
        suffixes = ["api", "swagger"]
    elif lead_type == "model_id":
        suffixes = ["api", "申请开通", "模型列表"]
    elif lead_type == "api_path":
        suffixes = ["docs", "swagger"]
    elif lead_type == "document":
        suffixes = ["docs"]
    else:
        suffixes = ["api"]
    return [f"{anchor} {suffix}".strip() for suffix in suffixes]


def extract_research_leads(
    sources: list[DiscoveredSource],
    keywords: list[str],
    *,
    max_leads: int = 8,
) -> list[ResearchLead]:
    lead_map: dict[str, ResearchLead] = {}
    keyword_set = {kw.lower() for kw in keywords}

    for keyword in keywords:
        for lead_type, pattern, base_conf in [
            ("api_action", _API_ACTION_RE, 0.8),
            ("model_id", _MODEL_ID_RE, 0.78),
            ("api_path", _API_PATH_RE, 0.76),
            ("document", _DOC_ID_RE, 0.72),
        ]:
            if not pattern.fullmatch(keyword):
                continue
            key = keyword.lower()
            if key in lead_map:
                continue
            lead_map[key] = ResearchLead(
                anchor=keyword,
                lead_type=lead_type,
                confidence=round(base_conf, 3),
                supporting_urls=[],
                queries=_anchor_queries(keyword, lead_type),
            )

    for source in sources:
        if not _source_matches_topic(source, keywords):
            continue
        text = _text_for_source(source)
        profile = classify_source_profile(source)
        for lead_type, pattern, base_conf in [
            ("api_action", _API_ACTION_RE, 0.88),
            ("model_id", _MODEL_ID_RE, 0.86),
            ("api_path", _API_PATH_RE, 0.84),
            ("document", _DOC_ID_RE, 0.76),
        ]:
            for match in pattern.findall(text):
                anchor = match[0] if isinstance(match, tuple) else match
                if anchor.lower() in keyword_set and not pattern.fullmatch(anchor):
                    continue
                if lead_type == "model_id" and not _is_high_signal_model_id(anchor):
                    continue
                key = anchor.lower()
                confidence = base_conf
                if profile.tier == "L1":
                    confidence += 0.06
                elif profile.tier == "L2":
                    confidence += 0.03
                confidence += min(0.06, 0.03 * max(0, len(source.providers) - 1))
                lead = lead_map.get(key)
                if lead is None:
                    lead = ResearchLead(
                        anchor=anchor,
                        lead_type=lead_type,
                        confidence=round(min(0.99, confidence), 3),
                        supporting_urls=[source.url],
                        queries=_anchor_queries(anchor, lead_type),
                    )
                    lead_map[key] = lead
                else:
                    if source.url not in lead.supporting_urls:
                        lead.supporting_urls.append(source.url)
                    lead.confidence = round(min(0.99, max(lead.confidence, confidence) + 0.01), 3)
    return sorted(
        lead_map.values(),
        key=lambda item: (-item.confidence, -len(item.supporting_urls), item.anchor.lower()),
    )[:max_leads]


def _status_signals(source: DiscoveredSource) -> set[str]:
    text = _text_for_source(source).lower()
    signals: set[str] = set()
    if any(token in text for token in ["api explorer", "swagger", "/api/", "createcontents", "sdk"]):
        signals.add("api_surface")
    if any(token in text for token in ["暂不支持 api", "不支持 api", "仅支持体验", "只支持体验", "体验中心"]):
        signals.add("api_restricted")
    if any(token in text for token in ["申请开通", "邀测", "白名单", "提交工单", "企业内测", "private preview", "invite-only"]):
        signals.add("gated_access")
    if any(token in text for token in ["模型列表", "model list", "modelid=", "model/detail", "foundation model", "版本"]):
        signals.add("public_surface")
    return signals


def detect_research_contradictions(
    sources: list[DiscoveredSource],
    keywords: list[str],
) -> list[ResearchContradiction]:
    topical_sources = [source for source in sources if _source_matches_topic(source, keywords)]
    api_surface_urls = [source.url for source in topical_sources if "api_surface" in _status_signals(source)]
    api_restricted_urls = [source.url for source in topical_sources if "api_restricted" in _status_signals(source)]
    gated_urls = [source.url for source in topical_sources if "gated_access" in _status_signals(source)]
    listed_urls = [source.url for source in topical_sources if "public_surface" in _status_signals(source)]
    topic = keywords[0] if keywords else "discovery topic"
    contradictions: list[ResearchContradiction] = []
    if api_surface_urls and api_restricted_urls:
        contradictions.append(
            ResearchContradiction(
                topic=topic,
                summary="同时发现公开 API 面和 API 限制信号",
                evidence_urls=list(dict.fromkeys(api_surface_urls[:2] + api_restricted_urls[:2])),
                severity="high",
            )
        )
    if gated_urls and listed_urls:
        contradictions.append(
            ResearchContradiction(
                topic=topic,
                summary="同时发现公开模型/控制面信号和受限开通信号",
                evidence_urls=list(dict.fromkeys(listed_urls[:2] + gated_urls[:2])),
                severity="medium",
            )
        )
    return contradictions


def build_next_probes(
    keywords: list[str],
    leads: list[ResearchLead],
    contradictions: list[ResearchContradiction],
    *,
    existing_queries: set[str],
    probe_budget: int,
) -> list[NextProbe]:
    probes: list[NextProbe] = []
    seen = set(existing_queries)
    topic_prefix = keywords[0].strip() if keywords else ""
    for lead in leads:
        for query in lead.queries:
            if topic_prefix and topic_prefix.lower() not in query.lower():
                query = f"{topic_prefix} {query}".strip()
            normalized = query.strip().lower()
            if not normalized or normalized in seen:
                continue
            priority = "high" if contradictions else "medium"
            probes.append(
                NextProbe(
                    query=query,
                    question=f"继续验证锚点 {lead.anchor} 的真实开放状态",
                    rationale=f"从 {lead.lead_type} 锚点派生下一轮查询",
                    priority=priority,
                )
            )
            seen.add(normalized)
            if len(probes) >= probe_budget:
                return probes
    if contradictions and keywords:
        fallback_query = f"{keywords[0]} 申请开通"
        if fallback_query.lower() not in seen and len(probes) < probe_budget:
            probes.append(
                NextProbe(
                    query=fallback_query,
                    question="验证公开能力与开通门槛是否存在冲突",
                    rationale="矛盾驱动的下一轮追查",
                    priority="high",
                )
            )
    return probes


def _build_claims(
    sources: list[DiscoveredSource],
    keywords: list[str],
    contradictions: list[ResearchContradiction],
) -> list[ResearchClaim]:
    claims: list[ResearchClaim] = []
    signal_to_claim = [
        ("api_surface", "发现公开视频/API 调用面存在"),
        ("api_restricted", "公开口径仍显示 API 调用受限"),
        ("gated_access", "访问权限可能受邀测或申请开通控制"),
        ("public_surface", "模型或控制面对象已公开可见"),
    ]
    contradiction_topics = " ".join(item.summary for item in contradictions).lower()
    for signal, statement in signal_to_claim:
        evidence_urls = [
            source.url
            for source in sources
            if _source_matches_topic(source, keywords) and signal in _status_signals(source)
        ]
        if not evidence_urls:
            continue
        status = "contradicted" if signal == "api_surface" and "api 限制" in contradiction_topics else "supported"
        claims.append(
            ResearchClaim(
                statement=statement,
                status=status,
                confidence=round(min(0.95, 0.55 + 0.08 * min(3, len(evidence_urls))), 3),
                evidence_urls=evidence_urls[:3],
            )
        )
    return claims


def _build_summary(
    leads: list[ResearchLead],
    claims: list[ResearchClaim],
    contradictions: list[ResearchContradiction],
    next_probes: list[NextProbe],
) -> str:
    parts = [
        f"Leads {len(leads)}",
        f"Claims {len(claims)}",
        f"Contradictions {len(contradictions)}",
    ]
    if next_probes:
        parts.append("Next probes: " + ", ".join(probe.query for probe in next_probes[:3]))
    return " | ".join(parts)


class DiscoveryResearchLoop:
    """Use structured anchors and contradictions to deepen discovery."""

    def __init__(
        self,
        *,
        search_registry,
        relevance_filter,
        merge_hits: Callable[[list[SearchHit]], list[DiscoveredSource]],
        discovery_config: DiscoveryConfig,
        evidence_dir: str | Path | None = None,
    ) -> None:
        self._search_registry = search_registry
        self._relevance_filter = relevance_filter
        self._merge_hits = merge_hits
        self._discovery_config = discovery_config
        self._evidence_dir = Path(evidence_dir) if evidence_dir is not None else None
        if self._evidence_dir is not None:
            self._evidence_dir.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        *,
        keywords: list[str],
        initial_queries: list[str],
        initial_hits: list[SearchHit],
        initial_sources: list[DiscoveredSource],
    ) -> ResearchRunResult:
        sources = list(initial_sources)
        all_hits = list(initial_hits)
        executed_queries = {query.lower() for query in initial_queries}
        executed_probes: list[NextProbe] = []
        rounds = 0

        if self._discovery_config.research_enabled and sources:
            for _ in range(max(0, self._discovery_config.research_iterations)):
                leads = extract_research_leads(
                    sources,
                    keywords,
                    max_leads=self._discovery_config.research_max_leads,
                )
                contradictions = detect_research_contradictions(sources, keywords)
                probes = build_next_probes(
                    keywords,
                    leads,
                    contradictions,
                    existing_queries=executed_queries,
                    probe_budget=self._discovery_config.research_probe_budget - len(executed_probes),
                )
                if not probes:
                    break
                query_batch = [probe.query for probe in probes]
                new_hits = self._search_registry.search(
                    query_batch,
                    enabled_providers=[
                        provider
                        for provider in self._discovery_config.enabled_providers
                        if provider not in set(self._discovery_config.disabled_providers)
                        and provider != "rss"
                    ],
                    max_results_per_provider=self._discovery_config.max_hits_per_provider,
                    max_queries=len(query_batch),
                )
                executed_probes.extend(probes)
                executed_queries.update(query.lower() for query in query_batch)
                if not new_hits:
                    break
                all_hits.extend(new_hits)
                sources = self._relevance_filter.filter(
                    self._merge_hits(all_hits),
                    keywords,
                    self._discovery_config,
                )
                rounds += 1

        leads = extract_research_leads(
            sources,
            keywords,
            max_leads=self._discovery_config.research_max_leads,
        )
        contradictions = detect_research_contradictions(sources, keywords)
        next_probes = build_next_probes(
            keywords,
            leads,
            contradictions,
            existing_queries=executed_queries,
            probe_budget=self._discovery_config.research_probe_budget,
        )
        profiles = [classify_source_profile(source) for source in sources]
        claims = _build_claims(sources, keywords, contradictions)
        dossier = ResearchDossier(
            topic=keywords[0] if keywords else "discovery",
            keywords=list(keywords),
            profiles=profiles,
            leads=leads,
            claims=claims,
            contradictions=contradictions,
            next_probes=next_probes,
            executed_probes=executed_probes,
        )
        dossier.summary = _build_summary(leads, claims, contradictions, next_probes)
        dossier.artifact_path = self._persist_dossier(dossier)
        self._annotate_sources(sources, dossier, profiles, rounds)
        return ResearchRunResult(sources=sources, dossier=dossier)

    def _persist_dossier(self, dossier: ResearchDossier) -> str:
        if self._evidence_dir is None:
            return ""
        artifact_path = self._evidence_dir / f"discovery_research_{uuid.uuid4().hex[:10]}.json"
        dossier.artifact_path = str(artifact_path)
        artifact_path.write_text(
            json.dumps(asdict(dossier), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return str(artifact_path)

    @staticmethod
    def _annotate_sources(
        sources: list[DiscoveredSource],
        dossier: ResearchDossier,
        profiles: list[SourceProfile],
        rounds: int,
    ) -> None:
        profile_map = {profile.source_url: profile for profile in profiles}
        for source in sources:
            profile = profile_map.get(source.resolved_url or source.url) or classify_source_profile(source)
            source.evidence_tier = profile.tier
            source.source_profile = profile.source_kind
            source.research_summary = dossier.summary
            source.research_artifact_path = dossier.artifact_path
            source.research_rounds = rounds
