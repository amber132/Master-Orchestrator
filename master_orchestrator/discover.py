"""自动发现博文模块：从 Web 搜索和 RSS 源发现相关技术文章 URL。

双通道搜索（DuckDuckGo + RSS）+ 关键词自动推导 + 相关性过滤。
发现的 URL 合并到 ExternalSourceScanner 管道中处理。
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
import urllib.error
from datetime import datetime
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .config import DEFAULT_CLAUDE_MODEL, DiscoveryConfig, PaginationConfig
from .discovery_research import DiscoveryResearchLoop
from .pagination import paginate_results
from .search_provider import (
    BingWebProvider,
    BraveWebProvider,
    DiscoveredSource,
    DuckDuckGoHtmlProvider,
    GitHubSearchProvider,
    SogouWeChatProvider,
    SearchHit,
    SearchProviderRegistry,
    canonicalize_url,
)

logger = logging.getLogger(__name__)

# Python 关键字和常见虚词，从关键词中过滤掉
_STOP_WORDS = frozenset({
    # Python 关键字
    "self", "none", "true", "false", "return", "import", "from", "class",
    "def", "if", "else", "elif", "for", "while", "try", "except", "finally",
    "with", "as", "yield", "raise", "pass", "break", "continue", "and", "or",
    "not", "in", "is", "lambda", "global", "nonlocal", "assert", "del",
    "async", "await", "init", "main", "args", "kwargs", "str", "int",
    "float", "bool", "list", "dict", "set", "tuple", "type", "object",
    "super", "print", "len", "range", "enumerate", "isinstance", "hasattr",
    "getattr", "setattr", "property", "staticmethod", "classmethod",
    "abstractmethod", "override", "dataclass", "field", "optional",
    "union", "any", "callable", "path", "file", "name", "value", "data",
    "result", "error", "info", "debug", "warning", "test", "setup",
    "teardown", "fixture", "mock", "patch",
    # 领域通用词（区分度低）
    "task", "node", "state", "config", "status", "phase",
    "model", "store", "engine", "controller", "manager", "handler",
    "tracker", "monitor", "logger", "exception", "timeout",
    "execute", "process", "handle", "update", "create", "load",
    "save", "start", "stop", "build", "parse", "check", "validate",
    # 英文虚词
    "used", "using", "based", "called", "these", "those", "their",
    "that", "this", "which", "when", "where", "what", "have", "been",
    "will", "would", "could", "should", "does", "make", "take",
    # 枚举值/常量拆分出的泛词（搜索区分度极低）
    "medium", "high", "low", "critical", "small", "large",
    "template", "default", "maximum", "minimum",
    "output", "input", "source", "target", "context", "content",
    "format", "version", "message", "response", "request",
    "prompt", "system", "external", "internal",
    "total", "current", "previous", "original", "final",
    "count", "index", "level", "score", "label", "title",
    "description", "summary", "detail", "entry", "record",
    "pending", "running", "completed", "failed", "success",
    "enabled", "disabled", "active", "inactive",
})

# 非技术博文域名黑名单
_DOMAIN_BLACKLIST = frozenset({
    "youtube.com", "twitter.com", "x.com", "facebook.com", "reddit.com",
    "instagram.com", "tiktok.com", "linkedin.com", "pinterest.com",
    "amazon.com", "ebay.com", "wikipedia.org", "stackoverflow.com",
    "github.com", "pypi.org", "npmjs.com",
})

# 白名单域名（高质量技术博客）
_DOMAIN_WHITELIST = frozenset({
    "simonwillison.net", "lilianweng.github.io", "anthropic.com",
    "openai.com", "blog.langchain.dev", "martinfowler.com",
    "mitchellh.com", "jvns.ca", "danluu.com", "brandur.org",
    "blog.pragmaticengineer.com", "architecturenotes.co",
    "newsletter.pragmaticengineer.com", "colah.github.io",
    "karpathy.github.io", "jaykmody.com", "eugeneyan.com",
})

# 预配置的高质量 RSS 源
_DEFAULT_RSS_FEEDS = [
    "https://simonwillison.net/atom/everything/",
    "https://lilianweng.github.io/index.xml",
    "https://www.anthropic.com/rss.xml",
    "https://openai.com/blog/rss.xml",
    "https://blog.langchain.dev/rss/",
    "https://martinfowler.com/feed.atom",
]

# 默认搜索查询模板（不含领域前缀，通用于任何项目）
_DEFAULT_SEARCH_TEMPLATE = "{keyword} best practices"

# 多模板轮换（增加搜索结果多样性）
_SEARCH_TEMPLATES = [
    "{keyword} python implementation",
    "{keyword} best practices tutorial",
    "AI agent {keyword}",
]

# HTTP 请求超时
_HTTP_TIMEOUT = 15

_DISCOVERY_REVIEW_MARGIN = 0.1
_SOGOU_WECHAT_TECH_TERMS = (
    "python", "爬虫", "源码", "代码", "api", "markdown", "rss",
    "自动化", "开发", "svg", "脚本", "接口", "github", "开源",
)
_SOGOU_WECHAT_NOISE_TERMS = (
    "旅游", "赚钱", "流量主", "报名", "活动", "课程", "副业", "变现",
    "直播", "抽奖", "加盟", "福利", "优惠", "招生", "加群", "创业",
)
_SOGOU_WECHAT_CLICKBAIT_TERMS = (
    "保姆级", "最简单", "绝了", "太牛了", "最后一种",
)


def _domain_for_url(url: str) -> str:
    try:
        domain = urllib.parse.urlparse(url).netloc.lower()
    except Exception:
        return ""
    if domain.startswith("www."):
        return domain[4:]
    return domain


def _compact_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _keyword_tokens(keyword: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", keyword.lower())
    return [
        token
        for token in tokens
        if len(token) >= 4
        and token not in _STOP_WORDS
        and not token.isdigit()
    ]


def _topic_text(candidate: DiscoveredSource) -> str:
    parsed = urllib.parse.urlparse(candidate.canonical_url or candidate.url)
    return " ".join(
        part
        for part in [
            candidate.title,
            candidate.snippet,
            urllib.parse.unquote(parsed.path or ""),
        ]
        if part
    )


def _keyword_relevance_score(candidate: DiscoveredSource, keywords: list[str]) -> float:
    if not keywords:
        return 0.0

    text = _topic_text(candidate).lower()
    compact = _compact_text(text)
    best = 0.0

    for keyword in keywords[:10]:
        normalized = keyword.strip().lower()
        if not normalized:
            continue

        if normalized in text:
            best = max(best, 0.6)
            continue

        compact_keyword = _compact_text(normalized)
        if compact_keyword and len(compact_keyword) >= 8 and compact_keyword in compact:
            best = max(best, 0.56)
            continue

        tokens = _keyword_tokens(normalized)
        if not tokens:
            continue
        matched = sum(1 for token in tokens if token in text)
        if matched:
            token_score = 0.16 + 0.08 * min(2, matched - 1)
            if matched == len(tokens) and len(tokens) >= 2:
                token_score += 0.08
            best = max(best, token_score)

    return round(min(0.7, best), 4)


def _is_valid_blog_url(url: str) -> bool:
    domain = _domain_for_url(url)
    return bool(domain) and domain not in _DOMAIN_BLACKLIST


def _is_valid_discovery_hit(hit: SearchHit) -> bool:
    domain = _domain_for_url(hit.url)
    if not domain:
        return False
    if domain not in _DOMAIN_BLACKLIST:
        return True
    return domain == "github.com" and hit.provider == "github_search"


def build_default_search_registry() -> SearchProviderRegistry:
    registry = SearchProviderRegistry()
    registry.register(DuckDuckGoHtmlProvider())
    registry.register(BraveWebProvider())
    registry.register(BingWebProvider())
    registry.register(GitHubSearchProvider())
    registry.register(SogouWeChatProvider())
    return registry


def _score_source(source: DiscoveredSource) -> float:
    domain = _domain_for_url(source.canonical_url or source.url)
    path = urllib.parse.urlparse(source.canonical_url or source.url).path.lower()
    avg_provider_score = (
        sum(source.provider_scores.values()) / len(source.provider_scores)
        if source.provider_scores
        else 0.0
    )
    best_rank = source.best_rank or 10
    query_text = " ".join(source.queries).lower()
    metadata_text = f"{source.title} {source.snippet} {path}".lower()

    score = 0.15
    score += min(0.25, avg_provider_score * 0.3)
    if domain in _DOMAIN_WHITELIST:
        score += 0.25
    if len(source.providers) > 1:
        score += min(0.18, 0.12 + 0.04 * (len(source.providers) - 2))
    score += max(0.0, 0.18 - 0.02 * (best_rank - 1))
    if source.title:
        score += 0.05
    if any(token and token in metadata_text for token in query_text.split()):
        score += 0.05

    if path in {"", "/", "/blog", "/blog/", "/post", "/post/", "/posts", "/posts/"}:
        score -= 0.18
    date_match = re.search(r"/(20\d{2})[/-](\d{2})", path)
    if date_match:
        year = int(date_match.group(1))
        if year <= datetime.now().year - 2:
            score -= 0.18

    score += _provider_specific_adjustment(source)

    return max(0.0, min(1.0, round(score, 4)))


def _provider_specific_adjustment(source: DiscoveredSource) -> float:
    providers = set(source.providers)
    if "sogou_wechat" not in providers:
        return 0.0

    metadata_text = f"{source.title} {source.snippet}".lower()
    tech_hits = sum(1 for term in _SOGOU_WECHAT_TECH_TERMS if term in metadata_text)
    noise_hits = sum(1 for term in _SOGOU_WECHAT_NOISE_TERMS if term in metadata_text)
    clickbait_hits = sum(1 for term in _SOGOU_WECHAT_CLICKBAIT_TERMS if term in metadata_text)

    adjustment = 0.0
    if tech_hits:
        adjustment += min(0.18, 0.05 + 0.03 * max(0, tech_hits - 1))
    if noise_hits:
        adjustment -= min(0.28, 0.08 * noise_hits)
        if tech_hits == 0:
            adjustment -= 0.04
    if clickbait_hits:
        adjustment -= min(0.12, 0.04 * clickbait_hits)
    return adjustment


def _merge_and_score_hits(hits: list[SearchHit]) -> list[DiscoveredSource]:
    grouped: dict[str, list[SearchHit]] = {}
    for hit in hits:
        if not hit.url or not _is_valid_discovery_hit(hit):
            continue
        canonical_url = canonicalize_url(hit.url)
        grouped.setdefault(canonical_url, []).append(hit)

    discovered: list[DiscoveredSource] = []
    for canonical_url, grouped_hits in grouped.items():
        providers = sorted({hit.provider for hit in grouped_hits if hit.provider})
        queries = list(dict.fromkeys(hit.query for hit in grouped_hits if hit.query))
        ranks = {hit.provider: hit.rank for hit in grouped_hits if hit.provider}
        provider_scores = {
            hit.provider: hit.provider_score
            for hit in grouped_hits
            if hit.provider
        }
        best_hit = min(grouped_hits, key=lambda item: item.rank or 999)
        source = DiscoveredSource(
            url=best_hit.url,
            canonical_url=canonical_url,
            title=best_hit.title,
            snippet=best_hit.snippet,
            providers=providers,
            queries=queries,
            ranks=ranks,
            provider_scores=provider_scores,
        )
        source.source_score = _score_source(source)
        discovered.append(source)

    return sorted(discovered, key=lambda item: (-item.source_score, item.best_rank or 999, item.canonical_url))


def score_discovered_hits(hits: list[SearchHit], min_source_score: float = 0.55) -> list[DiscoveredSource]:
    """Score normalized hits and keep only candidates above the trust gate."""
    return [source for source in _merge_and_score_hits(hits) if source.source_score >= min_source_score]


class KeywordExtractor:
    """从源码结构和运行历史自动推导搜索关键词。"""

    def __init__(
        self,
        orchestrator_dir: Path,
        working_dir: Path,
    ):
        self._orchestrator_dir = orchestrator_dir
        self._working_dir = working_dir

    def extract(self, max_keywords: int = 20) -> list[str]:
        """提取关键词，加权评分排序取 top-N。

        评分规则：
        - 基础分 = 出现频率
        - 复合词（含 _）权重 ×3
        - 来自 raw_tasks.prompt 的词权重 ×2
        - 单词长度 ≥8 权重 ×1.5
        - IDF 惩罚：出现在 >50% 文件中的词 ×0.3
        """
        freq: dict[str, float] = {}
        prompt_words: set[str] = set()

        # 从源码结构提取（含 IDF 统计）
        source_words, file_count, total_files = self._from_source_structure()
        for word in source_words:
            freq[word] = freq.get(word, 0) + 1.0

        # 从运行历史提取（同时收集 prompt 来源词）
        for word, from_prompt in self._from_run_history():
            freq[word] = freq.get(word, 0) + 1.0
            if from_prompt:
                prompt_words.add(word)

        # 加权评分
        scored: dict[str, float] = {}
        for word, base_score in freq.items():
            score = base_score
            if "_" in word:
                score *= 3.0  # 复合词区分度高
            if word in prompt_words:
                score *= 2.0  # 来自 prompt 的词技术相关性高
            if len(word) >= 8:
                score *= 1.5  # 长词通常更具体
            # IDF 惩罚：出现在 >50% 文件中的词降权（泛词过滤）
            if total_files > 0 and file_count.get(word, 0) > total_files * 0.5:
                score *= 0.3
            scored[word] = score

        sorted_words = sorted(scored.items(), key=lambda x: x[1], reverse=True)
        keywords = [w for w, _ in sorted_words[:max_keywords]]

        logger.info("提取了 %d 个关键词: %s", len(keywords), keywords[:10])
        return keywords

    def _from_source_structure(self) -> tuple[list[str], dict[str, int], int]:
        """从源码的类名、函数名、docstring 提取关键词。

        复合词保留策略：CamelCase/snake_case 同时保留完整复合词和拆分后的单词。
        复合词（含 _）不受停用词过滤。

        Returns:
            (words, file_count, total_files) — 词列表、每个词出现的文件数、总文件数
        """
        words: list[str] = []
        # IDF 统计：每个词出现在多少个文件中
        file_count: dict[str, int] = {}
        total_files = 0
        exclude_dirs = {"__pycache__", ".venv", ".git", ".idea", "venv"}

        for py_file in sorted(self._orchestrator_dir.rglob("*.py")):
            if any(part in exclude_dirs or part.startswith("test-") for part in py_file.parts):
                continue
            try:
                content = py_file.read_text(encoding="utf-8")
            except Exception:
                continue

            total_files += 1
            file_words: set[str] = set()  # 当前文件出现的词（去重）

            # CamelCase 类名：保留 snake_case 复合词 + 拆分单词
            for cls_name in re.findall(r"^class\s+(\w+)", content, re.MULTILINE):
                compound = self._camel_to_snake(cls_name)
                if "_" in compound:
                    words.append(compound)  # 复合词整体保留
                    file_words.add(compound)
                for part in self._split_camel(cls_name):
                    words.append(part)
                    file_words.add(part.lower())

            # snake_case 公开函数名：保留完整函数名 + 拆分单词
            for func_name in re.findall(r"^def\s+([a-z]\w+)", content, re.MULTILINE):
                if "_" in func_name:
                    words.append(func_name)  # 复合词整体保留
                    file_words.add(func_name)
                for part in func_name.split("_"):
                    words.append(part)
                    file_words.add(part.lower())

            # 模块 docstring 提取英文词（≥4 字符）
            doc_match = re.match(r'^"""(.*?)"""', content, re.DOTALL)
            if doc_match:
                doc_text = doc_match.group(1)
                for w in re.findall(r"[a-zA-Z]{4,}", doc_text):
                    words.append(w)
                    file_words.add(w.lower())

            # 统计文件级出现次数
            for w in file_words:
                file_count[w] = file_count.get(w, 0) + 1

        # 过滤：复合词（含 _）不受停用词过滤，单词需过滤停用词
        result: list[str] = []
        for w in words:
            w_lower = w.lower()
            if "_" in w_lower:
                # 复合词：只要长度 ≥4 就保留
                if len(w_lower) >= 4:
                    result.append(w_lower)
            elif w_lower not in _STOP_WORDS and len(w_lower) >= 4:
                result.append(w_lower)
        return result, file_count, total_files

    def _from_run_history(self) -> list[tuple[str, bool]]:
        """从 goal_state.json 提取关键词。

        来源：goal_text、phase name/description、raw_tasks[].prompt。
        返回 (word, from_prompt) 元组，from_prompt 标记是否来自 prompt 字段。
        """
        results: list[tuple[str, bool]] = []

        candidates = sorted(
            self._working_dir.rglob("goal_state.json"),
            key=lambda p: p.stat().st_mtime if p.exists() else 0,
            reverse=True,
        )[:5]

        for gs_path in candidates:
            try:
                data = json.loads(gs_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            # goal_text
            goal_text = data.get("goal_text", "")
            for w in re.findall(r"[a-zA-Z]{4,}", goal_text):
                w_lower = w.lower()
                if w_lower not in _STOP_WORDS:
                    results.append((w_lower, False))

            # phase name/description + raw_tasks[].prompt
            for phase in data.get("phases", []):
                for field in ("name", "description"):
                    for w in re.findall(r"[a-zA-Z]{4,}", phase.get(field, "")):
                        w_lower = w.lower()
                        if w_lower not in _STOP_WORDS:
                            results.append((w_lower, False))

                # raw_tasks.prompt — 信息密度最高的字段
                for raw_task in phase.get("raw_tasks", []):
                    prompt_text = raw_task.get("prompt", "")
                    for w in re.findall(r"[a-zA-Z]{4,}", prompt_text):
                        w_lower = w.lower()
                        if w_lower not in _STOP_WORDS:
                            results.append((w_lower, True))

        return results

    @staticmethod
    def _camel_to_snake(name: str) -> str:
        """CamelCase → snake_case。"""
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
        return s.lower()

    @staticmethod
    def _split_camel(name: str) -> list[str]:
        """CamelCase → 拆分为单词列表。"""
        # 在大写字母前插入分隔符
        parts = re.sub(r"([A-Z])", r" \1", name).split()
        return [p for p in parts if len(p) >= 4]


class WebSearchChannel:
    """通过 DuckDuckGo HTML 版搜索技术博文。"""

    _DDG_URL = "https://html.duckduckgo.com/html/"
    _MAX_RESULTS_PER_QUERY = 10

    def __init__(self, search_template: str = _DEFAULT_SEARCH_TEMPLATE):
        self._search_template = search_template
        # 如果用户指定了自定义模板，用它替换默认多模板的第一个
        if search_template != _DEFAULT_SEARCH_TEMPLATE:
            self._templates = [search_template] + _SEARCH_TEMPLATES[1:]
        else:
            self._templates = list(_SEARCH_TEMPLATES)

    def search(self, keywords: list[str], max_queries: int = 5) -> list[str]:
        """对每个关键词轮换使用不同模板搜索，返回去重的 URL 列表。"""
        all_urls: list[str] = []
        seen: set[str] = set()
        query_count = 0

        for i, kw in enumerate(keywords):
            if query_count >= max_queries:
                break
            # 轮换模板：每个关键词用不同模板
            template = self._templates[i % len(self._templates)]
            query = template.format(keyword=kw)
            # 请求间隔，避免被限流
            if query_count > 0:
                delay = random.uniform(2.0, 4.0)
                logger.debug("DuckDuckGo 请求间隔 %.1fs", delay)
                time.sleep(delay)
            urls = self._search_one(query)
            for url in urls:
                if url not in seen:
                    seen.add(url)
                    all_urls.append(url)
            query_count += 1

        logger.info("Web 搜索: %d 个查询 → %d 个 URL", query_count, len(all_urls))
        return all_urls

    def _search_one(self, query: str) -> list[str]:
        """执行单次 DuckDuckGo 搜索，返回结果 URL。"""
        try:
            data = urllib.parse.urlencode({"q": query}).encode("utf-8")
            req = urllib.request.Request(
                self._DDG_URL,
                data=data,
                headers={"User-Agent": "Mozilla/5.0 (compatible; claude-orchestrator/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("DuckDuckGo 搜索失败 (%s): %s", query[:50], e)
            return []

        # 提取 class="result__a" 的 href
        urls: list[str] = []
        for match in re.finditer(r'class="result__a"\s+href="([^"]+)"', html):
            raw_url = match.group(1)
            # 处理 DDG 重定向 URL（uddg= 参数）
            url = self._resolve_ddg_redirect(raw_url)
            if url and self._is_valid_blog_url(url):
                urls.append(url)
                if len(urls) >= self._MAX_RESULTS_PER_QUERY:
                    break

        return urls

    @staticmethod
    def _resolve_ddg_redirect(raw_url: str) -> str | None:
        """解析 DuckDuckGo 重定向 URL，提取真实目标。"""
        if "uddg=" in raw_url:
            parsed = urllib.parse.urlparse(raw_url)
            params = urllib.parse.parse_qs(parsed.query)
            uddg = params.get("uddg", [None])[0]
            if uddg:
                return urllib.parse.unquote(uddg)
        # 非重定向 URL 直接返回
        if raw_url.startswith("http"):
            return raw_url
        return None

    @staticmethod
    def _is_valid_blog_url(url: str) -> bool:
        """过滤非技术博文域名。"""
        try:
            domain = urllib.parse.urlparse(url).netloc.lower()
            # 去掉 www. 前缀
            if domain.startswith("www."):
                domain = domain[4:]
            return domain not in _DOMAIN_BLACKLIST
        except Exception:
            return False


class RSSChannel:
    """从 RSS/Atom 源获取文章 URL。"""

    _MAX_ENTRIES_PER_FEED = 20
    # 类级缓存：记录已知不可用的 RSS 源，避免重复请求
    _FEED_HEALTH: dict[str, bool] = {}

    def __init__(self, extra_feeds: list[str] | None = None):
        self._feeds = list(_DEFAULT_RSS_FEEDS)
        if extra_feeds:
            self._feeds.extend(extra_feeds)

    def fetch(self, keywords: list[str] | None = None) -> list[SearchHit]:
        """获取所有 RSS 源的文章链接，并归一化为 SearchHit。"""
        all_hits: list[SearchHit] = []
        seen: set[str] = set()

        for feed_url in self._feeds:
            hits = self._fetch_one(feed_url, keywords or [])
            for hit in hits:
                canonical = canonicalize_url(hit.url)
                if canonical in seen:
                    continue
                seen.add(canonical)
                all_hits.append(hit)

        logger.info("RSS 获取: %d 个源 → %d 个命中", len(self._feeds), len(all_hits))
        return all_hits

    def _fetch_one(self, feed_url: str, keywords: list[str]) -> list[SearchHit]:
        """获取单个 RSS/Atom feed 的文章链接。"""
        # 跳过已知不可用的源
        if self._FEED_HEALTH.get(feed_url) is False:
            logger.debug("跳过已知不可用的 RSS 源: %s", feed_url[:60])
            return []

        try:
            req = urllib.request.Request(
                feed_url,
                headers={"User-Agent": "claude-orchestrator/1.0"},
            )
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
                xml_bytes = resp.read()
        except Exception as e:
            logger.warning("RSS 获取失败 (%s): %s", feed_url[:60], e)
            self._FEED_HEALTH[feed_url] = False  # 标记为不可用
            return []

        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            logger.warning("RSS 解析失败 (%s): %s", feed_url[:60], e)
            self._FEED_HEALTH[feed_url] = False
            return []

        entries: list[tuple[str, str]] = []

        # 三层兜底：带命名空间 Atom → 无命名空间 Atom → RSS 2.0
        # 1. Atom (带命名空间)
        atom_ns = "{http://www.w3.org/2005/Atom}"
        for entry in root.findall(f".//{atom_ns}entry"):
            link = entry.find(f"{atom_ns}link[@href]")
            if link is not None:
                href = link.get("href", "")
                if href.startswith("http"):
                    title = entry.findtext(f"{atom_ns}title", default="") or ""
                    entries.append((href, title))

        # 2. Atom (无命名空间)
        if not entries:
            for entry in root.findall(".//entry"):
                link = entry.find("link[@href]")
                if link is not None:
                    href = link.get("href", "")
                    if href.startswith("http"):
                        title = entry.findtext("title", default="") or ""
                        entries.append((href, title))

        # 3. RSS 2.0
        if not entries:
            for item in root.findall(".//item"):
                link = item.find("link")
                if link is not None and link.text:
                    url = link.text.strip()
                    if url.startswith("http"):
                        title = item.findtext("title", default="") or ""
                        entries.append((url, title))

        keyword_text = " ".join(keywords).lower()
        hits: list[SearchHit] = []
        for index, (url, title) in enumerate(entries[: self._MAX_ENTRIES_PER_FEED], start=1):
            if not _is_valid_blog_url(url):
                continue
            if keyword_text and title:
                title_lower = title.lower()
                if not any(token and token in title_lower for token in keyword_text.split()):
                    # RSS 保持第一类来源，但仍尽量减少明显无关命中
                    if index > 5:
                        continue
            hits.append(
                SearchHit(
                    provider="rss",
                    query=keywords[0] if keywords else "",
                    url=url,
                    title=title.strip(),
                    rank=index,
                    provider_score=0.82,
                )
            )

        return hits


class RelevanceFilter:
    """两级过滤：规则快筛 + Claude 精筛。"""

    def __init__(
        self,
        claude_config: Any | None = None,
        limits_config: Any | None = None,
        budget_tracker: Any | None = None,
    ):
        self._claude_config = claude_config
        self._limits_config = limits_config
        self._budget_tracker = budget_tracker

    def filter(
        self,
        candidates: list[DiscoveredSource],
        keywords: list[str],
        discovery_config: DiscoveryConfig,
        *,
        explicit_keyword_count: int = 0,
        smart_mode: bool = False,
    ) -> list[DiscoveredSource]:
        """按 score 门禁，再对边界候选做 Claude metadata 审核。"""
        if not candidates:
            return []

        accepted: list[DiscoveredSource] = []
        borderline: list[DiscoveredSource] = []

        for candidate in candidates:
            topic_score = _keyword_relevance_score(candidate, keywords)
            candidate.topic_relevance_score = topic_score

            if topic_score <= 0:
                logger.debug(
                    "丢弃离题候选: score=%.2f url=%s",
                    candidate.source_score,
                    candidate.canonical_url,
                )
                continue

            if candidate.source_score >= discovery_config.min_source_score:
                accepted.append(candidate)
            elif (
                candidate.source_score >= max(0.0, discovery_config.min_source_score - _DISCOVERY_REVIEW_MARGIN)
                and (topic_score >= 0.16 or self._rule_check(candidate, keywords))
            ):
                borderline.append(candidate)
            else:
                logger.debug(
                    "丢弃低信任候选: score=%.2f url=%s",
                    candidate.source_score,
                    candidate.canonical_url,
                )

        logger.info("信任门禁: %d 通过, %d 待复核", len(accepted), len(borderline))

        if borderline:
            if self._should_short_circuit_claude_review(
                discovery_config,
                explicit_keyword_count=explicit_keyword_count,
                smart_mode=smart_mode,
            ):
                reviewed = [
                    candidate
                    for candidate in borderline
                    if candidate.topic_relevance_score >= 0.4 or self._rule_check(candidate, keywords)
                ]
                accepted.extend(reviewed)
                logger.info(
                    "规则复核短路: %d → %d (explicit_keywords=%d providers=%d)",
                    len(borderline),
                    len(reviewed),
                    explicit_keyword_count,
                    self._active_provider_count(discovery_config),
                )
            elif self._claude_config:
                reviewed = self._claude_filter(borderline, keywords, discovery_config)
                accepted.extend(reviewed)
                logger.info("Claude 元数据复核: %d → %d", len(borderline), len(reviewed))

        accepted.sort(
            key=lambda item: (
                -(item.source_score + item.topic_relevance_score * 0.2),
                -(item.topic_relevance_score),
                item.best_rank or 999,
                item.canonical_url,
            )
        )
        return accepted

    def _active_provider_count(self, discovery_config: DiscoveryConfig) -> int:
        disabled = set(discovery_config.disabled_providers)
        return sum(
            1
            for provider in discovery_config.enabled_providers
            if provider not in disabled and provider != "rss"
        )

    def _should_short_circuit_claude_review(
        self,
        discovery_config: DiscoveryConfig,
        *,
        explicit_keyword_count: int,
        smart_mode: bool,
    ) -> bool:
        if smart_mode or discovery_config.research_enabled or explicit_keyword_count < 3:
            return False

        active_provider_count = self._active_provider_count(discovery_config)
        if active_provider_count <= 2:
            return True
        return active_provider_count <= 3 and discovery_config.max_hits_per_provider <= 5

    def _rule_check(self, candidate: DiscoveredSource, keywords: list[str]) -> bool:
        """规则快筛：URL 路径含关键词 / 白名单域名 / 博客路径模式。"""
        parsed = urllib.parse.urlparse(candidate.canonical_url)
        domain = _domain_for_url(candidate.canonical_url)
        path_lower = parsed.path.lower()
        metadata_text = f"{candidate.title} {candidate.snippet}".lower()

        # 白名单域名直接通过
        if domain in _DOMAIN_WHITELIST:
            return True

        # URL 路径或标题摘要含关键词
        for kw in keywords[:10]:
            if kw.lower() in path_lower or kw.lower() in metadata_text:
                return True

        # 路径含博客模式
        blog_patterns = ["/blog/", "/post/", "/posts/", "/article/", "/articles/"]
        if any(p in path_lower for p in blog_patterns):
            return True

        # 路径含日期模式（如 /2024/01/ 或 /2024-01-15）
        if re.search(r"/20\d{2}[/-]\d{2}", path_lower):
            return True

        return False

    def _build_claude_prompt(
        self,
        candidates: list[DiscoveredSource],
        keywords: list[str],
        discovery_config: DiscoveryConfig,
    ) -> str:
        candidate_lines = []
        for candidate in candidates[:30]:
            candidate_lines.append(
                "\n".join(
                    [
                        f"- url: {candidate.url}",
                        f"  title: {candidate.title or '(none)'}",
                        f"  snippet: {candidate.snippet or '(none)'}",
                        f"  providers: {', '.join(candidate.providers) or '(none)'}",
                        f"  queries: {', '.join(candidate.queries) or '(none)'}",
                        f"  source_score: {candidate.source_score:.2f}",
                        f"  topic_relevance_score: {candidate.topic_relevance_score:.2f}",
                    ]
                )
            )

        return (
            "以下是一组外部技术文章候选，请判断哪些候选值得进入外部扫描阶段。\n"
            f"主题关键词: {', '.join(keywords[:10])}\n"
            f"最低信任分阈值: {discovery_config.min_source_score:.2f}\n"
            "请仅根据候选 metadata 判断相关性和可信度，输出应保留 URL 的 JSON 数组。\n\n"
            f"候选列表:\n{chr(10).join(candidate_lines)}"
        )

    def _claude_filter(
        self,
        candidates: list[DiscoveredSource],
        keywords: list[str],
        discovery_config: DiscoveryConfig,
    ) -> list[DiscoveredSource]:
        """用 Claude 判断边界候选是否值得保留。"""
        try:
            from .claude_cli import run_claude_task
            from .model import TaskNode

            prompt = self._build_claude_prompt(candidates, keywords, discovery_config)

            task = TaskNode(
                id="relevance_filter",
                prompt_template=prompt,
                model=getattr(self._claude_config, "default_model", DEFAULT_CLAUDE_MODEL),
                timeout=60,
            )
            result = run_claude_task(
                task=task,
                prompt=prompt,
                claude_config=self._claude_config,
                limits=self._limits_config,
                budget_tracker=self._budget_tracker,
            )

            if result.output:
                # 尝试解析 JSON 数组
                text = result.output.strip()
                # 提取 JSON 数组
                match = re.search(r"\[.*\]", text, re.DOTALL)
                if match:
                    filtered = json.loads(match.group(0))
                    if isinstance(filtered, list):
                        allowed = {
                            candidate.url: candidate
                            for candidate in candidates
                        }
                        return [
                            allowed[url]
                            for url in filtered
                            if isinstance(url, str) and url in allowed
                        ]

        except Exception as e:
            logger.warning("Claude 精筛失败，降级为全部保留: %s", e)

        return list(candidates)


class SmartKeywordExtractor:
    """让 Claude 读项目代码后自主决定搜索关键词和搜索模板。

    比 KeywordExtractor 的纯规则提取更智能：
    - 理解项目的技术栈和架构
    - 识别薄弱环节和可改进方向
    - 生成针对性的搜索关键词和领域模板
    """

    # 让 Claude 分析项目并输出关键词的 prompt
    _PROMPT = """你是一个技术顾问。请分析以下项目的源码结构和代码，然后：

1. 理解这个项目的核心功能、技术栈、架构模式
2. 识别项目中可以改进的方向（代码质量、架构、性能、可靠性、可观测性等）
3. 基于你的分析，生成用于搜索技术博文的关键词和搜索模板

输出严格 JSON 格式（不要 markdown 代码块）：
{{
  "project_summary": "一句话描述项目",
  "tech_stack": ["技术1", "技术2"],
  "improvement_areas": ["方向1", "方向2"],
  "keywords": ["keyword1", "keyword2", ...],
  "search_templates": [
    "{{keyword}} best practices",
    "Python {{keyword}} implementation"
  ]
}}

要求：
- keywords 数量 10-25 个，英文，具体且有搜索价值（如 "DAG retry backoff" 而非 "error"）
- search_templates 数量 3-5 个，用 {{keyword}} 占位符
- 关键词应覆盖：当前技术栈最佳实践、已知薄弱环节、相关领域前沿

以下是项目文件：

{file_contents}
"""

    def __init__(
        self,
        orchestrator_dir: Path,
        claude_config: Any | None = None,
        limits_config: Any | None = None,
        budget_tracker: Any | None = None,
    ):
        self._orchestrator_dir = orchestrator_dir
        self._claude_config = claude_config
        self._limits_config = limits_config
        self._budget_tracker = budget_tracker

    def extract(self) -> tuple[list[str], list[str]]:
        """让 Claude 分析项目，返回 (keywords, search_templates)。"""
        from .claude_cli import run_claude_task
        from .model import TaskNode

        # 收集项目源码（限制总量避免 token 爆炸）
        file_contents = self._collect_source_files()
        if not file_contents:
            logger.warning("smart-discover: 未找到源码文件，回退到规则提取")
            return [], []

        prompt = self._PROMPT.format(file_contents=file_contents)

        task = TaskNode(
            id="_smart_keyword_extract",
            prompt_template=prompt,
            model="sonnet",  # sonnet 性价比高，够用
            timeout=120,
        )

        result = run_claude_task(
            task=task,
            prompt=prompt,
            claude_config=self._claude_config,
            limits=self._limits_config,
            budget_tracker=self._budget_tracker,
            working_dir=str(self._orchestrator_dir),
        )

        if not result.output:
            logger.warning("smart-discover: Claude 无输出，回退到规则提取")
            return [], []

        return self._parse_result(result.output)

    def _collect_source_files(self, max_chars: int = 50000) -> str:
        """收集项目 Python 源码，截断到 max_chars。"""
        exclude_dirs = {"__pycache__", ".venv", ".git", ".idea", "venv", "node_modules"}
        parts: list[str] = []
        total = 0

        py_files = sorted(self._orchestrator_dir.rglob("*.py"))
        for py_file in py_files:
            if any(part in exclude_dirs or part.startswith("test-") for part in py_file.parts):
                continue
            try:
                content = py_file.read_text(encoding="utf-8")
            except Exception:
                continue

            rel_path = py_file.relative_to(self._orchestrator_dir)
            entry = f"--- {rel_path} ---\n{content}\n"

            if total + len(entry) > max_chars:
                # 截断：只取前 N 行
                remaining = max_chars - total
                if remaining > 200:
                    entry = entry[:remaining] + "\n... (截断)\n"
                    parts.append(entry)
                break

            parts.append(entry)
            total += len(entry)

        return "".join(parts)

    def _parse_result(self, text: str) -> tuple[list[str], list[str]]:
        """解析 Claude 输出的 JSON，提取 keywords 和 search_templates。"""
        # 尝试直接解析
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # 提取 JSON 块
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if not match:
                logger.warning("smart-discover: 无法解析 Claude 输出")
                return [], []
            try:
                data = json.loads(match.group(0))
            except json.JSONDecodeError:
                logger.warning("smart-discover: JSON 解析失败")
                return [], []

        keywords = data.get("keywords", [])
        templates = data.get("search_templates", [])

        if not isinstance(keywords, list):
            keywords = []
        if not isinstance(templates, list):
            templates = []

        # 清洗：确保都是字符串
        keywords = [str(k) for k in keywords if k]
        templates = [str(t) for t in templates if t and "{keyword}" in str(t)]

        logger.info("smart-discover: 提取 %d 个关键词, %d 个搜索模板", len(keywords), len(templates))
        logger.info("smart-discover 关键词: %s", keywords[:10])
        logger.info("smart-discover 模板: %s", templates)

        return keywords, templates


class ArticleDiscoverer:
    """顶层门面：组合关键词提取、Web 搜索、RSS 获取、相关性过滤。

    只负责发现 URL，内容获取和提案提取复用 ExternalSourceScanner。
    """

    def __init__(
        self,
        orchestrator_dir: str | Path,
        working_dir: str | Path,
        extra_keywords: list[str] | None = None,
        extra_rss_feeds: list[str] | None = None,
        max_results: int = 20,
        search_template: str = _DEFAULT_SEARCH_TEMPLATE,
        claude_config: Any | None = None,
        limits_config: Any | None = None,
        budget_tracker: Any | None = None,
        smart_mode: bool = False,
        pagination_config: PaginationConfig | None = None,
        discovery_config: DiscoveryConfig | None = None,
        keyword_extractor: KeywordExtractor | None = None,
        smart_extractor: SmartKeywordExtractor | None = None,
        search_registry: SearchProviderRegistry | None = None,
        rss_channel: RSSChannel | None = None,
        relevance_filter: RelevanceFilter | None = None,
        evidence_dir: str | Path | None = None,
        research_loop: DiscoveryResearchLoop | None = None,
    ):
        self._orchestrator_dir = Path(orchestrator_dir)
        self._working_dir = Path(working_dir)
        self._extra_keywords = extra_keywords or []
        self._max_results = max_results
        self._smart_mode = smart_mode
        self._pagination_config = pagination_config
        self._discovery_config = discovery_config or DiscoveryConfig()
        self._evidence_dir = Path(evidence_dir) if evidence_dir is not None else None

        self._keyword_extractor = keyword_extractor or KeywordExtractor(
            orchestrator_dir=self._orchestrator_dir,
            working_dir=self._working_dir,
        )
        self._smart_extractor = (
            smart_extractor
            or SmartKeywordExtractor(
                orchestrator_dir=self._orchestrator_dir,
                claude_config=claude_config,
                limits_config=limits_config,
                budget_tracker=budget_tracker,
            )
        ) if smart_mode else smart_extractor

        self._search_template = search_template
        self._search_registry = search_registry or build_default_search_registry()
        self._rss_channel = rss_channel or RSSChannel(extra_feeds=extra_rss_feeds)
        self._relevance_filter = relevance_filter or RelevanceFilter(
            claude_config=claude_config,
            limits_config=limits_config,
            budget_tracker=budget_tracker,
        )
        self._research_loop = research_loop

    def discover(self) -> list[DiscoveredSource]:
        """执行完整的发现流程，返回通过门禁的来源候选。"""
        logger.info("开始自动发现博文...")

        # 1. 提取关键词
        smart_templates: list[str] = []
        explicit_keywords = list(dict.fromkeys(keyword for keyword in self._extra_keywords if keyword))
        if self._smart_mode and self._smart_extractor:
            logger.info("smart-discover 模式：让 Claude 分析项目后决定搜索策略")
            smart_keywords, smart_templates = self._smart_extractor.extract()
            if smart_keywords:
                # smart 关键词优先，再合并规则提取和手动关键词
                rule_keywords = self._keyword_extractor.extract()
                keywords = list(dict.fromkeys(
                    smart_keywords + explicit_keywords + rule_keywords
                ))
                logger.info("smart-discover: 合并后 %d 个关键词", len(keywords))
            else:
                # smart 提取失败，回退到规则提取
                logger.info("smart-discover 回退到规则提取")
                keywords = self._keyword_extractor.extract()
                keywords = list(dict.fromkeys(keywords + explicit_keywords))
        else:
            if len(explicit_keywords) >= 3:
                keywords = explicit_keywords
                logger.info("显式关键词已足够 (%d)，跳过本地关键词提取", len(explicit_keywords))
            else:
                keywords = self._keyword_extractor.extract()
                keywords = list(dict.fromkeys(keywords + explicit_keywords))

        logger.info("最终关键词 (%d): %s", len(keywords), keywords[:10])

        # 2. 构建搜索查询并聚合多来源命中
        queries = self._build_queries(keywords, smart_templates)
        enabled_providers = [
            provider
            for provider in self._discovery_config.enabled_providers
            if provider not in set(self._discovery_config.disabled_providers)
            and provider != "rss"
        ]
        web_hits = self._search_registry.search(
            queries,
            enabled_providers=enabled_providers,
            max_results_per_provider=self._discovery_config.max_hits_per_provider,
            max_queries=self._discovery_config.max_queries,
        )
        rss_hits: list[SearchHit] = []
        rss_enabled = (
            "rss" in self._discovery_config.enabled_providers
            and "rss" not in set(self._discovery_config.disabled_providers)
        )
        if rss_enabled:
            rss_hits = self._rss_channel.fetch(keywords)

        merged = _merge_and_score_hits(web_hits + rss_hits)
        logger.info(
            "多来源发现: Web %d + RSS %d → %d 个归一化来源",
            len(web_hits),
            len(rss_hits),
            len(merged),
        )

        # 3. 基于 score 做门禁，并对边界候选做元数据复核
        filtered = self._relevance_filter.filter(
            merged,
            keywords,
            self._discovery_config,
            explicit_keyword_count=len(explicit_keywords),
            smart_mode=self._smart_mode,
        )
        logger.info("相关性过滤: %d → %d", len(merged), len(filtered))

        if self._discovery_config.research_enabled and filtered:
            research_loop = self._research_loop or DiscoveryResearchLoop(
                search_registry=self._search_registry,
                relevance_filter=self._relevance_filter,
                merge_hits=_merge_and_score_hits,
                discovery_config=self._discovery_config,
                evidence_dir=self._evidence_dir,
            )
            research_result = research_loop.run(
                keywords=keywords,
                initial_queries=queries,
                initial_hits=web_hits + rss_hits,
                initial_sources=filtered,
            )
            filtered = research_result.sources
            logger.info(
                "研究闭环: leads=%d claims=%d contradictions=%d executed=%d",
                len(research_result.dossier.leads),
                len(research_result.dossier.claims),
                len(research_result.dossier.contradictions),
                len(research_result.dossier.executed_probes),
            )

        # 5. 应用分页配置（如果提供）
        if self._pagination_config:
            original_count = len(filtered)
            if original_count > self._pagination_config.max_items:
                logger.warning(
                    "发现的 URL 数量 (%d) 超过分页配置的 max_items (%d)，将截断",
                    original_count,
                    self._pagination_config.max_items
                )

            # 使用 paginate_results 分页并截断
            pages = paginate_results(filtered, self._pagination_config)
            # 扁平化分页结果
            result = [source for page in pages for source in page]
        else:
            # 使用原有的 max_results 截断逻辑
            result = filtered[: self._max_results]

        logger.info("自动发现完成: 返回 %d 个来源", len(result))
        return result

    def _build_queries(self, keywords: list[str], smart_templates: list[str]) -> list[str]:
        templates = smart_templates or [self._search_template]
        queries: list[str] = []
        for index, keyword in enumerate(keywords):
            template = templates[index % len(templates)]
            query = template.format(keyword=keyword)
            if query not in queries:
                queries.append(query)
            if len(queries) >= self._discovery_config.max_queries:
                break
        return queries
