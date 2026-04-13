"""Normalized multi-provider search abstractions for self-discovery."""
from __future__ import annotations

from dataclasses import dataclass, field
from html import unescape
import json
import logging
import os
import re
import shutil
import subprocess
from typing import Protocol
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 15
_USER_AGENT = "Mozilla/5.0 (compatible; claude-orchestrator/1.0)"
_RESULT_TAG_RE = re.compile(r"<[^>]+>")
_GITHUB_TOKEN_ENV_VARS = ("GITHUB_TOKEN", "GH_TOKEN")


def canonicalize_url(url: str) -> str:
    """Normalize URLs so duplicate hits across providers merge reliably."""
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = parsed.path.rstrip("/") or "/"
    query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=False)
    filtered_query = [
        (key, value)
        for key, value in query_pairs
        if not key.lower().startswith("utm_")
        and key.lower() not in {"ref", "ref_src", "source", "fbclid", "gclid"}
    ]
    query = urllib.parse.urlencode(filtered_query, doseq=True)
    return urllib.parse.urlunparse((scheme, netloc, path, "", query, ""))


def _strip_tags(value: str) -> str:
    text = unescape(_RESULT_TAG_RE.sub("", value or ""))
    return " ".join(text.split()).strip()


def _requote_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    path = urllib.parse.quote(parsed.path, safe="/%:@")
    query = urllib.parse.quote(parsed.query, safe="=&%:@/+?,-._~")
    fragment = urllib.parse.quote(parsed.fragment, safe="%:@/+?,-._~")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))


@dataclass
class SearchHit:
    provider: str
    query: str
    url: str
    title: str = ""
    snippet: str = ""
    rank: int = 0
    provider_score: float = 0.0


@dataclass
class DiscoveredSource:
    url: str
    canonical_url: str
    resolved_url: str = ""
    title: str = ""
    snippet: str = ""
    providers: list[str] = field(default_factory=list)
    queries: list[str] = field(default_factory=list)
    ranks: dict[str, int] = field(default_factory=dict)
    provider_scores: dict[str, float] = field(default_factory=dict)
    source_score: float = 0.0
    topic_relevance_score: float = 0.0
    evidence_tier: str = ""
    source_profile: str = ""
    research_summary: str = ""
    research_artifact_path: str = ""
    research_rounds: int = 0

    @property
    def best_rank(self) -> int:
        if not self.ranks:
            return 0
        return min(self.ranks.values())


class SearchProvider(Protocol):
    name: str

    def search(self, query: str, *, max_results: int) -> list[SearchHit]:
        """Return normalized search hits for a single query."""


class SearchProviderRegistry:
    """Fan out queries to independent providers and merge normalized hits."""

    def __init__(self, *, failure_threshold: int = 2) -> None:
        self._providers: dict[str, SearchProvider] = {}
        self._failure_counts: dict[str, int] = {}
        self._failure_threshold = max(1, failure_threshold)

    def register(self, provider: SearchProvider) -> None:
        self._providers[provider.name] = provider
        self._failure_counts.setdefault(provider.name, 0)

    @property
    def providers(self) -> dict[str, SearchProvider]:
        return dict(self._providers)

    def search(
        self,
        queries: list[str],
        *,
        enabled_providers: list[str] | None = None,
        max_results_per_provider: int = 10,
        max_queries: int = 5,
    ) -> list[SearchHit]:
        hits: list[SearchHit] = []
        provider_names = enabled_providers or list(self._providers)

        for query in queries[:max_queries]:
            for provider_name in provider_names:
                provider = self._providers.get(provider_name)
                if provider is None:
                    continue
                if self._failure_counts.get(provider_name, 0) >= self._failure_threshold:
                    logger.debug("provider cooled down for this run: %s", provider_name)
                    continue
                try:
                    provider_hits = provider.search(query, max_results=max_results_per_provider)
                except Exception as exc:
                    self._failure_counts[provider_name] = self._failure_counts.get(provider_name, 0) + 1
                    logger.warning("search provider %s failed: %s", provider_name, exc)
                    continue

                for index, hit in enumerate(provider_hits, start=1):
                    rank = hit.rank or index
                    hits.append(
                        SearchHit(
                            provider=hit.provider or provider_name,
                            query=hit.query or query,
                            url=hit.url,
                            title=hit.title,
                            snippet=hit.snippet,
                            rank=rank,
                            provider_score=hit.provider_score,
                        )
                    )

        return hits


class _HtmlSearchProvider:
    name = ""
    provider_score = 0.5

    def search(self, query: str, *, max_results: int) -> list[SearchHit]:
        html = self._fetch_results(query)
        return self._parse_results(query, html)[:max_results]

    def _fetch_results(self, query: str) -> str:
        url, payload = self._build_request(query)
        request = urllib.request.Request(
            url,
            data=payload,
            headers={"User-Agent": _USER_AGENT},
        )
        with urllib.request.urlopen(request, timeout=_HTTP_TIMEOUT) as response:
            return response.read().decode("utf-8", errors="replace")

    def _build_request(self, query: str) -> tuple[str, bytes | None]:
        raise NotImplementedError

    def _parse_results(self, query: str, html: str) -> list[SearchHit]:
        raise NotImplementedError


class DuckDuckGoHtmlProvider(_HtmlSearchProvider):
    name = "duckduckgo_html"
    provider_score = 0.72
    _SEARCH_URL = "https://html.duckduckgo.com/html/"
    _BLOCK_RE = re.compile(
        r'<a[^>]*class="result__a"[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>'
        r'(?P<tail>.*?)(?:</div>|$)',
        re.DOTALL | re.IGNORECASE,
    )
    _SNIPPET_RE = re.compile(r'class="result__snippet"[^>]*>(?P<snippet>.*?)</', re.DOTALL | re.IGNORECASE)

    def _build_request(self, query: str) -> tuple[str, bytes | None]:
        return self._SEARCH_URL, urllib.parse.urlencode({"q": query}).encode("utf-8")

    def _parse_results(self, query: str, html: str) -> list[SearchHit]:
        hits: list[SearchHit] = []
        for index, match in enumerate(self._BLOCK_RE.finditer(html), start=1):
            href = match.group("href")
            url = self._resolve_redirect(href)
            if not url:
                continue
            snippet_match = self._SNIPPET_RE.search(match.group("tail"))
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=url,
                    title=_strip_tags(match.group("title")),
                    snippet=_strip_tags(snippet_match.group("snippet") if snippet_match else ""),
                    rank=index,
                    provider_score=self.provider_score,
                )
            )
        return hits

    @staticmethod
    def _resolve_redirect(raw_url: str) -> str | None:
        if "uddg=" in raw_url:
            parsed = urllib.parse.urlparse(raw_url)
            params = urllib.parse.parse_qs(parsed.query)
            target = params.get("uddg", [None])[0]
            if target:
                return urllib.parse.unquote(target)
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            return raw_url
        return None


class BraveWebProvider(_HtmlSearchProvider):
    name = "brave_web"
    provider_score = 0.68
    _SEARCH_URL = "https://search.brave.com/search"
    _TITLE_RE = re.compile(
        r'<a[^>]*href="(?P<href>https?://[^"]+)"[^>]*>(?P<title>.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )
    _SNIPPET_RE = re.compile(r'class="snippet-description"[^>]*>(?P<snippet>.*?)</', re.DOTALL | re.IGNORECASE)

    def _build_request(self, query: str) -> tuple[str, bytes | None]:
        return f"{self._SEARCH_URL}?{urllib.parse.urlencode({'q': query, 'source': 'web'})}", None

    def _parse_results(self, query: str, html: str) -> list[SearchHit]:
        hits: list[SearchHit] = []
        for index, match in enumerate(self._TITLE_RE.finditer(html), start=1):
            search_window = html[match.end(): match.end() + 800]
            snippet_match = self._SNIPPET_RE.search(search_window)
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=match.group("href"),
                    title=_strip_tags(match.group("title")),
                    snippet=_strip_tags(snippet_match.group("snippet") if snippet_match else ""),
                    rank=index,
                    provider_score=self.provider_score,
                )
            )
        return hits


class BingWebProvider(_HtmlSearchProvider):
    name = "bing_web"
    provider_score = 0.66
    _SEARCH_URL = "https://www.bing.com/search"
    _BLOCK_RE = re.compile(
        r'<li[^>]*class="b_algo"[^>]*>(?P<body>.*?)</li>',
        re.DOTALL | re.IGNORECASE,
    )
    _TITLE_RE = re.compile(r'<h2>\s*<a[^>]*href="(?P<href>https?://[^"]+)"[^>]*>(?P<title>.*?)</a>', re.DOTALL | re.IGNORECASE)
    _SNIPPET_RE = re.compile(r'<p>(?P<snippet>.*?)</p>', re.DOTALL | re.IGNORECASE)

    def _build_request(self, query: str) -> tuple[str, bytes | None]:
        return f"{self._SEARCH_URL}?{urllib.parse.urlencode({'q': query, 'setlang': 'en-US'})}", None

    def _parse_results(self, query: str, html: str) -> list[SearchHit]:
        hits: list[SearchHit] = []
        for index, match in enumerate(self._BLOCK_RE.finditer(html), start=1):
            body = match.group("body")
            title_match = self._TITLE_RE.search(body)
            if not title_match:
                continue
            snippet_match = self._SNIPPET_RE.search(body)
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=title_match.group("href"),
                    title=_strip_tags(title_match.group("title")),
                    snippet=_strip_tags(snippet_match.group("snippet") if snippet_match else ""),
                    rank=index,
                    provider_score=self.provider_score,
                )
            )
        return hits


class SogouWeChatProvider(_HtmlSearchProvider):
    name = "sogou_wechat"
    provider_score = 0.58
    _SEARCH_URL = "https://weixin.sogou.com/weixin"
    _TITLE_RE = re.compile(
        r'<a[^>]*href="(?P<href>/link\?url=[^"]+)"[^>]*uigs="article_title_\d+"[^>]*>(?P<title>.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )
    _SUMMARY_RE = re.compile(r'<p[^>]*class="txt-info"[^>]*>(?P<summary>.*?)</p>', re.DOTALL | re.IGNORECASE)
    _ACCOUNT_RE = re.compile(r'<span[^>]*class="all-time-y2"[^>]*>(?P<account>.*?)</span>', re.DOTALL | re.IGNORECASE)

    def _build_request(self, query: str) -> tuple[str, bytes | None]:
        return f"{self._SEARCH_URL}?{urllib.parse.urlencode({'type': 2, 'query': query})}", None

    def _parse_results(self, query: str, html: str) -> list[SearchHit]:
        hits: list[SearchHit] = []
        for index, match in enumerate(self._TITLE_RE.finditer(html), start=1):
            search_window = html[match.end(): match.end() + 1500]
            summary_match = self._SUMMARY_RE.search(search_window)
            account_match = self._ACCOUNT_RE.search(search_window)
            account = _strip_tags(account_match.group("account") if account_match else "")
            summary = _strip_tags(summary_match.group("summary") if summary_match else "")
            snippet_parts = []
            if account:
                snippet_parts.append(f"公众号: {account}")
            if summary:
                snippet_parts.append(summary)
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=self._resolve_result_url(match.group("href")),
                    title=_strip_tags(match.group("title")),
                    snippet=" | ".join(snippet_parts),
                    rank=index,
                    provider_score=self.provider_score,
                )
            )
        return hits

    @staticmethod
    def _resolve_result_url(href: str) -> str:
        resolved = href.replace("&amp;", "&")
        if resolved.startswith("/"):
            return _requote_url(f"https://weixin.sogou.com{resolved}")
        return _requote_url(resolved)


class GitHubSearchProvider:
    name = "github_search"
    provider_score = 0.7
    _API_BASE = "https://api.github.com"
    _API_VERSION = "2022-11-28"

    def __init__(self, *, token_env_vars: tuple[str, ...] = _GITHUB_TOKEN_ENV_VARS) -> None:
        self._token_env_vars = token_env_vars
        self._cached_token: str | None = None

    def search(self, query: str, *, max_results: int) -> list[SearchHit]:
        token = self._resolve_token()
        budgets = self._allocate_budgets(max_results=max(1, max_results), authenticated=bool(token))
        result_sets = [
            self._search_repositories(query, budgets["repositories"], token),
            self._search_issues(query, budgets["issues"], token),
        ]
        if budgets.get("code", 0) > 0:
            result_sets.append(self._search_code(query, budgets["code"], token))
        return self._interleave(result_sets, max_results=max_results)

    def _resolve_token(self) -> str:
        if self._cached_token is not None:
            return self._cached_token
        for env_name in self._token_env_vars:
            token = os.getenv(env_name, "").strip()
            if token:
                self._cached_token = token
                return token
        self._cached_token = self._resolve_windows_env_token()
        return self._cached_token

    def _resolve_windows_env_token(self) -> str:
        powershell = shutil.which("powershell.exe")
        if not powershell:
            return ""
        command = (
            "$names=@('GITHUB_TOKEN','GH_TOKEN');"
            "foreach($scope in @('Process','User','Machine')){"
            "foreach($name in $names){"
            "$value=[Environment]::GetEnvironmentVariable($name,$scope);"
            "if($value){[Console]::Write($value);exit 0}"
            "}"
            "}"
        )
        try:
            completed = subprocess.run(
                [powershell, "-NoProfile", "-Command", command],
                check=False,
                capture_output=True,
                text=True,
                timeout=3,
            )
        except Exception:
            return ""
        return (completed.stdout or "").strip()

    @staticmethod
    def _allocate_budgets(*, max_results: int, authenticated: bool) -> dict[str, int]:
        search_types = ["repositories", "issues"]
        if authenticated:
            search_types.append("code")
        base = max(1, max_results // len(search_types))
        remainder = max_results - base * len(search_types)
        budgets: dict[str, int] = {}
        for index, search_type in enumerate(search_types):
            budgets[search_type] = base + (1 if index < remainder else 0)
        return budgets

    def _build_headers(self, token: str, *, text_match: bool = False) -> dict[str, str]:
        accept = "application/vnd.github+json"
        if text_match:
            accept = "application/vnd.github.text-match+json"
        headers = {
            "User-Agent": _USER_AGENT,
            "Accept": accept,
            "X-GitHub-Api-Version": self._API_VERSION,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _fetch_json(
        self,
        path: str,
        *,
        params: dict[str, str | int],
        token: str,
        text_match: bool = False,
    ) -> dict:
        url = f"{self._API_BASE}{path}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(
            url,
            headers=self._build_headers(token, text_match=text_match),
        )
        try:
            with urllib.request.urlopen(request, timeout=_HTTP_TIMEOUT) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            logger.warning("github search request failed (%s %s): %s", path, exc.code, body[:240])
            return {}
        except urllib.error.URLError as exc:
            logger.warning("github search request failed (%s): %s", path, exc)
            return {}

    def _search_repositories(self, query: str, max_results: int, token: str) -> list[SearchHit]:
        payload = self._fetch_json(
            "/search/repositories",
            params={
                "q": f"{query} archived:false",
                "per_page": max_results,
                "sort": "stars",
                "order": "desc",
            },
            token=token,
        )
        hits: list[SearchHit] = []
        for index, item in enumerate(payload.get("items", []), start=1):
            stars = item.get("stargazers_count", 0)
            language = item.get("language") or ""
            snippet_parts = [item.get("description") or ""]
            meta = " | ".join(
                part
                for part in [
                    item.get("full_name") or "",
                    f"stars {stars}" if isinstance(stars, int) else "",
                    language,
                ]
                if part
            )
            if meta:
                snippet_parts.append(meta)
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=item.get("html_url") or "",
                    title=item.get("full_name") or item.get("name") or "",
                    snippet=" | ".join(part for part in snippet_parts if part),
                    rank=index,
                    provider_score=0.68,
                )
            )
        return [hit for hit in hits if hit.url]

    def _search_issues(self, query: str, max_results: int, token: str) -> list[SearchHit]:
        payload = self._fetch_json(
            "/search/issues",
            params={
                "q": f"{query} is:issue",
                "per_page": max_results,
                "sort": "updated",
                "order": "desc",
            },
            token=token,
        )
        hits: list[SearchHit] = []
        for index, item in enumerate(payload.get("items", []), start=1):
            repo_name = self._repo_name_from_api_url(item.get("repository_url") or "")
            body = self._trim_snippet(item.get("body") or "")
            snippet_parts = [
                repo_name,
                item.get("state") or "",
                f"comments {item.get('comments')}" if item.get("comments") is not None else "",
                body,
            ]
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=item.get("html_url") or "",
                    title=item.get("title") or "",
                    snippet=" | ".join(part for part in snippet_parts if part),
                    rank=index,
                    provider_score=0.72,
                )
            )
        return [hit for hit in hits if hit.url]

    def _search_code(self, query: str, max_results: int, token: str) -> list[SearchHit]:
        if not token:
            return []
        payload = self._fetch_json(
            "/search/code",
            params={
                "q": query,
                "per_page": max_results,
            },
            token=token,
            text_match=True,
        )
        hits: list[SearchHit] = []
        for index, item in enumerate(payload.get("items", []), start=1):
            repo = (item.get("repository") or {}).get("full_name") or ""
            snippet_parts = [repo, item.get("path") or ""]
            text_matches = item.get("text_matches") or []
            if text_matches:
                snippet_parts.append(self._trim_snippet(" ".join(match.get("fragment", "") for match in text_matches)))
            hits.append(
                SearchHit(
                    provider=self.name,
                    query=query,
                    url=item.get("html_url") or "",
                    title=f"{repo}/{item.get('path')}".strip("/"),
                    snippet=" | ".join(part for part in snippet_parts if part),
                    rank=index,
                    provider_score=0.74,
                )
            )
        return [hit for hit in hits if hit.url]

    @staticmethod
    def _repo_name_from_api_url(api_url: str) -> str:
        if not api_url:
            return ""
        parts = urllib.parse.urlparse(api_url).path.strip("/").split("/")
        if len(parts) >= 3 and parts[0] == "repos":
            return "/".join(parts[1:3])
        return ""

    @staticmethod
    def _trim_snippet(value: str, limit: int = 220) -> str:
        text = " ".join((value or "").split())
        if len(text) <= limit:
            return text
        return text[: limit - 1].rstrip() + "…"

    @staticmethod
    def _interleave(result_sets: list[list[SearchHit]], *, max_results: int) -> list[SearchHit]:
        merged: list[SearchHit] = []
        seen_urls: set[str] = set()
        while len(merged) < max_results and any(result_sets):
            progressed = False
            for result_set in result_sets:
                if not result_set or len(merged) >= max_results:
                    continue
                hit = result_set.pop(0)
                canonical = canonicalize_url(hit.url)
                if canonical in seen_urls:
                    continue
                seen_urls.add(canonical)
                merged.append(hit)
                progressed = True
            if not progressed:
                break
        return merged
