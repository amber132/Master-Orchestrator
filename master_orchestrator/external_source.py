"""外部文档扫描器：从 URL 或本地文件获取内容，调用 Claude 提取改进建议。"""

from __future__ import annotations

import base64
import json
import logging
import re
import tempfile
import time
import uuid
import http.cookiejar
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

from .auto_model import (
    ImprovementPriority,
    ImprovementProposal,
    ImprovementSource,
)
from .claude_cli import BudgetTracker, run_claude_task
from .config import ClaudeConfig, LimitsConfig
from .json_utils import robust_parse_json
from .model import TaskNode, TaskStatus
from .sanitizer import PromptSanitizer
from .search_provider import DiscoveredSource, GitHubSearchProvider

logger = logging.getLogger(__name__)

# URL 获取超时
_URL_TIMEOUT = 30
# 单个文档最大字符数（防止超长文档撑爆 prompt）
_MAX_DOC_CHARS = 50_000
_MP_WEIXIN_URL_RE = re.compile(r"https://mp\.weixin\.qq\.com/s[^\s\"'<>]+", re.IGNORECASE)
_APPROVE_URL_CHUNK_RE = re.compile(r"url \+= '([^']*)';")
_GITHUB_REPO_RE = re.compile(r"^https?://(?:www\.)?github\.com/(?P<owner>[^/\s]+)/(?P<repo>[^/\s?#]+?)(?:\.git)?(?:/)?(?:[?#].*)?$", re.IGNORECASE)
_GITHUB_ISSUE_RE = re.compile(r"^https?://(?:www\.)?github\.com/(?P<owner>[^/\s]+)/(?P<repo>[^/\s?#]+?)/issues/(?P<number>\d+)(?:[/?#].*)?$", re.IGNORECASE)
_GITHUB_BLOB_RE = re.compile(r"^https?://(?:www\.)?github\.com/(?P<owner>[^/\s]+)/(?P<repo>[^/\s?#]+?)/blob/(?P<blob_path>[^?#]+)", re.IGNORECASE)

_EXTRACT_SYSTEM_PROMPT = """\
你是一位技术顾问。你正在审阅一份外部文档，目标是从中提取与当前系统改进焦点相关的建议。

这是一个闭卷提取任务。你唯一允许使用的信息，是当前 prompt 中给出的来源元数据、焦点关键词和文档内容。
绝对禁止：
- 读取本地仓库、工作目录、技能说明、README、源码、测试或任意额外文件
- 访问与当前来源无关的额外网页、搜索结果或网络资源
- 根据你对项目的猜测补充文档里没有出现的事实
- 输出无法从当前文档内容直接支撑的建议

如果当前文档不足以支撑建议，直接输出 []。

系统能力概述：
- 基于 Claude CLI 的 DAG 任务编排（并行执行、依赖管理、重试）
- 自主目标驱动模式（目标分解 → 阶段执行 → AI 审查 → 迭代优化）
- 质量门禁（外部命令硬检查）、回归检测、恶化检测
- 迭代间 Handoff Protocol（结构化上下文传递）
- 失败分类与自适应重试策略
- GoalState 持久化与断点续传
- 外部 discovery / network search / 文档扫描 / 来源归一化
- GitHub 搜索、公众号来源发现、反爬降级与证据落盘

输出要求：
- 输出一个 JSON 数组，每条包含以下字段：
  title, description, rationale, priority (critical/high/medium/low),
  affected_files (文件名列表), complexity (small/medium/large), evidence (引用原文)
- 只输出 JSON 数组，不要包含其他文本
- 只提取与当前焦点直接相关、且可转化为本项目代码改进的建议，忽略无关内容
- 如果文档中没有相关建议，输出空数组 []
"""

_GENERIC_TECH_SIGNALS = (
    "dag", "orchestrat", "pipeline", "retry", "agent",
    "concurrent", "parallel", "workflow", "queue",
    "async", "thread", "subprocess", "cli", "api",
    "llm", "prompt", "token", "model", "inference",
    "scheduler", "executor", "dependency", "graph",
)
_FOCUS_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_./+-]{2,}|[\u4e00-\u9fff]{2,}", re.IGNORECASE)


def _fetch_url(url: str) -> str:
    """用 urllib 获取 URL 内容，返回文本。先 HEAD 预检可达性和内容类型。"""
    logger.info("获取 URL: %s", url)

    # HEAD 预检：快速检测 403/404 和非文本内容，避免浪费带宽
    head_req = urllib.request.Request(
        url,
        method="HEAD",
        headers={"User-Agent": "claude-orchestrator/0.1"},
    )
    try:
        with urllib.request.urlopen(head_req, timeout=10) as resp:
            ct = resp.headers.get("Content-Type", "")
            if ct and not any(t in ct for t in ("text/", "application/json", "application/xml", "application/xhtml")):
                logger.info("跳过非文本 URL (Content-Type=%s): %s", ct, url)
                return ""
    except urllib.error.HTTPError as e:
        if e.code in (404, 410, 451):
            logger.info("URL 不可达 (HTTP %d): %s", e.code, url)
            return ""
        elif e.code == 429:
            logger.info("URL 限流 (429)，等待后尝试 GET: %s", url)
            time.sleep(2)
        elif e.code == 403:
            logger.debug("HEAD 返回 403，继续尝试 GET: %s", url)
        # 其他 HTTP 错误继续尝试 GET（有些服务器不支持 HEAD）
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        logger.debug("HEAD 请求失败，继续 GET: %s", e)

    # GET 请求：添加完整的异常处理
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "claude-orchestrator/0.1"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_URL_TIMEOUT) as resp:
            raw = resp.read()
            # 尝试从 Content-Type 获取编码
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        logger.warning("HTTP 错误 %d: %s", e.code, url)
        return ""
    except urllib.error.URLError as e:
        logger.warning("URL 错误: %s - %s", url, e.reason)
        return ""
    except TimeoutError:
        logger.warning("请求超时: %s", url)
        return ""
    except OSError as e:
        logger.warning("网络错误: %s - %s", url, e)
        return ""


def _read_local_file(path: str) -> str:
    """读取本地文件内容。"""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    return p.read_text(encoding="utf-8", errors="replace")


class _ContentExtractor(HTMLParser):
    """语义化 HTML 内容提取器。

    优先提取 <main>/<article> 内的文本，跳过导航/页脚等噪音标签。
    保留标题、段落、列表、代码块的结构（用换行分隔）。
    """

    # 需要跳过内容的标签
    _SKIP_TAGS = frozenset({"script", "style", "nav", "footer", "aside", "header", "noscript"})
    # 语义化内容标签（优先提取）
    _CONTENT_TAGS = frozenset({"main", "article"})
    # 块级标签（前后加换行）
    _BLOCK_TAGS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "pre", "blockquote", "div", "section"})

    def __init__(self) -> None:
        super().__init__()
        self._all_text: list[str] = []       # 全文本（回退用）
        self._content_text: list[str] = []   # <main>/<article> 内的文本
        self._skip_depth: int = 0            # 跳过标签嵌套深度
        self._content_depth: int = 0         # 语义内容标签嵌套深度
        self._in_block: bool = False         # 当前是否在块级标签内

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag_lower = tag.lower()
        if tag_lower in self._SKIP_TAGS:
            self._skip_depth += 1
            return
        if tag_lower in self._CONTENT_TAGS:
            self._content_depth += 1
        if tag_lower in self._BLOCK_TAGS:
            self._in_block = True
            # 块级标签前加换行
            if self._content_depth > 0:
                self._content_text.append("\n")
            self._all_text.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower in self._SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if tag_lower in self._CONTENT_TAGS:
            self._content_depth = max(0, self._content_depth - 1)
        if tag_lower in self._BLOCK_TAGS:
            self._in_block = False
            if self._content_depth > 0:
                self._content_text.append("\n")
            self._all_text.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = data.strip()
        if not text:
            return
        self._all_text.append(text)
        if self._content_depth > 0:
            self._content_text.append(text)

    def get_text(self) -> str:
        """返回提取的文本。优先返回语义标签内容，回退到全文本。"""
        # 优先使用 <main>/<article> 内的内容
        if self._content_text:
            raw = " ".join(self._content_text)
        else:
            raw = " ".join(self._all_text)
        # 合并多余空白，保留段落换行
        text = re.sub(r"[ \t]+", " ", raw)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def _strip_html(html: str) -> str:
    """语义化 HTML 清洗：优先提取 main/article 内容，跳过导航/页脚噪音。"""
    extractor = _ContentExtractor()
    extractor.feed(html)
    return extractor.get_text()


def _load_source(source: str) -> str:
    """加载单个来源（URL 或本地文件），返回清洗后的文本。"""
    if source.startswith("http://") or source.startswith("https://"):
        raw = _fetch_url(source)
        # 如果看起来是 HTML，做清洗
        if "<html" in raw.lower()[:500] or "<body" in raw.lower()[:500]:
            return _strip_html(raw)[:_MAX_DOC_CHARS]
        return raw[:_MAX_DOC_CHARS]
    else:
        return _read_local_file(source)[:_MAX_DOC_CHARS]


def _sanitize_http_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    path = urllib.parse.quote(parsed.path, safe="/%:@")
    query = urllib.parse.quote(parsed.query, safe="=&%:@/+?,-._~")
    fragment = urllib.parse.quote(parsed.fragment, safe="%:@/+?,-._~")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))


def _looks_like_antispider_page(content: str) -> bool:
    lowered = content.lower()
    return (
        "antispider.min.js" in lowered
        or "验证码" in content
        or "verifycode" in lowered
        or "seccodeform" in lowered
    )


class SogouWeChatResolver:
    """Resolve sogou wechat result links into article URLs when browser state is available."""

    def __init__(
        self,
        *,
        cookie_header: str = "",
        cookie_file: str = "",
        storage_state_path: str = "",
    ) -> None:
        self._cookie_header = cookie_header.strip()
        self._cookie_file = cookie_file.strip()
        self._storage_state_path = storage_state_path.strip()

    def resolve(self, source_url: str) -> tuple[str, str]:
        source_url = _sanitize_http_url(source_url)
        cookie_header = self._build_cookie_header()
        content, final_url = self._fetch(source_url, cookie_header)
        normalized_final_url = self._normalize_wechat_target_url(final_url)
        if "mp.weixin.qq.com/" in normalized_final_url and not self._looks_like_wechat_captcha_page(final_url, content):
            return normalized_final_url, content
        if not _looks_like_antispider_page(content):
            direct_url = self._extract_mp_weixin_url(content)
            if direct_url:
                return self._fetch_resolved_article(direct_url)
            if normalized_final_url != final_url:
                return normalized_final_url, ""
        resolved_url, article_content = self._resolve_via_session(source_url, cookie_header)
        if resolved_url:
            return resolved_url, article_content
        return "", ""

    def _fetch(self, url: str, cookie_header: str) -> tuple[str, str]:
        url = _sanitize_http_url(url)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; claude-orchestrator/1.0)"}
        if cookie_header:
            headers["Cookie"] = cookie_header
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=_URL_TIMEOUT) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace"), resp.geturl()

    def _build_cookie_header(self) -> str:
        if self._cookie_header:
            return self._cookie_header
        if self._storage_state_path:
            cookies = self._load_storage_state_cookies(Path(self._storage_state_path))
            if cookies:
                return self._cookies_to_header(cookies)
        if self._cookie_file:
            cookies = self._load_cookie_file(Path(self._cookie_file))
            if cookies:
                return self._cookies_to_header(cookies)
        return ""

    def _resolve_via_session(self, source_url: str, cookie_header: str) -> tuple[str, str]:
        opener = self._build_cookie_opener()
        warmup_url = self._build_warmup_url(source_url)
        if warmup_url:
            try:
                self._open_with_opener(opener, warmup_url, cookie_header)
            except Exception:
                logger.debug("sogou warmup failed: %s", warmup_url, exc_info=True)
        try:
            content, final_url = self._open_with_opener(opener, source_url, cookie_header)
        except Exception:
            return "", ""

        normalized_final_url = self._normalize_wechat_target_url(final_url)
        if "mp.weixin.qq.com/" in normalized_final_url and not self._looks_like_wechat_captcha_page(final_url, content):
            return normalized_final_url, content

        direct_url = self._extract_mp_weixin_url(content)
        if not direct_url and normalized_final_url != final_url:
            direct_url = normalized_final_url
        if not direct_url:
            return "", ""
        return self._fetch_resolved_article(direct_url, opener=opener)

    def _fetch_resolved_article(
        self,
        direct_url: str,
        *,
        opener: urllib.request.OpenerDirector | None = None,
    ) -> tuple[str, str]:
        try:
            if opener is None:
                article_content, article_final_url = self._fetch(direct_url, "")
            else:
                article_content, article_final_url = self._open_with_opener(opener, direct_url, "")
        except Exception:
            return direct_url, ""

        resolved_url = self._normalize_wechat_target_url(article_final_url) or direct_url
        if self._looks_like_wechat_captcha_page(article_final_url, article_content):
            return resolved_url, ""
        return resolved_url, article_content

    def _build_cookie_opener(self) -> urllib.request.OpenerDirector:
        jar = http.cookiejar.CookieJar()
        for domain, path, name, value, secure in self._load_session_cookie_records():
            jar.set_cookie(
                http.cookiejar.Cookie(
                    version=0,
                    name=name,
                    value=value,
                    port=None,
                    port_specified=False,
                    domain=domain,
                    domain_specified=bool(domain),
                    domain_initial_dot=domain.startswith("."),
                    path=path or "/",
                    path_specified=True,
                    secure=secure,
                    expires=None,
                    discard=True,
                    comment=None,
                    comment_url=None,
                    rest={},
                    rfc2109=False,
                )
            )
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
        opener.addheaders = [("User-Agent", "Mozilla/5.0 (compatible; claude-orchestrator/1.0)")]
        return opener

    def _open_with_opener(
        self,
        opener: urllib.request.OpenerDirector,
        url: str,
        cookie_header: str,
    ) -> tuple[str, str]:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; claude-orchestrator/1.0)"}
        if cookie_header and "sogou.com" in urllib.parse.urlparse(url).netloc:
            headers["Cookie"] = cookie_header
        req = urllib.request.Request(url, headers=headers)
        with opener.open(req, timeout=_URL_TIMEOUT) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace"), resp.geturl()

    def _load_session_cookie_records(self) -> list[tuple[str, str, str, str, bool]]:
        if self._storage_state_path:
            records = self._load_storage_state_cookie_records(Path(self._storage_state_path))
            if records:
                return records
        if self._cookie_file:
            records = self._load_cookie_file_records(Path(self._cookie_file))
            if records:
                return records
        return []

    def _load_storage_state_cookies(self, path: Path) -> list[tuple[str, str]]:
        return [
            (name, value)
            for domain, _path, name, value, _secure in self._load_storage_state_cookie_records(path)
            if "sogou.com" in domain
        ]

    def _load_storage_state_cookie_records(self, path: Path) -> list[tuple[str, str, str, str, bool]]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        return self._load_cookie_item_records(payload.get("cookies", []))

    def _load_cookie_file(self, path: Path) -> list[tuple[str, str]]:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return []

        if text.lstrip().startswith("{") or text.lstrip().startswith("["):
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, list):
                return self._load_cookie_items(payload)
            if isinstance(payload, dict):
                if isinstance(payload.get("cookies"), list):
                    return self._load_cookie_items(payload["cookies"])
                return [(str(k), str(v)) for k, v in payload.items() if k and v]

        cookies: list[tuple[str, str]] = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7 and "sogou.com" in parts[0]:
                cookies.append((parts[5], parts[6]))
        return cookies

    def _load_cookie_file_records(self, path: Path) -> list[tuple[str, str, str, str, bool]]:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            return []

        if text.lstrip().startswith("{") or text.lstrip().startswith("["):
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, list):
                return self._load_cookie_item_records(payload)
            if isinstance(payload, dict) and isinstance(payload.get("cookies"), list):
                return self._load_cookie_item_records(payload["cookies"])
            return []

        cookies: list[tuple[str, str, str, str, bool]] = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            domain = parts[0].strip()
            if not self._is_supported_session_domain(domain):
                continue
            cookies.append((domain, parts[2].strip() or "/", parts[5].strip(), parts[6].strip(), parts[3].upper() == "TRUE"))
        return cookies

    def _load_cookie_items(self, items: Iterable[object]) -> list[tuple[str, str]]:
        return [
            (name, value)
            for domain, _path, name, value, _secure in self._load_cookie_item_records(items)
            if "sogou.com" in domain or not domain
        ]

    def _load_cookie_item_records(self, items: Iterable[object]) -> list[tuple[str, str, str, str, bool]]:
        cookies: list[tuple[str, str, str, str, bool]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain", "")).strip()
            if domain and not self._is_supported_session_domain(domain):
                continue
            name = str(item.get("name", "")).strip()
            value = str(item.get("value", "")).strip()
            if name:
                cookies.append(
                    (
                        domain,
                        str(item.get("path", "/")).strip() or "/",
                        name,
                        value,
                        bool(item.get("secure", False)),
                    )
                )
        return cookies

    @staticmethod
    def _is_supported_session_domain(domain: str) -> bool:
        domain = domain.lower()
        return (
            "sogou.com" in domain
            or domain.endswith("weixin.qq.com")
            or domain.endswith(".weixin.qq.com")
            or domain == "qq.com"
            or domain.endswith(".qq.com")
        )

    @staticmethod
    def _cookies_to_header(cookies: Iterable[tuple[str, str]]) -> str:
        return "; ".join(f"{name}={value}" for name, value in cookies if name)

    def _build_warmup_url(self, source_url: str) -> str:
        parsed = urllib.parse.urlparse(source_url)
        if "weixin.sogou.com" not in parsed.netloc:
            return ""
        query = urllib.parse.parse_qs(parsed.query).get("query", [""])[0].strip()
        if not query:
            return ""
        return urllib.parse.urlunparse(
            (
                "https",
                "weixin.sogou.com",
                "/weixin",
                "",
                urllib.parse.urlencode({"type": 2, "query": query}),
                "",
            )
        )

    @staticmethod
    def _normalize_wechat_target_url(url: str) -> str:
        if "target_url=" not in url:
            return url
        parsed = urllib.parse.urlparse(url)
        target = urllib.parse.parse_qs(parsed.query).get("target_url", [""])[0]
        return urllib.parse.unquote(target) or url

    @staticmethod
    def _looks_like_wechat_captcha_page(final_url: str, content: str) -> bool:
        lowered_url = final_url.lower()
        lowered_content = content.lower()
        return "wappoc_appmsgcaptcha" in lowered_url or "wappoc_appmsgcaptcha" in lowered_content

    @classmethod
    def _extract_mp_weixin_url(cls, content: str) -> str:
        match = _MP_WEIXIN_URL_RE.search(content)
        if match:
            return match.group(0)
        if "window.location.replace(url)" not in content:
            return ""
        chunks = _APPROVE_URL_CHUNK_RE.findall(content)
        candidate = "".join(chunks).replace("@", "")
        if "mp.weixin.qq.com/" in candidate:
            return candidate
        return ""


class GitHubContentResolver:
    """Load structured content from GitHub URLs instead of scanning HTML pages."""

    _API_BASE = "https://api.github.com"

    def __init__(self, github_provider: GitHubSearchProvider | None = None) -> None:
        self._github_provider = github_provider or GitHubSearchProvider()

    def resolve(self, source_url: str) -> tuple[str, str]:
        source_url = source_url.strip()
        if not source_url:
            return "", ""

        issue_match = _GITHUB_ISSUE_RE.match(source_url)
        if issue_match:
            owner = issue_match.group("owner")
            repo = issue_match.group("repo")
            number = issue_match.group("number")
            content = self._load_issue(owner, repo, number)
            return source_url, content

        blob_match = _GITHUB_BLOB_RE.match(source_url)
        if blob_match:
            owner = blob_match.group("owner")
            repo = blob_match.group("repo")
            blob_path = urllib.parse.unquote(blob_match.group("blob_path"))
            content = self._load_blob(owner, repo, blob_path)
            return source_url, content

        repo_match = _GITHUB_REPO_RE.match(source_url)
        if repo_match:
            owner = repo_match.group("owner")
            repo = repo_match.group("repo")
            content = self._load_repository_readme(owner, repo)
            return source_url, content

        return "", ""

    def _load_repository_readme(self, owner: str, repo: str) -> str:
        payload = self._fetch_json(f"/repos/{owner}/{repo}/readme")
        readme = self._decode_file_content(payload)
        if not readme:
            return ""
        lines = [
            "GitHub repository README",
            f"Repository: {owner}/{repo}",
            "",
            readme,
        ]
        return "\n".join(lines).strip()

    def _load_issue(self, owner: str, repo: str, number: str) -> str:
        payload = self._fetch_json(f"/repos/{owner}/{repo}/issues/{number}")
        if not isinstance(payload, dict) or not payload:
            return ""
        title = str(payload.get("title") or "").strip()
        body = str(payload.get("body") or "").strip()
        state = str(payload.get("state") or "").strip()
        labels = [
            str(label.get("name") or "").strip()
            for label in payload.get("labels", [])
            if isinstance(label, dict) and label.get("name")
        ]
        parts = [
            "GitHub issue",
            f"Repository: {owner}/{repo}",
            f"Issue: #{number}",
        ]
        if title:
            parts.append(f"Title: {title}")
        if state:
            parts.append(f"State: {state}")
        if labels:
            parts.append(f"Labels: {', '.join(labels)}")
        if body:
            parts.extend(["", body])
        return "\n".join(parts).strip()

    def _load_blob(self, owner: str, repo: str, blob_path: str) -> str:
        segments = [segment for segment in blob_path.split("/") if segment]
        if len(segments) < 2:
            return ""

        for split_index in range(1, len(segments)):
            ref = "/".join(segments[:split_index])
            file_path = "/".join(segments[split_index:])
            payload = self._fetch_json(
                f"/repos/{owner}/{repo}/contents/{urllib.parse.quote(file_path, safe='/')}",
                params={"ref": ref},
            )
            content = self._decode_file_content(payload)
            if not content and isinstance(payload, dict):
                content = self._fetch_download_text(str(payload.get("download_url") or ""))
            if not content:
                continue
            parts = [
                "GitHub blob",
                f"Repository: {owner}/{repo}",
                f"Ref: {ref}",
                f"Path: {file_path}",
                "",
                content,
            ]
            return "\n".join(parts).strip()
        return ""

    def _fetch_json(
        self,
        path: str,
        *,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        token = self._github_provider._resolve_token()
        query = urllib.parse.urlencode(params or {})
        url = f"{self._API_BASE}{path}"
        if query:
            url = f"{url}?{query}"
        request = urllib.request.Request(
            url,
            headers=self._github_provider._build_headers(token),
        )
        try:
            with urllib.request.urlopen(request, timeout=_URL_TIMEOUT) as response:
                return json.loads(response.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            logger.info("GitHub API 请求失败 (%s %s): %s", path, exc.code, body[:200])
            return {}
        except urllib.error.URLError as exc:
            logger.info("GitHub API 请求失败 (%s): %s", path, exc)
            return {}

    def _fetch_download_text(self, url: str) -> str:
        if not url:
            return ""
        raw = _fetch_url(url)
        return raw[:_MAX_DOC_CHARS]

    @staticmethod
    def _decode_file_content(payload: dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        if payload.get("type") != "file":
            return ""

        content = payload.get("content")
        encoding = str(payload.get("encoding") or "").lower()
        if isinstance(content, str) and content:
            if encoding == "base64":
                try:
                    decoded = base64.b64decode(content, validate=False)
                    return decoded.decode("utf-8", errors="replace")[:_MAX_DOC_CHARS]
                except (ValueError, TypeError):
                    return ""
            return content[:_MAX_DOC_CHARS]
        return ""


class ExternalSourceScanner:
    """外部文档扫描器：从 URL/文件提取与编排器改进相关的建议。

    支持并行扫描多个来源，通过 max_parallel 控制并发数。
    """

    def __init__(
        self,
        claude_config: ClaudeConfig,
        limits_config: LimitsConfig,
        budget_tracker: BudgetTracker | None,
        orchestrator_dir: str | Path,
        evidence_dir: str | Path | None = None,
        sogou_cookie_header: str = "",
        sogou_cookie_file: str = "",
        sogou_storage_state_path: str = "",
        focus_keywords: Iterable[str] | None = None,
        max_parallel: int = 4,
    ):
        self._claude_config = claude_config
        self._limits = limits_config
        self._budget = budget_tracker
        self._orchestrator_dir = Path(orchestrator_dir)
        self._evidence_dir = Path(evidence_dir) if evidence_dir is not None else (self._orchestrator_dir / "evidence")
        self._evidence_dir.mkdir(parents=True, exist_ok=True)
        self._scan_runtime_root = self._evidence_dir / "_scan_runtime"
        self._scan_runtime_root.mkdir(parents=True, exist_ok=True)
        self._max_parallel = max(1, max_parallel)
        self._focus_keywords = [keyword.strip() for keyword in (focus_keywords or []) if keyword and keyword.strip()]
        self._sanitizer = PromptSanitizer()
        self._sogou_resolver = SogouWeChatResolver(
            cookie_header=sogou_cookie_header,
            cookie_file=sogou_cookie_file,
            storage_state_path=sogou_storage_state_path,
        )
        self._github_resolver = GitHubContentResolver()

    def scan(self, sources: list[str | DiscoveredSource]) -> list[ImprovementProposal]:
        """并行扫描多个外部来源，返回合并的改进提案列表。

        所有来源同时并发，不做人为限制。
        """
        if not sources:
            return []

        workers = min(len(sources), self._max_parallel)
        logger.info("外部扫描启动: %d 个来源, 并发数 %d", len(sources), workers)
        all_proposals: list[ImprovementProposal] = []

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='ext-scanner') as pool:
            future_to_source = {
                pool.submit(self._scan_single_safe, source): source
                for source in sources
            }
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    proposals = future.result()
                    all_proposals.extend(proposals)
                    logger.info("从 %s 提取了 %d 条建议", source, len(proposals))
                except Exception as e:
                    # 保持宽泛捕获，因为 _scan_single_safe 已经处理了大部分异常
                    # 这里只捕获线程池本身的异常（如 CancelledError）
                    logger.warning("扫描 %s 失败: %s", source, e)

        logger.info("外部扫描共提取 %d 条改进提案", len(all_proposals))
        return all_proposals

    def _scan_single_safe(self, source: str | DiscoveredSource) -> list[ImprovementProposal]:
        """线程安全的单来源扫描包装，捕获异常避免影响其他任务。"""
        try:
            return self._scan_single(source)
        except Exception as e:
            logger.warning("扫描 %s 异常: %s", source, e)
            return []

    def _scan_single(self, source: str | DiscoveredSource) -> list[ImprovementProposal]:
        """扫描单个来源。"""
        source_ref, source_metadata = self._resolve_source(source)
        # 加载内容
        content = self._load_scan_content(source_ref, source_metadata)
        if not content.strip():
            logger.warning("来源 %s 内容为空", source_ref)
            return []

        focus_keywords = self._build_focus_keywords(source_metadata)
        if not self._should_scan_content(content, focus_keywords):
            content_lower = content[:5000].lower()
            generic_hits = sum(1 for signal in _GENERIC_TECH_SIGNALS if signal in content_lower)
            focus_phrase_hits = sum(1 for keyword in focus_keywords if keyword.lower() in content_lower)
            logger.info(
                "内容相关性不足 (generic=%d focus=%d): %s",
                generic_hits,
                focus_phrase_hits,
                source_ref[:80],
            )
            return []

        # 构建 prompt
        clean_content = self._sanitize_content(content)
        prompt = self._build_extract_prompt(source_ref, content, source_metadata, focus_keywords)

        task_node = TaskNode(
            id=f"_external_scan_{uuid.uuid4().hex[:6]}",
            prompt_template=prompt,
            timeout=600,
            model=self._claude_config.default_model if self._claude_config else ClaudeConfig().default_model,
            output_format="text",
            system_prompt=_EXTRACT_SYSTEM_PROMPT,
        )

        with tempfile.TemporaryDirectory(dir=self._scan_runtime_root, prefix="scan_") as scan_working_dir:
            result = run_claude_task(
                task=task_node,
                prompt=prompt,
                claude_config=self._claude_config,
                limits=self._limits,
                budget_tracker=self._budget,
                working_dir=scan_working_dir,
            )

        if result.status != TaskStatus.SUCCESS:
            logger.error("外部文档分析失败 (%s): %s", source_ref, result.error)
            return []

        evidence_path = self._write_evidence_artifact(source_metadata or source_ref, clean_content)
        return self._parse_proposals(result.output or "", source_ref, source_metadata, evidence_path)

    def _build_extract_prompt(
        self,
        source: str,
        content: str,
        source_metadata: DiscoveredSource | None = None,
        focus_keywords: list[str] | None = None,
    ) -> str:
        """构建提取 prompt。"""
        clean_content = self._sanitize_content(content)
        focus_keywords = focus_keywords or self._build_focus_keywords(source_metadata)
        focus_section = ""
        if focus_keywords:
            focus_section = (
                "## 当前改进焦点\n\n"
                f"{', '.join(focus_keywords[:10])}\n\n"
            )
        source_context = ""
        if source_metadata:
            source_context = "\n".join(
                [
                    "## 来源元数据",
                    f"- 标题: {source_metadata.title or 'N/A'}",
                    f"- Providers: {', '.join(source_metadata.providers) or 'N/A'}",
                    f"- Queries: {', '.join(source_metadata.queries) or 'N/A'}",
                    f"- Source score: {source_metadata.source_score:.2f}",
                    "",
                ]
            )
        task_line = "从上述文档中提取与本项目改进相关的建议。"
        if focus_keywords:
            task_line = "从上述文档中提取与当前改进焦点直接相关、且可转化为本项目代码改进的建议。"

        return (
            f"# 外部文档分析\n\n"
            f"来源: {source}\n\n"
            f"{focus_section}"
            f"{source_context}"
            f"## 文档内容\n\n"
            f"{clean_content}\n\n"
            f"## 硬性约束\n\n"
            f"- 这是闭卷提取任务，只能依据本 prompt 中的来源元数据和文档内容作答。\n"
            f"- 不要读取本地仓库、技能文件、测试、README 或任何额外文件。\n"
            f"- 不要访问当前来源之外的网络资源，不要补充未在文档中出现的事实。\n"
            f"- 只有能直接落到本项目代码改进的建议才允许保留。\n\n"
            f"## 任务\n\n"
            f"{task_line}\n"
            f"输出 JSON 数组，第一个字符必须是 [。\n"
            f"如果没有相关建议，输出 []。"
        )

    def _build_focus_keywords(self, source_metadata: DiscoveredSource | None) -> list[str]:
        keywords: list[str] = []
        for keyword in self._focus_keywords:
            if keyword not in keywords:
                keywords.append(keyword)
        if source_metadata:
            for keyword in source_metadata.queries:
                cleaned = keyword.strip()
                if cleaned and cleaned not in keywords:
                    keywords.append(cleaned)
        return keywords[:12]

    def _focus_signals(self, focus_keywords: list[str]) -> list[str]:
        signals: list[str] = []
        for keyword in focus_keywords:
            lowered = keyword.lower().strip()
            if lowered and lowered not in signals:
                signals.append(lowered)
            for token in _FOCUS_TOKEN_RE.findall(lowered):
                normalized = token.lower().strip()
                if len(normalized) >= 2 and normalized not in signals:
                    signals.append(normalized)
        return signals

    def _signal_text(self, content: str) -> str:
        if "<" in content and ">" in content:
            return _strip_html(content)
        return content

    def _should_scan_content(self, content: str, focus_keywords: list[str]) -> bool:
        content_lower = self._signal_text(content)[:5000].lower()
        generic_hits = sum(1 for signal in _GENERIC_TECH_SIGNALS if signal in content_lower)
        if generic_hits >= 2:
            return True
        if not focus_keywords:
            return False
        phrase_hits = sum(1 for keyword in focus_keywords if keyword.lower() in content_lower)
        if phrase_hits >= 1:
            return True
        token_hits = sum(1 for signal in self._focus_signals(focus_keywords) if signal in content_lower)
        return token_hits >= 2

    def _sanitize_content(self, content: str) -> str:
        extracted = _strip_html(content) if "<" in content and ">" in content else content
        sanitized = self._sanitizer.sanitize(extracted, max_length=40000)
        if sanitized.warnings:
            logger.debug("外部内容清洗: %s", ", ".join(sanitized.warnings))
        clean_text = re.sub(
            r"(?i)\b(ignore|disregard)\b.{0,40}\b(prior|previous)\b.{0,40}\binstructions?\b",
            "[filtered prompt-injection]",
            sanitized.cleaned_text,
        )
        return clean_text

    def _resolve_source(self, source: str | DiscoveredSource) -> tuple[str, DiscoveredSource | None]:
        if isinstance(source, DiscoveredSource):
            return source.url, source
        return source, None

    def _load_scan_content(self, source_ref: str, source_metadata: DiscoveredSource | None) -> str:
        if source_metadata and self._is_sogou_wechat_source(source_metadata):
            resolved_url, article_content = self._sogou_resolver.resolve(source_ref)
            if resolved_url:
                source_metadata.resolved_url = resolved_url
            if article_content:
                return article_content
            return self._build_metadata_fallback_content(source_metadata)

        if self._is_github_source(source_ref, source_metadata):
            resolved_url, github_content = self._github_resolver.resolve(source_ref)
            if source_metadata and resolved_url:
                source_metadata.resolved_url = resolved_url
            if github_content:
                return github_content
            if source_metadata:
                logger.info("GitHub 结构化加载失败，降级为 metadata-only 扫描: %s", source_ref)
                return self._build_metadata_fallback_content(source_metadata)

        content = _load_source(source_ref)
        if source_metadata and _looks_like_antispider_page(content):
            logger.info("来源 %s 命中反爬页，降级为 metadata-only 扫描", source_ref)
            return self._build_metadata_fallback_content(source_metadata)
        return content

    def _is_sogou_wechat_source(self, source_metadata: DiscoveredSource) -> bool:
        return any(provider == "sogou_wechat" for provider in source_metadata.providers)

    def _is_github_source(self, source_ref: str, source_metadata: DiscoveredSource | None) -> bool:
        if source_metadata and any(provider == "github_search" for provider in source_metadata.providers):
            return True
        try:
            domain = urllib.parse.urlparse(source_ref).netloc.lower()
        except Exception:
            return False
        return domain in {"github.com", "www.github.com"}

    def _build_metadata_fallback_content(self, source_metadata: DiscoveredSource) -> str:
        lines = [
            "Metadata-only source summary",
            f"Title: {source_metadata.title}",
            f"Snippet: {source_metadata.snippet}",
            f"Queries: {', '.join(source_metadata.queries)}",
            f"Providers: {', '.join(source_metadata.providers)}",
            f"Source score: {source_metadata.source_score:.2f}",
        ]
        if source_metadata.evidence_tier or source_metadata.source_profile:
            lines.append(
                "Research profile: {} {}".format(
                    source_metadata.evidence_tier or "",
                    source_metadata.source_profile or "",
                ).strip()
            )
        if source_metadata.research_summary:
            lines.append(f"Research summary: {source_metadata.research_summary}")
        if source_metadata.research_artifact_path:
            lines.append(f"Research artifact: {source_metadata.research_artifact_path}")
        return "\n".join(lines).strip()

    def _write_evidence_artifact(self, source: str | DiscoveredSource, clean_content: str) -> str:
        if isinstance(source, DiscoveredSource):
            payload = {
                "source": {
                    "url": source.url,
                    "canonical_url": source.canonical_url,
                    "resolved_url": source.resolved_url,
                    "title": source.title,
                    "snippet": source.snippet,
                    "providers": source.providers,
                    "queries": source.queries,
                    "ranks": source.ranks,
                    "provider_scores": source.provider_scores,
                    "source_score": source.source_score,
                    "evidence_tier": source.evidence_tier,
                    "source_profile": source.source_profile,
                    "research_summary": source.research_summary,
                    "research_artifact_path": source.research_artifact_path,
                    "research_rounds": source.research_rounds,
                },
                "excerpt": clean_content[:2000],
            }
        else:
            payload = {
                "source": {
                    "url": source,
                    "canonical_url": source,
                    "providers": ["manual"],
                    "queries": [],
                    "source_score": 0.0,
                },
                "excerpt": clean_content[:2000],
            }

        artifact_path = self._evidence_dir / f"external_source_{uuid.uuid4().hex[:10]}.json"
        artifact_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return str(artifact_path)

    def _parse_proposals(
        self,
        raw_output: str,
        source: str,
        source_metadata: DiscoveredSource | None = None,
        evidence_path: str = "",
    ) -> list[ImprovementProposal]:
        """解析 Claude 返回的改进提案。"""
        try:
            data = robust_parse_json(raw_output)
        except ValueError as e:
            logger.error("解析外部提案失败 (%s): %s", source, e)
            return []

        if isinstance(data, dict):
            data = data.get("proposals", data.get("improvements", [data]))
        if not isinstance(data, list):
            logger.error("外部提案格式错误: 期望数组，得到 %s", type(data).__name__)
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
                    source=ImprovementSource.EXTERNAL_DOC,
                    priority=ImprovementPriority(priority_str),
                    affected_files=item.get("affected_files", []),
                    estimated_complexity=item.get("complexity", "medium"),
                    evidence=(
                        f"{source_metadata.research_summary} | {item.get('evidence', f'来源: {source}')}"
                        if source_metadata and source_metadata.research_summary
                        else item.get("evidence", f"来源: {source}")
                    ),
                    source_url=(source_metadata.resolved_url or source_metadata.url) if source_metadata else source,
                    source_provider=",".join(source_metadata.providers) if source_metadata else "manual",
                    evidence_path=evidence_path,
                    source_score=source_metadata.source_score if source_metadata else 0.0,
                )
                proposals.append(proposal)
            except Exception as e:
                logger.warning("解析单条外部提案失败: %s", e)

        return proposals
