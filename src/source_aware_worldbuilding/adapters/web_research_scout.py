from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.robotparser import RobotFileParser

import httpx

from source_aware_worldbuilding.domain.models import (
    ResearchFetchedPage,
    ResearchScoutCapabilities,
    ResearchSearchHit,
)

_PUBLISH_DATE_RE = re.compile(r"\b(19|20)\d{2}(?:-\d{2}-\d{2})?\b")


class _DuckDuckGoSearchParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[ResearchSearchHit] = []
        self._current_href: str | None = None
        self._current_title: list[str] = []
        self._current_snippet: list[str] = []
        self._inside_result_link = False
        self._inside_result_snippet = False
        self._rank = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs_map = dict(attrs)
        klass = attrs_map.get("class", "")
        if tag == "a" and "result__a" in klass:
            self._inside_result_link = True
            self._current_href = attrs_map.get("href")
            self._current_title = []
            self._current_snippet = []
        elif tag in {"a", "div"} and "result__snippet" in klass:
            self._inside_result_snippet = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._inside_result_link:
            self._inside_result_link = False
            if self._current_href:
                self._rank += 1
                self.results.append(
                    ResearchSearchHit(
                        query="",
                        url=self._unwrap_duckduckgo_url(self._current_href),
                        title=" ".join(part for part in self._current_title if part).strip(),
                        snippet=" ".join(part for part in self._current_snippet if part).strip() or None,
                        rank=self._rank,
                    )
                )
        elif tag in {"a", "div"} and self._inside_result_snippet:
            self._inside_result_snippet = False

    def handle_data(self, data: str) -> None:
        text = " ".join(unescape(data).split())
        if not text:
            return
        if self._inside_result_link:
            self._current_title.append(text)
        elif self._inside_result_snippet:
            self._current_snippet.append(text)

    def _unwrap_duckduckgo_url(self, value: str) -> str:
        parsed = urlparse(value)
        query = parse_qs(parsed.query)
        if "uddg" in query:
            return query["uddg"][0]
        return value


class _PageContentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title: list[str] = []
        self.text_parts: list[str] = []
        self.publisher: str | None = None
        self.published_at: str | None = None
        self._inside_title = False
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs_map = dict(attrs)
        if tag == "title":
            self._inside_title = True
            return
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if tag == "meta":
            meta_name = (attrs_map.get("name") or attrs_map.get("property") or "").lower()
            content = attrs_map.get("content")
            if not content:
                return
            if meta_name in {"og:site_name", "application-name"} and not self.publisher:
                self.publisher = content
            if meta_name in {"article:published_time", "pubdate", "date", "article:modified_time"}:
                self.published_at = content
        elif tag in {"p", "div", "section", "article", "br", "li", "h1", "h2", "h3"}:
            self.text_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._inside_title = False
        elif tag in {"script", "style", "noscript"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        text = " ".join(unescape(data).split())
        if not text:
            return
        if self._inside_title:
            self.title.append(text)
        else:
            self.text_parts.append(text)


class ResearchScoutRegistry:
    def __init__(self, adapters: list[object], *, default_adapter_id: str = "web_open") -> None:
        self._adapters = {
            getattr(adapter, "adapter_id"): adapter
            for adapter in adapters
        }
        self.default_adapter_id = default_adapter_id

    def get(self, adapter_id: str | None = None):
        resolved = adapter_id or self.default_adapter_id
        return self._adapters.get(resolved)

    def list_adapter_ids(self) -> list[str]:
        return sorted(self._adapters)


class _BaseHtmlResearchScout:
    capabilities = ResearchScoutCapabilities(
        supports_search=False,
        supports_fetch=True,
        supports_text_inputs=False,
        supports_robots=True,
        supports_domain_policy=True,
    )

    def __init__(self, *, user_agent: str) -> None:
        self._client = httpx.Client(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": user_agent},
        )
        self._user_agent = user_agent
        self._robots_cache: dict[str, RobotFileParser | None] = {}

    def fetch_page(self, url: str) -> ResearchFetchedPage:
        response = self._client.get(url)
        response.raise_for_status()
        parser = _PageContentParser()
        parser.feed(response.text)
        text = "\n".join(part for part in parser.text_parts if part).strip()
        published_at = parser.published_at or self._infer_publish_date(text)
        final_url = str(response.url)
        return ResearchFetchedPage(
            url=url,
            final_url=final_url,
            title=" ".join(parser.title).strip() or None,
            publisher=parser.publisher or self._publisher_from_url(final_url),
            published_at=published_at,
            locator=None,
            source_type=self._classify_source(final_url),
            text=text[:12000],
        )

    def allows_fetch(self, url: str, *, user_agent: str) -> bool | None:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False
        robots_url = urljoin(f"{parsed.scheme}://{parsed.netloc}", "/robots.txt")
        parser = self._robots_cache.get(robots_url)
        if parser is None:
            try:
                response = self._client.get(robots_url)
                if response.status_code >= 400:
                    self._robots_cache[robots_url] = None
                    return None
                parser = RobotFileParser()
                parser.parse(response.text.splitlines())
                self._robots_cache[robots_url] = parser
            except httpx.HTTPError:
                self._robots_cache[robots_url] = None
                return None
        if parser is None:
            return None
        return parser.can_fetch(user_agent or self._user_agent, url)

    def _infer_publish_date(self, text: str) -> str | None:
        match = _PUBLISH_DATE_RE.search(text[:3000])
        return match.group(0) if match else None

    def _publisher_from_url(self, url: str) -> str:
        host = urlparse(url).netloc.lower()
        return host.removeprefix("www.")

    def _classify_source(self, url: str) -> str:
        host = urlparse(url).netloc.lower()
        if host.endswith(".gov"):
            return "government"
        if host.endswith(".edu"):
            return "educational"
        if any(part in host for part in ("archive", "library", "museum")):
            return "archive"
        if any(part in host for part in ("news", "newspaper", "times", "post", "guardian", "bbc")):
            return "news"
        if any(part in host for part in ("magazine", "rollingstone", "billboard", "vice")):
            return "magazine"
        if any(part in host for part in ("forum", "board")):
            return "forum"
        if any(part in host for part in ("twitter", "x.com", "facebook", "instagram", "tiktok")):
            return "social"
        if any(part in host for part in ("shop", "store", "ebay", "amazon")):
            return "shopping"
        if any(part in host for part in ("blog", "substack")):
            return "blog"
        return "web"


class WebOpenResearchScout(_BaseHtmlResearchScout):
    adapter_id = "web_open"
    capabilities = ResearchScoutCapabilities(
        supports_search=True,
        supports_fetch=True,
        supports_text_inputs=False,
        supports_robots=True,
        supports_domain_policy=True,
    )

    def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
        response = self._client.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
        )
        response.raise_for_status()
        parser = _DuckDuckGoSearchParser()
        parser.feed(response.text)
        results = parser.results[:limit]
        for item in results:
            item.query = query
        return results


class CuratedInputsResearchScout(_BaseHtmlResearchScout):
    adapter_id = "curated_inputs"
    capabilities = ResearchScoutCapabilities(
        supports_search=False,
        supports_fetch=True,
        supports_text_inputs=True,
        supports_robots=True,
        supports_domain_policy=True,
    )

    def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
        _ = query, limit
        raise NotImplementedError("curated_inputs does not support search")
