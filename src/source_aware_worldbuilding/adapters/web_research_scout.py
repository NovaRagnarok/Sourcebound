from __future__ import annotations

from io import BytesIO
import re
from html import unescape
from html.parser import HTMLParser
from hashlib import sha1
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import httpx

from source_aware_worldbuilding.domain.models import (
    ResearchFetchedPage,
    ResearchSearchProviderResult,
    ResearchScoutCapabilities,
    ResearchSearchHit,
)

_PUBLISH_DATE_RE = re.compile(r"\b((?:19|20)\d{2})(?:[-/]\d{2}[-/]\d{2})?\b")
_GUIDE_TERMS = ("guide", "history", "top ", "best ", "timeline", "origins")
_TRACKING_PARAMS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "ref",
    "ref_src",
    "source",
}


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


class ResearchSearchProviderRegistry:
    def __init__(self, providers: list[object], *, default_order: list[str] | None = None) -> None:
        self._providers = {
            getattr(provider, "provider_id"): provider
            for provider in providers
        }
        self.default_order = default_order or list(self._providers)

    def get(self, provider_id: str):
        return self._providers.get(provider_id)

    def ordered(self, provider_ids: list[str] | None = None) -> list[object]:
        order = provider_ids or self.default_order
        return [self._providers[provider_id] for provider_id in order if provider_id in self._providers]

    def list_provider_ids(self) -> list[str]:
        return sorted(self._providers)


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
            timeout=8.0,
            follow_redirects=True,
            headers={"User-Agent": user_agent},
        )
        self._user_agent = user_agent
        self._robots_cache: dict[str, RobotFileParser | None] = {}
        self._last_search_metadata: dict[str, object] | None = None

    def fetch_page(self, url: str) -> ResearchFetchedPage:
        response = self._client.get(url)
        response.raise_for_status()
        final_url = str(response.url)
        content_type = response.headers.get("content-type", "").lower()
        title: str | None = None
        publisher: str | None = None
        if "pdf" in content_type or final_url.lower().endswith(".pdf"):
            text = self._extract_pdf_text(response.content)
            title = self._pdf_title(final_url, text)
            published_at = self._infer_publish_date(final_url, title or "")
        else:
            parser = _PageContentParser()
            parser.feed(response.text)
            text = "\n".join(part for part in parser.text_parts if part).strip()
            title = " ".join(parser.title).strip() or None
            publisher = parser.publisher
            published_at = parser.published_at or self._infer_publish_date(final_url, title or "")
        return ResearchFetchedPage(
            url=url,
            final_url=final_url,
            title=title,
            publisher=publisher or self._publisher_from_url(final_url),
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

    def _infer_publish_date(self, url: str, title: str) -> str | None:
        haystacks = [url, title]
        for haystack in haystacks:
            match = _PUBLISH_DATE_RE.search(haystack[:400])
            if match:
                return match.group(1)
        return None

    def _publisher_from_url(self, url: str) -> str:
        host = urlparse(url).netloc.lower()
        return host.removeprefix("www.")

    def _extract_pdf_text(self, content: bytes) -> str:
        try:
            from pdfminer.high_level import extract_text

            return extract_text(BytesIO(content)).strip()
        except Exception:
            return ""

    def _pdf_title(self, url: str, text: str) -> str | None:
        first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
        if first_line and len(first_line) <= 180:
            return first_line
        path = urlparse(url).path.rsplit("/", 1)[-1]
        return path or None

    def _classify_source(self, url: str) -> str:
        host = urlparse(url).netloc.lower()
        if host.endswith(".gov"):
            return "government"
        if host.endswith(".edu"):
            return "educational"
        if any(part in host for part in ("archive", "library", "museum")):
            return "archive"
        if any(part in host for part in ("wikipedia", "britannica", "encyclopedia")):
            return "reference"
        if any(part in host for part in ("youtube", "youtu.be", "vimeo", "soundcloud", "mixcloud")):
            return "video"
        if any(
            part in host
            for part in (
                "news",
                "newspaper",
                "times",
                "post",
                "guardian",
                "bbc",
                "tribune",
                "reader",
                "journal",
                "npr",
                "fox",
                "abc",
                "cbs",
                "nbc",
                "wttw",
                "kutx",
            )
        ):
            return "news"
        if any(part in host for part in ("magazine", "rollingstone", "billboard", "vice", "djmag", "5mag", "chicagomag")):
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

    def get_last_search_metadata(self) -> dict[str, object] | None:
        return self._last_search_metadata


class DuckDuckGoHtmlSearchProvider:
    provider_id = "duckduckgo_html"

    def __init__(self, *, user_agent: str) -> None:
        self._client = httpx.Client(
            timeout=8.0,
            follow_redirects=True,
            headers={"User-Agent": user_agent},
        )

    def search(self, query: str, *, limit: int = 5) -> ResearchSearchProviderResult:
        response = self._client.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
        )
        response.raise_for_status()
        parser = _DuckDuckGoSearchParser()
        parser.feed(response.text)
        hits = [
            item
            for item in parser.results
            if _is_organic_search_hit(item.url)
        ][:limit]
        for item in hits:
            item.query = query
            item.search_provider_id = self.provider_id
            item.provider_rank = item.rank
            item.matched_providers = [self.provider_id]
        return ResearchSearchProviderResult(provider_id=self.provider_id, hits=hits)


class BraveSearchApiProvider:
    provider_id = "brave_search_api"

    def __init__(self, *, api_key: str, base_url: str, user_agent: str) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            timeout=8.0,
            follow_redirects=True,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
                "X-Subscription-Token": api_key,
            },
        )

    def search(self, query: str, *, limit: int = 5) -> ResearchSearchProviderResult:
        response = self._client.get(
            f"{self._base_url}/res/v1/web/search",
            params={"q": query, "count": limit},
        )
        response.raise_for_status()
        payload = response.json()
        raw_hits = (((payload or {}).get("web") or {}).get("results") or [])[:limit]
        hits: list[ResearchSearchHit] = []
        for index, item in enumerate(raw_hits, start=1):
            url = item.get("url") or item.get("profile", {}).get("url") or ""
            title = item.get("title") or item.get("meta_title") or url
            snippet = (
                item.get("description")
                or item.get("snippet")
                or next(iter(item.get("extra_snippets") or []), None)
            )
            if not url or not title:
                continue
            hits.append(
                ResearchSearchHit(
                    query=query,
                    url=url,
                    title=title,
                    snippet=snippet,
                    rank=index,
                    search_provider_id=self.provider_id,
                    provider_rank=index,
                    matched_providers=[self.provider_id],
                )
            )
        return ResearchSearchProviderResult(provider_id=self.provider_id, hits=hits)


class WebOpenResearchScout(_BaseHtmlResearchScout):
    adapter_id = "web_open"
    capabilities = ResearchScoutCapabilities(
        supports_search=True,
        supports_fetch=True,
        supports_text_inputs=False,
        supports_robots=True,
        supports_domain_policy=True,
    )

    def __init__(
        self,
        *,
        user_agent: str,
        search_provider_registry: ResearchSearchProviderRegistry | None = None,
        search_provider_ids: list[str] | None = None,
    ) -> None:
        super().__init__(user_agent=user_agent)
        self._search_provider_registry = search_provider_registry or ResearchSearchProviderRegistry(
            [DuckDuckGoHtmlSearchProvider(user_agent=user_agent)],
            default_order=["duckduckgo_html"],
        )
        self._search_provider_ids = search_provider_ids or self._search_provider_registry.default_order

    def search(self, query: str, *, limit: int = 5) -> list[ResearchSearchHit]:
        provider_results: list[ResearchSearchProviderResult] = []
        providers_used: list[str] = []
        fallback_reasons: list[str] = []

        for provider in self._search_provider_registry.ordered(self._search_provider_ids):
            providers_used.append(provider.provider_id)
            try:
                provider_results.append(provider.search(query, limit=limit))
            except Exception as exc:
                fallback_reasons.append(f"{provider.provider_id}: {exc}")

        fused = self._fuse_provider_results(query, provider_results, limit=limit)
        self._last_search_metadata = {
            "providers_used": providers_used,
            "queries_by_provider": {provider_id: 1 for provider_id in providers_used},
            "hits_by_provider": {
                result.provider_id: len(result.hits)
                for result in provider_results
            },
            "fallback_used": bool(fallback_reasons),
            "fallback_reason": "; ".join(fallback_reasons) if fallback_reasons else None,
            "matched_provider_count": len(provider_results),
        }
        return fused

    def _fuse_provider_results(
        self,
        query: str,
        provider_results: list[ResearchSearchProviderResult],
        *,
        limit: int,
    ) -> list[ResearchSearchHit]:
        merged: dict[str, dict[str, object]] = {}
        provider_order = [result.provider_id for result in provider_results]
        for result in provider_results:
            for hit in result.hits:
                canonical_url = _canonicalize_search_url(hit.url)
                if not canonical_url:
                    continue
                bucket = merged.setdefault(
                    canonical_url,
                    {
                        "hit": hit.model_copy(update={"url": canonical_url, "query": query}),
                        "providers": [],
                        "best_rank": hit.provider_rank or hit.rank,
                    },
                )
                bucket["providers"].append(result.provider_id)
                bucket["best_rank"] = min(bucket["best_rank"], hit.provider_rank or hit.rank)
                current = bucket["hit"]
                if len((hit.snippet or "")) > len((current.snippet or "")):
                    bucket["hit"] = current.model_copy(
                        update={
                            "title": hit.title or current.title,
                            "snippet": hit.snippet or current.snippet,
                            "url": canonical_url,
                            "query": query,
                        }
                    )
        fused_hits: list[ResearchSearchHit] = []
        for canonical_url, payload in merged.items():
            hit: ResearchSearchHit = payload["hit"]
            matched_providers = sorted(set(payload["providers"]), key=lambda item: provider_order.index(item))
            primary_provider = matched_providers[0] if matched_providers else hit.search_provider_id
            fusion_score = self._fusion_score(hit, query, len(matched_providers), payload["best_rank"])
            fused_hits.append(
                hit.model_copy(
                    update={
                        "url": canonical_url,
                        "search_provider_id": primary_provider,
                        "provider_rank": payload["best_rank"],
                        "provider_hit_count": len(matched_providers),
                        "matched_providers": matched_providers,
                        "fusion_score": round(fusion_score, 4),
                    }
                )
            )
        fused_hits.sort(
            key=lambda item: (
                item.fusion_score or 0.0,
                item.provider_hit_count,
                -(item.provider_rank or 9999),
            ),
            reverse=True,
        )
        return fused_hits[:limit]

    def _fusion_score(
        self,
        hit: ResearchSearchHit,
        query: str,
        provider_hit_count: int,
        provider_rank: int,
    ) -> float:
        text = " ".join(filter(None, [hit.title, hit.snippet, hit.url])).lower()
        query_tokens = {token for token in re.findall(r"[a-z0-9]{3,}", query.lower())}
        overlap = sum(1 for token in query_tokens if token in text)
        guide_penalty = any(term in text for term in _GUIDE_TERMS)
        year_anchor = bool(_PUBLISH_DATE_RE.search(text))
        root_path = (urlparse(hit.url).path or "/") == "/"
        score = 0.0
        score += provider_hit_count * 1.0
        score += min(overlap / max(len(query_tokens), 1), 1.0) * 0.8
        score += 0.45 if year_anchor else 0.0
        score += 0.1 if not root_path else -0.15
        score -= 0.35 if guide_penalty else 0.0
        score -= max(provider_rank - 1, 0) * 0.05
        return score


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


def _is_organic_search_hit(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    if host.endswith("duckduckgo.com"):
        return False
    return bool(host)


def _canonicalize_search_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.netloc:
        return ""
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower().removeprefix("www.")
    query_items = [
        (key, value)
        for key, value in parse_qs(parsed.query, keep_blank_values=False).items()
        for value in value
        if key.lower() not in _TRACKING_PARAMS and not key.lower().startswith("utm_")
    ]
    query_items.sort()
    query = urlencode(query_items, doseq=True)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, "", query, ""))
