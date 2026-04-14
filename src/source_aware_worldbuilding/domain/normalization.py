from __future__ import annotations

import re
import unicodedata

from source_aware_worldbuilding.domain.models import SourceRecord

SPACE_RE = re.compile(r"\s+")
YEAR_RE = re.compile(r"\b(\d{4})\b")
PLACE_RE = re.compile(
    r"\b(?:in|at|from|near|within|across|around|outside|inside|through|throughout|into|of)\s+"
    r"([A-Z][A-Za-z'’.-]*(?:\s+[A-Z][A-Za-z'’.-]*){0,2})\b"
)
LOCATORISH_RE = re.compile(
    r"\b(?:folio|folios|chapter|chapters|page|pages|leaf|leaves|section|sections|appendix|"
    r"book|books|line|lines|part|parts)\b",
    re.IGNORECASE,
)
SEASON_RANGES = {
    "spring": ("03-01", "05-31"),
    "summer": ("06-01", "08-31"),
    "autumn": ("09-01", "11-30"),
    "fall": ("09-01", "11-30"),
}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value)
    return SPACE_RE.sub(" ", normalized).strip(" \t\r\n.,;:!?").lower()


def normalized_candidate_key(subject: str, predicate: str, value: str) -> tuple[str, str, str]:
    return (
        normalize_text(subject),
        normalize_text(predicate),
        normalize_text(value),
    )


def infer_place(sentence: str, source: SourceRecord) -> str | None:
    for text in (sentence, source.title or ""):
        for match in PLACE_RE.finditer(text):
            candidate = _clean_place(match.group(1))
            if candidate:
                return candidate
    return None


def infer_time_range(sentence: str, source_year: str | None) -> tuple[str | None, str | None]:
    year = _sentence_year(sentence) or _clean_year(source_year)
    if year is None:
        return None, None

    sentence_lower = sentence.lower()
    if " winter " in f" {sentence_lower} ":
        return f"{year}-12-01", f"{int(year) + 1:04d}-02-28"
    for season, (start, end) in SEASON_RANGES.items():
        if f" {season} " in f" {sentence_lower} ":
            return f"{year}-{start}", f"{year}-{end}"
    return f"{year}-01-01", f"{year}-12-31"


def _clean_place(value: str) -> str | None:
    cleaned = SPACE_RE.sub(" ", value).strip(" ,.;:!?")
    if not cleaned or LOCATORISH_RE.search(cleaned):
        return None
    return cleaned


def _sentence_year(sentence: str) -> str | None:
    match = YEAR_RE.search(sentence)
    if not match:
        return None
    return match.group(1)


def _clean_year(value: str | None) -> str | None:
    if value and len(value) == 4 and value.isdigit():
        return value
    return None
