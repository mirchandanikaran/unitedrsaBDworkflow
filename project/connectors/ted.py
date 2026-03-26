"""Connector for TED (Tenders Electronic Daily) notices."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
import os
from typing import Any

import requests

from utils.helpers import generate_tender_id, normalize_status, parse_datetime_to_iso, utc_now_iso


logger = logging.getLogger(__name__)

TED_SOURCE = "ted"
TED_API_URL = os.getenv("TED_API_URL", "https://api.ted.europa.eu/v3/notices/search")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
TED_PAGE_SIZE = int(os.getenv("TED_PAGE_SIZE", "250"))
TED_MAX_PAGES = int(os.getenv("TED_MAX_PAGES", "60"))


def _build_query() -> str:
    """Build default TED expert query, configurable via env."""
    configured_query = os.getenv("TED_QUERY")
    if configured_query:
        return configured_query
    # Wider default window to maximize tender coverage for ingestion.
    year_start = datetime.now(UTC).replace(year=max(datetime.now(UTC).year - 2, 2020)).strftime("%Y0101")
    return f"publication-date>={year_start}"


def _build_request_payload(page: int, page_size: int) -> dict[str, Any]:
    """Build TED search payload using v3 POST API contract."""
    return {
        "query": _build_query(),
        "fields": [
            "publication-number",
            "notice-title",
            "buyer-name",
            "publication-date",
            "deadline-receipt-request",
            "place-of-performance",
            "links",
        ],
        "page": page,
        "limit": page_size,
        "paginationMode": "PAGE_NUMBER",
    }


def _request_page(page: int, page_size: int) -> dict[str, Any]:
    """Request one TED result page with simple retry logic."""
    payload = _build_request_payload(page=page, page_size=page_size)
    last_error: Exception | None = None
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0"),
    }

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.post(
                TED_API_URL,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "TED request failed",
                extra={"source": TED_SOURCE, "page": page, "attempt": attempt, "error": str(exc)},
            )

    raise RuntimeError(f"TED page request failed after retries: page={page}") from last_error


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract result items from common TED payload shapes."""
    if isinstance(payload.get("notices"), list):
        return payload["notices"]
    if isinstance(payload.get("results"), list):
        return payload["results"]
    if isinstance(payload.get("items"), list):
        return payload["items"]
    if isinstance(payload.get("data"), list):
        return payload["data"]
    return []


def _extract_multilingual_text(value: Any) -> str:
    """Extract one text value from TED multilingual field shape."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for lang_values in value.values():
            if isinstance(lang_values, list) and lang_values:
                if isinstance(lang_values[0], str):
                    return lang_values[0]
            if isinstance(lang_values, str):
                return lang_values
    if isinstance(value, list) and value:
        if isinstance(value[0], str):
            return value[0]
    return ""


def _extract_notice_url(item: dict[str, Any]) -> str:
    """Extract best-available TED notice URL."""
    links = item.get("links")
    if not isinstance(links, dict):
        return ""

    for section_key, lang_code in (("html", "ENG"), ("pdf", "ENG"), ("xml", "MUL")):
        section = links.get(section_key)
        if isinstance(section, dict):
            if section.get(lang_code):
                return str(section[lang_code])
            for fallback_value in section.values():
                if isinstance(fallback_value, str):
                    return fallback_value
    return ""


def _normalize_notice(item: dict[str, Any]) -> dict[str, Any] | None:
    """Map a TED notice into the unified tender schema."""
    url = _extract_notice_url(item) or item.get("url") or item.get("noticeUrl") or item.get("link")
    if not url:
        return None

    title = _extract_multilingual_text(item.get("notice-title")) or item.get("title") or item.get("noticeTitle") or ""
    description = item.get("description") or item.get("summary") or title
    organization = _extract_multilingual_text(item.get("buyer-name")) or item.get("organization") or item.get("buyerName") or ""
    place_of_performance = item.get("place-of-performance")
    if isinstance(place_of_performance, list) and place_of_performance:
        country = str(place_of_performance[0])
    else:
        country = item.get("country") or item.get("countryCode") or ""
    published_date = parse_datetime_to_iso(
        _extract_multilingual_text(item.get("publishedDate") or item.get("publicationDate"))
    )
    if not published_date:
        published_date = parse_datetime_to_iso(_extract_multilingual_text(item.get("publication-date")))
    closing_date = parse_datetime_to_iso(
        _extract_multilingual_text(item.get("closingDate") or item.get("deadlineDate"))
    )
    if not closing_date:
        closing_date = parse_datetime_to_iso(_extract_multilingual_text(item.get("deadline-receipt-request")))
    status = normalize_status(_extract_multilingual_text(item.get("status") or item.get("noticeStatus")))

    return {
        "id": generate_tender_id(TED_SOURCE, str(url)),
        "title": str(title),
        "description": str(description),
        "organization": str(organization),
        "country": str(country),
        "source": TED_SOURCE,
        "published_date": published_date,
        "closing_date": closing_date,
        "status": status,
        "budget": "",
        "url": str(url),
        "created_at": utc_now_iso(),
    }


def fetch_ted_tenders(max_pages: int = TED_MAX_PAGES, page_size: int = TED_PAGE_SIZE) -> list[dict[str, Any]]:
    """
    Fetch tenders from TED and return normalized JSON records only.

    This connector does not write to the database.
    """
    normalized_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        try:
            payload = _request_page(page=page, page_size=page_size)
        except RuntimeError as exc:
            logger.error("TED connector page failed", extra={"source": TED_SOURCE, "page": page, "error": str(exc)})
            if page == 1:
                raise
            break

        page_items = _extract_items(payload)
        if not page_items:
            break

        for item in page_items:
            normalized = _normalize_notice(item)
            if normalized is None:
                logger.info("TED notice skipped due to missing URL", extra={"source": TED_SOURCE})
                continue
            row_id = str(normalized.get("id", ""))
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            normalized_rows.append(normalized)

        if len(page_items) < page_size:
            break

    return normalized_rows
