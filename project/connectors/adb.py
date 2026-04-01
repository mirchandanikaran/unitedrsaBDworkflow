"""Connector for Asian Development Bank procurement notices."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlencode

import requests

from utils.helpers import (
    compact_text,
    extract_candidate_notice_links,
    extract_dates_from_text,
    fetch_rendered_html,
    fetch_detail_enrichment,
    generate_tender_id,
    utc_now_iso,
)


logger = logging.getLogger(__name__)

SOURCE = "adb"
ADB_URLS = tuple(
    part.strip()
    for part in os.getenv(
        "ADB_URLS",
        "https://www.adb.org/business/institutional-procurement/notices,https://www.adb.org/projects/tenders",
    ).split(",")
    if part.strip()
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
MAX_PAGES = int(os.getenv("ADB_MAX_PAGES", "0"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"
DETAIL_FETCH_LIMIT = int(os.getenv("ADB_DETAIL_FETCH_LIMIT", "0"))


def _request_page(base_url: str, page: int) -> str:
    """Fetch one ADB notices page."""
    params = {"page": page}
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "ADB request failed",
                extra={"source": SOURCE, "base_url": base_url, "page": page, "attempt": attempt, "error": str(exc)},
            )
    if PLAYWRIGHT_FALLBACK:
        query = urlencode(params)
        return fetch_rendered_html(f"{base_url}?{query}")
    raise RuntimeError(f"ADB request failed for page={page}") from last_error


def fetch_adb_tenders(max_pages: int = MAX_PAGES) -> list[dict[str, Any]]:
    """Return normalized tenders from ADB notices pages."""
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    enriched_count = 0

    for base_url in ADB_URLS:
        page = 0
        while True:
            if max_pages > 0 and page >= max_pages:
                break
            html = _request_page(base_url=base_url, page=page)
            links = extract_candidate_notice_links(
                html,
                base_url=base_url,
                include_patterns=("procurement", "tender", "bid", "consulting", "goods", "works", "project"),
                max_links=int(os.getenv("ADB_MAX_LINKS_PER_PAGE", "0")),
            )
            if not links:
                break

            added_this_page = 0
            for link in links:
                tender_id = generate_tender_id(SOURCE, link["url"])
                if tender_id in seen_ids:
                    continue
                seen_ids.add(tender_id)
                added_this_page += 1
                description = compact_text(link.get("context") or link["title"])
                published_date, closing_date = extract_dates_from_text(description)
                country = "Global"
                if DETAIL_FETCH_LIMIT <= 0 or enriched_count < DETAIL_FETCH_LIMIT:
                    enrich = fetch_detail_enrichment(
                        url=link["url"],
                        use_playwright_fallback=PLAYWRIGHT_FALLBACK,
                        timeout_seconds=25,
                    )
                    enriched_count += 1
                    if enrich.get("description"):
                        description = compact_text(enrich["description"])
                    if enrich.get("published_date"):
                        published_date = enrich["published_date"]
                    if enrich.get("closing_date"):
                        closing_date = enrich["closing_date"]
                    if enrich.get("country"):
                        country = str(enrich["country"])
                rows.append(
                    {
                        "id": tender_id,
                        "title": compact_text(link["title"]),
                        "description": description,
                        "organization": "Asian Development Bank",
                        "country": country,
                        "source": SOURCE,
                        "published_date": published_date,
                        "closing_date": closing_date,
                        "status": "open",
                        "budget": "",
                        "url": link["url"],
                        "created_at": utc_now_iso(),
                    }
                )
            if added_this_page == 0:
                break
            page += 1

    return rows
