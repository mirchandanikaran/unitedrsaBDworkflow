"""Connector for WFP procurement opportunities."""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from utils.helpers import (
    compact_text,
    extract_candidate_notice_links,
    extract_dates_from_text,
    fetch_detail_enrichment,
    fetch_rendered_html,
    generate_tender_id,
    utc_now_iso,
)


logger = logging.getLogger(__name__)

SOURCE = "wfp"
BASE_URL = os.getenv("WFP_URL", "https://www.wfp.org/procurement")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"
MAX_LINKS = int(os.getenv("WFP_MAX_LINKS", "1200"))
DETAIL_FETCH_LIMIT = int(os.getenv("WFP_DETAIL_FETCH_LIMIT", "300"))


def _request_page() -> str:
    """Fetch WFP procurement page HTML."""
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(BASE_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("WFP request failed", extra={"source": SOURCE, "attempt": attempt, "error": str(exc)})
    if PLAYWRIGHT_FALLBACK:
        return fetch_rendered_html(BASE_URL)
    raise RuntimeError("WFP request failed after retries") from last_error


def fetch_wfp_tenders() -> list[dict[str, Any]]:
    """Return normalized tenders from WFP procurement pages."""
    html = _request_page()
    links = extract_candidate_notice_links(
        html,
        base_url=BASE_URL,
        include_patterns=("procurement", "tender", "vendor", "notice", "rfp", "eoi"),
        max_links=MAX_LINKS,
    )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    enriched_count = 0
    for link in links:
        url = link["url"]
        if url in seen:
            continue
        seen.add(url)
        description = compact_text(link.get("context") or link["title"])
        published_date, closing_date = extract_dates_from_text(description)
        country = "Global"
        if enriched_count < DETAIL_FETCH_LIMIT:
            enrich = fetch_detail_enrichment(
                url=url,
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
                "id": generate_tender_id(SOURCE, url),
                "title": compact_text(link["title"]),
                "description": description,
                "organization": "World Food Programme",
                "country": country,
                "source": SOURCE,
                "published_date": published_date,
                "closing_date": closing_date,
                "status": "open",
                "budget": "",
                "url": url,
                "created_at": utc_now_iso(),
            }
        )
    return rows
