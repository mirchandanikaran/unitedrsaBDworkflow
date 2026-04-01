"""Connector for African Development Bank solicitations."""

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
    generate_tender_id,
    utc_now_iso,
)


logger = logging.getLogger(__name__)

SOURCE = "afdb"
BASE_URL = os.getenv(
    "AFDB_URL",
    "https://www.afdb.org/en/about-us/corporate-procurement/procurement-notices/current-solicitations",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"


def _request_page() -> str:
    """Fetch AFDB current solicitations page."""
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(BASE_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("AFDB request failed", extra={"source": SOURCE, "attempt": attempt, "error": str(exc)})
    if PLAYWRIGHT_FALLBACK:
        logger.info("AFDB fallback to Playwright", extra={"source": SOURCE})
        return fetch_rendered_html(BASE_URL)
    raise RuntimeError("AFDB request failed after retries") from last_error


def fetch_afdb_tenders() -> list[dict[str, Any]]:
    """Return normalized tenders from AFDB current solicitations page."""
    html = _request_page()
    links = extract_candidate_notice_links(
        html,
        base_url=BASE_URL,
        include_patterns=("solicitation", "procurement", "tender", "bid", "notice"),
        max_links=int(os.getenv("AFDB_MAX_LINKS", "0")),
    )

    rows: list[dict[str, Any]] = []
    for link in links:
        description = compact_text(link.get("context") or link["title"])
        published_date, closing_date = extract_dates_from_text(description)
        rows.append(
            {
                "id": generate_tender_id(SOURCE, link["url"]),
                "title": compact_text(link["title"]),
                "description": description,
                "organization": "African Development Bank",
                "country": "Global",
                "source": SOURCE,
                "published_date": published_date,
                "closing_date": closing_date,
                "status": "open",
                "budget": "",
                "url": link["url"],
                "created_at": utc_now_iso(),
            }
        )
    return rows
