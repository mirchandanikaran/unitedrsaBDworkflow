"""Connector for European Commission TED browse by place page."""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from utils.helpers import compact_text, extract_candidate_notice_links, extract_dates_from_text, generate_tender_id, utc_now_iso


logger = logging.getLogger(__name__)

SOURCE = "ec_ted"
BASE_URL = os.getenv("EC_TED_URL", "https://ted.europa.eu/en/browse-by-place-of-performance")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))


def _request_page() -> str:
    """Fetch EC TED browse page."""
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(BASE_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("EC TED request failed", extra={"source": SOURCE, "attempt": attempt, "error": str(exc)})
    raise RuntimeError("EC TED request failed after retries") from last_error


def fetch_ec_ted_tenders() -> list[dict[str, Any]]:
    """Return normalized records discovered from EC TED browse page."""
    html = _request_page()
    links = extract_candidate_notice_links(
        html,
        base_url=BASE_URL,
        include_patterns=("notice", "tender", "procurement", "browse", "performance"),
        max_links=int(os.getenv("EC_TED_MAX_LINKS", "500")),
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
                "organization": "European Commission",
                "country": "EU",
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
