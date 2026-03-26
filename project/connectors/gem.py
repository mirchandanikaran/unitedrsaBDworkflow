"""Connector for GeM all bids listing."""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from utils.helpers import compact_text, extract_candidate_notice_links, extract_dates_from_text, generate_tender_id, utc_now_iso


logger = logging.getLogger(__name__)

SOURCE = "gem"
BASE_URL = os.getenv("GEM_URL", "https://bidplus.gem.gov.in/all-bids")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
MAX_PAGES = int(os.getenv("GEM_MAX_PAGES", "40"))


def _request_page(page: int) -> str:
    """Fetch one GeM bids page."""
    params = {"page": page}
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(BASE_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("GeM request failed", extra={"source": SOURCE, "page": page, "attempt": attempt, "error": str(exc)})
    raise RuntimeError(f"GeM request failed for page={page}") from last_error


def fetch_gem_tenders(max_pages: int = MAX_PAGES) -> list[dict[str, Any]]:
    """Return normalized tenders from GeM all bids pages."""
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        html = _request_page(page=page)
        links = extract_candidate_notice_links(
            html,
            base_url=BASE_URL,
            include_patterns=("bid", "gem", "tender", "procurement", "contract"),
            max_links=int(os.getenv("GEM_MAX_LINKS_PER_PAGE", "300")),
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
            rows.append(
                {
                    "id": tender_id,
                    "title": compact_text(link["title"]),
                    "description": description,
                    "organization": "Government e Marketplace",
                    "country": "India",
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

    return rows
