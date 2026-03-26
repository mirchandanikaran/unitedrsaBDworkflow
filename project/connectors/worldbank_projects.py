"""Connector for World Bank projects procurement portal."""

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
    generate_tender_id,
    utc_now_iso,
)


logger = logging.getLogger(__name__)

SOURCE = "worldbank_projects"
BASE_URL = os.getenv(
    "WORLD_BANK_PROJECTS_URL",
    "https://projects.worldbank.org/en/projects-operations/procurement?srce=both",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
MAX_PAGES = int(os.getenv("WORLD_BANK_PROJECTS_MAX_PAGES", "20"))
MAX_LINKS_PER_PAGE = int(os.getenv("WORLD_BANK_PROJECTS_MAX_LINKS_PER_PAGE", "300"))
DETAIL_FETCH_LIMIT = int(os.getenv("WORLD_BANK_PROJECTS_DETAIL_FETCH_LIMIT", "300"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"


def _request_page(page: int) -> str:
    """Fetch one World Bank projects procurement page."""
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
            logger.warning(
                "World Bank projects request failed",
                extra={"source": SOURCE, "page": page, "attempt": attempt, "error": str(exc)},
            )

    raise RuntimeError(f"World Bank projects request failed for page={page}") from last_error


def fetch_world_bank_projects_tenders(max_pages: int = MAX_PAGES) -> list[dict[str, Any]]:
    """Return normalized tenders from World Bank projects procurement listing."""
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    enriched_count = 0

    for page in range(1, max_pages + 1):
        html = _request_page(page=page)
        links = extract_candidate_notice_links(
            html,
            base_url=BASE_URL,
            include_patterns=("procurement", "tender", "project", "bid"),
            max_links=MAX_LINKS_PER_PAGE,
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
            if enriched_count < DETAIL_FETCH_LIMIT:
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
                    "organization": "World Bank",
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

    return rows
