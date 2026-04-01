"""Connector for World Bank RfxNow public advertisements."""

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

SOURCE = "worldbank_rfx"
BASE_URL = os.getenv(
    "WORLD_BANK_RFX_URL",
    "https://wbgeprocure-rfxnow.worldbank.org/rfxnow/public/advertisement/index.html",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"
DETAIL_FETCH_LIMIT = int(os.getenv("WORLD_BANK_RFX_DETAIL_FETCH_LIMIT", "0"))


def _request_page() -> str:
    """Fetch World Bank RfxNow page content."""
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(BASE_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "World Bank RFX request failed",
                extra={"source": SOURCE, "attempt": attempt, "error": str(exc)},
            )

    if PLAYWRIGHT_FALLBACK:
        logger.info("World Bank RFX fallback to Playwright", extra={"source": SOURCE})
        return fetch_rendered_html(BASE_URL)

    raise RuntimeError("World Bank RFX request failed after retries") from last_error


def fetch_world_bank_rfx_tenders() -> list[dict[str, Any]]:
    """Return normalized tenders from World Bank RFX public page."""
    html = _request_page()
    links = extract_candidate_notice_links(
        html,
        base_url=BASE_URL,
        include_patterns=("rfx", "tender", "bid", "procurement", "advertisement", "solicitation"),
        max_links=int(os.getenv("WORLD_BANK_RFX_MAX_LINKS", "0")),
    )
    if not links and PLAYWRIGHT_FALLBACK:
        rendered_html = fetch_rendered_html(BASE_URL)
        links = extract_candidate_notice_links(
            rendered_html,
            base_url=BASE_URL,
            include_patterns=("rfx", "tender", "bid", "procurement", "advertisement", "solicitation"),
            max_links=int(os.getenv("WORLD_BANK_RFX_MAX_LINKS", "0")),
        )

    rows: list[dict[str, Any]] = []
    enriched_count = 0
    for link in links:
        title_text = compact_text(link["title"]).lower()
        if title_text in {"advertisements", "download terms & conditions"}:
            continue
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
                "id": generate_tender_id(SOURCE, link["url"]),
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
    return rows
