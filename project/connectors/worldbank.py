"""Connector for World Bank procurement notices."""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import requests

from utils.helpers import generate_tender_id, normalize_status, parse_datetime_to_iso, utc_now_iso


logger = logging.getLogger(__name__)

WORLD_BANK_SOURCE = "worldbank"
WORLD_BANK_API_URL = os.getenv(
    "WORLD_BANK_API_URL",
    "https://search.worldbank.org/api/v2/wds",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
WORLD_BANK_QUERY = os.getenv("WORLD_BANK_QUERY", "tender OR procurement OR bid")
WORLD_BANK_PAGE_SIZE = int(os.getenv("WORLD_BANK_PAGE_SIZE", "250"))
WORLD_BANK_MAX_PAGES = int(os.getenv("WORLD_BANK_MAX_PAGES", "0"))


def _textify(value: Any) -> str:
    """Extract readable text from mixed World Bank field formats."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for nested in value.values():
            text = _textify(nested)
            if text:
                return text
        return ""
    if isinstance(value, list):
        for nested in value:
            text = _textify(nested)
            if text:
                return text
        return ""
    return str(value).strip()


def _request_page(page: int, page_size: int) -> dict[str, Any]:
    """Request one World Bank page with retries."""
    params = {
        "format": "json",
        "rows": page_size,
        "os": (page - 1) * page_size,
        "qterm": WORLD_BANK_QUERY,
    }
    last_error: Exception | None = None
    headers = {
        "Accept": "application/json",
        "User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0"),
    }

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(
                WORLD_BANK_API_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "World Bank request failed",
                extra={"source": WORLD_BANK_SOURCE, "page": page, "attempt": attempt, "error": str(exc)},
            )

    raise RuntimeError(f"World Bank page request failed after retries: page={page}") from last_error


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract records from common World Bank payload shape."""
    documents = payload.get("documents")
    if isinstance(documents, dict):
        return [item for item in documents.values() if isinstance(item, dict)]
    if isinstance(documents, list):
        return [item for item in documents if isinstance(item, dict)]
    if isinstance(payload.get("results"), list):
        return payload["results"]
    return []


def _normalize_item(item: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one World Bank notice/document into tender schema."""
    url = item.get("url") or item.get("docurl") or item.get("pdfurl")
    if not url:
        return None

    title = _textify(item.get("display_title") or item.get("title"))
    description = _textify(item.get("abstracts") or item.get("description"))
    if not description:
        description = title
    organization = item.get("author") or item.get("orgname") or "World Bank"
    country = item.get("countryshortname") or item.get("country") or item.get("count") or ""
    if not country and isinstance(title, str):
        match = re.match(r"^\s*([A-Za-z][A-Za-z .'-]{2,})\s*-\s*", title)
        if match:
            country = match.group(1).strip()
    if not country:
        country = "Global"

    published_date = parse_datetime_to_iso(item.get("docty_date") or item.get("date"))
    if not published_date:
        published_date = parse_datetime_to_iso(item.get("docdt") or item.get("disclosure_date"))

    closing_date = parse_datetime_to_iso(item.get("closing_date") or item.get("deadline_date"))
    if not closing_date:
        closing_date = parse_datetime_to_iso(item.get("deadline for receipt of bid"))

    status = normalize_status(_textify(item.get("status") or item.get("disclstat")))
    budget = item.get("lendprojectcost") or item.get("budget") or ""

    return {
        "id": generate_tender_id(WORLD_BANK_SOURCE, str(url)),
        "title": str(title),
        "description": str(description),
        "organization": str(organization),
        "country": str(country),
        "source": WORLD_BANK_SOURCE,
        "published_date": published_date,
        "closing_date": closing_date,
        "status": status,
        "budget": str(budget) if budget is not None else "",
        "url": str(url),
        "created_at": utc_now_iso(),
    }


def fetch_world_bank_tenders(
    max_pages: int = WORLD_BANK_MAX_PAGES,
    page_size: int = WORLD_BANK_PAGE_SIZE,
) -> list[dict[str, Any]]:
    """
    Fetch tenders from World Bank and return normalized JSON records only.

    This connector does not write to the database.
    """
    normalized_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    page = 1
    while True:
        if max_pages > 0 and page > max_pages:
            break
        try:
            payload = _request_page(page=page, page_size=page_size)
        except RuntimeError as exc:
            logger.error(
                "World Bank connector page failed",
                extra={"source": WORLD_BANK_SOURCE, "page": page, "error": str(exc)},
            )
            if page == 1:
                raise
            break

        page_items = _extract_items(payload)
        if not page_items:
            break

        for item in page_items:
            normalized = _normalize_item(item)
            if normalized is None:
                logger.info("World Bank item skipped due to missing URL", extra={"source": WORLD_BANK_SOURCE})
                continue
            row_id = str(normalized.get("id", ""))
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            normalized_rows.append(normalized)

        if len(page_items) < page_size:
            break
        page += 1

    return normalized_rows
