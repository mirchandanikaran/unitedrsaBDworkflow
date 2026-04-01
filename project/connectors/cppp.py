"""Connector for CPPP (India NIC procurement portal)."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils.helpers import generate_tender_id, normalize_status, parse_datetime_to_iso, utc_now_iso


logger = logging.getLogger(__name__)

CPPP_SOURCE = "cppp"
CPPP_BASE_URL = os.getenv("CPPP_BASE_URL", "https://eprocure.gov.in/eprocure/app")
CPPP_ORG_LIST_URL = os.getenv(
    "CPPP_ORG_LIST_URL",
    "https://eprocure.gov.in/eprocure/app?page=FrontEndTendersByOrganisation&service=page",
)
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
CPPP_MAX_ORG_LINKS = int(os.getenv("CPPP_MAX_ORG_LINKS", "0"))
CPPP_MAX_RESULTS = int(os.getenv("CPPP_MAX_RESULTS", "0"))


def _absolute_url(path_or_url: str) -> str:
    """Convert relative CPPP URL into absolute form."""
    if not path_or_url:
        return ""
    return urljoin("https://eprocure.gov.in", path_or_url)


def _request_page(url: str, session: requests.Session) -> str:
    """Fetch one CPPP page with retries."""
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, headers=headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning(
                "CPPP request failed",
                extra={"source": CPPP_SOURCE, "url": url, "attempt": attempt, "error": str(exc)},
            )

    raise RuntimeError(f"CPPP page request failed after retries: url={url}") from last_error


def _extract_org_listing_links(html: str) -> list[str]:
    """Extract per-organisation listing links from the main CPPP org page."""
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []

    for anchor in soup.find_all("a"):
        href = (anchor.get("href") or "").strip()
        if not href or "component=%24DirectLink" not in href:
            continue
        if "page=FrontEndTendersByOrganisation" not in href:
            continue
        links.append(_absolute_url(href))

    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)

    return deduped


def _extract_tender_id(title_cell_text: str) -> str:
    """Extract stable tender id token from CPPP title/reference text."""
    patterns = [
        r"Tender ID[:\s/-]*([A-Za-z0-9._-]+)",
        r"/\s*([A-Za-z0-9._-]{6,})\s*$",
    ]
    for pattern in patterns:
        match = re.search(pattern, title_cell_text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _build_cppp_id_key(
    title_text: str,
    organization: str,
    published_raw: str,
    closing_raw: str,
    tender_id_token: str,
) -> str:
    """Build stable key string used to derive deterministic tender ID."""
    if tender_id_token:
        return f"https://eprocure.gov.in/tender/{tender_id_token.lower()}"

    stable_text = "||".join(
        [
            title_text.strip().lower(),
            organization.strip().lower(),
            published_raw.strip().lower(),
            closing_raw.strip().lower(),
        ]
    )
    digest = hashlib.sha256(stable_text.encode("utf-8")).hexdigest()
    return f"https://eprocure.gov.in/tender/{digest}"


def _normalize_row(row: Any, headers: list[str]) -> dict[str, Any] | None:
    """Normalize one CPPP table row into the unified tender schema."""
    cols = row.find_all("td")
    if not cols:
        return None

    cells: dict[str, str] = {}
    for idx, col in enumerate(cols):
        key = headers[idx] if idx < len(headers) else f"col_{idx}"
        cells[key] = col.get_text(" ", strip=True)

    # Column layout expected: published, closing, opening, title/ref, organization
    title_col = cols[4] if len(cols) >= 5 else cols[-1]
    anchor = title_col.find("a") if title_col else None
    raw_detail_url = _absolute_url((anchor.get("href") or "").strip()) if anchor else ""
    if not raw_detail_url:
        return None
    detail_url = raw_detail_url

    title_text = (anchor.get_text(" ", strip=True) if anchor else title_col.get_text(" ", strip=True)).strip()
    tender_id_token = _extract_tender_id(title_col.get_text(" ", strip=True))

    organization = (
        cells.get("organisation chain")
        or cells.get("organisation")
        or (cols[5].get_text(" ", strip=True) if len(cols) >= 6 else "")
    )
    published_raw = (
        cells.get("e-published date")
        or cells.get("published date")
        or cells.get("date")
        or (cols[1].get_text(" ", strip=True) if len(cols) >= 2 else "")
    )
    closing_raw = (
        cells.get("closing date")
        or cells.get("bid submission closing date")
        or (cols[2].get_text(" ", strip=True) if len(cols) >= 3 else "")
    )
    status_raw = cells.get("status") or ("open" if closing_raw else "")

    description = title_col.get_text(" ", strip=True)
    id_key = _build_cppp_id_key(
        title_text=title_text,
        organization=organization,
        published_raw=published_raw,
        closing_raw=closing_raw,
        tender_id_token=tender_id_token,
    )

    return {
        "id": generate_tender_id(CPPP_SOURCE, id_key),
        "title": title_text,
        "description": description,
        "organization": organization,
        "country": "India",
        "source": CPPP_SOURCE,
        "published_date": parse_datetime_to_iso(published_raw),
        "closing_date": parse_datetime_to_iso(closing_raw),
        "status": normalize_status(status_raw),
        "budget": cells.get("value") or "",
        "url": detail_url or raw_detail_url,
        "created_at": utc_now_iso(),
    }


def _extract_tenders_from_html(html: str) -> list[dict[str, Any]]:
    """Parse one CPPP organisation listing HTML and return tenders."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.list_table") or soup.select_one("table")
    if not table:
        return []

    header_cells = table.select("thead tr th")
    headers = [th.get_text(" ", strip=True).lower() for th in header_cells]
    body_rows = table.select("tbody tr")
    if not body_rows:
        body_rows = table.select("tr")[1:]

    normalized: list[dict[str, Any]] = []
    for row in body_rows:
        item = _normalize_row(row, headers=headers)
        if item:
            normalized.append(item)

    return normalized


def fetch_cppp_tenders(max_pages: int = 0) -> list[dict[str, Any]]:
    """
    Fetch tenders from CPPP and return normalized JSON records only.

    This connector does not write to the database.
    """
    all_rows: list[dict[str, Any]] = []
    session = requests.Session()

    try:
        root_html = _request_page(url=CPPP_ORG_LIST_URL, session=session)
    except RuntimeError as exc:
        logger.error("CPPP root page failed", extra={"source": CPPP_SOURCE, "error": str(exc)})
        raise

    org_links = _extract_org_listing_links(root_html)
    if not org_links:
        logger.warning("CPPP organization links not found", extra={"source": CPPP_SOURCE})
        return []

    org_links_to_scan = list(org_links)
    if max_pages > 0:
        org_links_to_scan = org_links_to_scan[: max_pages * (CPPP_MAX_ORG_LINKS if CPPP_MAX_ORG_LINKS > 0 else len(org_links_to_scan))]
    if CPPP_MAX_ORG_LINKS > 0:
        org_links_to_scan = org_links_to_scan[:CPPP_MAX_ORG_LINKS]

    for idx, org_link in enumerate(org_links_to_scan, start=1):
        try:
            org_html = _request_page(url=org_link, session=session)
        except RuntimeError as exc:
            logger.warning(
                "CPPP organisation page failed",
                extra={"source": CPPP_SOURCE, "index": idx, "url": org_link, "error": str(exc)},
            )
            continue

        org_rows = _extract_tenders_from_html(org_html)
        if org_rows:
            all_rows.extend(org_rows)
        if CPPP_MAX_RESULTS > 0 and len(all_rows) >= CPPP_MAX_RESULTS:
            break

    if CPPP_MAX_RESULTS > 0:
        return all_rows[:CPPP_MAX_RESULTS]
    return all_rows
