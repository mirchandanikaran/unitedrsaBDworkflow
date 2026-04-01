"""Connector for ILO UNGM RSS tender feed."""

from __future__ import annotations

import logging
import os
import xml.etree.ElementTree as ET
from typing import Any

import requests

from utils.helpers import compact_text, generate_tender_id, parse_datetime_to_iso, utc_now_iso


logger = logging.getLogger(__name__)

SOURCE = "ilo"
RSS_URL = os.getenv("ILO_RSS_URL", "https://webapps.ilo.org/webcommon/php/ungm-rss.php")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
MAX_ITEMS = int(os.getenv("ILO_MAX_ITEMS", "0"))


def _request_feed() -> str:
    """Fetch ILO RSS XML feed."""
    last_error: Exception | None = None
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(RSS_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("ILO RSS request failed", extra={"source": SOURCE, "attempt": attempt, "error": str(exc)})
    raise RuntimeError("ILO RSS request failed after retries") from last_error


def fetch_ilo_tenders() -> list[dict[str, Any]]:
    """Return normalized tenders parsed from ILO RSS items."""
    xml_text = _request_feed()
    root = ET.fromstring(xml_text)
    rows: list[dict[str, Any]] = []

    channel = root.find("channel")
    if channel is None:
        return rows

    items = channel.findall("item")
    if MAX_ITEMS > 0:
        items = items[:MAX_ITEMS]
    for item in items:
        title = compact_text(item.findtext("title", default=""))
        url = compact_text(item.findtext("link", default=""))
        if not title or not url:
            continue
        description = compact_text(item.findtext("description", default=title))
        published = parse_datetime_to_iso(item.findtext("pubDate", default=""))

        rows.append(
            {
                "id": generate_tender_id(SOURCE, url),
                "title": title,
                "description": description,
                "organization": "International Labour Organization",
                "country": "Global",
                "source": SOURCE,
                "published_date": published,
                "closing_date": None,
                "status": "open",
                "budget": "",
                "url": url,
                "created_at": utc_now_iso(),
            }
        )
    return rows
