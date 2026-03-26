"""Shared utility helpers for tender connectors and services."""

from __future__ import annotations

import hashlib
import logging
import re
from urllib.parse import urljoin
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from bs4 import BeautifulSoup
import requests


def generate_tender_id(source: str, url: str) -> str:
    """Build deterministic tender ID from source and URL."""
    payload = f"{source.strip().lower()}::{url.strip()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def utc_now_iso() -> str:
    """Return current UTC datetime in ISO format."""
    return datetime.now(UTC).isoformat()


def parse_datetime_to_iso(value: object) -> str | None:
    """Convert source datetime text into UTC ISO8601 when possible."""
    if not value:
        return None

    if isinstance(value, list):
        if not value:
            return None
        first = value[0]
        if not isinstance(first, str):
            return None
        text = first.strip()
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None

    if not text:
        return None

    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            else:
                parsed = parsed.astimezone(UTC)
            return parsed.isoformat()
        parsed = None
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%d-%m-%Y %H:%M",
            "%d/%m/%Y %H:%M",
            "%d-%b-%Y",
            "%d-%b-%Y %I:%M %p",
            "%d-%b-%Y %H:%M",
        ):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)

    return parsed.isoformat()


def normalize_status(value: str | None) -> str:
    """Map source-specific status into open/closed/awarded."""
    if not value:
        return "open"

    status_text = value.strip().lower()
    if any(token in status_text for token in ("award", "awarded", "contract signed")):
        return "awarded"
    if any(token in status_text for token in ("closed", "cancelled", "canceled", "expired")):
        return "closed"
    return "open"


def configure_logging(log_level: str = "INFO") -> None:
    """Configure simple structured logging once for the application."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def extract_candidate_notice_links(
    html: str,
    *,
    base_url: str,
    include_patterns: tuple[str, ...] = ("tender", "bid", "procurement", "notice", "solicitation", "rfx"),
    max_links: int = 200,
) -> list[dict[str, str]]:
    """Extract likely tender links from generic procurement listing pages."""
    soup = BeautifulSoup(html, "html.parser")
    records: list[dict[str, str]] = []
    seen: set[str] = set()
    noisy_titles = {
        "home",
        "login",
        "register",
        "read more",
        "published",
        "deadline",
        "deadline (local time)",
        "view",
        "details",
        "open",
        "english",
        "francais",
        "fran",
        "portugues",
        "espanol",
        "deutsch",
    }

    for anchor in soup.find_all("a"):
        href = (anchor.get("href") or "").strip()
        title = anchor.get_text(" ", strip=True)
        if not href or not title:
            continue

        lower_title = title.lower()
        lower_href = href.lower()
        if not any(token in lower_title or token in lower_href for token in include_patterns):
            continue
        if len(title) < 8:
            continue
        if " " not in title and len(title) < 14:
            continue
        if lower_title in noisy_titles:
            continue
        if href.startswith("#") or href.lower().startswith("javascript:"):
            continue

        absolute_url = urljoin(base_url, href)
        if absolute_url.endswith((".css", ".js", ".png", ".jpg", ".jpeg", ".svg", ".pdf")):
            continue
        if absolute_url in seen:
            continue
        seen.add(absolute_url)

        context = compact_text(anchor.parent.get_text(" ", strip=True) if anchor.parent else "")
        records.append({"title": title, "url": absolute_url, "context": context})
        if len(records) >= max_links:
            break

    return records


def compact_text(value: Any) -> str:
    """Normalize arbitrary text-like value into single-line text."""
    if value is None:
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    return re.sub(r"\s+", " ", str(value)).strip()


def extract_dates_from_text(text: str) -> tuple[str | None, str | None]:
    """Extract potential published/closing datetimes from free-form text."""
    if not text:
        return None, None

    patterns = (
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{2}-[A-Za-z]{3}-\d{4}(?:\s+\d{1,2}:\d{2}(?:\s*[APap][Mm])?)?\b",
        r"\b\d{2}/\d{2}/\d{4}(?:\s+\d{1,2}:\d{2})?\b",
        r"\b\d{2}-\d{2}-\d{4}(?:\s+\d{1,2}:\d{2})?\b",
    )
    found: list[str] = []
    for pattern in patterns:
        found.extend(re.findall(pattern, text))
    if not found:
        return None, None

    parsed = [parse_datetime_to_iso(token) for token in found]
    parsed = [value for value in parsed if value is not None]
    if not parsed:
        return None, None

    if len(parsed) == 1:
        return parsed[0], parsed[0]
    return parsed[0], parsed[1]


def fetch_rendered_html(url: str, timeout_ms: int = 45000) -> str:
    """Render a page with Playwright and return the final HTML."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("Playwright is not available") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except Exception:
            pass
        html = page.content()
        context.close()
        browser.close()
        return html


def fetch_detail_enrichment(
    *,
    url: str,
    use_playwright_fallback: bool = True,
    timeout_seconds: int = 30,
) -> dict[str, str | None]:
    """Fetch a detail page and extract best-effort structured fields."""
    html = ""
    try:
        response = requests.get(url, timeout=timeout_seconds, headers={"User-Agent": "TenderIntelBot/1.0"})
        response.raise_for_status()
        html = response.text
    except Exception:
        if use_playwright_fallback:
            try:
                html = fetch_rendered_html(url, timeout_ms=timeout_seconds * 1000)
            except Exception:
                return {"description": None, "published_date": None, "closing_date": None, "country": None}
        else:
            return {"description": None, "published_date": None, "closing_date": None, "country": None}

    soup = BeautifulSoup(html, "html.parser")
    meta_description = ""
    meta_tag = soup.find("meta", attrs={"name": "description"}) or soup.find("meta", attrs={"property": "og:description"})
    if meta_tag and meta_tag.get("content"):
        meta_description = compact_text(meta_tag.get("content"))

    body_text = compact_text(soup.get_text(" ", strip=True))
    if len(body_text) > 12000:
        body_text = body_text[:12000]

    description = meta_description or body_text[:500]
    published_date, closing_date = extract_dates_from_text(body_text)

    country = None
    country_match = re.search(r"(country|location|place of performance)\s*[:\-]\s*([A-Za-z][A-Za-z .,'-]{2,60})", body_text, re.IGNORECASE)
    if country_match:
        country = compact_text(country_match.group(2))

    return {
        "description": description or None,
        "published_date": published_date,
        "closing_date": closing_date,
        "country": country,
    }
