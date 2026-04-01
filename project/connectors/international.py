"""Connectors for international portals not yet covered by dedicated modules.

Each portal is defined as a metadata dict and handled by the generic
page-scraping approach used across the codebase.
"""

from __future__ import annotations

import logging
import os
from typing import Any

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

REQUEST_TIMEOUT = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"
DEFAULT_MAX_LINKS = 0

INTERNATIONAL_PORTALS: list[dict[str, str]] = [
    {"source": "iucn", "url": "https://iucn.org/procurement/currently-running-tenders",
     "org": "IUCN", "country": "Global"},
    {"source": "unido", "url": "https://www.unido.org/get-involved/procurement/procurement-opportunities",
     "org": "UNIDO", "country": "Global"},
    {"source": "conservation_intl", "url": "https://www.conservation.org/procurement-opportunities",
     "org": "Conservation International", "country": "Global"},
    {"source": "sam_gov", "url": "https://sam.gov/opportunities",
     "org": "SAM.gov", "country": "United States"},
    {"source": "usaid", "url": "https://www.globaltenders.com/funding-agency/USAID-tenders",
     "org": "USAID", "country": "Global"},
    {"source": "danida", "url": "https://um.dk/en/danida/procurement/danida-contracts",
     "org": "DANIDA", "country": "Denmark"},
    {"source": "jica", "url": "https://www.jica.go.jp/english/activities/schemes/grant_aid/procurement.html",
     "org": "JICA", "country": "Japan"},
    {"source": "fcdo", "url": "https://www.gov.uk/government/organisations/foreign-commonwealth-development-office/about/procurement",
     "org": "FCDO", "country": "United Kingdom"},
    {"source": "enabel", "url": "https://www.enabel.be/public-procurement/",
     "org": "Enabel", "country": "Belgium"},
    {"source": "finnpartnership", "url": "https://finlandabroad.fi/web/mdg/opportunities-in-development-cooperation-for-companies",
     "org": "Ministry for Foreign Affairs / Finnpartnership", "country": "Finland"},
    {"source": "kfw", "url": "https://www.gtai.de/en/meta/search/kfw-tenders/795748!search",
     "org": "KfW Development Bank", "country": "Germany"},
    {"source": "gcf", "url": "https://iaayou.fa.ocs.oraclecloud.com/fscmUI/faces/NegotiationAbstracts?prcBuId=300000003621906",
     "org": "Green Climate Fund", "country": "Global"},
    {"source": "tgf", "url": "https://fa-enmo-saasfaprod1.fa.ocs.oraclecloud.com/fscmUI/faces/NegotiationAbstracts?prcBuId=300000003071579",
     "org": "The Global Fund", "country": "Global"},
    {"source": "gggi", "url": "https://in-tendhost.co.uk/gggi/aspx/Tenders/Current",
     "org": "GGGI", "country": "Global"},
    {"source": "gavi", "url": "https://www.gavi.org/our-alliance/work-us/rfps-eois-and-consulting-opportunities",
     "org": "GAVI", "country": "Global"},
    {"source": "dfat", "url": "https://www.dfat.gov.au/about-us/business-opportunities/business-notifications",
     "org": "DFAT", "country": "Australia"},
    {"source": "mcc", "url": "https://www.mcc.gov/where-we-work/",
     "org": "Millennium Challenge Corporation", "country": "United States"},
    {"source": "giz", "url": "https://ausschreibungen.giz.de/Satellite/company/welcome.do",
     "org": "GIZ", "country": "Germany"},
    {"source": "african_union", "url": "https://au.int/en/bids",
     "org": "African Union", "country": "Africa"},
    {"source": "sida", "url": "https://www.kommersannons.se/eLite/Notice/NoticeList.aspx?ProcuringEntityId=243",
     "org": "SIDA", "country": "Sweden"},
    {"source": "norad", "url": "https://www.globaltenders.com/funding-agency/norwegian-agency-for-development-co-operation-tenders.php",
     "org": "Norad", "country": "Norway"},
]


def _fetch_portal_html(url: str) -> str:
    headers = {"User-Agent": os.getenv("CONNECTOR_USER_AGENT", "TenderIntelBot/1.0")}
    last_error: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Intl portal request failed", extra={"url": url, "attempt": attempt})
    if PLAYWRIGHT_FALLBACK:
        return fetch_rendered_html(url)
    raise RuntimeError(f"International portal request failed: {url}") from last_error


def fetch_single_international(source: str, url: str, organization: str, country: str,
                                max_links: int = DEFAULT_MAX_LINKS) -> list[dict[str, Any]]:
    """Fetch tenders from a single international portal."""
    try:
        html = _fetch_portal_html(url)
    except Exception as exc:
        logger.warning("Skipping intl portal", extra={"source": source, "error": str(exc)})
        return []

    links = extract_candidate_notice_links(
        html,
        base_url=url,
        include_patterns=(
            "tender", "bid", "procurement", "notice", "solicitation",
            "rfp", "eoi", "rfq", "opportunity", "contract",
        ),
        max_links=max_links,
    )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in links:
        link_url = link["url"]
        if link_url in seen:
            continue
        seen.add(link_url)
        desc = compact_text(link.get("context") or link["title"])
        pub, closing = extract_dates_from_text(desc)
        rows.append({
            "id": generate_tender_id(source, link_url),
            "title": compact_text(link["title"]),
            "description": desc,
            "organization": organization,
            "country": country,
            "source": source,
            "published_date": pub,
            "closing_date": closing,
            "status": "open",
            "budget": "",
            "url": link_url,
            "created_at": utc_now_iso(),
        })
    return rows


def fetch_all_international_portals() -> list[dict[str, Any]]:
    """Fetch tenders from all configured international portals."""
    all_rows: list[dict[str, Any]] = []
    max_links = int(os.getenv("INTL_MAX_LINKS_PER_PORTAL", str(DEFAULT_MAX_LINKS)))
    for portal in INTERNATIONAL_PORTALS:
        rows = fetch_single_international(
            source=portal["source"],
            url=portal["url"],
            organization=portal["org"],
            country=portal["country"],
            max_links=max_links,
        )
        logger.info("Intl portal fetched", extra={"source": portal["source"], "count": len(rows)})
        all_rows.extend(rows)
    return all_rows
