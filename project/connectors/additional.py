"""Connectors for additional country-specific and niche portals from the BD document."""

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

ADDITIONAL_PORTALS: list[dict[str, str]] = [
    {"source": "bongthom", "url": "https://www.bongthom.com/job_list.html?opportunity_type_ids=5,3&search=true",
     "org": "Bongthom", "country": "Cambodia"},
    {"source": "araa_togo", "url": "https://www.araa.org/fr/appels-projets",
     "org": "Regional Agency for Agriculture and Food", "country": "Togo"},
    {"source": "jao_guinea", "url": "https://jaoguinee.com/categ.php?c=appels-d-offres",
     "org": "JAO", "country": "Guinea"},
    {"source": "selco", "url": "https://selcofoundation.org/tender/",
     "org": "SELCO Foundation", "country": "India"},
    {"source": "sbi", "url": "https://etender.sbi/SBI/",
     "org": "State Bank of India", "country": "India"},
    {"source": "unfpa_lk", "url": "https://srilanka.unfpa.org/en/submission/request-quotations-rfq-conduct-quantitative-and-qualitative-research-understand",
     "org": "UNFPA Sri Lanka", "country": "Sri Lanka"},
    {"source": "jobs_nepal", "url": "https://www.jobsnepal.com/category/expression-of-interest",
     "org": "Jobs Nepal", "country": "Nepal"},
    {"source": "jobs_ge", "url": "https://www.jobs.ge/en/ads/?page=1&q=disaster&cid=&lid=&jid=4&is_en=1",
     "org": "Jobs GE", "country": "Georgia"},
    {"source": "thaingo", "url": "https://www.thaingo.org/jobs/detail/20657",
     "org": "Thai NGO", "country": "Thailand"},
    {"source": "skillspedia", "url": "https://skillspedia.in/tenders-eoi-rfp/",
     "org": "SkillsPedia", "country": "India"},
    {"source": "ethiopia_egp", "url": "https://production.egp.gov.et/egp/bids/all",
     "org": "Ethiopia eGP", "country": "Ethiopia"},
    {"source": "acbar", "url": "https://www.acbar.org/site-rfq?r=Request%20for%20Proposal",
     "org": "ACBAR", "country": "Afghanistan"},
    {"source": "yemenhr", "url": "https://yemenhr.com/tenders",
     "org": "YemenHR", "country": "Yemen"},
    {"source": "kenha", "url": "https://kenha.co.ke/tenders-consultancy-services/",
     "org": "Kenya National Highway Authority", "country": "Kenya"},
    {"source": "malibaara", "url": "https://malibaara.com/tenders/search",
     "org": "MaliBaara", "country": "Mali"},
    {"source": "kenya_redcross", "url": "https://redcross.or.ke/tenders/",
     "org": "Kenya Red Cross", "country": "Kenya"},
    {"source": "aecf", "url": "https://www.aecfafrica.org/careers/terms-of-reference-for-consultancy-services-for-the-final-evaluation-of-the-finance-for-inclusive-growth-in-somalia-fig-somalia-programme/",
     "org": "AECF", "country": "Somalia"},
    {"source": "france_mp", "url": "https://www.marches-publics.gouv.fr/?page=Entreprise.EntrepriseAdvancedSearch&AllCons",
     "org": "Republic Francaise", "country": "France"},
    {"source": "acted", "url": "https://www.acted.org/en/call-for-tenders/",
     "org": "ACTED", "country": "Global"},
    {"source": "pdicai", "url": "https://pdicai.org/Opportunities.aspx",
     "org": "PDICAI", "country": "India"},
    {"source": "rcc_india", "url": "https://int.rcc.com.br/tender/101173793",
     "org": "RCC", "country": "India"},
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
            logger.warning("Additional portal request failed", extra={"url": url, "attempt": attempt})
    if PLAYWRIGHT_FALLBACK:
        return fetch_rendered_html(url)
    raise RuntimeError(f"Additional portal request failed: {url}") from last_error


def fetch_single_additional(source: str, url: str, organization: str, country: str,
                             max_links: int = DEFAULT_MAX_LINKS) -> list[dict[str, Any]]:
    """Fetch tenders from a single additional portal."""
    try:
        html = _fetch_portal_html(url)
    except Exception as exc:
        logger.warning("Skipping additional portal", extra={"source": source, "error": str(exc)})
        return []

    links = extract_candidate_notice_links(
        html,
        base_url=url,
        include_patterns=(
            "tender", "bid", "procurement", "notice", "rfp", "eoi",
            "rfq", "opportunity", "consultancy", "appels", "offres",
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


def fetch_all_additional_portals() -> list[dict[str, Any]]:
    """Fetch tenders from all configured additional portals."""
    all_rows: list[dict[str, Any]] = []
    max_links = int(os.getenv("ADDITIONAL_MAX_LINKS_PER_PORTAL", str(DEFAULT_MAX_LINKS)))
    for portal in ADDITIONAL_PORTALS:
        rows = fetch_single_additional(
            source=portal["source"],
            url=portal["url"],
            organization=portal["org"],
            country=portal["country"],
            max_links=max_links,
        )
        logger.info("Additional portal fetched", extra={"source": portal["source"], "count": len(rows)})
        all_rows.extend(rows)
    return all_rows
