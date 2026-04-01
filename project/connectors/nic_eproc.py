"""Generic connector for Indian NIC GePNIC e-procurement portals.

All Indian state and central portals running the standard nicgep/app
platform share the same HTML structure, so a single parameterized
connector handles 30+ portals.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from utils.helpers import (
    compact_text,
    extract_candidate_notice_links,
    extract_dates_from_text,
    fetch_rendered_html,
    generate_tender_id,
    utc_now_iso,
)

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = int(os.getenv("CONNECTOR_TIMEOUT_SECONDS", "20"))
RETRY_ATTEMPTS = int(os.getenv("CONNECTOR_RETRY_ATTEMPTS", "3"))
PLAYWRIGHT_FALLBACK = os.getenv("PLAYWRIGHT_FALLBACK", "true").lower() == "true"
DEFAULT_MAX_LINKS = 0

NIC_PORTALS: list[dict[str, str]] = [
    {"source": "eproc_cppp", "url": "https://eprocure.gov.in/eprocure/app", "org": "CPPP eProcure", "state": "India"},
    {"source": "eproc_up", "url": "https://etender.up.nic.in/nicgep/app", "org": "UP eProcure", "state": "Uttar Pradesh"},
    {"source": "eproc_haryana", "url": "https://etenders.hry.nic.in/nicgep/app", "org": "Haryana eProcure", "state": "Haryana"},
    {"source": "eproc_gujarat", "url": "https://gil.gujarat.gov.in/eprocurement", "org": "Gujarat eProcure", "state": "Gujarat"},
    {"source": "eproc_bihar", "url": "https://eproc2.bihar.gov.in/EPSV2Web/", "org": "Bihar eProcure", "state": "Bihar"},
    {"source": "eproc_tn", "url": "https://tntenders.gov.in/nicgep/app", "org": "Tamil Nadu eProcure", "state": "Tamil Nadu"},
    {"source": "eproc_punjab", "url": "https://eproc.punjab.gov.in/nicgep/app", "org": "Punjab eProcure", "state": "Punjab"},
    {"source": "eproc_lakshadweep", "url": "https://tendersutl.gov.in/nicgep/app", "org": "Lakshadweep eProcure", "state": "Lakshadweep"},
    {"source": "eproc_delhi", "url": "https://govtprocurement.delhi.gov.in/nicgep/app", "org": "Delhi eProcure", "state": "Delhi"},
    {"source": "eproc_manipur", "url": "https://manipurtenders.gov.in/nicgep/app", "org": "Manipur eProcure", "state": "Manipur"},
    {"source": "eproc_ntpc", "url": "https://eprocurentpc.nic.in/nicgep/app", "org": "NTPC", "state": "India"},
    {"source": "eproc_daman", "url": "https://ddtenders.gov.in/nicgep/app", "org": "Daman & Diu eProcure", "state": "Daman & Diu"},
    {"source": "eproc_ladakh", "url": "https://tenders.ladakh.gov.in/nicgep/app", "org": "Ladakh eProcure", "state": "Ladakh"},
    {"source": "eproc_cil", "url": "https://coalindiatenders.nic.in/nicgep/app", "org": "Coal India Limited", "state": "India"},
    {"source": "eproc_jk", "url": "https://jktenders.gov.in/nicgep/app", "org": "J&K eProcure", "state": "Jammu & Kashmir"},
    {"source": "eproc_karnataka", "url": "https://www.eproc.karnataka.gov.in/eprocportal/pages/index.jsp", "org": "Karnataka eProcure", "state": "Karnataka"},
    {"source": "eproc_kerala", "url": "https://etenders.kerala.gov.in/nicgep/app", "org": "Kerala eProcure", "state": "Kerala"},
    {"source": "eproc_chandigarh", "url": "https://etenders.chd.nic.in/nicgep/app", "org": "Chandigarh eProcure", "state": "Chandigarh"},
    {"source": "eproc_odisha", "url": "https://tendersodisha.gov.in/nicgep/app", "org": "Odisha eProcure", "state": "Odisha"},
    {"source": "eproc_assam", "url": "https://assamtenders.gov.in/nicgep/app", "org": "Assam eProcure", "state": "Assam"},
    {"source": "eproc_cg", "url": "https://eproc.cgstate.gov.in/", "org": "Chhattisgarh eProcure", "state": "Chhattisgarh"},
    {"source": "eproc_telangana", "url": "https://tender.telangana.gov.in/login.html", "org": "Telangana eProcure", "state": "Telangana"},
    {"source": "eproc_tripura", "url": "https://tripuratenders.gov.in/nicgep/app", "org": "Tripura eProcure", "state": "Tripura"},
    {"source": "eproc_andaman", "url": "https://eprocure.andamannicobar.gov.in/nicgep/app", "org": "Andaman & Nicobar eProcure", "state": "Andaman & Nicobar"},
    {"source": "eproc_arunachal", "url": "https://arunachaltenders.gov.in/nicgep/app", "org": "Arunachal Pradesh eProcure", "state": "Arunachal Pradesh"},
    {"source": "eproc_goa", "url": "https://eprocure.goa.gov.in/nicgep/app", "org": "Goa eProcure", "state": "Goa"},
    {"source": "eproc_nagaland", "url": "https://nagalandtenders.gov.in/", "org": "Nagaland eProcure", "state": "Nagaland"},
    {"source": "eproc_mizoram", "url": "https://mizoramtenders.gov.in/", "org": "Mizoram eProcure", "state": "Mizoram"},
    {"source": "eproc_sikkim", "url": "https://sikkimtender.gov.in/nicgep/app", "org": "Sikkim eProcure", "state": "Sikkim"},
    {"source": "eproc_mp", "url": "https://mptenders.gov.in/nicgep/app", "org": "Madhya Pradesh eProcure", "state": "Madhya Pradesh"},
    {"source": "eproc_uttarakhand", "url": "https://uktenders.gov.in/nicgep/app", "org": "Uttarakhand eProcure", "state": "Uttarakhand"},
    {"source": "eproc_meghalaya", "url": "https://meghalayatenders.gov.in/nicgep/app", "org": "Meghalaya eProcure", "state": "Meghalaya"},
    {"source": "eproc_jharkhand", "url": "https://jharkhandtenders.gov.in/nicgep/app", "org": "Jharkhand eProcure", "state": "Jharkhand"},
    {"source": "eproc_wb", "url": "https://wbtenders.gov.in/nicgep/app", "org": "West Bengal eProcure", "state": "West Bengal"},
    {"source": "eproc_hp", "url": "https://hptenders.gov.in/nicgep/app", "org": "Himachal Pradesh eProcure", "state": "Himachal Pradesh"},
    {"source": "eproc_ap", "url": "https://tender.apeprocurement.gov.in/login.html", "org": "Andhra Pradesh eProcure", "state": "Andhra Pradesh"},
    {"source": "eproc_rajasthan", "url": "https://eproc.rajasthan.gov.in/nicgep/app", "org": "Rajasthan eProcure", "state": "Rajasthan"},
    {"source": "eproc_maharashtra", "url": "https://mahatenders.gov.in/nicgep/app", "org": "Maharashtra eProcure", "state": "Maharashtra"},
    {"source": "eproc_puducherry", "url": "https://pudutenders.gov.in/nicgep/app", "org": "Puducherry eProcure", "state": "Puducherry"},
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
            logger.warning("NIC portal request failed", extra={"url": url, "attempt": attempt})
    if PLAYWRIGHT_FALLBACK:
        return fetch_rendered_html(url)
    raise RuntimeError(f"NIC portal request failed: {url}") from last_error


def fetch_nic_portal(source: str, url: str, organization: str, state: str,
                     max_links: int = DEFAULT_MAX_LINKS) -> list[dict[str, Any]]:
    """Fetch tenders from a single NIC GePNIC portal."""
    try:
        html = _fetch_portal_html(url)
    except Exception as exc:
        logger.warning("Skipping NIC portal", extra={"source": source, "error": str(exc)})
        return []

    links = extract_candidate_notice_links(
        html,
        base_url=url,
        include_patterns=("tender", "bid", "procurement", "notice", "epublish", "eprocure", "etender"),
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
            "country": "India",
            "source": source,
            "published_date": pub,
            "closing_date": closing,
            "status": "open",
            "budget": "",
            "url": link_url,
            "created_at": utc_now_iso(),
        })
    return rows


def fetch_all_nic_portals() -> list[dict[str, Any]]:
    """Fetch tenders from all configured NIC e-procurement portals."""
    all_rows: list[dict[str, Any]] = []
    max_links = int(os.getenv("NIC_MAX_LINKS_PER_PORTAL", str(DEFAULT_MAX_LINKS)))
    for portal in NIC_PORTALS:
        rows = fetch_nic_portal(
            source=portal["source"],
            url=portal["url"],
            organization=portal["org"],
            state=portal["state"],
            max_links=max_links,
        )
        logger.info("NIC portal fetched", extra={"source": portal["source"], "count": len(rows)})
        all_rows.extend(rows)
    return all_rows
