"""Ingestion pipeline to fetch, deduplicate, and persist tenders."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import case, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from connectors.cppp import fetch_cppp_tenders
from connectors.adb import fetch_adb_tenders
from connectors.afd import fetch_afd_tenders
from connectors.afdb import fetch_afdb_tenders
from connectors.ebrd import fetch_ebrd_tenders
from connectors.ec_ted import fetch_ec_ted_tenders
from connectors.gem import fetch_gem_tenders
from connectors.aiib import fetch_aiib_tenders
from connectors.ilo import fetch_ilo_tenders
from connectors.iom import fetch_iom_tenders
from connectors.ted import fetch_ted_tenders
from connectors.ungm import fetch_ungm_tenders
from connectors.undp_quantum import fetch_undp_quantum_tenders
from connectors.wfp import fetch_wfp_tenders
from connectors.worldbank import fetch_world_bank_tenders
from connectors.worldbank_projects import fetch_world_bank_projects_tenders
from connectors.worldbank_rfx import fetch_world_bank_rfx_tenders
from models.schemas import SourceIngestionState, Tender
from services.deduplication import deduplicate_tenders


logger = logging.getLogger(__name__)
SOURCE_DEFAULTS: dict[str, dict[str, str]] = {
    "cppp": {"organization": "CPPP", "country": "India"},
    "gem": {"organization": "Government e Marketplace", "country": "India"},
    "ted": {"organization": "TED", "country": "EU"},
    "ec_ted": {"organization": "European Commission", "country": "EU"},
    "worldbank": {"organization": "World Bank", "country": "Global"},
    "worldbank_projects": {"organization": "World Bank", "country": "Global"},
    "worldbank_rfx": {"organization": "World Bank", "country": "Global"},
    "adb": {"organization": "Asian Development Bank", "country": "Global"},
    "afdb": {"organization": "African Development Bank", "country": "Global"},
    "ungm": {"organization": "United Nations", "country": "Global"},
    "undp_quantum": {"organization": "United Nations Development Programme", "country": "Global"},
    "iom": {"organization": "International Organization for Migration", "country": "Global"},
    "wfp": {"organization": "World Food Programme", "country": "Global"},
    "ilo": {"organization": "International Labour Organization", "country": "Global"},
    "aiib": {"organization": "Asian Infrastructure Investment Bank", "country": "Global"},
    "ebrd": {"organization": "European Bank for Reconstruction and Development", "country": "Global"},
    "afd": {"organization": "Agence Francaise de Developpement", "country": "Global"},
}


def _parse_iso_datetime(value: Any) -> datetime | None:
    """Parse ISO string into timezone-aware UTC datetime."""
    if not value or not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _build_tender_model(record: dict[str, Any]) -> Tender | None:
    """Convert normalized record dictionary into Tender ORM object."""
    tender_id = str(record.get("id", "")).strip()
    source = str(record.get("source", "")).strip()
    url = str(record.get("url", "")).strip()
    title = str(record.get("title", "")).strip()
    if not (tender_id and source and url and title):
        return None

    description = str(record.get("description", "")).strip() or title
    organization = str(record.get("organization", "")).strip()
    country = str(record.get("country", "")).strip()

    defaults = SOURCE_DEFAULTS.get(source.lower(), {})
    if _is_blank(organization):
        organization = defaults.get("organization", "")
    if _is_blank(country):
        country = defaults.get("country", "")

    closing_dt = _parse_iso_datetime(record.get("closing_date"))
    status_text = str(record.get("status", "open")).strip() or "open"
    if closing_dt and closing_dt < datetime.now(UTC) and status_text == "open":
        status_text = "closed"

    return Tender(
        id=tender_id,
        title=title,
        description=description,
        organization=organization,
        country=country,
        source=source,
        published_date=_parse_iso_datetime(record.get("published_date")),
        closing_date=closing_dt,
        status=status_text,
        budget=str(record.get("budget", "")).strip(),
        url=url,
        created_at=_parse_iso_datetime(record.get("created_at")) or datetime.now(UTC),
        raw_data=json.dumps(record, ensure_ascii=True),
    )


def _is_blank(value: str | None) -> bool:
    """Return true when text field is unset or placeholder-like."""
    return not value or not value.strip() or value.strip() == "-"


def _update_existing_tender(existing: Tender, incoming: Tender) -> bool:
    """Backfill missing fields on existing tender with new normalized data."""
    changed = False

    text_fields = ("title", "description", "organization", "country", "budget", "url", "status")
    for field_name in text_fields:
        old_value = getattr(existing, field_name)
        new_value = getattr(incoming, field_name)
        if _is_blank(old_value) and not _is_blank(new_value):
            setattr(existing, field_name, new_value)
            changed = True

    if existing.published_date is None and incoming.published_date is not None:
        existing.published_date = incoming.published_date
        changed = True
    if existing.closing_date is None and incoming.closing_date is not None:
        existing.closing_date = incoming.closing_date
        changed = True

    if incoming.raw_data and incoming.raw_data != existing.raw_data:
        existing.raw_data = incoming.raw_data
        changed = True

    return changed


def run_ingestion(db: Session) -> dict[str, Any]:
    """Run all connectors and persist deduplicated tenders."""
    connector_results: dict[str, dict[str, Any]] = {}
    all_records: list[dict[str, Any]] = []

    connector_calls = {
        "ted": lambda: fetch_ted_tenders(),
        "ec_ted": lambda: fetch_ec_ted_tenders(),
        "cppp": lambda: fetch_cppp_tenders(),
        "gem": lambda: fetch_gem_tenders(),
        "worldbank": lambda: fetch_world_bank_tenders(),
        "worldbank_projects": lambda: fetch_world_bank_projects_tenders(),
        "worldbank_rfx": lambda: fetch_world_bank_rfx_tenders(),
        "adb": lambda: fetch_adb_tenders(),
        "afdb": lambda: fetch_afdb_tenders(),
        "ungm": lambda: fetch_ungm_tenders(),
        "undp_quantum": lambda: fetch_undp_quantum_tenders(),
        "iom": lambda: fetch_iom_tenders(),
        "wfp": lambda: fetch_wfp_tenders(),
        "ilo": lambda: fetch_ilo_tenders(),
        "aiib": lambda: fetch_aiib_tenders(),
        "ebrd": lambda: fetch_ebrd_tenders(),
        "afd": lambda: fetch_afd_tenders(),
    }

    for connector_name in connector_calls:
        connector_results[connector_name] = {"ok": False, "fetched": 0, "error": None}

    for connector_name, connector_call in connector_calls.items():
        try:
            records = connector_call()
            connector_results[connector_name]["ok"] = True
            connector_results[connector_name]["fetched"] = len(records)
            all_records.extend(records)
            logger.info(
                "Connector ingestion succeeded",
                extra={"connector": connector_name, "fetched_count": len(records)},
            )
        except Exception as exc:  # pragma: no cover - defensive pipeline isolation
            connector_results[connector_name]["error"] = str(exc)
            logger.exception(
                "Connector ingestion failed",
                extra={"connector": connector_name, "error": str(exc)},
            )

    deduplicated_records = deduplicate_tenders(all_records)
    inserted_count = 0
    skipped_existing = 0
    updated_existing = 0

    for record in deduplicated_records:
        model_obj = _build_tender_model(record)
        if model_obj is None:
            continue

        existing = db.query(Tender).filter(Tender.id == model_obj.id).first()
        if existing:
            if _update_existing_tender(existing=existing, incoming=model_obj):
                try:
                    db.commit()
                    updated_existing += 1
                except Exception:
                    db.rollback()
                    logger.exception(
                        "Failed to update existing tender",
                        extra={"tender_id": model_obj.id, "source": model_obj.source},
                    )
            else:
                skipped_existing += 1
            continue

        existing_by_source_url = (
            db.query(Tender)
            .filter(Tender.source == model_obj.source)
            .filter(Tender.url == model_obj.url)
            .first()
        )
        if existing_by_source_url:
            if _update_existing_tender(existing=existing_by_source_url, incoming=model_obj):
                try:
                    db.commit()
                    updated_existing += 1
                except Exception:
                    db.rollback()
                    logger.exception(
                        "Failed to update tender by source+url",
                        extra={"source": model_obj.source, "url": model_obj.url},
                    )
            else:
                skipped_existing += 1
            continue

        try:
            db.add(model_obj)
            db.commit()
            inserted_count += 1
        except IntegrityError:
            db.rollback()
            skipped_existing += 1
        except Exception:
            db.rollback()
            logger.exception("Failed to insert tender", extra={"tender_id": model_obj.id, "source": model_obj.source})

    run_timestamp = datetime.now(UTC)
    for source_name, run_data in connector_results.items():
        metrics_row = (
            db.query(
                func.count(Tender.id).label("total"),
                func.sum(case((func.trim(func.coalesce(Tender.organization, "")) == "", 1), else_=0)).label("miss_org"),
                func.sum(case((func.trim(func.coalesce(Tender.country, "")) == "", 1), else_=0)).label("miss_country"),
                func.sum(case((Tender.closing_date.is_(None), 1), else_=0)).label("miss_closing"),
                func.max(Tender.published_date).label("max_published"),
            )
            .filter(Tender.source == source_name)
            .one()
        )

        cursor_payload = {
            "max_published_date": metrics_row.max_published.isoformat() if metrics_row.max_published else None,
            "last_run_at": run_timestamp.isoformat(),
        }

        state = db.query(SourceIngestionState).filter(SourceIngestionState.source == source_name).first()
        if state is None:
            state = SourceIngestionState(source=source_name)
            db.add(state)

        fetched_count = int(run_data.get("fetched") or 0)
        state.last_run_at = run_timestamp
        state.last_status = "ok" if bool(run_data.get("ok")) else "error"
        state.last_error = run_data.get("error")
        state.last_fetched = fetched_count
        state.max_observed_fetched = max(int(state.max_observed_fetched or 0), fetched_count)
        state.last_total_in_db = int(metrics_row.total or 0)
        state.missing_organization = int(metrics_row.miss_org or 0)
        state.missing_country = int(metrics_row.miss_country or 0)
        state.missing_closing_date = int(metrics_row.miss_closing or 0)
        state.cursor_data = json.dumps(cursor_payload, ensure_ascii=True)

    try:
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("Failed to persist source ingestion state")

    return {
        "total_fetched": len(all_records),
        "total_after_deduplication": len(deduplicated_records),
        "inserted": inserted_count,
        "updated_existing": updated_existing,
        "skipped_existing": skipped_existing,
        "connectors": connector_results,
    }
