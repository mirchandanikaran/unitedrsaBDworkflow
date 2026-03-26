"""API routes for tender querying and ingestion control."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
import json

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from models.db import get_db
from models.schemas import SourceIngestionState, Tender
from services.ingestion import run_ingestion


router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))


def _apply_tender_filters(
    query: Any,
    *,
    country: str | None,
    keyword: str | None,
    status: str | None,
    source: str | None,
) -> Any:
    """Apply reusable tender filters to a SQLAlchemy query."""
    if country:
        query = query.filter(Tender.country == country.strip())
    if status:
        query = query.filter(Tender.status == status.strip().lower())
    if source:
        query = query.filter(Tender.source == source.strip().lower())
    if keyword:
        query = query.filter(Tender.title.ilike(f"%{keyword.strip()}%"))
    return query


def _serialize_tender(tender: Tender) -> dict[str, Any]:
    """Serialize ORM model to API response dictionary."""
    return {
        "id": tender.id,
        "title": tender.title,
        "description": tender.description or "",
        "organization": tender.organization or "",
        "country": tender.country or "",
        "source": tender.source,
        "published_date": tender.published_date.isoformat() if tender.published_date else None,
        "closing_date": tender.closing_date.isoformat() if tender.closing_date else None,
        "status": tender.status,
        "budget": tender.budget or "",
        "url": tender.url,
        "created_at": tender.created_at.isoformat() if tender.created_at else None,
    }


@router.get("/", response_class=HTMLResponse)
def ui_home(request: Request) -> HTMLResponse:
    """Serve minimal POC UI."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/how-to", response_class=HTMLResponse)
def ui_how_to(request: Request) -> HTMLResponse:
    """Serve dashboard usage instructions."""
    return templates.TemplateResponse("how_to.html", {"request": request})


@router.get("/tenders")
def list_tenders(
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """List tenders with optional filtering."""
    query = _apply_tender_filters(
        db.query(Tender),
        country=country,
        keyword=keyword,
        status=status,
        source=source,
    )

    tenders = query.order_by(Tender.created_at.desc()).offset(offset).limit(limit).all()
    return [_serialize_tender(tender) for tender in tenders]


@router.get("/tenders/count")
def count_tenders(
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, int]:
    """Return total tender count for current filters."""
    query = _apply_tender_filters(
        db.query(func.count(Tender.id)),
        country=country,
        keyword=keyword,
        status=status,
        source=source,
    )
    total = int(query.scalar() or 0)
    return {"total": total}


@router.get("/sources")
def list_sources(
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """Return source names with total counts."""
    query = _apply_tender_filters(
        db.query(Tender.source, func.count(Tender.id)),
        country=country,
        keyword=keyword,
        status=status,
        source=source,
    )
    rows = query.group_by(Tender.source).order_by(Tender.source.asc()).all()
    return [{"source": source, "count": count} for source, count in rows]


@router.get("/sources/health")
def list_source_health(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    """Return per-source health and completeness metrics."""
    rows = db.query(SourceIngestionState).order_by(SourceIngestionState.source.asc()).all()
    payload: list[dict[str, Any]] = []
    for row in rows:
        cursor = None
        if row.cursor_data:
            try:
                cursor = json.loads(row.cursor_data)
            except json.JSONDecodeError:
                cursor = {"raw": row.cursor_data}

        total = int(row.last_total_in_db or 0)
        miss_org = int(row.missing_organization or 0)
        miss_country = int(row.missing_country or 0)
        miss_closing = int(row.missing_closing_date or 0)
        payload.append(
            {
                "source": row.source,
                "last_status": row.last_status,
                "last_run_at": row.last_run_at.isoformat() if row.last_run_at else None,
                "last_error": row.last_error,
                "last_fetched": int(row.last_fetched or 0),
                "max_observed_fetched": int(row.max_observed_fetched or 0),
                "total_in_db": total,
                "missing_organization": miss_org,
                "missing_country": miss_country,
                "missing_closing_date": miss_closing,
                "missing_organization_pct": round((miss_org / total * 100), 2) if total else 0.0,
                "missing_country_pct": round((miss_country / total * 100), 2) if total else 0.0,
                "missing_closing_date_pct": round((miss_closing / total * 100), 2) if total else 0.0,
                "cursor": cursor,
            }
        )
    return payload


@router.get("/tenders/closing-soon")
def list_closing_soon_tenders(
    days: int = Query(default=7, ge=1, le=30),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    """List open tenders closing within the next N days."""
    now_utc = datetime.now(UTC)
    end_window = now_utc + timedelta(days=days)

    tenders = (
        db.query(Tender)
        .filter(Tender.status == "open")
        .filter(Tender.closing_date.isnot(None))
        .filter(Tender.closing_date >= now_utc)
        .filter(Tender.closing_date <= end_window)
        .order_by(Tender.closing_date.asc())
        .all()
    )
    return [_serialize_tender(tender) for tender in tenders]


@router.post("/ingest")
def ingest_tenders(db: Session = Depends(get_db)) -> dict[str, Any]:
    """Trigger full ingestion across all configured connectors."""
    return run_ingestion(db=db)
