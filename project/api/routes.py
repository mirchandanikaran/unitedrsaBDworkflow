"""API routes for tender querying, ingestion, and BD workflow management."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from models.db import get_db
from models.schemas import SourceIngestionState, Tender
from services.ingestion import STANDARD_BENCHMARKS, run_ingestion
from services.workflow import (
    advance_workflow_stage,
    assign_partner_to_tender,
    assign_team_member,
    create_scoring_keyword,
    create_partner,
    delete_scoring_keyword,
    get_overdue_items,
    get_pipeline_summary,
    get_lifecycle_catalog,
    get_tender_lifecycle,
    list_checklists,
    list_partners,
    list_proposals,
    list_quality_reviews,
    list_scoring_keywords,
    list_team_assignments,
    list_tender_partners,
    list_workflow_items,
    run_automated_screening,
    toggle_checklist_item,
    update_scoring_keyword,
    update_mou_status,
    update_quality_review,
    upsert_proposal,
    BD_TEAM_ROLES,
    WORKFLOW_STAGES,
)


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


# ---------------------------------------------------------------------------
# UI pages
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
def ui_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/how-to", response_class=HTMLResponse)
def ui_how_to(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("how_to.html", {"request": request})


@router.get("/bd", response_class=HTMLResponse)
def ui_bd_dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("bd_dashboard.html", {"request": request})


@router.get("/keyword-planner", response_class=HTMLResponse)
def ui_keyword_planner(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("keyword_planner.html", {"request": request})


# ---------------------------------------------------------------------------
# Tender CRUD
# ---------------------------------------------------------------------------

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
    query = _apply_tender_filters(
        db.query(Tender), country=country, keyword=keyword, status=status, source=source,
    )
    tenders = query.order_by(Tender.created_at.desc()).offset(offset).limit(limit).all()
    return [_serialize_tender(t) for t in tenders]


@router.get("/tenders/count")
def count_tenders(
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, int]:
    query = _apply_tender_filters(
        db.query(func.count(Tender.id)), country=country, keyword=keyword, status=status, source=source,
    )
    return {"total": int(query.scalar() or 0)}


@router.get("/sources")
def list_sources(
    country: str | None = Query(default=None),
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    query = _apply_tender_filters(
        db.query(Tender.source, func.count(Tender.id)),
        country=country, keyword=keyword, status=status, source=source,
    )
    rows = query.group_by(Tender.source).order_by(Tender.source.asc()).all()
    return [{"source": s, "count": c} for s, c in rows]


@router.get("/sources/health")
def list_source_health(db: Session = Depends(get_db)) -> list[dict[str, Any]]:
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
        mo = int(row.missing_organization or 0)
        mc = int(row.missing_country or 0)
        md = int(row.missing_closing_date or 0)
        payload.append({
            "source": row.source,
            "last_status": row.last_status,
            "last_run_at": row.last_run_at.isoformat() if row.last_run_at else None,
            "last_error": row.last_error,
            "last_fetched": int(row.last_fetched or 0),
            "max_observed_fetched": int(row.max_observed_fetched or 0),
            "total_in_db": total,
            "missing_organization": mo, "missing_country": mc, "missing_closing_date": md,
            "missing_organization_pct": round(mo / total * 100, 2) if total else 0.0,
            "missing_country_pct": round(mc / total * 100, 2) if total else 0.0,
            "missing_closing_date_pct": round(md / total * 100, 2) if total else 0.0,
            "benchmarks": STANDARD_BENCHMARKS,
            "benchmark_pass": {
                "missing_organization_pct": (round(mo / total * 100, 2) if total else 0.0)
                <= STANDARD_BENCHMARKS["max_missing_organization_pct"],
                "missing_country_pct": (round(mc / total * 100, 2) if total else 0.0)
                <= STANDARD_BENCHMARKS["max_missing_country_pct"],
                "missing_closing_date_pct": (round(md / total * 100, 2) if total else 0.0)
                <= STANDARD_BENCHMARKS["max_missing_closing_date_pct"],
            },
            "cursor": cursor,
        })
    return payload


@router.get("/tenders/closing-soon")
def list_closing_soon_tenders(
    days: int = Query(default=7, ge=1, le=30),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    now_utc = datetime.now(UTC)
    end = now_utc + timedelta(days=days)
    tenders = (
        db.query(Tender)
        .filter(Tender.status == "open", Tender.closing_date.isnot(None),
                Tender.closing_date >= now_utc, Tender.closing_date <= end)
        .order_by(Tender.closing_date.asc())
        .all()
    )
    return [_serialize_tender(t) for t in tenders]


@router.post("/ingest")
def ingest_tenders(db: Session = Depends(get_db)) -> dict[str, Any]:
    return run_ingestion(db=db)


@router.post("/ingest-all")
def ingest_all_sources(db: Session = Depends(get_db)) -> dict[str, Any]:
    return run_ingestion(db=db)


# ---------------------------------------------------------------------------
# Workflow: screening, pipeline, stage advancement
# ---------------------------------------------------------------------------

@router.post("/workflow/run")
def run_workflow_screening(
    limit: int = Query(default=500, ge=1, le=5000),
    force: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    return run_automated_screening(db=db, limit=limit, force=force)


@router.get("/workflow/items")
def api_workflow_items(
    stage: str | None = Query(default=None),
    decision: str | None = Query(default=None),
    assigned_lead: str | None = Query(default=None),
    priority: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    return list_workflow_items(
        db=db, stage=stage, decision=decision, assigned_lead=assigned_lead,
        priority=priority, limit=limit, offset=offset,
    )


@router.get("/workflow/summary")
def api_workflow_summary(db: Session = Depends(get_db)) -> dict[str, Any]:
    return get_pipeline_summary(db=db)


@router.post("/workflow/advance/{tender_id}")
def api_workflow_advance(
    tender_id: str,
    stage: str = Query(...),
    assigned_lead: str | None = Query(default=None),
    note: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return advance_workflow_stage(
            db=db, tender_id=tender_id, stage=stage,
            assigned_lead=assigned_lead, note=note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/workflow/overdue")
def api_overdue(limit: int = Query(default=100, ge=1), db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return get_overdue_items(db=db, limit=limit)


@router.get("/workflow/stages")
def api_stages() -> dict[str, Any]:
    return {"stages": list(WORKFLOW_STAGES), "team_roles": list(BD_TEAM_ROLES)}


@router.get("/workflow/lifecycle")
def api_workflow_lifecycle_catalog() -> dict[str, Any]:
    return {"stages": get_lifecycle_catalog()}


@router.get("/workflow/lifecycle/{tender_id}")
def api_tender_lifecycle(tender_id: str, db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        return get_tender_lifecycle(db=db, tender_id=tender_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/workflow/keywords")
def api_list_workflow_keywords(db: Session = Depends(get_db)) -> dict[str, Any]:
    return list_scoring_keywords(db=db)


@router.post("/workflow/keywords")
def api_create_workflow_keyword(
    keyword: str = Query(...),
    weight: int = Query(default=4, ge=1, le=20),
    is_active: bool = Query(default=True),
    notes: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return create_scoring_keyword(
            db=db, keyword=keyword, weight=weight, is_active=is_active, notes=notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/workflow/keywords/{keyword_id}")
def api_update_workflow_keyword(
    keyword_id: int,
    weight: int | None = Query(default=None, ge=1, le=20),
    is_active: bool | None = Query(default=None),
    notes: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return update_scoring_keyword(
            db=db, keyword_id=keyword_id, weight=weight, is_active=is_active, notes=notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.delete("/workflow/keywords/{keyword_id}")
def api_delete_workflow_keyword(keyword_id: int, db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        return delete_scoring_keyword(db=db, keyword_id=keyword_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Partners / Consortium
# ---------------------------------------------------------------------------

@router.post("/partners")
def api_create_partner(
    name: str = Query(...), partner_type: str = Query(...),
    country: str | None = Query(default=None),
    contact_name: str | None = Query(default=None),
    contact_email: str | None = Query(default=None),
    specializations: str | None = Query(default=None),
    notes: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return create_partner(
            db=db, name=name, partner_type=partner_type, country=country,
            contact_name=contact_name, contact_email=contact_email,
            specializations=specializations, notes=notes,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/partners")
def api_list_partners(
    partner_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    return list_partners(db=db, partner_type=partner_type, limit=limit)


@router.post("/tenders/{tender_id}/partners")
def api_assign_partner(
    tender_id: str,
    partner_id: int = Query(...),
    role: str = Query(default="local"),
    notes: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return assign_partner_to_tender(db=db, tender_id=tender_id, partner_id=partner_id, role=role, notes=notes)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tenders/{tender_id}/partners")
def api_tender_partners(tender_id: str, db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return list_tender_partners(db=db, tender_id=tender_id)


@router.patch("/tender-partners/{tp_id}/mou")
def api_update_mou(tp_id: int, mou_status: str = Query(...), db: Session = Depends(get_db)) -> dict[str, Any]:
    try:
        return update_mou_status(db=db, tender_partner_id=tp_id, mou_status=mou_status)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Proposals
# ---------------------------------------------------------------------------

@router.post("/tenders/{tender_id}/proposals")
def api_upsert_proposal(
    tender_id: str,
    proposal_type: str = Query(...),
    status: str | None = Query(default=None),
    assigned_to: str | None = Query(default=None),
    due_date: str | None = Query(default=None),
    document_ref: str | None = Query(default=None),
    notes: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    return upsert_proposal(
        db=db, tender_id=tender_id, proposal_type=proposal_type,
        status=status, assigned_to=assigned_to, due_date=due_date,
        document_ref=document_ref, notes=notes,
    )


@router.get("/tenders/{tender_id}/proposals")
def api_list_proposals(tender_id: str, db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return list_proposals(db=db, tender_id=tender_id)


# ---------------------------------------------------------------------------
# Quality Reviews
# ---------------------------------------------------------------------------

@router.get("/tenders/{tender_id}/reviews")
def api_list_reviews(tender_id: str, db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return list_quality_reviews(db=db, tender_id=tender_id)


@router.post("/tenders/{tender_id}/reviews/{level}")
def api_update_review(
    tender_id: str, level: int,
    status: str = Query(...),
    comments: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return update_quality_review(db=db, tender_id=tender_id, level=level, status=status, comments=comments)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Checklists
# ---------------------------------------------------------------------------

@router.get("/tenders/{tender_id}/checklists")
def api_checklists(
    tender_id: str,
    stage: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[dict[str, Any]]:
    return list_checklists(db=db, tender_id=tender_id, stage=stage)


@router.post("/checklists/{item_id}/toggle")
def api_toggle_checklist(
    item_id: int,
    completed_by: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    try:
        return toggle_checklist_item(db=db, item_id=item_id, completed_by=completed_by)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Team Assignments
# ---------------------------------------------------------------------------

@router.post("/tenders/{tender_id}/team")
def api_assign_team(
    tender_id: str,
    role: str = Query(...),
    person_name: str = Query(...),
    notes: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    return assign_team_member(db=db, tender_id=tender_id, role=role, person_name=person_name, notes=notes)


@router.get("/tenders/{tender_id}/team")
def api_list_team(tender_id: str, db: Session = Depends(get_db)) -> list[dict[str, Any]]:
    return list_team_assignments(db=db, tender_id=tender_id)
