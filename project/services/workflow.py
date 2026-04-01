"""BD workflow automation: screening, scoring, pipeline, partners, proposals, QC, and SLAs."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from models.schemas import (
    Partner,
    ProposalTracker,
    QualityReview,
    ScoringKeyword,
    StageChecklist,
    TeamAssignment,
    Tender,
    TenderPartner,
    WorkflowDecision,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Full keyword list extracted from the BD Process document
# ---------------------------------------------------------------------------
DEFAULT_BD_KEYWORDS: tuple[str, ...] = (
    # General Consulting & Advisory
    "consultancy services", "consultancy", "consulting",
    "project management", "construction supervision", "project management support",
    "technical assistance", "advisory services", "feasibility study", "feasibility",
    "capacity building", "institutional strengthening", "policy advisory",
    "strategic planning", "public sector reform", "governance", "accountability",
    "environmental and social impact", "esia",
    "economic and financial analysis", "technical due diligence",
    "fiscal agent", "procurement agent",
    # Project Management & Monitoring
    "project design", "implementation", "monitoring & evaluation", "m&e",
    "monitoring agent", "monitoring", "evaluation",
    "independent evaluation", "country evaluation", "external evaluation",
    "impact assessment", "impact evaluation",
    "baseline survey", "baseline review", "baseline",
    "mid term review", "midterm review",
    "endline survey", "endline review", "endline",
    "performance monitoring", "data collection", "analysis",
    "results-based management",
    # Verification & Results-Based
    "independent verification agent", "independent verification agency",
    "third-party monitoring", "verification",
    "results-based verification", "results-based financing",
    "performance-based financing", "outcome-based financing",
    "disbursement-linked indicators", "key performance indicators",
    "results-based disbursement", "milestone-based payments", "payment for results",
    # Economic & Social Development
    "sustainable development", "livelihood programs", "livelihood",
    "rural development", "urban development",
    "women's empowerment", "social protection", "poverty alleviation",
    # Sector: Public Financial Management
    "public financial management", "pfm", "public policy reform",
    "decentralization", "local governance",
    # Sector: Health & Nutrition
    "public health consulting", "health systems strengthening", "health systems",
    "universal health coverage", "uhc",
    "maternal & child health", "disease prevention", "nutrition programs", "nutrition",
    # Sector: Education
    "education sector reform", "digital learning", "vocational training",
    "curriculum development", "teacher training", "skills development", "education",
    # Sector: WASH
    "water sanitation", "wash", "water resource management",
    "clean drinking water", "sanitation", "hygiene promotion",
    "community-led total sanitation", "clts", "wastewater treatment",
    # Sector: Climate & Environment
    "climate change", "climate resilience", "climate",
    "renewable energy", "environmental impact assessment",
    "sustainable agriculture", "disaster risk reduction", "drr",
    "carbon credit",
)

DEFAULT_FOCUS_COUNTRIES = (
    "india", "kenya", "ethiopia", "somalia", "nepal", "cambodia",
    "togo", "guinea", "sri lanka", "georgia", "thailand", "bihar",
    "afghanistan", "yemen", "mali", "republic francaise",
)

DEFAULT_BD_LEADS = ("Vishal Bhargav", "Siddharth")

WORKFLOW_STAGES = (
    "identified",
    "screened",
    "partner_development",
    "eoi_preparation",
    "technical_proposal",
    "financial_proposal",
    "quality_control",
    "submitted",
    "post_submission",
    "negotiation",
    "awarded",
    "handover",
    "closed",
)

STAGE_SLA_DAYS: dict[str, int] = {
    "identified": 1,
    "screened": 2,
    "partner_development": 6,
    "eoi_preparation": 10,
    "technical_proposal": 28,
    "financial_proposal": 28,
    "quality_control": 3,
    "submitted": 1,
    "post_submission": 60,
    "negotiation": 21,
    "awarded": 7,
    "handover": 7,
}

STAGE_CHECKLISTS: dict[str, list[str]] = {
    "identified": [
        "Capture opportunity in tender tracker.",
        "Assign BD lead and share internally.",
        "Collect source URL, closing date, and basic metadata.",
    ],
    "screened": [
        "Check eligibility requirements.",
        "Verify geographic presence.",
        "Assess relevant experience.",
        "Evaluate competition level.",
        "Estimate contract value.",
        "Assess strategic value.",
        "Schedule 15-minute Go/No-Go call.",
        "Finalize decision within 48 hours.",
    ],
    "partner_development": [
        "Identify local country firm partner.",
        "Identify international consulting firm partner.",
        "Identify technical specialists.",
        "Identify research organizations if required.",
        "Share tender documents with shortlisted partners.",
        "Agree on consortium roles (Lead / International / Local).",
        "Draft consortium MoU agreement.",
    ],
    "eoi_preparation": [
        "Select relevant firm experience records.",
        "Identify and shortlist key experts.",
        "Format and validate CVs.",
        "Prepare relevant project descriptions.",
        "Run compliance checklist.",
        "Write firm profile section.",
        "Assemble consortium information section.",
    ],
    "technical_proposal": [
        "Draft technical approach and methodology.",
        "Prepare work plan.",
        "Build team composition section.",
        "Create staffing schedule.",
        "Develop quality assurance framework.",
        "Assign: Methodology to Technical Lead.",
        "Assign: Staffing to Proposal Manager.",
        "Assign: Work plan to Project Specialist.",
        "Assign: Quality review to Senior Expert.",
    ],
    "financial_proposal": [
        "Build cost structure and assumptions.",
        "Define pricing strategy.",
        "Plan profit margin.",
        "Calculate taxes and reimbursable expenses.",
        "Internal financial review sign-off.",
    ],
    "quality_control": [
        "Level 1: Technical review by Technical Lead.",
        "Level 2: Compliance check by Proposal Manager.",
        "Level 3: Executive approval by CEO/Partner.",
    ],
    "submitted": [
        "Submit on portal and capture confirmation.",
        "Record submission version and timestamp.",
        "Set reminder for clarification window.",
    ],
    "post_submission": [
        "Respond to clarification requests.",
        "Maintain client communication log.",
        "Prepare negotiation strategy.",
        "Track evaluation stage updates weekly.",
    ],
    "negotiation": [
        "Align technical negotiation points.",
        "Align financial adjustments.",
        "Confirm staffing changes.",
        "Agree timeline revisions.",
        "Finalize contract conditions.",
    ],
    "awarded": [
        "Sign contract.",
        "Schedule project kickoff meeting.",
        "Prepare handover package.",
    ],
    "handover": [
        "Transfer proposal and work plan to delivery team.",
        "Transfer staffing schedule.",
        "Close BD ownership with handover note.",
    ],
    "closed": [
        "Archive outcome and lessons learned.",
        "Update win/loss analytics.",
    ],
}

BD_TEAM_ROLES = (
    "BD Director",
    "BD Manager",
    "Proposal Manager",
    "Technical Lead",
    "Finance Manager",
    "Research Analyst",
)

STAGE_DESCRIPTIONS: dict[str, str] = {
    "identified": "Opportunity captured from portal and queued for qualification.",
    "screened": "Go/Consortium/No-Go decision prepared with score and rationale.",
    "partner_development": "Consortium partners identified and role alignment started.",
    "eoi_preparation": "EOI package prepared with CVs, references, and compliance inputs.",
    "technical_proposal": "Technical methodology, staffing, and delivery design produced.",
    "financial_proposal": "Pricing model, assumptions, and financial response finalized.",
    "quality_control": "Three-level quality and compliance review gate executed.",
    "submitted": "Proposal submitted to portal and submission evidence captured.",
    "post_submission": "Clarifications and client communication managed post-submission.",
    "negotiation": "Commercial and technical negotiation items discussed and updated.",
    "awarded": "Award confirmed and contract package finalized for execution.",
    "handover": "Bid artifacts handed over from BD to delivery team.",
    "closed": "Opportunity archived with outcome and lessons learned.",
}


def _parse_csv_env(var_name: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(var_name, "").strip()
    if not raw:
        return fallback
    parts = [part.strip() for part in raw.split(",")]
    values = tuple(part for part in parts if part)
    return values or fallback


def _normalize_text(value: str | None) -> str:
    return (value or "").strip().lower()


def _match_keywords(text: str, keywords: list[tuple[str, int]]) -> list[tuple[str, int]]:
    seen: set[str] = set()
    weighted_matches: list[tuple[str, int]] = []
    for keyword, weight in keywords:
        k = keyword.strip().lower()
        if k and k in text and k not in seen:
            weighted_matches.append((keyword, max(1, int(weight))))
            seen.add(k)
    return weighted_matches


def _decision_from_score(score: int) -> str:
    if score >= 70:
        return "go"
    if score >= 40:
        return "consortium"
    return "no-go"


def _next_stage_for_decision(decision: str) -> str:
    if decision in ("go", "consortium"):
        return "partner_development"
    return "closed"


def _aware_closing(tender: Tender) -> datetime | None:
    cd = tender.closing_date
    if cd is None:
        return None
    if cd.tzinfo is None:
        return cd.replace(tzinfo=UTC)
    return cd.astimezone(UTC)


def _priority_for_tender(tender: Tender) -> str:
    cd = _aware_closing(tender)
    if cd is None:
        return "normal"
    remaining = cd - datetime.now(UTC)
    if remaining <= timedelta(days=3):
        return "urgent"
    if remaining <= timedelta(days=7):
        return "high"
    return "normal"


def _build_rationale(
    *,
    tender: Tender,
    score: int,
    matched_keywords: list[str],
    focus_country_hit: bool,
    decision: str,
) -> str:
    reasons: list[str] = [f"Score={score}/100"]
    if matched_keywords:
        reasons.append(f"Keywords({len(matched_keywords)}): {', '.join(matched_keywords[:6])}")
    if focus_country_hit:
        reasons.append("Focus geography")
    if tender.budget and tender.budget.strip():
        reasons.append(f"Budget: {tender.budget.strip()}")
    cd = _aware_closing(tender)
    if cd:
        reasons.append(f"Closes: {cd.strftime('%Y-%m-%d')}")
    reasons.append(f"Decision: {decision.upper()}")
    return " | ".join(reasons)


def _build_weighted_keywords(db: Session) -> list[tuple[str, int]]:
    weighted: dict[str, tuple[str, int]] = {k.lower(): (k, 4) for k in DEFAULT_BD_KEYWORDS}
    custom_rows = (
        db.query(ScoringKeyword)
        .filter(ScoringKeyword.is_active == 1)
        .order_by(ScoringKeyword.keyword.asc())
        .all()
    )
    for row in custom_rows:
        key = row.keyword.strip().lower()
        if key:
            weighted[key] = (row.keyword.strip(), max(1, int(row.weight or 4)))
    return list(weighted.values())


# ---------------------------------------------------------------------------
# Auto-generate checklists for a tender entering a new stage
# ---------------------------------------------------------------------------

def _ensure_checklists(db: Session, tender_id: str, stage: str) -> None:
    items = STAGE_CHECKLISTS.get(stage, [])
    for item_text in items:
        existing = (
            db.query(StageChecklist)
            .filter(
                StageChecklist.tender_id == tender_id,
                StageChecklist.stage == stage,
                StageChecklist.item_text == item_text,
            )
            .first()
        )
        if existing is None:
            db.add(StageChecklist(tender_id=tender_id, stage=stage, item_text=item_text))


def _ensure_quality_reviews(db: Session, tender_id: str) -> None:
    labels = {1: "Technical Lead", 2: "Proposal Manager", 3: "CEO/Partner"}
    for level, reviewer in labels.items():
        existing = (
            db.query(QualityReview)
            .filter(QualityReview.tender_id == tender_id, QualityReview.level == level)
            .first()
        )
        if existing is None:
            db.add(QualityReview(tender_id=tender_id, level=level, reviewer=reviewer))


def _ensure_proposal_records(db: Session, tender_id: str, stage: str) -> None:
    type_map = {
        "eoi_preparation": "eoi",
        "technical_proposal": "technical",
        "financial_proposal": "financial",
    }
    ptype = type_map.get(stage)
    if not ptype:
        return
    existing = (
        db.query(ProposalTracker)
        .filter(ProposalTracker.tender_id == tender_id, ProposalTracker.proposal_type == ptype)
        .first()
    )
    if existing is None:
        db.add(ProposalTracker(tender_id=tender_id, proposal_type=ptype, status="draft"))


# ---------------------------------------------------------------------------
# Screening
# ---------------------------------------------------------------------------

def run_automated_screening(db: Session, *, limit: int = 500, force: bool = False) -> dict[str, Any]:
    """Score open tenders and persist workflow decisions with checklists."""
    env_keywords = _parse_csv_env("BD_KEYWORDS", DEFAULT_BD_KEYWORDS)
    keywords = _build_weighted_keywords(db)
    if not keywords:
        keywords = [(k, 4) for k in env_keywords]
    focus_countries = tuple(c.lower() for c in _parse_csv_env("BD_FOCUS_COUNTRIES", DEFAULT_FOCUS_COUNTRIES))
    leads = _parse_csv_env("BD_LEADS", DEFAULT_BD_LEADS)

    open_tenders = (
        db.query(Tender)
        .filter(Tender.status == "open")
        .order_by(Tender.created_at.desc())
        .limit(limit)
        .all()
    )

    screened = 0
    created = 0
    updated = 0
    counts_by_decision: dict[str, int] = {"go": 0, "consortium": 0, "no-go": 0}
    now = datetime.now(UTC)

    for tender in open_tenders:
        existing = db.query(WorkflowDecision).filter(WorkflowDecision.tender_id == tender.id).first()
        if existing and not force:
            continue

        searchable = " ".join([
            _normalize_text(tender.title),
            _normalize_text(tender.description),
            _normalize_text(tender.organization),
            _normalize_text(tender.country),
        ])
        matched_pairs = _match_keywords(searchable, keywords)
        matched_keywords = [k for k, _w in matched_pairs]

        score = 0
        if matched_keywords:
            keyword_points = sum(w for _k, w in matched_pairs)
            score += 35 + min(35, keyword_points)

        focus_country_hit = _normalize_text(tender.country) in focus_countries
        if focus_country_hit:
            score += 15

        if tender.organization and tender.organization.strip():
            score += 5

        if tender.budget and tender.budget.strip():
            score += 5

        cd = _aware_closing(tender)
        if cd:
            if cd <= now + timedelta(days=3):
                score += 10
            elif cd <= now + timedelta(days=7):
                score += 5

        score = max(0, min(score, 100))
        decision = _decision_from_score(score)
        counts_by_decision[decision] += 1

        assigned = leads[abs(hash(tender.id)) % len(leads)] if leads else None
        stage = _next_stage_for_decision(decision)

        record = existing or WorkflowDecision(tender_id=tender.id)
        record.stage = stage
        record.decision = decision
        record.score = score
        record.priority = _priority_for_tender(tender)
        record.assigned_lead = assigned
        record.matched_keywords = ",".join(matched_keywords)
        record.rationale = _build_rationale(
            tender=tender,
            score=score,
            matched_keywords=matched_keywords,
            focus_country_hit=focus_country_hit,
            decision=decision,
        )
        record.next_action = (
            "Proceed independently and draft bid kickoff."
            if decision == "go"
            else "Identify consortium partners and share tender pack."
            if decision == "consortium"
            else "Archive opportunity unless strategic override."
        )
        record.screening_due_at = now + timedelta(hours=48)
        record.screened_at = now

        if existing is None:
            db.add(record)
            created += 1
        else:
            updated += 1
        screened += 1

        _ensure_checklists(db, tender.id, stage)

    db.commit()
    return {
        "open_tenders_checked": len(open_tenders),
        "screened": screened,
        "created": created,
        "updated": updated,
        "decision_counts": counts_by_decision,
    }


# ---------------------------------------------------------------------------
# Stage advancement
# ---------------------------------------------------------------------------

def advance_workflow_stage(
    db: Session,
    *,
    tender_id: str,
    stage: str,
    assigned_lead: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    normalized = stage.strip().lower()
    if normalized not in WORKFLOW_STAGES:
        raise ValueError(f"Unsupported stage '{stage}'.")

    record = db.query(WorkflowDecision).filter(WorkflowDecision.tender_id == tender_id).first()
    if record is None:
        raise ValueError(f"No workflow decision for tender '{tender_id}'. Run screening first.")

    record.stage = normalized
    if assigned_lead and assigned_lead.strip():
        record.assigned_lead = assigned_lead.strip()

    checklist = STAGE_CHECKLISTS.get(normalized, [])
    hint = f"Checklist: {' | '.join(checklist[:3])}" if checklist else ""
    note_text = (note or "").strip()
    if note_text and hint:
        record.next_action = f"{note_text}. {hint}"
    elif note_text:
        record.next_action = note_text
    elif hint:
        record.next_action = hint

    record.updated_at = datetime.now(UTC)

    _ensure_checklists(db, tender_id, normalized)

    if normalized == "quality_control":
        _ensure_quality_reviews(db, tender_id)
    if normalized in ("eoi_preparation", "technical_proposal", "financial_proposal"):
        _ensure_proposal_records(db, tender_id, normalized)

    db.commit()
    return {
        "tender_id": record.tender_id,
        "stage": record.stage,
        "decision": record.decision,
        "assigned_lead": record.assigned_lead,
        "next_action": record.next_action,
    }


# ---------------------------------------------------------------------------
# Pipeline summary with funnel model
# ---------------------------------------------------------------------------

def get_pipeline_summary(db: Session) -> dict[str, Any]:
    total = int(db.query(func.count(WorkflowDecision.tender_id)).scalar() or 0)

    stage_rows = (
        db.query(WorkflowDecision.stage, func.count(WorkflowDecision.tender_id))
        .group_by(WorkflowDecision.stage)
        .order_by(WorkflowDecision.stage.asc())
        .all()
    )
    decision_rows = (
        db.query(WorkflowDecision.decision, func.count(WorkflowDecision.tender_id))
        .group_by(WorkflowDecision.decision)
        .order_by(WorkflowDecision.decision.asc())
        .all()
    )

    stage_counts = {stage: int(count) for stage, count in stage_rows}

    eoi_plus = sum(
        stage_counts.get(s, 0)
        for s in ("eoi_preparation", "technical_proposal", "financial_proposal",
                   "quality_control", "submitted", "post_submission",
                   "negotiation", "awarded", "handover")
    )
    submitted = sum(
        stage_counts.get(s, 0)
        for s in ("submitted", "post_submission", "negotiation", "awarded", "handover")
    )
    won = sum(stage_counts.get(s, 0) for s in ("awarded", "handover"))

    win_rate = round((won / submitted) * 100, 2) if submitted else 0.0
    funnel = {
        "tracked": total,
        "eoi_submitted": eoi_plus,
        "proposals_submitted": submitted,
        "won": won,
        "win_rate_pct": win_rate,
    }

    overdue = int(
        db.query(func.count(WorkflowDecision.tender_id))
        .filter(
            WorkflowDecision.screening_due_at.isnot(None),
            WorkflowDecision.screening_due_at < datetime.now(UTC),
            WorkflowDecision.stage.notin_(("awarded", "handover", "closed")),
        )
        .scalar()
        or 0
    )

    priority_rows = (
        db.query(WorkflowDecision.priority, func.count(WorkflowDecision.tender_id))
        .group_by(WorkflowDecision.priority)
        .all()
    )

    return {
        "total_workflow_items": total,
        "by_stage": [{"stage": s, "count": int(c)} for s, c in stage_rows],
        "by_decision": [{"decision": d, "count": int(c)} for d, c in decision_rows],
        "by_priority": [{"priority": p, "count": int(c)} for p, c in priority_rows],
        "funnel": funnel,
        "overdue_items": overdue,
        "known_stages": list(WORKFLOW_STAGES),
        "stage_sla_days": STAGE_SLA_DAYS,
    }


# ---------------------------------------------------------------------------
# Keyword planner
# ---------------------------------------------------------------------------

def list_scoring_keywords(db: Session) -> dict[str, Any]:
    custom = db.query(ScoringKeyword).order_by(ScoringKeyword.keyword.asc()).all()
    custom_payload = [{
        "id": row.id,
        "keyword": row.keyword,
        "weight": int(row.weight or 4),
        "is_active": bool(row.is_active),
        "notes": row.notes or "",
    } for row in custom]
    effective = _build_weighted_keywords(db)
    return {
        "defaults_count": len(DEFAULT_BD_KEYWORDS),
        "custom_count": len(custom_payload),
        "active_custom_count": sum(1 for row in custom_payload if row["is_active"]),
        "effective_keywords_count": len(effective),
        "default_keywords": [{"keyword": k, "weight": 4, "source": "default"} for k in DEFAULT_BD_KEYWORDS],
        "custom_keywords": custom_payload,
    }


def create_scoring_keyword(
    db: Session,
    *,
    keyword: str,
    weight: int = 4,
    notes: str | None = None,
    is_active: bool = True,
) -> dict[str, Any]:
    normalized = keyword.strip()
    if not normalized:
        raise ValueError("Keyword cannot be blank.")

    existing = (
        db.query(ScoringKeyword)
        .filter(func.lower(ScoringKeyword.keyword) == normalized.lower())
        .first()
    )
    if existing:
        existing.weight = max(1, min(int(weight), 20))
        existing.is_active = 1 if is_active else 0
        existing.notes = (notes or "").strip() or None
        db.commit()
        db.refresh(existing)
        return {
            "id": existing.id,
            "keyword": existing.keyword,
            "weight": int(existing.weight),
            "is_active": bool(existing.is_active),
            "notes": existing.notes or "",
        }

    row = ScoringKeyword(
        keyword=normalized,
        weight=max(1, min(int(weight), 20)),
        is_active=1 if is_active else 0,
        notes=(notes or "").strip() or None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {
        "id": row.id,
        "keyword": row.keyword,
        "weight": int(row.weight),
        "is_active": bool(row.is_active),
        "notes": row.notes or "",
    }


def update_scoring_keyword(
    db: Session,
    *,
    keyword_id: int,
    weight: int | None = None,
    is_active: bool | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    row = db.query(ScoringKeyword).filter(ScoringKeyword.id == keyword_id).first()
    if row is None:
        raise ValueError(f"Scoring keyword {keyword_id} not found.")

    if weight is not None:
        row.weight = max(1, min(int(weight), 20))
    if is_active is not None:
        row.is_active = 1 if is_active else 0
    if notes is not None:
        row.notes = notes.strip() or None
    db.commit()
    db.refresh(row)
    return {
        "id": row.id,
        "keyword": row.keyword,
        "weight": int(row.weight),
        "is_active": bool(row.is_active),
        "notes": row.notes or "",
    }


def delete_scoring_keyword(db: Session, *, keyword_id: int) -> dict[str, Any]:
    row = db.query(ScoringKeyword).filter(ScoringKeyword.id == keyword_id).first()
    if row is None:
        raise ValueError(f"Scoring keyword {keyword_id} not found.")
    db.delete(row)
    db.commit()
    return {"deleted": True, "id": keyword_id}


# ---------------------------------------------------------------------------
# Lifecycle explanation
# ---------------------------------------------------------------------------

def get_lifecycle_catalog() -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for idx, stage in enumerate(WORKFLOW_STAGES):
        catalog.append({
            "order": idx + 1,
            "stage": stage,
            "label": stage.replace("_", " ").title(),
            "description": STAGE_DESCRIPTIONS.get(stage, ""),
            "sla_days": STAGE_SLA_DAYS.get(stage),
        })
    return catalog


def get_tender_lifecycle(db: Session, *, tender_id: str) -> dict[str, Any]:
    wf = db.query(WorkflowDecision).filter(WorkflowDecision.tender_id == tender_id).first()
    if wf is None:
        raise ValueError(f"No workflow decision for tender '{tender_id}'. Run screening first.")

    stages = get_lifecycle_catalog()
    current_idx = WORKFLOW_STAGES.index(wf.stage) if wf.stage in WORKFLOW_STAGES else -1
    for stage in stages:
        stage_idx = int(stage["order"]) - 1
        if stage_idx < current_idx:
            stage["state"] = "completed"
        elif stage_idx == current_idx:
            stage["state"] = "current"
        else:
            stage["state"] = "upcoming"

    next_stage = WORKFLOW_STAGES[current_idx + 1] if 0 <= current_idx < len(WORKFLOW_STAGES) - 1 else None
    return {
        "tender_id": tender_id,
        "current_stage": wf.stage,
        "current_stage_label": wf.stage.replace("_", " ").title(),
        "current_stage_description": STAGE_DESCRIPTIONS.get(wf.stage, ""),
        "next_stage": next_stage,
        "next_stage_label": next_stage.replace("_", " ").title() if next_stage else None,
        "next_stage_description": STAGE_DESCRIPTIONS.get(next_stage, "") if next_stage else "",
        "lifecycle": stages,
    }


# ---------------------------------------------------------------------------
# Workflow item listing
# ---------------------------------------------------------------------------

def list_workflow_items(
    db: Session,
    *,
    stage: str | None = None,
    decision: str | None = None,
    assigned_lead: str | None = None,
    priority: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    query = db.query(WorkflowDecision, Tender).join(Tender, Tender.id == WorkflowDecision.tender_id)
    if stage:
        query = query.filter(WorkflowDecision.stage == stage.strip().lower())
    if decision:
        query = query.filter(WorkflowDecision.decision == decision.strip().lower())
    if assigned_lead:
        query = query.filter(WorkflowDecision.assigned_lead == assigned_lead.strip())
    if priority:
        query = query.filter(WorkflowDecision.priority == priority.strip().lower())

    rows = query.order_by(WorkflowDecision.updated_at.desc()).offset(offset).limit(limit).all()
    payload: list[dict[str, Any]] = []
    for wf, tender in rows:
        payload.append({
            "tender_id": tender.id,
            "title": tender.title,
            "source": tender.source,
            "country": tender.country or "",
            "organization": tender.organization or "",
            "budget": tender.budget or "",
            "closing_date": tender.closing_date.isoformat() if tender.closing_date else None,
            "url": tender.url,
            "stage": wf.stage,
            "decision": wf.decision,
            "score": int(wf.score),
            "priority": wf.priority,
            "assigned_lead": wf.assigned_lead or "",
            "matched_keywords": wf.matched_keywords.split(",") if wf.matched_keywords else [],
            "rationale": wf.rationale or "",
            "next_action": wf.next_action or "",
            "screening_due_at": wf.screening_due_at.isoformat() if wf.screening_due_at else None,
            "updated_at": wf.updated_at.isoformat() if wf.updated_at else None,
        })
    return payload


# ---------------------------------------------------------------------------
# SLA / deadline alerts
# ---------------------------------------------------------------------------

def get_overdue_items(db: Session, *, limit: int = 100) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    rows = (
        db.query(WorkflowDecision, Tender)
        .join(Tender, Tender.id == WorkflowDecision.tender_id)
        .filter(
            WorkflowDecision.screening_due_at.isnot(None),
            WorkflowDecision.screening_due_at < now,
            WorkflowDecision.stage.notin_(("awarded", "handover", "closed")),
        )
        .order_by(WorkflowDecision.screening_due_at.asc())
        .limit(limit)
        .all()
    )
    payload: list[dict[str, Any]] = []
    for wf, tender in rows:
        hours_overdue = round((now - wf.screening_due_at).total_seconds() / 3600, 1)
        payload.append({
            "tender_id": tender.id,
            "title": tender.title,
            "stage": wf.stage,
            "priority": wf.priority,
            "assigned_lead": wf.assigned_lead or "",
            "screening_due_at": wf.screening_due_at.isoformat(),
            "hours_overdue": hours_overdue,
        })
    return payload


# ---------------------------------------------------------------------------
# Partner / Consortium CRUD
# ---------------------------------------------------------------------------

def create_partner(db: Session, *, name: str, partner_type: str, country: str | None = None,
                   contact_name: str | None = None, contact_email: str | None = None,
                   specializations: str | None = None, notes: str | None = None) -> dict[str, Any]:
    partner = Partner(
        name=name.strip(),
        partner_type=partner_type.strip(),
        country=(country or "").strip() or None,
        contact_name=(contact_name or "").strip() or None,
        contact_email=(contact_email or "").strip() or None,
        specializations=(specializations or "").strip() or None,
        notes=(notes or "").strip() or None,
    )
    db.add(partner)
    db.commit()
    db.refresh(partner)
    return _serialize_partner(partner)


def list_partners(db: Session, *, partner_type: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = db.query(Partner)
    if partner_type:
        query = query.filter(Partner.partner_type == partner_type.strip())
    return [_serialize_partner(p) for p in query.order_by(Partner.name.asc()).limit(limit).all()]


def _serialize_partner(p: Partner) -> dict[str, Any]:
    return {
        "id": p.id, "name": p.name, "partner_type": p.partner_type,
        "country": p.country or "", "contact_name": p.contact_name or "",
        "contact_email": p.contact_email or "", "specializations": p.specializations or "",
        "notes": p.notes or "",
    }


def assign_partner_to_tender(db: Session, *, tender_id: str, partner_id: int,
                              role: str = "local", notes: str | None = None) -> dict[str, Any]:
    tp = TenderPartner(
        tender_id=tender_id, partner_id=partner_id,
        role=role.strip(), notes=(notes or "").strip() or None,
    )
    db.add(tp)
    db.commit()
    db.refresh(tp)
    return {"id": tp.id, "tender_id": tp.tender_id, "partner_id": tp.partner_id,
            "role": tp.role, "mou_status": tp.mou_status, "notes": tp.notes or ""}


def list_tender_partners(db: Session, *, tender_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(TenderPartner, Partner)
        .join(Partner, Partner.id == TenderPartner.partner_id)
        .filter(TenderPartner.tender_id == tender_id)
        .order_by(TenderPartner.role.asc())
        .all()
    )
    return [
        {"id": tp.id, "tender_id": tp.tender_id, "partner_id": tp.partner_id,
         "role": tp.role, "mou_status": tp.mou_status, "notes": tp.notes or "",
         "partner_name": p.name, "partner_type": p.partner_type, "partner_country": p.country or ""}
        for tp, p in rows
    ]


def update_mou_status(db: Session, *, tender_partner_id: int, mou_status: str) -> dict[str, Any]:
    tp = db.query(TenderPartner).filter(TenderPartner.id == tender_partner_id).first()
    if tp is None:
        raise ValueError(f"TenderPartner {tender_partner_id} not found.")
    tp.mou_status = mou_status.strip()
    db.commit()
    return {"id": tp.id, "mou_status": tp.mou_status}


# ---------------------------------------------------------------------------
# Proposal tracker CRUD
# ---------------------------------------------------------------------------

def upsert_proposal(db: Session, *, tender_id: str, proposal_type: str,
                    status: str | None = None, assigned_to: str | None = None,
                    due_date: str | None = None, document_ref: str | None = None,
                    notes: str | None = None) -> dict[str, Any]:
    ptype = proposal_type.strip().lower()
    record = (
        db.query(ProposalTracker)
        .filter(ProposalTracker.tender_id == tender_id, ProposalTracker.proposal_type == ptype)
        .first()
    )
    if record is None:
        record = ProposalTracker(tender_id=tender_id, proposal_type=ptype)
        db.add(record)

    if status:
        record.status = status.strip()
        if status.strip().lower() == "submitted" and record.submitted_at is None:
            record.submitted_at = datetime.now(UTC)
    if assigned_to is not None:
        record.assigned_to = assigned_to.strip() or None
    if due_date:
        try:
            record.due_date = datetime.fromisoformat(due_date.strip())
        except ValueError:
            pass
    if document_ref is not None:
        record.document_ref = document_ref.strip() or None
    if notes is not None:
        record.notes = notes.strip() or None

    db.commit()
    db.refresh(record)
    return _serialize_proposal(record)


def list_proposals(db: Session, *, tender_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(ProposalTracker)
        .filter(ProposalTracker.tender_id == tender_id)
        .order_by(ProposalTracker.proposal_type.asc())
        .all()
    )
    return [_serialize_proposal(p) for p in rows]


def _serialize_proposal(p: ProposalTracker) -> dict[str, Any]:
    return {
        "id": p.id, "tender_id": p.tender_id, "proposal_type": p.proposal_type,
        "status": p.status, "assigned_to": p.assigned_to or "",
        "due_date": p.due_date.isoformat() if p.due_date else None,
        "submitted_at": p.submitted_at.isoformat() if p.submitted_at else None,
        "document_ref": p.document_ref or "", "notes": p.notes or "",
    }


# ---------------------------------------------------------------------------
# Quality Review
# ---------------------------------------------------------------------------

def update_quality_review(db: Session, *, tender_id: str, level: int,
                          status: str, comments: str | None = None) -> dict[str, Any]:
    row = (
        db.query(QualityReview)
        .filter(QualityReview.tender_id == tender_id, QualityReview.level == level)
        .first()
    )
    if row is None:
        raise ValueError(f"QualityReview level {level} not found for tender '{tender_id}'.")
    row.status = status.strip()
    if comments is not None:
        row.comments = comments.strip() or None
    if status.strip().lower() in ("approved", "rejected"):
        row.reviewed_at = datetime.now(UTC)
    db.commit()
    return _serialize_review(row)


def list_quality_reviews(db: Session, *, tender_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(QualityReview)
        .filter(QualityReview.tender_id == tender_id)
        .order_by(QualityReview.level.asc())
        .all()
    )
    return [_serialize_review(r) for r in rows]


def _serialize_review(r: QualityReview) -> dict[str, Any]:
    return {
        "id": r.id, "tender_id": r.tender_id, "level": r.level,
        "reviewer": r.reviewer or "", "status": r.status,
        "comments": r.comments or "",
        "reviewed_at": r.reviewed_at.isoformat() if r.reviewed_at else None,
    }


# ---------------------------------------------------------------------------
# Checklists
# ---------------------------------------------------------------------------

def list_checklists(db: Session, *, tender_id: str, stage: str | None = None) -> list[dict[str, Any]]:
    query = db.query(StageChecklist).filter(StageChecklist.tender_id == tender_id)
    if stage:
        query = query.filter(StageChecklist.stage == stage.strip().lower())
    rows = query.order_by(StageChecklist.stage.asc(), StageChecklist.id.asc()).all()
    return [_serialize_checklist(c) for c in rows]


def toggle_checklist_item(db: Session, *, item_id: int, completed_by: str | None = None) -> dict[str, Any]:
    item = db.query(StageChecklist).filter(StageChecklist.id == item_id).first()
    if item is None:
        raise ValueError(f"Checklist item {item_id} not found.")
    if item.completed:
        item.completed = 0
        item.completed_by = None
        item.completed_at = None
    else:
        item.completed = 1
        item.completed_by = (completed_by or "").strip() or None
        item.completed_at = datetime.now(UTC)
    db.commit()
    return _serialize_checklist(item)


def _serialize_checklist(c: StageChecklist) -> dict[str, Any]:
    return {
        "id": c.id, "tender_id": c.tender_id, "stage": c.stage,
        "item_text": c.item_text, "completed": bool(c.completed),
        "completed_by": c.completed_by or "",
        "completed_at": c.completed_at.isoformat() if c.completed_at else None,
    }


# ---------------------------------------------------------------------------
# Team assignments
# ---------------------------------------------------------------------------

def assign_team_member(db: Session, *, tender_id: str, role: str, person_name: str,
                       notes: str | None = None) -> dict[str, Any]:
    existing = (
        db.query(TeamAssignment)
        .filter(TeamAssignment.tender_id == tender_id, TeamAssignment.role == role.strip())
        .first()
    )
    if existing:
        existing.person_name = person_name.strip()
        if notes is not None:
            existing.notes = notes.strip() or None
        db.commit()
        return _serialize_assignment(existing)

    assignment = TeamAssignment(
        tender_id=tender_id, role=role.strip(),
        person_name=person_name.strip(), notes=(notes or "").strip() or None,
    )
    db.add(assignment)
    db.commit()
    db.refresh(assignment)
    return _serialize_assignment(assignment)


def list_team_assignments(db: Session, *, tender_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(TeamAssignment)
        .filter(TeamAssignment.tender_id == tender_id)
        .order_by(TeamAssignment.role.asc())
        .all()
    )
    return [_serialize_assignment(a) for a in rows]


def _serialize_assignment(a: TeamAssignment) -> dict[str, Any]:
    return {
        "id": a.id, "tender_id": a.tender_id, "role": a.role,
        "person_name": a.person_name, "notes": a.notes or "",
    }
