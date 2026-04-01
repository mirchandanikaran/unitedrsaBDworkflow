"""ORM models for tenders and BD workflow automation."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from models.db import Base


class Tender(Base):
    """Unified tender record stored across all source systems."""

    __tablename__ = "tenders"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    organization: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    source: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    published_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closing_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(40), nullable=False, default="open", index=True)
    budget: Mapped[str | None] = mapped_column(String(120), nullable=True)
    url: Mapped[str] = mapped_column(String(1200), nullable=False)
    raw_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("source", "url", name="uq_source_url"),
        Index("ix_tenders_title_lower", func.lower(title), postgresql_using="btree"),
    )


class SourceIngestionState(Base):
    """Tracks per-source run health, coverage, and lightweight cursor data."""

    __tablename__ = "source_ingestion_state"

    source: Mapped[str] = mapped_column(String(80), primary_key=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_observed_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_total_in_db: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    missing_organization: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    missing_country: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    missing_closing_date: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cursor_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


# ---------------------------------------------------------------------------
# BD Workflow Models
# ---------------------------------------------------------------------------

class WorkflowDecision(Base):
    """Automated Go/No-Go screening, pipeline stage, and assignment per tender."""

    __tablename__ = "workflow_decisions"

    tender_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenders.id"), primary_key=True)
    stage: Mapped[str] = mapped_column(String(40), nullable=False, default="identified", index=True)
    decision: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    score: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    priority: Mapped[str] = mapped_column(String(20), nullable=False, default="normal", index=True)
    assigned_lead: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    matched_keywords: Mapped[str | None] = mapped_column(Text, nullable=True)
    rationale: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    screening_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    screened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class ScoringKeyword(Base):
    """User-managed keywords that tune workflow scoring behavior."""

    __tablename__ = "scoring_keywords"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    keyword: Mapped[str] = mapped_column(String(120), nullable=False, unique=True, index=True)
    weight: Mapped[int] = mapped_column(Integer, nullable=False, default=4)
    is_active: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)
    notes: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )


class Partner(Base):
    """Reusable partner/firm record for consortium building."""

    __tablename__ = "partners"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    partner_type: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    country: Mapped[str | None] = mapped_column(String(120), nullable=True)
    contact_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    contact_email: Mapped[str | None] = mapped_column(String(200), nullable=True)
    specializations: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)


class TenderPartner(Base):
    """Links a partner to a specific tender opportunity with consortium role."""

    __tablename__ = "tender_partners"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tender_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenders.id"), nullable=False, index=True)
    partner_id: Mapped[int] = mapped_column(Integer, ForeignKey("partners.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(40), nullable=False, default="local")
    mou_status: Mapped[str] = mapped_column(String(30), nullable=False, default="pending")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (UniqueConstraint("tender_id", "partner_id", name="uq_tender_partner"),)


class ProposalTracker(Base):
    """Tracks EOI, Technical, and Financial proposals per tender."""

    __tablename__ = "proposal_tracker"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tender_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenders.id"), nullable=False, index=True)
    proposal_type: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="draft", index=True)
    assigned_to: Mapped[str | None] = mapped_column(String(120), nullable=True)
    due_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    document_ref: Mapped[str | None] = mapped_column(String(500), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False,
    )

    __table_args__ = (UniqueConstraint("tender_id", "proposal_type", name="uq_tender_proposal_type"),)


class QualityReview(Base):
    """3-level quality control review per tender."""

    __tablename__ = "quality_reviews"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tender_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenders.id"), nullable=False, index=True)
    level: Mapped[int] = mapped_column(Integer, nullable=False)
    reviewer: Mapped[str | None] = mapped_column(String(120), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    comments: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (UniqueConstraint("tender_id", "level", name="uq_tender_review_level"),)


class StageChecklist(Base):
    """Per-tender, per-stage actionable checklist items."""

    __tablename__ = "stage_checklists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tender_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenders.id"), nullable=False, index=True)
    stage: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    item_text: Mapped[str] = mapped_column(String(500), nullable=False)
    completed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (UniqueConstraint("tender_id", "stage", "item_text", name="uq_checklist_item"),)


class TeamAssignment(Base):
    """BD team role assignments per tender opportunity."""

    __tablename__ = "team_assignments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tender_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenders.id"), nullable=False, index=True)
    role: Mapped[str] = mapped_column(String(60), nullable=False)
    person_name: Mapped[str] = mapped_column(String(120), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)

    __table_args__ = (UniqueConstraint("tender_id", "role", name="uq_tender_role"),)
