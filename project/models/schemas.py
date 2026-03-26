"""ORM models for tenders."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text, UniqueConstraint, func
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
