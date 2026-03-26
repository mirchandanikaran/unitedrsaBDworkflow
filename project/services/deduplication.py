"""Deduplication helpers for normalized tender records."""

from __future__ import annotations

from typing import Any


def deduplicate_tenders(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Remove duplicate tenders using deterministic ID and source+url key."""
    unique_records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_source_url: set[tuple[str, str]] = set()

    for record in records:
        tender_id = str(record.get("id", "")).strip()
        source = str(record.get("source", "")).strip().lower()
        url = str(record.get("url", "")).strip()
        source_url_key = (source, url)

        if not tender_id or not source or not url:
            continue
        if tender_id in seen_ids or source_url_key in seen_source_url:
            continue

        seen_ids.add(tender_id)
        seen_source_url.add(source_url_key)
        unique_records.append(record)

    return unique_records
