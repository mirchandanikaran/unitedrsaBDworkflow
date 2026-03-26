"""Application entrypoint with API and scheduled ingestion."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from api.routes import router as tender_router
from models.db import SessionLocal, init_db
from services.ingestion import run_ingestion
from utils.helpers import configure_logging


logger = logging.getLogger(__name__)
scheduler = BackgroundScheduler(timezone="UTC")


def _scheduled_ingestion_job() -> None:
    """Execute ingestion job from scheduler context."""
    db = SessionLocal()
    try:
        result = run_ingestion(db=db)
        logger.info("Scheduled ingestion completed", extra={"result": result})
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception("Scheduled ingestion failed", extra={"error": str(exc)})
    finally:
        db.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Initialize application resources at startup and cleanly stop at shutdown."""
    configure_logging(log_level=os.getenv("LOG_LEVEL", "INFO"))
    init_db()

    if not scheduler.running:
        scheduler.add_job(
            _scheduled_ingestion_job,
            trigger=CronTrigger(hour="*/6"),
            id="ingestion_every_6_hours",
            replace_existing=True,
        )
        scheduler.start()
        logger.info("Scheduler started with 6-hour ingestion cadence")

    yield

    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down")


app = FastAPI(title="Tender Intelligence & Tracking System", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "static")), name="static")
app.include_router(tender_router)
