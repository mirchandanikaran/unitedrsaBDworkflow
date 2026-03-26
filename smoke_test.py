"""Basic API smoke test for Tender Intelligence service."""

from __future__ import annotations

import os
import sys

import requests


BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
TIMEOUT_SECONDS = int(os.getenv("SMOKE_TEST_TIMEOUT_SECONDS", "60"))


def _check(response: requests.Response, name: str) -> None:
    """Validate a response and exit with details on failure."""
    if response.status_code >= 400:
        print(f"[FAIL] {name}: status={response.status_code} body={response.text}")
        sys.exit(1)
    print(f"[OK] {name}: status={response.status_code}")


def main() -> None:
    """Run smoke tests against core API endpoints."""
    ingest = requests.post(f"{BASE_URL}/ingest", timeout=TIMEOUT_SECONDS)
    _check(ingest, "POST /ingest")

    tenders = requests.get(f"{BASE_URL}/tenders", timeout=TIMEOUT_SECONDS)
    _check(tenders, "GET /tenders")

    closing = requests.get(f"{BASE_URL}/tenders/closing-soon", timeout=TIMEOUT_SECONDS)
    _check(closing, "GET /tenders/closing-soon")

    print("Smoke test completed successfully.")


if __name__ == "__main__":
    main()
