# Tender Intelligence & Tracking System

## Run locally

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Copy env template:
   - `copy .env.example .env` (Windows PowerShell: `Copy-Item .env.example .env`)
4. Start API server from the `project` directory:
   - `uvicorn main:app --reload`
5. Open the POC UI:
   - `http://127.0.0.1:8000/`

## Key endpoints

- `GET /tenders`
- `GET /tenders?country=&keyword=&status=`
- `GET /tenders/closing-soon`
- `POST /ingest`

## Smoke test

With the API running, execute:

- `python smoke_test.py`

Optional env overrides:

- `BASE_URL` (default `http://127.0.0.1:8000`)
- `SMOKE_TEST_TIMEOUT_SECONDS` (default `60`)
