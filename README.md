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
6. Open the BD Workflow Dashboard:
   - `http://127.0.0.1:8000/bd`
7. Open the Keyword Planner:
   - `http://127.0.0.1:8000/keyword-planner`

## Key endpoints

### Tenders

- `GET /tenders`
- `GET /tenders?country=&keyword=&status=`
- `GET /tenders/closing-soon`
- `POST /ingest`
- `POST /ingest-all`
- `GET /sources/health` (now includes per-source benchmark pass/fail flags)

### BD Workflow

- `POST /workflow/run?limit=500&force=false` - Run automated screening
- `GET /workflow/items?stage=&decision=&assigned_lead=&priority=` - List pipeline items
- `GET /workflow/summary` - Pipeline KPIs and funnel metrics
- `POST /workflow/advance/{tender_id}?stage=eoi_preparation` - Advance stage
- `GET /workflow/overdue` - SLA overdue items
- `GET /workflow/stages` - Available stages and team roles
- `GET /workflow/lifecycle` - Lifecycle stage catalog with descriptions and SLAs
- `GET /workflow/lifecycle/{tender_id}` - Lifecycle status for one tender
- `GET /workflow/keywords` - Scoring keyword catalog (default + custom)
- `POST /workflow/keywords?keyword=...&weight=4` - Add/update custom keyword
- `PATCH /workflow/keywords/{id}?is_active=true&weight=5` - Update keyword status/weight
- `DELETE /workflow/keywords/{id}` - Delete custom keyword

### Partners / Consortium

- `POST /partners?name=...&partner_type=local` - Create partner
- `GET /partners` - List all partners
- `POST /tenders/{id}/partners?partner_id=1&role=lead` - Assign partner to tender
- `GET /tenders/{id}/partners` - List consortium for a tender
- `PATCH /tender-partners/{id}/mou?mou_status=signed` - Update MoU status

### Proposals

- `POST /tenders/{id}/proposals?proposal_type=eoi&status=draft` - Create/update proposal
- `GET /tenders/{id}/proposals` - List proposals for tender

### Quality Reviews

- `GET /tenders/{id}/reviews` - List 3-level reviews
- `POST /tenders/{id}/reviews/{level}?status=approved` - Update review

### Checklists

- `GET /tenders/{id}/checklists?stage=eoi_preparation` - List checklist items
- `POST /checklists/{item_id}/toggle` - Toggle checklist completion

### Team Assignments

- `POST /tenders/{id}/team?role=Technical Lead&person_name=...` - Assign role
- `GET /tenders/{id}/team` - List team assignments

## BD workflow automation

Automates the full BD Process document end-to-end:

1. **Opportunity Identification** - Daily ingestion from 17+ portals
2. **Go/No-Go Screening** - Automated scoring using 100+ keywords, geographic fit, budget, deadline urgency
3. **Partner & Consortium Development** - Partner database, consortium role assignment, MoU tracking
4. **EOI Preparation** - Proposal tracking, firm experience selection checklists
5. **Technical Proposal** - Methodology/staffing/work plan checklists with team assignments
6. **Financial Proposal** - Cost structure and pricing checklists
7. **Quality Control** - 3-level review system (Technical Lead, Proposal Manager, CEO/Partner)
8. **Submission & Post-Submission** - Submission tracking, clarification management
9. **Negotiation & Award** - Contract negotiation checklists
10. **Handover** - Delivery team transfer checklists

### Pipeline stages

`identified` > `screened` > `partner_development` > `eoi_preparation` > `technical_proposal` > `financial_proposal` > `quality_control` > `submitted` > `post_submission` > `negotiation` > `awarded` > `handover` > `closed`

### Environment variables (optional)

- `BD_KEYWORDS` (comma-separated keyword list for scoring)
- `BD_FOCUS_COUNTRIES` (comma-separated list for geographic fit)
- `BD_LEADS` (comma-separated names for lead assignment rotation)
- `NIC_MAX_LINKS_PER_PORTAL`, `INTL_MAX_LINKS_PER_PORTAL`, `ADDITIONAL_MAX_LINKS_PER_PORTAL` (`0` = unbounded extraction)
- `TED_MAX_PAGES`, `WORLD_BANK_MAX_PAGES`, `WORLD_BANK_PROJECTS_MAX_PAGES`, `ADB_MAX_PAGES`, `AFD_MAX_PAGES` (`0` = unbounded pagination where supported)
- `*_DETAIL_FETCH_LIMIT` (`0` = unbounded detail enrichment where supported)
- `BENCHMARK_MAX_MISSING_ORG_PCT`, `BENCHMARK_MAX_MISSING_COUNTRY_PCT`, `BENCHMARK_MAX_MISSING_CLOSING_PCT`
- `INGESTION_TIME_BUDGET_SECONDS` (soft runtime budget per ingestion run; defaults to 1800s)

## Smoke test

With the API running, execute:

- `python smoke_test.py`

Optional env overrides:

- `BASE_URL` (default `http://127.0.0.1:8000`)
- `SMOKE_TEST_TIMEOUT_SECONDS` (default `60`)
