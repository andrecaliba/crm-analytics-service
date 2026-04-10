# Sales CRM Analytics Service

FastAPI analytics service for the Sales CRM. Connects to the shared PostgreSQL
database and serves aggregated metrics to the frontend dashboards and report exports.

Weekly pipeline snapshots are orchestrated by **Apache Airflow 3.x** running in
Docker alongside this service.

---

## Prerequisites

- Python 3.11+ and [uv](https://docs.astral.sh/uv/) installed in WSL
- Docker Desktop running
- PostgreSQL running locally (same instance as the CRM service) on port 5433
- CRM service running — BD accounts and deal data must exist before seeding

Install uv if needed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
```

---

## 1. First-time setup

```bash
git clone https://github.com/your-username/crm-analytics-service.git
cd crm-analytics-service
```

### Install dependencies

```bash
uv sync
source .venv/bin/activate
```

### Set up environment variables

```bash
cp .env.example .env
```

Fill in `.env` — at minimum set `DATABASE_URL`, `JWT_SECRET`, and
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`. See `.env.example` for format and notes.

> **Key distinction:** `DATABASE_URL` uses `localhost` (for uvicorn running natively).
> `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` uses `host.docker.internal` (for Airflow running in Docker).

---

## 2. Seed the database (run once)

These only insert data — re-running is safe (`ON CONFLICT DO NOTHING`).

### Step 1 — Date dimension

```bash
PYTHONPATH=. uv run python scripts/seed_dates.py
```

Expected: `Done — seeded 1827 date rows (2024–2028)`

### Step 2 — BD targets / quotas

Make sure BD accounts exist in the CRM first.

```bash
PYTHONPATH=. uv run python scripts/seed_targets.py
```

Expected:
```
  Seeded targets for Brian Siriban
  Seeded targets for Henne Zarate
  Seeded targets for Kristina Villarta

Done — seeded annual/quarterly/monthly targets for 3 BD reps
```

---

## 3. Start the FastAPI service

```bash
source .venv/bin/activate
uvicorn main:app --reload --port 8001
```

- **Swagger UI**: http://localhost:8001/docs
- **ReDoc**: http://localhost:8001/redoc

---

## 4. Start Airflow (Docker)

Airflow runs in a single Docker container using `airflow standalone`.

```bash
docker compose -f docker-compose.airflow.yml up -d
```

Wait about 60–90 seconds for startup (it installs Python deps on first run),
then open **http://localhost:8080** and log in with the credentials from your `.env`.

You should see two DAGs:
- `forecast_snapshot_dag` — every Sunday at 00:00 Asia/Manila
- `deal_snapshot_dag` — every Sunday at 00:30 Asia/Manila

### Stop Airflow

```bash
docker compose -f docker-compose.airflow.yml down
```

### Full reset (wipe Airflow metadata)

```bash
docker compose -f docker-compose.airflow.yml down -v
psql -h localhost -U postgres -p 5433 -d sales-crm -c "DROP SCHEMA IF EXISTS airflow CASCADE;"
```

---

## 5. Test the snapshot jobs

### Option A — Test script (no Airflow needed, fastest)

```bash
PYTHONPATH=. python scripts/test_airflow_jobs.py           # both jobs
PYTHONPATH=. python scripts/test_airflow_jobs.py --job forecast
PYTHONPATH=. python scripts/test_airflow_jobs.py --job deal
```

Expected output:
```
Airflow Snapshot Job Test
Date: 2026-04-10

── Preflight ─────────────────────────────────────
  ✓  date_dimension row found — date_id: <uuid>

── forecast_snapshot ─────────────────────────────
  →  Rows in forecast_snapshot for today before run: 0
  ✓  weekly_forecast_snapshot() completed without error
  ✓  Inserted 4 row(s) into forecast_snapshot

── Summary ───────────────────────────────────────
  ✓  forecast_snapshot_dag
  ✓  deal_snapshot_dag

All jobs passed.
```

### Option B — Trigger via Airflow UI

1. Open http://localhost:8080
2. Click the **▶ Trigger** button on either DAG
3. Watch the task grid — both tasks should turn green

### Verify rows in the DB

```sql
-- Latest forecast snapshots
SELECT
    COALESCE(b.first_name || ' ' || b.last_name, 'TEAM') AS name,
    fs.total_pipeline_value,
    fs.deal_count,
    dd.timestamp::date AS snapshot_date
FROM forecast_snapshot fs
LEFT JOIN bd b ON b.id = fs.bd_id
JOIN date_dimension dd ON dd.id = fs.snapshot_date_id
ORDER BY snapshot_date DESC, name
LIMIT 10;

-- Latest deal snapshots
SELECT
    d.deal_name,
    ps.name AS stage,
    ds.projected_amount,
    dd.timestamp::date AS snapshot_date
FROM deal_snapshot ds
JOIN deal d ON d.id = ds.deal_id
JOIN pipeline_stage ps ON ps.id = ds.stage_id
JOIN date_dimension dd ON dd.id = ds.date_id
ORDER BY snapshot_date DESC, ds.projected_amount DESC
LIMIT 10;
```

---

## 6. API authentication

Get a JWT from the CRM login endpoint:

```bash
curl -s -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "your@email.com", "password": "yourpassword"}' \
  | python -m json.tool
```

Set for use in curl commands:
```bash
TOKEN="paste_your_jwt_here"
BD_ID="paste_a_bd_rep_uuid_here"
YEAR=2026
QUARTER=2
BASE=http://localhost:8001
```

---

## 7. API endpoints

### Health check
```bash
curl -s $BASE/health | python -m json.tool
# → { "status": "ok", "service": "sales-crm-analytics" }
```

### BD Dashboard
```bash
curl -s "$BASE/api/analytics/dashboard/bd?year=$YEAR&quarter=$QUARTER&bd_id=$BD_ID" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

### Executive Dashboard (SALES_MANAGER only)
```bash
curl -s "$BASE/api/analytics/dashboard/executive?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

### Reports
```bash
# Pipeline
curl -s "$BASE/api/analytics/reports/pipeline?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

# Pipeline — Excel download
curl -s "$BASE/api/analytics/reports/pipeline?year=$YEAR&quarter=$QUARTER&format=xlsx" \
  -H "Authorization: Bearer $TOKEN" -o pipeline.xlsx

# Quota
curl -s "$BASE/api/analytics/reports/quota?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

# Loss analysis
curl -s "$BASE/api/analytics/reports/loss-analysis?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

# Sales cycle
curl -s "$BASE/api/analytics/reports/sales-cycle?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool

# Win rate
curl -s "$BASE/api/analytics/reports/win-rate?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

---

## 8. Project structure

```
crm-analytics-service/
├── main.py                       FastAPI app entrypoint
├── db.py                         SQLAlchemy engine + session factory
├── auth.py                       JWT verification, role-based dependencies
├── scheduler.py                  Snapshot functions imported by Airflow DAGs
├── dags/
│   ├── forecast_snapshot_dag.py  Runs weekly_forecast_snapshot() — Sundays 00:00
│   └── deal_snapshot_dag.py      Runs weekly_deal_snapshot() — Sundays 00:30
├── routers/
│   ├── dashboard.py              /api/analytics/dashboard/bd + /executive
│   ├── reports.py                /api/analytics/reports/*
│   └── team.py
├── queries/
│   ├── bd_dashboard.py           SQL for BD dashboard
│   ├── exec_dashboard.py         SQL for Executive dashboard
│   └── reports.py                SQL for all report types
├── scripts/
│   ├── seed_dates.py             One-time: populate date_dimension 2024–2028
│   ├── seed_targets.py           One-time: seed BD quotas
│   ├── create_airflow_schema.py  Run by Docker on Airflow startup
│   └── test_airflow_jobs.py      Manually test snapshot jobs + verify DB output
├── tests/
│   ├── test_endpoints.py         Automated API endpoint tests
│   ├── test_cross_service.py     Cross-service integration tests
│   └── test_schema_contract.py   Schema contract checks
├── docker-compose.airflow.yml    Airflow standalone (single container)
├── pyproject.toml                Project dependencies
├── uv.lock                       Locked dependency versions
├── .env.example                  Environment variable template
└── README.md
```

---

## 9. Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: No module named 'db'` | Missing PYTHONPATH | Prefix command with `PYTHONPATH=.` |
| `RuntimeError: DATABASE_URL is not set` | Missing .env | `cp .env.example .env` and fill in values |
| `connection refused` on port 5433 | Postgres not running | Start PostgreSQL |
| `host.docker.internal` connection refused in uvicorn | Wrong DATABASE_URL | Use `localhost` in DATABASE_URL (not `host.docker.internal`) |
| `401 Unauthorized` | Token expired | Get a fresh token from CRM login endpoint |
| `403 Forbidden` on executive endpoints | Wrong role | Use a SALES_MANAGER account |
| `No date_dimension row for today` | seed_dates.py not run | `PYTHONPATH=. uv run python scripts/seed_dates.py` |
| `All quotas return 0` | seed_targets.py not run | `PYTHONPATH=. uv run python scripts/seed_targets.py` |
| DAG not appearing in Airflow UI | Import error | Check Docker logs: `docker compose -f docker-compose.airflow.yml logs airflow` |
| Airflow tasks failing with state mismatch | Airflow 3 bug with split services | Already handled — using `standalone` mode |
| `uv: command not found` | uv not installed | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |