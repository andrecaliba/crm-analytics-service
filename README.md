# Sales CRM Analytics API

Python / FastAPI analytics service for the Sales CRM.
Connects read-only to the shared PostgreSQL database and serves
aggregated metrics to the frontend dashboards and report exports.

---

## Prerequisites

- Python 3.11+ in WSL
- [uv](https://docs.astral.sh/uv/) installed
- PostgreSQL running locally (same instance as Zeandy's project)
- Zeandy's project running — BD accounts and deal data must exist before testing

Install uv if you don't have it:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# then restart your terminal or run:
source $HOME/.local/bin/env
```

---

## 1. First-time setup

### Clone and enter the repo

```bash
git clone https://github.com/your-username/sales-crm-analytics.git
cd sales-crm-analytics
```

### Install dependencies and create virtualenv

```bash
uv sync
```

That's it. `uv sync` reads `pyproject.toml` and `uv.lock`, creates a `.venv`
automatically, and installs everything into it. No separate `venv` or `pip install` needed.

### Activate the virtualenv

```bash
source .venv/bin/activate
```

### Set up environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in:

```
DATABASE_URL=postgresql://user:password@localhost:5432/sales-crm
JWT_SECRET=your-jwt-secret-here
```

These must **exactly match** the values in Zeandy's project `.env`.
Ask Zeandy for both values if you don't have them.

---

## 2. Seed the database (run once)

These scripts must be run **before** starting the API.
They only insert data — re-running them is safe (uses `ON CONFLICT DO NOTHING`).

### Step 1 — Seed date_dimension

```bash
python scripts/seed_dates.py
```

Expected output:
```
Done — seeded 1827 date rows (2024–2028)
```

### Step 2 — Seed targets (quotas)

Make sure Zeandy has already created the BD accounts in the database first.

```bash
python scripts/seed_targets.py
```

Expected output:
```
  Seeded targets for Brian Siriban
  Seeded targets for Henne Zarate
  Seeded targets for Kristina Villarta

Done — seeded annual/quarterly/monthly targets for 3 BD reps
  Annual:    ₱7,000,000
  Quarterly: ₱1,750,000
  Monthly:   ₱583,333.33
```

---

## 3. Start the API

```bash
uvicorn main:app --reload --port 8001
```

The API is now running at **http://localhost:8001**

---

## 4. Interactive documentation

Open in your browser:

- **Swagger UI** (try endpoints live): http://localhost:8001/docs
- **ReDoc** (clean reference): http://localhost:8001/redoc

To use the Swagger UI:
1. Click **Authorize** (top right)
2. Enter: `Bearer <your_jwt_token>`
3. Get a JWT by calling Zeandy's `POST /api/auth/login` first

---

## 5. Getting a JWT token for testing

Zeandy's login endpoint:

```bash
curl -s -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "your@email.com", "password": "yourpassword"}' \
  | python -m json.tool
```

Copy the `token` value from the response. Use it in all requests below.

---

## 6. Testing each endpoint manually (curl)

Replace these variables at the top of your terminal session:

```bash
TOKEN="paste_your_jwt_here"
BD_ID="paste_a_bd_rep_uuid_here"   # get from Zeandy's DB or /api/deals response
YEAR=2026
QUARTER=1
BASE=http://localhost:8001
```

---

### Health check (no auth needed)

```bash
curl -s $BASE/health | python -m json.tool
```

Expected:
```json
{ "status": "ok", "service": "sales-crm-analytics" }
```

---

### BD Dashboard

```bash
curl -s "$BASE/api/analytics/dashboard/bd?year=$YEAR&quarter=$QUARTER&bd_id=$BD_ID" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Expected keys in response:
`total_revenue`, `open_pipeline`, `quota`, `attainment_pct`, `sales_forecast`,
`variance`, `excess_deficit`, `revenue_by_month`, `pipeline_by_stage`, `open_deals`

To verify access control — a BD_REP token should get 403 if bd_id is another BD:
```bash
curl -s "$BASE/api/analytics/dashboard/bd?year=$YEAR&quarter=$QUARTER&bd_id=wrong-uuid" \
  -H "Authorization: Bearer $TOKEN" \
  -o /dev/null -w "%{http_code}\n"
```
Expected: `403`

---

### Executive Dashboard (manager token only)

```bash
curl -s "$BASE/api/analytics/dashboard/executive?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Expected keys: `team`, `leaderboard`, `stuck_deals`, `pipeline_by_stage`,
`by_account_type`, `by_service`

Test that a BD_REP token gets rejected:
```bash
# (use a BD_REP token here)
curl -s "$BASE/api/analytics/dashboard/executive?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $BD_REP_TOKEN" \
  -o /dev/null -w "%{http_code}\n"
```
Expected: `403`

---

### Pipeline Report

```bash
curl -s "$BASE/api/analytics/reports/pipeline?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Excel download:
```bash
curl -s "$BASE/api/analytics/reports/pipeline?year=$YEAR&quarter=$QUARTER&format=xlsx" \
  -H "Authorization: Bearer $TOKEN" \
  -o pipeline.xlsx
open pipeline.xlsx
```

---

### Quota Report

```bash
curl -s "$BASE/api/analytics/reports/quota?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Check that each member has a `status` of `Exceeded`, `On Track`, or `Behind`.

Excel download:
```bash
curl -s "$BASE/api/analytics/reports/quota?year=$YEAR&quarter=$QUARTER&format=xlsx" \
  -H "Authorization: Bearer $TOKEN" \
  -o quota.xlsx
```

---

### Loss Analysis Report

```bash
curl -s "$BASE/api/analytics/reports/loss-analysis?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Note: `total_lost_deals` will be 0 if there are no Closed Lost deals in the period — that is correct.

Excel download:
```bash
curl -s "$BASE/api/analytics/reports/loss-analysis?year=$YEAR&quarter=$QUARTER&format=xlsx" \
  -H "Authorization: Bearer $TOKEN" \
  -o loss-analysis.xlsx
```

---

### Sales Cycle Report

```bash
curl -s "$BASE/api/analytics/reports/sales-cycle?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Note: `avg_total_cycle_days` and `by_stage` averages will be null/empty if no deals
have completed stage transitions yet (all deals are still in their first stage).
Create a deal, move it through 2+ stages, then re-test.

---

### Win Rate Report

```bash
curl -s "$BASE/api/analytics/reports/win-rate?year=$YEAR&quarter=$QUARTER" \
  -H "Authorization: Bearer $TOKEN" \
  | python -m json.tool
```

Note: win rate is 0 if there are no closed deals in the period.

Excel download (3 sheets: By Lead Source, By Service, By Industry):
```bash
curl -s "$BASE/api/analytics/reports/win-rate?year=$YEAR&quarter=$QUARTER&format=xlsx" \
  -H "Authorization: Bearer $TOKEN" \
  -o win-rate.xlsx
```

---

## 7. Automated test runner

Once the API is running and you have a token:

```bash
export TEST_TOKEN="your_jwt_token"
export TEST_BD_ID="a_bd_rep_uuid"
export TEST_YEAR=2026
export TEST_QUARTER=1

python tests/test_endpoints.py
```

This runs all endpoint checks and prints pass/fail for each.

---

## 8. Manually trigger the cron jobs (for testing)

You don't need to wait until Sunday to verify the snapshot jobs work.
Call them directly from Python:

```bash
python - <<'EOF'
from scheduler import weekly_forecast_snapshot, weekly_deal_snapshot
print("Running forecast snapshot...")
weekly_forecast_snapshot()
print("Running deal snapshot...")
weekly_deal_snapshot()
print("Done — check the forecast_snapshot and deal_snapshot tables in your DB.")
EOF
```

Then verify in your database:
```sql
-- Should show rows with today's date
SELECT COUNT(*), snapshot_date_id FROM forecast_snapshot GROUP BY snapshot_date_id ORDER BY 2 DESC LIMIT 5;
SELECT COUNT(*), date_id FROM deal_snapshot GROUP BY date_id ORDER BY 2 DESC LIMIT 5;
```

---

## 9. Common errors and fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `RuntimeError: DATABASE_URL is not set` | Missing .env | Copy .env.example to .env and fill it in |
| `connection refused` on DB | PostgreSQL not running | Start PostgreSQL: `sudo service postgresql start` |
| `401 Unauthorized` on all requests | Token wrong or expired | Get a fresh token from Zeandy's login endpoint |
| `403 Forbidden` on executive endpoints | Using a BD_REP token | Use a SALES_MANAGER account to log in |
| `404 BD not found` on BD dashboard | Wrong bd_id | Get a valid UUID from `SELECT id, first_name FROM bd` in your DB |
| All quotas return 0 | seed_targets.py not run | `python scripts/seed_targets.py` |
| All date filters return empty | seed_dates.py not run | `python scripts/seed_dates.py` |
| `uv: command not found` | uv not installed | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| `ModuleNotFoundError` after `uv sync` | venv not activated | `source .venv/bin/activate` |

---

## 10. Project structure

```
sales-crm-analytics/
  main.py              FastAPI app, CORS, scheduler startup/shutdown
  db.py                SQLAlchemy engine and session factory
  auth.py              JWT verification, role-based dependencies
  scheduler.py         Weekly cron job functions
  routers/
    dashboard.py       /api/analytics/dashboard/bd + /executive
    reports.py         /api/analytics/reports/*
  queries/
    bd_dashboard.py    SQL for BD dashboard (KPIs, chart, stages, deals)
    exec_dashboard.py  SQL for Executive dashboard
    reports.py         SQL for all 5 report types
  scripts/
    seed_dates.py      One-time: populate date_dimension 2024–2028
    seed_targets.py    One-time: seed BD quotas from PRD
  tests/
    test_endpoints.py  Automated endpoint verification script
  pyproject.toml       Project metadata + dependencies (uv manages this)
  uv.lock              Exact locked versions of all dependencies
  .env.example         Template for environment variables
  README.md
```

---

## 11. Deployment on Railway

1. Push this repo to GitHub
2. In Railway, open Zeandy's project → **Add Service** → **GitHub Repo** → select this repo
3. Set start command: `uvicorn main:app --host 0.0.0.0 --port 8001`
4. Add environment variables:
   - `DATABASE_URL` — the Railway PostgreSQL connection string (same as Zeandy's)
   - `JWT_SECRET` — the same secret as Zeandy's service
5. Railway will give you a public URL — share it with Zeandy to add to the frontend proxy config
6. Run seed scripts once against Railway DB:
   ```bash
   railway run python scripts/seed_dates.py
   railway run python scripts/seed_targets.py
   ```

Railway will automatically detect `pyproject.toml` and run `uv sync` during the build step.
No additional build configuration needed.
