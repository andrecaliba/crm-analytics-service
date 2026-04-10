"""
Sales CRM Analytics Service
============================
Separate Python FastAPI service for OLAP / analytics.
Reads from the shared PostgreSQL database (same DB as the CRM service).

Run locally:
    uvicorn main:app --reload --port 8001

Docs available at:
    http://localhost:8001/docs        (Swagger UI)
    http://localhost:8001/redoc       (ReDoc)

Snapshot jobs (weekly_forecast_snapshot, weekly_deal_snapshot) are now
orchestrated by Apache Airflow 3.x — see dags/ directory.
APScheduler has been removed.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import dashboard, reports, team

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Snapshot jobs are now managed by Airflow DAGs (dags/).
    # Nothing to start or stop here for scheduling.
    logger.info("Analytics service started — snapshot jobs managed by Airflow")
    yield
    logger.info("Analytics service stopped")


app = FastAPI(
    title="Sales CRM Analytics API",
    description="""
## Sales CRM Analytics Service

This is the OLAP / analytics layer for the Sales CRM.
It reads from the shared PostgreSQL database and serves aggregated metrics
to the frontend dashboards and report exports.

### Authentication
All endpoints require a `Bearer <JWT>` token in the `Authorization` header.
Use the same token issued by the CRM service's `/api/auth/login` endpoint.

### Access Control
- **BD_REP** — can only access their own BD dashboard (`/dashboard/bd?bd_id=<own id>`)
- **SALES_MANAGER** — full access to all endpoints

### Endpoints
| Group | Prefix | Description |
|-------|--------|-------------|
| Dashboard | `/api/analytics/dashboard` | BD + Executive dashboard metrics |
| Reports | `/api/analytics/reports` | Pipeline, quota, loss analysis, sales cycle, win rate |

### Snapshot Jobs
Weekly pipeline snapshots are scheduled via Apache Airflow 3.x.
See `dags/` for DAG definitions. To trigger manually:
    airflow dags trigger forecast_snapshot_dag
    airflow dags trigger deal_snapshot_dag
""",
    version="1.1.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Allow the CRM frontend to call this API.
# In production, replace "*" with the actual Railway frontend URL.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(dashboard.router)
app.include_router(reports.router)
app.include_router(team.router)


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"], summary="Health check")
def health():
    """Returns 200 OK if the service is running. No auth required."""
    return {"status": "ok", "service": "sales-crm-analytics"}