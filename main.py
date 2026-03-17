"""
Sales CRM Analytics Service
============================
Separate Python FastAPI service for OLAP / analytics.
Reads from the shared PostgreSQL database (same DB as Zeandy's CRM).

Run locally:
    uvicorn main:app --reload --port 8001

Docs available at:
    http://localhost:8001/docs        (Swagger UI)
    http://localhost:8001/redoc       (ReDoc)
"""

import logging
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import dashboard, reports
from scheduler import weekly_forecast_snapshot, weekly_deal_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    scheduler.add_job(
        weekly_forecast_snapshot,
        trigger="cron",
        day_of_week="sun",
        hour=0,
        minute=0,
        id="forecast_snapshot",
        replace_existing=True,
    )
    scheduler.add_job(
        weekly_deal_snapshot,
        trigger="cron",
        day_of_week="sun",
        hour=0,
        minute=30,
        id="deal_snapshot",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started — snapshot jobs registered (every Sunday)")
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────────
    scheduler.shutdown(wait=False)
    logger.info("Scheduler stopped")


app = FastAPI(
    title="Sales CRM Analytics API",
    description="""
## Sales CRM Analytics Service

This is the OLAP / analytics layer for the Sales CRM.
It reads from the shared PostgreSQL database and serves aggregated metrics
to the frontend dashboards and report exports.

### Authentication
All endpoints require a `Bearer <JWT>` token in the `Authorization` header.
Use the same token issued by Zeandy's `/api/auth/login` endpoint.

### Access Control
- **BD_REP** — can only access their own BD dashboard (`/dashboard/bd?bd_id=<own id>`)
- **SALES_MANAGER** — full access to all endpoints

### Endpoints
| Group | Prefix | Description |
|-------|--------|-------------|
| Dashboard | `/api/analytics/dashboard` | BD + Executive dashboard metrics |
| Reports | `/api/analytics/reports` | Pipeline, quota, loss analysis, sales cycle, win rate |
""",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Allow Zeandy's frontend to call this API.
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


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"], summary="Health check")
def health():
    """Returns 200 OK if the service is running. No auth required."""
    return {"status": "ok", "service": "sales-crm-analytics"}
