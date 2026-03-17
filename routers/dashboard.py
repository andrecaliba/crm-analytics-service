from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from db import get_db
from auth import get_current_user, require_manager
from queries.bd_dashboard import (
    BD_KPIS, BD_REVENUE_BY_MONTH, BD_PIPELINE_BY_STAGE, BD_OPEN_DEALS
)
from queries.exec_dashboard import (
    EXEC_TEAM_KPIS, EXEC_LEADERBOARD, EXEC_STUCK_DEALS,
    EXEC_PIPELINE_BY_STAGE, EXEC_BY_ACCOUNT_TYPE, EXEC_BY_SERVICE
)

router = APIRouter(prefix="/api/analytics/dashboard", tags=["Dashboard"])


# ── BD Dashboard ──────────────────────────────────────────────────────────────

@router.get(
    "/bd",
    summary="BD Dashboard — 10 metrics",
    description="""
Returns all metrics needed for an individual BD rep's performance dashboard.

**Access rules:**
- `BD_REP` role: can only request their own `bd_id`. Requesting another BD's data returns 403.
- `SALES_MANAGER` role: can request any `bd_id`.

**Metrics returned:**
1. `total_revenue` — Closed Won revenue this quarter
2. `open_pipeline` — Sum of all open deal values
3. `quota` — Quarterly quota from the target table
4. `attainment_pct` — total_revenue / quota × 100
5. `sales_forecast` — Sum of weighted deal projections (open deals only)
6. `variance` — total_revenue − quota (negative = behind)
7. `excess_deficit` — "Excess" or "Deficit" label
8. `revenue_by_month` — Monthly revenue array for the bar chart
9. `pipeline_by_stage` — Open deal count + value per stage
10. `open_deals` — List of all open deals with stage and days in stage
""",
)
def bd_dashboard(
    year: int = Query(..., description="Calendar year, e.g. 2026"),
    quarter: int = Query(..., ge=1, le=4, description="Quarter number 1–4"),
    bd_id: str = Query(..., description="UUID of the BD rep"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    # BD_REP can only see their own data
    if user["role"] == "BD_REP" and user["bd_id"] != bd_id:
        raise HTTPException(status_code=403, detail="You can only view your own dashboard")

    params = {"year": year, "quarter": quarter, "bd_id": bd_id}

    kpis = db.execute(text(BD_KPIS), params).mappings().one_or_none()
    if not kpis:
        raise HTTPException(status_code=404, detail="BD not found or no data for this period")

    revenue_by_month = [
        dict(r) for r in db.execute(text(BD_REVENUE_BY_MONTH), params).mappings()
    ]
    pipeline_by_stage = [
        dict(r) for r in db.execute(text(BD_PIPELINE_BY_STAGE), params).mappings()
    ]
    open_deals = [
        dict(r) for r in db.execute(text(BD_OPEN_DEALS), params).mappings()
    ]

    return {
        **dict(kpis),
        "revenue_by_month":  revenue_by_month,
        "pipeline_by_stage": pipeline_by_stage,
        "open_deals":        open_deals,
    }


# ── Executive Dashboard ───────────────────────────────────────────────────────

@router.get(
    "/executive",
    summary="Executive Dashboard — 9 metrics",
    description="""
Returns all metrics for the team-wide executive dashboard.

**Access:** SALES_MANAGER only.

**Metrics returned:**
1. `team.total_revenue` — Team Closed Won revenue this quarter
2. `team.total_quota` — Sum of all BD quarterly quotas
3. `team.sales_forecast` — Sum of all weighted deal projections
4. `team.attainment_pct` — team revenue / team quota × 100
5. `leaderboard` — All BD reps ranked by revenue with attainment and win rate
6. `stuck_deals` — Open deals exceeding their stage duration threshold
7. `pipeline_by_stage` — Team-wide open deal count + value per stage
8. `by_account_type` — Closed Won revenue grouped by client account type
9. `by_service` — Closed Won revenue grouped by service or bundle name
""",
)
def executive_dashboard(
    year: int = Query(..., description="Calendar year, e.g. 2026"),
    quarter: int = Query(..., ge=1, le=4, description="Quarter number 1–4"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_manager),
):
    params = {"year": year, "quarter": quarter}

    team = db.execute(text(EXEC_TEAM_KPIS), params).mappings().one()
    leaderboard = [dict(r) for r in db.execute(text(EXEC_LEADERBOARD), params).mappings()]
    stuck_deals = [dict(r) for r in db.execute(text(EXEC_STUCK_DEALS), params).mappings()]
    pipeline_by_stage = [dict(r) for r in db.execute(text(EXEC_PIPELINE_BY_STAGE), params).mappings()]
    by_account_type = [dict(r) for r in db.execute(text(EXEC_BY_ACCOUNT_TYPE), params).mappings()]
    by_service = [dict(r) for r in db.execute(text(EXEC_BY_SERVICE), params).mappings()]

    return {
        "team":             dict(team),
        "leaderboard":      leaderboard,
        "stuck_deals":      stuck_deals,
        "pipeline_by_stage": pipeline_by_stage,
        "by_account_type":  by_account_type,
        "by_service":       by_service,
    }
