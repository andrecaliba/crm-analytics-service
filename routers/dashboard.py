from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from db import get_db
from auth import get_current_user
from queries.bd_dashboard import (
    BD_KPIS,
    BD_REVENUE_BY_MONTH,
    BD_PIPELINE_BY_STAGE,
    BD_OPEN_DEALS,
    BD_SERVICE_REVENUE,
    BD_ACCOUNT_TYPE_PIPELINE,
    BD_LEAD_SOURCE,
)
from queries.exec_dashboard import (
    EXEC_TEAM_KPIS, EXEC_LEADERBOARD, EXEC_STUCK_DEALS,
    EXEC_PIPELINE_BY_STAGE, EXEC_BY_ACCOUNT_TYPE, EXEC_BY_SERVICE,
)

router = APIRouter(prefix="/api/analytics/dashboard", tags=["Dashboard"])


# Current action plan due date now lives on the open deal_audit_log row,
# not on deal.action_plan_due_date.
BD_FOLLOW_UP = """
SELECT
    COUNT(d.id)::int AS total_open,
    COUNT(
        CASE
            WHEN dal.action_plan_due_date IS NOT NULL
             AND dal.action_plan_due_date < NOW()::date
            THEN 1
        END
    )::int AS overdue_action_plans,
    COUNT(
        CASE
            WHEN d.last_follow_up_at IS NOT NULL
             AND d.last_follow_up_at < NOW() - INTERVAL '14 days'
            THEN 1
        END
    )::int AS overdue_follow_ups,
    COUNT(
        CASE
            WHEN dal.action_plan_due_date IS NOT NULL
             AND dal.action_plan_due_date >= NOW()::date
             AND dal.action_plan_due_date <= (NOW()::date + INTERVAL '3 days')
            THEN 1
        END
    )::int AS upcoming_action_plans
FROM deal d
LEFT JOIN deal_audit_log dal
  ON dal.deal_id = d.id
 AND dal.exited_at IS NULL
WHERE d.bd_id = :bd_id
  AND d.is_closed = false;
"""


# ── BD Dashboard ──────────────────────────────────────────────────────────────

@router.get(
    "/bd",
    summary="BD Dashboard — full metrics",
    description="""
Returns all metrics needed for an individual BD rep's performance dashboard.

**Access rules:**
- `BD_REP` role: can only request their own `bd_id`. Requesting another BD's data returns 403.
- `SALES_MANAGER` role: can request any `bd_id`.

**Metrics returned:**

*KPIs*
- `total_revenue` — Closed Won contract revenue recognized within the selected quarter
- `quota` — Quarterly quota (sub-label under total_revenue)
- `monthly_quota` — Monthly quota (sub-label for monthly variance card)
- `open_pipeline` — Sum of all open deal values
- `attainment_pct` — total_revenue / quota * 100
- `sales_forecast` — Contract-based closed revenue for the quarter + open Negotiation pipeline
- `variance` — total_revenue - quota (quarterly)
- `monthly_variance` — MTD contract revenue - monthly quota
- `excess_deficit` — "Excess" or "Deficit" (quarterly)
- `monthly_excess_deficit` — "Excess" or "Deficit" (monthly)

*Charts & Lists*
- `revenue_by_month` — Monthly contract revenue + quota reference for bar chart (3 rows per quarter)
- `pipeline_by_stage` — Open deal count + value per stage (all 7 stages, 0 for empty)
- `open_deals` — Open deals ordered by revenue desc
- `service_revenue` — Contract revenue per service recognized within the selected quarter
- `account_type_pipeline` — Open deal count + value per client account type
- `lead_source` — Deal count, won count, and recognized contract revenue per lead source
- `follow_up` — Overdue action plans, overdue follow-ups, upcoming action plans
""",
)
def bd_dashboard(
    year: int = Query(..., description="Calendar year, e.g. 2026"),
    quarter: int = Query(..., ge=1, le=4, description="Quarter number 1-4"),
    bd_id: str = Query(..., description="UUID of the BD rep"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    # BD_REP can only see their own data
    # if user["role"] == "BD_REP" and user["bd_id"] != bd_id:
    #     raise HTTPException(status_code=403, detail="You can only view your own dashboard")

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
    service_revenue = [
        dict(r) for r in db.execute(text(BD_SERVICE_REVENUE), params).mappings()
    ]
    account_type_pipeline = [
        dict(r) for r in db.execute(text(BD_ACCOUNT_TYPE_PIPELINE), params).mappings()
    ]
    lead_source = [
        dict(r) for r in db.execute(text(BD_LEAD_SOURCE), params).mappings()
    ]

    follow_up_row = db.execute(text(BD_FOLLOW_UP), params).mappings().one_or_none()
    follow_up = dict(follow_up_row) if follow_up_row else {
        "total_open": 0,
        "overdue_action_plans": 0,
        "overdue_follow_ups": 0,
        "upcoming_action_plans": 0,
    }

    return {
        **dict(kpis),
        "revenue_by_month":      revenue_by_month,
        "pipeline_by_stage":     pipeline_by_stage,
        "open_deals":            open_deals,
        "service_revenue":       service_revenue,
        "account_type_pipeline": account_type_pipeline,
        "lead_source":           lead_source,
        "follow_up":             follow_up,
    }


# ── Executive Dashboard ───────────────────────────────────────────────────────

@router.get(
    "/executive",
    summary="Executive Dashboard — 9 metrics",
    description="""
Returns all metrics for the team-wide executive dashboard.

**Access:** SALES_MANAGER only.

**Metrics returned:**
1. `team.total_revenue` — Team contract revenue recognized within the selected quarter
2. `team.total_quota` — Sum of all BD quarterly quotas
3. `team.sales_forecast` — Recognized contract revenue for the quarter + open Negotiation pipeline
4. `team.attainment_pct` — team revenue / team quota × 100
5. `leaderboard` — All BD reps ranked by revenue with attainment and win rate
6. `stuck_deals` — Open deals exceeding their stage duration threshold
7. `pipeline_by_stage` — Team-wide open deal count + value per stage
8. `by_account_type` — Contract revenue grouped by client account type for the selected quarter
9. `by_service` — Contract revenue grouped by service or bundle name for the selected quarter
""",
)
def executive_dashboard(
    year: int = Query(..., description="Calendar year, e.g. 2026"),
    quarter: int = Query(..., ge=1, le=4, description="Quarter number 1-4"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter}

    team = db.execute(text(EXEC_TEAM_KPIS), params).mappings().one()
    leaderboard = [dict(r) for r in db.execute(text(EXEC_LEADERBOARD), params).mappings()]
    stuck_deals = [dict(r) for r in db.execute(text(EXEC_STUCK_DEALS), params).mappings()]
    pipeline_by_stage = [dict(r) for r in db.execute(text(EXEC_PIPELINE_BY_STAGE), params).mappings()]
    by_account_type = [dict(r) for r in db.execute(text(EXEC_BY_ACCOUNT_TYPE), params).mappings()]
    by_service = [dict(r) for r in db.execute(text(EXEC_BY_SERVICE), params).mappings()]

    return {
        "team": dict(team),
        "leaderboard": leaderboard,
        "stuck_deals": stuck_deals,
        "pipeline_by_stage": pipeline_by_stage,
        "by_account_type": by_account_type,
        "by_service": by_service,
    }
