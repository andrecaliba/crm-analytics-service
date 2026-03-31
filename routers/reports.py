from typing import Optional
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from io import BytesIO
from typing import Optional
import openpyxl

from db import get_db
from auth import get_current_user
from queries.reports import (
    PIPELINE_REPORT, PIPELINE_TOTALS,
    QUOTA_REPORT, QUOTA_TEAM_TOTALS,
    LOSS_BY_STAGE, LOSS_DEALS_LIST, LOSS_TOTALS,
    SALES_CYCLE_BY_STAGE, SALES_CYCLE_TOTAL,
    WIN_RATE_BY_LEAD_SOURCE, WIN_RATE_BY_SERVICE,
    WIN_RATE_BY_INDUSTRY, WIN_RATE_OVERALL,
    PIPELINE_BY_BD, PIPELINE_BY_SERVICE, PIPELINE_BY_ACCOUNT_TYPE,
    PIPELINE_LEAD_SOURCE, PIPELINE_STAGE_TOTALS,
    GROWTH_BY_MONTH, GROWTH_BY_QUARTER, GROWTH_BY_YEAR,
    SERVICE_PERFORMANCE,
    BD_LIST,
)

router = APIRouter(prefix="/api/analytics/reports", tags=["Reports"])


# ── BD filter helper ─────────────────────────────────────────────────────────

def _inject_bd_filter(sql: str, bd_id: Optional[str], params: dict, deal_alias: str = "d") -> tuple[str, dict]:
    """Inject an optional BD filter into a SQL query.
    Returns (modified_sql, modified_params)."""
    if not bd_id:
        return sql, params
    new_params = {**params, "bd_id": bd_id}
    # Find the best injection point: before GROUP BY, ORDER BY, or at end
    for keyword in ["GROUP BY", "ORDER BY", "LIMIT"]:
        idx = sql.upper().rfind(keyword)
        if idx > 0:
            inject = f"\n  AND {deal_alias}.bd_id = :bd_id\n"
            return sql[:idx] + inject + sql[idx:], new_params
    # Append at end
    return sql + f"\n  AND {deal_alias}.bd_id = :bd_id", new_params


# ── Pipeline enrichment queries ──────────────────────────────────────────────

PIPELINE_BY_SERVICE = """
SELECT
    COALESCE(s.name, b.name, 'Unknown') AS service_name,
    COUNT(d.id)::int                    AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float  AS total_value
FROM deal d
LEFT JOIN service s ON s.id = d.service_id
LEFT JOIN bundle  b ON b.id = d.bundle_id
WHERE d.is_closed = false
GROUP BY COALESCE(s.name, b.name, 'Unknown')
ORDER BY total_value DESC;
"""

PIPELINE_BY_ACCOUNT_TYPE = """
SELECT
    c.account_type::text               AS account_type,
    COUNT(d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM deal d
JOIN client c ON c.id = d.client_id
WHERE d.is_closed = false
GROUP BY c.account_type
ORDER BY total_value DESC;
"""

PIPELINE_BY_LEAD_SOURCE = """
SELECT
    d.lead_source::text                AS lead_source,
    COUNT(d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM deal d
WHERE d.is_closed = false
GROUP BY d.lead_source
ORDER BY total_value DESC;
"""

PIPELINE_BY_BD = """
SELECT
    b.id                               AS bd_id,
    b.first_name || ' ' || b.last_name AS bd_name,
    COUNT(d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM deal d
JOIN bd b ON b.id = d.bd_id
WHERE d.is_closed = false
GROUP BY b.id, b.first_name, b.last_name
ORDER BY total_value DESC;
"""


# ── Excel helper ──────────────────────────────────────────────────────────────

def _to_excel(sheets: dict, filename: str) -> StreamingResponse:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    for sheet_name, rows in sheets.items():
        ws = wb.create_sheet(sheet_name)
        if not rows:
            ws.append(["No data for this period"])
            continue
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([row.get(h) for h in headers])
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}.xlsx"'},
    )


# ── Pipeline report ───────────────────────────────────────────────────────────

@router.get(
    "/pipeline",
    summary="Pipeline report — open deals by stage with service/account/lead breakdowns",
)
def pipeline_report(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    format: str = Query("json", description="Response format: 'json' or 'xlsx'"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter}

    # Apply optional bd_id filter
    pipe_sql, pipe_params = _inject_bd_filter(PIPELINE_REPORT, bd_id, params)
    tot_sql, tot_params = _inject_bd_filter(PIPELINE_TOTALS, bd_id, params, "deal")
    svc_sql, svc_params = _inject_bd_filter(PIPELINE_BY_SERVICE, bd_id, params)
    acct_sql, acct_params = _inject_bd_filter(PIPELINE_BY_ACCOUNT_TYPE, bd_id, params)
    ls_sql, ls_params = _inject_bd_filter(PIPELINE_BY_LEAD_SOURCE, bd_id, params)
    bd_sql = PIPELINE_BY_BD

    stages = [dict(r) for r in db.execute(text(pipe_sql), pipe_params).mappings()]

    # Fix totals query - it uses no alias for deal table
    if bd_id:
        tot_sql_fixed = PIPELINE_TOTALS.replace(
            "WHERE is_closed = false;",
            f"WHERE is_closed = false AND bd_id = :bd_id;"
        )
        totals = dict(db.execute(text(tot_sql_fixed), {"year": year, "quarter": quarter, "bd_id": bd_id}).mappings().one())
    else:
        totals = dict(db.execute(text(PIPELINE_TOTALS), params).mappings().one())

    by_service = [dict(r) for r in db.execute(text(svc_sql), svc_params).mappings()]
    by_account_type = [dict(r) for r in db.execute(text(acct_sql), acct_params).mappings()]
    by_lead_source = [dict(r) for r in db.execute(text(ls_sql), ls_params).mappings()]
    by_bd = [dict(r) for r in db.execute(text(bd_sql), params).mappings()]

    if format == "xlsx":
        return _to_excel({"Pipeline": stages}, f"pipeline-Q{quarter}-{year}")

    return {
        "report":               "pipeline",
        "period":               f"Q{quarter} {year}",
        "bd_id":                bd_id,
        "stages":               stages,
        "total_deals":          totals["total_deals"],
        "total_pipeline_value": totals["total_pipeline_value"],
        "by_service":           by_service,
        "by_account_type":      by_account_type,
        "by_lead_source":       by_lead_source,
        "by_bd":                by_bd,
    }


# ── Quota report ──────────────────────────────────────────────────────────────

@router.get(
    "/quota",
    summary="Quota report — actual vs target per BD",
)
def quota_report(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter}
    members = [dict(r) for r in db.execute(text(QUOTA_REPORT), params).mappings()]

    # Filter members client-side if bd_id is specified
    if bd_id:
        members = [m for m in members if m.get("bd_id") == bd_id]

    totals = dict(db.execute(text(QUOTA_TEAM_TOTALS), params).mappings().one())

    if bd_id and members:
        team_actual = sum(m.get("actual", 0) for m in members)
        team_quota = sum(m.get("quota", 0) for m in members)
    else:
        team_actual = totals["team_actual"]
        team_quota = totals["team_quota"]

    team_attainment = round(
        team_actual / team_quota * 100 if team_quota else 0, 1
    )
    if format == "xlsx":
        return _to_excel({"Quota": members}, f"quota-Q{quarter}-{year}")
    return {
        "report":              "quota",
        "period":              f"Q{quarter} {year}",
        "members":             members,
        "team_quota":          team_quota,
        "team_actual":         team_actual,
        "team_attainment_pct": team_attainment,
    }


# ── Loss analysis ─────────────────────────────────────────────────────────────

@router.get(
    "/loss-analysis",
    summary="Loss analysis — Closed Lost deals by stage and BD",
)
def loss_analysis(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    params = {"year": year, "quarter": quarter}

    if bd_id:
        # Filter loss deals by bd_id
        deals_sql = LOSS_DEALS_LIST.replace(
            "ORDER BY d.closed_date DESC;",
            "AND d.bd_id = :bd_id\nORDER BY d.closed_date DESC;"
        )
        totals_sql = LOSS_TOTALS.replace(
            "AND d.closed_date <  qr.q_end;",
            "AND d.closed_date <  qr.q_end\n  AND d.bd_id = :bd_id;"
        )
        params_with_bd = {**params, "bd_id": bd_id}
        deals = [dict(r) for r in db.execute(text(deals_sql), params_with_bd).mappings()]
        totals = dict(db.execute(text(totals_sql), params_with_bd).mappings().one())
    else:
        deals = [dict(r) for r in db.execute(text(LOSS_DEALS_LIST), params).mappings()]
        totals = dict(db.execute(text(LOSS_TOTALS), params).mappings().one())

    by_stage = [dict(r) for r in db.execute(text(LOSS_BY_STAGE), params).mappings()]

    if format == "xlsx":
        return _to_excel({"By Stage": by_stage, "Lost Deals": deals}, f"loss-analysis-Q{quarter}-{year}")
    return {
        "report":           "loss_analysis",
        "period":           f"Q{quarter} {year}",
        "total_lost_deals": totals["total_lost_deals"],
        "total_lost_value": totals["total_lost_value"],
        "by_stage":         by_stage,
        "deals":            deals,
    }


# ── Sales cycle ───────────────────────────────────────────────────────────────

@router.get(
    "/sales-cycle",
    summary="Sales cycle analysis — average days per stage",
)
def sales_cycle(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    params = {"year": year, "quarter": quarter}

    if bd_id:
        stage_sql = SALES_CYCLE_BY_STAGE.replace(
            "GROUP BY ps.id, ps.name",
            "AND d.bd_id = :bd_id\nGROUP BY ps.id, ps.name"
        )
        total_sql = SALES_CYCLE_TOTAL.replace(
            "AND EXTRACT(QUARTER FROM d.closed_date) = :quarter;",
            "AND EXTRACT(QUARTER FROM d.closed_date) = :quarter\n  AND d.bd_id = :bd_id;"
        )
        params_with_bd = {**params, "bd_id": bd_id}
        by_stage = [dict(r) for r in db.execute(text(stage_sql), params_with_bd).mappings()]
        total = dict(db.execute(text(total_sql), params_with_bd).mappings().one())
    else:
        by_stage = [dict(r) for r in db.execute(text(SALES_CYCLE_BY_STAGE), params).mappings()]
        total = dict(db.execute(text(SALES_CYCLE_TOTAL), params).mappings().one())

    if format == "xlsx":
        return _to_excel({"Sales Cycle": by_stage}, f"sales-cycle-Q{quarter}-{year}")
    return {
        "report":               "sales_cycle",
        "period":               f"Q{quarter} {year}",
        "avg_total_cycle_days": total["avg_total_cycle_days"],
        "max_cycle_days":       total["max_cycle_days"],
        "sample_size":          total["sample_size"],
        "by_stage":             by_stage,
    }


# ── Win rate ──────────────────────────────────────────────────────────────────

@router.get(
    "/win-rate",
    summary="Win rate — by lead source, service, and industry",
)
def win_rate(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user)
):
    params = {"year": year, "quarter": quarter}

    if bd_id:
        params_with_bd = {**params, "bd_id": bd_id}
        # Inject bd_id filter before GROUP BY in each query
        def inject(sql):
            return sql.replace("GROUP BY", "AND d.bd_id = :bd_id\nGROUP BY", 1)

        overall = dict(db.execute(text(inject(WIN_RATE_OVERALL)), params_with_bd).mappings().one())
        by_lead_source = [dict(r) for r in db.execute(text(inject(WIN_RATE_BY_LEAD_SOURCE)), params_with_bd).mappings()]
        by_service = [dict(r) for r in db.execute(text(inject(WIN_RATE_BY_SERVICE)), params_with_bd).mappings()]
        by_industry = [dict(r) for r in db.execute(text(inject(WIN_RATE_BY_INDUSTRY)), params_with_bd).mappings()]
    else:
        overall = dict(db.execute(text(WIN_RATE_OVERALL), params).mappings().one())
        by_lead_source = [dict(r) for r in db.execute(text(WIN_RATE_BY_LEAD_SOURCE), params).mappings()]
        by_service = [dict(r) for r in db.execute(text(WIN_RATE_BY_SERVICE), params).mappings()]
        by_industry = [dict(r) for r in db.execute(text(WIN_RATE_BY_INDUSTRY), params).mappings()]

    if format == "xlsx":
        return _to_excel(
            {"By Lead Source": by_lead_source, "By Service": by_service, "By Industry": by_industry},
            f"win-rate-Q{quarter}-{year}",
        )
    return {
        "report":           "win_rate",
        "period":           f"Q{quarter} {year}",
        "overall_win_rate": overall["overall_win_rate"],
        "by_lead_source":   by_lead_source,
        "by_service":       by_service,
        "by_industry":      by_industry,
    }


# ── Growth sandbox ────────────────────────────────────────────────────────────

@router.get(
    "/growth",
    summary="Growth sandbox — revenue trend by month / quarter / year",
    description="""
Returns a revenue series for the given year and granularity.
Use bd_id to scope to a single rep; omit for the whole team.
Call this endpoint multiple times with different params to populate
side-by-side comparison series on the frontend.
""",
)
def growth(
    year: int = Query(..., description="Reference year, e.g. 2026"),
    granularity: str = Query("quarter", description="'month', 'quarter', or 'year'"),
    bd_id: Optional[str] = Query(None, description="BD UUID; omit for all BDs"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "bd_id": bd_id}
    query_map = {"month": GROWTH_BY_MONTH, "quarter": GROWTH_BY_QUARTER, "year": GROWTH_BY_YEAR}
    sql = query_map.get(granularity, GROWTH_BY_QUARTER)
    series = [dict(r) for r in db.execute(text(sql), params).mappings()]
    return {
        "year":        year,
        "granularity": granularity,
        "bd_id":       bd_id,
        "series":      series,
    }

# ── Service performance ───────────────────────────────────────────────────────

@router.get("/service-performance", summary="Service performance — revenue, win rate, avg deal size per service")
def service_performance(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter}
    services = [dict(r) for r in db.execute(text(SERVICE_PERFORMANCE), params).mappings()]
    if format == "xlsx":
        return _to_excel({"Service Performance": services}, f"service-performance-Q{quarter}-{year}")
    return {
        "report":  "service_performance",
        "period":  f"Q{quarter} {year}",
        "services": services,
    }