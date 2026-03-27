from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from io import BytesIO
from typing import Optional
import openpyxl

from db import get_db
from auth import get_current_user          # everyone can read reports now
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


# ── BD list (for filter dropdowns) ───────────────────────────────────────────

@router.get("/bds", summary="List all active BD reps — for filter dropdowns")
def list_bds(
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    rows = [dict(r) for r in db.execute(text(BD_LIST)).mappings()]
    return rows


# ── Pipeline report — basic (existing) ───────────────────────────────────────

@router.get("/pipeline", summary="Pipeline report — open deals by stage")
def pipeline_report(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Filter by BD UUID; omit for all BDs"),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter, "bd_id": bd_id}
    base_params = {"year": year, "quarter": quarter}

    stages = [dict(r) for r in db.execute(text(PIPELINE_REPORT), base_params).mappings()]
    totals = dict(db.execute(text(PIPELINE_TOTALS), base_params).mappings().one())

    # New enrichment queries — all respect optional bd_id
    stage_totals      = [dict(r) for r in db.execute(text(PIPELINE_STAGE_TOTALS), params).mappings()]
    by_bd             = [dict(r) for r in db.execute(text(PIPELINE_BY_BD), params).mappings()]
    by_service        = [dict(r) for r in db.execute(text(PIPELINE_BY_SERVICE), params).mappings()]
    by_account_type   = [dict(r) for r in db.execute(text(PIPELINE_BY_ACCOUNT_TYPE), params).mappings()]
    lead_source       = [dict(r) for r in db.execute(text(PIPELINE_LEAD_SOURCE), params).mappings()]

    if format == "xlsx":
        return _to_excel({"Pipeline": stages}, f"pipeline-Q{quarter}-{year}")

    return {
        "report":               "pipeline",
        "period":               f"Q{quarter} {year}",
        "bd_id":                bd_id,
        "stages":               stages,
        "total_deals":          totals["total_deals"],
        "total_pipeline_value": totals["total_pipeline_value"],
        "stage_totals":         stage_totals,
        "by_bd":                by_bd,
        "by_service":           by_service,
        "by_account_type":      by_account_type,
        "lead_source":          lead_source,
    }


# ── Quota report ──────────────────────────────────────────────────────────────

@router.get("/quota", summary="Quota report — actual vs target per BD")
def quota_report(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter}
    members = [dict(r) for r in db.execute(text(QUOTA_REPORT), params).mappings()]
    totals  = dict(db.execute(text(QUOTA_TEAM_TOTALS), params).mappings().one())
    team_attainment = round(
        totals["team_actual"] / totals["team_quota"] * 100
        if totals["team_quota"] else 0, 1
    )
    if format == "xlsx":
        return _to_excel({"Quota": members}, f"quota-Q{quarter}-{year}")
    return {
        "report":              "quota",
        "period":              f"Q{quarter} {year}",
        "members":             members,
        "team_quota":          totals["team_quota"],
        "team_actual":         totals["team_actual"],
        "team_attainment_pct": team_attainment,
    }


# ── Loss analysis ─────────────────────────────────────────────────────────────

@router.get("/loss-analysis", summary="Loss analysis — Closed Lost deals")
def loss_analysis(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params = {"year": year, "quarter": quarter}
    by_stage = [dict(r) for r in db.execute(text(LOSS_BY_STAGE), params).mappings()]
    deals    = [dict(r) for r in db.execute(text(LOSS_DEALS_LIST), params).mappings()]
    totals   = dict(db.execute(text(LOSS_TOTALS), params).mappings().one())
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

@router.get("/sales-cycle", summary="Sales cycle analysis — average days per stage")
def sales_cycle(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params   = {"year": year, "quarter": quarter}
    by_stage = [dict(r) for r in db.execute(text(SALES_CYCLE_BY_STAGE), params).mappings()]
    total    = dict(db.execute(text(SALES_CYCLE_TOTAL), params).mappings().one())
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

@router.get("/win-rate", summary="Win rate — by lead source, service, and industry")
def win_rate(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    params         = {"year": year, "quarter": quarter}
    overall        = dict(db.execute(text(WIN_RATE_OVERALL), params).mappings().one())
    by_lead_source = [dict(r) for r in db.execute(text(WIN_RATE_BY_LEAD_SOURCE), params).mappings()]
    by_service     = [dict(r) for r in db.execute(text(WIN_RATE_BY_SERVICE), params).mappings()]
    by_industry    = [dict(r) for r in db.execute(text(WIN_RATE_BY_INDUSTRY), params).mappings()]
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