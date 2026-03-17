from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from io import BytesIO
import openpyxl

from db import get_db
from auth import require_manager
from queries.reports import (
    PIPELINE_REPORT, PIPELINE_TOTALS,
    QUOTA_REPORT, QUOTA_TEAM_TOTALS,
    LOSS_BY_STAGE, LOSS_DEALS_LIST, LOSS_TOTALS,
    SALES_CYCLE_BY_STAGE, SALES_CYCLE_TOTAL,
    WIN_RATE_BY_LEAD_SOURCE, WIN_RATE_BY_SERVICE,
    WIN_RATE_BY_INDUSTRY, WIN_RATE_OVERALL,
)

router = APIRouter(prefix="/api/analytics/reports", tags=["Reports"])


# ── Excel helper ──────────────────────────────────────────────────────────────

def _to_excel(sheets: dict, filename: str) -> StreamingResponse:
    """
    sheets = { "Sheet Name": [{"col": val, ...}, ...] }
    Returns a StreamingResponse that downloads as .xlsx
    """
    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # remove default empty sheet

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
    summary="Pipeline report — open deals by stage",
    description="""
Returns the current open pipeline broken down by stage.
Note: this report reflects the **current** state, not a historical snapshot.
The `year` and `quarter` params are accepted for API consistency but
pipeline is always the live open deal list.

Use `?format=xlsx` to download as Excel.
""",
)
def pipeline_report(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json", description="Response format: 'json' or 'xlsx'"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_manager),
):
    params = {"year": year, "quarter": quarter}
    stages = [dict(r) for r in db.execute(text(PIPELINE_REPORT), params).mappings()]
    totals = dict(db.execute(text(PIPELINE_TOTALS), params).mappings().one())

    if format == "xlsx":
        return _to_excel({"Pipeline": stages}, f"pipeline-Q{quarter}-{year}")

    return {
        "report":               "pipeline",
        "period":               f"Q{quarter} {year}",
        "stages":               stages,
        "total_deals":          totals["total_deals"],
        "total_pipeline_value": totals["total_pipeline_value"],
    }


# ── Quota report ──────────────────────────────────────────────────────────────

@router.get(
    "/quota",
    summary="Quota report — actual vs target per BD",
    description="""
Returns each BD rep's actual Closed Won revenue vs their quarterly quota.

Status values:
- `Exceeded` — actual >= quota
- `On Track` — actual >= 80% of quota
- `Behind` — actual < 80% of quota

Use `?format=xlsx` to download as Excel.
""",
)
def quota_report(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_manager),
):
    params = {"year": year, "quarter": quarter}
    members = [dict(r) for r in db.execute(text(QUOTA_REPORT), params).mappings()]
    totals = dict(db.execute(text(QUOTA_TEAM_TOTALS), params).mappings().one())
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

@router.get(
    "/loss-analysis",
    summary="Loss analysis — Closed Lost deals by stage and BD",
    description="""
Analyses all Closed Lost deals in the period. Shows:
- Which stages deals were lost from most often
- List of individual lost deals with BD name, final value, and last remarks

Use `?format=xlsx` to download as Excel (two sheets: Summary and Deals).
""",
)
def loss_analysis(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_manager),
):
    params = {"year": year, "quarter": quarter}
    by_stage = [dict(r) for r in db.execute(text(LOSS_BY_STAGE), params).mappings()]
    deals = [dict(r) for r in db.execute(text(LOSS_DEALS_LIST), params).mappings()]
    totals = dict(db.execute(text(LOSS_TOTALS), params).mappings().one())

    if format == "xlsx":
        return _to_excel(
            {"By Stage": by_stage, "Lost Deals": deals},
            f"loss-analysis-Q{quarter}-{year}",
        )

    return {
        "report":            "loss_analysis",
        "period":            f"Q{quarter} {year}",
        "total_lost_deals":  totals["total_lost_deals"],
        "total_lost_value":  totals["total_lost_value"],
        "by_stage":          by_stage,
        "deals":             deals,
    }


# ── Sales cycle ───────────────────────────────────────────────────────────────

@router.get(
    "/sales-cycle",
    summary="Sales cycle analysis — average days per stage",
    description="""
Shows how long deals spend in each pipeline stage on average.
Data comes from deal_audit_log — only completed stage transitions
(where exited_at is not null) are included.

Use `?format=xlsx` to download as Excel.
""",
)
def sales_cycle(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_manager),
):
    params = {"year": year, "quarter": quarter}
    by_stage = [dict(r) for r in db.execute(text(SALES_CYCLE_BY_STAGE), params).mappings()]
    total = dict(db.execute(text(SALES_CYCLE_TOTAL), params).mappings().one())

    if format == "xlsx":
        return _to_excel({"Sales Cycle": by_stage}, f"sales-cycle-Q{quarter}-{year}")

    return {
        "report":                "sales_cycle",
        "period":                f"Q{quarter} {year}",
        "avg_total_cycle_days":  total["avg_total_cycle_days"],
        "max_cycle_days":        total["max_cycle_days"],
        "sample_size":           total["sample_size"],
        "by_stage":              by_stage,
    }


# ── Win rate ──────────────────────────────────────────────────────────────────

@router.get(
    "/win-rate",
    summary="Win rate — by lead source, service, and industry",
    description="""
Calculates win rate (Closed Won / all closed) broken down three ways:
- By lead source (INBOUND / OUTBOUND / REFERRAL)
- By service or bundle name
- By client industry

Use `?format=xlsx` to download as Excel (three sheets).
""",
)
def win_rate(
    year: int = Query(...),
    quarter: int = Query(..., ge=1, le=4),
    format: str = Query("json"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_manager),
):
    params = {"year": year, "quarter": quarter}
    overall = dict(db.execute(text(WIN_RATE_OVERALL), params).mappings().one())
    by_lead_source = [dict(r) for r in db.execute(text(WIN_RATE_BY_LEAD_SOURCE), params).mappings()]
    by_service = [dict(r) for r in db.execute(text(WIN_RATE_BY_SERVICE), params).mappings()]
    by_industry = [dict(r) for r in db.execute(text(WIN_RATE_BY_INDUSTRY), params).mappings()]

    if format == "xlsx":
        return _to_excel(
            {
                "By Lead Source": by_lead_source,
                "By Service":     by_service,
                "By Industry":    by_industry,
            },
            f"win-rate-Q{quarter}-{year}",
        )

    return {
        "report":          "win_rate",
        "period":          f"Q{quarter} {year}",
        "overall_win_rate": overall["overall_win_rate"],
        "by_lead_source":  by_lead_source,
        "by_service":      by_service,
        "by_industry":     by_industry,
    }
