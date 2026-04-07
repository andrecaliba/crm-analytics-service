from typing import Optional
from datetime import datetime, timezone
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

MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _month_start(value) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime(value.year, value.month, 1, tzinfo=timezone.utc)


def _add_months(value: datetime, months: int) -> datetime:
    total_months = (value.year * 12 + (value.month - 1)) + months
    year = total_months // 12
    month = (total_months % 12) + 1
    return datetime(year, month, 1, tzinfo=timezone.utc)


def _month_key(year: int, month: int) -> str:
    return f"{year}-{month:02d}"


def _month_label(year: int, month: int) -> str:
    return f"{MONTH_LABELS[month - 1]} {year}"


def _scope_months(year: Optional[int], quarter: Optional[int]) -> set[str]:
    resolved_year = year or datetime.now(timezone.utc).year
    if quarter:
        months = [((quarter - 1) * 3) + 1, ((quarter - 1) * 3) + 2, ((quarter - 1) * 3) + 3]
    else:
        months = list(range(1, 13))
    return {_month_key(resolved_year, month) for month in months}


def _collections_dataset(db: Session, year: Optional[int], quarter: Optional[int], bd_id: Optional[str]):
    deal_params = {}
    bd_filter = ""
    if bd_id:
        deal_params["bd_id"] = bd_id
        bd_filter = "AND d.bd_id = :bd_id"

    deals_sql = f"""
    SELECT
        d.id AS deal_id,
        d.deal_name,
        d.monthly_subscription::float AS monthly_subscription,
        COALESCE(d.revenue, d.monthly_subscription * d.duration)::float AS booked_revenue,
        d.duration,
        d.start_date,
        d.closed_date,
        d.terminated_at,
        c.name AS client_name,
        COALESCE(c.account_type::text, 'Unknown') AS account_type,
        b.id AS bd_id,
        b.first_name || ' ' || b.last_name AS bd_name
    FROM deal d
    JOIN pipeline_stage ps ON ps.id = d.stage_id
    JOIN client c ON c.id = d.client_id
    JOIN bd b ON b.id = d.bd_id
    WHERE (ps.name = 'Closed Won' OR d.is_closed = true)
      {bd_filter}
    ORDER BY d.closed_date DESC NULLS LAST, d.deal_name ASC
    """
    deals = [dict(row) for row in db.execute(text(deals_sql), deal_params).mappings()]

    payments_sql = f"""
    SELECT
        p.id,
        p.deal_id,
        p.amount::float AS amount,
        dd.year,
        dd.month,
        dd.quarter
    FROM payment p
    JOIN deal d ON d.id = p.deal_id
    LEFT JOIN date_dimension dd ON dd.id = p.date_id
    WHERE 1 = 1
      {bd_filter}
    """
    payments = [dict(row) for row in db.execute(text(payments_sql), deal_params).mappings()]

    payments_by_deal: dict[str, list[dict]] = {}
    for payment in payments:
        payments_by_deal.setdefault(payment["deal_id"], []).append(payment)

    scope = _scope_months(year, quarter)
    now_month = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    summary = {
        "bookedRevenue": 0.0,
        "expectedRevenue": 0.0,
        "collectedRevenue": 0.0,
        "overdueRevenue": 0.0,
        "outstandingRevenue": 0.0,
        "collectionRate": 0.0,
    }
    trend_map: dict[str, dict] = {}
    by_bd: dict[str, dict] = {}
    by_account: dict[str, dict] = {}
    overdue_accounts: list[dict] = []

    for deal in deals:
        payment_map: dict[str, float] = {}
        deal_collected = 0.0
        deal_collected_in_scope = 0.0
        last_paid_month = None
        next_due_month = None
        overdue_value = 0.0
        expected_value = 0.0
        counts_toward_booked = False

        deal_payments = payments_by_deal.get(deal["deal_id"], [])
        for payment in deal_payments:
            year_value = payment.get("year")
            month_value = payment.get("month")
            amount = float(payment.get("amount") or 0)
            if year_value and month_value:
                key = _month_key(int(year_value), int(month_value))
                payment_map[key] = payment_map.get(key, 0.0) + amount
                if key in scope:
                    counts_toward_booked = True
                    trend = trend_map.setdefault(key, {
                        "monthKey": key,
                        "label": _month_label(int(year_value), int(month_value)),
                        "bookedRevenue": 0.0,
                        "expectedRevenue": 0.0,
                        "collectedRevenue": 0.0,
                    })
                    trend["collectedRevenue"] += amount
                    summary["collectedRevenue"] += amount
                    deal_collected_in_scope += amount
                deal_collected += amount
                last_paid_month = _month_label(int(year_value), int(month_value))

        start_date = deal.get("start_date")
        terminated_at = deal.get("terminated_at")
        termination_month = _month_start(terminated_at) if terminated_at else None
        if start_date:
            start_month = _month_start(start_date)
            for offset in range(int(deal.get("duration") or 0)):
                due_month = _add_months(start_month, offset)
                if termination_month and due_month > termination_month:
                    break
                key = _month_key(due_month.year, due_month.month)
                paid_amount = payment_map.get(key, 0.0)
                remaining = max(float(deal["monthly_subscription"] or 0) - paid_amount, 0.0)

                if key in scope:
                    trend = trend_map.setdefault(key, {
                        "monthKey": key,
                        "label": _month_label(due_month.year, due_month.month),
                        "bookedRevenue": 0.0,
                        "expectedRevenue": 0.0,
                        "collectedRevenue": 0.0,
                    })
                    trend["expectedRevenue"] += float(deal["monthly_subscription"] or 0)
                    summary["expectedRevenue"] += float(deal["monthly_subscription"] or 0)
                    expected_value += float(deal["monthly_subscription"] or 0)

                if due_month <= now_month and remaining > 0 and not next_due_month:
                    next_due_month = _month_label(due_month.year, due_month.month)
                if due_month < now_month and remaining > 0:
                    overdue_value += remaining

        closed_date = deal.get("closed_date")
        if closed_date and counts_toward_booked:
            closed_month = _month_start(closed_date)
            closed_key = _month_key(closed_month.year, closed_month.month)
            if closed_key in scope:
                trend = trend_map.setdefault(closed_key, {
                    "monthKey": closed_key,
                    "label": _month_label(closed_month.year, closed_month.month),
                    "bookedRevenue": 0.0,
                    "expectedRevenue": 0.0,
                    "collectedRevenue": 0.0,
                })
                trend["bookedRevenue"] += float(deal["booked_revenue"] or 0)
                summary["bookedRevenue"] += float(deal["booked_revenue"] or 0)

        bd_entry = by_bd.setdefault(deal["bd_id"], {
            "id": deal["bd_id"],
            "name": deal["bd_name"],
            "bookedRevenue": 0.0,
            "expectedRevenue": 0.0,
            "collectedRevenue": 0.0,
            "overdueRevenue": 0.0,
        })
        bd_entry["bookedRevenue"] += float(deal["booked_revenue"] or 0) if counts_toward_booked else 0.0
        bd_entry["expectedRevenue"] += expected_value
        bd_entry["collectedRevenue"] += deal_collected_in_scope
        bd_entry["overdueRevenue"] += overdue_value

        account_entry = by_account.setdefault(deal["account_type"], {
            "id": deal["account_type"],
            "name": deal["account_type"],
            "bookedRevenue": 0.0,
            "expectedRevenue": 0.0,
            "collectedRevenue": 0.0,
            "overdueRevenue": 0.0,
        })
        account_entry["bookedRevenue"] += float(deal["booked_revenue"] or 0) if counts_toward_booked else 0.0
        account_entry["expectedRevenue"] += expected_value
        account_entry["collectedRevenue"] += deal_collected_in_scope
        account_entry["overdueRevenue"] += overdue_value

        summary["overdueRevenue"] += overdue_value
        summary["outstandingRevenue"] += max(expected_value - deal_collected_in_scope, 0.0)

        if overdue_value > 0 or max(expected_value - deal_collected_in_scope, 0.0) > 0:
            overdue_accounts.append({
                "dealId": deal["deal_id"],
                "dealName": deal["deal_name"],
                "clientName": deal["client_name"],
                "bdName": deal["bd_name"],
                "expectedRevenue": expected_value,
                "collectedRevenue": deal_collected_in_scope,
                "overdueRevenue": overdue_value,
                "lastPaidMonth": last_paid_month,
                "nextDueMonth": next_due_month,
            })

    if summary["expectedRevenue"]:
        summary["collectionRate"] = round((summary["collectedRevenue"] / summary["expectedRevenue"]) * 100, 1)

    return {
        "summary": summary,
        "monthlyTrend": sorted(trend_map.values(), key=lambda item: item["monthKey"]),
        "byBd": sorted(by_bd.values(), key=lambda item: item["collectedRevenue"], reverse=True),
        "byAccount": sorted(by_account.values(), key=lambda item: item["collectedRevenue"], reverse=True),
        "overdueAccounts": sorted(overdue_accounts, key=lambda item: item["overdueRevenue"], reverse=True)[:12],
    }


def _parse_csv_numbers(value: Optional[str]) -> list[int]:
    if not value:
        return []
    parsed: list[int] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            parsed.append(int(item))
        except ValueError:
            continue
    return sorted(set(parsed))


def _build_periods(years: list[int], quarters: list[int]) -> list[tuple[int, int]]:
    chosen_years = years or []
    chosen_quarters = quarters or [1, 2, 3, 4]
    return [(year, quarter) for year in chosen_years for quarter in chosen_quarters]


def _build_label(years: list[int], quarters: list[int]) -> str:
    year_label = ", ".join(str(year) for year in years) if years else "No years"
    quarter_label = "All Quarters" if not quarters or len(quarters) == 4 else ", ".join(f"Q{quarter}" for quarter in quarters)
    return f"{year_label} · {quarter_label}"


def _get_snapshot(db: Session, years: list[int], quarters: list[int], bd_id: Optional[str]):
    periods = _build_periods(years, quarters)
    label = _build_label(years, quarters)

    quota = 0.0
    actual = 0.0
    pipeline_value = 0.0
    open_deals = 0
    wins = 0
    losses = 0
    sample_size = 0
    weighted_cycle_days = 0.0
    longest_cycle_days = None
    lost_deals = 0
    lost_value = 0.0

    service_revenue: dict[str, dict] = {}
    account_revenue: dict[str, dict] = {}
    lead_source_performance: dict[str, dict] = {}
    stage_cycle: dict[str, dict] = {}

    bd_filter = "AND d.bd_id = :bd_id" if bd_id else ""
    target_bd_filter = "AND t.bd_id = :bd_id" if bd_id else ""
    audit_bd_filter = "AND d.bd_id = :bd_id" if bd_id else ""

    for period_year, period_quarter in periods:
        params = {"year": period_year, "quarter": period_quarter}
        if bd_id:
            params["bd_id"] = bd_id

        quota_sql = f"""
        SELECT COALESCE(SUM(t.quota), 0)::float AS quota
        FROM target t
        JOIN date_dimension dd ON dd.id = t.date_id
        WHERE t.period_type = 'QUARTERLY'
          AND dd.year = :year
          AND dd.quarter = :quarter
          {target_bd_filter}
        """
        quota += float(db.execute(text(quota_sql), params).scalar() or 0)

        closed_won_sql = f"""
        WITH quarter_range AS (
            SELECT
                make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
                (make_date(:year, (:quarter - 1) * 3 + 1, 1) + INTERVAL '3 months')::timestamptz AS q_end
        )
        SELECT
            COALESCE(s.name, b.name, 'Unknown')                         AS service_name,
            COALESCE(c.account_type::text, 'Unknown')                  AS account_type,
            COALESCE(d.lead_source::text, 'UNKNOWN')                   AS lead_source,
            COALESCE(SUM(d.revenue), 0)::float                         AS revenue,
            COUNT(d.id)::int                                           AS deal_count
        FROM deal d
        JOIN pipeline_stage ps ON ps.id = d.stage_id
        JOIN client c ON c.id = d.client_id
        LEFT JOIN service s ON s.id = d.service_id
        LEFT JOIN bundle b ON b.id = d.bundle_id
        CROSS JOIN quarter_range qr
        WHERE d.is_closed = true
          AND ps.name = 'Closed Won'
          AND d.closed_date >= qr.q_start
          AND d.closed_date < qr.q_end
          {bd_filter}
        GROUP BY COALESCE(s.name, b.name, 'Unknown'), COALESCE(c.account_type::text, 'Unknown'), COALESCE(d.lead_source::text, 'UNKNOWN')
        """
        for row in db.execute(text(closed_won_sql), params).mappings():
            revenue = float(row["revenue"] or 0)
            deal_count = int(row["deal_count"] or 0)
            actual += revenue
            wins += deal_count

            service_name = row["service_name"]
            service_entry = service_revenue.setdefault(service_name, {"name": service_name, "value": 0.0, "deals": 0})
            service_entry["value"] += revenue
            service_entry["deals"] += deal_count

            account_type = row["account_type"]
            account_entry = account_revenue.setdefault(account_type, {"name": account_type, "value": 0.0, "deals": 0})
            account_entry["value"] += revenue
            account_entry["deals"] += deal_count

            lead_source = row["lead_source"]
            lead_entry = lead_source_performance.setdefault(lead_source, {
                "source": lead_source,
                "value": 0.0,
                "deals": 0,
                "wins": 0,
                "losses": 0,
                "winRate": 0.0,
            })
            lead_entry["value"] += revenue
            lead_entry["deals"] += deal_count
            lead_entry["wins"] += deal_count

        closed_lost_sql = f"""
        WITH quarter_range AS (
            SELECT
                make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
                (make_date(:year, (:quarter - 1) * 3 + 1, 1) + INTERVAL '3 months')::timestamptz AS q_end
        )
        SELECT
            COALESCE(d.lead_source::text, 'UNKNOWN') AS lead_source,
            COUNT(d.id)::int                         AS deal_count,
            COALESCE(SUM(COALESCE(d.final_proposed_value, d.revenue)), 0)::float AS lost_value
        FROM deal d
        JOIN pipeline_stage ps ON ps.id = d.stage_id
        CROSS JOIN quarter_range qr
        WHERE d.is_closed = true
          AND ps.name = 'Closed Lost'
          AND d.closed_date >= qr.q_start
          AND d.closed_date < qr.q_end
          {bd_filter}
        GROUP BY COALESCE(d.lead_source::text, 'UNKNOWN')
        """
        for row in db.execute(text(closed_lost_sql), params).mappings():
            deal_count = int(row["deal_count"] or 0)
            loss_value = float(row["lost_value"] or 0)
            losses += deal_count
            lost_deals += deal_count
            lost_value += loss_value

            lead_source = row["lead_source"]
            lead_entry = lead_source_performance.setdefault(lead_source, {
                "source": lead_source,
                "value": 0.0,
                "deals": 0,
                "wins": 0,
                "losses": 0,
                "winRate": 0.0,
            })
            lead_entry["deals"] += deal_count
            lead_entry["losses"] += deal_count

        pipeline_sql = f"""
        WITH quarter_range AS (
            SELECT
                (make_date(:year, (:quarter - 1) * 3 + 1, 1) + INTERVAL '3 months')::timestamptz AS q_end
        )
        SELECT
            COUNT(d.id)::int AS open_deals,
            COALESCE(SUM(d.revenue), 0)::float AS pipeline_value
        FROM deal d
        CROSS JOIN quarter_range qr
        WHERE d.start_date <= qr.q_end
          AND (d.closed_date IS NULL OR d.closed_date > qr.q_end)
          {bd_filter}
        """
        pipeline_row = db.execute(text(pipeline_sql), params).mappings().one()
        open_deals += int(pipeline_row["open_deals"] or 0)
        pipeline_value += float(pipeline_row["pipeline_value"] or 0)

        cycle_total_sql = f"""
        WITH quarter_range AS (
            SELECT
                make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
                (make_date(:year, (:quarter - 1) * 3 + 1, 1) + INTERVAL '3 months')::timestamptz AS q_end
        )
        SELECT
            COUNT(d.id)::int AS sample_size,
            COALESCE(SUM(d.sales_cycle_days), 0)::float AS total_cycle_days,
            MAX(d.sales_cycle_days)::int AS max_cycle_days
        FROM deal d
        JOIN pipeline_stage ps ON ps.id = d.stage_id
        CROSS JOIN quarter_range qr
        WHERE d.is_closed = true
          AND d.sales_cycle_days IS NOT NULL
          AND ps.name = 'Closed Won'
          AND d.closed_date >= qr.q_start
          AND d.closed_date < qr.q_end
          {bd_filter}
        """
        cycle_total = db.execute(text(cycle_total_sql), params).mappings().one()
        sample_size += int(cycle_total["sample_size"] or 0)
        weighted_cycle_days += float(cycle_total["total_cycle_days"] or 0)
        max_cycle = cycle_total["max_cycle_days"]
        if max_cycle is not None:
            longest_cycle_days = max(int(max_cycle), longest_cycle_days or int(max_cycle))

        cycle_stage_sql = f"""
        WITH quarter_range AS (
            SELECT
                make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
                (make_date(:year, (:quarter - 1) * 3 + 1, 1) + INTERVAL '3 months')::timestamptz AS q_end
        )
        SELECT
            ps.name AS stage,
            COALESCE(SUM(dal.days_in_stage), 0)::float AS total_days,
            COUNT(*)::int AS sample_size
        FROM deal_audit_log dal
        JOIN pipeline_stage ps ON ps.id = dal.stage_id
        JOIN deal d ON d.id = dal.deal_id
        CROSS JOIN quarter_range qr
        WHERE dal.exited_at IS NOT NULL
          AND dal.days_in_stage IS NOT NULL
          AND dal.exited_at >= qr.q_start
          AND dal.exited_at < qr.q_end
          {audit_bd_filter}
        GROUP BY ps.name
        """
        for row in db.execute(text(cycle_stage_sql), params).mappings():
            stage_name = row["stage"]
            stage_entry = stage_cycle.setdefault(stage_name, {"stage": stage_name, "totalDays": 0.0, "count": 0})
            stage_entry["totalDays"] += float(row["total_days"] or 0)
            stage_entry["count"] += int(row["sample_size"] or 0)

    lead_source_items = []
    for item in lead_source_performance.values():
        total_closed = item["wins"] + item["losses"]
        item["winRate"] = round((item["wins"] / total_closed) * 100, 1) if total_closed else 0.0
        lead_source_items.append(item)

    stage_cycle_items = []
    for item in stage_cycle.values():
        avg_days = item["totalDays"] / item["count"] if item["count"] else 0.0
        stage_cycle_items.append({"stage": item["stage"], "avgDays": round(avg_days, 1)})

    return {
        "label": label,
        "periods": [{"year": y, "quarter": q} for y, q in periods],
        "quota": quota,
        "actual": actual,
        "attainmentPct": round((actual / quota) * 100, 1) if quota else 0.0,
        "pipelineValue": pipeline_value,
        "openDeals": open_deals,
        "wins": wins,
        "losses": losses,
        "winRate": round((wins / (wins + losses)) * 100, 1) if (wins + losses) else 0.0,
        "avgSalesCycleDays": round(weighted_cycle_days / sample_size, 1) if sample_size else None,
        "longestCycleDays": longest_cycle_days,
        "sampleSize": sample_size,
        "lostDeals": lost_deals,
        "lostValue": lost_value,
        "serviceRevenue": sorted(service_revenue.values(), key=lambda item: item["value"], reverse=True)[:6],
        "accountRevenue": sorted(account_revenue.values(), key=lambda item: item["value"], reverse=True)[:6],
        "leadSourcePerformance": sorted(lead_source_items, key=lambda item: item["value"], reverse=True)[:6],
        "stageCycle": sorted(stage_cycle_items, key=lambda item: item["avgDays"], reverse=True),
    }


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


@router.get(
    "/collections-overview",
    summary="Collections reporting — booked revenue versus expected and collected subscription receipts",
)
def collections_overview(
    year: Optional[int] = Query(None),
    quarter: Optional[int] = Query(None, ge=1, le=4),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    scoped_bd_id = bd_id
    if user["role"] != "SALES_MANAGER":
        scoped_bd_id = user["bd_id"]

    return _collections_dataset(
        db=db,
        year=year,
        quarter=quarter,
        bd_id=scoped_bd_id,
    )


@router.get(
    "/growth-comparison",
    summary="Growth comparison — side-by-side period analytics snapshots",
)
def growth_comparison(
    leftYear: Optional[int] = Query(None),
    leftQuarter: Optional[int] = Query(None, ge=1, le=4),
    leftYears: Optional[str] = Query(None, description="Comma-separated years for quarter mode"),
    leftQuarters: Optional[str] = Query(None, description="Comma-separated quarters"),
    rightYear: Optional[int] = Query(None),
    rightQuarter: Optional[int] = Query(None, ge=1, le=4),
    rightYears: Optional[str] = Query(None, description="Comma-separated years for quarter mode"),
    rightQuarters: Optional[str] = Query(None, description="Comma-separated quarters"),
    bd_id: Optional[str] = Query(None, description="Optional BD filter"),
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    scoped_bd_id = bd_id
    if user["role"] != "SALES_MANAGER":
        scoped_bd_id = user["bd_id"]

    resolved_left_years = _parse_csv_numbers(leftYears)
    resolved_right_years = _parse_csv_numbers(rightYears)
    resolved_left_quarters = _parse_csv_numbers(leftQuarters)
    resolved_right_quarters = _parse_csv_numbers(rightQuarters)

    if leftYear is not None and leftYear not in resolved_left_years:
        resolved_left_years.append(leftYear)
    if rightYear is not None and rightYear not in resolved_right_years:
        resolved_right_years.append(rightYear)
    if leftQuarter is not None and leftQuarter not in resolved_left_quarters:
        resolved_left_quarters.append(leftQuarter)
    if rightQuarter is not None and rightQuarter not in resolved_right_quarters:
        resolved_right_quarters.append(rightQuarter)

    resolved_left_years = sorted(set(resolved_left_years))
    resolved_right_years = sorted(set(resolved_right_years))
    resolved_left_quarters = sorted(set(resolved_left_quarters))
    resolved_right_quarters = sorted(set(resolved_right_quarters))

    if not resolved_left_years or not resolved_right_years:
        return {"error": "leftYears and rightYears must include at least one real year"}

    left = _get_snapshot(
        db=db,
        years=resolved_left_years,
        quarters=resolved_left_quarters,
        bd_id=scoped_bd_id,
    )
    right = _get_snapshot(
        db=db,
        years=resolved_right_years,
        quarters=resolved_right_quarters,
        bd_id=scoped_bd_id,
    )

    return {"left": left, "right": right}


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