"""
BD Dashboard queries.
All queries accept :year (int), :quarter (int), :bd_id (str UUID).
Quarter range is computed inside the query using make_date().

Forecast definition:
  sales_forecast = Closed Won revenue (this quarter) + Negotiation stage revenue (open)
  Stage percentages are labels only — no multiplication applied.
"""

# ── Main KPI query ────────────────────────────────────────────────────────────
# Returns: total_revenue, open_pipeline, quota, monthly_quota, attainment_pct,
#          sales_forecast, variance, monthly_variance, excess_deficit,
#          monthly_excess_deficit
BD_KPIS = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz          AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                            AS q_end
),
month_range AS (
    SELECT
        date_trunc('month', NOW())::timestamptz                            AS m_start,
        (date_trunc('month', NOW()) + INTERVAL '1 month')::timestamptz     AS m_end
),
closed_won AS (
    SELECT COALESCE(SUM(d.revenue), 0) AS total_revenue
    FROM deal d
    CROSS JOIN quarter_range qr
    WHERE d.bd_id     = :bd_id
      AND d.is_closed = true
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
),
closed_won_mtd AS (
    SELECT COALESCE(SUM(d.revenue), 0) AS mtd_revenue
    FROM deal d
    CROSS JOIN month_range mr
    WHERE d.bd_id     = :bd_id
      AND d.is_closed = true
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
      AND d.closed_date >= mr.m_start
      AND d.closed_date <  mr.m_end
),
open_pipe AS (
    SELECT COALESCE(SUM(revenue), 0) AS open_pipeline
    FROM deal
    WHERE bd_id = :bd_id AND is_closed = false
),
quota_row AS (
    SELECT
        COALESCE(MAX(CASE WHEN t.period_type = 'QUARTERLY' THEN t.quota END), 0) AS quota,
        COALESCE(MAX(CASE WHEN t.period_type = 'MONTHLY'   THEN t.quota END), 0) AS monthly_quota
    FROM target t
    WHERE t.bd_id = :bd_id
),
negotiation AS (
    -- Forecast = Closed Won + Negotiation (stage % are labels only — full revenue used)
    SELECT COALESCE(SUM(d.revenue), 0) AS negotiation_revenue
    FROM deal d
    WHERE d.bd_id     = :bd_id
      AND d.is_closed = false
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Negotiation')
)
SELECT
    cw.total_revenue::float                                              AS total_revenue,
    op.open_pipeline::float                                              AS open_pipeline,
    qr.quota::float                                                      AS quota,
    qr.monthly_quota::float                                              AS monthly_quota,
    ROUND(cw.total_revenue / NULLIF(qr.quota, 0) * 100, 1)::float       AS attainment_pct,
    (cw.total_revenue + n.negotiation_revenue)::float                    AS sales_forecast,
    (cw.total_revenue - qr.quota)::float                                 AS variance,
    (cwm.mtd_revenue - qr.monthly_quota)::float                         AS monthly_variance,
    CASE WHEN cw.total_revenue >= qr.quota THEN 'Excess' ELSE 'Deficit' END
                                                                         AS excess_deficit,
    CASE WHEN cwm.mtd_revenue >= qr.monthly_quota THEN 'Excess' ELSE 'Deficit' END
                                                                         AS monthly_excess_deficit
FROM closed_won cw, closed_won_mtd cwm, open_pipe op, quota_row qr, negotiation n;
"""

# ── Revenue by month (bar chart) ──────────────────────────────────────────────
# Always returns exactly 3 rows — one per month in the selected quarter,
# even if no revenue was closed in that month (revenue = 0).
BD_REVENUE_BY_MONTH = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
),
months AS (
    SELECT generate_series(
        (:quarter - 1) * 3 + 1,
        (:quarter - 1) * 3 + 3
    ) AS month_num
),
closed_by_month AS (
    SELECT
        EXTRACT(MONTH FROM d.closed_date)::int  AS month,
        COALESCE(SUM(d.revenue), 0)::float      AS revenue
    FROM deal d
    CROSS JOIN quarter_range qr
    WHERE d.bd_id     = :bd_id
      AND d.is_closed = true
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
    GROUP BY EXTRACT(MONTH FROM d.closed_date)
)
SELECT
    m.month_num                                            AS month,
    TO_CHAR(make_date(:year, m.month_num, 1), 'Mon')      AS month_name,
    COALESCE(cbm.revenue, 0)::float                       AS revenue,
    -- Monthly quota line reference for the bar chart
    (SELECT COALESCE(MAX(t.quota), 0) FROM target t
     WHERE t.bd_id = :bd_id AND t.period_type = 'MONTHLY')::float  AS quota
FROM months m
LEFT JOIN closed_by_month cbm ON cbm.month = m.month_num
ORDER BY m.month_num;
"""

# ── Pipeline by stage ─────────────────────────────────────────────────────────
BD_PIPELINE_BY_STAGE = """
SELECT
    ps.name                            AS stage_name,
    COUNT(d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM pipeline_stage ps
LEFT JOIN deal d ON d.stage_id = ps.id
    AND d.bd_id     = :bd_id
    AND d.is_closed = false
GROUP BY ps.id, ps.name
ORDER BY ps.id;
"""

# ── Open deals list ───────────────────────────────────────────────────────────
BD_OPEN_DEALS = """
SELECT
    d.id                                                               AS deal_id,
    d.deal_name,
    ps.name                                                            AS stage_name,
    d.revenue::float                                                   AS revenue,
    EXTRACT(DAY FROM NOW() - dal.entered_at)::int                     AS days_in_stage
FROM deal d
JOIN pipeline_stage ps ON ps.id = d.stage_id
LEFT JOIN deal_audit_log dal ON dal.deal_id = d.id AND dal.exited_at IS NULL
WHERE d.bd_id    = :bd_id
  AND d.is_closed = false
ORDER BY d.revenue DESC NULLS LAST;
"""

# ── Service revenue breakdown (pie chart) ────────────────────────────────────
# Closed Won revenue per service for this BD, this quarter.
# Handles single-service deals and bundle deals (proportional via revenue_share_pct).
BD_SERVICE_REVENUE = """
-- Closed Won revenue by service for this BD, filtered to the selected quarter.
-- Deals with no service or bundle assigned are grouped under the service name
-- they were created with. If service_id and bundle_id are both NULL the deal
-- still appears so it is never silently dropped from the pie chart.
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
),
won_deals AS (
    SELECT d.id, d.revenue, d.service_id, d.bundle_id
    FROM deal d
    CROSS JOIN quarter_range qr
    WHERE d.bd_id        = :bd_id
      AND d.is_closed    = true
      AND d.stage_id     = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
),
single_svc AS (
    -- Single-service deals: attribute full revenue to the service
    SELECT
        s.name                 AS service_name,
        SUM(wd.revenue)::float AS revenue,
        COUNT(wd.id)::int      AS deal_count
    FROM won_deals wd
    JOIN service s ON s.id = wd.service_id
    WHERE wd.service_id IS NOT NULL
    GROUP BY s.name
),
bundle_svc AS (
    -- Bundle deals: revenue attributed proportionally via revenue_share_pct
    SELECT
        s.name                                                AS service_name,
        SUM(wd.revenue * bs.revenue_share_pct / 100.0)::float AS revenue,
        COUNT(DISTINCT wd.id)::int                            AS deal_count
    FROM won_deals wd
    JOIN bundle_service bs ON bs.bundle_id = wd.bundle_id
    JOIN service s ON s.id = bs.service_id
    WHERE wd.bundle_id IS NOT NULL
    GROUP BY s.name
),
combined AS (
    SELECT service_name, revenue, deal_count FROM single_svc
    UNION ALL
    SELECT service_name, revenue, deal_count FROM bundle_svc
    UNION ALL
    -- Deals where neither service_id nor bundle_id is set
    SELECT
        'Unassigned'           AS service_name,
        SUM(wd.revenue)::float AS revenue,
        COUNT(wd.id)::int      AS deal_count
    FROM won_deals wd
    WHERE wd.service_id IS NULL
      AND wd.bundle_id  IS NULL
    HAVING COUNT(wd.id) > 0
)
SELECT
    service_name,
    SUM(revenue)::float  AS revenue,
    SUM(deal_count)::int AS deal_count
FROM combined
GROUP BY service_name
ORDER BY revenue DESC;
"""

# ── Account type breakdown on open pipeline ───────────────────────────────────
# Count and value of open deals per client account type for this BD.
BD_ACCOUNT_TYPE_PIPELINE = """
SELECT
    INITCAP(c.account_type::text) AS account_type,
    COUNT(d.id)::int              AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM deal d
JOIN client c ON c.id = d.client_id
WHERE d.bd_id     = :bd_id
  AND d.is_closed = false
GROUP BY c.account_type
ORDER BY deal_count DESC;
"""

# ── Lead source breakdown ─────────────────────────────────────────────────────
# Deals for this BD grouped by lead source.
# Returns total deal count, won count, and won revenue per source (this quarter).
BD_LEAD_SOURCE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    INITCAP(d.lead_source::text)                                             AS lead_source,
    COUNT(d.id)::int                                                         AS total_deals,
    COUNT(CASE
        WHEN d.is_closed = true
         AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
        THEN 1 END)::int                                                     AS won_deals,
    COALESCE(SUM(CASE
        WHEN d.is_closed = true
         AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
         AND d.closed_date >= qr.q_start
         AND d.closed_date <  qr.q_end
        THEN d.revenue ELSE 0
    END), 0)::float                                                          AS won_revenue
FROM deal d
CROSS JOIN quarter_range qr
WHERE d.bd_id = :bd_id
GROUP BY d.lead_source
ORDER BY won_revenue DESC;
"""

# ── Follow-up metrics ─────────────────────────────────────────────────────────
# Summary of open deals requiring follow-up action for this BD.
BD_FOLLOW_UP = """
SELECT
    COUNT(d.id)::int                                                          AS total_open,
    COUNT(CASE
        WHEN d.action_plan_due_date IS NOT NULL
         AND d.action_plan_due_date < NOW()::date
        THEN 1 END)::int                                                      AS overdue_action_plans,
    COUNT(CASE
        WHEN d.last_follow_up_at IS NOT NULL
         AND d.last_follow_up_at < NOW() - INTERVAL '14 days'
        THEN 1 END)::int                                                      AS overdue_follow_ups,
    COUNT(CASE
        WHEN d.action_plan_due_date IS NOT NULL
         AND d.action_plan_due_date >= NOW()::date
         AND d.action_plan_due_date <= (NOW()::date + INTERVAL '3 days')
        THEN 1 END)::int                                                      AS upcoming_action_plans
FROM deal d
WHERE d.bd_id    = :bd_id
  AND d.is_closed = false;
"""