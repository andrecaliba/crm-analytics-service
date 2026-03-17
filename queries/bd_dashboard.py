"""
BD Dashboard queries.
All queries accept :year (int), :quarter (int), :bd_id (str UUID).
Quarter range is computed inside the query using make_date().

Forecast definition:
  sales_forecast = Closed Won revenue (this quarter) + Negotiation stage revenue (open)
  Stage percentages are labels only — no multiplication applied.
"""

# ── Main KPI query ────────────────────────────────────────────────────────────
# Returns: total_revenue, open_pipeline, quota, attainment_pct,
#          sales_forecast, variance, excess_deficit
BD_KPIS = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz          AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                            AS q_end
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
open_pipe AS (
    SELECT COALESCE(SUM(revenue), 0) AS open_pipeline
    FROM deal
    WHERE bd_id = :bd_id AND is_closed = false
),
quota_row AS (
    SELECT COALESCE(MAX(t.quota), 0) AS quota
    FROM target t
    WHERE t.bd_id       = :bd_id
      AND t.period_type = 'QUARTERLY'
),
negotiation AS (
    -- Forecast = Closed Won + Negotiation (the "most likely to close" open deals)
    -- Stage % are labels only — full revenue value used, no multiplication
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
    ROUND(cw.total_revenue / NULLIF(qr.quota, 0) * 100, 1)::float       AS attainment_pct,
    (cw.total_revenue + n.negotiation_revenue)::float                    AS sales_forecast,
    (cw.total_revenue - qr.quota)::float                                 AS variance,
    CASE WHEN cw.total_revenue >= qr.quota THEN 'Excess' ELSE 'Deficit' END AS excess_deficit
FROM closed_won cw, open_pipe op, quota_row qr, negotiation n;
"""

# ── Revenue by month (for chart) ─────────────────────────────────────────────
BD_REVENUE_BY_MONTH = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    EXTRACT(MONTH FROM d.closed_date)::int        AS month,
    TO_CHAR(d.closed_date, 'Mon')                 AS month_name,
    COALESCE(SUM(d.revenue), 0)::float            AS revenue
FROM deal d
CROSS JOIN quarter_range qr
WHERE d.bd_id     = :bd_id
  AND d.is_closed = true
  AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
GROUP BY EXTRACT(MONTH FROM d.closed_date), TO_CHAR(d.closed_date, 'Mon')
ORDER BY month;
"""

# ── Pipeline by stage ─────────────────────────────────────────────────────────
BD_PIPELINE_BY_STAGE = """
SELECT
    ps.name                           AS stage_name,
    COUNT(d.id)::int                  AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM pipeline_stage ps
LEFT JOIN deal d ON d.stage_id = ps.id
    AND d.bd_id    = :bd_id
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