"""
Executive Dashboard queries.
All queries accept :year (int) and :quarter (int).
Manager-only — enforced at the route level.

Forecast definition:
  sales_forecast = Closed Won revenue (this quarter) + Negotiation stage revenue (all open)
  Stage percentages are labels only — no multiplication applied.
"""

# ── Team KPIs ─────────────────────────────────────────────────────────────────
EXEC_TEAM_KPIS = """
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
    WHERE d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
      AND COALESCE(d.contract_status::text, 'ACTIVE') <> 'TERMINATED'
      AND COALESCE(d.start_date, d.closed_date, NOW()) < qr.q_end
      AND COALESCE(d.terminated_at, d.due_date, d.start_date, d.closed_date, NOW()) >= qr.q_start
),
team_quota AS (
    SELECT COALESCE(SUM(t.quota), 0) AS total_quota
    FROM target t
    JOIN date_dimension dd ON dd.id = t.date_id
    JOIN bd b ON b.id = t.bd_id
    WHERE t.period_type = 'QUARTERLY'
      AND dd.year = :year
      AND dd.quarter = :quarter
      AND b.role = 'BD_REP'
),
negotiation AS (
    SELECT COALESCE(SUM(d.revenue), 0) AS negotiation_revenue
    FROM deal d
    WHERE d.is_closed = false
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Negotiation')
)
SELECT
    cw.total_revenue::float                                              AS total_revenue,
    tq.total_quota::float                                                AS total_quota,
    (cw.total_revenue + n.negotiation_revenue)::float                    AS sales_forecast,
    ROUND(cw.total_revenue / NULLIF(tq.total_quota, 0) * 100, 1)::float AS attainment_pct
FROM closed_won cw, team_quota tq, negotiation n;
"""

# ── Leaderboard ───────────────────────────────────────────────────────────────
EXEC_LEADERBOARD = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
),
bd_quota AS (
    SELECT bd_id, COALESCE(MAX(quota), 0) AS quota
    FROM target t
    JOIN date_dimension dd ON dd.id = t.date_id
    WHERE period_type = 'QUARTERLY'
      AND dd.year = :year
      AND dd.quarter = :quarter
    GROUP BY bd_id
),
bd_revenue AS (
    SELECT
        d.bd_id,
        COALESCE(SUM(d.revenue), 0) AS revenue
    FROM deal d
    CROSS JOIN quarter_range qr
    WHERE COALESCE(d.start_date, d.closed_date, NOW()) < qr.q_end
      AND COALESCE(d.contract_status::text, 'ACTIVE') <> 'TERMINATED'
      AND COALESCE(d.terminated_at, d.due_date, d.start_date, d.closed_date, NOW()) >= qr.q_start
      AND d.stage_id = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
    GROUP BY d.bd_id
),
bd_closures AS (
    SELECT
        d.bd_id,
        COUNT(CASE WHEN ps.name = 'Closed Won' THEN 1 END) AS won_count,
        COUNT(CASE WHEN d.is_closed = true THEN 1 END)     AS closed_count
    FROM deal d
    JOIN pipeline_stage ps ON ps.id = d.stage_id
    GROUP BY d.bd_id
)
SELECT
    b.id                                                                   AS bd_id,
    b.first_name,
    b.last_name,
    b.role,
    COALESCE(br.revenue, 0)::float                                         AS revenue,
    COALESCE(q.quota, 0)::float                                            AS quota,
    ROUND(
        COALESCE(br.revenue, 0) / NULLIF(COALESCE(q.quota, 0), 0) * 100, 1
    )::float                                                               AS attainment_pct,
    ROUND(
        100.0 * COALESCE(bc.won_count, 0)
        / NULLIF(COALESCE(bc.closed_count, 0), 0), 1
    )::float                                                               AS win_rate,
    RANK() OVER (
        ORDER BY COALESCE(br.revenue, 0) DESC
    )::int                                                                 AS rank
FROM bd b
LEFT JOIN bd_revenue br ON br.bd_id = b.id
LEFT JOIN bd_closures bc ON bc.bd_id = b.id
LEFT JOIN bd_quota  q  ON q.bd_id  = b.id
WHERE b.role = 'BD_REP'
ORDER BY revenue DESC;
"""

# ── Stuck deals ───────────────────────────────────────────────────────────────
EXEC_STUCK_DEALS = """
SELECT
    d.id                                                            AS deal_id,
    d.deal_name,
    ps.name                                                         AS stage_name,
    EXTRACT(DAY FROM NOW() - dal.entered_at)::int                  AS days_in_stage,
    ps.duration                                                     AS stage_duration_threshold,
    b.first_name,
    b.last_name
FROM deal d
JOIN pipeline_stage ps   ON ps.id      = d.stage_id
JOIN deal_audit_log dal  ON dal.deal_id = d.id AND dal.exited_at IS NULL
JOIN bd b                ON b.id       = d.bd_id
WHERE d.is_closed   = false
  AND ps.duration   IS NOT NULL
  AND EXTRACT(DAY FROM NOW() - dal.entered_at) > ps.duration
ORDER BY days_in_stage DESC;
"""

# ── Pipeline by stage (team-wide) ─────────────────────────────────────────────
EXEC_PIPELINE_BY_STAGE = """
SELECT
    ps.name                            AS stage_name,
    COUNT(d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float AS total_value
FROM pipeline_stage ps
LEFT JOIN deal d ON d.stage_id = ps.id AND d.is_closed = false
GROUP BY ps.id, ps.name
ORDER BY ps.id;
"""

# ── By account type ───────────────────────────────────────────────────────────
EXEC_BY_ACCOUNT_TYPE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    c.account_type::text                        AS account_type,
    COUNT(DISTINCT d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float          AS revenue
FROM deal d
JOIN client c ON c.id = d.client_id
CROSS JOIN quarter_range qr
WHERE d.stage_id = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
  AND COALESCE(d.contract_status::text, 'ACTIVE') <> 'TERMINATED'
  AND COALESCE(d.start_date, d.closed_date, NOW()) < qr.q_end
  AND COALESCE(d.terminated_at, d.due_date, d.start_date, d.closed_date, NOW()) >= qr.q_start
GROUP BY c.account_type
ORDER BY revenue DESC;
"""

# ── By service ────────────────────────────────────────────────────────────────
EXEC_BY_SERVICE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    COALESCE(s.name, b.name, 'Unknown')         AS service_name,
    COUNT(DISTINCT d.id)::int                   AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float          AS revenue
FROM deal d
LEFT JOIN service s ON s.id = d.service_id
LEFT JOIN bundle  b ON b.id = d.bundle_id
CROSS JOIN quarter_range qr
WHERE d.stage_id = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
  AND COALESCE(d.contract_status::text, 'ACTIVE') <> 'TERMINATED'
  AND COALESCE(d.start_date, d.closed_date, NOW()) < qr.q_end
  AND COALESCE(d.terminated_at, d.due_date, d.start_date, d.closed_date, NOW()) >= qr.q_start
GROUP BY COALESCE(s.name, b.name, 'Unknown')
ORDER BY revenue DESC;
"""
