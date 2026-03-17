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
    WHERE d.is_closed = true
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
),
team_quota AS (
    SELECT COALESCE(SUM(t.quota), 0) AS total_quota
    FROM target t
    JOIN bd b ON b.id = t.bd_id
    WHERE t.period_type = 'QUARTERLY'
      AND b.role = 'BD_REP'
),
negotiation AS (
    -- Forecast = Closed Won + Negotiation revenue (no weighting — stage % are labels only)
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
)
SELECT
    b.id                                                                   AS bd_id,
    b.first_name,
    b.last_name,
    b.role,
    COALESCE(SUM(
        CASE WHEN ps.name = 'Closed Won'
             AND d.closed_date >= qr.q_start
             AND d.closed_date <  qr.q_end
             THEN d.revenue ELSE 0 END
    ), 0)::float                                                           AS revenue,
    COALESCE(MAX(t.quota), 0)::float                                       AS quota,
    ROUND(
        COALESCE(SUM(
            CASE WHEN ps.name = 'Closed Won'
                 AND d.closed_date >= qr.q_start
                 AND d.closed_date <  qr.q_end
                 THEN d.revenue ELSE 0 END
        ), 0) / NULLIF(COALESCE(MAX(t.quota), 0), 0) * 100, 1
    )::float                                                               AS attainment_pct,
    ROUND(
        100.0 * COUNT(CASE WHEN ps.name = 'Closed Won'
                           AND d.closed_date >= qr.q_start
                           AND d.closed_date <  qr.q_end
                           THEN 1 END)
        / NULLIF(COUNT(
            CASE WHEN d.is_closed = true
                 AND d.closed_date >= qr.q_start
                 AND d.closed_date <  qr.q_end
                 THEN 1 END
        ), 0), 1
    )::float                                                               AS win_rate,
    RANK() OVER (
        ORDER BY SUM(
            CASE WHEN ps.name = 'Closed Won'
                 AND d.closed_date >= qr.q_start
                 AND d.closed_date <  qr.q_end
                 THEN d.revenue ELSE 0 END
        ) DESC
    )::int                                                                 AS rank
FROM bd b
CROSS JOIN quarter_range qr
LEFT JOIN deal d      ON d.bd_id    = b.id
LEFT JOIN pipeline_stage ps ON ps.id = d.stage_id
LEFT JOIN target t    ON t.bd_id    = b.id AND t.period_type = 'QUARTERLY'
WHERE b.role = 'BD_REP'
GROUP BY b.id, b.first_name, b.last_name, b.role, qr.q_start, qr.q_end
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
    c.account_type::text                       AS account_type,
    COUNT(d.id)::int                           AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float         AS revenue
FROM deal d
JOIN client c ON c.id = d.client_id
CROSS JOIN quarter_range qr
WHERE d.is_closed = true
  AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
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
    COALESCE(s.name, b.name, 'Unknown')        AS service_name,
    COUNT(d.id)::int                           AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float         AS revenue
FROM deal d
LEFT JOIN service s ON s.id = d.service_id
LEFT JOIN bundle  b ON b.id = d.bundle_id
CROSS JOIN quarter_range qr
WHERE d.is_closed = true
  AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
GROUP BY COALESCE(s.name, b.name, 'Unknown')
ORDER BY revenue DESC;
"""