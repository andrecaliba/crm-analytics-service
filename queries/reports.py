"""
Report queries.
All accept :year (int) and :quarter (int).
Manager-only — enforced at the route level.
"""

# ── Pipeline report ───────────────────────────────────────────────────────────
PIPELINE_REPORT = """
SELECT
    ps.name                             AS stage_name,
    COUNT(d.id)::int                    AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float  AS total_value,
    ROUND(
        100.0 * COALESCE(SUM(d.revenue), 0)
        / NULLIF((SELECT SUM(revenue) FROM deal WHERE is_closed = false), 0), 1
    )::float                            AS pct_of_total
FROM pipeline_stage ps
LEFT JOIN deal d ON d.stage_id = ps.id AND d.is_closed = false
WHERE ps.name NOT IN ('Closed Won', 'Closed Lost')
GROUP BY ps.id, ps.name
ORDER BY ps.id;
"""

PIPELINE_TOTALS = """
SELECT
    COUNT(id)::int                   AS total_deals,
    COALESCE(SUM(revenue), 0)::float AS total_pipeline_value
FROM deal
WHERE is_closed = false;
"""

# ── Quota report ──────────────────────────────────────────────────────────────
QUOTA_REPORT = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
),
bd_quota AS (
    SELECT t.bd_id, COALESCE(MAX(t.quota), 0) AS quota
    FROM target t
    JOIN date_dimension dd ON dd.id = t.date_id
    WHERE period_type = 'QUARTERLY'
      AND dd.year = :year
      AND dd.quarter = :quarter
    GROUP BY t.bd_id
),
bd_actual AS (
    SELECT
        d.bd_id,
        COALESCE(SUM(d.revenue), 0) AS actual
    FROM deal d
    JOIN pipeline_stage ps ON ps.id = d.stage_id
    CROSS JOIN quarter_range qr
    WHERE d.is_closed = true
      AND ps.name = 'Closed Won'
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
    GROUP BY d.bd_id
)
SELECT
    b.id                                                                AS bd_id,
    b.first_name || ' ' || b.last_name                                  AS name,
    COALESCE(q.quota, 0)::float                                         AS quota,
    COALESCE(a.actual, 0)::float                                        AS actual,
    ROUND(
        COALESCE(a.actual, 0) / NULLIF(COALESCE(q.quota, 0), 0) * 100, 1
    )::float                                                            AS attainment_pct,
    (COALESCE(a.actual, 0) - COALESCE(q.quota, 0))::float               AS variance,
    CASE
        WHEN COALESCE(a.actual, 0) >= COALESCE(q.quota, 0)             THEN 'Exceeded'
        WHEN COALESCE(a.actual, 0) >= COALESCE(q.quota, 0) * 0.8       THEN 'On Track'
        ELSE 'Behind'
    END                                                                 AS status
FROM bd b
LEFT JOIN bd_quota  q ON q.bd_id = b.id
LEFT JOIN bd_actual a ON a.bd_id = b.id
WHERE b.role = 'BD_REP'
ORDER BY actual DESC;
"""

QUOTA_TEAM_TOTALS = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    (
        SELECT COALESCE(SUM(t.quota), 0)
        FROM target t
        JOIN date_dimension dd ON dd.id = t.date_id
        JOIN bd b ON b.id = t.bd_id
        WHERE t.period_type = 'QUARTERLY'
          AND dd.year = :year
          AND dd.quarter = :quarter
          AND b.role = 'BD_REP'
    )::float AS team_quota,
    (
        SELECT COALESCE(SUM(d.revenue), 0)
        FROM deal d
        JOIN pipeline_stage ps ON ps.id = d.stage_id
        CROSS JOIN quarter_range qr
        WHERE d.is_closed = true
          AND ps.name = 'Closed Won'
          AND d.closed_date >= qr.q_start
          AND d.closed_date <  qr.q_end
    )::float AS team_actual
FROM quarter_range;
"""

# ── Loss analysis ─────────────────────────────────────────────────────────────
LOSS_BY_STAGE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
),
lost_deals AS (
    SELECT d.id, d.revenue
    FROM deal d
    CROSS JOIN quarter_range qr
    WHERE d.is_closed = true
      AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Lost')
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
)
SELECT
    -- Stage the deal was in just before being marked Closed Lost
    prev_stage.name                                                   AS lost_from_stage,
    COUNT(ld.id)::int                                                 AS lost_count,
    COALESCE(SUM(ld.revenue), 0)::float                               AS lost_value,
    ROUND(
        100.0 * COUNT(ld.id) / NULLIF((SELECT COUNT(*) FROM lost_deals), 0), 1
    )::float                                                          AS pct_of_lost
FROM lost_deals ld
JOIN deal_audit_log dal ON dal.deal_id = ld.id
    AND dal.stage_id = (SELECT id FROM pipeline_stage WHERE name = 'Closed Lost')
-- Get the stage entry just before the Closed Lost entry
LEFT JOIN LATERAL (
    SELECT ps2.name
    FROM deal_audit_log dal2
    JOIN pipeline_stage ps2 ON ps2.id = dal2.stage_id
    WHERE dal2.deal_id   = ld.id
      AND dal2.entered_at < dal.entered_at
    ORDER BY dal2.entered_at DESC
    LIMIT 1
) prev_stage ON true
GROUP BY prev_stage.name
ORDER BY lost_count DESC;
"""

LOSS_DEALS_LIST = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    d.id                                                   AS deal_id,
    d.deal_name,
    b.first_name || ' ' || b.last_name                     AS bd_name,
    d.final_proposed_value::float                          AS final_proposed_value,
    d.closed_date::text                                    AS closed_date,
    -- Last remarks before closing lost
    (
        SELECT dal2.remarks
        FROM deal_audit_log dal2
        WHERE dal2.deal_id = d.id
        ORDER BY dal2.entered_at DESC
        LIMIT 1
    )                                                      AS last_remarks,
    -- Stage just before Closed Lost
    (
        SELECT ps2.name
        FROM deal_audit_log dal3
        JOIN pipeline_stage ps2 ON ps2.id = dal3.stage_id
        WHERE dal3.deal_id = d.id
          AND ps2.name != 'Closed Lost'
        ORDER BY dal3.entered_at DESC
        LIMIT 1
    )                                                      AS lost_from_stage
FROM deal d
JOIN bd b ON b.id = d.bd_id
CROSS JOIN quarter_range qr
WHERE d.is_closed = true
  AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Lost')
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
ORDER BY d.closed_date DESC;
"""

LOSS_TOTALS = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    COUNT(d.id)::int                    AS total_lost_deals,
    COALESCE(SUM(d.revenue), 0)::float  AS total_lost_value
FROM deal d
CROSS JOIN quarter_range qr
WHERE d.is_closed = true
  AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Lost')
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end;
"""

# ── Sales cycle ───────────────────────────────────────────────────────────────
SALES_CYCLE_BY_STAGE = """
SELECT
    ps.name                                AS stage_name,
    ROUND(AVG(dal.days_in_stage), 1)::float AS avg_days,
    MAX(dal.days_in_stage)::int            AS max_days,
    MIN(dal.days_in_stage)::int            AS min_days,
    COUNT(*)::int                          AS sample_size
FROM deal_audit_log dal
JOIN pipeline_stage ps ON ps.id = dal.stage_id
JOIN deal d             ON d.id  = dal.deal_id
WHERE dal.exited_at IS NOT NULL
  AND dal.days_in_stage IS NOT NULL
  AND EXTRACT(YEAR    FROM dal.entered_at) = :year
  AND EXTRACT(QUARTER FROM dal.entered_at) = :quarter
GROUP BY ps.id, ps.name
ORDER BY ps.id;
"""

SALES_CYCLE_TOTAL = """
SELECT
    ROUND(AVG(d.sales_cycle_days), 1)::float AS avg_total_cycle_days,
    MAX(d.sales_cycle_days)::int             AS max_cycle_days,
    COUNT(d.id)::int                         AS sample_size
FROM deal d
WHERE d.is_closed      = true
  AND d.sales_cycle_days IS NOT NULL
  AND d.stage_id        = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
  AND EXTRACT(YEAR    FROM d.closed_date) = :year
  AND EXTRACT(QUARTER FROM d.closed_date) = :quarter;
"""

# ── Win rate ──────────────────────────────────────────────────────────────────
WIN_RATE_BY_LEAD_SOURCE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    d.lead_source::text                                              AS source,
    COUNT(CASE WHEN ps.name = 'Closed Won'  THEN 1 END)::int        AS won,
    COUNT(CASE WHEN ps.name = 'Closed Lost' THEN 1 END)::int        AS lost,
    ROUND(
        100.0 * COUNT(CASE WHEN ps.name = 'Closed Won' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN d.is_closed = true THEN 1 END), 0), 1
    )::float                                                         AS win_rate
FROM deal d
JOIN pipeline_stage ps ON ps.id = d.stage_id
CROSS JOIN quarter_range qr
WHERE d.is_closed   = true
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
GROUP BY d.lead_source
ORDER BY win_rate DESC;
"""

WIN_RATE_BY_SERVICE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    COALESCE(s.name, b.name, 'Unknown')                              AS service,
    COUNT(CASE WHEN ps.name = 'Closed Won'  THEN 1 END)::int        AS won,
    COUNT(CASE WHEN ps.name = 'Closed Lost' THEN 1 END)::int        AS lost,
    ROUND(
        100.0 * COUNT(CASE WHEN ps.name = 'Closed Won' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN d.is_closed = true THEN 1 END), 0), 1
    )::float                                                         AS win_rate
FROM deal d
JOIN pipeline_stage ps ON ps.id = d.stage_id
LEFT JOIN service s    ON s.id  = d.service_id
LEFT JOIN bundle  b    ON b.id  = d.bundle_id
CROSS JOIN quarter_range qr
WHERE d.is_closed   = true
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
GROUP BY COALESCE(s.name, b.name, 'Unknown')
ORDER BY win_rate DESC;
"""

WIN_RATE_BY_INDUSTRY = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    COALESCE(i.name, 'Unknown')                                      AS industry,
    COUNT(CASE WHEN ps.name = 'Closed Won'  THEN 1 END)::int        AS won,
    COUNT(CASE WHEN ps.name = 'Closed Lost' THEN 1 END)::int        AS lost,
    ROUND(
        100.0 * COUNT(CASE WHEN ps.name = 'Closed Won' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN d.is_closed = true THEN 1 END), 0), 1
    )::float                                                         AS win_rate
FROM deal d
JOIN pipeline_stage ps ON ps.id = d.stage_id
JOIN client c          ON c.id  = d.client_id
LEFT JOIN industry i   ON i.id  = c.industry_id
CROSS JOIN quarter_range qr
WHERE d.is_closed   = true
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end
GROUP BY COALESCE(i.name, 'Unknown')
ORDER BY win_rate DESC;
"""

WIN_RATE_OVERALL = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    ROUND(
        100.0 * COUNT(CASE WHEN ps.name = 'Closed Won' THEN 1 END)
        / NULLIF(COUNT(CASE WHEN d.is_closed = true THEN 1 END), 0), 1
    )::float AS overall_win_rate
FROM deal d
JOIN pipeline_stage ps ON ps.id = d.stage_id
CROSS JOIN quarter_range qr
WHERE d.is_closed   = true
  AND d.closed_date >= qr.q_start
  AND d.closed_date <  qr.q_end;
"""
# ── Pipeline detail — per-stage enrichment ────────────────────────────────────
# All queries accept optional :bd_id (NULL = all BDs).
# Used by the Pipeline tab on the Executive Reports page.

PIPELINE_BY_BD = """
-- Who owns deals at each open pipeline stage (contributor breakdown).
-- Scoped to deals whose start_date falls within the selected year/quarter.
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    ps.name                              AS stage_name,
    b.first_name || ' ' || b.last_name  AS bd_name,
    b.id                                 AS bd_id,
    COUNT(d.id)::int                     AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float   AS total_value
FROM pipeline_stage ps
JOIN deal d ON d.stage_id = ps.id
    AND d.is_closed = false
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
CROSS JOIN quarter_range qr
JOIN bd b ON b.id = d.bd_id
WHERE ps.name NOT IN ('Closed Won', 'Closed Lost')
  AND d.start_date >= qr.q_start
  AND d.start_date <  qr.q_end
GROUP BY ps.id, ps.name, b.id, b.first_name, b.last_name
ORDER BY ps.id, total_value DESC;
"""

PIPELINE_BY_SERVICE = """
-- Services mix across the open pipeline, optionally filtered by BD and period.
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    ps.name                                    AS stage_name,
    COALESCE(s.name, bun.name, 'Unassigned')  AS service_name,
    COUNT(d.id)::int                           AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float         AS total_value
FROM pipeline_stage ps
JOIN deal d ON d.stage_id = ps.id
    AND d.is_closed = false
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
CROSS JOIN quarter_range qr
LEFT JOIN service s   ON s.id = d.service_id
LEFT JOIN bundle  bun ON bun.id = d.bundle_id
WHERE ps.name NOT IN ('Closed Won', 'Closed Lost')
  AND d.start_date >= qr.q_start
  AND d.start_date <  qr.q_end
GROUP BY ps.id, ps.name, COALESCE(s.name, bun.name, 'Unassigned')
ORDER BY ps.id, total_value DESC;
"""

PIPELINE_BY_ACCOUNT_TYPE = """
-- Account/client type mix across the open pipeline, optionally filtered by BD and period.
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    ps.name                              AS stage_name,
    INITCAP(c.account_type::text)        AS account_type,
    COUNT(d.id)::int                     AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float   AS total_value
FROM pipeline_stage ps
JOIN deal d ON d.stage_id = ps.id
    AND d.is_closed = false
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
CROSS JOIN quarter_range qr
JOIN client c ON c.id = d.client_id
WHERE ps.name NOT IN ('Closed Won', 'Closed Lost')
  AND d.start_date >= qr.q_start
  AND d.start_date <  qr.q_end
GROUP BY ps.id, ps.name, INITCAP(c.account_type::text)
ORDER BY ps.id, total_value DESC;
"""

PIPELINE_LEAD_SOURCE = """
-- Lead source breakdown across open pipeline, optionally filtered by BD and period.
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    INITCAP(d.lead_source::text)         AS lead_source,
    COUNT(d.id)::int                     AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float   AS total_value
FROM deal d
CROSS JOIN quarter_range qr
WHERE d.is_closed = false
  AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
  AND d.start_date >= qr.q_start
  AND d.start_date <  qr.q_end
GROUP BY INITCAP(d.lead_source::text)
ORDER BY total_value DESC;
"""

PIPELINE_STAGE_TOTALS = """
-- Total value per open stage (for the stage selector value badges), scoped to period.
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
)
SELECT
    ps.name                              AS stage_name,
    COUNT(d.id)::int                     AS deal_count,
    COALESCE(SUM(d.revenue), 0)::float   AS total_value
FROM pipeline_stage ps
LEFT JOIN deal d ON d.stage_id = ps.id
    AND d.is_closed = false
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
    AND d.start_date >= (SELECT q_start FROM quarter_range)
    AND d.start_date <  (SELECT q_end   FROM quarter_range)
WHERE ps.name NOT IN ('Closed Won', 'Closed Lost')
GROUP BY ps.id, ps.name
ORDER BY ps.id;
"""

# ── Service performance report ────────────────────────────────────────────────
# Revenue, deal count, win rate, avg deal size, avg sales cycle per service.
# Accepts :year and :quarter.

SERVICE_PERFORMANCE = """
WITH quarter_range AS (
    SELECT
        make_date(:year, (:quarter - 1) * 3 + 1, 1)::timestamptz AS q_start,
        (make_date(:year, (:quarter - 1) * 3 + 1, 1)
            + INTERVAL '3 months')::timestamptz                   AS q_end
),
all_services AS (
    SELECT id, name FROM service
),
closed_deals AS (
    SELECT
        d.id,
        d.revenue,
        d.sales_cycle_days,
        d.service_id,
        ps.name AS stage_name
    FROM deal d
    JOIN pipeline_stage ps ON ps.id = d.stage_id
    CROSS JOIN quarter_range qr
    WHERE d.is_closed = true
      AND d.closed_date >= qr.q_start
      AND d.closed_date <  qr.q_end
      AND d.service_id IS NOT NULL
),
open_deals AS (
    SELECT
        d.id,
        d.revenue,
        d.service_id
    FROM deal d
    CROSS JOIN quarter_range qr
    WHERE d.is_closed = false
      AND d.service_id IS NOT NULL
      AND d.start_date >= qr.q_start
      AND d.start_date <  qr.q_end
)
SELECT
    s.name                                                                       AS service_name,
    COUNT(CASE WHEN cd.stage_name = 'Closed Won'  THEN 1 END)::int             AS won_deals,
    COUNT(CASE WHEN cd.stage_name = 'Closed Lost' THEN 1 END)::int             AS lost_deals,
    COUNT(cd.id)::int                                                            AS closed_deals,
    COALESCE(COUNT(od.id), 0)::int                                              AS open_deals,
    COALESCE(SUM(CASE WHEN cd.stage_name = 'Closed Won' THEN cd.revenue END), 0)::float   AS won_revenue,
    ROUND(
        100.0 * COUNT(CASE WHEN cd.stage_name = 'Closed Won' THEN 1 END)
        / NULLIF(COUNT(cd.id), 0), 1
    )::float                                                                     AS win_rate,
    ROUND(
        COALESCE(AVG(CASE WHEN cd.stage_name = 'Closed Won' THEN cd.revenue END), 0), 0
    )::float                                                                     AS avg_deal_size,
    ROUND(
        COALESCE(AVG(CASE WHEN cd.stage_name = 'Closed Won' THEN cd.sales_cycle_days END), 0), 1
    )::float                                                                     AS avg_cycle_days
FROM all_services s
LEFT JOIN closed_deals cd ON cd.service_id = s.id
LEFT JOIN open_deals   od ON od.service_id = s.id
GROUP BY s.id, s.name
ORDER BY won_revenue DESC;
"""

# ── Growth sandbox queries ────────────────────────────────────────────────────
# Support month / quarter / year granularity with optional bd_id filter.
# Each returns a series of { period_label, revenue } rows.

GROWTH_BY_MONTH = """
-- Closed Won revenue by calendar month for a given year, optional BD filter.
SELECT
    TO_CHAR(make_date(:year, m.n, 1), 'Mon YY') AS period_label,
    m.n                                          AS period_order,
    COALESCE(SUM(d.revenue), 0)::float           AS revenue
FROM generate_series(1, 12) AS m(n)
LEFT JOIN deal d ON EXTRACT(YEAR  FROM d.closed_date)::int = :year
    AND EXTRACT(MONTH FROM d.closed_date)::int = m.n
    AND d.is_closed = true
    AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
GROUP BY m.n
ORDER BY m.n;
"""

GROWTH_BY_QUARTER = """
-- Closed Won revenue by quarter for a given year, optional BD filter.
SELECT
    'Q' || q.n || ' ' || :year            AS period_label,
    q.n                                   AS period_order,
    COALESCE(SUM(d.revenue), 0)::float    AS revenue
FROM generate_series(1, 4) AS q(n)
LEFT JOIN deal d ON EXTRACT(YEAR    FROM d.closed_date)::int = :year
    AND EXTRACT(QUARTER FROM d.closed_date)::int = q.n
    AND d.is_closed = true
    AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
GROUP BY q.n
ORDER BY q.n;
"""

GROWTH_BY_YEAR = """
-- Closed Won revenue by year across a range, optional BD filter.
-- Shows the last 5 years ending at :year.
SELECT
    y.yr::text                             AS period_label,
    y.yr                                   AS period_order,
    COALESCE(SUM(d.revenue), 0)::float     AS revenue
FROM generate_series(:year - 4, :year) AS y(yr)
LEFT JOIN deal d ON EXTRACT(YEAR FROM d.closed_date)::int = y.yr
    AND d.is_closed = true
    AND d.stage_id  = (SELECT id FROM pipeline_stage WHERE name = 'Closed Won')
    AND (:bd_id IS NULL OR d.bd_id::text = :bd_id)
GROUP BY y.yr
ORDER BY y.yr;
"""

BD_LIST = """
-- All active BD reps — for the BD filter dropdown.
SELECT id, first_name || ' ' || last_name AS full_name, role
FROM bd
WHERE is_active = true
ORDER BY first_name;
"""

