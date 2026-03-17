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
)
SELECT
    b.id                                                                AS bd_id,
    b.first_name || ' ' || b.last_name                                  AS name,
    COALESCE(MAX(t.quota), 0)::float                                    AS quota,
    COALESCE(SUM(
        CASE WHEN d.is_closed = true
             AND ps.name = 'Closed Won'
             AND d.closed_date >= qr.q_start
             AND d.closed_date <  qr.q_end
             THEN d.revenue ELSE 0 END
    ), 0)::float                                                        AS actual,
    ROUND(
        COALESCE(SUM(
            CASE WHEN d.is_closed = true
                 AND ps.name = 'Closed Won'
                 AND d.closed_date >= qr.q_start
                 AND d.closed_date <  qr.q_end
                 THEN d.revenue ELSE 0 END
        ), 0) / NULLIF(COALESCE(MAX(t.quota), 0), 0) * 100, 1
    )::float                                                            AS attainment_pct,
    (COALESCE(SUM(
        CASE WHEN d.is_closed = true
             AND ps.name = 'Closed Won'
             AND d.closed_date >= qr.q_start
             AND d.closed_date <  qr.q_end
             THEN d.revenue ELSE 0 END
    ), 0) - COALESCE(MAX(t.quota), 0))::float                          AS variance,
    CASE
        WHEN COALESCE(SUM(
            CASE WHEN d.is_closed = true
                 AND ps.name = 'Closed Won'
                 AND d.closed_date >= qr.q_start
                 AND d.closed_date <  qr.q_end
                 THEN d.revenue ELSE 0 END
        ), 0) >= COALESCE(MAX(t.quota), 0) THEN 'Exceeded'
        WHEN COALESCE(SUM(
            CASE WHEN d.is_closed = true
                 AND ps.name = 'Closed Won'
                 AND d.closed_date >= qr.q_start
                 AND d.closed_date <  qr.q_end
                 THEN d.revenue ELSE 0 END
        ), 0) >= COALESCE(MAX(t.quota), 0) * 0.8 THEN 'On Track'
        ELSE 'Behind'
    END                                                                 AS status
FROM bd b
CROSS JOIN quarter_range qr
LEFT JOIN deal d          ON d.bd_id    = b.id
LEFT JOIN pipeline_stage ps ON ps.id   = d.stage_id
LEFT JOIN target t        ON t.bd_id   = b.id AND t.period_type = 'QUARTERLY'
WHERE b.role = 'BD_REP'
GROUP BY b.id, b.first_name, b.last_name, qr.q_start, qr.q_end
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
    COALESCE(SUM(t.quota), 0)::float AS team_quota,
    COALESCE(SUM(
        CASE WHEN d.is_closed = true
             AND ps.name = 'Closed Won'
             AND d.closed_date >= qr.q_start
             AND d.closed_date <  qr.q_end
             THEN d.revenue ELSE 0 END
    ), 0)::float                     AS team_actual
FROM target t
JOIN bd b ON b.id = t.bd_id
CROSS JOIN quarter_range qr
LEFT JOIN deal d          ON d.bd_id    = b.id
LEFT JOIN pipeline_stage ps ON ps.id   = d.stage_id
WHERE t.period_type = 'QUARTERLY'
  AND b.role = 'BD_REP';
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
