"""
Cron jobs for the analytics service.
Two jobs run every Sunday:
  00:00 — forecast_snapshot (per-BD + team aggregate)
  00:30 — deal_snapshot (one row per open deal)

Both are registered in main.py on startup.
"""

import logging
from datetime import date
from sqlalchemy import text
from db import SessionLocal

logger = logging.getLogger(__name__)


def _get_date_id(db, target_date: date) -> str | None:
    """Return the date_dimension.id for a given date, or None if not seeded."""
    result = db.execute(
        text("SELECT id FROM date_dimension WHERE timestamp::date = :d LIMIT 1"),
        {"d": target_date},
    ).fetchone()
    return result.id if result else None


def weekly_forecast_snapshot():
    """
    Runs: every Sunday at midnight.

    For each BD rep with open deals, inserts one forecast_snapshot row
    capturing their total pipeline value and weighted forecast.
    Also inserts one team-level row (bd_id = NULL).

    This powers the "forecast trend over time" chart.
    """
    db = SessionLocal()
    try:
        today = date.today()
        date_id = _get_date_id(db, today)
        if not date_id:
            logger.warning(
                f"weekly_forecast_snapshot: no date_dimension row for {today}. "
                "Run scripts/seed_dates.py to fix."
            )

        # Per-BD snapshots
        bd_rows = db.execute(text("""
            SELECT
                dp.bd_id,
                COALESCE(SUM(d.revenue), 0)         AS total_pipeline_value,
                COALESCE(SUM(dp.weighted_value), 0)  AS total_weighted_value,
                COUNT(d.id)                          AS deal_count
            FROM deal_projection dp
            JOIN deal d ON d.id = dp.deal_id
            WHERE d.is_closed = false
            GROUP BY dp.bd_id
        """)).fetchall()

        for row in bd_rows:
            db.execute(text("""
                INSERT INTO forecast_snapshot
                    (id, bd_id, total_pipeline_value,
                     total_weighted_value, deal_count, snapshot_date_id)
                VALUES
                    (gen_random_uuid(), :bd_id, :pipeline,
                     :weighted, :count, :date_id)
            """), {
                "bd_id":    row.bd_id,
                "pipeline": row.total_pipeline_value,
                "weighted": row.total_weighted_value,
                "count":    row.deal_count,
                "date_id":  date_id,
            })

        # Team-level snapshot (bd_id = NULL)
        team = db.execute(text("""
            SELECT
                COALESCE(SUM(d.revenue), 0)         AS pipeline,
                COALESCE(SUM(dp.weighted_value), 0)  AS weighted,
                COUNT(d.id)                          AS cnt
            FROM deal_projection dp
            JOIN deal d ON d.id = dp.deal_id
            WHERE d.is_closed = false
        """)).fetchone()

        db.execute(text("""
            INSERT INTO forecast_snapshot
                (id, bd_id, total_pipeline_value,
                 total_weighted_value, deal_count, snapshot_date_id)
            VALUES
                (gen_random_uuid(), NULL, :pipeline,
                 :weighted, :count, :date_id)
        """), {
            "pipeline": team.pipeline,
            "weighted": team.weighted,
            "count":    team.cnt,
            "date_id":  date_id,
        })

        db.commit()
        logger.info(
            f"weekly_forecast_snapshot: inserted {len(bd_rows)} BD rows + 1 team row"
        )

    except Exception as e:
        db.rollback()
        logger.error(f"weekly_forecast_snapshot failed: {e}")
        raise
    finally:
        db.close()


def weekly_deal_snapshot():
    """
    Runs: every Sunday at 00:30.

    For every currently open deal, inserts one deal_snapshot row
    capturing its stage, probability, projected amount, and weighted value.

    This allows historical reconstruction: "what did the pipeline look like
    4 weeks ago?" by filtering deal_snapshot on date_id.
    """
    db = SessionLocal()
    try:
        today = date.today()
        date_id = _get_date_id(db, today)
        if not date_id:
            logger.warning(
                f"weekly_deal_snapshot: no date_dimension row for {today}. "
                "Run scripts/seed_dates.py to fix."
            )

        open_deals = db.execute(text("""
            SELECT
                d.id              AS deal_id,
                d.stage_id,
                dp.probability_pct,
                dp.projected_amount,
                dp.weighted_value
            FROM deal d
            JOIN deal_projection dp ON dp.deal_id = d.id
            WHERE d.is_closed = false
        """)).fetchall()

        for deal in open_deals:
            db.execute(text("""
                INSERT INTO deal_snapshot
                    (id, deal_id, stage_id, probability_pct,
                     projected_amount, weighted_value, date_id)
                VALUES
                    (gen_random_uuid(), :deal_id, :stage_id,
                     :prob, :projected, :weighted, :date_id)
            """), {
                "deal_id":   deal.deal_id,
                "stage_id":  deal.stage_id,
                "prob":      deal.probability_pct,
                "projected": deal.projected_amount,
                "weighted":  deal.weighted_value,
                "date_id":   date_id,
            })

        db.commit()
        logger.info(f"weekly_deal_snapshot: inserted {len(open_deals)} deal rows")

    except Exception as e:
        db.rollback()
        logger.error(f"weekly_deal_snapshot failed: {e}")
        raise
    finally:
        db.close()
