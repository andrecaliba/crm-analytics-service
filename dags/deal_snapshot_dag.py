"""
deal_snapshot_dag.py
====================
Runs every Sunday at 00:30 (Asia/Manila) — 30 minutes after forecast_snapshot_dag.

Tasks:
  1. check_date_dimension — confirms a date_dimension row exists for today.
     Fails fast if seed_dates.py has not been run far enough into the future.
  2. run_deal_snapshot — calls weekly_deal_snapshot() from scheduler.py.
     Inserts one deal_snapshot row per currently open deal.

Retry behaviour: 3 retries, 5-minute delay.
Manual trigger: airflow dags trigger deal_snapshot_dag
"""

from __future__ import annotations

from datetime import date, datetime, timedelta


from airflow.sdk import dag, task


DEFAULT_ARGS = {
    "owner": "analytics-service",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="deal_snapshot_dag",
    description="Weekly deal_snapshot — one row per open deal",
    schedule="30 0 * * 0",         # Every Sunday at 00:30
    start_date=datetime(2026, 1, 1),   # First Sunday on or after deploy
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["analytics", "snapshot", "weekly"],
)
def deal_snapshot_dag():

    @task()
    def check_date_dimension():
        """
        Confirm date_dimension has a row for today.
        Raises ValueError (fails the task) if the row is missing,
        which blocks run_deal_snapshot from executing.
        Run scripts/seed_dates.py to fix.
        """
        from db import SessionLocal
        from sqlalchemy import text

        today = date.today()
        db = SessionLocal()
        try:
            result = db.execute(
                text("SELECT id FROM date_dimension WHERE timestamp::date = :d LIMIT 1"),
                {"d": today},
            ).fetchone()
            if not result:
                raise ValueError(
                    f"No date_dimension row for {today}. "
                    "Run: python scripts/seed_dates.py"
                )
        finally:
            db.close()

    @task()
    def run_deal_snapshot():
        """
        Calls weekly_deal_snapshot() unchanged from scheduler.py.
        Inserts one deal_snapshot row per currently open deal.
        """
        from scheduler import weekly_deal_snapshot
        weekly_deal_snapshot()

    # Task dependency: preflight must pass before snapshot runs
    check_date_dimension() >> run_deal_snapshot()


deal_snapshot_dag()