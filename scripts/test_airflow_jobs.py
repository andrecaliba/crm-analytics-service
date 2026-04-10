#!/usr/bin/env python3
"""
scripts/test_airflow_jobs.py
=============================
Manually runs both snapshot jobs and verifies they inserted rows correctly.
Use this to confirm the Airflow DAG logic works before waiting for Sunday.

Usage:
    # From the repo root with your venv activated:
    python scripts/test_airflow_jobs.py

    # Or test one job at a time:
    python scripts/test_airflow_jobs.py --job forecast
    python scripts/test_airflow_jobs.py --job deal

What it does:
    1. Checks date_dimension has a row for today (same as the DAG preflight task)
    2. Calls weekly_forecast_snapshot() and/or weekly_deal_snapshot()
    3. Queries the DB to confirm rows were inserted with today's date_id
    4. Prints a pass/fail summary
"""

import argparse
import sys
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from db import SessionLocal


GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):  print(f"  {GREEN}✓{RESET}  {msg}")
def fail(msg): print(f"  {RED}✗{RESET}  {msg}")
def info(msg): print(f"  {YELLOW}→{RESET}  {msg}")


def check_date_dimension(db) -> str | None:
    """Returns the date_id for today, or None if missing."""
    today = date.today()
    result = db.execute(
        text("SELECT id FROM date_dimension WHERE timestamp::date = :d LIMIT 1"),
        {"d": today},
    ).fetchone()
    return result.id if result else None


def run_forecast(db, date_id: str) -> bool:
    print(f"\n{BOLD}── forecast_snapshot ─────────────────────────────{RESET}")

    # Count rows before
    before = db.execute(
        text("SELECT COUNT(*) FROM forecast_snapshot WHERE snapshot_date_id = :d"),
        {"d": date_id},
    ).scalar()
    info(f"Rows in forecast_snapshot for today before run: {before}")

    try:
        from scheduler import weekly_forecast_snapshot
        weekly_forecast_snapshot()
        ok("weekly_forecast_snapshot() completed without error")
    except Exception as e:
        fail(f"weekly_forecast_snapshot() raised: {e}")
        return False

    # Count rows after
    after = db.execute(
        text("SELECT COUNT(*) FROM forecast_snapshot WHERE snapshot_date_id = :d"),
        {"d": date_id},
    ).scalar()
    inserted = after - before

    if inserted > 0:
        ok(f"Inserted {inserted} row(s) into forecast_snapshot")

        # Show what was inserted
        rows = db.execute(text("""
            SELECT
                COALESCE(b.first_name || ' ' || b.last_name, 'TEAM') AS name,
                fs.total_pipeline_value,
                fs.deal_count
            FROM forecast_snapshot fs
            LEFT JOIN bd b ON b.id = fs.bd_id
            WHERE fs.snapshot_date_id = :d
            ORDER BY name
        """), {"d": date_id}).fetchall()

        print()
        print(f"  {'Name':<25} {'Pipeline Value':>18} {'Deal Count':>12}")
        print(f"  {'-'*25} {'-'*18} {'-'*12}")
        for row in rows:
            print(f"  {row.name:<25} ₱{row.total_pipeline_value:>17,.2f} {row.deal_count:>12}")
        return True
    else:
        fail(f"No new rows inserted (still {after} rows for today). Already ran today?")
        return False


def run_deal(db, date_id: str) -> bool:
    print(f"\n{BOLD}── deal_snapshot ─────────────────────────────────{RESET}")

    # Count rows before
    before = db.execute(
        text("SELECT COUNT(*) FROM deal_snapshot WHERE date_id = :d"),
        {"d": date_id},
    ).scalar()
    info(f"Rows in deal_snapshot for today before run: {before}")

    try:
        from scheduler import weekly_deal_snapshot
        weekly_deal_snapshot()
        ok("weekly_deal_snapshot() completed without error")
    except Exception as e:
        fail(f"weekly_deal_snapshot() raised: {e}")
        return False

    # Count rows after
    after = db.execute(
        text("SELECT COUNT(*) FROM deal_snapshot WHERE date_id = :d"),
        {"d": date_id},
    ).scalar()
    inserted = after - before

    if inserted > 0:
        ok(f"Inserted {inserted} row(s) into deal_snapshot")

        # Show sample of what was inserted
        rows = db.execute(text("""
            SELECT
                d.deal_name,
                ps.name AS stage,
                ds.projected_amount
            FROM deal_snapshot ds
            JOIN deal d ON d.id = ds.deal_id
            JOIN pipeline_stage ps ON ps.id = ds.stage_id
            WHERE ds.date_id = :d
            ORDER BY ds.projected_amount DESC
            LIMIT 5
        """), {"d": date_id}).fetchall()

        print()
        print(f"  {'Deal Name':<35} {'Stage':<20} {'Value':>15}")
        print(f"  {'-'*35} {'-'*20} {'-'*15}")
        for row in rows:
            print(f"  {row.deal_name[:34]:<35} {row.stage:<20} ₱{row.projected_amount:>14,.2f}")
        if inserted > 5:
            info(f"  ... and {inserted - 5} more rows")
        return True
    else:
        fail(f"No new rows inserted (still {after} rows for today). Already ran today?")
        return False


def main():
    parser = argparse.ArgumentParser(description="Test Airflow snapshot jobs locally")
    parser.add_argument(
        "--job",
        choices=["forecast", "deal", "both"],
        default="both",
        help="Which job to test (default: both)",
    )
    args = parser.parse_args()

    print(f"\n{BOLD}Airflow Snapshot Job Test{RESET}")
    print(f"Date: {date.today()}\n")

    db = SessionLocal()
    results = []

    try:
        # ── Preflight: check date_dimension ──────────────────────────────────
        print(f"{BOLD}── Preflight ─────────────────────────────────────{RESET}")
        date_id = check_date_dimension(db)
        if not date_id:
            fail(f"No date_dimension row for {date.today()}")
            info("Fix: python scripts/seed_dates.py")
            sys.exit(1)
        ok(f"date_dimension row found — date_id: {date_id}")

        # ── Run selected jobs ─────────────────────────────────────────────────
        if args.job in ("forecast", "both"):
            results.append(("forecast_snapshot_dag", run_forecast(db, date_id)))

        if args.job in ("deal", "both"):
            results.append(("deal_snapshot_dag", run_deal(db, date_id)))

    finally:
        db.close()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{BOLD}── Summary ───────────────────────────────────────{RESET}")
    all_passed = True
    for name, passed in results:
        if passed:
            ok(f"{name}")
        else:
            fail(f"{name}")
            all_passed = False

    print()
    if all_passed:
        print(f"{GREEN}{BOLD}All jobs passed.{RESET}")
        print("Your Airflow DAGs are ready. Run `airflow dags list` to confirm they appear.\n")
    else:
        print(f"{RED}{BOLD}Some jobs failed — check output above.{RESET}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()