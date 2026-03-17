"""
One-time script: populate the date_dimension table for 2024–2028.

Run this ONCE before starting the API:
    python scripts/seed_dates.py

All dashboard and snapshot queries that filter by year/quarter
depend on this table being populated.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import date, timedelta
from sqlalchemy import text
from db import engine


def last_day_of_month(d: date) -> int:
    """Return the last day number of the month for a given date."""
    if d.month == 12:
        return 31
    return (date(d.year, d.month + 1, 1) - timedelta(days=1)).day


def seed_dates(start_year: int = 2024, end_year: int = 2028):
    current = date(start_year, 1, 1)
    end     = date(end_year, 12, 31)
    count   = 0

    with engine.begin() as conn:
        while current <= end:
            quarter      = (current.month - 1) // 3 + 1
            is_qtr_end   = current.month in (3, 6, 9, 12) and current.day == last_day_of_month(current)

            conn.execute(text("""
                INSERT INTO date_dimension
                    (id, timestamp, year, month, month_number,
                     day, day_of_week, quarter, is_quarter_end)
                VALUES
                    (gen_random_uuid(), :ts, :yr, :mo_num,
                     :mo_num, :day, :dow, :qtr, :qtr_end)
                ON CONFLICT DO NOTHING
            """), {
                "ts":      current,
                "yr":      current.year,
                "mo_num":  current.month,
                "day":     current.day,
                "dow":     current.strftime("%A"),
                "qtr":     quarter,
                "qtr_end": is_qtr_end,
            })

            count   += 1
            current += timedelta(days=1)

    print(f"Done — seeded {count} date rows ({start_year}–{end_year})")


if __name__ == "__main__":
    seed_dates()