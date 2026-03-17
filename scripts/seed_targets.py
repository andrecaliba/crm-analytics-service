"""
Seed BD quota targets into the target table.

Quotas from PRD v1.0:
  - Henne Zarate:    ₱7,000,000 annual  →  ₱1,750,000 per quarter
  - Kristina Villarta: ₱7,000,000 annual  →  ₱1,750,000 per quarter
  - Brian Siriban:   ₱7,000,000 annual  →  ₱1,750,000 per quarter
  - Team total:      ₱22,590,000 annual; Q1 target ₱2,130,000

Run this once after seeding dates:
    python scripts/seed_targets.py

Re-running is safe — ON CONFLICT DO NOTHING prevents duplicates.
To update a quota, delete the old target row first then re-run.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import text
from db import engine

# Annual quota per BD rep (PHP)
ANNUAL_QUOTA = 7_000_000

# Breakdown
QUARTERLY_QUOTA = ANNUAL_QUOTA / 4       # 1,750,000
MONTHLY_QUOTA   = ANNUAL_QUOTA / 12      # ~583,333.33


def seed_targets():
    with engine.begin() as conn:

        # Look up all BD_REP members
        bd_reps = conn.execute(text("""
            SELECT id, first_name, last_name
            FROM bd
            WHERE role = 'BD_REP' AND is_active = true
            ORDER BY first_name
        """)).fetchall()

        if not bd_reps:
            print("No BD_REP members found in the database.")
            print("Make sure Zeandy has seeded BD accounts first.")
            return

        inserted = 0
        for bd in bd_reps:
            # Annual target
            conn.execute(text("""
                INSERT INTO target (id, quota, period_type, bd_id)
                VALUES (gen_random_uuid(), :quota, 'ANNUAL', :bd_id)
                ON CONFLICT DO NOTHING
            """), {"quota": ANNUAL_QUOTA, "bd_id": bd.id})

            # Quarterly targets for Q1–Q4
            for q in range(1, 5):
                conn.execute(text("""
                    INSERT INTO target (id, quota, period_type, bd_id)
                    VALUES (gen_random_uuid(), :quota, 'QUARTERLY', :bd_id)
                    ON CONFLICT DO NOTHING
                """), {"quota": QUARTERLY_QUOTA, "bd_id": bd.id})

            # Monthly targets for all 12 months
            for m in range(1, 13):
                conn.execute(text("""
                    INSERT INTO target (id, quota, period_type, bd_id)
                    VALUES (gen_random_uuid(), :quota, 'MONTHLY', :bd_id)
                    ON CONFLICT DO NOTHING
                """), {"quota": MONTHLY_QUOTA, "bd_id": bd.id})

            inserted += 1
            print(f"  Seeded targets for {bd.first_name} {bd.last_name}")

    print(f"\nDone — seeded annual/quarterly/monthly targets for {inserted} BD reps")
    print(f"  Annual:    ₱{ANNUAL_QUOTA:,.0f}")
    print(f"  Quarterly: ₱{QUARTERLY_QUOTA:,.0f}")
    print(f"  Monthly:   ₱{MONTHLY_QUOTA:,.2f}")


if __name__ == "__main__":
    seed_targets()
