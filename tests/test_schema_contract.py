"""
Schema Contract Tests
=====================
Run these in your CRM repo BEFORE deploying a migration to verify
that the tables and columns the Analytics service depends on still exist.

Copy this file into project-sales-crm/tests/test_schema_contract.py
and run it against your local or staging database:

    DATABASE_URL=postgresql://... python tests/test_schema_contract.py

These tests do NOT validate data — only structure. They answer:
"Will Andre's analytics queries still work after this migration?"

Exit code 0 = safe to deploy. Exit code 1 = something will break.
"""

import os
import sys
from sqlalchemy import create_engine, text, inspect

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: Set DATABASE_URL environment variable first.")
    sys.exit(1)

engine = create_engine(DATABASE_URL)

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

passed = 0
failed = 0
warnings = 0

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA CONTRACT
# These are the exact tables and columns that Andre's Analytics queries use.
# If you rename or remove any of these, his service will break.
#
# Format: table_name -> { column_name: impact_level }
#   HIGH   = breaks dashboards and/or multiple reports
#   MEDIUM = breaks specific reports or calculations
#   LOW    = breaks a single chart or breakdown
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_CONTRACT = {
    "deal": {
        "id":                    "HIGH",
        "deal_name":             "HIGH",
        "revenue":               "HIGH",
        "bd_id":                 "HIGH",
        "client_id":             "MEDIUM",
        "stage_id":              "HIGH",
        "service_id":            "LOW",
        "bundle_id":             "LOW",
        "is_closed":             "HIGH",
        "closed_date":           "HIGH",
        "final_proposed_value":  "MEDIUM",
        "lead_source":           "MEDIUM",
        "action_plan_due_date":  "LOW",
        "last_follow_up_at":     "LOW",
        "sales_cycle_days":      "MEDIUM",
    },
    "pipeline_stage": {
        "id":       "HIGH",
        "name":     "HIGH",
        "duration": "MEDIUM",
    },
    "bd": {
        "id":         "HIGH",
        "first_name": "HIGH",
        "last_name":  "HIGH",
        "role":       "HIGH",
        "is_active":  "MEDIUM",
    },
    "target": {
        "id":          "MEDIUM",
        "bd_id":       "MEDIUM",
        "quota":       "MEDIUM",
        "period_type": "MEDIUM",
    },
    "client": {
        "id":           "MEDIUM",
        "account_type": "LOW",
        "industry_id":  "LOW",
    },
    "industry": {
        "id":   "LOW",
        "name": "LOW",
    },
    "service": {
        "id":   "LOW",
        "name": "LOW",
    },
    "bundle": {
        "id":   "LOW",
        "name": "LOW",
    },
    "bundle_service": {
        "bundle_id":         "LOW",
        "service_id":        "LOW",
        "revenue_share_pct": "LOW",
    },
    "deal_audit_log": {
        "deal_id":       "MEDIUM",
        "stage_id":      "MEDIUM",
        "entered_at":    "MEDIUM",
        "exited_at":     "MEDIUM",
        "days_in_stage": "MEDIUM",
        "remarks":       "LOW",
    },
    "date_dimension": {
        "id":             "MEDIUM",
        "timestamp":      "MEDIUM",
        "year":           "MEDIUM",
        "month":          "MEDIUM",
        "quarter":        "MEDIUM",
        "is_quarter_end": "LOW",
    },
}

# Pipeline stage names that Analytics hardcodes in SQL queries
REQUIRED_STAGE_NAMES = ["Closed Won", "Closed Lost", "Negotiation"]

# Enum/role values hardcoded in Analytics queries
REQUIRED_BD_ROLES = ["BD_REP", "SALES_MANAGER"]
REQUIRED_PERIOD_TYPES = ["ANNUAL", "QUARTERLY", "MONTHLY"]


def check_table_exists(inspector, table_name):
    """Check if a table exists in the database."""
    global passed, failed
    tables = inspector.get_table_names()
    if table_name in tables:
        passed += 1
        print(f"  {GREEN}✓{RESET} Table '{table_name}' exists")
        return True
    else:
        failed += 1
        print(f"  {RED}✗{RESET} Table '{table_name}' is MISSING")
        return False


def check_columns(inspector, table_name, expected_columns):
    """Check if all expected columns exist in a table."""
    global passed, failed, warnings
    actual_columns = {col["name"] for col in inspector.get_columns(table_name)}

    all_ok = True
    for col_name, impact in expected_columns.items():
        if col_name in actual_columns:
            passed += 1
        else:
            failed += 1
            all_ok = False
            print(f"    {RED}✗{RESET} Column '{table_name}.{col_name}' is MISSING "
                  f"[{YELLOW}{impact} impact{RESET}]")

    if all_ok:
        print(f"    {GREEN}✓{RESET} All {len(expected_columns)} columns present")

    # Warn about new columns Analytics doesn't know about (informational only)
    extra = actual_columns - set(expected_columns.keys())
    if extra:
        warnings += 1
        extras_str = ", ".join(sorted(extra))
        print(f"    {YELLOW}ℹ{RESET} New columns not in contract: {extras_str}")


def check_stage_names(conn):
    """Verify that hardcoded stage names exist in pipeline_stage."""
    global passed, failed
    result = conn.execute(text("SELECT name FROM pipeline_stage")).fetchall()
    actual_names = {row[0] for row in result}

    for name in REQUIRED_STAGE_NAMES:
        if name in actual_names:
            passed += 1
            print(f"  {GREEN}✓{RESET} Stage '{name}' exists")
        else:
            failed += 1
            print(f"  {RED}✗{RESET} Stage '{name}' is MISSING — Analytics queries filter on this exact string")


def check_enum_values(conn, table, column, expected_values, label):
    """Check that expected enum/string values exist in the database."""
    global passed, failed
    result = conn.execute(text(f"SELECT DISTINCT {column} FROM {table}")).fetchall()
    actual_values = {row[0] for row in result}

    for val in expected_values:
        if val in actual_values:
            passed += 1
        else:
            failed += 1
            print(f"  {RED}✗{RESET} {label} value '{val}' not found in {table}.{column}")


def main():
    print(f"\n{BOLD}Analytics Schema Contract Verification{RESET}")
    print(f"Database: {DATABASE_URL[:40]}...\n")

    inspector = inspect(engine)

    # ── Check tables and columns ─────────────────────────────────────────────
    print(f"{BOLD}Tables & Columns{RESET}")
    for table_name, columns in SCHEMA_CONTRACT.items():
        if check_table_exists(inspector, table_name):
            check_columns(inspector, table_name, columns)

    # ── Check hardcoded values ───────────────────────────────────────────────
    print(f"\n{BOLD}Hardcoded Values (used in Analytics SQL){RESET}")
    with engine.connect() as conn:
        print(f"\n  Pipeline stages:")
        check_stage_names(conn)

        print(f"\n  BD roles:")
        check_enum_values(conn, "bd", "role", REQUIRED_BD_ROLES, "Role")

        print(f"\n  Target period types:")
        check_enum_values(conn, "target", "period_type", REQUIRED_PERIOD_TYPES, "Period")

    # ── Summary ──────────────────────────────────────────────────────────────
    total = passed + failed
    print(f"\n{'─' * 60}")
    print(f"Results: {GREEN}{passed} passed{RESET} / {RED}{failed} failed{RESET} / {total} total")
    if warnings:
        print(f"         {YELLOW}{warnings} info notices{RESET} (new columns not in contract)")
    if failed:
        print(f"\n{RED}BLOCKED:{RESET} Analytics queries will break if you deploy this migration.")
        print(f"Coordinate with Andre before proceeding.")
    else:
        print(f"\n{GREEN}SAFE:{RESET} All Analytics dependencies are intact. Deploy freely.")
    print()

    return failed == 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
