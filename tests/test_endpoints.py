"""
Manual test script — run this to verify all endpoints return expected output.

Usage:
    python tests/test_endpoints.py

Requirements:
    - API must be running on http://localhost:8001
    - DATABASE_URL and JWT_SECRET must be set in .env
    - You need a valid JWT token (get one from Zeandy's /api/auth/login)

Set your token before running:
    export TEST_TOKEN="your_jwt_token_here"
    export TEST_BD_ID="uuid_of_a_bd_rep"
    export TEST_YEAR=2026
    export TEST_QUARTER=1
"""

import os
import sys
import json
import urllib.request
import urllib.error

BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:8001")
TOKEN    = os.environ.get("TEST_TOKEN", "")
BD_ID    = os.environ.get("TEST_BD_ID", "")
YEAR     = os.environ.get("TEST_YEAR",    "2026")
QUARTER  = os.environ.get("TEST_QUARTER", "1")

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

passed = 0
failed = 0


def _request(path: str, require_auth: bool = True) -> tuple[int, dict | None]:
    url = BASE_URL + path
    headers = {"Content-Type": "application/json"}
    if require_auth:
        headers["Authorization"] = f"Bearer {TOKEN}"
    try:
        req  = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(req, timeout=10)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return 0, None


def check(name: str, status: int, data: dict | None, *assertions):
    global passed, failed
    errors = []

    if status == 0:
        errors.append("Connection refused — is the API running?")
    elif status != 200:
        errors.append(f"Expected HTTP 200, got {status}")
    elif data is None:
        errors.append("Response body is empty")
    else:
        for assertion_fn, msg in assertions:
            try:
                if not assertion_fn(data):
                    errors.append(msg)
            except Exception as e:
                errors.append(f"Assertion error: {e}")

    if errors:
        failed += 1
        print(f"  {RED}✗ {name}{RESET}")
        for e in errors:
            print(f"      {YELLOW}→ {e}{RESET}")
    else:
        passed += 1
        print(f"  {GREEN}✓ {name}{RESET}")


def has_key(key):
    return lambda d: key in d, f"Response missing key '{key}'"


def is_list(key):
    return lambda d: isinstance(d.get(key), list), f"'{key}' should be a list"


def list_len_gte(key, n):
    return lambda d: len(d.get(key, [])) >= n, f"'{key}' should have >= {n} items"


def key_is_number(key):
    return lambda d: isinstance(d.get(key), (int, float)), f"'{key}' should be a number"


# ─────────────────────────────────────────────────────────────────────────────

def run_tests():
    print(f"\n{BOLD}Sales CRM Analytics — Endpoint Tests{RESET}")
    print(f"Base URL : {BASE_URL}")
    print(f"Year     : {YEAR}  Quarter: Q{QUARTER}")
    print(f"BD ID    : {BD_ID or '(not set — BD dashboard tests will fail)'}")
    print()

    if not TOKEN:
        print(f"{YELLOW}WARNING: TEST_TOKEN is not set. All auth tests will fail.{RESET}")
        print("  Set it with: export TEST_TOKEN='your_jwt_here'\n")

    # ── Health check ──────────────────────────────────────────────────────────
    print(f"{BOLD}Health{RESET}")
    status, data = _request("/health", require_auth=False)
    check("GET /health → 200", status, data,
          (lambda d: d.get("status") == "ok", "status should be 'ok'"))

    # ── BD Dashboard ──────────────────────────────────────────────────────────
    print(f"\n{BOLD}BD Dashboard{RESET}")

    if BD_ID:
        status, data = _request(f"/api/analytics/dashboard/bd?year={YEAR}&quarter={QUARTER}&bd_id={BD_ID}")
        check(f"GET /dashboard/bd → 200 with all 11 keys", status, data,
              has_key("total_revenue"),
              has_key("open_pipeline"),
              has_key("quota"),
              has_key("attainment_pct"),
              has_key("sales_forecast"),
              has_key("variance"),
              has_key("excess_deficit"),
              is_list("revenue_by_month"),
              is_list("pipeline_by_stage"),
              is_list("open_deals"),
              is_list("service_revenue"),
              is_list("bundle_revenue"),
        )
        if data:
            check("  pipeline_by_stage has 7 stages", 200, data,
                  (lambda d: len(d.get("pipeline_by_stage", [])) == 7,
                   "Should have 7 pipeline stages"))
            check("  excess_deficit is 'Excess' or 'Deficit'", 200, data,
                  (lambda d: d.get("excess_deficit") in ("Excess", "Deficit"),
                   "excess_deficit should be 'Excess' or 'Deficit'"))

        # Test 401 with no token
        status, _ = _request(
            f"/api/analytics/dashboard/bd?year={YEAR}&quarter={QUARTER}&bd_id={BD_ID}",
            require_auth=False
        )
        check("GET /dashboard/bd with no token → 403 or 401", status, {"x": True},
              (lambda _: status in (401, 403), f"Expected 401/403, got {status}"))
    else:
        print(f"  {YELLOW}⚠ Skipped BD Dashboard tests (TEST_BD_ID not set){RESET}")

    # ── Executive Dashboard ───────────────────────────────────────────────────
    print(f"\n{BOLD}Executive Dashboard{RESET}")
    status, data = _request(f"/api/analytics/dashboard/executive?year={YEAR}&quarter={QUARTER}")
    check("GET /dashboard/executive → 200 with all 9 keys", status, data,
          has_key("team"),
          is_list("leaderboard"),
          is_list("stuck_deals"),
          is_list("pipeline_by_stage"),
          is_list("by_account_type"),
          is_list("by_service"),
    )
    if data:
        check("  team has 4 sub-keys", 200, data,
              (lambda d: all(k in d.get("team", {})
                             for k in ("total_revenue", "total_quota", "sales_forecast", "attainment_pct")),
               "team object missing sub-keys"))
        check("  leaderboard items have rank field", 200, data,
              (lambda d: all("rank" in item for item in d.get("leaderboard", [])),
               "leaderboard items should have 'rank'"))

    # ── Pipeline Report ───────────────────────────────────────────────────────
    print(f"\n{BOLD}Reports{RESET}")
    status, data = _request(f"/api/analytics/reports/pipeline?year={YEAR}&quarter={QUARTER}")
    check("GET /reports/pipeline → 200", status, data,
          has_key("stages"), has_key("total_pipeline_value"), has_key("total_deals"))

    # ── Quota Report ──────────────────────────────────────────────────────────
    status, data = _request(f"/api/analytics/reports/quota?year={YEAR}&quarter={QUARTER}")
    check("GET /reports/quota → 200", status, data,
          has_key("members"), has_key("team_quota"), has_key("team_actual"))
    if data:
        check("  quota members have 'status' field", 200, data,
              (lambda d: all("status" in m for m in d.get("members", [])),
               "each member should have 'status'"))

    # ── Loss Analysis ─────────────────────────────────────────────────────────
    status, data = _request(f"/api/analytics/reports/loss-analysis?year={YEAR}&quarter={QUARTER}")
    check("GET /reports/loss-analysis → 200", status, data,
          has_key("total_lost_deals"), has_key("total_lost_value"),
          is_list("by_stage"), is_list("deals"))

    # ── Sales Cycle ───────────────────────────────────────────────────────────
    status, data = _request(f"/api/analytics/reports/sales-cycle?year={YEAR}&quarter={QUARTER}")
    check("GET /reports/sales-cycle → 200", status, data,
          has_key("avg_total_cycle_days"), is_list("by_stage"))

    # ── Win Rate ──────────────────────────────────────────────────────────────
    status, data = _request(f"/api/analytics/reports/win-rate?year={YEAR}&quarter={QUARTER}")
    check("GET /reports/win-rate → 200", status, data,
          has_key("overall_win_rate"),
          is_list("by_lead_source"), is_list("by_service"), is_list("by_industry"))

    status, data = _request(
        f"/api/analytics/reports/growth-comparison"
        f"?leftMode=year&leftYear={YEAR}&leftQuarter={QUARTER}&leftQuarters=1,2"
        f"&rightMode=quarter&rightYear={YEAR}&rightQuarter={QUARTER}&rightYears={YEAR}"
    )
    check("GET /reports/growth-comparison → 200", status, data,
          has_key("left"), has_key("right"))
    if data:
        check("  growth comparison snapshots have key metrics", 200, data,
              (lambda d: all(k in d.get("left", {}) for k in ("actual", "quota", "pipelineValue", "serviceRevenue")),
               "left snapshot missing expected keys"),
              (lambda d: all(k in d.get("right", {}) for k in ("actual", "quota", "pipelineValue", "serviceRevenue")),
               "right snapshot missing expected keys"))

    status, data = _request(f"/api/analytics/reports/collections-overview?year={YEAR}&quarter={QUARTER}")
    check("GET /reports/collections-overview → 200", status, data,
          has_key("summary"), is_list("monthlyTrend"), is_list("byBd"), is_list("byAccount"), is_list("overdueAccounts"))
    if data:
        check("  collections summary has revenue keys", 200, data,
              (lambda d: all(k in d.get("summary", {}) for k in ("bookedRevenue", "expectedRevenue", "collectedRevenue", "overdueRevenue")),
               "summary missing expected collections keys"))

    # ── Excel exports ─────────────────────────────────────────────────────────
    print(f"\n{BOLD}Excel exports (checking 200 + correct content-type){RESET}")
    for path, name in [
        (f"/api/analytics/reports/pipeline?year={YEAR}&quarter={QUARTER}&format=xlsx", "Pipeline"),
        (f"/api/analytics/reports/quota?year={YEAR}&quarter={QUARTER}&format=xlsx",    "Quota"),
        (f"/api/analytics/reports/win-rate?year={YEAR}&quarter={QUARTER}&format=xlsx", "Win Rate"),
    ]:
        url = BASE_URL + path
        try:
            req  = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
            resp = urllib.request.urlopen(req, timeout=10)
            ct   = resp.headers.get("Content-Type", "")
            ok   = "spreadsheetml" in ct
            if ok:
                passed += 1
                print(f"  {GREEN}✓ GET {name} xlsx → 200 + correct content-type{RESET}")
            else:
                failed += 1
                print(f"  {RED}✗ GET {name} xlsx — wrong content-type: {ct}{RESET}")
        except urllib.error.HTTPError as e:
            failed += 1
            print(f"  {RED}✗ GET {name} xlsx → HTTP {e.code}{RESET}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total = passed + failed
    print(f"\n{'─'*50}")
    print(f"Results: {GREEN}{passed} passed{RESET} / {RED}{failed} failed{RESET} / {total} total")
    if failed:
        print(f"{YELLOW}Tip: check the API terminal for error details.{RESET}")
    print()
    return failed == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
