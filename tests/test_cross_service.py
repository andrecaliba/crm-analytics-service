"""
Cross-Service Integration Test Runner
======================================
Tests the full flow: CRM issues a token → Analytics accepts it and returns data.

This script validates that both services are running, can talk to the same DB,
and that the auth handshake works end-to-end.

Prerequisites:
    - Both services running (via docker-compose.dev.yml or manually)
    - CRM on port 8000, Analytics on port 8001
    - A BD account and login credentials in the database

Usage:
    # Option A: Provide CRM login credentials (tests the full auth flow)
    export CRM_EMAIL="test@example.com"
    export CRM_PASSWORD="password"
    python tests/test_cross_service.py

    # Option B: Provide a pre-generated token (skips CRM login)
    export TEST_TOKEN=$(python scripts/generate_token.py --from-db --role SALES_MANAGER -q)
    python tests/test_cross_service.py

    # Option C: Specify custom URLs
    export CRM_BASE_URL="http://localhost:8000"
    export ANALYTICS_BASE_URL="http://localhost:8001"
    python tests/test_cross_service.py
"""

import os
import sys
import json
import urllib.request
import urllib.error

CRM_BASE_URL       = os.environ.get("CRM_BASE_URL", "http://localhost:8000")
ANALYTICS_BASE_URL = os.environ.get("ANALYTICS_BASE_URL", "http://localhost:8001")
CRM_EMAIL          = os.environ.get("CRM_EMAIL", "")
CRM_PASSWORD       = os.environ.get("CRM_PASSWORD", "")
TEST_TOKEN         = os.environ.get("TEST_TOKEN", "")

YEAR    = os.environ.get("TEST_YEAR", "2026")
QUARTER = os.environ.get("TEST_QUARTER", "1")

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

passed = 0
failed = 0


def http_get(url, token=None, timeout=10):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        req = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = None
        try:
            body = json.loads(e.read())
        except Exception:
            pass
        return e.code, body
    except Exception as e:
        return 0, {"error": str(e)}


def http_post(url, data, timeout=10):
    headers = {"Content-Type": "application/json"}
    body = json.dumps(data).encode()
    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_resp = None
        try:
            body_resp = json.loads(e.read())
        except Exception:
            pass
        return e.code, body_resp
    except Exception as e:
        return 0, {"error": str(e)}


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  {GREEN}✓{RESET} {name}")
    else:
        failed += 1
        print(f"  {RED}✗{RESET} {name}")
        if detail:
            print(f"      {YELLOW}→ {detail}{RESET}")


def main():
    global passed, failed

    print(f"\n{BOLD}Cross-Service Integration Tests{RESET}")
    print(f"CRM:       {CRM_BASE_URL}")
    print(f"Analytics: {ANALYTICS_BASE_URL}")
    print(f"Year: {YEAR}  Quarter: Q{QUARTER}\n")

    # ── Step 1: Health checks ────────────────────────────────────────────────
    print(f"{BOLD}1. Service Health{RESET}")

    status, data = http_get(f"{CRM_BASE_URL}/health")
    crm_up = status == 200
    check("CRM is running", crm_up,
          "Start with: uvicorn main:app --reload --port 8000" if not crm_up else "")

    status, data = http_get(f"{ANALYTICS_BASE_URL}/health")
    analytics_up = status == 200
    check("Analytics is running", analytics_up,
          "Start with: uvicorn main:app --reload --port 8001" if not analytics_up else "")

    if not analytics_up:
        print(f"\n{RED}Cannot continue — Analytics service is down.{RESET}\n")
        return False

    # ── Step 2: Get a token ──────────────────────────────────────────────────
    print(f"\n{BOLD}2. Authentication{RESET}")
    token = TEST_TOKEN
    bd_id = os.environ.get("TEST_BD_ID", "")

    if not token and CRM_EMAIL and CRM_PASSWORD and crm_up:
        # Try logging in through the CRM
        print(f"  Logging in via CRM as {CRM_EMAIL}...")
        status, data = http_post(f"{CRM_BASE_URL}/api/auth/login", {
            "email": CRM_EMAIL,
            "password": CRM_PASSWORD,
        })
        if status == 200 and data:
            # Try common token field names
            token = data.get("token") or data.get("access_token") or data.get("jwt", "")
            bd_id = data.get("bdId") or data.get("bd_id") or data.get("user", {}).get("id", "")
            check("CRM login successful", bool(token), "Token not found in response")
        else:
            check("CRM login successful", False, f"HTTP {status}: {data}")
    elif token:
        print(f"  Using pre-set TEST_TOKEN")
        check("Token available", True)
    else:
        print(f"  {YELLOW}No credentials or token provided.{RESET}")
        print(f"  Set CRM_EMAIL + CRM_PASSWORD, or TEST_TOKEN")
        print(f"  Tip: python scripts/generate_token.py --from-db -q")
        check("Token available", False, "Cannot test authenticated endpoints")
        _print_summary()
        return False

    # ── Step 3: Token works on Analytics ─────────────────────────────────────
    print(f"\n{BOLD}3. Cross-Service Auth (CRM token → Analytics){RESET}")

    status, data = http_get(
        f"{ANALYTICS_BASE_URL}/api/analytics/dashboard/executive?year={YEAR}&quarter={QUARTER}",
        token=token
    )
    check("Analytics accepts CRM-issued token", status == 200,
          f"HTTP {status}: {data}" if status != 200 else "")

    if status == 200 and data:
        check("Executive dashboard returns team data", "team" in data)
        check("Executive dashboard returns leaderboard", "leaderboard" in data and isinstance(data["leaderboard"], list))

    # ── Step 4: Test BD dashboard if we have a bd_id ─────────────────────────
    if bd_id:
        print(f"\n{BOLD}4. BD Dashboard (bd_id: {bd_id[:20]}...){RESET}")
        status, data = http_get(
            f"{ANALYTICS_BASE_URL}/api/analytics/dashboard/bd?year={YEAR}&quarter={QUARTER}&bd_id={bd_id}",
            token=token
        )
        check("BD dashboard returns 200", status == 200,
              f"HTTP {status}" if status != 200 else "")

        if status == 200 and data:
            for key in ["total_revenue", "open_pipeline", "quota", "attainment_pct"]:
                check(f"  BD dashboard has '{key}'", key in data)
    else:
        print(f"\n{BOLD}4. BD Dashboard{RESET}")
        print(f"  {YELLOW}⚠ Skipped (no TEST_BD_ID set){RESET}")

    # ── Step 5: Reports ──────────────────────────────────────────────────────
    print(f"\n{BOLD}5. Analytics Reports{RESET}")
    report_endpoints = [
        ("/api/analytics/reports/pipeline", "Pipeline"),
        ("/api/analytics/reports/quota", "Quota"),
        ("/api/analytics/reports/loss-analysis", "Loss Analysis"),
        ("/api/analytics/reports/sales-cycle", "Sales Cycle"),
        ("/api/analytics/reports/win-rate", "Win Rate"),
    ]
    for path, name in report_endpoints:
        status, data = http_get(
            f"{ANALYTICS_BASE_URL}{path}?year={YEAR}&quarter={QUARTER}",
            token=token
        )
        check(f"{name} report returns 200", status == 200,
              f"HTTP {status}" if status != 200 else "")

    # ── Step 6: Auth boundary test ───────────────────────────────────────────
    print(f"\n{BOLD}6. Auth Boundary{RESET}")
    status, _ = http_get(
        f"{ANALYTICS_BASE_URL}/api/analytics/dashboard/executive?year={YEAR}&quarter={QUARTER}"
    )
    check("Analytics rejects request without token", status in (401, 403),
          f"Expected 401/403, got {status}")

    status, _ = http_get(
        f"{ANALYTICS_BASE_URL}/api/analytics/dashboard/executive?year={YEAR}&quarter={QUARTER}",
        token="invalid.fake.token"
    )
    check("Analytics rejects invalid token", status in (401, 403),
          f"Expected 401/403, got {status}")

    _print_summary()
    return failed == 0


def _print_summary():
    total = passed + failed
    print(f"\n{'─' * 60}")
    print(f"Results: {GREEN}{passed} passed{RESET} / {RED}{failed} failed{RESET} / {total} total")
    if failed:
        print(f"\n{YELLOW}Tips:{RESET}")
        print(f"  - Check both services are running and sharing the same DB")
        print(f"  - Verify JWT_SECRET matches in both .env files")
        print(f"  - Run: python scripts/generate_token.py --from-db")
    else:
        print(f"\n{GREEN}All cross-service checks passed!{RESET}")
    print()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
