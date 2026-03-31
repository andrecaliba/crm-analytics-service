#!/usr/bin/env bash
# =============================================================================
# Quick local dev setup for cross-service testing
# =============================================================================
# Run this from the crm-analytics-service/ directory.
# It starts just Postgres (via Docker), then you run both services manually.
#
# Usage:
#   chmod +x scripts/dev_setup.sh
#   ./scripts/dev_setup.sh
# =============================================================================

set -e

GREEN='\033[92m'
YELLOW='\033[93m'
RED='\033[91m'
BOLD='\033[1m'
RESET='\033[0m'

echo -e "\n${BOLD}Cross-Service Dev Setup${RESET}\n"

# ── Step 1: Start Postgres ───────────────────────────────────────────────────
echo -e "${BOLD}1. Starting PostgreSQL...${RESET}"
if command -v docker &> /dev/null; then
    docker run -d \
        --name sales-crm-postgres \
        -e POSTGRES_DB=sales_crm_dev \
        -e POSTGRES_USER=crm_user \
        -e POSTGRES_PASSWORD=local_dev_password \
        -p 5433:5432 \
        postgres:16-alpine 2>/dev/null \
    && echo -e "  ${GREEN}✓${RESET} Postgres started on port 5433" \
    || echo -e "  ${YELLOW}ℹ${RESET} Postgres already running (or use docker-compose)"
else
    echo -e "  ${RED}✗${RESET} Docker not found. Install Docker or start Postgres manually on port 5433."
    exit 1
fi

echo -e "  Waiting for Postgres to be ready..."
for i in {1..15}; do
    if pg_isready -h localhost -p 5433 -U crm_user -q 2>/dev/null; then
        echo -e "  ${GREEN}✓${RESET} Postgres is ready"
        break
    fi
    sleep 1
done

# ── Step 2: Copy dev env ────────────────────────────────────────────────────
echo -e "\n${BOLD}2. Environment setup${RESET}"
if [ ! -f .env ]; then
    cp .env.dev .env
    echo -e "  ${GREEN}✓${RESET} Created .env from .env.dev"
else
    echo -e "  ${YELLOW}ℹ${RESET} .env already exists (not overwriting)"
fi

# ── Step 3: Remind about CRM .env ───────────────────────────────────────────
CRM_PATH="../project-sales-crm"
if [ -d "$CRM_PATH" ]; then
    echo -e "\n${BOLD}3. CRM repo detected at ${CRM_PATH}${RESET}"
    if [ ! -f "$CRM_PATH/.env" ]; then
        echo -e "  ${YELLOW}→${RESET} Copy .env.dev to the CRM repo too:"
        echo -e "    cp .env.dev ${CRM_PATH}/.env"
    else
        echo -e "  ${GREEN}✓${RESET} CRM .env exists"
        echo -e "  ${YELLOW}→${RESET} Make sure DATABASE_URL and JWT_SECRET match this service"
    fi
else
    echo -e "\n${BOLD}3. CRM repo not found at ${CRM_PATH}${RESET}"
    echo -e "  ${YELLOW}→${RESET} Clone it alongside this repo, or adjust the path"
fi

# ── Step 4: Print next steps ────────────────────────────────────────────────
echo -e "\n${BOLD}═══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Next steps:${RESET}"
echo -e "${BOLD}═══════════════════════════════════════════════════${RESET}"
echo -e ""
echo -e "  ${BOLD}Terminal 1 — CRM (Zeandy):${RESET}"
echo -e "    cd ../project-sales-crm"
echo -e "    uvicorn main:app --reload --port 8000"
echo -e ""
echo -e "  ${BOLD}Terminal 2 — Analytics (Andre):${RESET}"
echo -e "    uvicorn main:app --reload --port 8001"
echo -e ""
echo -e "  ${BOLD}Terminal 3 — Tests:${RESET}"
echo -e "    # Generate a test token (no CRM needed)"
echo -e "    python scripts/generate_token.py --from-db --role SALES_MANAGER"
echo -e ""
echo -e "    # Run Andre's endpoint tests"
echo -e "    export TEST_TOKEN=\$(python scripts/generate_token.py --from-db -q --role SALES_MANAGER)"
echo -e "    python tests/test_endpoints.py"
echo -e ""
echo -e "    # Run cross-service integration tests"
echo -e "    python tests/test_cross_service.py"
echo -e ""
echo -e "    # Run schema contract check (from CRM repo)"
echo -e "    cd ../project-sales-crm"
echo -e "    DATABASE_URL=\$DATABASE_URL python tests/test_schema_contract.py"
echo -e ""
echo -e "  ${BOLD}Cleanup:${RESET}"
echo -e "    docker stop sales-crm-postgres && docker rm sales-crm-postgres"
echo -e ""
