"""
Standalone JWT token generator for testing the Analytics service
WITHOUT needing the CRM running.

Usage:
    # Generate a BD_REP token (default)
    python scripts/generate_token.py

    # Generate a SALES_MANAGER token
    python scripts/generate_token.py --role SALES_MANAGER

    # Specify a custom BD ID
    python scripts/generate_token.py --bd-id "some-uuid-here"

    # Generate and export in one line (for use with test_endpoints.py)
    export TEST_TOKEN=$(python scripts/generate_token.py --role SALES_MANAGER --quiet)

    # Auto-detect a BD from the database
    python scripts/generate_token.py --from-db

The token is signed with the same JWT_SECRET from your .env file,
so the Analytics service will accept it as valid.
"""

import sys
import os
import argparse
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
from jose import jwt

load_dotenv()

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-to-a-32-char-random-string")


def get_bd_from_db(role: str = "BD_REP"):
    """Look up a real BD from the database to use in the token."""
    try:
        from db import engine
        from sqlalchemy import text

        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT id, first_name, last_name, role FROM bd "
                "WHERE role = :role AND is_active = true "
                "ORDER BY first_name LIMIT 1"
            ), {"role": role}).fetchone()

            if row:
                return {
                    "bd_id": str(row.id),
                    "name": f"{row.first_name} {row.last_name}",
                    "role": row.role,
                }
            return None
    except Exception as e:
        print(f"  Could not connect to database: {e}", file=sys.stderr)
        return None


def generate_token(bd_id: str, role: str, email: str = None, expires_hours: int = 24):
    """Generate a JWT token matching the CRM's format."""
    now = datetime.now(timezone.utc)
    payload = {
        "bdId": bd_id,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=expires_hours)).timestamp()),
    }
    if email:
        payload["email"] = email

    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def main():
    parser = argparse.ArgumentParser(description="Generate JWT tokens for analytics testing")
    parser.add_argument("--bd-id", default="00000000-0000-0000-0000-000000000001",
                        help="BD UUID to embed in the token (default: placeholder UUID)")
    parser.add_argument("--role", default="BD_REP", choices=["BD_REP", "SALES_MANAGER"],
                        help="Role to assign (default: BD_REP)")
    parser.add_argument("--email", default=None, help="Optional email claim")
    parser.add_argument("--expires", type=int, default=24, help="Token expiry in hours (default: 24)")
    parser.add_argument("--from-db", action="store_true",
                        help="Auto-detect a BD from the database instead of using --bd-id")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="Print only the token (for use with export)")
    args = parser.parse_args()

    bd_id = args.bd_id
    role = args.role
    name = None

    if args.from_db:
        result = get_bd_from_db(role)
        if result:
            bd_id = result["bd_id"]
            role = result["role"]
            name = result["name"]
        else:
            if not args.quiet:
                print("No BD found in database, using placeholder UUID.", file=sys.stderr)

    token = generate_token(bd_id, role, args.email, args.expires)

    if args.quiet:
        print(token)
    else:
        print()
        print("=" * 60)
        print("  JWT Token Generated")
        print("=" * 60)
        if name:
            print(f"  Name     : {name}")
        print(f"  BD ID    : {bd_id}")
        print(f"  Role     : {role}")
        print(f"  Expires  : {args.expires}h from now")
        print(f"  Secret   : {JWT_SECRET[:8]}...")
        print()
        print(f"  Token:")
        print(f"  {token}")
        print()
        print("  Quick export:")
        print(f'  export TEST_TOKEN="{token}"')
        print(f'  export TEST_BD_ID="{bd_id}"')
        print("=" * 60)


if __name__ == "__main__":
    main()
