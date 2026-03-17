"""
AUTH — temporarily disabled for local testing.
All endpoints return a dummy user with SALES_MANAGER role.

To re-enable JWT: replace this file with the original auth.py
(use auth.py.bak if you saved it, or restore from git).
"""

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends

bearer_scheme = HTTPBearer(auto_error=False)

DUMMY_USER = {"bd_id": "00000000-0000-0000-0000-000000000000", "role": "SALES_MANAGER"}


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    return DUMMY_USER


def require_manager(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    return DUMMY_USER


def require_bd_or_manager(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    return DUMMY_USER