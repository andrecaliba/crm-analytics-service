import logging
import os

from dotenv import load_dotenv
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

load_dotenv()

logger = logging.getLogger(__name__)

# Must match lib/auth.ts fallback exactly.
# In production set JWT_SECRET to the same value in both Railway services.
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-to-a-32-char-random-string")

logger.info("Analytics auth using JWT_SECRET starting with: %s...", JWT_SECRET[:8])

bearer_scheme = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])

        if "bdId" not in payload or "role" not in payload:
            logger.warning("Token missing bdId or role. Keys present: %s", list(payload.keys()))
            raise HTTPException(status_code=401, detail="Token payload missing required fields")

        return {
            "bd_id": payload["bdId"],
            "role":  payload["role"],
            "email": payload.get("email"),
        }

    except JWTError as e:
        # Log the first 30 chars of the token so you can compare with what the CRM signed
        logger.warning("JWT decode failed: %s | token prefix: %s...", e, token[:30])
        raise HTTPException(status_code=401, detail=f"Invalid or expired token: {e}")


def require_manager(user: dict = Depends(get_current_user)) -> dict:
    if user["role"] != "SALES_MANAGER":
        raise HTTPException(status_code=403, detail="Access restricted to Sales Manager")
    return user


def require_bd_or_manager(user: dict = Depends(get_current_user)) -> dict:
    return user