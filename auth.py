from jose import jwt, JWTError
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
import os

load_dotenv()

JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET is not set in .env")

bearer_scheme = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    """
    Verify the Bearer JWT token sent by Zeandy's frontend.
    Returns the decoded payload: { "bd_id": "...", "role": "BD_REP" | "SALES_MANAGER" }
    Raises 401 if token is invalid or expired.
    """
    try:
        payload = jwt.decode(
            credentials.credentials,
            JWT_SECRET,
            algorithms=["HS256"],
        )
        if "bd_id" not in payload or "role" not in payload:
            raise HTTPException(status_code=401, detail="Token payload missing required fields")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


def require_manager(user: dict = Depends(get_current_user)) -> dict:
    """
    Dependency that requires SALES_MANAGER role.
    Use this on executive dashboard and all report endpoints.
    """
    if user["role"] != "SALES_MANAGER":
        raise HTTPException(status_code=403, detail="Access restricted to Sales Manager")
    return user


def require_bd_or_manager(user: dict = Depends(get_current_user)) -> dict:
    """
    Dependency that allows both BD_REP and SALES_MANAGER.
    BD_REP can only access their own data (enforce in the route).
    """
    return user
