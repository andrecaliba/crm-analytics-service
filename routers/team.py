from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from db import get_db
from auth import get_current_user

router = APIRouter(prefix="/api/analytics/team", tags=["Team"])

BD_LIST = """
SELECT
    b.id,
    b.first_name,
    b.last_name,
    b.role
FROM bd b
WHERE b.role = 'BD_REP'
ORDER BY b.first_name, b.last_name;
"""


@router.get(
    "/bds",
    summary="List all BD reps",
    description="Returns a list of all BD_REP users for use in filters.",
)
def list_bds(
    db: Session = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    rows = [dict(r) for r in db.execute(text(BD_LIST)).mappings()]
    return {"bds": rows}
