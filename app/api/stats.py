from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.stats_service import StatsService


router = APIRouter(tags=["Statistics"])


@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    return StatsService.get_stats(db)