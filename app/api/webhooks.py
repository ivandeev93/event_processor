from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.event import EventCreate
from app.services.event_service import EventService


router = APIRouter(tags=["Webhooks"])


@router.post("/webhook", status_code=200)
def receive_webhook(
    event_data: EventCreate,
    db: Session = Depends(get_db),
):
    return EventService.receive_event(
        db=db,
        event_data=event_data,
    )