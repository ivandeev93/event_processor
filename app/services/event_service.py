from sqlalchemy.orm import Session

from app.repositories.event_repository import EventRepository
from app.repositories.task_repository import TaskRepository
from app.schemas.event import EventCreate


class EventService:
    @staticmethod
    def receive_event(db: Session, event_data: EventCreate) -> dict:
        """
        Принимает событие и создаёт задачу на его обработку.
        Создание события и задачи выполняется в одной транзакции.
        """
        with db.begin():
            event_pk, created = EventRepository.create_if_not_exists(
                db=db,
                event_id=event_data.event_id,
                event_type=event_data.type,
                payload=event_data.payload,
            )

            TaskRepository.create_if_not_exists(
                db=db,
                event_pk=event_pk,
            )

        return {
            "event_id": event_data.event_id,
            "created": created,
        }