from sqlalchemy import select, func
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from app.models.event import Event


class EventRepository:

    @staticmethod
    def create_if_not_exists(
        db: Session,
        event_id: str,
        event_type: str,
        payload: dict,
    ) -> tuple[int, bool]:
        statement = (
            insert(Event)
            .values(
                event_id=event_id,
                type=event_type,
                payload=payload,
            )
            .on_conflict_do_nothing(
                index_elements=[Event.event_id],
            )
        )

        result = db.execute(statement)
        created = result.rowcount == 1

        event = db.scalar(
            select(Event).where(Event.event_id == event_id)
        )

        return event.id, created

    @staticmethod
    def get_type_counts(db: Session) -> dict[str, int]:
        statement = (
            select(Event.type, func.count(Event.id))
            .group_by(Event.type)
        )

        return {
            event_type: count
            for event_type, count in db.execute(statement)
        }