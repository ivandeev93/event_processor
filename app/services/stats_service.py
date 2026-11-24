from sqlalchemy.orm import Session

from app.repositories.event_repository import EventRepository
from app.repositories.task_repository import TaskRepository


class StatsService:

    @staticmethod
    def get_stats(db: Session) -> dict:
        event_counts = EventRepository.get_type_counts(db)
        task_counts = TaskRepository.get_status_counts(db)

        statuses = ("pending", "success", "failed", "skipped")

        return {
            "events_by_type": event_counts,
            "tasks_by_status": {
                status: task_counts.get(status, 0)
                for status in statuses
            },
        }