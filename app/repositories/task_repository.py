from datetime import datetime, timedelta, timezone

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from app.models.task import Task


MAX_ATTEMPTS = 3
LOCK_DURATION_SECONDS = 60


def utcnow() -> datetime:
    """
    Возвращает текущее время в UTC без информации о часовом поясе.

    SQLite не сохраняет timezone в DateTime, поэтому используем
    единый формат naive UTC для хранения и сравнений.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


class TaskRepository:
    @staticmethod
    def create_if_not_exists(db: Session, event_pk: int) -> None:
        """
        Создаёт задачу для события, если такой задачи ещё нет.
        """
        statement = (
            insert(Task)
            .values(event_pk=event_pk)
            .on_conflict_do_nothing(index_elements=[Task.event_pk])
        )

        db.execute(statement)


    @staticmethod
    def claim_next(db: Session) -> tuple[int, int, int] | None:
        """
        Атомарно получает следующую доступную задачу.

        Возвращает:
            (task_id, event_pk, attempts), если задача найдена;
            None, если доступных задач нет.

        Выбор задачи и установка блокировки выполняются
        одним SQL UPDATE с подзапросом.
        """
        now = utcnow()
        locked_until = now + timedelta(seconds=LOCK_DURATION_SECONDS)

        # Подзапрос выбирает ID следующей доступной задачи.
        candidate_id = (
            select(Task.id)
            .where(
                Task.status == "pending",
                Task.attempts < MAX_ATTEMPTS,
                or_(
                    Task.locked_until.is_(None),
                    Task.locked_until <= now,
                ),
                or_(
                    Task.next_retry_at.is_(None),
                    Task.next_retry_at <= now,
                ),
            )
            .order_by(Task.created_at, Task.id)
            .limit(1)
            .scalar_subquery()
        )

        # Одним запросом захватываем задачу и увеличиваем
        # количество попыток обработки.
        statement = (
            update(Task)
            .where(
                Task.id == candidate_id,
                Task.status == "pending",
                Task.attempts < MAX_ATTEMPTS,
                or_(
                    Task.locked_until.is_(None),
                    Task.locked_until <= now,
                ),
                or_(
                    Task.next_retry_at.is_(None),
                    Task.next_retry_at <= now,
                ),
            )
            .values(
                attempts=Task.attempts + 1,
                locked_until=locked_until,
                updated_at=now,
            )
            .returning(
                Task.id,
                Task.event_pk,
                Task.attempts,
            )
        )

        with db.begin():
            result = db.execute(statement)
            claimed_task = result.one_or_none()

        if claimed_task is None:
            return None

        return (
            claimed_task.id,
            claimed_task.event_pk,
            claimed_task.attempts,
        )


    @staticmethod
    def mark_success(db: Session, task_id: int) -> None:
        """
        Отмечает задачу как успешно обработанную.
        """
        now = utcnow()

        db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                Task.status == "pending",
            )
            .values(
                status="success",
                locked_until=None,
                next_retry_at=None,
                last_error=None,
                updated_at=now,
            )
        )

        db.commit()

    @staticmethod
    def mark_skipped(
        db: Session,
        task_id: int,
        error_message: str,
    ) -> None:
        """
        Переводит задачу в skipped, если повторная обработка не имеет смысла.
        """
        now = utcnow()

        db.execute(
            update(Task)
            .where(
                Task.id == task_id,
                Task.status == "pending",
            )
            .values(
                status="skipped",
                locked_until=None,
                next_retry_at=None,
                last_error=error_message,
                updated_at=now,
            )
        )

        db.commit()

    @staticmethod
    def schedule_retry(
        db: Session,
        task_id: int,
        attempts: int,
        error_message: str,
    ) -> None:
        """
        Планирует повторную обработку задачи.

        Задержка между попытками:
            1-я неудача: 1 секунда;
            2-я неудача: 2 секунды.

        После третьей неудачной попытки задача получает статус failed.
        """
        now = utcnow()

        if attempts >= MAX_ATTEMPTS:
            db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.status == "pending",
                )
                .values(
                    status="failed",
                    locked_until=None,
                    next_retry_at=None,
                    last_error=error_message,
                    updated_at=now,
                )
            )
        else:
            retry_delay = 2 ** (attempts - 1)
            next_retry_at = now + timedelta(seconds=retry_delay)

            db.execute(
                update(Task)
                .where(
                    Task.id == task_id,
                    Task.status == "pending",
                )
                .values(
                    status="pending",
                    locked_until=None,
                    next_retry_at=next_retry_at,
                    last_error=error_message,
                    updated_at=now,
                )
            )

        db.commit()

    @staticmethod
    def recover_expired_tasks(db: Session) -> int:
        """
        Завершает задачи, у которых исчерпан лимит попыток,
        но которые остались в pending после аварийного завершения worker.

        Для задач с меньшим количеством попыток повторное получение
        выполняется через claim_next после истечения блокировки.
        """
        now = utcnow()

        statement = (
            update(Task)
            .where(
                Task.status == "pending",
                Task.attempts >= MAX_ATTEMPTS,
                or_(
                    Task.locked_until.is_(None),
                    Task.locked_until <= now,
                ),
            )
            .values(
                status="failed",
                locked_until=None,
                next_retry_at=None,
                last_error=func.coalesce(
                    Task.last_error,
                    "Task lease expired after maximum attempts",
                ),
                updated_at=now,
            )
        )

        result = db.execute(statement)
        db.commit()

        return result.rowcount or 0

    @staticmethod
    def get_status_counts(db: Session) -> dict[str, int]:
        """
        Возвращает количество задач для каждого имеющегося статуса.
        """
        statement = (
            select(Task.status, func.count(Task.id))
            .group_by(Task.status)
        )

        return {
            status: count
            for status, count in db.execute(statement)
        }
