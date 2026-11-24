import logging
import time

from app.database import SessionLocal
from app.models.event import Event
from app.repositories.task_repository import TaskRepository
from app.services.exceptions import PermanentProcessingError
from app.services.task_processor import process_event


logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 2


def process_one_task() -> bool:
    """
    Обрабатывает одну доступную задачу из очереди.

    Возвращает:
        True — задача была получена и обработана;
        False — доступных задач нет.
    """
    # Получаем задачу и устанавливаем блокировку.
    with SessionLocal() as db:
        claimed_task = TaskRepository.claim_next(db)

    if claimed_task is None:
        return False

    task_id, event_pk, attempts = claimed_task

    try:
        # Загружаем событие из базы данных.
        with SessionLocal() as db:
            event = db.get(Event, event_pk)

        if event is None:
            raise PermanentProcessingError(
                f"Event with id={event_pk} not found"
            )

        # Выполняем обработку события.
        process_event(event)

        # Фиксируем успешное выполнение.
        with SessionLocal() as db:
            TaskRepository.mark_success(db, task_id)

        logger.info("Task %s completed successfully", task_id)

    except PermanentProcessingError as exc:
        # Ошибки, которые не имеет смысла повторять.
        with SessionLocal() as db:
            TaskRepository.mark_skipped(
                db=db,
                task_id=task_id,
                error_message=str(exc),
            )

        logger.warning(
            "Task %s skipped: %s",
            task_id,
            exc,
        )

    except Exception as exc:
        # Временные ошибки: планируем повторную попытку.
        with SessionLocal() as db:
            TaskRepository.schedule_retry(
                db=db,
                task_id=task_id,
                attempts=attempts,
                error_message=str(exc),
            )

        logger.exception(
            "Task %s failed on attempt %s",
            task_id,
            attempts,
        )

    return True


def recover_tasks() -> None:
    """
    Проверяет задачи, которые могли остаться незавершёнными
    после аварийного завершения worker.
    """
    with SessionLocal() as db:
        recovered_count = TaskRepository.recover_expired_tasks(db)

    if recovered_count:
        logger.warning(
            "Recovered %s expired tasks",
            recovered_count,
        )


def run_worker() -> None:
    """
    Запускает бесконечный цикл обработки очереди.
    """
    logger.info("Worker started")

    while True:
        try:
            recover_tasks()

            task_found = process_one_task()

            if not task_found:
                time.sleep(POLL_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logger.info("Worker stopped")
            break

        except Exception:
            logger.exception("Unexpected worker error")
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    run_worker()
