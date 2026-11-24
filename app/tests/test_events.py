from datetime import timedelta

import pytest
from sqlalchemy import select

from app.models import Event, Task
from app.repositories.task_repository import utcnow, TaskRepository
from app.services.exceptions import PermanentProcessingError
from app import worker
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier


@pytest.fixture
def webhook_payload():
    return {
        "event_id": "event-001",
        "type": "user_registered",
        "payload": {
            "user_id": 123,
        },
    }


def test_webhook_creates_event_and_task(client, test_db, webhook_payload):
    """
    Проверяем, что webhook создаёт событие и задачу в очереди.
    """
    response = client.post("/webhook", json=webhook_payload)

    assert response.status_code == 200
    assert response.json()["created"] is True

    with test_db() as db:
        event = db.scalar(
            select(Event).where(Event.event_id == "event-001")
        )
        task = db.scalar(
            select(Task).where(Task.event_pk == event.id)
        )

        assert event is not None
        assert task is not None
        assert task.status == "pending"
        assert task.attempts == 0


def test_duplicate_webhook_does_not_create_duplicates(
    client,
    test_db,
    webhook_payload,
):
    """
    Повторный webhook не должен создавать дубликаты события и задачи.
    """
    first_response = client.post("/webhook", json=webhook_payload)
    second_response = client.post("/webhook", json=webhook_payload)

    assert first_response.status_code == 200
    assert second_response.status_code == 200

    assert first_response.json()["created"] is True
    assert second_response.json()["created"] is False

    with test_db() as db:
        events = db.scalars(select(Event)).all()
        tasks = db.scalars(select(Task)).all()

        assert len(events) == 1
        assert len(tasks) == 1


def test_invalid_webhook_returns_422(client):
    """
    Проверяем валидацию некорректного тела запроса.
    """
    response = client.post(
        "/webhook",
        json={
            "event_id": "event-002",
            "payload": {},
        },
    )

    assert response.status_code == 422


def test_worker_marks_task_as_success(
    client,
    test_db,
    webhook_payload,
    monkeypatch,
):
    """
    Проверяем успешную обработку задачи.
    """
    client.post("/webhook", json=webhook_payload)

    monkeypatch.setattr(worker, "SessionLocal", test_db)
    monkeypatch.setattr(
        worker,
        "process_event",
        lambda event: None,
    )

    result = worker.process_one_task()

    assert result is True

    with test_db() as db:
        task = db.scalar(select(Task))

        assert task.status == "success"
        assert task.attempts == 1
        assert task.locked_until is None


def test_worker_retries_and_marks_task_as_failed(
    client,
    test_db,
    monkeypatch,
):
    """
    Проверяем три неудачные попытки обработки задачи.

    После третьей попытки задача должна получить статус failed.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "payment-001",
            "type": "payment_succeeded",
            "payload": {},
        },
    )

    monkeypatch.setattr(worker, "SessionLocal", test_db)

    def failed_processing(event):
        raise RuntimeError("Simulated processing error")

    monkeypatch.setattr(worker, "process_event", failed_processing)

    for attempt in range(1, 4):
        result = worker.process_one_task()

        assert result is True

        with test_db() as db:
            task = db.scalar(select(Task))

            assert task.attempts == attempt

            if attempt < 3:
                assert task.status == "pending"

                # Перемещаем время следующей попытки в прошлое,
                # чтобы не ждать реальную задержку между retry.
                task.next_retry_at = utcnow() - timedelta(seconds=1)
                db.commit()
            else:
                assert task.status == "failed"
                assert task.locked_until is None
                assert task.next_retry_at is None


def test_worker_marks_permanent_error_as_skipped(
    client,
    test_db,
    monkeypatch,
):
    """
    Постоянная ошибка не должна приводить к повторным попыткам.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "unknown-001",
            "type": "unknown_event",
            "payload": {},
        },
    )

    monkeypatch.setattr(worker, "SessionLocal", test_db)

    def permanent_failure(event):
        raise PermanentProcessingError("Unsupported event type")

    monkeypatch.setattr(worker, "process_event", permanent_failure)

    result = worker.process_one_task()

    assert result is True

    with test_db() as db:
        task = db.scalar(select(Task))

        assert task.status == "skipped"
        assert task.attempts == 1
        assert task.locked_until is None


def test_stats_returns_event_and_task_counts(client, test_db):
    """
    Проверяем статистику по типам событий и статусам задач.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "user-001",
            "type": "user_registered",
            "payload": {},
        },
    )

    client.post(
        "/webhook",
        json={
            "event_id": "payment-001",
            "type": "payment_succeeded",
            "payload": {},
        },
    )

    response = client.get("/stats")

    assert response.status_code == 200

    data = response.json()

    assert data["events_by_type"] == {
        "user_registered": 1,
        "payment_succeeded": 1,
    }

    assert data["tasks_by_status"] == {
        "pending": 2,
        "success": 0,
        "failed": 0,
        "skipped": 0,
    }


def test_expired_task_after_third_attempt_becomes_failed(
    client,
    test_db,
):
    """
    Если worker аварийно завершился во время третьей попытки,
    задача должна перейти в failed после истечения блокировки.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "recovery-001",
            "type": "user_registered",
            "payload": {},
        },
    )

    # Имитируем три получения задачи worker.
    for attempt in range(1, 4):
        with test_db() as db:
            claimed_task = TaskRepository.claim_next(db)

            assert claimed_task is not None
            assert claimed_task[2] == attempt

            # Имитируем истечение аренды задачи:
            # worker получил задачу, но не завершил обработку.
            task = db.get(Task, claimed_task[0])
            task.locked_until = utcnow() - timedelta(seconds=1)
            db.commit()

    # После третьей попытки задача должна быть восстановлена
    # и переведена в failed.
    with test_db() as db:
        recovered_count = TaskRepository.recover_expired_tasks(db)

        assert recovered_count == 1

        task = db.scalar(select(Task))

        assert task.status == "failed"
        assert task.attempts == 3
        assert task.locked_until is None


def test_expired_task_can_be_claimed_again(
    client,
    test_db,
):
    """
    Если worker аварийно завершился после первой попытки,
    задача должна стать доступной после истечения блокировки.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "retry-recovery-001",
            "type": "user_registered",
            "payload": {},
        },
    )

    # Первый worker получает задачу.
    with test_db() as db:
        first_claim = TaskRepository.claim_next(db)

        assert first_claim is not None
        assert first_claim[2] == 1

        task = db.get(Task, first_claim[0])
        task.locked_until = utcnow() - timedelta(seconds=1)
        db.commit()

    # Второй worker получает ту же задачу после истечения блокировки.
    with test_db() as db:
        second_claim = TaskRepository.claim_next(db)

        assert second_claim is not None
        assert second_claim[0] == first_claim[0]
        assert second_claim[1] == first_claim[1]
        assert second_claim[2] == 2


def test_two_workers_cannot_claim_same_locked_task(
    client,
    test_db,
):
    """
    Проверяем, что второй worker не получает задачу,
    которая уже заблокирована первым worker.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "concurrent-001",
            "type": "user_registered",
            "payload": {},
        },
    )

    # Первый worker получает задачу.
    with test_db() as db:
        first_claim = TaskRepository.claim_next(db)

    assert first_claim is not None

    # Второй worker пытается получить задачу,
    # пока блокировка первого worker ещё действует.
    with test_db() as db:
        second_claim = TaskRepository.claim_next(db)

    assert second_claim is None


def test_concurrent_workers_cannot_claim_same_task(
    client,
    test_db,
):
    """
    Два worker одновременно пытаются получить одну задачу.
    Только один из них должен успешно её захватить.
    """
    client.post(
        "/webhook",
        json={
            "event_id": "concurrent-race-001",
            "type": "user_registered",
            "payload": {},
        },
    )

    barrier = Barrier(2)

    def claim_task():
        barrier.wait()

        with test_db() as db:
            return TaskRepository.claim_next(db)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(claim_task),
            executor.submit(claim_task),
        ]

        results = [future.result() for future in futures]

    successful_claims = [
        result for result in results
        if result is not None
    ]

    assert len(successful_claims) == 1

    with test_db() as db:
        task = db.scalar(select(Task))

        assert task.status == "pending"
        assert task.attempts == 1
        assert task.locked_until is not None
