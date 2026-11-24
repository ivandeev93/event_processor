import random

from app.models.event import Event
from app.services.exceptions import PermanentProcessingError


def process_event(event: Event) -> None:
    """
    Имитирует бизнес-обработку события.

    Временные ошибки приводят к повторной попытке.
    Постоянные ошибки переводят задачу в skipped.
    """

    if event.type == "payment_succeeded":
        # Позволяет принудительно имитировать временную ошибку
        # при тестировании механизма повторных попыток.
        if event.payload.get("simulate_failure") is True:
            raise RuntimeError("Simulated temporary payment error")

        # В обычном режиме иногда имитируем временный сбой.
        if random.random() < 0.3:
            raise RuntimeError("Temporary payment processing error")

        print(f"Payment processed successfully: {event.event_id}")

    elif event.type == "user_registered":
        print(f"User registration processed: {event.event_id}")

    else:
        raise PermanentProcessingError(
            f"Unsupported event type: {event.type}"
        )
