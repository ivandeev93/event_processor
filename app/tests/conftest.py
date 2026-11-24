import pytest

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.main import app
from app.models import Event, Task


@pytest.fixture
def test_db(tmp_path):
    """
    Создаёт отдельную SQLite-базу для каждого теста.
    """
    database_path = tmp_path / "test.db"

    test_engine = create_engine(
        f"sqlite:///{database_path}",
        connect_args={"check_same_thread": False},
    )

    TestingSessionLocal = sessionmaker(
        bind=test_engine,
        autoflush=False,
        autocommit=False,
    )

    Base.metadata.create_all(bind=test_engine)

    yield TestingSessionLocal

    Base.metadata.drop_all(bind=test_engine)
    test_engine.dispose()


@pytest.fixture
def client(test_db):
    """
    Создаёт тестовый HTTP-клиент FastAPI
    с переопределённой зависимостью базы данных.
    """

    def override_get_db():
        db = test_db()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    # Не используем контекстный менеджер, чтобы не запускать
    # lifespan приложения с основной базой данных.
    test_client = TestClient(app)

    yield test_client

    app.dependency_overrides.clear()
