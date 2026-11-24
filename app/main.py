from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.database import Base, engine
from app.models import Event, Task
from app.api.webhooks import router as webhooks_router
from app.api.stats import router as stats_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(
    title="Event Processing Service",
    description="Webhook receiver with database-backed task queue",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(webhooks_router)
app.include_router(stats_router)


@app.get("/health")
def health_check():
    return {"status": "ok"}