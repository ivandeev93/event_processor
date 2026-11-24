from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=255)
    type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any]