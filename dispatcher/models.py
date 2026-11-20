import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class DispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    destinationUrl: HttpUrl
    eventId: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    payload: dict[str, Any]

    @field_validator("payload")
    @classmethod
    def finite_json(cls, value):
        # Some JSON parsers accept NaN/Infinity extensions; PostgreSQL JSON does not.
        json.dumps(value, allow_nan=False)
        return value

    @field_validator("destinationUrl")
    @classmethod
    def bound_url(cls, value):
        if len(str(value)) > 2048:
            raise ValueError("URL must be at most 2048 characters")
        return value


class QueuedResponse(BaseModel):
    status: str = "queued"
    jobId: str
