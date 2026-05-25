import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("usage-api")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
USAGE_QUEUE = "usage_queue"

redis_client: aioredis.Redis | None = None


class UsageEvent(BaseModel):
    call_id: str | None = None
    event_type: str
    org_id: int
    provider: str
    model: str
    session_id: str
    session_name: str | None = None
    timestamp: str
    usage: dict[str, Any]


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info(f"Connected to Redis at {REDIS_URL}")
    yield
    if redis_client:
        await redis_client.close()


app = FastAPI(title="Usage API", lifespan=lifespan)


@app.post("/usage", status_code=202)
async def ingest_usage(events: list[UsageEvent]):
    for event in events:
        if event.call_id is None:
            event.call_id = str(uuid.uuid4())
        payload = event.model_dump()
        await redis_client.lpush(USAGE_QUEUE, json.dumps(payload))
        logger.info(f"Enqueued usage event: call_id={event.call_id[:16]} model={event.model} session={event.session_id[:8]}")
    return JSONResponse(status_code=202, content={"status": "accepted", "count": len(events)})
