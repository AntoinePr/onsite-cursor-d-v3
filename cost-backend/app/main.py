import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import func, select, text

from app.database import async_session, init_db
from app.models import Cost, Usage
from app.pricing import get_unit_cost

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cost-backend")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
USAGE_QUEUE = "usage_queue"

redis_client: aioredis.Redis | None = None
ws_subscribers: dict[str, set[WebSocket]] = {}


def flatten_usage(usage: dict) -> list[tuple[str, int]]:
    """Walk the provider's usage dict and extract (usage_type, quantity) pairs.

    Top-level numeric keys become rows directly.
    Nested *_details dicts are walked to extract their numeric children.
    """
    rows = []
    for key, value in usage.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if int(value) > 0:
                rows.append((key, int(value)))
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, (int, float)) and not isinstance(sub_value, bool):
                    if int(sub_value) > 0:
                        rows.append((sub_key, int(sub_value)))
    return rows


async def process_usage_event(event: dict):
    event_id = uuid.uuid4()
    org_id = event.get("org_id", 1)
    session_id = event["session_id"]
    provider = event["provider"]
    model = event["model"]
    event_type = event["event_type"]
    usage_raw = event.get("usage", {})

    metering_rows = flatten_usage(usage_raw)
    if not metering_rows:
        logger.warning(f"No metering data in event for session {session_id[:8]}")
        return

    total_event_cost = Decimal(0)
    total_event_tokens = 0

    async with async_session() as db:
        for usage_type, quantity in metering_rows:
            usage_row = Usage(
                event_id=event_id,
                org_id=org_id,
                session_id=session_id,
                provider=provider,
                model=model,
                event_type=event_type,
                usage_type=usage_type,
                quantity=quantity,
            )
            db.add(usage_row)
            await db.flush()

            unit_cost = Decimal(str(get_unit_cost(provider, model, usage_type)))
            row_total_cost = unit_cost * quantity

            cost_row = Cost(
                usage_id=usage_row.id,
                usage_type=usage_type,
                unit_cost=unit_cost,
                total_cost=row_total_cost,
            )
            db.add(cost_row)

            total_event_cost += row_total_cost
            total_event_tokens += quantity

        await db.commit()

    logger.info(
        f"Processed event {event_id}: {len(metering_rows)} usage rows, "
        f"total_cost=${float(total_event_cost):.8f}, session={session_id[:8]}"
    )

    cumulative = await get_cumulative_costs()
    history = await get_cost_history_minutes(15)

    update_msg = {
        "type": "cost_update",
        "session_id": session_id,
        "latest": {
            "model": model,
            "tokens": total_event_tokens,
            "cost": float(total_event_cost),
        },
        "cumulative": cumulative,
        "history": history,
    }

    all_subscribers = set()
    for subs in ws_subscribers.values():
        all_subscribers.update(subs)
    dead = set()
    for ws_conn in all_subscribers:
        try:
            await ws_conn.send_json(update_msg)
        except Exception:
            dead.add(ws_conn)
    for subs in ws_subscribers.values():
        subs -= dead


async def get_cumulative_costs(session_id: str | None = None) -> dict:
    async with async_session() as db:
        query = (
            select(
                func.sum(Cost.total_cost).label("total_cost"),
                func.sum(Usage.quantity).label("total_tokens"),
            )
            .join(Usage, Cost.usage_id == Usage.id)
        )
        if session_id:
            query = query.where(Usage.session_id == session_id)
        result = await db.execute(query)
        row = result.one_or_none()
        if row and row.total_cost is not None:
            return {
                "total_cost": float(row.total_cost),
                "total_tokens": int(row.total_tokens),
            }
        return {"total_cost": 0.0, "total_tokens": 0}


async def get_cost_history_minutes(minutes: int = 15) -> list[dict]:
    """Return per-minute cost buckets for the last N minutes."""
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=minutes)

    async with async_session() as db:
        result = await db.execute(
            text("""
                SELECT
                    date_trunc('minute', c.created_at) AS minute,
                    COALESCE(SUM(c.total_cost), 0) AS cost
                FROM costs c
                JOIN usage u ON c.usage_id = u.id
                WHERE c.created_at >= :window_start
                GROUP BY date_trunc('minute', c.created_at)
                ORDER BY minute
            """),
            {"window_start": window_start},
        )
        db_rows = {row.minute: float(row.cost) for row in result}

    buckets = []
    cumulative = 0.0
    for i in range(minutes):
        bucket_time = window_start.replace(second=0, microsecond=0) + timedelta(minutes=i)
        cost = db_rows.get(bucket_time, 0.0)
        cumulative += cost
        buckets.append({
            "minute": bucket_time.isoformat(),
            "cost": round(cost, 10),
            "cumulative": round(cumulative, 10),
        })

    return buckets


async def redis_consumer():
    global redis_client
    logger.info("Starting Redis consumer loop")
    while True:
        try:
            result = await redis_client.brpop(USAGE_QUEUE, timeout=1)
            if result is None:
                continue
            _, raw = result
            event = json.loads(raw)
            await process_usage_event(event)
        except aioredis.ConnectionError:
            logger.warning("Redis connection lost, retrying in 2s...")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    await init_db()
    logger.info("Billing database initialized")

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    logger.info(f"Connected to Redis at {REDIS_URL}")

    consumer_task = asyncio.create_task(redis_consumer())
    yield
    consumer_task.cancel()
    if redis_client:
        await redis_client.close()


app = FastAPI(title="Cost Backend", lifespan=lifespan)


@app.websocket("/ws/costs/{session_id}")
async def ws_costs(websocket: WebSocket, session_id: str):
    await websocket.accept()

    if session_id not in ws_subscribers:
        ws_subscribers[session_id] = set()
    ws_subscribers[session_id].add(websocket)

    logger.info(f"Cost WS connected for session {session_id[:8]}")

    try:
        cumulative = await get_cumulative_costs()
        history = await get_cost_history_minutes(15)
        await websocket.send_json({
            "type": "cost_update",
            "session_id": session_id,
            "latest": None,
            "cumulative": cumulative,
            "history": history,
        })

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_subscribers.get(session_id, set()).discard(websocket)
        logger.info(f"Cost WS disconnected for session {session_id[:8]}")


@app.get("/costs/{session_id}")
async def get_costs(session_id: str):
    cumulative = await get_cumulative_costs(session_id)
    return {"session_id": session_id, **cumulative}


@app.get("/costs")
async def get_all_costs():
    cumulative = await get_cumulative_costs()
    history = await get_cost_history_minutes(15)
    return {**cumulative, "history": history}
