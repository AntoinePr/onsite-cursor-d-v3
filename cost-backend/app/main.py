import asyncio
import json
import logging
import os
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import redis.asyncio as aioredis
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select, text

from app.database import async_session, init_db
from app.models import Cost, Usage
from app.pricing import get_unit_cost

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cost-backend")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
USAGE_QUEUE = "usage_queue"

redis_client: aioredis.Redis | None = None

RANGE_PRESETS = {
    "10m": {"window": timedelta(minutes=10), "granularity": "minute", "bucket_count": 10, "bucket_step": timedelta(minutes=1)},
    "1h":  {"window": timedelta(hours=1),    "granularity": "5min",   "bucket_count": 12, "bucket_step": timedelta(minutes=5)},
    "1d":  {"window": timedelta(days=1),     "granularity": "hour",   "bucket_count": 24, "bucket_step": timedelta(hours=1)},
    "1m":  {"window": timedelta(days=30),    "granularity": "day",    "bucket_count": 30, "bucket_step": timedelta(days=1)},
}

ALLOWED_GROUP_BY = {"provider", "model", "usage_type", "session_id"}


def _trunc_expr(granularity: str) -> str:
    if granularity == "minute":
        return "date_trunc('minute', c.created_at)"
    elif granularity == "5min":
        return "(timestamp '1970-01-01' + INTERVAL '5 min' * FLOOR(EXTRACT(EPOCH FROM c.created_at) / 300)) AT TIME ZONE 'UTC'"
    elif granularity == "hour":
        return "date_trunc('hour', c.created_at)"
    elif granularity == "day":
        return "date_trunc('day', c.created_at)"
    raise ValueError(f"Unknown granularity: {granularity}")


def _trunc_bucket(dt: datetime, granularity: str) -> datetime:
    if granularity == "minute":
        return dt.replace(second=0, microsecond=0)
    elif granularity == "5min":
        return dt.replace(minute=(dt.minute // 5) * 5, second=0, microsecond=0)
    elif granularity == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif granularity == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unknown granularity: {granularity}")


SKIP_USAGE_TYPES = {"total_tokens"}


def flatten_usage(usage: dict) -> list[tuple[str, int]]:
    rows = []
    for key, value in usage.items():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if int(value) > 0 and key not in SKIP_USAGE_TYPES:
                rows.append((key, int(value)))
        elif isinstance(value, dict):
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, (int, float)) and not isinstance(sub_value, bool):
                    if int(sub_value) > 0 and sub_key not in SKIP_USAGE_TYPES:
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


async def get_history(
    granularity: str,
    window_start: datetime,
    window_end: datetime,
    metric: str,
    group_by: str | None = None,
) -> dict:
    trunc = _trunc_expr(granularity)
    if metric == "cost":
        value_expr = "COALESCE(SUM(c.total_cost), 0)"
    else:
        value_expr = "COALESCE(SUM(u.quantity), 0)"

    group_col_sql = ""
    extra_select = ""
    extra_group = ""
    if group_by and group_by in ALLOWED_GROUP_BY:
        extra_select = f", u.{group_by} AS group_val"
        extra_group = f", u.{group_by}"

    sql = f"""
        SELECT
            {trunc} AS bucket
            {extra_select},
            {value_expr} AS val
        FROM costs c
        JOIN usage u ON c.usage_id = u.id
        WHERE c.created_at >= :window_start AND c.created_at < :window_end
        GROUP BY {trunc}{extra_group}
        ORDER BY bucket
    """

    async with async_session() as db:
        result = await db.execute(
            text(sql),
            {"window_start": window_start, "window_end": window_end},
        )
        rows = result.all()

    preset = None
    for p in RANGE_PRESETS.values():
        if p["granularity"] == granularity:
            preset = p
            break

    bucket_step = preset["bucket_step"] if preset else timedelta(minutes=1)
    start = _trunc_bucket(window_start, granularity)

    bucket_times = []
    bt = start
    while bt < window_end:
        bucket_times.append(bt)
        bt = bt + bucket_step

    if group_by and group_by in ALLOWED_GROUP_BY:
        grouped: dict[datetime, dict[str, float]] = defaultdict(dict)
        all_groups: set[str] = set()
        for row in rows:
            grouped[row.bucket][row.group_val] = float(row.val)
            all_groups.add(row.group_val)

        groups_list = sorted(all_groups)
        buckets = []
        cumulative = 0.0
        for bt in bucket_times:
            breakdown = {g: grouped.get(bt, {}).get(g, 0.0) for g in groups_list}
            value = sum(breakdown.values())
            cumulative += value
            buckets.append({
                "bucket": bt.isoformat(),
                "value": round(value, 10),
                "cumulative": round(cumulative, 10),
                "breakdown": {k: round(v, 10) for k, v in breakdown.items()},
            })
    else:
        db_map: dict[datetime, float] = {}
        for row in rows:
            db_map[row.bucket] = float(row.val)

        groups_list = None
        buckets = []
        cumulative = 0.0
        for bt in bucket_times:
            value = db_map.get(bt, 0.0)
            cumulative += value
            buckets.append({
                "bucket": bt.isoformat(),
                "value": round(value, 10),
                "cumulative": round(cumulative, 10),
            })

    window_total = sum(b["value"] for b in buckets)

    return {
        "window_total": round(window_total, 10),
        "groups": groups_list,
        "buckets": buckets,
    }


async def get_cumulative(metric: str = "cost", session_id: str | None = None) -> dict:
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _snap_window(range_key: str, offset: int, now: datetime) -> tuple[datetime, datetime]:
    """Compute window_start and window_end snapped to clock boundaries.

    10m: sliding window (last 10 minutes from now, shifted by offset * 10m)
    1h:  snap to clock hours   (e.g. 17:00-18:00)
    1d:  snap to calendar days (e.g. 2026-05-25 00:00 - 2026-05-26 00:00)
    1m:  snap to calendar months (e.g. 2026-05-01 - 2026-06-01)
    """
    if range_key == "10m":
        window_end = now - timedelta(minutes=10) * offset
        window_start = window_end - timedelta(minutes=10)
        return window_start, window_end

    if range_key == "1h":
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        window_end = current_hour + timedelta(hours=1)
        window_end = window_end - timedelta(hours=1) * offset
        window_start = window_end - timedelta(hours=1)
        return window_start, window_end

    if range_key == "1d":
        current_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        window_end = current_day + timedelta(days=1)
        window_end = window_end - timedelta(days=1) * offset
        window_start = window_end - timedelta(days=1)
        return window_start, window_end

    if range_key == "1m":
        year, month = now.year, now.month
        first_of_current = now.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)
        if month == 12:
            first_of_next = first_of_current.replace(year=year + 1, month=1)
        else:
            first_of_next = first_of_current.replace(month=month + 1)
        window_end = first_of_next
        for _ in range(offset):
            window_end = window_end.replace(day=1) - timedelta(days=1)
            window_end = window_end.replace(day=1)
            if window_end.month == 12:
                window_end = window_end.replace(year=window_end.year + 1, month=1)
            else:
                window_end = window_end.replace(month=window_end.month + 1)
        window_start = window_end.replace(day=1) - timedelta(days=1)
        window_start = window_start.replace(day=1)
        return window_start, window_end

    raise ValueError(f"Unknown range: {range_key}")


async def _history_handler(metric: str, range_key: str, offset: int, group_by: str | None):
    if range_key not in RANGE_PRESETS:
        return {"error": f"Invalid range. Allowed: {list(RANGE_PRESETS.keys())}"}
    preset = RANGE_PRESETS[range_key]
    now = datetime.now(timezone.utc)
    window_start, window_end = _snap_window(range_key, offset, now)

    result = await get_history(
        granularity=preset["granularity"],
        window_start=window_start,
        window_end=window_end,
        metric=metric,
        group_by=group_by,
    )
    return {
        "range": range_key,
        "offset": offset,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "now": now.isoformat(),
        **result,
    }


@app.get("/cost/history")
async def cost_history(
    range: str = Query("10m", alias="range"),
    offset: int = Query(0),
    group_by: str | None = Query(None),
):
    return await _history_handler("cost", range, offset, group_by)


@app.get("/usage/history")
async def usage_history(
    range: str = Query("10m", alias="range"),
    offset: int = Query(0),
    group_by: str | None = Query(None),
):
    return await _history_handler("usage", range, offset, group_by)


@app.get("/cost")
async def get_cost_total():
    return await get_cumulative("cost")


@app.get("/cost/{session_id}")
async def get_cost_session(session_id: str):
    result = await get_cumulative("cost", session_id)
    return {"session_id": session_id, **result}


@app.get("/usage")
async def get_usage_total():
    return await get_cumulative("usage")
