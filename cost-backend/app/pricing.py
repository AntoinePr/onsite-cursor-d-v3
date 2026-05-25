import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.database import async_session
from app.models import ListPrice, PricingUpdate

logger = logging.getLogger("cost-backend")

SEED_PRICES = [
    ("openai", "gpt-4o-mini", "prompt_tokens", 0.15 / 1_000_000),
    ("openai", "gpt-4o-mini", "cached_tokens", 0.075 / 1_000_000),
    ("openai", "gpt-4o-mini", "completion_tokens", 0.60 / 1_000_000),
    ("openai", "gpt-4o", "prompt_tokens", 2.50 / 1_000_000),
    ("openai", "gpt-4o", "cached_tokens", 1.25 / 1_000_000),
    ("openai", "gpt-4o", "completion_tokens", 10.00 / 1_000_000),
]

_pricing_cache: dict[tuple[str, str, str], float] = {}
_last_loaded: datetime | None = None

DEFAULT_UNIT_COST = 0.0


def get_unit_cost(provider: str, model: str, usage_type: str) -> float:
    return _pricing_cache.get((provider, model, usage_type), DEFAULT_UNIT_COST)


async def seed_pricing():
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        for provider, model, usage_type, unit_cost in SEED_PRICES:
            existing = await db.execute(
                select(ListPrice).where(
                    ListPrice.provider == provider,
                    ListPrice.model == model,
                    ListPrice.usage_type == usage_type,
                )
            )
            row = existing.scalar_one_or_none()
            if row:
                row.unit_cost = unit_cost
            else:
                db.add(ListPrice(
                    provider=provider, model=model,
                    usage_type=usage_type, unit_cost=unit_cost,
                ))

        providers = {p for p, _, _, _ in SEED_PRICES}
        for provider in providers:
            existing = await db.execute(
                select(PricingUpdate).where(PricingUpdate.provider == provider)
            )
            row = existing.scalar_one_or_none()
            if row:
                row.last_update = now
            else:
                db.add(PricingUpdate(provider=provider, last_update=now))

        await db.commit()
    logger.info(f"Seeded {len(SEED_PRICES)} list prices for {len(providers)} provider(s)")


async def load_pricing_cache():
    global _pricing_cache, _last_loaded
    async with async_session() as db:
        result = await db.execute(select(ListPrice))
        rows = result.scalars().all()

    new_cache = {}
    for row in rows:
        new_cache[(row.provider, row.model, row.usage_type)] = float(row.unit_cost)

    _pricing_cache = new_cache
    _last_loaded = datetime.now(timezone.utc)
    logger.info(f"Loaded {len(new_cache)} pricing entries into cache")


async def check_and_refresh():
    global _last_loaded
    async with async_session() as db:
        result = await db.execute(
            select(PricingUpdate.last_update).order_by(PricingUpdate.last_update.desc()).limit(1)
        )
        latest = result.scalar_one_or_none()

    if latest and (_last_loaded is None or latest > _last_loaded):
        logger.info("Pricing update detected, refreshing cache")
        await load_pricing_cache()
    else:
        logger.debug("Pricing cache is up to date")
