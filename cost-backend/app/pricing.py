PRICING: dict[tuple[str, str], dict[str, float]] = {
    ("openai", "gpt-4o-mini"): {
        "prompt_tokens": 0.15 / 1_000_000,
        "cached_tokens": 0.075 / 1_000_000,
        "completion_tokens": 0.60 / 1_000_000,
    },
    ("openai", "gpt-4o"): {
        "prompt_tokens": 2.50 / 1_000_000,
        "cached_tokens": 1.25 / 1_000_000,
        "completion_tokens": 10.00 / 1_000_000,
    },
}

DEFAULT_UNIT_COST = 0.0


def get_unit_cost(provider: str, model: str, usage_type: str) -> float:
    model_pricing = PRICING.get((provider, model), {})
    return model_pricing.get(usage_type, DEFAULT_UNIT_COST)
