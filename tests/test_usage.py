"""LLM usage telemetry — parse the provider envelopes, record + summarize per-call spend."""
from __future__ import annotations

from src.actions.core import Actions
from src.ingest.providers import Usage, _usage
from src.ingest.usage import record_usage, usage_summary


def test_usage_parses_the_cli_envelope_including_cost() -> None:
    data = {
        "result": "…", "total_cost_usd": 0.0021, "duration_ms": 1500,
        "usage": {"input_tokens": 1800, "output_tokens": 250,
                  "cache_read_input_tokens": 600, "cache_creation_input_tokens": 0},
    }
    u = _usage(data, "claude-haiku-4-5", with_cost=True)
    assert (u.input_tokens, u.output_tokens, u.cache_read_tokens) == (1800, 250, 600)
    assert u.cost_usd == 0.0021 and u.duration_ms == 1500


def test_usage_parses_the_api_envelope_without_cost() -> None:
    data = {"content": [{"type": "text", "text": "x"}],
            "usage": {"input_tokens": 1200, "output_tokens": 100}}
    u = _usage(data, "claude-haiku-4-5", with_cost=False)
    assert u.input_tokens == 1200 and u.output_tokens == 100
    assert u.cost_usd is None and u.duration_ms is None


async def test_record_and_summarize_usage(actions: Actions) -> None:
    await record_usage(actions.pool, purpose="session-extract",
                       usage=Usage("claude-haiku-4-5", input_tokens=2000, output_tokens=300,
                                   cost_usd=0.002))
    await record_usage(actions.pool, purpose="session-extract",
                       usage=Usage("claude-haiku-4-5", input_tokens=1000, output_tokens=200,
                                   cost_usd=0.001))
    s = await usage_summary(actions.pool, hours=24)
    assert s["calls"] == 2
    assert s["input_tokens"] == 3000 and s["output_tokens"] == 500
    assert s["total_tokens"] == 3500
    assert abs(s["cost_usd"] - 0.003) < 1e-6
    assert s["by_group"][0]["purpose"] == "session-extract"
