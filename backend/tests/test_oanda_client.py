"""Unit tests for OANDA client validation and candle parsing."""

import sys
from pathlib import Path

import pytest

_MARKET_DATA = Path(__file__).resolve().parents[2] / "backend" / "services" / "market-data-service"
sys.path.insert(0, str(_MARKET_DATA))

from market_data_app.core.oanda_client import OandaStreamingClient


def test_fetch_candles_rejects_invalid_count():
    client = object.__new__(OandaStreamingClient)
    with pytest.raises(ValueError, match="greater than zero"):
        # Only exercise argument validation; no settings/network needed.
        import asyncio
        asyncio.run(client.fetch_candles("EUR_USD", "H1", count=0))


def test_fetch_candles_rejects_count_above_oanda_limit():
    client = object.__new__(OandaStreamingClient)
    with pytest.raises(ValueError, match="5000"):
        import asyncio
        asyncio.run(client.fetch_candles("EUR_USD", "H1", count=5001))
