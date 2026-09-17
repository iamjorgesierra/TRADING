"""
Endpoints HTTP del historical-data-service.

/health                     — health-check.
/ticks/{symbol}             — últimos N ticks para un instrumento.
/candles/{symbol}           — últimas N velas para un instrumento + timeframe.
/symbols                    — lista de instrumentos con datos en DB.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database.connection import get_session

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="historical-data-service",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/ticks/{symbol}", tags=["Historical Data"])
async def get_ticks(
    symbol: str,
    limit: int = Query(default=100, ge=1, le=5000, description="Número de ticks"),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Devuelve los últimos N ticks para el instrumento dado.
    Ordenados por timestamp DESC.
    """
    from historical_data_app.repositories.tick_repo import TickRepository

    repo = TickRepository(session)
    ticks = await repo.get_latest_ticks(symbol=symbol.upper(), limit=limit)

    return {
        "symbol": symbol.upper(),
        "count": len(ticks),
        "ticks": [
            {
                "timestamp": t.time.isoformat(),
                "bid": t.bid,
                "ask": t.ask,
                "spread": t.spread,
                "source": t.source,
            }
            for t in ticks
        ],
    }


@router.get("/candles/{symbol}", tags=["Historical Data"])
async def get_candles(
    symbol: str,
    timeframe: str = Query(default="M15"),
    limit: int = Query(default=200, ge=1, le=5000),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Devuelve las últimas N velas para instrumento + timeframe."""
    from historical_data_app.repositories.candle_repo import CandleRepository

    repo = CandleRepository(session)
    candles = await repo.get_latest_candles(
        symbol=symbol.upper(), timeframe=timeframe.upper(), limit=limit
    )

    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe.upper(),
        "count": len(candles),
        "candles": [
            {
                "timestamp": c.time.isoformat(),
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in candles
        ],
    }
