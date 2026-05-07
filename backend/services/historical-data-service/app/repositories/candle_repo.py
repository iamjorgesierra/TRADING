"""
Repositorio para candle data en TimescaleDB.
"""

from datetime import datetime

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.timeseries import Candle


class CandleRepository:
    """Operaciones de persistencia para velas OHLCV en TimescaleDB."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_candle(
        self,
        symbol: str,
        timeframe: str,
        timestamp: datetime,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int = 0,
        complete: bool = True,
        source: str = "oanda",
    ) -> None:
        """Inserta o actualiza una vela. ON CONFLICT actualiza OHLCV."""
        stmt = text(
            """
            INSERT INTO candles (time, symbol, timeframe, open, high, low, close, volume, complete, source)
            VALUES (:time, :symbol, :timeframe, :open, :high, :low, :close, :volume, :complete, :source)
            ON CONFLICT (time, symbol, timeframe) DO UPDATE
              SET open = EXCLUDED.open,
                  high = EXCLUDED.high,
                  low  = EXCLUDED.low,
                  close = EXCLUDED.close,
                  volume = EXCLUDED.volume,
                  complete = EXCLUDED.complete
            """
        )
        await self._session.execute(
            stmt,
            {
                "time": timestamp,
                "symbol": symbol,
                "timeframe": timeframe,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "complete": complete,
                "source": source,
            },
        )
        await self._session.commit()

    async def get_latest_candles(
        self, symbol: str, timeframe: str, limit: int = 200
    ) -> list[Candle]:
        """Devuelve las últimas N velas para símbolo + timeframe."""
        result = await self._session.execute(
            select(Candle)
            .where(Candle.symbol == symbol)
            .where(Candle.timeframe == timeframe)
            .order_by(Candle.time.desc())
            .limit(limit)
        )
        return list(result.scalars().all())
