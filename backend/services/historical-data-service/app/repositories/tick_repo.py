"""
Repositorio para tick data en TimescaleDB.

Decisión técnica:
- Usar INSERT ... ON CONFLICT DO NOTHING evita errores si el consumer
  procesa un mensaje duplicado (at-least-once delivery de Redis Streams).
- Las queries usan text() de SQLAlchemy para aprovechar funciones
  específicas de TimescaleDB como time_bucket().
- El repositorio no expone la sesión externamente; está encapsulado.
"""

from datetime import datetime

from loguru import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.models.timeseries import Tick


class TickRepository:
    """Operaciones de persistencia para ticks en TimescaleDB."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def upsert_tick(
        self,
        symbol: str,
        timestamp: datetime,
        bid: float,
        ask: float,
        spread: float,
        source: str = "oanda",
    ) -> None:
        """
        Inserta un tick. Si ya existe la misma (time, symbol), no hace nada.

        La combinación (time, symbol) actúa como clave de idempotencia.
        """
        stmt = text(
            """
            INSERT INTO ticks (time, symbol, bid, ask, spread, source)
            VALUES (:time, :symbol, :bid, :ask, :spread, :source)
            ON CONFLICT (time, symbol) DO NOTHING
            """
        )
        await self._session.execute(
            stmt,
            {
                "time": timestamp,
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "source": source,
            },
        )
        await self._session.commit()

    async def get_latest_ticks(self, symbol: str, limit: int = 100) -> list[Tick]:
        """Devuelve los últimos N ticks para un símbolo, ordenados DESC."""
        result = await self._session.execute(
            select(Tick)
            .where(Tick.symbol == symbol)
            .order_by(Tick.time.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def get_ticks_in_range(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> list[Tick]:
        """Devuelve ticks entre dos timestamps (útil para backtesting)."""
        result = await self._session.execute(
            select(Tick)
            .where(Tick.symbol == symbol)
            .where(Tick.time >= start)
            .where(Tick.time <= end)
            .order_by(Tick.time.asc())
        )
        return list(result.scalars().all())

    async def count_ticks(self, symbol: str) -> int:
        """Cuenta total de ticks almacenados para un símbolo."""
        result = await self._session.execute(
            text("SELECT COUNT(*) FROM ticks WHERE symbol = :symbol"),
            {"symbol": symbol},
        )
        row = result.fetchone()
        return int(row[0]) if row else 0
