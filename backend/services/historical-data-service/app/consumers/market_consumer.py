"""
Consumer de market data desde Redis Streams.

Consume el stream market:ticks y persiste cada tick en TimescaleDB.

Decisión técnica — async_sessionmaker vs sesión global:
- Crear una sesión por mensaje (via async_sessionmaker) es el patrón
  correcto en async: evita estados compartidos entre coroutines.
- El overhead de crear sesiones es mínimo con asyncpg (pool de conexiones).
- El commit es por mensaje: en producción con volumen alto se puede
  batching (commit cada N mensajes) para mejorar throughput.
"""

import asyncio
from datetime import datetime

from loguru import logger
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from shared.config.settings import get_settings
from shared.redis.streams import StreamConsumer

from app.repositories.tick_repo import TickRepository

settings = get_settings()


class MarketDataConsumer:
    """
    Consumer de ticks desde Redis Streams → TimescaleDB.

    Parámetros:
        redis:           Cliente Redis async.
        session_factory: Fábrica de sesiones SQLAlchemy async.
    """

    def __init__(
        self,
        redis: Redis,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory
        self._consumer = StreamConsumer(
            client=redis,
            stream_name=settings.stream_market_ticks,
            group_name="historical-data-service",
            consumer_name="consumer-1",
            batch_size=50,
        )

    async def start(self) -> None:
        """Inicia el bucle de consumo. Diseñado para correr como asyncio.Task."""
        logger.info("MarketDataConsumer starting...")
        await self._consumer.consume(handler=self._handle_tick)

    async def _handle_tick(self, data: dict) -> None:
        """
        Handler llamado por StreamConsumer por cada tick recibido.

        Valida los campos mínimos y persiste en TimescaleDB.
        Los errores de validación se logean sin propagar para no
        detener el consumer (mensajes malformados se ignoran).
        """
        try:
            symbol = data["symbol"]
            timestamp = datetime.fromisoformat(data["timestamp"])
            bid = float(data["bid"])
            ask = float(data["ask"])
            spread = float(data["spread"])
            source = data.get("source", "oanda")
        except (KeyError, ValueError, TypeError) as exc:
            logger.error(f"Invalid tick message  error={exc}  data={data}")
            return

        async with self._session_factory() as session:
            repo = TickRepository(session)
            await repo.upsert_tick(
                symbol=symbol,
                timestamp=timestamp,
                bid=bid,
                ask=ask,
                spread=spread,
                source=source,
            )

        logger.debug(f"Tick persisted  symbol={symbol}  bid={bid}  ask={ask}")
