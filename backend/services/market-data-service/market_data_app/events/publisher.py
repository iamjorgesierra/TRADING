"""
Publisher de eventos de mercado hacia Redis Streams.

Decisión técnica:
- Separar la lógica de publicación del cliente OANDA sigue SRP.
- MarketDataPublisher actúa como adapter entre el modelo de dominio
  (TickData) y el sistema de mensajería (Redis Streams).
- Los campos del mensaje usan tipos primitivos (str, float) para
  compatibilidad con Redis Streams (solo acepta strings).
"""

from loguru import logger
from redis.asyncio import Redis

from shared.config.settings import Settings
from shared.redis.streams import StreamProducer
from shared.schemas.market import TickData


class MarketDataPublisher:
    """
    Publica ticks y candles en Redis Streams.

    Streams utilizados:
        market:ticks   — tick data en tiempo real (bid/ask/spread)
        market:candles — velas OHLCV (generadas por indicator-engine en FASE 2)
    """

    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._settings = settings
        self._tick_producer = StreamProducer(
            client=redis,
            stream_name=settings.stream_market_ticks,
            max_len=settings.stream_ticks_maxlen,
        )

    async def on_tick(self, tick: TickData) -> None:
        """
        Callback asíncrono llamado por OandaStreamingClient por cada tick.

        Serializa el TickData y lo publica en Redis Streams.
        El timestamp se convierte a ISO 8601 UTC para consistencia.
        """
        try:
            await self._tick_producer.publish(
                {
                    "symbol": tick.symbol,
                    "timestamp": tick.timestamp.isoformat(),
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "spread": tick.spread,
                    "source": tick.source,
                }
            )
            logger.debug(
                f"Tick published  symbol={tick.symbol}  bid={tick.bid}  ask={tick.ask}  spread={tick.spread}"
            )
        except Exception as exc:
            logger.error(f"Failed to publish tick  symbol={tick.symbol}  error={exc}")
