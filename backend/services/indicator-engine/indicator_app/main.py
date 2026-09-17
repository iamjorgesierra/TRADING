"""
indicator-engine — Punto de entrada FastAPI.

Responsabilidades:
1. Consumir velas del stream `market:candles` (Redis Streams).
2. Mantener buffers de velas en memoria por símbolo+timeframe.
3. Calcular indicadores técnicos (EMA, RSI, MACD, ATR, BB) en tiempo real.
4. Publicar snapshots de indicadores en `indicators:values`.
5. Exponer endpoints HTTP para consulta bajo demanda.

Flujo de datos:
    market:candles → CandleConsumer → CandleBuffer → IndicatorCalculator
                   → IndicatorPublisher → indicators:values
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from loguru import logger

from shared.config.settings import get_settings
from shared.logging.logger import setup_logging
from shared.redis.client import close_redis_client, get_redis_client
from shared.redis.streams import StreamConsumer, StreamProducer

from indicator_app.core.buffer import BufferRegistry
from indicator_app.core.calculator import IndicatorCalculator
from indicator_app.consumers.candle_consumer import CandleConsumer
from indicator_app.events.indicator_publisher import IndicatorPublisher
from indicator_app.api.routes import router

settings = get_settings()
setup_logging("indicator-engine", settings.log_level)

_consumer_task: asyncio.Task | None = None

STREAM_INDICATORS_MAXLEN = 50_000


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Gestiona el ciclo de vida del indicator-engine.

    STARTUP:
    - Conecta a Redis.
    - Inicializa BufferRegistry y IndicatorCalculator.
    - Crea StreamConsumer (market:candles) y StreamProducer (indicators:values).
    - Lanza CandleConsumer como asyncio.Task en background.

    SHUTDOWN:
    - Cancela la consumer task.
    - Cierra Redis.
    """
    global _consumer_task

    redis = await get_redis_client(settings.redis_url)

    buffer_registry = BufferRegistry(maxlen=500)
    calculator = IndicatorCalculator()

    producer = StreamProducer(
        client=redis,
        stream_name=settings.stream_indicators,
        max_len=STREAM_INDICATORS_MAXLEN,
    )
    publisher = IndicatorPublisher(producer=producer)

    stream_consumer = StreamConsumer(
        client=redis,
        stream_name=settings.stream_market_candles,
        group_name="indicator-engine",
        consumer_name="consumer-1",
        batch_size=20,
    )

    candle_consumer = CandleConsumer(
        stream_consumer=stream_consumer,
        buffer_registry=buffer_registry,
        calculator=calculator,
        indicator_publisher=publisher,
    )

    app.state.redis = redis
    app.state.buffer_registry = buffer_registry
    app.state.calculator = calculator
    app.state.consumer = candle_consumer

    _consumer_task = asyncio.create_task(
        candle_consumer.start(),
        name="candle-consumer",
    )
    logger.info("indicator-engine iniciado — consumiendo {}", settings.stream_market_candles)

    yield

    # ── SHUTDOWN ────────────────────────────────────────────────────────
    if _consumer_task and not _consumer_task.done():
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass

    await close_redis_client(redis)
    logger.info("indicator-engine detenido correctamente")


app = FastAPI(
    title="Indicator Engine",
    description="Cálculo de indicadores técnicos en tiempo real para la plataforma de trading.",
    version="1.0.0",
    lifespan=lifespan,
)

indicator_app.include_router(router)
