"""
market-data-service — punto de entrada FastAPI.

Responsabilidades:
1. Conectar a OANDA Streaming API y recibir tick data en tiempo real.
2. Publicar ticks normalizados en Redis Streams (market:ticks).
3. Exponer endpoints HTTP para health-check y estado del servicio.

Decisión técnica — lifespan vs @app.on_event:
- lifespan es la forma recomendada en FastAPI >= 0.93 (on_event deprecated).
- Permite manejo limpio de startup/shutdown con async context managers.
- La streaming task se cancela ordenadamente en shutdown.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from loguru import logger

from shared.config.settings import get_settings
from shared.logging.logger import setup_logging
from shared.redis.client import close_redis_client, get_redis_client

from market_data_app.api.routes import router
from market_data_app.core.oanda_client import OandaStreamingClient
from market_data_app.events.publisher import MarketDataPublisher

settings = get_settings()
setup_logging("market-data-service", settings.log_level)

_streaming_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Gestiona el ciclo de vida del servicio.

    STARTUP:
    - Conecta a Redis.
    - Inicia el cliente OANDA (si API key está configurada).
    - Lanza la streaming task como asyncio.Task en background.

    SHUTDOWN:
    - Cancela la streaming task.
    - Cierra el cliente Redis.
    """
    global _streaming_task

    redis = await get_redis_client(settings.redis_url)
    publisher = MarketDataPublisher(redis=redis, settings=settings)
    oanda = OandaStreamingClient(settings=settings)

    # Guardar referencias en app.state para acceso desde endpoints
    app.state.redis = redis
    app.state.publisher = publisher
    app.state.oanda_client = oanda

    if settings.oanda_api_key:
        oanda.start()
        _streaming_task = asyncio.create_task(
            oanda.stream_prices(
                symbols=settings.default_symbols,
                on_tick=publisher.on_tick,
            ),
            name="oanda-streaming",
        )
        logger.info(
            f"OANDA streaming started  symbols={settings.default_symbols}"
        )
    else:
        logger.warning(
            "OANDA_API_KEY not set — streaming disabled. "
            "Configure credentials in .env to enable live data."
        )

    logger.info("market-data-service ready")
    yield

    # ── Shutdown ──────────────────────────────────────────────
    oanda.stop()
    if _streaming_task and not _streaming_task.done():
        _streaming_task.cancel()
        try:
            await _streaming_task
        except asyncio.CancelledError:
            pass

    await close_redis_client()
    logger.info("market-data-service stopped")


app = FastAPI(
    title="market-data-service",
    version="0.1.0",
    description="Real-time Forex market data collection via OANDA API",
    lifespan=lifespan,
)

market_data_app.include_router(router)
