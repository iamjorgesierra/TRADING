"""
historical-data-service — punto de entrada FastAPI.

Responsabilidades:
1. Consumir ticks desde Redis Streams (market:ticks).
2. Persistir datos en TimescaleDB (PostgreSQL + extensión TimescaleDB).
3. Exponer endpoints de consulta de datos históricos.

Decisión técnica — consumer group:
- Usar consumer groups garantiza que en producción varios workers del
  servicio puedan escalar horizontalmente sin duplicar inserciones.
- El group se llama "historical-data-service".
- consumer_name puede ser dinámico (hostname/pod) en producción.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from loguru import logger

from shared.config.settings import get_settings
from shared.database.connection import close_engine, create_db_engine, create_session_factory
from shared.logging.logger import setup_logging
from shared.redis.client import close_redis_client, get_redis_client

from app.api.routes import router
from app.consumers.market_consumer import MarketDataConsumer

settings = get_settings()
setup_logging("historical-data-service", settings.log_level)

_consumer_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global _consumer_task

    # ── Inicializar base de datos ──────────────────────────────
    engine = create_db_engine(settings.database_url)
    session_factory = create_session_factory(engine)

    # ── Inicializar Redis ──────────────────────────────────────
    redis = await get_redis_client(settings.redis_url)

    # ── Lanzar consumer en background ─────────────────────────
    consumer = MarketDataConsumer(redis=redis, session_factory=session_factory)
    _consumer_task = asyncio.create_task(consumer.start(), name="market-data-consumer")

    app.state.redis = redis
    app.state.session_factory = session_factory

    logger.info("historical-data-service ready")
    yield

    # ── Shutdown ───────────────────────────────────────────────
    if _consumer_task and not _consumer_task.done():
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass

    await close_redis_client()
    await close_engine()
    logger.info("historical-data-service stopped")


app = FastAPI(
    title="historical-data-service",
    version="0.1.0",
    description="Historical Forex data storage via TimescaleDB",
    lifespan=lifespan,
)

app.include_router(router)
