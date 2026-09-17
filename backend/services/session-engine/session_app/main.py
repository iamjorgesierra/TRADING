"""
session-engine — punto de entrada FastAPI.

Responsabilidades:
1. Detectar la sesión Forex activa en base a hora UTC.
2. Publicar eventos de cambio de sesión en Redis Streams (sessions:events).
3. Exponer endpoints HTTP para consultar sesión actual y características.

Sesiones Forex (UTC):
  Asian              00:00 – 08:00  (Tokyo + Sydney)
  London             08:00 – 13:00  (alta volatilidad)
  London/NY Overlap  13:00 – 17:00  (máxima volatilidad y volumen)
  New York           17:00 – 22:00  (media volatilidad)
  Off-hours          22:00 – 00:00  (mínima liquidez)
"""

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from loguru import logger

from shared.config.settings import get_settings
from shared.logging.logger import setup_logging
from shared.redis.client import close_redis_client, get_redis_client
from shared.redis.streams import StreamProducer

from session_app.api.routes import router
from session_app.core.session_detector import SessionDetector
from session_app.core.session_publisher import SessionPublisher

settings = get_settings()
setup_logging("session-engine", settings.log_level)

_session_task: asyncio.Task | None = None
_detector = SessionDetector()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global _session_task

    redis = await get_redis_client(settings.redis_url)
    producer = StreamProducer(redis, settings.stream_session_events)
    publisher = SessionPublisher(detector=_detector, producer=producer)

    # Background task: publica eventos de sesión cada minuto
    _session_task = asyncio.create_task(
        publisher.run_loop(), name="session-publisher"
    )

    app.state.redis = redis
    app.state.detector = _detector
    app.state.publisher = publisher

    logger.info("session-engine ready")
    yield

    if _session_task and not _session_task.done():
        _session_task.cancel()
        try:
            await _session_task
        except asyncio.CancelledError:
            pass

    await close_redis_client()
    logger.info("session-engine stopped")


app = FastAPI(
    title="session-engine",
    version="0.1.0",
    description="Forex trading session detection and event publishing",
    lifespan=lifespan,
)

session_app.include_router(router)
