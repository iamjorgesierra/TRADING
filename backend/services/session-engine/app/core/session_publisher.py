"""
Publisher de eventos de sesión hacia Redis Streams.

Corre como background task y publica un evento cada vez que la sesión cambia.
También publica el estado actual al arrancar (para consumidores que se conecten tarde).
"""

import asyncio
from datetime import datetime, timezone

from loguru import logger

from shared.redis.streams import StreamProducer

from app.core.session_detector import ForexSession, SessionDetector, SessionInfo


class SessionPublisher:
    """
    Monitoriza la sesión Forex y publica eventos de cambio en Redis Streams.

    Frecuencia de check: cada 60 segundos (la sesión cambia a hora exacta).
    En producción se puede afinar a 30s para mayor precisión.
    """

    _CHECK_INTERVAL_SECONDS: int = 60

    def __init__(self, detector: SessionDetector, producer: StreamProducer) -> None:
        self._detector = detector
        self._producer = producer
        self._last_session: ForexSession | None = None

    async def run_loop(self) -> None:
        """
        Bucle principal de publicación. Diseñado para asyncio.Task.

        Al arrancar publica el estado actual (evento "session_start").
        Luego entra en bucle y publica "session_change" cuando cambia.
        """
        logger.info("SessionPublisher loop started")

        # Publicar estado inicial al arrancar
        await self._publish_current(event_type="session_start")

        while True:
            try:
                await asyncio.sleep(self._CHECK_INTERVAL_SECONDS)
                await self._check_and_publish()
            except asyncio.CancelledError:
                logger.info("SessionPublisher loop cancelled")
                raise
            except Exception as exc:
                logger.error(f"SessionPublisher error  error={exc}")

    async def _check_and_publish(self) -> None:
        """Comprueba si la sesión cambió y publica evento si es así."""
        now = datetime.now(timezone.utc)
        current_info = self._detector.get_current_session(now)

        if current_info.session != self._last_session:
            await self._publish_event(
                event_type="session_change",
                session_info=current_info,
                now=now,
            )
            logger.info(
                f"Session changed  "
                f"prev={self._last_session}  "
                f"new={current_info.session.value}  "
                f"volatility={current_info.volatility}"
            )
            self._last_session = current_info.session

    async def _publish_current(self, event_type: str = "session_start") -> None:
        """Publica el estado de sesión actual."""
        now = datetime.now(timezone.utc)
        current_info = self._detector.get_current_session(now)
        await self._publish_event(event_type=event_type, session_info=current_info, now=now)
        self._last_session = current_info.session

    async def _publish_event(
        self,
        event_type: str,
        session_info: SessionInfo,
        now: datetime,
    ) -> None:
        """Serializa y publica un SessionEvent en Redis Streams."""
        await self._producer.publish(
            {
                "event_type": event_type,
                "session": session_info.session.value,
                "timestamp": now.isoformat(),
                "utc_hour": now.hour,
                "volatility": session_info.volatility,
                "active_pairs": ",".join(session_info.active_pairs),
                "description": session_info.description,
            }
        )
