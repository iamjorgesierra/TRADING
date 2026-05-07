"""
Schemas de eventos del sistema.

Los eventos son el lenguaje entre microservicios.
Cada tipo de evento tiene un schema estricto que garantiza
compatibilidad entre productores y consumidores.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EventType(str, Enum):
    TICK = "tick"
    CANDLE = "candle"
    SESSION_START = "session_start"
    SESSION_CHANGE = "session_change"
    SESSION_END = "session_end"
    SYSTEM_HEALTH = "system_health"
    SYSTEM_ERROR = "system_error"


class SessionType(str, Enum):
    ASIAN = "asian"
    LONDON = "london"
    NEW_YORK = "new_york"
    LONDON_NEW_YORK = "london_new_york"  # overlap máxima volatilidad
    OFF_HOURS = "off_hours"


class SessionEvent(BaseModel):
    """
    Evento de cambio de sesión Forex publicado por session-engine.
    Consumido por strategy-engine, signal-engine y risk-engine.
    """

    event_type: EventType
    session: SessionType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    utc_hour: int = Field(ge=0, le=23)
    expected_volatility: str = Field(
        description="Volatilidad esperada: 'low', 'medium', 'high'"
    )
    active_pairs: list[str] = Field(default_factory=list)
    description: str = ""


class SystemEvent(BaseModel):
    """Evento de salud y estado del sistema."""

    event_type: EventType
    service: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message: str = ""
    details: Optional[dict] = None
