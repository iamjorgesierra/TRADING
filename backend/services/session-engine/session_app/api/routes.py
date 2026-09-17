"""
Endpoints HTTP del session-engine.

/health              — health-check.
/session/current     — sesión Forex activa ahora mismo.
/session/all         — lista completa de todas las sesiones.
/session/pair/{pair} — si un par está activo en la sesión actual.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class SessionResponse(BaseModel):
    session: str
    start_utc_hour: int
    end_utc_hour: int
    volatility: str
    active_pairs: list[str]
    description: str
    utc_hour: int
    timestamp: str


@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="session-engine",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/session/current", response_model=SessionResponse, tags=["Sessions"])
async def current_session(request: Request) -> SessionResponse:
    """
    Devuelve la sesión Forex activa en este momento.

    Respuesta incluye volatilidad esperada y pares más activos.
    Consumida por strategy-engine para filtrar señales.
    """
    detector = request.app.state.detector
    now = datetime.now(timezone.utc)
    info = detector.get_current_session(now)

    return SessionResponse(
        session=info.session.value,
        start_utc_hour=info.start_utc_hour,
        end_utc_hour=info.end_utc_hour,
        volatility=info.volatility,
        active_pairs=info.active_pairs,
        description=info.description,
        utc_hour=now.hour,
        timestamp=now.isoformat(),
    )


@router.get("/session/all", tags=["Sessions"])
async def all_sessions(request: Request) -> list[SessionResponse]:
    """Lista completa de todas las sesiones Forex con sus características."""
    detector = request.app.state.detector
    now = datetime.now(timezone.utc)

    return [
        SessionResponse(
            session=s.session.value,
            start_utc_hour=s.start_utc_hour,
            end_utc_hour=s.end_utc_hour,
            volatility=s.volatility,
            active_pairs=s.active_pairs,
            description=s.description,
            utc_hour=now.hour,
            timestamp=now.isoformat(),
        )
        for s in detector.get_all_sessions()
    ]


@router.get("/session/pair/{symbol}", tags=["Sessions"])
async def is_pair_active(symbol: str, request: Request) -> dict:
    """Comprueba si un par Forex está activo en la sesión actual."""
    detector = request.app.state.detector
    active = detector.is_pair_active_now(symbol.upper())
    session = detector.get_current_session()

    return {
        "symbol": symbol.upper(),
        "active_in_current_session": active,
        "current_session": session.session.value,
        "session_volatility": session.volatility,
    }
