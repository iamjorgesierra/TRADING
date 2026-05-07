"""
Endpoints HTTP del market-data-service.

/health  — health-check para Docker y load balancers.
/status  — estado detallado del servicio (streaming, Redis, símbolos).
/candles — fetch de velas históricas vía OANDA REST API.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

router = APIRouter()


# ── Modelos de respuesta ──────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


class StatusResponse(BaseModel):
    status: str
    redis_connected: bool
    oanda_streaming: bool
    oanda_env: str
    symbols: list[str]
    timestamp: str


class CandlesResponse(BaseModel):
    symbol: str
    timeframe: str
    count: int
    candles: list[dict]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health() -> HealthResponse:
    """Health-check básico. Siempre devuelve 200 si el proceso está vivo."""
    return HealthResponse(
        status="ok",
        service="market-data-service",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/status", response_model=StatusResponse, tags=["System"])
async def status(request: Request) -> StatusResponse:
    """Estado detallado del servicio: conexión Redis y streaming OANDA."""
    oanda = request.app.state.oanda_client
    settings = request.app.state.publisher._settings

    redis_ok = False
    try:
        await request.app.state.redis.ping()
        redis_ok = True
    except Exception:
        pass

    return StatusResponse(
        status="running",
        redis_connected=redis_ok,
        oanda_streaming=oanda.is_running,
        oanda_env=settings.oanda_env,
        symbols=settings.default_symbols,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/candles/{symbol}", response_model=CandlesResponse, tags=["Market Data"])
async def get_candles(
    symbol: str,
    request: Request,
    timeframe: str = Query(default="M15", description="Granularidad OANDA (M1, M5, H1...)"),
    count: int = Query(default=100, ge=1, le=500),
) -> CandlesResponse:
    """
    Obtiene velas históricas desde OANDA REST API.

    Útil para inicializar indicadores técnicos con datos históricos.
    """
    oanda = request.app.state.oanda_client

    try:
        candles = await oanda.fetch_candles(symbol=symbol, timeframe=timeframe, count=count)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"OANDA API error: {exc}") from exc

    return CandlesResponse(
        symbol=symbol,
        timeframe=timeframe,
        count=len(candles),
        candles=[c.model_dump(mode="json") for c in candles],
    )
