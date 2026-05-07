"""
Endpoints HTTP del indicator-engine.

/health                        — health-check para Docker.
/status                        — estado interno: buffers, consumer, Redis.
/indicators/{symbol}/{tf}      — indicadores actuales de un símbolo+timeframe.
/buffers                       — lista todos los buffers activos.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..schemas import EngineStatus, IndicatorSnapshot

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


# ── Endpoints ─────────────────────────────────────────────────────────────────────────────────

@router.get("/health", response_model=HealthResponse, tags=["System"])
async def health() -> HealthResponse:
    """Health-check básico. Siempre devuelve 200 si el proceso está vivo."""
    return HealthResponse(
        status="ok",
        service="indicator-engine",
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/status", response_model=EngineStatus, tags=["System"])
async def status(request: Request) -> EngineStatus:
    """Estado detallado: buffers cargados, consumer running, Redis conectado."""
    registry = request.app.state.buffer_registry

    redis_ok = False
    try:
        await request.app.state.redis.ping()
        redis_ok = True
    except Exception:
        pass

    ready_count = sum(
        1
        for (sym, tf) in registry.all_keys
        if registry.get(sym, tf) and registry.get(sym, tf).is_ready  # type: ignore[union-attr]
    )

    return EngineStatus(
        status="running",
        redis_connected=redis_ok,
        total_buffers=registry.total_buffers,
        ready_buffers=ready_count,
        stream_consumer_running=True,
    )


@router.get(
    "/indicators/{symbol}/{timeframe}",
    response_model=IndicatorSnapshot,
    tags=["Indicators"],
)
async def get_indicators(symbol: str, timeframe: str, request: Request) -> IndicatorSnapshot:
    """
    Devuelve el snapshot de indicadores más reciente para un símbolo+timeframe.

    Si el buffer no tiene suficientes datos, los campos de indicadores serán None.
    """
    symbol_upper = symbol.upper().replace("-", "_")
    tf_upper = timeframe.upper()
    registry = request.app.state.buffer_registry
    calculator = request.app.state.calculator

    buffer = registry.get(symbol_upper, tf_upper)
    if buffer is None or buffer.size == 0:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No hay datos para {symbol_upper}/{tf_upper}. "
                "Espera a que el consumer reciba velas de market:candles."
            ),
        )

    snapshot = calculator.calculate(buffer)
    return snapshot


@router.get("/buffers", tags=["System"])
async def list_buffers(request: Request) -> dict:
    """Lista todos los buffers activos con su estado actual."""
    registry = request.app.state.buffer_registry
    result = []
    for sym, tf in registry.all_keys:
        buf = registry.get(sym, tf)
        if buf:
            result.append({
                "symbol": sym,
                "timeframe": tf,
                "size": buf.size,
                "ready": buf.is_ready,
                "last_close": buf.last_close(),
            })
    return {"total": len(result), "buffers": result}
