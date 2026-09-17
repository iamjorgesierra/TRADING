"""api package."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ..schemas import EngineStatus, IndicatorSnapshot

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    service: str
    timestamp: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

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
    consumer_task = getattr(request.app.state, "_consumer_task", None)

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
        stream_consumer_running=(consumer_task is not None and not consumer_task.done())
        if consumer_task
        else True,
    )


@router.get(
    "/indicators/{symbol}/{timeframe}",
    response_model=IndicatorSnapshot,
    tags=["Indicators"],
)
async def get_indicators(symbol: str, timeframe: str, request: Request) -> IndicatorSnapshot:
    """
    Devuelve el snapshot de indicadores más reciente para un símbolo+timeframe.

    El snapshot se calcula en el momento de la petición a partir del buffer en memoria.
    Si el buffer no tiene suficientes datos, los campos de indicadores serán None.

    Args:
        symbol:    Instrumento (e.g., EUR_USD, GBP_USD).
        timeframe: Granularidad (e.g., M5, H1, D).
    """
    symbol = symbol.upper().replace("-", "_")
    registry = request.app.state.buffer_registry
    calculator = request.app.state.calculator

    buffer = registry.get(symbol, timeframe.upper())
    if buffer is None or buffer.size == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No hay datos para {symbol}/{timeframe}. "
                   f"Espera a que el consumer reciba velas de market:candles.",
        )

    snapshot = calculator.calculate(buffer)
    return snapshot


@router.get("/buffers", tags=["System"])
async def list_buffers(request: Request) -> dict:
    """Lista todos los buffers activos con su estado."""
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
