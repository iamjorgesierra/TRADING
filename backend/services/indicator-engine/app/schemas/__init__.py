"""
Schemas Pydantic del indicator-engine.

Define los contratos de datos publicados en Redis Streams y expuestos
por los endpoints REST. Separados de shared/schemas para mantener
la autonomía del servicio (cada servicio puede evolucionar sus schemas).
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class IndicatorSnapshot(BaseModel):
    """
    Snapshot completo de todos los indicadores calculados para
    un símbolo + timeframe en un momento dado.

    Publicado en el stream `indicators:values` tras cada nueva vela.
    """

    symbol: str
    timeframe: str
    timestamp: datetime

    # EMA
    ema_9: float | None = None
    ema_21: float | None = None
    ema_50: float | None = None
    ema_200: float | None = None

    # RSI
    rsi_14: float | None = None
    rsi_signal: Literal["overbought", "oversold", "neutral"] = "neutral"

    # MACD
    macd_line: float | None = None
    macd_signal: float | None = None
    macd_histogram: float | None = None
    macd_cross: Literal["bullish", "bearish", "none"] = "none"

    # ATR
    atr_14: float | None = None

    # Bollinger Bands
    bb_upper: float | None = None
    bb_middle: float | None = None
    bb_lower: float | None = None
    bb_position: Literal["above_upper", "near_upper", "middle", "near_lower", "below_lower"] = "middle"

    # Precio de referencia
    close_price: float | None = None

    def to_stream_dict(self) -> dict:
        """Serializa para publicación en Redis Streams (todo str)."""
        return {
            k: ("" if v is None else str(v))
            for k, v in self.model_dump().items()
        }


class IndicatorRequest(BaseModel):
    """Parámetros de petición para calcular indicadores bajo demanda."""

    symbol: str = Field(..., examples=["EUR_USD"])
    timeframe: str = Field(..., examples=["M5"])
    include_indicators: list[str] = Field(
        default=["ema", "rsi", "macd", "atr", "bb"],
        description="Lista de indicadores a incluir en la respuesta",
    )


class EngineStatus(BaseModel):
    """Estado interno del indicator-engine para el endpoint /status."""

    service: str = "indicator-engine"
    status: str
    redis_connected: bool
    total_buffers: int
    ready_buffers: int
    stream_consumer_running: bool
