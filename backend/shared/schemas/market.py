"""
Schemas canónicos de datos de mercado.

Decisión técnica:
- Modelos Pydantic v2 con validación estricta.
- Representan el contrato de datos entre todos los microservicios.
- TickData y CandleData son los tipos centrales del pipeline de datos.
- Métodos de fábrica (from_oanda) encapsulan la transformación del
  formato de proveedor al formato interno normalizado.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class TickData(BaseModel):
    """
    Precio bid/ask en tiempo real para un instrumento Forex.

    El spread se calcula y normaliza en pips:
    - Para pares no-JPY: (ask - bid) * 10_000
    - Para pares JPY   : (ask - bid) * 100
    """

    symbol: str = Field(description="Instrumento OANDA (e.g., EUR_USD)")
    timestamp: datetime = Field(description="Timestamp UTC del tick")
    bid: float = Field(gt=0, description="Precio de venta (bid)")
    ask: float = Field(gt=0, description="Precio de compra (ask)")
    spread: float = Field(ge=0, description="Spread en pips")
    source: str = Field(default="oanda", description="Proveedor del dato")

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: object) -> datetime:
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        raise ValueError(f"Cannot parse timestamp: {v!r}")

    @classmethod
    def from_oanda(
        cls,
        symbol: str,
        timestamp: str,
        bid: str | float,
        ask: str | float,
        source: str = "oanda",
    ) -> "TickData":
        """
        Fábrica que transforma el payload de OANDA al modelo canónico.

        OANDA streaming entrega:
        {
          "type": "PRICE",
          "instrument": "EUR_USD",
          "time": "2026-05-07T10:00:00.123456Z",
          "bids": [{"price": "1.08234", ...}],
          "asks": [{"price": "1.08240", ...}]
        }
        """
        bid_f = float(bid)
        ask_f = float(ask)
        pip_factor = 100.0 if "JPY" in symbol else 10_000.0
        spread = round((ask_f - bid_f) * pip_factor, 2)

        return cls(
            symbol=symbol,
            timestamp=timestamp,  # type: ignore[arg-type]
            bid=bid_f,
            ask=ask_f,
            spread=spread,
            source=source,
        )


class CandleData(BaseModel):
    """Vela OHLCV para un instrumento y timeframe específicos."""

    symbol: str
    timeframe: str = Field(description="Granularidad OANDA (M1, M5, H1, ...)")
    timestamp: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: int = Field(ge=0, default=0)
    complete: bool = Field(default=True, description="False si la vela aún está formándose")
    source: str = "oanda"

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: object) -> datetime:
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        raise ValueError(f"Cannot parse timestamp: {v!r}")

    @model_validator(mode="after")
    def validate_ohlc(self) -> "CandleData":
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if self.high < self.open or self.high < self.close:
            raise ValueError("high must be >= open and close")
        if self.low > self.open or self.low > self.close:
            raise ValueError("low must be <= open and close")
        return self

    @classmethod
    def from_oanda(
        cls,
        symbol: str,
        timeframe: str,
        raw: dict,
    ) -> "CandleData":
        """Transforma candle de OANDA al modelo canónico."""
        mid = raw["mid"]
        return cls(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=raw["time"],  # type: ignore[arg-type]
            open=float(mid["o"]),
            high=float(mid["h"]),
            low=float(mid["l"]),
            close=float(mid["c"]),
            volume=raw.get("volume", 0),
            complete=raw.get("complete", True),
        )


class OrderBookLevel(BaseModel):
    """Nivel del order book de OANDA (precio + % de órdenes)."""

    price: float = Field(gt=0)
    count: int = Field(ge=0)
    percent: float = Field(ge=0, le=100)


class OrderBook(BaseModel):
    """Snapshot del order book de OANDA para un instrumento."""

    symbol: str
    timestamp: datetime
    buckets: list[OrderBookLevel]
