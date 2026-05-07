"""
Modelos SQLAlchemy para series temporales en TimescaleDB.

Decisiones técnicas:
- Ticks y Candles son hypertables de TimescaleDB (creadas por init.sql).
  SQLAlchemy no necesita saber que son hypertables; el DDL lo gestiona
  el script de inicialización de PostgreSQL.
- La columna `time` es TIMESTAMPTZ (con timezone) — requisito de TimescaleDB.
- Para las velas, la clave primaria compuesta (time, symbol, timeframe) evita
  duplicados si el consumer reintenta un mensaje.
- expire_on_commit=False en la sesión async evita accesos lazy post-commit.
"""

from datetime import datetime

from sqlalchemy import Boolean, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from shared.database.connection import Base


class Tick(Base):
    """
    Tick data (bid/ask) en tiempo real.

    TimescaleDB hypertable particionada por `time` con chunks de 1 día.
    """

    __tablename__ = "ticks"

    # TimescaleDB hypertable requiere que la columna tiempo sea parte de la PK
    time: Mapped[datetime] = mapped_column(primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(20), primary_key=True, index=True)
    bid: Mapped[float] = mapped_column(Float, nullable=False)
    ask: Mapped[float] = mapped_column(Float, nullable=False)
    spread: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(50), default="oanda")

    __table_args__ = (
        Index("idx_ticks_symbol_time_desc", "symbol", time.desc()),
    )


class Candle(Base):
    """
    Vela OHLCV para instrumento + timeframe.

    TimescaleDB hypertable particionada por `time` con chunks de 7 días.
    """

    __tablename__ = "candles"

    time: Mapped[datetime] = mapped_column(primary_key=True, index=True)
    symbol: Mapped[str] = mapped_column(String(20), primary_key=True, index=True)
    timeframe: Mapped[str] = mapped_column(String(10), primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[int] = mapped_column(Integer, default=0)
    complete: Mapped[bool] = mapped_column(Boolean, default=True)
    source: Mapped[str] = mapped_column(String(50), default="oanda")

    __table_args__ = (
        Index("idx_candles_symbol_tf_time", "symbol", "timeframe", time.desc()),
    )


class SessionEvent(Base):
    """
    Eventos de sesión Forex (London open, NY open, overlap, etc.).

    Referencia histórica del contexto de sesión para backtesting.
    """

    __tablename__ = "session_events"

    time: Mapped[datetime] = mapped_column(primary_key=True, index=True)
    session_type: Mapped[str] = mapped_column(String(50), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    expected_volatility: Mapped[str] = mapped_column(String(20), nullable=True)
    description: Mapped[str] = mapped_column(String(200), nullable=True)
