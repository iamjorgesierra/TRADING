"""
Buffer de velas en memoria por símbolo y timeframe.

Propósito: el indicator-engine necesita una ventana deslizante de velas
para calcular indicadores técnicos en tiempo real. La ventana tiene
un tamaño máximo configurable (por defecto 500 velas).

Decisión técnica:
- Usar collections.deque con maxlen para O(1) append y pop automático.
- Almacenamiento en RAM (no Redis) porque los indicadores se calculan
  en el mismo proceso. Redis se usa solo para publicar los resultados.
- Thread-safety: no necesaria porque todo corre en el mismo event loop asyncio.
- Persistencia: en caso de reinicio, el buffer se repopula desde historical-data-service.
"""

from collections import deque
from dataclasses import dataclass, field

import numpy as np


@dataclass
class CandleBuffer:
    """
    Buffer circular de OHLCV para un símbolo + timeframe específicos.

    Mantiene las últimas `maxlen` velas en memoria. Las más antiguas
    se descartan automáticamente cuando se añade una nueva.
    """

    symbol: str
    timeframe: str
    maxlen: int = 500

    # Buffers internos como deque para O(1) append/pop
    _open: deque = field(default_factory=deque, init=False, repr=False)
    _high: deque = field(default_factory=deque, init=False, repr=False)
    _low: deque = field(default_factory=deque, init=False, repr=False)
    _close: deque = field(default_factory=deque, init=False, repr=False)
    _volume: deque = field(default_factory=deque, init=False, repr=False)

    def __post_init__(self) -> None:
        self._open = deque(maxlen=self.maxlen)
        self._high = deque(maxlen=self.maxlen)
        self._low = deque(maxlen=self.maxlen)
        self._close = deque(maxlen=self.maxlen)
        self._volume = deque(maxlen=self.maxlen)

    def add(
        self,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float = 0.0,
    ) -> None:
        """Añade una nueva vela al buffer. Descarta la más antigua si el buffer está lleno."""
        self._open.append(open_)
        self._high.append(high)
        self._low.append(low)
        self._close.append(close)
        self._volume.append(volume)

    def bulk_load(self, candles: list[dict]) -> None:
        """
        Carga masiva de velas históricas (para repopular tras reinicio).

        Args:
            candles: Lista de dicts con keys: open, high, low, close, volume (opcional).
                     Ordenada de más antigua a más reciente.
        """
        for c in candles[-self.maxlen :]:  # solo las últimas maxlen
            self.add(
                open_=float(c["open"]),
                high=float(c["high"]),
                low=float(c["low"]),
                close=float(c["close"]),
                volume=float(c.get("volume", 0.0)),
            )

    @property
    def size(self) -> int:
        """Número de velas actuales en el buffer."""
        return len(self._close)

    @property
    def is_ready(self) -> bool:
        """True si el buffer tiene suficiente historial para calcular todos los indicadores."""
        # Necesitamos al menos 35 barras para MACD(12,26,9): slow=26 + signal=9
        return self.size >= 35

    def get_closes(self) -> np.ndarray:
        return np.array(self._close, dtype=np.float64)

    def get_highs(self) -> np.ndarray:
        return np.array(self._high, dtype=np.float64)

    def get_lows(self) -> np.ndarray:
        return np.array(self._low, dtype=np.float64)

    def get_volumes(self) -> np.ndarray:
        return np.array(self._volume, dtype=np.float64)

    def last_close(self) -> float | None:
        """Precio de cierre de la última vela, o None si el buffer está vacío."""
        return self._close[-1] if self._close else None


class BufferRegistry:
    """
    Registro global de CandleBuffer indexado por (symbol, timeframe).

    Un solo BufferRegistry existe por proceso del indicator-engine.
    Se inicializa en el lifespan de FastAPI y se accede vía app.state.
    """

    def __init__(self, maxlen: int = 500) -> None:
        self._buffers: dict[tuple[str, str], CandleBuffer] = {}
        self._maxlen = maxlen

    def get_or_create(self, symbol: str, timeframe: str) -> CandleBuffer:
        """Obtiene un buffer existente o crea uno nuevo si no existe."""
        key = (symbol, timeframe)
        if key not in self._buffers:
            self._buffers[key] = CandleBuffer(
                symbol=symbol,
                timeframe=timeframe,
                maxlen=self._maxlen,
            )
        return self._buffers[key]

    def get(self, symbol: str, timeframe: str) -> CandleBuffer | None:
        """Obtiene un buffer existente. None si no existe."""
        return self._buffers.get((symbol, timeframe))

    @property
    def all_keys(self) -> list[tuple[str, str]]:
        """Lista de todos los (symbol, timeframe) en el registro."""
        return list(self._buffers.keys())

    @property
    def total_buffers(self) -> int:
        return len(self._buffers)
