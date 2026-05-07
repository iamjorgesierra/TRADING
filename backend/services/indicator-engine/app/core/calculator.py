"""
Motor de cálculo de indicadores.

Orquesta el cálculo de todos los indicadores técnicos
a partir de un CandleBuffer y produce un IndicatorSnapshot.

Diseño:
- Un solo IndicatorCalculator por proceso.
- Stateless: recibe un buffer, devuelve un snapshot.
- No tiene acceso a Redis ni a DB. Solo matemáticas.
- Los NaN de numpy se convierten en None para los schemas.
"""

from datetime import datetime, timezone

import numpy as np

from .buffer import CandleBuffer
from .indicators import atr, bollinger_bands, ema, macd, rsi
from ..schemas import IndicatorSnapshot


def _nan_to_none(value: float | np.floating) -> float | None:
    """Convierte NaN/inf de numpy en None para serialización Pydantic."""
    if value is None:
        return None
    if isinstance(value, (float, np.floating)) and (np.isnan(value) or np.isinf(value)):
        return None
    return float(value)


def _last_valid(arr: np.ndarray) -> float | None:
    """Devuelve el último valor no-NaN del array, o None si todos son NaN."""
    valid = arr[~np.isnan(arr)]
    return float(valid[-1]) if len(valid) > 0 else None


def _rsi_signal(value: float | None) -> str:
    if value is None:
        return "neutral"
    if value >= 70:
        return "overbought"
    if value <= 30:
        return "oversold"
    return "neutral"


def _macd_cross(histogram: np.ndarray) -> str:
    """
    Detecta cruce MACD en las últimas 2 barras.
    Si el histograma pasó de negativo a positivo → bullish cross.
    Si pasó de positivo a negativo → bearish cross.
    """
    valid = histogram[~np.isnan(histogram)]
    if len(valid) < 2:
        return "none"
    prev, curr = valid[-2], valid[-1]
    if prev < 0 and curr >= 0:
        return "bullish"
    if prev > 0 and curr <= 0:
        return "bearish"
    return "none"


def _bb_position(close: float | None, upper: float | None, lower: float | None, middle: float | None) -> str:
    if close is None or upper is None or lower is None or middle is None:
        return "middle"
    range_ = upper - lower
    if range_ == 0:
        return "middle"
    if close > upper:
        return "above_upper"
    if close < lower:
        return "below_lower"
    # Zona: upper 20% = near_upper, lower 20% = near_lower
    pos = (close - lower) / range_
    if pos >= 0.8:
        return "near_upper"
    if pos <= 0.2:
        return "near_lower"
    return "middle"


class IndicatorCalculator:
    """
    Calcula todos los indicadores técnicos a partir de un CandleBuffer.

    Uso:
        calculator = IndicatorCalculator()
        snapshot = calculator.calculate(buffer, timeframe="M5")
    """

    def calculate(
        self,
        buffer: CandleBuffer,
        timestamp: datetime | None = None,
    ) -> IndicatorSnapshot:
        """
        Calcula todos los indicadores y devuelve un IndicatorSnapshot.

        Args:
            buffer:    CandleBuffer con al menos 35 velas para resultados completos.
            timestamp: Timestamp del snapshot. Si None, usa datetime.now(UTC).

        Returns:
            IndicatorSnapshot con todos los campos calculados (None si no hay datos suficientes).
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        closes = buffer.get_closes()
        highs = buffer.get_highs()
        lows = buffer.get_lows()
        volumes = buffer.get_volumes()
        n = len(closes)

        # ── EMA ──────────────────────────────────────────────────────────
        ema9_val = _last_valid(ema(closes, 9)) if n >= 9 else None
        ema21_val = _last_valid(ema(closes, 21)) if n >= 21 else None
        ema50_val = _last_valid(ema(closes, 50)) if n >= 50 else None
        ema200_val = _last_valid(ema(closes, 200)) if n >= 200 else None

        # ── RSI ──────────────────────────────────────────────────────────
        rsi_val = _last_valid(rsi(closes, 14)) if n >= 15 else None

        # ── MACD ─────────────────────────────────────────────────────────
        macd_line_arr, signal_arr, hist_arr = macd(closes)
        macd_line_val = _last_valid(macd_line_arr)
        macd_signal_val = _last_valid(signal_arr)
        macd_hist_val = _last_valid(hist_arr)
        cross = _macd_cross(hist_arr) if not np.all(np.isnan(hist_arr)) else "none"

        # ── ATR ──────────────────────────────────────────────────────────
        atr_val = _last_valid(atr(highs, lows, closes, 14)) if n >= 15 else None

        # ── Bollinger Bands ───────────────────────────────────────────────
        bb_u, bb_m, bb_l = bollinger_bands(closes, 20) if n >= 20 else (
            np.full(n, np.nan),
            np.full(n, np.nan),
            np.full(n, np.nan),
        )
        bb_upper_val = _last_valid(bb_u)
        bb_middle_val = _last_valid(bb_m)
        bb_lower_val = _last_valid(bb_l)

        close_price = float(closes[-1]) if n > 0 else None

        return IndicatorSnapshot(
            symbol=buffer.symbol,
            timeframe=buffer.timeframe,
            timestamp=timestamp,
            # EMA
            ema_9=ema9_val,
            ema_21=ema21_val,
            ema_50=ema50_val,
            ema_200=ema200_val,
            # RSI
            rsi_14=rsi_val,
            rsi_signal=_rsi_signal(rsi_val),
            # MACD
            macd_line=macd_line_val,
            macd_signal=macd_signal_val,
            macd_histogram=macd_hist_val,
            macd_cross=cross,
            # ATR
            atr_14=atr_val,
            # Bollinger Bands
            bb_upper=bb_upper_val,
            bb_middle=bb_middle_val,
            bb_lower=bb_lower_val,
            bb_position=_bb_position(close_price, bb_upper_val, bb_lower_val, bb_middle_val),
            # Precio
            close_price=close_price,
        )
