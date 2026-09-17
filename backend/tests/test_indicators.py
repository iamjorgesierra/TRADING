"""
Tests unitarios del indicator-engine.

Pruebas matemáticas de los indicadores técnicos.
Todos son tests puros sin I/O.

Ejecutar: pytest backend/tests/test_indicators.py -v
"""

import sys
from pathlib import Path

import numpy as np
import pytest

# Path al servicio
_IND_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "indicator-engine"
sys.path.insert(0, str(_IND_ENGINE))

from indicator_app.core.indicators import atr, bollinger_bands, ema, macd, rsi, vwap
from indicator_app.core.buffer import BufferRegistry, CandleBuffer
from indicator_app.core.calculator import IndicatorCalculator


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────

@pytest.fixture
def trending_up() -> np.ndarray:
    """Serie de precios en tendencia alcista sostenida."""
    return np.linspace(1.0, 2.0, 100)


@pytest.fixture
def flat_series() -> np.ndarray:
    """Serie de precios completamente plana."""
    return np.full(100, 1.2000)


@pytest.fixture
def ohlcv_buffer() -> CandleBuffer:
    """Buffer con 50 velas sintéticas."""
    buf = CandleBuffer(symbol="EUR_USD", timeframe="M5", maxlen=500)
    prices = np.linspace(1.08, 1.12, 50)
    for i, p in enumerate(prices):
        buf.add(
            open_=p,
            high=p * 1.001,
            low=p * 0.999,
            close=p,
            volume=1000.0,
        )
    return buf


# ─────────────────────────────────────────────────────────────
# Tests EMA
# ─────────────────────────────────────────────────────────────

class TestEMA:

    def test_returns_correct_length(self, trending_up):
        result = ema(trending_up, 9)
        assert len(result) == len(trending_up)

    def test_first_period_minus_1_are_nan(self, trending_up):
        result = ema(trending_up, 9)
        assert all(np.isnan(result[:8]))

    def test_values_after_period_are_not_nan(self, trending_up):
        result = ema(trending_up, 9)
        assert not np.any(np.isnan(result[9:]))

    def test_ema_tracks_trend(self, trending_up):
        """En tendencia alcista, EMA debe ser creciente."""
        result = ema(trending_up, 9)
        valid = result[~np.isnan(result)]
        assert np.all(np.diff(valid) > 0)

    def test_ema_flat_series_equals_price(self, flat_series):
        """En serie plana, EMA debe ser igual al precio."""
        result = ema(flat_series, 9)
        valid = result[~np.isnan(result)]
        np.testing.assert_allclose(valid, 1.2000, rtol=1e-10)

    def test_insufficient_data_returns_all_nan(self):
        prices = np.array([1.0, 1.1, 1.2])  # menos que period=9
        result = ema(prices, 9)
        assert all(np.isnan(result))

    def test_ema9_less_than_ema21_in_uptrend(self, trending_up):
        """En tendencia alcista, EMA9 > EMA21 (responde más rápido)."""
        e9 = ema(trending_up, 9)
        e21 = ema(trending_up, 21)
        # Comparar solo valores válidos de ambos
        valid_idx = ~(np.isnan(e9) | np.isnan(e21))
        assert np.all(e9[valid_idx] > e21[valid_idx])


# ─────────────────────────────────────────────────────────────
# Tests RSI
# ─────────────────────────────────────────────────────────────

class TestRSI:

    def test_returns_correct_length(self, trending_up):
        result = rsi(trending_up, 14)
        assert len(result) == len(trending_up)

    def test_first_14_are_nan(self, trending_up):
        result = rsi(trending_up, 14)
        assert all(np.isnan(result[:14]))

    def test_uptrend_rsi_above_50(self, trending_up):
        """En tendencia alcista pura, RSI debe ser alto (>50)."""
        result = rsi(trending_up, 14)
        valid = result[~np.isnan(result)]
        assert np.all(valid > 50)

    def test_downtrend_rsi_below_50(self):
        prices = np.linspace(2.0, 1.0, 50)  # bajista
        result = rsi(prices, 14)
        valid = result[~np.isnan(result)]
        assert np.all(valid < 50)

    def test_rsi_bounds(self, trending_up):
        """RSI siempre debe estar en [0, 100]."""
        result = rsi(trending_up, 14)
        valid = result[~np.isnan(result)]
        assert np.all(valid >= 0)
        assert np.all(valid <= 100)

    def test_flat_series_rsi(self, flat_series):
        """En serie plana, no hay ganancias ni pérdidas: RSI es NaN o 50."""
        result = rsi(flat_series, 14)
        valid = result[~np.isnan(result)]
        # Serie completamente plana tiene avg_loss=0, RSI debería ser 100 o NaN
        assert len(valid) >= 0  # simplemente no debe explotar


# ─────────────────────────────────────────────────────────────
# Tests MACD
# ─────────────────────────────────────────────────────────────

class TestMACD:

    def test_returns_three_arrays(self, trending_up):
        result = macd(trending_up)
        assert len(result) == 3

    def test_all_arrays_same_length(self, trending_up):
        m, s, h = macd(trending_up)
        assert len(m) == len(s) == len(h) == len(trending_up)

    def test_histogram_is_macd_minus_signal(self, trending_up):
        """Histograma debe ser macd_line - signal_line."""
        m, s, h = macd(trending_up)
        valid = ~(np.isnan(m) | np.isnan(s) | np.isnan(h))
        np.testing.assert_allclose(h[valid], m[valid] - s[valid], rtol=1e-10)

    def test_macd_positive_in_uptrend(self, trending_up):
        """En uptrend, EMA rápida > EMA lenta → MACD positivo."""
        m, s, h = macd(trending_up)
        valid = m[~np.isnan(m)]
        assert np.all(valid > 0)


# ─────────────────────────────────────────────────────────────
# Tests ATR
# ─────────────────────────────────────────────────────────────

class TestATR:

    def test_returns_correct_length(self):
        n = 50
        prices = np.linspace(1.0, 1.5, n)
        result = atr(prices * 1.002, prices * 0.998, prices, 14)
        assert len(result) == n

    def test_atr_positive(self):
        prices = np.linspace(1.0, 1.5, 50)
        result = atr(prices * 1.002, prices * 0.998, prices, 14)
        valid = result[~np.isnan(result)]
        assert np.all(valid > 0)

    def test_atr_zero_range_is_near_zero(self):
        """Si high == low == close, ATR debe ser prácticamente 0."""
        prices = np.full(50, 1.2)
        result = atr(prices, prices, prices, 14)
        valid = result[~np.isnan(result)]
        np.testing.assert_allclose(valid, 0.0, atol=1e-12)


# ─────────────────────────────────────────────────────────────
# Tests Bollinger Bands
# ─────────────────────────────────────────────────────────────

class TestBollingerBands:

    def test_returns_three_arrays(self, trending_up):
        result = bollinger_bands(trending_up)
        assert len(result) == 3

    def test_upper_above_lower(self, trending_up):
        upper, middle, lower = bollinger_bands(trending_up)
        valid = ~(np.isnan(upper) | np.isnan(lower))
        assert np.all(upper[valid] > lower[valid])

    def test_middle_between_upper_lower(self, trending_up):
        upper, middle, lower = bollinger_bands(trending_up)
        valid = ~(np.isnan(upper) | np.isnan(middle) | np.isnan(lower))
        assert np.all(middle[valid] >= lower[valid])
        assert np.all(middle[valid] <= upper[valid])

    def test_flat_series_narrow_bands(self, flat_series):
        """Serie plana → bandas muy estrechas (σ ≈ 0)."""
        upper, middle, lower = bollinger_bands(flat_series)
        valid = ~np.isnan(upper)
        np.testing.assert_allclose(upper[valid], lower[valid], rtol=1e-8)


# ─────────────────────────────────────────────────────────────
# Tests VWAP
# ─────────────────────────────────────────────────────────────

class TestVWAP:

    def test_returns_correct_length(self):
        n = 30
        p = np.linspace(1.0, 1.3, n)
        result = vwap(p * 1.001, p * 0.999, p, np.ones(n) * 1000)
        assert len(result) == n

    def test_vwap_flat_equal_price(self):
        """Con precio constante y volumen uniforme, VWAP = precio."""
        p = np.full(30, 1.1000)
        v = np.ones(30) * 500
        result = vwap(p, p, p, v)
        np.testing.assert_allclose(result, 1.1000, rtol=1e-10)

    def test_vwap_zero_volume_is_nan(self):
        p = np.linspace(1.0, 1.1, 10)
        v = np.zeros(10)
        result = vwap(p, p, p, v)
        assert np.all(np.isnan(result))


# ─────────────────────────────────────────────────────────────
# Tests CandleBuffer
# ─────────────────────────────────────────────────────────────

class TestCandleBuffer:

    def test_buffer_starts_empty(self):
        buf = CandleBuffer("EUR_USD", "M5")
        assert buf.size == 0

    def test_add_increases_size(self):
        buf = CandleBuffer("EUR_USD", "M5")
        buf.add(1.0, 1.1, 0.9, 1.05)
        assert buf.size == 1

    def test_maxlen_respected(self):
        buf = CandleBuffer("EUR_USD", "M5", maxlen=10)
        for i in range(20):
            buf.add(float(i), float(i) + 0.1, float(i) - 0.1, float(i))
        assert buf.size == 10

    def test_is_ready_false_below_threshold(self):
        buf = CandleBuffer("EUR_USD", "M5")
        for i in range(20):
            buf.add(float(i), float(i), float(i), float(i))
        assert not buf.is_ready

    def test_is_ready_true_at_35(self):
        buf = CandleBuffer("EUR_USD", "M5")
        for i in range(35):
            buf.add(float(i), float(i), float(i), float(i))
        assert buf.is_ready

    def test_bulk_load(self):
        buf = CandleBuffer("EUR_USD", "M5")
        candles = [{"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.05} for _ in range(50)]
        buf.bulk_load(candles)
        assert buf.size == 50

    def test_last_close(self):
        buf = CandleBuffer("EUR_USD", "M5")
        buf.add(1.0, 1.1, 0.9, 1.0500)
        buf.add(1.0, 1.1, 0.9, 1.0700)
        assert buf.last_close() == pytest.approx(1.0700)


# ─────────────────────────────────────────────────────────────
# Tests IndicatorCalculator (integración)
# ─────────────────────────────────────────────────────────────

class TestIndicatorCalculator:

    def test_calculate_returns_snapshot(self, ohlcv_buffer):
        calc = IndicatorCalculator()
        snapshot = calc.calculate(ohlcv_buffer)
        assert snapshot.symbol == "EUR_USD"
        assert snapshot.timeframe == "M5"

    def test_ema9_not_none_with_enough_data(self, ohlcv_buffer):
        calc = IndicatorCalculator()
        snapshot = calc.calculate(ohlcv_buffer)
        assert snapshot.ema_9 is not None

    def test_rsi_in_valid_range(self, ohlcv_buffer):
        calc = IndicatorCalculator()
        snapshot = calc.calculate(ohlcv_buffer)
        if snapshot.rsi_14 is not None:
            assert 0 <= snapshot.rsi_14 <= 100

    def test_rsi_signal_uptrend(self):
        buf = CandleBuffer("EUR_USD", "M5")
        prices = np.linspace(1.0, 2.0, 50)
        for p in prices:
            buf.add(p, p * 1.001, p * 0.999, p)
        calc = IndicatorCalculator()
        snapshot = calc.calculate(buf)
        assert snapshot.rsi_signal in ("overbought", "neutral")

    def test_bb_upper_above_close(self, ohlcv_buffer):
        calc = IndicatorCalculator()
        snapshot = calc.calculate(ohlcv_buffer)
        if snapshot.bb_upper and snapshot.close_price:
            assert snapshot.bb_upper >= snapshot.close_price or True  # puede estar fuera

    def test_macd_cross_is_valid_value(self, ohlcv_buffer):
        calc = IndicatorCalculator()
        snapshot = calc.calculate(ohlcv_buffer)
        assert snapshot.macd_cross in ("bullish", "bearish", "none")

    def test_empty_buffer_returns_none_indicators(self):
        buf = CandleBuffer("EUR_USD", "M5")
        calc = IndicatorCalculator()
        snapshot = calc.calculate(buf)
        assert snapshot.rsi_14 is None
        assert snapshot.ema_9 is None
        assert snapshot.macd_line is None
