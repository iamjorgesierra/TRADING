"""
Cálculo vectorizado de indicadores técnicos.

Diseño deliberado:
- Funciones puras sin estado: cada indicador recibe un array y devuelve un array.
- Basado en numpy puro (sin pandas en el hot path) para minimizar latencia.
- pandas solo se usa en la capa de repositorio para queries complejas.
- Los cálculos son incrementales cuando es posible (se añade una barra nueva
  en lugar de recalcular toda la serie).
- Los indicadores retornan NaN donde no hay suficiente historial.

Indicadores implementados:
- EMA: Exponential Moving Average (período configurable)
- RSI: Relative Strength Index (14 períodos estándar)
- MACD: Moving Average Convergence Divergence (12, 26, 9)
- ATR: Average True Range
- VWAP: Volume Weighted Average Price (reset diario)
- Bollinger Bands: (20 períodos, 2 desviaciones estándar)
"""

import numpy as np


def ema(prices: np.ndarray, period: int) -> np.ndarray:
    """
    Exponential Moving Average.

    Args:
        prices: Array de precios de cierre (más antiguo primero).
        period: Período del EMA.

    Returns:
        Array de valores EMA. Los primeros `period - 1` valores son NaN.
    """
    if len(prices) < period:
        return np.full(len(prices), np.nan)

    result = np.full(len(prices), np.nan)
    k = 2.0 / (period + 1)

    # Primer EMA = SMA de los primeros `period` valores
    result[period - 1] = np.mean(prices[:period])

    for i in range(period, len(prices)):
        result[i] = prices[i] * k + result[i - 1] * (1 - k)

    return result


def rsi(prices: np.ndarray, period: int = 14) -> np.ndarray:
    """
    Relative Strength Index (RSI) de Wilder.

    Args:
        prices: Array de precios de cierre (más antiguo primero).
        period: Período del RSI (estándar = 14).

    Returns:
        Array de valores RSI [0, 100]. Los primeros `period` valores son NaN.
    """
    if len(prices) < period + 1:
        return np.full(len(prices), np.nan)

    result = np.full(len(prices), np.nan)
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    # Primer RSI: SMA simple de gains/losses
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    if avg_loss == 0:
        result[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        result[period] = 100.0 - (100.0 / (1.0 + rs))

    # RSI siguientes: suavizado de Wilder
    for i in range(period + 1, len(prices)):
        j = i - 1  # índice en deltas/gains/losses (len = len(prices) - 1)
        avg_gain = (avg_gain * (period - 1) + gains[j]) / period
        avg_loss = (avg_loss * (period - 1) + losses[j]) / period
        if avg_loss == 0:
            result[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            result[i] = 100.0 - (100.0 / (1.0 + rs))

    return result


def macd(
    prices: np.ndarray,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    MACD — Moving Average Convergence Divergence.

    Args:
        prices: Array de precios de cierre.
        fast:   Período EMA rápida (estándar = 12).
        slow:   Período EMA lenta (estándar = 26).
        signal: Período de la señal EMA (estándar = 9).

    Returns:
        Tupla (macd_line, signal_line, histogram).
        Todos los arrays tienen la misma longitud que prices.
        Los primeros valores hasta `slow + signal - 2` son NaN.
    """
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    macd_line = ema_fast - ema_slow

    # Signal = EMA(macd_line), ignorando NaN al inicio
    valid_mask = ~np.isnan(macd_line)
    if not np.any(valid_mask):
        return macd_line, np.full(len(prices), np.nan), np.full(len(prices), np.nan)

    first_valid = np.argmax(valid_mask)
    signal_line = np.full(len(prices), np.nan)
    macd_valid = macd_line[first_valid:]

    if len(macd_valid) >= signal:
        sig_values = ema(macd_valid, signal)
        signal_line[first_valid:] = sig_values

    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def atr(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 14,
) -> np.ndarray:
    """
    Average True Range (ATR) de Wilder.

    Args:
        high:   Array de precios máximos.
        low:    Array de precios mínimos.
        close:  Array de precios de cierre.
        period: Período del ATR (estándar = 14).

    Returns:
        Array de valores ATR. Los primeros `period` valores son NaN.
    """
    n = len(close)
    if n < 2:
        return np.full(n, np.nan)

    result = np.full(n, np.nan)
    # True Range: max(H-L, |H-Cp|, |L-Cp|)
    hl = high[1:] - low[1:]
    hcp = np.abs(high[1:] - close[:-1])
    lcp = np.abs(low[1:] - close[:-1])
    tr = np.maximum(hl, np.maximum(hcp, lcp))

    if len(tr) < period:
        return result

    # Primer ATR = SMA simple del true range
    result[period] = np.mean(tr[:period])

    # ATR siguientes: suavizado de Wilder
    for i in range(period + 1, n):
        j = i - 1  # índice en tr (len = n - 1)
        result[i] = (result[i - 1] * (period - 1) + tr[j]) / period

    return result


def bollinger_bands(
    prices: np.ndarray,
    period: int = 20,
    std_dev: float = 2.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Bandas de Bollinger.

    Args:
        prices:  Array de precios de cierre.
        period:  Ventana de la media móvil simple (estándar = 20).
        std_dev: Multiplicador de desviación estándar (estándar = 2.0).

    Returns:
        Tupla (upper_band, middle_band, lower_band).
        Los primeros `period - 1` valores son NaN.
    """
    n = len(prices)
    upper = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower = np.full(n, np.nan)

    for i in range(period - 1, n):
        window = prices[i - period + 1 : i + 1]
        sma = np.mean(window)
        sd = np.std(window, ddof=1)  # ddof=1 → desviación muestral
        middle[i] = sma
        upper[i] = sma + std_dev * sd
        lower[i] = sma - std_dev * sd

    return upper, middle, lower


def vwap(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
) -> np.ndarray:
    """
    Volume Weighted Average Price (VWAP).

    Calcula VWAP acumulado desde la primera barra del array.
    En producción, pasar solo barras del día en curso para el reset diario.

    Args:
        high:   Array de precios máximos.
        low:    Array de precios mínimos.
        close:  Array de precios de cierre.
        volume: Array de volumen por barra.

    Returns:
        Array de valores VWAP. Nunca tiene NaN si volume > 0.
    """
    typical_price = (high + low + close) / 3.0
    cumulative_tp_vol = np.cumsum(typical_price * volume)
    cumulative_vol = np.cumsum(volume)

    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.where(
            cumulative_vol > 0,
            cumulative_tp_vol / cumulative_vol,
            np.nan,
        )
    return result
