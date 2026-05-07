"""
Utilidades de tiempo para sistemas financieros.

En trading, todos los timestamps deben ser UTC.
Estas funciones garantizan consistencia en toda la plataforma.
"""

from datetime import datetime, timezone, timedelta


def utcnow() -> datetime:
    """Devuelve datetime actual con timezone UTC explícita."""
    return datetime.now(timezone.utc)


def to_utc(dt: datetime) -> datetime:
    """
    Convierte un datetime naive o aware a UTC.

    Un datetime naive se asume como UTC (convención interna del sistema).
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def floor_to_minute(dt: datetime) -> datetime:
    """Trunca el datetime al minuto (útil para alinear timestamps de velas)."""
    return dt.replace(second=0, microsecond=0)


def floor_to_seconds(dt: datetime, seconds: int) -> datetime:
    """Trunca al bucket de N segundos (e.g., 5s para velas S5)."""
    total_seconds = int(dt.timestamp())
    floored = total_seconds - (total_seconds % seconds)
    return datetime.fromtimestamp(floored, tz=dt.tzinfo or timezone.utc)


def timeframe_to_seconds(timeframe: str) -> int:
    """
    Convierte granularidad OANDA a segundos.

    OANDA soporta: S5, S10, S15, S30, M1, M2, M4, M5, M10, M15, M30,
                   H1, H2, H3, H4, H6, H8, H12, D, W, M
    """
    mapping: dict[str, int] = {
        "S5": 5,
        "S10": 10,
        "S15": 15,
        "S30": 30,
        "M1": 60,
        "M2": 120,
        "M4": 240,
        "M5": 300,
        "M10": 600,
        "M15": 900,
        "M30": 1800,
        "H1": 3_600,
        "H2": 7_200,
        "H3": 10_800,
        "H4": 14_400,
        "H6": 21_600,
        "H8": 28_800,
        "H12": 43_200,
        "D": 86_400,
        "W": 604_800,
        "M": 2_592_000,
    }
    return mapping.get(timeframe.upper(), 60)
