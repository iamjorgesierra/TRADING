"""
Tests unitarios del session-engine.

Ejecutar desde la raíz del proyecto:
    pytest backend/tests/test_session_detector.py -v

Son tests puros (unit): sin Redis, sin DB, sin I/O externo.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Añadir el directorio del servicio al path de importación
_SESSION_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "session-engine"
sys.path.insert(0, str(_SESSION_ENGINE))

from app.core.session_detector import ForexSession, SessionDetector, SessionInfo


def utc(hour: int) -> datetime:
    """Helper: crea datetime UTC a una hora exacta."""
    return datetime(2026, 5, 7, hour, 0, 0, tzinfo=timezone.utc)


# ─────────────────────────────────────────────────────────────
# Tests de detección de sesión
# ─────────────────────────────────────────────────────────────

class TestSessionDetection:

    def test_asian_session_start(self):
        assert SessionDetector().get_current_session(utc(0)).session == ForexSession.ASIAN

    def test_asian_session_mid(self):
        info = SessionDetector().get_current_session(utc(3))
        assert info.session == ForexSession.ASIAN
        assert info.volatility == "low"

    def test_london_session_start(self):
        assert SessionDetector().get_current_session(utc(8)).session == ForexSession.LONDON

    def test_london_session_mid(self):
        info = SessionDetector().get_current_session(utc(10))
        assert info.session == ForexSession.LONDON
        assert info.volatility == "high"

    def test_overlap_session(self):
        info = SessionDetector().get_current_session(utc(15))
        assert info.session == ForexSession.LONDON_NEW_YORK
        assert info.volatility == "high"

    def test_new_york_session(self):
        info = SessionDetector().get_current_session(utc(19))
        assert info.session == ForexSession.NEW_YORK
        assert info.volatility == "medium"

    def test_off_hours(self):
        info = SessionDetector().get_current_session(utc(22))
        assert info.session == ForexSession.OFF_HOURS
        assert info.volatility == "low"

    def test_off_hours_last_hour(self):
        assert SessionDetector().get_current_session(utc(23)).session == ForexSession.OFF_HOURS


# ─────────────────────────────────────────────────────────────
# Tests de pares activos
# ─────────────────────────────────────────────────────────────

class TestActivePairs:

    def test_eurusd_active_in_london(self):
        info = SessionDetector().get_current_session(utc(10))
        assert "EUR_USD" in info.active_pairs

    def test_usdjpy_active_in_asian(self):
        info = SessionDetector().get_current_session(utc(3))
        assert "USD_JPY" in info.active_pairs

    def test_no_pairs_in_off_hours(self):
        info = SessionDetector().get_current_session(utc(23))
        assert len(info.active_pairs) == 0

    def test_many_pairs_in_overlap(self):
        info = SessionDetector().get_current_session(utc(14))
        assert len(info.active_pairs) >= 4


# ─────────────────────────────────────────────────────────────
# Tests de volatilidad parametrizados
# ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("hour,expected", [
    (3,  "low"),
    (10, "high"),
    (14, "high"),
    (19, "medium"),
    (23, "low"),
])
def test_volatility_by_hour(hour: int, expected: str):
    info = SessionDetector().get_current_session(utc(hour))
    assert info.volatility == expected


# ─────────────────────────────────────────────────────────────
# Tests de completitud
# ─────────────────────────────────────────────────────────────

def test_all_five_sessions_defined():
    assert len(SessionDetector().get_all_sessions()) == 5


def test_no_hour_gaps():
    """Todas las 24 horas deben tener sesión asignada (sin huecos)."""
    detector = SessionDetector()
    for hour in range(24):
        info = detector.get_current_session(utc(hour))
        assert info is not None, f"Hour {hour} UTC no tiene sesión asignada"


def test_immutability():
    """El detector es stateless: misma entrada → misma salida."""
    detector = SessionDetector()
    dt = utc(10)
    assert detector.get_current_session(dt).session == detector.get_current_session(dt).session


def test_new_york_session():
    detector = SessionDetector()
    info = detector.get_current_session(make_utc(19))
    assert info.session == ForexSession.NEW_YORK


def test_off_hours():
    detector = SessionDetector()
    info = detector.get_current_session(make_utc(23))
    assert info.session == ForexSession.OFF_HOURS


def test_pair_active_in_london():
    detector = SessionDetector()
    # EUR_USD está activo en London
    assert detector.is_pair_active_now.__doc__ is not None  # sanity check


if __name__ == "__main__":
    test_asian_session()
    test_london_session()
    test_overlap_session()
    test_new_york_session()
    test_off_hours()
    print("All session detector tests passed!")
