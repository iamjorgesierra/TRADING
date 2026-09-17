"""Unit tests for the session-engine session detector."""

from datetime import datetime, timezone

import pytest

from session_app.core.session_detector import ForexSession, SessionDetector


def utc(hour: int) -> datetime:
    """Create a UTC datetime at an exact hour."""
    return datetime(2026, 5, 7, hour, 0, 0, tzinfo=timezone.utc)


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


@pytest.mark.parametrize(
    "hour,expected",
    [(3, "low"), (10, "high"), (14, "high"), (19, "medium"), (23, "low")],
)
def test_volatility_by_hour(hour: int, expected: str):
    info = SessionDetector().get_current_session(utc(hour))
    assert info.volatility == expected


def test_all_five_sessions_defined():
    assert len(SessionDetector().get_all_sessions()) == 5


def test_no_hour_gaps():
    detector = SessionDetector()
    for hour in range(24):
        assert detector.get_current_session(utc(hour)) is not None


def test_detector_is_stateless():
    detector = SessionDetector()
    dt = utc(10)
    first = detector.get_current_session(dt)
    second = detector.get_current_session(dt)
    assert first == second


def test_pair_active_contract():
    detector = SessionDetector()
    assert detector.is_pair_active_now.__doc__ is not None
