"""
Detector de sesiones Forex basado en hora UTC.

Las sesiones Forex son períodos del día con características distintas:
- Volumen, spread, volatilidad y pares activos cambian por sesión.
- Conocer la sesión activa es fundamental para:
  * Filtrar señales (no operar en off-hours o Asian si se opera EUR/USD)
  * Ajustar SL/TP según volatilidad esperada
  * Contextualizar el análisis de price action

Implementación:
- Pure Python, sin I/O ni dependencias externas.
- Fácil de testear unitariamente.
- Inmutable: SessionDetector no tiene estado mutable.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class ForexSession(str, Enum):
    ASIAN = "asian"
    LONDON = "london"
    LONDON_NEW_YORK = "london_new_york"
    NEW_YORK = "new_york"
    OFF_HOURS = "off_hours"


@dataclass(frozen=True)
class SessionInfo:
    """
    Descripción inmutable de una sesión Forex.

    Atributos:
        session:          Identificador de la sesión.
        start_utc_hour:   Hora UTC de apertura (inclusive).
        end_utc_hour:     Hora UTC de cierre (exclusive).
        volatility:       Nivel de volatilidad esperado.
        active_pairs:     Pares más activos en esta sesión.
        description:      Descripción legible.
    """

    session: ForexSession
    start_utc_hour: int
    end_utc_hour: int
    volatility: str  # "low" | "medium" | "high"
    active_pairs: list[str] = field(default_factory=list)
    description: str = ""


# Definición oficial de sesiones Forex en UTC
# Nota: Los horarios varían ligeramente con cambio de hora en distintos países.
# Estos valores son los más utilizados en análisis institucional.
SESSION_SCHEDULE: list[SessionInfo] = [
    SessionInfo(
        session=ForexSession.ASIAN,
        start_utc_hour=0,
        end_utc_hour=8,
        volatility="low",
        active_pairs=["USD_JPY", "AUD_USD", "NZD_USD", "AUD_JPY", "EUR_JPY"],
        description="Asian Session (Tokyo + Sydney) — Low volatility, range-bound",
    ),
    SessionInfo(
        session=ForexSession.LONDON,
        start_utc_hour=8,
        end_utc_hour=13,
        volatility="high",
        active_pairs=["EUR_USD", "GBP_USD", "EUR_GBP", "USD_CHF", "EUR_JPY"],
        description="London Session — Highest liquidity in EUR/GBP pairs, large moves",
    ),
    SessionInfo(
        session=ForexSession.LONDON_NEW_YORK,
        start_utc_hour=13,
        end_utc_hour=17,
        volatility="high",
        active_pairs=[
            "EUR_USD",
            "GBP_USD",
            "USD_JPY",
            "USD_CHF",
            "USD_CAD",
            "GBP_JPY",
        ],
        description="London/NY Overlap — Maximum volume, best trends and breakouts",
    ),
    SessionInfo(
        session=ForexSession.NEW_YORK,
        start_utc_hour=17,
        end_utc_hour=22,
        volatility="medium",
        active_pairs=["USD_JPY", "USD_CAD", "EUR_USD", "USD_CHF"],
        description="New York Session — USD-centric, economic data releases",
    ),
    SessionInfo(
        session=ForexSession.OFF_HOURS,
        start_utc_hour=22,
        end_utc_hour=24,
        volatility="low",
        active_pairs=[],
        description="Off-hours — Minimal liquidity, avoid trading",
    ),
]

# Lookup rápido por hora UTC → SessionInfo
_HOUR_TO_SESSION: dict[int, SessionInfo] = {}
for _session_info in SESSION_SCHEDULE:
    for _h in range(_session_info.start_utc_hour, _session_info.end_utc_hour):
        _HOUR_TO_SESSION[_h] = _session_info


class SessionDetector:
    """
    Detecta la sesión Forex activa para un datetime UTC dado.

    Sin estado mutable: thread-safe y async-safe por diseño.
    """

    def get_current_session(self, dt: datetime | None = None) -> SessionInfo:
        """
        Devuelve la SessionInfo para la hora dada (o ahora si dt=None).

        Args:
            dt: datetime UTC. Si es None, usa datetime.now(UTC).

        Returns:
            SessionInfo con descripción, volatilidad y pares activos.
        """
        if dt is None:
            dt = datetime.now(timezone.utc)

        hour = dt.hour
        return _HOUR_TO_SESSION.get(hour, SESSION_SCHEDULE[-1])  # fallback OFF_HOURS

    def get_all_sessions(self) -> list[SessionInfo]:
        """Devuelve la lista completa de definiciones de sesiones."""
        return list(SESSION_SCHEDULE)

    def is_high_volatility_now(self) -> bool:
        """True si la sesión actual es de alta volatilidad."""
        return self.get_current_session().volatility == "high"

    def is_pair_active_now(self, symbol: str) -> bool:
        """True si el par dado está en la lista de pares activos de la sesión actual."""
        session = self.get_current_session()
        return symbol in session.active_pairs
