"""
Configuración centralizada del sistema usando Pydantic Settings v2.

Decisión técnica:
- Pydantic Settings permite validación estricta de variables de entorno.
- El decorador @lru_cache garantiza una única instancia (Singleton) por proceso.
- Todas las configuraciones se derivan de variables de entorno o valores por defecto.
- Facilita el testing mediante override de settings.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Configuración principal de la plataforma.

    Carga variables de entorno desde .env o del entorno del proceso.
    Validación automática de tipos en tiempo de arranque.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # ------------------------------------------------------------------
    # Aplicación
    # ------------------------------------------------------------------
    env: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"

    # ------------------------------------------------------------------
    # PostgreSQL / TimescaleDB
    # ------------------------------------------------------------------
    database_url: str = Field(
        default="postgresql+asyncpg://trading:trading_secret@localhost:5432/tradingdb",
        description="SQLAlchemy async DSN para PostgreSQL/TimescaleDB",
    )

    # ------------------------------------------------------------------
    # Redis
    # ------------------------------------------------------------------
    redis_url: str = Field(
        default="redis://:redis_secret@localhost:6379/0",
        description="URL de conexión Redis con autenticación",
    )

    # ------------------------------------------------------------------
    # OANDA
    # ------------------------------------------------------------------
    oanda_api_key: str = Field(default="", description="Token de API OANDA")
    oanda_account_id: str = Field(default="", description="ID de cuenta OANDA")
    oanda_env: Literal["practice", "live"] = Field(
        default="practice",
        description="Entorno OANDA: 'practice' (demo) o 'live' (real)",
    )

    # ------------------------------------------------------------------
    # Mercados — Símbolos y timeframes por defecto
    # ------------------------------------------------------------------
    default_symbols: list[str] = Field(
        default=[
            "EUR_USD",
            "GBP_USD",
            "USD_JPY",
            "USD_CHF",
            "AUD_USD",
            "USD_CAD",
            "NZD_USD",
        ],
        description="Lista de instrumentos Forex a monitorear",
    )
    default_timeframes: list[str] = Field(
        default=["S10", "M1", "M5", "M15", "H1", "H4", "D"],
        description="Granularidades OANDA a soportar",
    )

    # ------------------------------------------------------------------
    # Redis Streams — Nombres de topics
    # ------------------------------------------------------------------
    stream_market_ticks: str = "market:ticks"
    stream_market_candles: str = "market:candles"
    stream_session_events: str = "sessions:events"
    stream_system_events: str = "system:events"
    stream_indicators: str = "indicators:values"

    # ------------------------------------------------------------------
    # Redis Streams — Retención
    # ------------------------------------------------------------------
    stream_ticks_maxlen: int = Field(
        default=100_000,
        description="Máximo de mensajes en stream market:ticks (aprox.)",
    )
    stream_candles_maxlen: int = Field(
        default=50_000,
        description="Máximo de mensajes en stream market:candles (aprox.)",
    )

    # ------------------------------------------------------------------
    # Computed properties (no leídas de .env)
    # ------------------------------------------------------------------
    @computed_field  # type: ignore[misc]
    @property
    def oanda_rest_url(self) -> str:
        """URL base para REST API de OANDA."""
        if self.oanda_env == "live":
            return "https://api-fxtrade.oanda.com"
        return "https://api-fxpractice.oanda.com"

    @computed_field  # type: ignore[misc]
    @property
    def oanda_stream_url(self) -> str:
        """URL base para Streaming API de OANDA."""
        if self.oanda_env == "live":
            return "https://stream-fxtrade.oanda.com"
        return "https://stream-fxpractice.oanda.com"

    @computed_field  # type: ignore[misc]
    @property
    def is_production(self) -> bool:
        return self.env == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Devuelve la instancia Singleton de Settings.

    El decorador lru_cache garantiza que la instancia se crea una sola vez
    por proceso. Para testing, usar get_settings.cache_clear() antes de cada test.
    """
    return Settings()
