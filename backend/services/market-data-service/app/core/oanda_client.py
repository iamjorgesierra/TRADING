"""
Cliente OANDA — Streaming de precios y fetch de velas históricas.

OANDA ofrece dos APIs relevantes:
  1. Streaming API (SSE/NDJSON): precios en tiempo real con bids/asks.
  2. REST API: velas históricas OHLCV.

Decisiones técnicas:
- httpx.AsyncClient con timeout=None para streaming (conexión larga).
- aiter_lines() para consumo eficiente línea a línea del NDJSON stream.
- Reconexión automática con backoff exponencial (1s → 60s).
- El cliente es stateful: _running controla el bucle de streaming.
- fetch_candles usa una sesión httpx por request (corta duración).

Referencia API:
  https://developer.oanda.com/rest-live-v20/pricing-ep/
"""

import asyncio
import json
from collections.abc import Awaitable, Callable

import httpx
from loguru import logger

from shared.config.settings import Settings
from shared.schemas.market import CandleData, TickData


class OandaStreamingClient:
    """
    Conector al streaming de precios OANDA.

    Ciclo de vida:
        client.start()                       → habilita el bucle
        await client.stream_prices(...)      → corre indefinidamente
        client.stop()                        → señala shutdown al bucle
    """

    _MIN_RETRY_DELAY: float = 1.0
    _MAX_RETRY_DELAY: float = 60.0

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._running: bool = False
        self._headers: dict[str, str] = {
            "Authorization": f"Bearer {settings.oanda_api_key}",
            "Accept-Datetime-Format": "RFC3339",
            "Content-Type": "application/json",
        }

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self) -> None:
        """Habilita el bucle de streaming. Llamar antes de stream_prices()."""
        self._running = True

    def stop(self) -> None:
        """Señala al bucle que debe detenerse en la próxima iteración."""
        self._running = False

    async def stream_prices(
        self,
        symbols: list[str],
        on_tick: Callable[[TickData], Awaitable[None]],
    ) -> None:
        """
        Conecta al OANDA Pricing Stream y emite ticks.

        OANDA envía NDJSON con dos tipos de mensaje:
          - {"type": "PRICE", "instrument": ..., "bids": [...], "asks": [...], ...}
          - {"type": "HEARTBEAT", "time": ...}

        Args:
            symbols:  Lista de instrumentos (e.g., ["EUR_USD", "GBP_USD"]).
            on_tick:  Coroutine llamada por cada tick recibido.
        """
        instruments = ",".join(symbols)
        url = (
            f"{self._settings.oanda_stream_url}/v3/accounts/"
            f"{self._settings.oanda_account_id}/pricing/stream"
        )
        params = {"instruments": instruments}
        retry_delay = self._MIN_RETRY_DELAY

        while self._running:
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream(
                        "GET", url, headers=self._headers, params=params
                    ) as response:
                        response.raise_for_status()
                        retry_delay = self._MIN_RETRY_DELAY  # reset tras conexión exitosa

                        logger.info(
                            f"OANDA stream connected  instruments={instruments}"
                        )

                        async for line in response.aiter_lines():
                            if not self._running:
                                return

                            if not line.strip():
                                continue

                            try:
                                msg = json.loads(line)
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON from OANDA: {line[:100]}")
                                continue

                            msg_type = msg.get("type")

                            if msg_type == "PRICE":
                                await self._handle_price(msg, on_tick)
                            elif msg_type == "HEARTBEAT":
                                logger.debug(f"OANDA heartbeat  time={msg.get('time')}")
                            else:
                                logger.debug(f"Unknown OANDA message type: {msg_type}")

            except asyncio.CancelledError:
                logger.info("OANDA streaming cancelled")
                return
            except httpx.HTTPStatusError as exc:
                logger.error(
                    f"OANDA HTTP error  status={exc.response.status_code}  "
                    f"retry_in={retry_delay}s"
                )
            except httpx.RequestError as exc:
                logger.error(
                    f"OANDA connection error  error={exc}  retry_in={retry_delay}s"
                )
            except Exception as exc:
                logger.error(
                    f"OANDA stream unexpected error  error={exc}  retry_in={retry_delay}s"
                )

            if self._running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self._MAX_RETRY_DELAY)

    async def _handle_price(
        self,
        msg: dict,
        on_tick: Callable[[TickData], Awaitable[None]],
    ) -> None:
        """Parsea un mensaje PRICE de OANDA y llama al callback on_tick."""
        try:
            tick = TickData.from_oanda(
                symbol=msg["instrument"],
                timestamp=msg["time"],
                bid=msg["bids"][0]["price"],
                ask=msg["asks"][0]["price"],
            )
            await on_tick(tick)
        except (KeyError, IndexError, ValueError) as exc:
            logger.error(f"Error parsing OANDA PRICE message  error={exc}  msg={msg}")

    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        count: int = 100,
    ) -> list[CandleData]:
        """
        Obtiene velas históricas desde OANDA REST API.

        Args:
            symbol:    Instrumento (e.g., "EUR_USD").
            timeframe: Granularidad OANDA (e.g., "M15", "H1").
            count:     Número de velas a obtener (máx. 5000 en OANDA).

        Returns:
            Lista de CandleData ordenada por tiempo ascendente.
        """
        url = f"{self._settings.oanda_rest_url}/v3/instruments/{symbol}/candles"
        params = {
            "count": count,
            "granularity": timeframe,
            "price": "M",  # Midpoint (promedio bid/ask)
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            data = response.json()

        candles = [
            CandleData.from_oanda(symbol=symbol, timeframe=timeframe, raw=c)
            for c in data.get("candles", [])
            if c.get("complete", True)
        ]

        logger.debug(
            f"Fetched candles  symbol={symbol}  timeframe={timeframe}  count={len(candles)}"
        )
        return candles
