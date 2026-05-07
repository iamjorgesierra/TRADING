"""consumers package."""

from datetime import datetime, timezone

from loguru import logger

from shared.redis.streams import StreamConsumer

from ..core.buffer import BufferRegistry
from ..core.calculator import IndicatorCalculator


class CandleConsumer:
    """
    Consumer de candles que actualiza buffers y lanza cálculo de indicadores.

    Args:
        stream_consumer: StreamConsumer pre-configurado para market:candles.
        buffer_registry: Registro global de CandleBuffers.
        calculator:      Motor de cálculo de indicadores.
        indicator_publisher: Callable async para publicar snapshots.
    """

    def __init__(
        self,
        stream_consumer: StreamConsumer,
        buffer_registry: BufferRegistry,
        calculator: IndicatorCalculator,
        indicator_publisher,  # IndicatorPublisher — evita import circular
    ) -> None:
        self._consumer = stream_consumer
        self._registry = buffer_registry
        self._calculator = calculator
        self._publisher = indicator_publisher

    async def start(self) -> None:
        """Inicia el bucle de consumo (diseñado para asyncio.Task)."""
        logger.info("CandleConsumer iniciado — escuchando market:candles")
        await self._consumer.consume(handler=self._handle_candle)

    async def _handle_candle(self, data: dict) -> None:
        """
        Procesa una vela recibida del stream.

        Args:
            data: Diccionario con campos: symbol, timeframe, open, high, low, close,
                  volume (opcional), timestamp.
        """
        symbol = data.get("symbol", "")
        timeframe = data.get("timeframe", "")

        if not symbol or not timeframe:
            logger.warning("Candle recibida sin symbol o timeframe: {}", data)
            return

        try:
            open_ = float(data["open"])
            high = float(data["high"])
            low = float(data["low"])
            close = float(data["close"])
            volume = float(data.get("volume", 0.0))
        except (KeyError, ValueError) as exc:
            logger.warning("Candle con campos OHLCV inválidos: {} — error: {}", data, exc)
            return

        # Actualizar buffer
        buffer = self._registry.get_or_create(symbol, timeframe)
        buffer.add(open_=open_, high=high, low=low, close=close, volume=volume)

        # Calcular indicadores solo si hay suficientes datos
        if not buffer.is_ready:
            logger.debug(
                "Buffer {}/{} aún cargando: {}/{} barras",
                symbol,
                timeframe,
                buffer.size,
                35,
            )
            return

        # Parsear timestamp de la vela
        try:
            ts_str = data.get("timestamp", "")
            timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else datetime.now(timezone.utc)
        except ValueError:
            timestamp = datetime.now(timezone.utc)

        # Calcular y publicar snapshot
        snapshot = self._calculator.calculate(buffer, timestamp=timestamp)
        await self._publisher.publish(snapshot)

        logger.debug(
            "Indicadores calculados {}/{} — RSI: {:.1f}, EMA9: {:.5f}",
            symbol,
            timeframe,
            snapshot.rsi_14 or 0.0,
            snapshot.ema_9 or 0.0,
        )
