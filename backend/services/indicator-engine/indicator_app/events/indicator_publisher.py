"""
Publisher de indicadores.

Publica IndicatorSnapshot en el stream `indicators:values` de Redis.
"""

from shared.redis.streams import StreamProducer
from ..schemas import IndicatorSnapshot


class IndicatorPublisher:
    """
    Serializa y publica snapshots de indicadores en Redis Streams.

    Args:
        producer: StreamProducer configurado para `indicators:values`.
    """

    def __init__(self, producer: StreamProducer) -> None:
        self._producer = producer

    async def publish(self, snapshot: IndicatorSnapshot) -> str:
        """
        Publica un IndicatorSnapshot en Redis Streams.

        Returns:
            ID del mensaje publicado.
        """
        return await self._producer.publish(snapshot.to_stream_dict())
