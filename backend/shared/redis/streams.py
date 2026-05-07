"""
Capa de abstracción sobre Redis Streams (XADD / XREADGROUP / XACK).

Arquitectura orientada a eventos:
- StreamProducer: publica mensajes en un stream con XADD.
  Usa MAXLEN APPROXIMATE para limitar el tamaño del stream sin overhead.
- StreamConsumer: consume mensajes de un consumer group con XREADGROUP.
  Garantía at-least-once: ACK solo tras procesamiento exitoso.
  En caso de error, el mensaje queda pendiente y puede ser reclamado.

Decisión técnica sobre Redis Streams vs Kafka:
- Redis Streams es suficiente para FASE 1: latencia <1ms, integración simple,
  sin brokers adicionales.
- Kafka se evaluará en FASE 2+ si se requiere replay extenso o fan-out masivo.
"""

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger
from redis.asyncio import Redis


class StreamProducer:
    """
    Publica eventos en un Redis Stream.

    Parámetros:
        client:      Cliente Redis asíncrono.
        stream_name: Nombre del stream (e.g., "market:ticks").
        max_len:     Tamaño máximo aproximado del stream (MAXLEN ~).
    """

    def __init__(self, client: Redis, stream_name: str, max_len: int = 100_000) -> None:
        self._client = client
        self._stream = stream_name
        self._max_len = max_len

    async def publish(self, data: dict[str, Any]) -> str:
        """
        Serializa y publica un evento en el stream.

        Los valores dict/list se serializan a JSON string ya que Redis Streams
        solo acepta strings en los fields.

        Returns:
            ID del mensaje publicado (e.g., "1234567890-0").
        """
        payload: dict[str, str] = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                payload[k] = json.dumps(v)
            else:
                payload[k] = str(v)

        msg_id: str = await self._client.xadd(
            self._stream,
            payload,
            maxlen=self._max_len,
            approximate=True,
        )
        logger.debug(f"Published  stream={self._stream}  id={msg_id}")
        return msg_id


class StreamConsumer:
    """
    Consume mensajes de un Redis Stream usando consumer groups.

    Garantías:
    - Cada mensaje es procesado por exactamente un consumer del grupo.
    - ACK solo tras handler exitoso → at-least-once delivery.
    - En caso de excepción en el handler, el mensaje queda PEL (pendiente)
      y puede ser reclamado con XCLAIM (no implementado en FASE 1).

    Args:
        client:        Cliente Redis asíncrono.
        stream_name:   Nombre del stream.
        group_name:    Nombre del consumer group.
        consumer_name: Nombre único del consumer (e.g., "worker-1").
        batch_size:    Mensajes por poll (XREADGROUP COUNT).
    """

    def __init__(
        self,
        client: Redis,
        stream_name: str,
        group_name: str,
        consumer_name: str,
        batch_size: int = 50,
    ) -> None:
        self._client = client
        self._stream = stream_name
        self._group = group_name
        self._consumer = consumer_name
        self._batch_size = batch_size

    async def ensure_group(self) -> None:
        """
        Crea el consumer group si no existe.
        MKSTREAM crea el stream si tampoco existe.
        """
        try:
            await self._client.xgroup_create(
                self._stream, self._group, id="$", mkstream=True
            )
            logger.info(
                f"Consumer group created  stream={self._stream}  group={self._group}"
            )
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.debug(
                    f"Consumer group already exists  stream={self._stream}  group={self._group}"
                )
            else:
                raise

    async def consume(
        self,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
        block_ms: int = 2000,
    ) -> None:
        """
        Bucle infinito de consumo. Diseñado para ejecutarse como asyncio.Task.

        - BLOCK block_ms: espera hasta block_ms ms nuevos mensajes.
        - ">" special ID: consume solo mensajes nuevos (no entregados aún).
        - Tras handler exitoso → XACK.
        - Excepción en handler → log + continúa (mensaje queda pendiente).
        - asyncio.CancelledError se propaga para shutdown limpio.
        """
        await self.ensure_group()
        logger.info(
            f"Consumer started  stream={self._stream}  group={self._group}  consumer={self._consumer}"
        )

        while True:
            try:
                results: list = await self._client.xreadgroup(
                    self._group,
                    self._consumer,
                    {self._stream: ">"},
                    count=self._batch_size,
                    block=block_ms,
                )

                if not results:
                    continue

                for _stream_name, messages in results:
                    for msg_id, fields in messages:
                        try:
                            parsed = {k: _try_parse_json(v) for k, v in fields.items()}
                            await handler(parsed)
                            await self._client.xack(self._stream, self._group, msg_id)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.error(
                                f"Handler error  msg_id={msg_id}  error={exc}"
                            )

            except asyncio.CancelledError:
                logger.info(
                    f"Consumer cancelled  stream={self._stream}  group={self._group}"
                )
                raise
            except Exception as exc:
                logger.error(f"Consumer loop error  stream={self._stream}  error={exc}")
                await asyncio.sleep(2)


def _try_parse_json(value: str) -> Any:
    """Intenta deserializar JSON; si falla, devuelve el string original."""
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value
