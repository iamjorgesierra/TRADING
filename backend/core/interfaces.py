"""
Protocolos (interfaces) del dominio — Python structural subtyping.

Decisión técnica:
- Usar Protocol en lugar de ABC permite duck typing estricto sin herencia.
- Todos los repositorios, publishers y consumers deben cumplir estos contratos.
- Facilita el testing con mocks que solo implementan el protocol.
- Compatible con mypy --strict para verificación estática completa.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class IPublisher(Protocol):
    """Contrato para cualquier publisher de eventos (Redis Streams, Kafka, etc.)."""

    async def publish(self, data: dict[str, Any]) -> str:
        """
        Publica un evento. Devuelve el ID del mensaje publicado.

        Args:
            data: Diccionario de campos del mensaje.

        Returns:
            ID del mensaje (e.g., Redis Stream entry ID).
        """
        ...


@runtime_checkable
class IConsumer(Protocol):
    """Contrato para cualquier consumer de eventos."""

    async def start(self) -> None:
        """Inicia el bucle de consumo (diseñado para asyncio.Task)."""
        ...


@runtime_checkable
class IHealthCheck(Protocol):
    """Contrato para verificación de salud de cualquier componente."""

    async def is_healthy(self) -> bool:
        """Devuelve True si el componente está operativo."""
        ...
