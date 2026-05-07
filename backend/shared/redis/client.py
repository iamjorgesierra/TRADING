"""
Cliente Redis asíncrono centralizado.

Decisión técnica:
- Patrón Singleton a nivel de proceso: una sola conexión (pool) reutilizada
  por todos los componentes del servicio.
- redis-py >= 5.x incluye redis.asyncio como módulo oficial.
- decode_responses=True simplifica el manejo de strings sin decode manual.
"""

from typing import Optional

from loguru import logger
from redis.asyncio import Redis

_client: Optional[Redis] = None


async def get_redis_client(url: str) -> Redis:
    """
    Devuelve el cliente Redis Singleton, creándolo si no existe.

    El cliente redis-py gestiona internamente un connection pool.
    Llamadas concurrentes son seguras; el pool maneja la concurrencia.

    Args:
        url: Redis URL con autenticación (redis://:password@host:port/db)
    """
    global _client
    if _client is None:
        _client = Redis.from_url(
            url,
            decode_responses=True,
            encoding="utf-8",
            socket_timeout=10,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        # Verificar conectividad en startup
        await _client.ping()
        logger.info("Redis connection established")
    return _client


async def close_redis_client() -> None:
    """Cierra el cliente y libera el pool de conexiones."""
    global _client
    if _client:
        await _client.aclose()
        _client = None
        logger.info("Redis connection closed")
