"""
Conexión asíncrona a PostgreSQL / TimescaleDB con SQLAlchemy 2.x.

Decisiones técnicas:
- SQLAlchemy 2.x async: API moderna, totalmente compatible con asyncio.
- asyncpg como driver: el más rápido disponible para PostgreSQL en Python.
- Pool de conexiones configurado para entornos de alta concurrencia:
    pool_size=10 + max_overflow=20 → hasta 30 conexiones concurrentes.
    pool_recycle=3600 → evita conexiones stale en producción.
- Base declarativa compartida: todos los modelos de todos los servicios
  heredan de esta Base para mantener coherencia en migraciones.
- expire_on_commit=False en sesiones async → evita lazy-load implícito
  que causaría MissingGreenlet en contextos async.
"""

from collections.abc import AsyncGenerator
from typing import Optional

from loguru import logger
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base declarativa para todos los modelos SQLAlchemy de la plataforma."""
    pass


_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def create_db_engine(database_url: str) -> AsyncEngine:
    """
    Crea el motor async de SQLAlchemy.

    Llamar una vez en startup del servicio.

    Args:
        database_url: DSN asyncpg (postgresql+asyncpg://user:pass@host/db)
    """
    global _engine
    _engine = create_async_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=3600,
        pool_pre_ping=True,   # verifica conexión antes de usarla del pool
        echo=False,           # True para debug SQL (muy verboso)
    )
    logger.info("Database engine created")
    return _engine


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """
    Crea la fábrica de sesiones async.

    Args:
        engine: Motor async creado con create_db_engine().
    """
    global _session_factory
    _session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return _session_factory


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency injection para FastAPI: yields una sesión async.

    Uso en endpoint:
        async def my_endpoint(session: AsyncSession = Depends(get_session)):
            ...
    """
    if _session_factory is None:
        raise RuntimeError(
            "Session factory not initialised. Call create_session_factory() in lifespan."
        )
    async with _session_factory() as session:
        yield session


async def close_engine() -> None:
    """Cierra el engine y libera el pool. Llamar en shutdown del servicio."""
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None
        logger.info("Database engine closed")
