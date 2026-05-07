"""
Logging profesional centralizado con Loguru.

Decisiones técnicas:
- Loguru sobre stdlib logging: API más simple, async-safe, soporte nativo
  de colores, serialización JSON y trace/diagnose en excepciones.
- Formato estructurado con service name, module, function y line.
- ContextVar para request_id permite trazar requests en async sin locks.
- En producción se puede añadir un sink adicional (archivo, Loki, Datadog)
  sin cambiar el código de los servicios.
"""

import sys
import uuid
from contextvars import ContextVar
from typing import Any

from loguru import logger

# ContextVar que propaga request_id en coroutines sin necesidad de pasar
# el valor manualmente por toda la call stack.
request_id_var: ContextVar[str] = ContextVar("request_id", default="")


def get_request_id() -> str:
    """Devuelve el request_id actual o genera uno efímero."""
    rid = request_id_var.get()
    return rid if rid else str(uuid.uuid4())[:8]


def setup_logging(service_name: str, log_level: str = "INFO") -> None:
    """
    Inicializa Loguru para el servicio dado.

    - Elimina el handler por defecto de Loguru.
    - Añade un handler a stderr con formato estructurado y colores.
    - Activa backtrace y diagnose para debugging de excepciones.

    Args:
        service_name: Nombre del microservicio (aparece en cada línea de log).
        log_level: Nivel mínimo de log (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    logger.remove()

    fmt = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        f"<magenta>{service_name}</magenta> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "{message}"
    )

    logger.add(
        sys.stderr,
        format=fmt,
        level=log_level.upper(),
        colorize=True,
        backtrace=True,
        diagnose=True,
        enqueue=True,   # async-safe: delega el I/O a un hilo separado
    )

    logger.info(f"Logging initialised  service={service_name}  level={log_level}")


def get_logger(name: str) -> Any:
    """
    Devuelve un logger con contexto de nombre.
    Uso: log = get_logger(__name__)
    """
    return logger.bind(module=name)
