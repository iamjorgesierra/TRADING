"""
Jerarquía de excepciones del dominio.

Decisión técnica:
- Tener excepciones propias del dominio permite diferenciar errores
  de infraestructura (DB caída, Redis timeout) de errores de negocio
  (símbolo inválido, sesión desconocida).
- Los servicios capturan PlatformException y la convierten en HTTP 4xx/5xx.
- Las excepciones de infraestructura se propagan y son capturadas por
  el middleware global de FastAPI.
"""


class PlatformException(Exception):
    """Excepción base de la plataforma. Todas las excepciones de dominio heredan de aquí."""

    def __init__(self, message: str, code: str = "PLATFORM_ERROR") -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(code={self.code!r}, message={self.message!r})"


class DataValidationError(PlatformException):
    """El dato recibido no cumple el schema esperado."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="DATA_VALIDATION_ERROR")


class SymbolNotFoundError(PlatformException):
    """El instrumento solicitado no existe o no está soportado."""

    def __init__(self, symbol: str) -> None:
        super().__init__(f"Symbol not found or not supported: {symbol}", code="SYMBOL_NOT_FOUND")


class ExternalAPIError(PlatformException):
    """Error en comunicación con API externa (OANDA, etc.)."""

    def __init__(self, provider: str, detail: str) -> None:
        super().__init__(
            f"External API error from {provider}: {detail}",
            code="EXTERNAL_API_ERROR",
        )


class StreamPublishError(PlatformException):
    """Fallo al publicar un evento en Redis Streams."""

    def __init__(self, stream: str, detail: str) -> None:
        super().__init__(
            f"Failed to publish to stream {stream!r}: {detail}",
            code="STREAM_PUBLISH_ERROR",
        )


class ConfigurationError(PlatformException):
    """Error en configuración del sistema (credenciales faltantes, parámetros inválidos)."""

    def __init__(self, detail: str) -> None:
        super().__init__(f"Configuration error: {detail}", code="CONFIGURATION_ERROR")
