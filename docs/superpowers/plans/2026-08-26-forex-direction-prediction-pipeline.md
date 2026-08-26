# Pipeline offline de predicción direccional (EUR/USD H1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir un pipeline offline (backfill + features + labeling + entrenamiento + evaluación) que valide si un modelo XGBoost puede predecir la dirección de la siguiente vela H1 de EUR/USD con precisión útil, antes de invertir en un servicio de IA en vivo.

**Architecture:** Nuevo paquete `ai/pipeline/` con etapas independientes y testeables (CLI para backfill/train, funciones puras para features/labeling/evaluate). Reutiliza `backend/shared/` (config, database) y componentes ya existentes de los microservicios (`OandaStreamingClient`, `CandleRepository`, `SessionDetector`) importándolos directamente desde sus directorios de servicio, siguiendo el patrón ya usado en `backend/tests/`.

**Tech Stack:** Python 3.12, pandas, numpy, scikit-learn (`TimeSeriesSplit`), XGBoost, joblib, SQLAlchemy async + asyncpg, pytest.

## Global Constraints

- 100% código tipado (excepto `ai/`, que ya está excluido de `mypy --strict` en `pyproject.toml` — se tipa igualmente por buena práctica, pero no bloquea).
- AsyncIO para toda operación de I/O (DB, HTTP a OANDA).
- pytest con `asyncio_mode = auto` (ver `pytest.ini` / `pyproject.toml`); requiere `pytest-asyncio` instalado (`pip install -r backend/tests/requirements-test.txt`).
- Validación de series temporales exclusivamente walk-forward (nunca k-fold aleatorio) — ver `TimeSeriesSplit` en `sklearn.model_selection`.
- Ninguna feature puede usar información posterior a la vela `t` (anti look-ahead bias) — verificado con tests dedicados.
- Fuera de alcance (no incluir en ninguna tarea): servicio `ai-engine` en vivo, features inspiradas en los PDFs (Fibonacci/patrones de vela), múltiples pares/timeframes simultáneos, calibración de probabilidades, ensemble de modelos.
- Spec de referencia: `docs/superpowers/specs/2026-08-26-forex-direction-prediction-design.md`.

---

## Contexto encontrado durante la planificación

Al verificar que la suite de tests actual pasa (`python -m pytest backend/tests/ -v` desde la raíz), se encontró un bug real preexistente: `backend/tests/test_indicators.py` y `backend/tests/test_session_detector.py` insertan en `sys.path` el directorio de un microservicio distinto cada uno, pero **ambos microservicios exponen un paquete top-level llamado `app`**. Cuando pytest los recolecta juntos en el mismo proceso, Python cachea el primer `app` importado en `sys.modules["app"]` y el segundo test falla con `ModuleNotFoundError: No module named 'app.core.session_detector'`.

Esto bloquea directamente este plan: se van a añadir más test files que cruzan límites de servicio (`test_oanda_client.py`, `ai/tests/test_features.py`) y se va a ampliar `testpaths` para que `ai/tests/` se recolecte junto con `backend/tests/` en una sola ejecución de `pytest`. La Tarea 1 arregla esto antes de tocar nada más, para partir de una suite verde.

---

### Task 1: Arreglar colisión de módulo `app` entre tests de distintos microservicios

**Files:**
- Modify: `backend/tests/test_indicators.py:16-22`
- Modify: `backend/tests/test_session_detector.py:16-20`

**Interfaces:**
- Consumes: nada nuevo.
- Produces: nada nuevo — es un fix de aislamiento de imports, no cambia ninguna API pública.

- [ ] **Step 1: Confirmar el fallo actual**

Run: `python -m pytest backend/tests/ -v`
Expected: `ERROR collecting backend/tests/test_session_detector.py` con `ModuleNotFoundError: No module named 'app.core.session_detector'`.

- [ ] **Step 2: Purgar el caché de `app` antes de insertar cada ruta de servicio**

En `backend/tests/test_indicators.py`, reemplazar:

```python
# Path al servicio
_IND_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "indicator-engine"
sys.path.insert(0, str(_IND_ENGINE))

from app.core.indicators import atr, bollinger_bands, ema, macd, rsi
from app.core.buffer import BufferRegistry, CandleBuffer
from app.core.calculator import IndicatorCalculator
```

por:

```python
# Path al servicio.
# Nota: varios microservicios exponen un paquete top-level llamado `app`.
# Si un test anterior ya importó el `app` de otro servicio en este mismo
# proceso de pytest, hay que purgarlo de sys.modules antes de insertar esta
# ruta, o Python reutilizará por error el `app` equivocado.
_IND_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "indicator-engine"
for _cached_module in list(sys.modules):
    if _cached_module == "app" or _cached_module.startswith("app."):
        del sys.modules[_cached_module]
sys.path.insert(0, str(_IND_ENGINE))

from app.core.indicators import atr, bollinger_bands, ema, macd, rsi
from app.core.buffer import BufferRegistry, CandleBuffer
from app.core.calculator import IndicatorCalculator
```

En `backend/tests/test_session_detector.py`, reemplazar:

```python
# Añadir el directorio del servicio al path de importación
_SESSION_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "session-engine"
sys.path.insert(0, str(_SESSION_ENGINE))

from app.core.session_detector import ForexSession, SessionDetector, SessionInfo
```

por:

```python
# Añadir el directorio del servicio al path de importación.
# Nota: ver comentario equivalente en test_indicators.py sobre la colisión
# del paquete `app` entre microservicios.
_SESSION_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "session-engine"
for _cached_module in list(sys.modules):
    if _cached_module == "app" or _cached_module.startswith("app."):
        del sys.modules[_cached_module]
sys.path.insert(0, str(_SESSION_ENGINE))

from app.core.session_detector import ForexSession, SessionDetector, SessionInfo
```

- [ ] **Step 3: Verificar que ambos test files corren juntos sin colisión**

Run: `python -m pytest backend/tests/ -v`
Expected: ambos archivos se recolectan sin `ModuleNotFoundError` (pueden seguir fallando tests individuales por otras razones ajenas a este fix, p.ej. si falta `pytest-asyncio`; lo que debe desaparecer es el error de *colección*).

- [ ] **Step 4: Commit**

```bash
git add backend/tests/test_indicators.py backend/tests/test_session_detector.py
git commit -m "fix: avoid app module collision when running cross-service tests together"
```

---

### Task 2: Mover los indicadores técnicos a `backend/shared/indicators/`

**Files:**
- Create: `backend/shared/indicators/__init__.py`
- Create: `backend/shared/indicators/technical.py`
- Modify: `backend/services/indicator-engine/app/core/calculator.py:19`
- Delete: `backend/services/indicator-engine/app/core/indicators.py`
- Modify: `backend/tests/test_indicators.py`

**Interfaces:**
- Produces: `shared.indicators.ema(prices: np.ndarray, period: int) -> np.ndarray`, `shared.indicators.rsi(prices: np.ndarray, period: int = 14) -> np.ndarray`, `shared.indicators.macd(prices: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[np.ndarray, np.ndarray, np.ndarray]`, `shared.indicators.atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray`, `shared.indicators.bollinger_bands(prices: np.ndarray, period: int = 20, std_dev: float = 2.0) -> tuple[np.ndarray, np.ndarray, np.ndarray]`, `shared.indicators.vwap(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray) -> np.ndarray`. Task 5 (`ai/pipeline/features.py`) consume estas funciones.

- [ ] **Step 1: Mover el archivo con `git mv` para preservar historial**

```bash
mkdir -p backend/shared/indicators
git mv backend/services/indicator-engine/app/core/indicators.py backend/shared/indicators/technical.py
```

- [ ] **Step 2: Crear el `__init__.py` del nuevo módulo**

`backend/shared/indicators/__init__.py`:

```python
from .technical import atr, bollinger_bands, ema, macd, rsi, vwap

__all__ = ["atr", "bollinger_bands", "ema", "macd", "rsi", "vwap"]
```

- [ ] **Step 3: Actualizar el import en `calculator.py`**

En `backend/services/indicator-engine/app/core/calculator.py:19`, reemplazar:

```python
from .indicators import atr, bollinger_bands, ema, macd, rsi
```

por:

```python
from shared.indicators import atr, bollinger_bands, ema, macd, rsi
```

- [ ] **Step 4: Actualizar `test_indicators.py` para importar desde `shared.indicators`**

Reemplazar el bloque de imports (ya con el fix de la Tarea 1 aplicado):

```python
_IND_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "indicator-engine"
for _cached_module in list(sys.modules):
    if _cached_module == "app" or _cached_module.startswith("app."):
        del sys.modules[_cached_module]
sys.path.insert(0, str(_IND_ENGINE))

from app.core.indicators import atr, bollinger_bands, ema, macd, rsi
from app.core.buffer import BufferRegistry, CandleBuffer
from app.core.calculator import IndicatorCalculator
```

por:

```python
_BACKEND = Path(__file__).resolve().parents[2] / "backend"
sys.path.insert(0, str(_BACKEND))
from shared.indicators import atr, bollinger_bands, ema, macd, rsi

_IND_ENGINE = _BACKEND / "services" / "indicator-engine"
for _cached_module in list(sys.modules):
    if _cached_module == "app" or _cached_module.startswith("app."):
        del sys.modules[_cached_module]
sys.path.insert(0, str(_IND_ENGINE))

from app.core.buffer import BufferRegistry, CandleBuffer
from app.core.calculator import IndicatorCalculator
```

- [ ] **Step 5: Ejecutar la suite y confirmar que sigue en verde**

Run: `python -m pytest backend/tests/test_indicators.py -v`
Expected: mismos tests que antes (`TestEMA`, `TestRSI`, `TestMACD`, `TestATR`, `TestBollingerBands`, `TestVWAP`, `TestCandleBuffer`, `TestIndicatorCalculator`), todos en PASS.

- [ ] **Step 6: (Opcional, si mypy está disponible) Verificar tipado estricto del nuevo módulo compartido**

Run: `python -m mypy --strict backend/shared/indicators/`
Expected: sin errores (el archivo movido ya tenía anotaciones completas en todas las funciones).

- [ ] **Step 7: Commit**

```bash
git add backend/shared/indicators backend/services/indicator-engine/app/core/calculator.py backend/tests/test_indicators.py
git commit -m "refactor: move technical indicators to backend/shared for reuse by ai/pipeline"
```

---

### Task 3: Extender `fetch_candles` con paginación por `to_time`

**Files:**
- Modify: `backend/services/market-data-service/app/core/oanda_client.py:18` (imports) y `:161-199` (`fetch_candles`)
- Create: `backend/tests/test_oanda_client.py`

**Interfaces:**
- Consumes: nada nuevo.
- Produces: `OandaStreamingClient.fetch_candles(self, symbol: str, timeframe: str, count: int = 100, to_time: datetime | None = None) -> list[CandleData]`. Task 7 (`ai/pipeline/backfill.py`) consume esta firma extendida.

- [ ] **Step 1: Escribir el test que falla (parámetro `to_time` no existe todavía)**

`backend/tests/test_oanda_client.py`:

```python
"""
Tests unitarios de OandaStreamingClient.fetch_candles.

Todas las llamadas HTTP se mockean: estos tests no hacen red real.
Ejecutar: pytest backend/tests/test_oanda_client.py -v
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
sys.path.insert(0, str(_BACKEND))
from shared.config.settings import Settings

_MARKET_DATA_SERVICE = _BACKEND / "services" / "market-data-service"
for _cached_module in list(sys.modules):
    if _cached_module == "app" or _cached_module.startswith("app."):
        del sys.modules[_cached_module]
sys.path.insert(0, str(_MARKET_DATA_SERVICE))

from app.core.oanda_client import OandaStreamingClient


def _fake_response(candles: list[dict]) -> MagicMock:
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json = MagicMock(return_value={"candles": candles})
    return response


@pytest.fixture
def settings() -> Settings:
    return Settings(oanda_api_key="test-key", oanda_account_id="test-account", oanda_env="practice")


class TestFetchCandlesPagination:
    async def test_without_to_time_omits_to_param(self, settings, mocker):
        client = OandaStreamingClient(settings)
        mock_get = mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_fake_response([])))

        await client.fetch_candles("EUR_USD", "H1", count=10)

        _, kwargs = mock_get.call_args
        assert "to" not in kwargs["params"]

    async def test_with_to_time_adds_to_param_as_isoformat(self, settings, mocker):
        client = OandaStreamingClient(settings)
        mock_get = mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_fake_response([])))
        to_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

        await client.fetch_candles("EUR_USD", "H1", count=10, to_time=to_time)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["to"] == "2026-01-01T00:00:00+00:00"

    async def test_returns_parsed_candles_from_response(self, settings, mocker):
        client = OandaStreamingClient(settings)
        raw_candle = {
            "time": "2026-01-01T00:00:00.000000000Z",
            "complete": True,
            "volume": 120,
            "mid": {"o": "1.0800", "h": "1.0820", "l": "1.0790", "c": "1.0810"},
        }
        mocker.patch("httpx.AsyncClient.get", new=AsyncMock(return_value=_fake_response([raw_candle])))

        result = await client.fetch_candles("EUR_USD", "H1", count=1)

        assert len(result) == 1
        assert result[0].close == pytest.approx(1.0810)
```

- [ ] **Step 2: Ejecutar y confirmar el fallo**

Run: `python -m pytest backend/tests/test_oanda_client.py -v`
Expected: FAIL — `TypeError: fetch_candles() got an unexpected keyword argument 'to_time'` en los dos primeros tests (el tercero puede pasar ya, ya que no usa `to_time`).

- [ ] **Step 3: Implementar `to_time` en `fetch_candles`**

En `backend/services/market-data-service/app/core/oanda_client.py`, añadir el import de `datetime` junto a los imports existentes (línea 19, `import asyncio`):

```python
import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
```

Reemplazar el método `fetch_candles` completo (líneas 161-199) por:

```python
    async def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        count: int = 100,
        to_time: datetime | None = None,
    ) -> list[CandleData]:
        """
        Obtiene velas históricas desde OANDA REST API.

        Args:
            symbol:    Instrumento (e.g., "EUR_USD").
            timeframe: Granularidad OANDA (e.g., "M15", "H1").
            count:     Número de velas a obtener (máx. 5000 en OANDA).
            to_time:   Si se especifica, obtiene las `count` velas anteriores
                       a este instante (paginación hacia atrás para backfill
                       histórico). Si es None, obtiene las `count` velas más
                       recientes.

        Returns:
            Lista de CandleData ordenada por tiempo ascendente.
        """
        url = f"{self._settings.oanda_rest_url}/v3/instruments/{symbol}/candles"
        params: dict[str, str | int] = {
            "count": count,
            "granularity": timeframe,
            "price": "M",  # Midpoint (promedio bid/ask)
        }
        if to_time is not None:
            params["to"] = to_time.isoformat()

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
            f"Fetched candles  symbol={symbol}  timeframe={timeframe}  "
            f"count={len(candles)}  to_time={to_time}"
        )
        return candles
```

- [ ] **Step 4: Verificar que los tests pasan**

Run: `python -m pytest backend/tests/test_oanda_client.py -v`
Expected: PASS en los 3 tests.

- [ ] **Step 5: Añadir `pytest-mock` y `pytest-asyncio` si faltan, y confirmar la suite completa**

Run: `pip install -r backend/tests/requirements-test.txt && python -m pytest backend/tests/ -v`
Expected: todos los tests de `test_indicators.py`, `test_session_detector.py` y `test_oanda_client.py` en PASS (sin errores de colección).

- [ ] **Step 6: Commit**

```bash
git add backend/services/market-data-service/app/core/oanda_client.py backend/tests/test_oanda_client.py
git commit -m "feat: add to_time pagination to fetch_candles for historical backfill"
```

---

### Task 4: Crear el esqueleto del paquete `ai/pipeline/`

**Files:**
- Create: `ai/__init__.py`
- Create: `ai/pipeline/__init__.py`
- Create: `ai/pipeline/config.py`
- Create: `ai/pipeline/_service_imports.py`
- Create: `ai/pipeline/requirements.txt`
- Create: `ai/tests/__init__.py` (vacío, solo para que `ai` sea un paquete regular consistente con el resto del repo)
- Create: `ai/tests/conftest.py`
- Create: `ai/tests/test_service_imports.py`
- Modify: `pytest.ini`
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `ai.pipeline.config.get_pipeline_settings() -> PipelineSettings` (con `.datasets_dir: Path`, `.models_dir: Path`, `.symbol: str`, `.timeframe: str`, más todos los campos heredados de `shared.config.settings.Settings`). `ai.pipeline._service_imports.import_service_module(service_dir: Path, module_path: str) -> ModuleType`, y las constantes `HISTORICAL_DATA_SERVICE`, `MARKET_DATA_SERVICE`, `SESSION_ENGINE` (todas `Path`). Tareas 5-9 consumen estas dos funciones y las constantes.
- Consumes: `shared.config.settings.Settings` (Tarea previa, ya existente).

- [ ] **Step 1: Crear los `__init__.py` de paquete**

`ai/__init__.py`: archivo vacío.

`ai/pipeline/__init__.py`:

```python
"""
Pipeline offline de predicción direccional.

Ver docs/superpowers/specs/2026-08-26-forex-direction-prediction-design.md.

Añade backend/ a sys.path para poder importar `shared.*` (el paquete
compartido de la plataforma): backend/ no es un paquete Python instalable,
es el build context de los microservicios Docker, por lo que no se puede
importar como `backend.shared`.
"""

import sys
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parents[2] / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))
```

`ai/tests/__init__.py`: archivo vacío.

- [ ] **Step 2: Crear el helper de import cross-servicio**

`ai/pipeline/_service_imports.py`:

```python
"""
Helper para importar módulos `app.*` de los microservicios del monorepo
(backend/services/<nombre>/) desde scripts de ai/pipeline.

Cada microservicio expone un paquete top-level llamado `app`. Si dos
servicios distintos se importan en el mismo proceso, hay que purgar
`sys.modules["app"]` (y submódulos) antes de insertar la ruta del segundo
servicio, o Python reutilizará por error el `app` ya cacheado del primero
(mismo problema que se arregló en backend/tests/, ver Tarea 1 del plan).
"""

import importlib
import sys
from pathlib import Path
from types import ModuleType

_SERVICES_ROOT = Path(__file__).resolve().parents[2] / "backend" / "services"

HISTORICAL_DATA_SERVICE = _SERVICES_ROOT / "historical-data-service"
MARKET_DATA_SERVICE = _SERVICES_ROOT / "market-data-service"
SESSION_ENGINE = _SERVICES_ROOT / "session-engine"


def import_service_module(service_dir: Path, module_path: str) -> ModuleType:
    """
    Importa `module_path` (p.ej. "app.core.oanda_client") desde `service_dir`,
    purgando cualquier `app` cacheado de un servicio importado previamente.
    """
    for cached in list(sys.modules):
        if cached == "app" or cached.startswith("app."):
            del sys.modules[cached]

    service_dir_str = str(service_dir)
    if service_dir_str in sys.path:
        sys.path.remove(service_dir_str)
    sys.path.insert(0, service_dir_str)

    return importlib.import_module(module_path)
```

- [ ] **Step 3: Crear `config.py`**

`ai/pipeline/config.py`:

```python
"""
Configuración del pipeline de IA — extiende Settings compartido con paths
específicos de datasets y modelos entrenados.
"""

from pathlib import Path

from pydantic import Field

from shared.config.settings import Settings

_PIPELINE_ROOT = Path(__file__).resolve().parent


class PipelineSettings(Settings):
    """Settings del pipeline offline de IA. Hereda toda la config de la plataforma."""

    datasets_dir: Path = Field(default=_PIPELINE_ROOT / "datasets")
    models_dir: Path = Field(default=_PIPELINE_ROOT / "models")

    symbol: str = "EUR_USD"
    timeframe: str = "H1"


def get_pipeline_settings() -> PipelineSettings:
    """Devuelve PipelineSettings, creando los directorios de salida si faltan."""
    settings = PipelineSettings()
    settings.datasets_dir.mkdir(parents=True, exist_ok=True)
    settings.models_dir.mkdir(parents=True, exist_ok=True)
    return settings
```

- [ ] **Step 4: Crear `requirements.txt` del pipeline**

`ai/pipeline/requirements.txt`:

```
pandas>=2.2.0
numpy>=1.26.0
scikit-learn>=1.4.0
xgboost>=2.0.0
joblib>=1.3.0
pyarrow>=15.0.0
sqlalchemy[asyncio]>=2.0.29
asyncpg>=0.29.0
httpx>=0.27.0
pydantic>=2.7.0
pydantic-settings>=2.2.0
loguru>=0.7.2
```

- [ ] **Step 5: Instalar dependencias**

Run: `pip install -r ai/pipeline/requirements.txt`
Expected: instalación exitosa (pandas/numpy probablemente ya estén satisfechos por otras dependencias del entorno).

- [ ] **Step 6: Crear `conftest.py` de `ai/tests/`**

`ai/tests/conftest.py`:

```python
"""
Configuración de sys.path para los tests del paquete ai/pipeline.

Necesario porque los tests importan `ai.pipeline.*`, lo que requiere la raíz
del repositorio en sys.path (pytest no la añade automáticamente cuando el
directorio de tests no tiene un ancestro con __init__.py hasta la raíz).
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
```

- [ ] **Step 7: Escribir el test de regresión de la colisión `app` (falla porque `ai.pipeline` no existe aún)**

`ai/tests/test_service_imports.py`:

```python
"""
Test de regresión: importar dos microservicios que comparten el nombre de
paquete `app` (session-engine y market-data-service) en el mismo proceso de
pytest no debe colisionar. Ver Tarea 1 del plan de implementación para el
bug original que motivó este helper.
"""

from ai.pipeline._service_imports import (
    MARKET_DATA_SERVICE,
    SESSION_ENGINE,
    import_service_module,
)


def test_importing_two_services_in_sequence_does_not_collide():
    session_module = import_service_module(SESSION_ENGINE, "app.core.session_detector")
    assert hasattr(session_module, "SessionDetector")

    oanda_module = import_service_module(MARKET_DATA_SERVICE, "app.core.oanda_client")
    assert hasattr(oanda_module, "OandaStreamingClient")

    session_module_again = import_service_module(SESSION_ENGINE, "app.core.session_detector")
    assert hasattr(session_module_again, "SessionDetector")
```

- [ ] **Step 8: Ampliar `testpaths` para incluir `ai/tests/`**

En `pytest.ini`, cambiar:

```ini
testpaths = backend/tests
```

por:

```ini
testpaths = backend/tests ai/tests
```

En `pyproject.toml`, cambiar:

```toml
testpaths = ["backend/tests"]
```

por:

```toml
testpaths = ["backend/tests", "ai/tests"]
```

- [ ] **Step 9: Ejecutar y confirmar que el test de regresión pasa**

Run: `python -m pytest ai/tests/test_service_imports.py -v`
Expected: PASS.

Run: `python -m pytest -v` (desde la raíz, recolecta `backend/tests` y `ai/tests` juntos)
Expected: ninguna colisión de `app` entre ningún par de archivos, en ningún orden de recolección.

- [ ] **Step 10: Commit**

```bash
git add ai/__init__.py ai/pipeline ai/tests pytest.ini pyproject.toml
git commit -m "feat: scaffold ai/pipeline package with cross-service import helper"
```

---

### Task 5: Feature engineering (`features.py`)

**Files:**
- Create: `ai/pipeline/features.py`
- Create: `ai/tests/test_features.py`

**Interfaces:**
- Consumes: `shared.indicators.{ema,rsi,macd,atr,bollinger_bands}` (Tarea 2). `SessionDetector` de `backend/services/session-engine/app/core/session_detector.py` (vía `import_service_module`, Tarea 4) — expone `.get_current_session(dt: datetime) -> SessionInfo` con `SessionInfo.session: ForexSession` (enum `str`) y `.volatility: str`.
- Produces: `build_features(candles: pd.DataFrame, session_detector) -> pd.DataFrame` (función pura, sin I/O) y `async load_candles(session: AsyncSession, candle_model, symbol: str, timeframe: str) -> pd.DataFrame`. Ambas consumidas por Tarea 8 (`train.py`). Columnas de salida de `build_features`: `time, close, high, low, ema_9, ema_21, ema_9_minus_21, rsi_14, macd_line, macd_signal, macd_hist, atr_14, bb_upper, bb_middle, bb_lower, bb_width, return_1, return_4, high_low_range, session, session_volatility`.

- [ ] **Step 1: Escribir el test que falla (el módulo no existe aún)**

`ai/tests/test_features.py`:

```python
"""
Tests de features.py — verifica ausencia de look-ahead bias.

Cada feature de la fila t debe depender solo de datos hasta t (inclusive).
Se verifica calculando las features sobre el historial completo y de nuevo
sobre un historial truncado justo después de la fila t: el valor de la fila
t debe ser idéntico en ambos casos.

Ejecutar: pytest ai/tests/test_features.py -v
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SESSION_ENGINE = Path(__file__).resolve().parents[2] / "backend" / "services" / "session-engine"
for _cached_module in list(sys.modules):
    if _cached_module == "app" or _cached_module.startswith("app."):
        del sys.modules[_cached_module]
sys.path.insert(0, str(_SESSION_ENGINE))

from app.core.session_detector import SessionDetector

from ai.pipeline.features import build_features

_FEATURE_COLUMNS = [
    "ema_9", "ema_21", "rsi_14", "macd_line", "macd_signal", "macd_hist",
    "atr_14", "bb_upper", "bb_middle", "bb_lower", "return_1", "return_4",
]


def _make_candles(n: int) -> pd.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    times = [start + timedelta(hours=i) for i in range(n)]
    closes = np.linspace(1.0800, 1.0800 + 0.0005 * n, n)
    return pd.DataFrame(
        {
            "time": times,
            "open": closes - 0.0001,
            "high": closes + 0.0005,
            "low": closes - 0.0005,
            "close": closes,
            "volume": [1000] * n,
        }
    )


class TestNoLookAheadBias:
    def test_row_values_unaffected_by_future_candles(self):
        candles = _make_candles(60)
        detector = SessionDetector()

        full = build_features(candles, detector)
        truncated = build_features(candles.iloc[:40].reset_index(drop=True), detector)

        row_full = full.iloc[39]
        row_truncated = truncated.iloc[39]

        for column in _FEATURE_COLUMNS:
            full_value = row_full[column]
            truncated_value = row_truncated[column]
            if pd.isna(full_value) and pd.isna(truncated_value):
                continue
            assert full_value == pytest.approx(truncated_value), f"{column} leaked future data"


class TestSessionFeatures:
    def test_session_column_matches_hour(self):
        candles = _make_candles(24)
        detector = SessionDetector()
        features = build_features(candles, detector)

        assert features.iloc[10]["session"] == "london"

    def test_session_volatility_is_valid_value(self):
        candles = _make_candles(24)
        detector = SessionDetector()
        features = build_features(candles, detector)
        assert set(features["session_volatility"].unique()) <= {"low", "medium", "high"}


class TestOutputShape:
    def test_output_has_same_row_count_as_input(self):
        candles = _make_candles(50)
        detector = SessionDetector()
        features = build_features(candles, detector)
        assert len(features) == len(candles)
```

- [ ] **Step 2: Ejecutar y confirmar el fallo**

Run: `python -m pytest ai/tests/test_features.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ai.pipeline.features'`.

- [ ] **Step 3: Implementar `features.py`**

`ai/pipeline/features.py`:

```python
"""
Feature engineering para el pipeline de predicción direccional.

`build_features` es una función pura (sin I/O) sobre un DataFrame de velas ya
cargado, para que sea trivial de testear sin base de datos. `load_candles` es
la única función con I/O de este módulo (lee de TimescaleDB).

Ninguna feature de la fila `t` usa información posterior a `t` — verificado
en ai/tests/test_features.py.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from shared.indicators import atr, bollinger_bands, ema, macd, rsi


async def load_candles(
    session: AsyncSession, candle_model: Any, symbol: str, timeframe: str
) -> pd.DataFrame:
    """
    Carga todas las velas completas de `symbol`/`timeframe` desde TimescaleDB,
    ordenadas ascendentemente por tiempo.

    Args:
        session: sesión async de SQLAlchemy ya abierta.
        candle_model: la clase `Candle` de historical-data-service (se pasa
            como parámetro para no acoplar este módulo a la ruta de import
            del servicio — ver ai/pipeline/train.py).
        symbol: instrumento OANDA (e.g. "EUR_USD").
        timeframe: granularidad OANDA (e.g. "H1").
    """
    result = await session.execute(
        select(candle_model)
        .where(candle_model.symbol == symbol)
        .where(candle_model.timeframe == timeframe)
        .where(candle_model.complete.is_(True))
        .order_by(candle_model.time.asc())
    )
    rows = result.scalars().all()
    return pd.DataFrame(
        {
            "time": [r.time for r in rows],
            "open": [r.open for r in rows],
            "high": [r.high for r in rows],
            "low": [r.low for r in rows],
            "close": [r.close for r in rows],
            "volume": [r.volume for r in rows],
        }
    )


def build_features(candles: pd.DataFrame, session_detector: Any) -> pd.DataFrame:
    """
    Construye el dataset de features a partir de velas OHLCV ordenadas
    ascendentemente.

    Args:
        candles: DataFrame con columnas ["time", "open", "high", "low",
            "close", "volume"], ordenado por `time` ascendente.
        session_detector: instancia de SessionDetector (session-engine).

    Returns:
        DataFrame con la misma cantidad de filas que `candles`, con columnas
        OHLC originales más las features. Las primeras filas (insuficiente
        historial para los indicadores de mayor período) tienen NaN.
    """
    close = candles["close"].to_numpy(dtype=np.float64)
    high = candles["high"].to_numpy(dtype=np.float64)
    low = candles["low"].to_numpy(dtype=np.float64)

    ema_9 = ema(close, 9)
    ema_21 = ema(close, 21)
    rsi_14 = rsi(close, 14)
    macd_line, macd_signal, macd_hist = macd(close)
    atr_14 = atr(high, low, close, 14)
    bb_upper, bb_middle, bb_lower = bollinger_bands(close)

    features = pd.DataFrame(
        {
            "time": candles["time"].to_numpy(),
            "close": close,
            "high": high,
            "low": low,
            "ema_9": ema_9,
            "ema_21": ema_21,
            "ema_9_minus_21": ema_9 - ema_21,
            "rsi_14": rsi_14,
            "macd_line": macd_line,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
            "atr_14": atr_14,
            "bb_upper": bb_upper,
            "bb_middle": bb_middle,
            "bb_lower": bb_lower,
            "bb_width": bb_upper - bb_lower,
            "return_1": pd.Series(close).pct_change(1).to_numpy(),
            "return_4": pd.Series(close).pct_change(4).to_numpy(),
            "high_low_range": high - low,
        }
    )

    timestamps = pd.to_datetime(candles["time"])
    sessions = [session_detector.get_current_session(ts.to_pydatetime()) for ts in timestamps]
    features["session"] = [s.session.value for s in sessions]
    features["session_volatility"] = [s.volatility for s in sessions]

    return features
```

- [ ] **Step 4: Ejecutar y confirmar que los tests pasan**

Run: `python -m pytest ai/tests/test_features.py -v`
Expected: PASS en los 4 tests.

- [ ] **Step 5: Commit**

```bash
git add ai/pipeline/features.py ai/tests/test_features.py
git commit -m "feat: add feature engineering module for the direction prediction pipeline"
```

---

### Task 6: Labeling (`labeling.py`)

**Files:**
- Create: `ai/pipeline/labeling.py`
- Create: `ai/tests/test_labeling.py`

**Interfaces:**
- Consumes: nada (función pura sobre un DataFrame con columnas `close` y `atr_14`, formato producido por Tarea 5).
- Produces: `label_direction(df: pd.DataFrame, atr_multiplier: float = 0.5) -> pd.Series` (valores `"LONG"`, `"SHORT"`, `"NEUTRAL"` o `None`). Consumida por Tarea 8 (`train.py`).

- [ ] **Step 1: Escribir el test que falla**

`ai/tests/test_labeling.py`:

```python
"""
Tests de labeling.py — verifica la banda NEUTRAL basada en ATR y el manejo
de bordes (última fila sin vela siguiente, ATR faltante).

Ejecutar: pytest ai/tests/test_labeling.py -v
"""

import numpy as np
import pandas as pd

from ai.pipeline.labeling import label_direction


def test_long_when_move_exceeds_atr_band():
    df = pd.DataFrame({"close": [1.0000, 1.0010], "atr_14": [0.0010, 0.0010]})
    labels = label_direction(df, atr_multiplier=0.5)
    assert labels.iloc[0] == "LONG"


def test_short_when_move_exceeds_negative_atr_band():
    df = pd.DataFrame({"close": [1.0000, 0.9990], "atr_14": [0.0010, 0.0010]})
    labels = label_direction(df, atr_multiplier=0.5)
    assert labels.iloc[0] == "SHORT"


def test_neutral_when_move_within_atr_band():
    df = pd.DataFrame({"close": [1.0000, 1.0002], "atr_14": [0.0010, 0.0010]})
    labels = label_direction(df, atr_multiplier=0.5)
    assert labels.iloc[0] == "NEUTRAL"


def test_last_row_has_no_label():
    df = pd.DataFrame({"close": [1.0000, 1.0010, 1.0020], "atr_14": [0.0010, 0.0010, 0.0010]})
    labels = label_direction(df, atr_multiplier=0.5)
    assert labels.iloc[-1] is None


def test_row_with_missing_atr_has_no_label():
    df = pd.DataFrame({"close": [1.0000, 1.0010], "atr_14": [np.nan, 0.0010]})
    labels = label_direction(df, atr_multiplier=0.5)
    assert labels.iloc[0] is None
```

- [ ] **Step 2: Ejecutar y confirmar el fallo**

Run: `python -m pytest ai/tests/test_labeling.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ai.pipeline.labeling'`.

- [ ] **Step 3: Implementar `labeling.py`**

`ai/pipeline/labeling.py`:

```python
"""
Genera el target de dirección para el pipeline de predicción.

LONG:    close(t+1) - close(t) >  atr_multiplier * atr_14(t)
SHORT:   close(t+1) - close(t) < -atr_multiplier * atr_14(t)
NEUTRAL: en cualquier otro caso (movimiento dentro del ruido esperado).

La banda NEUTRAL se dimensiona como un múltiplo del ATR de la vela t para no
etiquetar como señal movimientos menores a la volatilidad típica del par.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def label_direction(df: pd.DataFrame, atr_multiplier: float = 0.5) -> pd.Series:
    """
    Etiqueta cada fila con la dirección de la vela siguiente.

    Args:
        df: DataFrame con columnas "close" y "atr_14" (salida de
            ai.pipeline.features.build_features).
        atr_multiplier: ancho de la banda NEUTRAL como múltiplo del ATR.

    Returns:
        Series de valores "LONG" / "SHORT" / "NEUTRAL" / None. Es None cuando
        no hay vela siguiente (última fila) o cuando falta el ATR de la fila.
    """
    next_close = df["close"].shift(-1)
    delta = next_close - df["close"]
    threshold = atr_multiplier * df["atr_14"]

    label = pd.Series(
        np.where(delta > threshold, "LONG", np.where(delta < -threshold, "SHORT", "NEUTRAL")),
        index=df.index,
        dtype="object",
    )
    missing = next_close.isna() | df["atr_14"].isna()
    label[missing] = None
    return label
```

- [ ] **Step 4: Ejecutar y confirmar que los tests pasan**

Run: `python -m pytest ai/tests/test_labeling.py -v`
Expected: PASS en los 5 tests.

- [ ] **Step 5: Commit**

```bash
git add ai/pipeline/labeling.py ai/tests/test_labeling.py
git commit -m "feat: add ATR-based direction labeling for the prediction pipeline"
```

---

### Task 7: Backfill (`backfill.py`)

**Files:**
- Create: `ai/pipeline/backfill.py`

**Interfaces:**
- Consumes: `OandaStreamingClient.fetch_candles(symbol, timeframe, count, to_time)` (Tarea 3), `import_service_module` + `HISTORICAL_DATA_SERVICE` + `MARKET_DATA_SERVICE` (Tarea 4), `get_pipeline_settings()` (Tarea 4), `CandleRepository.upsert_candle(...)` (ya existente en `backend/services/historical-data-service/app/repositories/candle_repo.py`).
- Produces: `async backfill(symbol: str, timeframe: str, years: int) -> int` (número de velas insertadas) y el CLI `python -m ai.pipeline.backfill`.

**Nota sobre testing:** este módulo es I/O puro contra una API externa real (OANDA) y una base de datos — igual que `OandaStreamingClient.stream_prices`, que tampoco tiene test unitario en el proyecto (solo se testean sus componentes puros). Se verifica con una ejecución manual acotada en vez de un test automatizado, siguiendo el mismo patrón ya establecido en el repo.

- [ ] **Step 1: Implementar `backfill.py`**

`ai/pipeline/backfill.py`:

```python
"""
CLI de backfill — descarga histórico de velas OANDA y lo persiste en
TimescaleDB, paginando hacia atrás en el tiempo.

Uso:
    python -m ai.pipeline.backfill --symbol EUR_USD --timeframe H1 --years 5

OANDA limita cada request a 5000 velas. Este script pide bloques de 5000
usando el parámetro `to_time` de fetch_candles, y usa el timestamp de la
vela más antigua de cada bloque como cursor para el siguiente request, hasta
cubrir `years` años o hasta que OANDA deje de devolver velas nuevas (fin del
histórico disponible para ese instrumento).
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from loguru import logger

from ai.pipeline._service_imports import (
    HISTORICAL_DATA_SERVICE,
    MARKET_DATA_SERVICE,
    import_service_module,
)
from ai.pipeline.config import get_pipeline_settings


async def backfill(symbol: str, timeframe: str, years: int) -> int:
    """Descarga y persiste histórico OHLCV. Devuelve el total de velas insertadas."""
    settings = get_pipeline_settings()

    oanda_module = import_service_module(MARKET_DATA_SERVICE, "app.core.oanda_client")
    OandaStreamingClient = oanda_module.OandaStreamingClient

    candle_repo_module = import_service_module(
        HISTORICAL_DATA_SERVICE, "app.repositories.candle_repo"
    )
    CandleRepository = candle_repo_module.CandleRepository

    from shared.database.connection import create_db_engine, create_session_factory

    engine = create_db_engine(settings.database_url)
    session_factory = create_session_factory(engine)

    client = OandaStreamingClient(settings)
    cutoff = datetime.now(timezone.utc) - timedelta(days=365 * years)
    cursor: datetime | None = None
    total_inserted = 0

    async with session_factory() as session:
        repo = CandleRepository(session)

        while True:
            candles = await client.fetch_candles(symbol, timeframe, count=5000, to_time=cursor)
            if not candles:
                logger.info("OANDA no devolvió más velas — fin del histórico disponible")
                break

            for candle in candles:
                if candle.timestamp < cutoff:
                    continue
                await repo.upsert_candle(
                    symbol=candle.symbol,
                    timeframe=candle.timeframe,
                    timestamp=candle.timestamp,
                    open_=candle.open,
                    high=candle.high,
                    low=candle.low,
                    close=candle.close,
                    volume=candle.volume,
                    complete=candle.complete,
                    source=candle.source,
                )
                total_inserted += 1

            oldest = candles[0].timestamp
            logger.info(
                f"Backfill  symbol={symbol}  hasta={oldest.isoformat()}  "
                f"insertadas_acumuladas={total_inserted}"
            )

            if oldest <= cutoff:
                break
            if cursor is not None and oldest >= cursor:
                logger.warning("OANDA no avanzó el cursor — deteniendo backfill")
                break
            cursor = oldest

    await engine.dispose()
    return total_inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill de histórico OANDA hacia TimescaleDB")
    parser.add_argument("--symbol", default="EUR_USD")
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--years", type=int, default=5)
    args = parser.parse_args()

    total = asyncio.run(backfill(args.symbol, args.timeframe, args.years))
    logger.info(f"Backfill completo — {total} velas insertadas")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verificación manual acotada (requiere Postgres/TimescaleDB corriendo y credenciales OANDA válidas en `.env`)**

Run: `docker compose up -d postgres` (si no está corriendo ya), luego:

```bash
python -m ai.pipeline.backfill --symbol EUR_USD --timeframe H1 --years 0
```

(`--years 0` limita el cutoff a "ahora", por lo que solo debe insertar el primer bloque de hasta 5000 velas — una forma rápida de verificar que la conexión a OANDA, la paginación y el insert en TimescaleDB funcionan sin esperar un backfill completo de varios años.)

Expected: logs de progreso sin excepciones, y `total_inserted > 0`. Verificar con:

```sql
SELECT symbol, timeframe, count(*), min(time), max(time) FROM candles GROUP BY symbol, timeframe;
```

- [ ] **Step 3: Commit**

```bash
git add ai/pipeline/backfill.py
git commit -m "feat: add OANDA historical backfill CLI"
```

---

### Task 8: Entrenamiento con walk-forward validation (`train.py`)

**Files:**
- Create: `ai/pipeline/train.py`
- Create: `ai/tests/test_train.py`

**Interfaces:**
- Consumes: `build_features`, `load_candles` (Tarea 5), `label_direction` (Tarea 6), `import_service_module` + `HISTORICAL_DATA_SERVICE` + `SESSION_ENGINE` (Tarea 4), `get_pipeline_settings()` (Tarea 4).
- Produces: `walk_forward_splits(n_samples: int, n_splits: int = 5)` (generador de tuplas `(train_idx: np.ndarray, val_idx: np.ndarray)`) y `train_and_validate(dataset: pd.DataFrame, n_splits: int = 5) -> dict` con claves `fold_metrics: list[dict]`, `model: xgb.XGBClassifier`, `feature_columns: list[str]`. Ambas consumidas por Tarea 9 (`evaluate.py`) y por el CLI de este mismo módulo.

- [ ] **Step 1: Escribir el test que falla (el módulo no existe aún)**

`ai/tests/test_train.py`:

```python
"""
Tests de train.py — verifica que el walk-forward split nunca mezcla
temporalmente entrenamiento y validación.

Ejecutar: pytest ai/tests/test_train.py -v
"""

from ai.pipeline.train import walk_forward_splits


class TestWalkForwardSplit:
    def test_validation_always_after_training(self):
        for train_idx, val_idx in walk_forward_splits(n_samples=100, n_splits=5):
            assert train_idx.max() < val_idx.min()

    def test_no_overlap_between_train_and_validation(self):
        for train_idx, val_idx in walk_forward_splits(n_samples=100, n_splits=5):
            assert set(train_idx.tolist()).isdisjoint(set(val_idx.tolist()))

    def test_produces_requested_number_of_splits(self):
        splits = list(walk_forward_splits(n_samples=100, n_splits=4))
        assert len(splits) == 4
```

- [ ] **Step 2: Ejecutar y confirmar el fallo**

Run: `python -m pytest ai/tests/test_train.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ai.pipeline.train'`.

- [ ] **Step 3: Implementar `train.py`**

`ai/pipeline/train.py`:

```python
"""
Entrena un clasificador XGBoost (LONG/SHORT/NEUTRAL) sobre el dataset de
features + labels, usando walk-forward validation (nunca k-fold aleatorio,
para no mezclar temporalmente pasado y futuro).

Uso:
    python -m ai.pipeline.train --symbol EUR_USD --timeframe H1
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from loguru import logger
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import TimeSeriesSplit

from ai.pipeline._service_imports import (
    HISTORICAL_DATA_SERVICE,
    SESSION_ENGINE,
    import_service_module,
)
from ai.pipeline.config import get_pipeline_settings
from ai.pipeline.features import build_features, load_candles
from ai.pipeline.labeling import label_direction

FEATURE_COLUMNS = [
    "ema_9", "ema_21", "ema_9_minus_21", "rsi_14",
    "macd_line", "macd_signal", "macd_hist",
    "atr_14", "bb_upper", "bb_middle", "bb_lower", "bb_width",
    "return_1", "return_4", "high_low_range",
]
CATEGORICAL_COLUMNS = ["session", "session_volatility"]
LABEL_ORDER = ["SHORT", "NEUTRAL", "LONG"]


def walk_forward_splits(n_samples: int, n_splits: int = 5) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """
    Genera índices (train_idx, val_idx) de validación walk-forward usando
    TimeSeriesSplit: cada fold entrena con el pasado y valida con el bloque
    de tiempo inmediatamente siguiente. Nunca mezcla temporalmente train y
    validación (verificado en ai/tests/test_train.py).
    """
    splitter = TimeSeriesSplit(n_splits=n_splits)
    yield from splitter.split(np.arange(n_samples))


def _prepare_xy(dataset: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    x = pd.get_dummies(dataset[FEATURE_COLUMNS + CATEGORICAL_COLUMNS], columns=CATEGORICAL_COLUMNS)
    y = dataset["label"].map(LABEL_ORDER.index)
    return x, y


def _new_classifier() -> xgb.XGBClassifier:
    return xgb.XGBClassifier(
        objective="multi:softprob",
        num_class=len(LABEL_ORDER),
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        eval_metric="mlogloss",
    )


def train_and_validate(dataset: pd.DataFrame, n_splits: int = 5) -> dict:
    """
    Entrena con walk-forward validation y devuelve métricas por fold más el
    modelo final (entrenado sobre todo el dataset disponible).
    """
    x, y = _prepare_xy(dataset)
    fold_metrics = []

    for train_idx, val_idx in walk_forward_splits(len(dataset), n_splits):
        model = _new_classifier()
        model.fit(x.iloc[train_idx], y.iloc[train_idx])
        preds = model.predict(x.iloc[val_idx])

        fold_metrics.append(
            {
                "accuracy": float(accuracy_score(y.iloc[val_idx], preds)),
                "f1_macro": float(f1_score(y.iloc[val_idx], preds, average="macro")),
                "val_start": str(dataset.iloc[val_idx[0]]["time"]),
                "val_end": str(dataset.iloc[val_idx[-1]]["time"]),
            }
        )

    final_model = _new_classifier()
    final_model.fit(x, y)

    return {"fold_metrics": fold_metrics, "model": final_model, "feature_columns": list(x.columns)}


async def load_dataset(symbol: str, timeframe: str) -> pd.DataFrame:
    """Carga velas desde TimescaleDB y construye el dataset de features + labels."""
    timeseries_module = import_service_module(HISTORICAL_DATA_SERVICE, "app.models.timeseries")
    candle_model = timeseries_module.Candle

    session_detector_module = import_service_module(SESSION_ENGINE, "app.core.session_detector")
    session_detector = session_detector_module.SessionDetector()

    settings = get_pipeline_settings()
    from shared.database.connection import create_db_engine, create_session_factory

    engine = create_db_engine(settings.database_url)
    session_factory = create_session_factory(engine)

    async with session_factory() as session:
        candles = await load_candles(session, candle_model, symbol, timeframe)
    await engine.dispose()

    features = build_features(candles, session_detector)
    features["label"] = label_direction(features)
    return features.dropna(subset=FEATURE_COLUMNS + ["label"]).reset_index(drop=True)


def save_model(result: dict, symbol: str, timeframe: str) -> Path:
    settings = get_pipeline_settings()
    trained_at = datetime.now(timezone.utc).isoformat()
    model_path = settings.models_dir / f"{symbol}_{timeframe}_xgboost.joblib"
    metadata_path = settings.models_dir / f"{symbol}_{timeframe}_xgboost.json"

    joblib.dump(result["model"], model_path)
    metadata_path.write_text(
        json.dumps(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "trained_at": trained_at,
                "feature_columns": result["feature_columns"],
                "label_order": LABEL_ORDER,
                "fold_metrics": result["fold_metrics"],
            },
            indent=2,
        )
    )
    return model_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Entrena el modelo XGBoost de predicción direccional")
    parser.add_argument("--symbol", default="EUR_USD")
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--n-splits", type=int, default=5)
    args = parser.parse_args()

    dataset = asyncio.run(load_dataset(args.symbol, args.timeframe))
    logger.info(f"Dataset cargado — {len(dataset)} filas utilizables")

    result = train_and_validate(dataset, args.n_splits)
    for fold in result["fold_metrics"]:
        logger.info(
            f"Fold  {fold['val_start']} -> {fold['val_end']}  "
            f"accuracy={fold['accuracy']:.3f}  f1_macro={fold['f1_macro']:.3f}"
        )

    model_path = save_model(result, args.symbol, args.timeframe)
    logger.info(f"Modelo guardado en {model_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Ejecutar y confirmar que los tests pasan**

Run: `python -m pytest ai/tests/test_train.py -v`
Expected: PASS en los 3 tests.

- [ ] **Step 5: Commit**

```bash
git add ai/pipeline/train.py ai/tests/test_train.py
git commit -m "feat: add XGBoost training with walk-forward validation"
```

---

### Task 9: Evaluación — baseline y backtest simple (`evaluate.py`)

**Files:**
- Create: `ai/pipeline/evaluate.py`
- Create: `ai/tests/test_evaluate.py`
- Modify: `ai/README.md`

**Interfaces:**
- Consumes: nada nuevo (funciones puras sobre DataFrames/Series con el formato producido por Tareas 5-6).
- Produces: `naive_baseline_predictions(dataset: pd.DataFrame) -> pd.Series`, `majority_class_baseline(labels: pd.Series) -> str`, `simple_backtest(dataset: pd.DataFrame, predictions: pd.Series, sl_pips: float = 10.0, tp_pips: float = 10.0, pip_size: float = 0.0001) -> dict` (claves `n_trades`, `win_rate`, `total_pips`, `sharpe_ratio`).

- [ ] **Step 1: Escribir el test que falla**

`ai/tests/test_evaluate.py`:

```python
"""
Tests de evaluate.py — baseline ingenuo y backtest simple con SL/TP fijos.

Ejecutar: pytest ai/tests/test_evaluate.py -v
"""

import pandas as pd

from ai.pipeline.evaluate import (
    majority_class_baseline,
    naive_baseline_predictions,
    simple_backtest,
)


class TestNaiveBaseline:
    def test_predicts_long_after_up_candle(self):
        dataset = pd.DataFrame({"close": [1.0, 1.1, 1.05]})
        preds = naive_baseline_predictions(dataset)
        assert preds.iloc[1] == "LONG"
        assert preds.iloc[2] == "SHORT"

    def test_majority_class_picks_most_frequent(self):
        labels = pd.Series(["LONG", "LONG", "SHORT", "NEUTRAL"])
        assert majority_class_baseline(labels) == "LONG"


class TestSimpleBacktest:
    def test_long_prediction_hits_take_profit(self):
        dataset = pd.DataFrame(
            {"close": [1.0000, 1.0000], "high": [1.0000, 1.0015], "low": [1.0000, 0.9995]}
        )
        predictions = pd.Series(["LONG", "NEUTRAL"])
        result = simple_backtest(dataset, predictions, sl_pips=10, tp_pips=10, pip_size=0.0001)
        assert result["n_trades"] == 1
        assert result["total_pips"] == 10.0

    def test_long_prediction_hits_stop_loss(self):
        dataset = pd.DataFrame(
            {"close": [1.0000, 1.0000], "high": [1.0000, 1.0005], "low": [1.0000, 0.9985]}
        )
        predictions = pd.Series(["LONG", "NEUTRAL"])
        result = simple_backtest(dataset, predictions, sl_pips=10, tp_pips=10, pip_size=0.0001)
        assert result["n_trades"] == 1
        assert result["total_pips"] == -10.0

    def test_neutral_predictions_generate_no_trades(self):
        dataset = pd.DataFrame({"close": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0]})
        predictions = pd.Series(["NEUTRAL", "NEUTRAL"])
        result = simple_backtest(dataset, predictions)
        assert result["n_trades"] == 0
        assert result["sharpe_ratio"] == 0.0
```

- [ ] **Step 2: Ejecutar y confirmar el fallo**

Run: `python -m pytest ai/tests/test_evaluate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ai.pipeline.evaluate'`.

- [ ] **Step 3: Implementar `evaluate.py`**

`ai/pipeline/evaluate.py`:

```python
"""
Evaluación del modelo: baseline ingenuo + backtest simple de negocio.

No repite el entrenamiento walk-forward (eso ya lo hace train.py y reporta
accuracy/F1 por fold) — se enfoca en comparar contra un baseline y en
traducir las predicciones a un backtest simple con SL/TP fijos, para saber
si la señal es explotable en la práctica y no solo si "acierta"
estadísticamente.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def naive_baseline_predictions(dataset: pd.DataFrame) -> pd.Series:
    """
    Baseline ingenuo: predice que la vela siguiente repite la dirección de
    la vela anterior (momentum naive). Sirve para saber si el modelo aporta
    señal real por encima de "seguir la última vela".
    """
    previous_return = dataset["close"].diff()
    return pd.Series(
        np.where(previous_return > 0, "LONG", np.where(previous_return < 0, "SHORT", "NEUTRAL")),
        index=dataset.index,
    )


def majority_class_baseline(labels: pd.Series) -> str:
    """Baseline ingenuo: la clase más frecuente del propio dataset."""
    return str(labels.value_counts().idxmax())


def simple_backtest(
    dataset: pd.DataFrame,
    predictions: pd.Series,
    sl_pips: float = 10.0,
    tp_pips: float = 10.0,
    pip_size: float = 0.0001,
) -> dict:
    """
    Simula operar según `predictions` con SL/TP fijos y simétricos (mismo
    esquema que describe el PDF del curso de Fernando en su ejemplo
    introductorio de "Probabilidades exactas y falsas": TP/SL simétricos
    como comparación base, aplicado aquí de forma cuantificada).

    Para cada fila con predicción LONG/SHORT (las NEUTRAL no generan
    operación), determina si la vela siguiente hubiera tocado el TP o el SL
    primero usando su high/low.

    Returns:
        dict con: n_trades, win_rate, total_pips, sharpe_ratio.
    """
    sl = sl_pips * pip_size
    tp = tp_pips * pip_size

    next_high = dataset["high"].shift(-1)
    next_low = dataset["low"].shift(-1)
    entry = dataset["close"]

    pnl_per_trade: list[float] = []
    for idx in dataset.index:
        pred = predictions.loc[idx]
        if pred not in ("LONG", "SHORT"):
            continue
        if pd.isna(next_high.loc[idx]) or pd.isna(next_low.loc[idx]):
            continue

        if pred == "LONG":
            hit_tp = next_high.loc[idx] >= entry.loc[idx] + tp
            hit_sl = next_low.loc[idx] <= entry.loc[idx] - sl
        else:
            hit_tp = next_low.loc[idx] <= entry.loc[idx] - tp
            hit_sl = next_high.loc[idx] >= entry.loc[idx] + sl

        if hit_tp and not hit_sl:
            pnl_per_trade.append(tp_pips)
        elif hit_sl and not hit_tp:
            pnl_per_trade.append(-sl_pips)
        elif hit_tp and hit_sl:
            pnl_per_trade.append(-sl_pips)  # ambos tocados en la misma vela: asume el peor caso
        else:
            pnl_per_trade.append(0.0)

    if not pnl_per_trade:
        return {"n_trades": 0, "win_rate": 0.0, "total_pips": 0.0, "sharpe_ratio": 0.0}

    pnl = np.array(pnl_per_trade)
    sharpe = float(pnl.mean() / pnl.std()) if pnl.std() > 0 else 0.0

    return {
        "n_trades": len(pnl),
        "win_rate": float((pnl > 0).mean()),
        "total_pips": float(pnl.sum()),
        "sharpe_ratio": sharpe,
    }
```

- [ ] **Step 4: Ejecutar y confirmar que los tests pasan**

Run: `python -m pytest ai/tests/test_evaluate.py -v`
Expected: PASS en los 5 tests.

- [ ] **Step 5: Ejecutar toda la suite del repo**

Run: `python -m pytest -v`
Expected: todos los tests de `backend/tests/` y `ai/tests/` en PASS, sin errores de colección.

- [ ] **Step 6: Documentar el uso end-to-end en `ai/README.md`**

Añadir al final de `ai/README.md`:

```markdown
## Cómo correr el pipeline offline (EUR/USD H1)

1. `pip install -r ai/pipeline/requirements.txt`
2. Backfill de histórico: `python -m ai.pipeline.backfill --symbol EUR_USD --timeframe H1 --years 5`
3. Entrenamiento + walk-forward validation: `python -m ai.pipeline.train --symbol EUR_USD --timeframe H1`
4. El modelo y sus métricas por fold quedan en `ai/pipeline/models/` (`.joblib` + `.json`, no versionados en git).
5. Comparar contra baseline y correr el backtest simple usando `ai.pipeline.evaluate` sobre el dataset devuelto por `ai.pipeline.train.load_dataset` (ver `docs/superpowers/specs/2026-08-26-forex-direction-prediction-design.md` para la interpretación de resultados).
```

- [ ] **Step 7: Commit**

```bash
git add ai/pipeline/evaluate.py ai/tests/test_evaluate.py ai/README.md
git commit -m "feat: add naive baselines and simple backtest for model evaluation"
```

---

## Self-review

**Cobertura del spec:**
- Backfill OANDA paginado → Tareas 3 y 7.
- Features con indicadores estándar + sesión, sin look-ahead → Tarea 5.
- Labeling LONG/SHORT/NEUTRAL con banda ATR → Tarea 6.
- Entrenamiento XGBoost con walk-forward validation → Tarea 8.
- Evaluación: accuracy/F1 walk-forward (Tarea 8) + baseline ingenuo + backtest Sharpe (Tarea 9).
- Mover indicadores a `shared/` (decisión de diseño del spec) → Tarea 2.
- Sin servicio en vivo, sin features de los PDFs, sin multi-par/timeframe, sin calibración/ensemble → ninguna tarea las incluye.

**Placeholders:** ninguno — cada paso tiene código completo, sin TODOs.

**Consistencia de tipos/firmas:** `fetch_candles(..., to_time: datetime | None = None)` (Tarea 3) es exactamente lo que consume `backfill()` (Tarea 7). `build_features`/`load_candles` (Tarea 5) coinciden con lo que usa `load_dataset` en `train.py` (Tarea 8). `label_direction` (Tarea 6) coincide con la columna `"label"` que `train.py` añade al dataset. `walk_forward_splits` y `train_and_validate` (Tarea 8) son las únicas dependencias de `evaluate.py`/tests de la Tarea 9, y las firmas coinciden.

**Alcance:** una sola pieza cohesiva (pipeline offline de un par/timeframe); no requiere descomponerse en sub-proyectos adicionales.
