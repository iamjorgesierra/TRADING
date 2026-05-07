# Arquitectura Técnica — FASE 1

## Visión General

La plataforma está construida como un sistema de microservicios orientado a eventos,
diseñado para ingestión, normalización y almacenamiento de datos Forex en tiempo real.

```
┌─────────────────────────────────────────────────────────────────┐
│                     QUANT TRADING PLATFORM                      │
│                          FASE 1                                  │
│                                                                   │
│  ┌──────────────────┐          ┌─────────────────────────────┐   │
│  │   OANDA API      │          │   Redis 7                   │   │
│  │  (SSE/NDJSON)    │          │                             │   │
│  └────────┬─────────┘          │  Streams:                   │   │
│           │                    │  ● market:ticks             │   │
│           ▼                    │  ● market:candles           │   │
│  ┌──────────────────┐  XADD   │  ● sessions:events          │   │
│  │market-data-svc   │────────▶│  ● system:events            │   │
│  │ :8001            │          │                             │   │
│  └──────────────────┘          └──────────┬──────────────────┘   │
│                                           │                       │
│  ┌──────────────────┐  XREADGROUP        │                       │
│  │historical-data   │◀───────────────────┤                       │
│  │ :8002            │                    │                       │
│  └────────┬─────────┘                   │                       │
│           │ INSERT                      │ XREADGROUP             │
│           ▼                             │                        │
│  ┌──────────────────┐          ┌────────▼─────────────────────┐  │
│  │  TimescaleDB     │          │  session-engine :8003        │  │
│  │  (PostgreSQL 15) │          │  Publica eventos de sesión   │  │
│  │  ● ticks         │          └──────────────────────────────┘  │
│  │  ● candles       │                                             │
│  │  ● session_events│                                             │
│  └──────────────────┘                                             │
└─────────────────────────────────────────────────────────────────┘
```

## Microservicios

### market-data-service (puerto 8001)

**Responsabilidad única**: Conectar a OANDA y publicar ticks.

Flujo:
1. Al arrancar, establece conexión SSE con OANDA Pricing Stream.
2. Por cada mensaje `PRICE`, crea un `TickData` normalizado.
3. Serializa y publica en `market:ticks` via `StreamProducer`.
4. Reconexión automática con backoff exponencial (1s → 60s).

Decisión — OANDA sobre otros proveedores:
- API gratuita (cuenta demo), bien documentada, streaming NDJSON.
- Soporta todos los pares Forex principales y cross.
- En FASE 2 se puede añadir soporte para Twelve Data o Dukascopy.

Endpoints:
- `GET /health`         → health-check
- `GET /status`         → estado streaming + Redis
- `GET /candles/{sym}`  → velas históricas OANDA

### historical-data-service (puerto 8002)

**Responsabilidad única**: Consumir ticks y persistir en TimescaleDB.

Flujo:
1. StreamConsumer (consumer group "historical-data-service") lee de `market:ticks`.
2. Por cada mensaje, llama a `TickRepository.upsert_tick()`.
3. `ON CONFLICT DO NOTHING` garantiza idempotencia.
4. ACK solo tras inserción exitosa → at-least-once delivery.

Decisión — TimescaleDB sobre InfluxDB:
- Extensión de PostgreSQL → SQL estándar, sin nuevo DSL.
- Hypertables particionadas por tiempo → queries ORDER BY time muy eficientes.
- Compresión nativa para datos históricos (80% menos espacio).
- Compatible con todas las herramientas del ecosistema PostgreSQL.

Endpoints:
- `GET /health`               → health-check
- `GET /ticks/{symbol}`        → últimos N ticks
- `GET /candles/{symbol}`      → últimas N velas

### session-engine (puerto 8003)

**Responsabilidad única**: Detectar sesión Forex activa y publicar eventos.

Flujo:
1. Al arrancar, publica sesión actual como evento `session_start`.
2. Cada 60 segundos comprueba si la sesión cambió.
3. Si cambió → publica `session_change` en `sessions:events`.

Sesiones implementadas:
- Asian          00:00 – 08:00 UTC  (baja volatilidad)
- London         08:00 – 13:00 UTC  (alta volatilidad)
- London/NY      13:00 – 17:00 UTC  (máxima volatilidad)
- New York       17:00 – 22:00 UTC  (media volatilidad)
- Off-hours      22:00 – 00:00 UTC  (mínima liquidez)

Endpoints:
- `GET /health`               → health-check
- `GET /session/current`       → sesión activa ahora
- `GET /session/all`           → todas las sesiones
- `GET /session/pair/{symbol}` → si el par está activo

## Módulo Shared

Paquete Python compartido entre todos los microservicios.

```
backend/shared/
├── config/settings.py     ← Pydantic Settings v2 + lru_cache Singleton
├── logging/logger.py      ← Loguru centralizado + async-safe
├── redis/
│   ├── client.py          ← Singleton connection pool
│   └── streams.py         ← StreamProducer + StreamConsumer
├── database/connection.py ← SQLAlchemy async engine + session factory
├── schemas/
│   ├── market.py          ← TickData, CandleData (modelos canónicos)
│   └── events.py          ← SessionEvent, SystemEvent
└── utils/time_utils.py    ← Helpers UTC, floor_to_minute, timeframe→seconds
```

**Por qué un shared package**:
- Evita duplicación de modelos entre servicios.
- Contrato único de datos → cambios en un solo lugar.
- Montado como volume en desarrollo, copiado en imagen en producción.

## Base de Datos — TimescaleDB

### Hypertables

| Tabla          | Partición   | Chunk Size | Compresión |
|---------------|------------|-----------|-----------|
| ticks         | Por día     | 1 día      | > 7 días  |
| candles       | Por semana  | 7 días     | > 30 días |
| session_events| Por mes     | 30 días    | —         |

### Índices críticos

```sql
idx_ticks_symbol_time        ON ticks (symbol, time DESC)
idx_candles_symbol_tf_time   ON candles (symbol, timeframe, time DESC)
```

## Redis Streams — Arquitectura de Eventos

| Stream              | Productor            | Consumidor(es)              |
|--------------------|---------------------|----------------------------|
| market:ticks        | market-data-service  | historical-data-service     |
| market:candles      | — (FASE 2)           | historical-data-service     |
| sessions:events     | session-engine       | strategy-engine (FASE 2)    |
| system:events       | todos los servicios  | monitoring (FASE 2)         |

**At-least-once delivery**:
- XREADGROUP con grupo → cada mensaje va a exactamente un consumer del grupo.
- XACK solo tras procesamiento exitoso → reintento automático si falla.
- En FASE 2: XCLAIM para reclamar mensajes pendientes (PEL).

## Decisiones Técnicas Principales

| Decisión | Alternativa descartada | Razón |
|---------|----------------------|-------|
| Redis Streams | Kafka | Simpler, sin broker extra, suficiente para FASE 1 |
| TimescaleDB | InfluxDB | SQL estándar, ecosistema PostgreSQL, hypertables |
| OANDA API | Twelve Data | Gratuito (demo), streaming NDJSON, pares Forex completos |
| asyncpg | psycopg3 | Mayor performance en benchmarks Python/PostgreSQL |
| Loguru | structlog | API más simple, async-safe enqueue, mejor DX |
| Pydantic v2 | Marshmallow | Performance 10x, integración nativa FastAPI |
| httpx | aiohttp | API moderna, sync + async, mejor para streaming SSE |

## Observabilidad

### FASE 1 (actual)
- Logs estructurados con Loguru en cada servicio.
- Endpoints `/health` y `/status` en cada servicio.
- Healthchecks en Docker Compose (postgres, redis).

### FASE 2 (planificado)
- Prometheus metrics + Grafana dashboards.
- Distributed tracing con OpenTelemetry.
- Alertas de latencia y errores.

## Seguridad

- Credenciales exclusivamente en `.env` (nunca en código).
- Redis con contraseña obligatoria.
- PostgreSQL con usuario de mínimos privilegios.
- PYTHONPATH controlado en Docker (no root install).
- `--no-install-recommends` en apt → imagen mínima.

## Evolución Prevista

```
FASE 1 (actual) → datos + infraestructura
FASE 2          → indicadores + Smart Money + backtesting
FASE 3          → AI Engine + ensemble models
FASE 4          → paper trading + risk automation
FASE 5          → ejecución automática
```
