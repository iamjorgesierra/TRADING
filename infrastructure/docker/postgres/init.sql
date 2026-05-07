-- =============================================================================
-- Quant Trading Platform — Inicialización de base de datos
-- PostgreSQL 15 + TimescaleDB
-- =============================================================================
-- Este script se ejecuta automáticamente al crear el contenedor PostgreSQL.
-- Crea la extensión TimescaleDB, las tablas y las hypertables.
-- =============================================================================

-- Habilitar TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =============================================================================
-- TABLA: ticks
-- Tick data en tiempo real (bid/ask/spread) por instrumento.
-- Hypertable particionada por día → chunks de 1 día.
-- =============================================================================
CREATE TABLE IF NOT EXISTS ticks (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    bid             DOUBLE PRECISION NOT NULL,
    ask             DOUBLE PRECISION NOT NULL,
    spread          DOUBLE PRECISION NOT NULL,
    source          VARCHAR(50)     NOT NULL DEFAULT 'oanda',

    -- Clave primaria compuesta garantiza idempotencia en inserciones
    PRIMARY KEY (time, symbol)
);

-- Convertir en hypertable particionada por tiempo
-- chunk_time_interval = 1 día → buena granularidad para tick data
SELECT create_hypertable(
    'ticks',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- Índice para queries por símbolo + tiempo descendente (más recientes primero)
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time
    ON ticks (symbol, time DESC);


-- =============================================================================
-- TABLA: candles
-- Velas OHLCV por instrumento y timeframe.
-- Hypertable particionada por semana.
-- =============================================================================
CREATE TABLE IF NOT EXISTS candles (
    time            TIMESTAMPTZ     NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    timeframe       VARCHAR(10)     NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          INTEGER         NOT NULL DEFAULT 0,
    complete        BOOLEAN         NOT NULL DEFAULT TRUE,
    source          VARCHAR(50)     NOT NULL DEFAULT 'oanda',

    PRIMARY KEY (time, symbol, timeframe)
);

SELECT create_hypertable(
    'candles',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '7 days'
);

CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_time
    ON candles (symbol, timeframe, time DESC);


-- =============================================================================
-- TABLA: session_events
-- Historial de cambios de sesión Forex (para contexto en backtesting).
-- =============================================================================
CREATE TABLE IF NOT EXISTS session_events (
    time                TIMESTAMPTZ     NOT NULL,
    session_type        VARCHAR(50)     NOT NULL,
    event_type          VARCHAR(50)     NOT NULL,
    expected_volatility VARCHAR(20),
    description         VARCHAR(200),

    PRIMARY KEY (time, session_type)
);

SELECT create_hypertable(
    'session_events',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '30 days'
);


-- =============================================================================
-- Políticas de retención de datos (TimescaleDB compression)
-- Activar compresión para ahorrar espacio en chunks históricos.
-- Los chunks más recientes se mantienen sin comprimir para performance.
-- =============================================================================

-- Comprimir chunks de ticks > 7 días
ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol'
);
SELECT add_compression_policy('ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- Comprimir chunks de candles > 30 días
ALTER TABLE candles SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, timeframe'
);
SELECT add_compression_policy('candles', INTERVAL '30 days', if_not_exists => TRUE);

-- =============================================================================
-- Log de inicialización
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Trading Platform DB initialized — TimescaleDB hypertables created';
END
$$;
