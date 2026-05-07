# indicator-engine

**FASE 2 — IMPLEMENTADO** ✅

Microservicio de cálculo de indicadores técnicos en tiempo real.

## Responsabilidades

1. Consume velas del stream `market:candles` (Redis Streams)
2. Mantiene buffers OHLCV en memoria por símbolo+timeframe (hasta 500 velas)
3. Calcula indicadores técnicos al recibir cada vela nueva
4. Publica snapshots completos en `indicators:values`
5. Expone endpoints HTTP para consulta bajo demanda

## Indicadores calculados

| Indicador | Parámetros | Notas |
|-----------|-----------|-------|
| EMA | 9, 21, 50, 200 | Exponential Moving Average |
| RSI | 14 | + señal: overbought/oversold/neutral |
| MACD | 12, 26, 9 | line + signal + histogram + cross detection |
| ATR | 14 | Average True Range de Wilder |
| Bollinger Bands | 20, 2σ | upper + middle + lower + position |

## Endpoints

| Endpoint | Descripción |
|----------|------------|
| `GET /health` | Health-check para Docker |
| `GET /status` | Estado: buffers, consumer, Redis |
| `GET /indicators/{symbol}/{tf}` | Snapshot de indicadores actual |
| `GET /buffers` | Lista de buffers activos |

## Flujo de datos

```
market:candles (Redis Stream)
    → CandleConsumer (consumer group: indicator-engine)
    → CandleBuffer (en memoria, deque maxlen=500)
    → IndicatorCalculator (numpy puro, stateless)
    → IndicatorPublisher
    → indicators:values (Redis Stream)
```

## Puerto

`8004` (configurable via `INDICATOR_ENGINE_PORT`)
