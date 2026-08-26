# Diseño — Pipeline offline de predicción direccional (EUR/USD H1)

**Fecha**: 2026-08-26
**Estado**: Aprobado, pendiente de implementación
**Fase del roadmap**: adelanto acotado de FASE 3 / FASE 6 (AI Engine) — ver `README.md` y `ai/README.md`

## Objetivo

La plataforma debe ayudar a predecir si el mercado Forex va a subir o bajar, con la mayor
precisión posible, en tiempo real. Este documento cubre el **primer paso** hacia ese objetivo:
un pipeline **offline** que valide si es posible predecir la dirección de la siguiente vela H1
de EUR/USD con una precisión útil, antes de invertir en construir un servicio en vivo.

No es realista ni el objetivo de este documento construir el sistema completo de una vez. Se
empieza acotado (1 par, 1 timeframe, features estándar, sin servicio en vivo) y se escala en
iteraciones futuras, cada una con su propio spec.

## Contexto y decisiones previas

### Los 3 PDFs de `docs/`

Se revisó el contenido de los tres PDFs pensados como "base de entrenamiento":

- **`Curso Fernando 2020 - Actualizacion 23 Junio.pdf`**: curso personal de un trader
  ("Fernando Martínez Gómez-Tejedor") con terminología propia no estándar (PHI/Fibonacci,
  "holones", SYS/ORD, hedging manual, "estrategia 666", money management). Es un manual de
  reglas **discrecionales**, no un dataset ni un paper con evidencia estadística.
- **`SIMPLE TRADING Book v1.pdf`** y **`Simple Trading Book v2.pdf`**: libros de patrones
  clásicos de análisis técnico (Double Top/Bottom, Triple Top/Bottom, etc.), mayormente
  contenido escaneado/en imágenes, no texto extraíble.

**Decisión**: estos PDFs no se pueden usar para "entrenar" un modelo directamente (no son
series numéricas etiquetadas). Sirven como **inspiración de features** cuantificables a futuro
(niveles Fibonacci, soporte-resistencia fractal, patrones de vela). Para esta primera iteración
se decidió **no** incluir esas features todavía (ver "Fuera de alcance").

> Nota aparte (no bloqueante para este spec): los 3 PDFs están en `docs/` pero no están
> trackeados en git (`git status` los muestra como untracked). `Curso Fernando` se declara
> gratuito por su autor, pero los libros "Simple Trading" parecen contenido editorial de
> terceros — antes de subirlos al repositorio (especialmente si el remoto es público) conviene
> confirmar que no hay problema de derechos de autor. Se dejan fuera de este commit.

### Alcance decidido con el usuario

| Decisión | Elegido |
|---|---|
| Granularidad de predicción | Por vela cerrada (no por tick), escalando complejidad después |
| Par | EUR/USD únicamente |
| Timeframe | H1 |
| Features v1 | Solo indicadores técnicos estándar + contexto de sesión (sin Fibonacci/patrones de los PDFs todavía) |
| Fuente de histórico | API de velas de OANDA (cuenta practice, gratuita), paginada |
| Métrica de éxito | Accuracy/F1 direccional (walk-forward) **y** Sharpe de un backtest simple, ambas desde el inicio |
| Alcance de este proyecto | Solo pipeline offline (backfill + features + train + evaluate). Sin servicio `ai-engine` en vivo todavía |
| Estructura de código | Paquete modular `ai/pipeline/` (tipado, testeado), no scripts sueltos ni notebooks |

Esto es consistente con el roadmap ya esbozado en `ai/README.md` (XGBoost → calibración →
ensemble → Transformers), que ya planeaba clasificación LONG/SHORT/NEUTRAL con walk-forward
validation, y con la filosofía del proyecto: "la IA es un filtro, no reemplaza las reglas".

## Arquitectura

```
ai/
├── README.md                # roadmap general de IA (existente, se referencia este spec)
├── pipeline/
│   ├── __init__.py
│   ├── config.py             # paths de datasets/modelos, reutiliza Settings compartido
│   ├── backfill.py           # CLI: descarga histórico OANDA → TimescaleDB
│   ├── features.py           # construye dataset de features desde TimescaleDB
│   ├── labeling.py           # genera el target (dirección de la vela siguiente)
│   ├── train.py              # CLI: entrena XGBoost con walk-forward validation
│   ├── evaluate.py           # métricas + backtest simple
│   ├── datasets/             # parquet cacheados localmente (gitignored)
│   └── models/               # modelos entrenados (.joblib) + metadata (gitignored)
└── tests/
    ├── test_features.py
    ├── test_labeling.py
    └── test_evaluate.py
```

Flujo de datos:

```
OANDA (histórico) → backfill.py → TimescaleDB (candles)
                                        │
                                        ▼
                                   features.py ──► DataFrame (features + timestamp)
                                        │
                                        ▼
                                  labeling.py ──► + columna target (LONG/SHORT/NEUTRAL)
                                        │
                                        ▼
                                   train.py ──► modelo XGBoost (.joblib) + metadata
                                        │
                                        ▼
                                 evaluate.py ──► accuracy/F1 (walk-forward) + backtest (Sharpe)
```

### Decisión de diseño: lógica de indicadores compartida

Actualmente `backend/services/indicator-engine/app/core/indicators.py` contiene funciones puras
de indicadores (EMA, RSI, MACD, ATR, Bollinger, VWAP) usadas solo por `indicator-engine`.
`ai/pipeline/features.py` necesita exactamente los mismos cálculos para no duplicar lógica ni
arriesgar inconsistencias entre lo que ve el modelo en entrenamiento y lo que vería en producción.

**Decisión**: mover estas funciones a `backend/shared/indicators/` (nuevo módulo del paquete
compartido, junto a `config/`, `database/`, `redis/`, `schemas/`, `utils/`). `indicator-engine`
pasa a importar desde `shared.indicators` en lugar de su copia local, y `ai/pipeline/features.py`
hace lo mismo. Esto sigue el patrón ya documentado en el propio proyecto ("Shared Package: evita
duplicación de modelos entre servicios — contrato único, cambios en un solo lugar").

## Componentes

### 1. Backfill (`backfill.py`)

- Extiende `OandaStreamingClient.fetch_candles` (`backend/services/market-data-service/app/core/oanda_client.py`)
  para aceptar paginación por rango de fechas (`from`/`to`), dado que OANDA limita cada
  request a ~5000 velas.
- Pagina hacia atrás en el tiempo hasta cubrir el histórico disponible de EUR/USD H1
  (objetivo: varios años; el límite real lo determina la profundidad de historial que OANDA
  ofrezca para ese par/granularidad).
- Reutiliza `CandleRepository` de `historical-data-service` para insertar en TimescaleDB con
  `ON CONFLICT DO NOTHING` (idempotente, seguro de re-ejecutar).
- Es un CLI de un solo uso, no un servicio persistente:
  `python -m ai.pipeline.backfill --symbol EUR_USD --timeframe H1 --years 5`

### 2. Feature engineering (`features.py`)

- Lee todas las velas H1 completas de EUR/USD desde TimescaleDB.
- Calcula indicadores vía `shared.indicators` (EMA, RSI, MACD, ATR, Bollinger).
- Añade contexto de sesión Forex (Asian/London/NY/overlap) reutilizando
  `session-engine/app/core/session_detector.py`.
- Añade features simples de retorno/volatilidad: retornos de N velas previas, rango
  high-low, distancia a EMA, etc.
- Restricción dura: **ninguna feature de la fila `t` puede usar información posterior a
  `t`** (anti look-ahead bias). Esto se verifica explícitamente en tests.

### 3. Labeling (`labeling.py`)

- Target de 3 clases: `LONG` / `SHORT` / `NEUTRAL`, comparando el cierre de la vela `t+1`
  contra el cierre de la vela `t`.
- Banda `NEUTRAL` dimensionada como un múltiplo del ATR de la vela `t` (evita etiquetar
  como señal movimientos de ruido menores al spread/volatilidad típica del par).
- El multiplicador del ATR queda como parámetro configurable; se ajusta empíricamente en la
  fase de evaluación, documentando el valor elegido y por qué.

### 4. Entrenamiento (`train.py`)

- Clasificador XGBoost multiclase (LONG/SHORT/NEUTRAL), como especifica `ai/README.md`.
- **Walk-forward validation**: nunca k-fold aleatorio (rompería la temporalidad). Se entrena
  sobre una ventana de tiempo y se valida en la ventana inmediatamente siguiente, repitiendo
  con varias particiones deslizantes.
- Persiste el modelo (`.joblib`) y su metadata (rango de fechas de entrenamiento, features
  usadas, hiperparámetros) en `ai/pipeline/models/`.

### 5. Evaluación (`evaluate.py`)

- **Métrica de modelo**: accuracy direccional y F1 por clase, calculadas solo sobre los folds
  de validación walk-forward (nunca sobre datos vistos en entrenamiento).
- **Baseline de comparación**: se reporta junto al modelo un baseline ingenuo (ej. "predecir
  siempre la clase mayoritaria" o "el mercado continúa la última vela") — sin esto no se puede
  saber si el modelo realmente aporta señal.
- **Backtest de negocio**: simulación simple que entra según la predicción con SL/TP fijos y
  simétricos (mismo esquema de ratio que describe el PDF de Fernando en su ejemplo introductorio,
  aplicado aquí de forma cuantificada), midiendo Sharpe ratio y retorno acumulado. Esto valida si
  la señal es explotable en la práctica, no solo si "acierta" estadísticamente.

### 6. Testing

- `test_features.py`: datos sintéticos pequeños, verifica ausencia de look-ahead bias y
  cálculo correcto de features derivadas de sesión.
- `test_labeling.py`: verifica el cálculo de la banda NEUTRAL basada en ATR con casos límite.
- `test_evaluate.py`: verifica que el split walk-forward nunca mezcla temporalmente
  train/test (ninguna fecha de validación es anterior a una fecha de entrenamiento del mismo
  fold).

## Fuera de alcance (explícitamente, para iteraciones futuras)

- Servicio `ai-engine` en vivo (API/Redis) sirviendo predicciones en tiempo real — se construye
  **después** de validar que el modelo supera el baseline de forma consistente.
- Features inspiradas en los PDFs (Fibonacci/PHI, soporte-resistencia fractal, patrones de
  vela tipo doble techo/suelo) — se evalúan en una v2 una vez exista el baseline con
  indicadores estándar.
- Múltiples pares o timeframes simultáneos.
- Predicción por tick o multi-horizonte.
- Calibración de probabilidades (Platt/isotonic) y ensemble XGBoost+LightGBM — siguientes
  pasos ya descritos en `ai/README.md`, una vez el modelo base esté validado.

## Riesgos y consideraciones

- **Profundidad de histórico de OANDA**: si el backfill no alcanza suficiente profundidad para
  EUR/USD H1, se evaluará complementar con un dataset externo (Dukascopy, HistData.com) — no
  bloquea el diseño, es una decisión a tomar durante el backfill si aplica.
- **Look-ahead bias**: es el error más común y silencioso en estos pipelines. Se mitiga con
  walk-forward validation estricta y tests dedicados, no solo con buenas intenciones en el código.
- **Derechos de autor de los PDFs**: ver nota en la sección de contexto — pendiente de decisión
  del usuario antes de trackearlos en git.
