# ai — AI Engine

> **FASE 6** — Pendiente de implementación.

Primer paso concreto en marcha: pipeline offline de predicción direccional para EUR/USD H1.
Diseño completo en
[`docs/superpowers/specs/2026-08-26-forex-direction-prediction-design.md`](../docs/superpowers/specs/2026-08-26-forex-direction-prediction-design.md).

## Roadmap de IA (en orden)

### Paso 1: Feature Engineering
- Construir datasets desde TimescaleDB
- Features: indicadores técnicos, sesión, SMC, order flow
- Ventanas temporales: 1h, 4h, D

### Paso 2: Clasificación XGBoost
- Target: dirección (LONG / SHORT / NEUTRAL)
- Features seleccionadas por importancia
- Walk-forward validation para evitar look-ahead bias
- Métricas: accuracy, F1, Sharpe ratio de señales

### Paso 3: Calibración de probabilidades
- Platt scaling o isotonic regression
- Output: probabilidad [0, 1] no label binario
- Threshold dinámico según régimen de mercado

### Paso 4: Ensemble LightGBM + XGBoost
- Stacking con meta-learner simple (logistic regression)
- Confidence score = promedio ponderado de modelos

### Paso 5: Transformers (futuro)
- Temporal Fusion Transformer para series temporales
- Atención sobre contexto multi-timeframe

## Filosofía de IA del proyecto

La IA NO reemplaza las reglas.
La IA es un FILTRO adicional sobre señales ya validadas por:
  1. Reglas cuantitativas
  2. Contexto de sesión
  3. Smart Money Concepts
  4. Gestión de riesgo

El ensemble final combina:
  rule_score * 0.5 + smc_score * 0.3 + ai_score * 0.2

(pesos ajustables por backtesting)
