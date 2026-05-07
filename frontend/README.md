# frontend — Dashboard Quant Trading

> **FASE 4** — Pendiente de implementación.

## Stack planificado

- **Next.js 14** (App Router)
- **React 18** + TypeScript
- **TradingView Lightweight Charts** — charts OHLCV en tiempo real
- **TailwindCSS** — estilado utility-first
- **SWR / React Query** — data fetching + cache
- **WebSockets** — streaming de ticks y señales en tiempo real

## Secciones del dashboard

1. **Market Overview** — precios en tiempo real, sesión activa, spreads
2. **Charts** — velas OHLCV con indicadores técnicos overlay
3. **Signals** — señales generadas por rule-engine + scoring
4. **Risk Monitor** — exposición, drawdown, alertas
5. **Backtesting** — resultados históricos, métricas

## Decisión técnica: Next.js sobre SPA pura

- SSR para carga inicial rápida del dashboard
- API Routes para proxy al backend (evita CORS desde browser)
- App Router para layouts compartidos entre secciones
- WebSocket desde browser → gateway → Redis Streams (fan-out)
