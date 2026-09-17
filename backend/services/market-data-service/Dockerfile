# ─────────────────────────────────────────────────────────────
# market-data-service  Dockerfile
# Build context: ./backend  (pasado desde docker-compose)
# ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS base

# Evitar escritura de .pyc y buffering de stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# ── Dependencias del sistema ──────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Dependencias Python ───────────────────────────────────────
COPY services/market-data-service/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# ── Código del servicio ───────────────────────────────────────
# shared/ se copia aquí para el build; en dev se monta como volume.
COPY shared/ ./shared/
COPY services/market-data-service/market_data_app/ ./market_data_app/

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
