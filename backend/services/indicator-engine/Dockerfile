FROM python:3.12-slim

WORKDIR /app

# Copiar dependencias primero para aprovechar cache de Docker
COPY services/indicator-engine/requirements.txt services/indicator-engine/requirements.txt
RUN pip install --no-cache-dir -r services/indicator-engine/requirements.txt

# Copiar shared y código del servicio
COPY shared/ shared/
COPY services/indicator-engine/app/ services/indicator-engine/app/

# Configurar PYTHONPATH para permitir imports absolutos desde /app
ENV PYTHONPATH=/app

EXPOSE 8004

CMD ["uvicorn", "services.indicator-engine.app.main:app", "--host", "0.0.0.0", "--port", "8004"]
