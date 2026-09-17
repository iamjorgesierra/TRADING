# Arquitectura resumida

Microservicios principales (esqueleto):
- market-data-service
- historical-data-service
- indicator-engine
- smart-money-engine
- order-flow-engine
- ai-engine
- strategy-engine
- signal-engine
- risk-engine
- backtesting-engine
- execution-engine

Stack inicial:
- Python, FastAPI, AsyncIO
- PostgreSQL / TimescaleDB
- Redis
- Kafka / Redis Streams
- PyTorch, XGBoost, LightGBM
- Docker, Kubernetes

Decisiones iniciales:
- Priorizar comunicación asíncrona entre servicios (Redis Streams / Kafka).
- Mantener servicios pequeños y con responsabilidades únicas.


## Python package namespaces

Cada microservicio usa un namespace Python único (`market_data_app`, `historical_data_app`, `indicator_app`, `session_app`) para evitar colisiones del paquete genérico `app` cuando el monorepo se ejecuta y se testea desde una sola raíz.
