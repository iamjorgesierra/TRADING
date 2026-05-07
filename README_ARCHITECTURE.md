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
