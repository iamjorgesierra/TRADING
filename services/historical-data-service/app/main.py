from fastapi import FastAPI
from pydantic import BaseModel
import logging

logger = logging.getLogger("historical-data")
app = FastAPI(title="historical-data-service", version="0.1.0")


class Health(BaseModel):
    status: str


@app.get("/health", response_model=Health)
async def health() -> Health:
    return Health(status="ok")


@app.post("/ingest")
async def ingest(payload: dict) -> dict:
    logger.debug("Ingest recibido")
    # TODO: validar y almacenar en TimescaleDB
    return {"status": "ok"}
