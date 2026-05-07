from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import logging

logger = logging.getLogger("market-data")
app = FastAPI(title="market-data-service", version="0.1.0")


class Health(BaseModel):
    status: str


@app.get("/health", response_model=Health)
async def health() -> Health:
    return Health(status="ok")


@app.on_event("startup")
async def startup_event() -> None:
    logger.info("market-data-service startup — inicializando clientes (redis, postgres, exchanges)")


@app.websocket("/ws/echo")
async def ws_echo(ws: WebSocket) -> None:
    await ws.accept()
    try:
        while True:
            data = await ws.receive_text()
            await ws.send_text(f"echo: {data}")
    except WebSocketDisconnect:
        logger.info("WebSocket cliente desconectado")
    except Exception:
        logger.exception("Error en WebSocket")
        await ws.close()
