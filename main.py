import orjson
import logging
import asyncio
import uvicorn

from fastapi import FastAPI
from fastapi import Request
from fastapi import HTTPException
from fastapi import Depends
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from yookassa.domain.common import SecurityHelper
from yookassa.domain.notification import WebhookNotificationFactory
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker

from config import Config
from common.setup_logger import setup_logger
from webhook_processor import WebhookProcessor
from rwms_tasks_processor import RwmsTasksProcessor
from redis_message_publisher import RedisMessagePublisher

config = Config()

log_level = logging.INFO

if config.log_level.lower() == "debug":
    log_level = logging.DEBUG
if config.log_level.lower() == "info":
    log_level = logging.INFO
if config.log_level.lower() == "warning":
    log_level = logging.WARN
if config.log_level.lower() == "error":
    log_level = logging.ERROR
if config.log_level.lower() == "critical":
    log_level = logging.CRITICAL

setup_logger(filename="monkey-island-payment.log", level=log_level)

DATABASE_URL = f"postgresql+asyncpg://{config.pg_user}:{config.pg_password}@{config.pg_host}:{config.pg_port}/{config.pg_db}"
ENGINE = create_async_engine(DATABASE_URL, echo=False)
SESSION_MAKER = async_sessionmaker(bind=ENGINE, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.rwms_tasks_processor = RwmsTasksProcessor(
        config=config, session_maker=SESSION_MAKER
    )

    publisher = RedisMessagePublisher(config=config)

    app.state.webhook_processor = WebhookProcessor(
        publisher=publisher,
        rwms_tasks_processor=app.state.rwms_tasks_processor,
        session_maker=SESSION_MAKER,
        config=config,
    )

    asyncio.create_task(app.state.webhook_processor.process())
    asyncio.create_task(app.state.rwms_tasks_processor.process())
    yield


def get_webhook_processor(request: Request) -> WebhookProcessor:
    return request.app.state.webhook_processor


app = FastAPI(lifespan=lifespan)


@app.post("/yookassa/webhook")
async def webhook(
    request: Request,
    webhook_processor: WebhookProcessor = Depends(get_webhook_processor),
):
    client_host_allowed = SecurityHelper().is_ip_trusted(request.client.host)

    if not client_host_allowed and config.trust_x_forwarded_for:
        x_forwarded_for = request.headers.get("x-forwarded-for")
        client_host_allowed = SecurityHelper().is_ip_trusted(x_forwarded_for)

    if not client_host_allowed:
        return JSONResponse(
            status_code=403, content={"error": "Forbidden: IP not allowed"}
        )

    body = (await request.body()).decode("utf-8")
    logging.info(f"received webhook payload: {body}")

    try:
        json_data = orjson.loads(body)
        notification_object = WebhookNotificationFactory().create(json_data)
        response_object = notification_object.object
        webhook_processor.schedule(response_object.id, body)
        logging.info(f"webhook {response_object.id} scheduled")

    except orjson.JSONDecodeError:
        logging.error("Failed to decode JSON", exc_info=True)
        raise HTTPException(status_code=400, detail="Invalid JSON")

    except Exception as e:
        logging.error("Error processing webhook", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    return {"status": "ok"}


if __name__ == "__main__":
    if not config.ssl_cert or not config.ssl_key:
        logging.info("starting without SSL")

        uvicorn.run(
            "main:app", host=config.server_host, port=config.server_port, reload=True
        )
    else:
        logging.info(f"starting with SSL, cert {config.ssl_cert}, key {config.ssl_key}")

        uvicorn.run(
            "main:app",
            host=config.server_host,
            port=config.server_port,
            reload=True,
            ssl_certfile=config.ssl_cert,
            ssl_keyfile=config.ssl_key,
        )
