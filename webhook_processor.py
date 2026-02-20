import orjson
import logging
import asyncio
import pydantic

from pathlib import Path
from sqlalchemy.ext.asyncio import async_sessionmaker
from yookassa.domain.notification import WebhookNotification
from yookassa.domain.notification import WebhookNotificationFactory
from yookassa.domain.notification import WebhookNotificationEventType

from config import Config
from metadata import Metadata
from common.rwms_client import RwmsClient
from redis_message_publisher import RedisMessagePublisher
from refund_handlers import handle_succeeded_refund
from payment_handlers import handle_canceled_payment
from payment_handlers import handle_succeeded_payment
from rwms_tasks_processor import RwmsTasksProcessor

PROCESS_WEBHOOK_PAUSE = 10  # seconds


class WebhookProcessor:
    def __init__(
        self,
        publisher: RedisMessagePublisher,
        rwms_tasks_processor: RwmsTasksProcessor,
        session_maker: async_sessionmaker,
        config: Config,
    ):
        self.__publisher = publisher
        self.__rwms_tasks_processor = rwms_tasks_processor
        self.__config = config
        self.__rwms_client = RwmsClient(addr=config.rwms_address, port=config.rwms_port)
        self.__session_maker = session_maker

        self.__webhooks_dir = Path("webhooks")
        self.__pending_dir = self.__webhooks_dir / "pending"
        self.__processing_dir = self.__webhooks_dir / "processing"

        self.__webhooks_dir.mkdir(parents=True, exist_ok=True)
        self.__pending_dir.mkdir(parents=True, exist_ok=True)
        self.__processing_dir.mkdir(parents=True, exist_ok=True)

    def schedule(self, event_id: str, body: str):
        logging.info(f"writing webhook {event_id} to disk")

        filename = f"{event_id}.json"

        path = self.__pending_dir / filename
        path.write_text(body)

        logging.info(f"webhook {event_id} saved on disk at {path}")

    async def process(self):
        while True:
            files = self.__get_pending_webhooks()

            for file in files:
                processing = self.__mark_webhook_as_processing(file)

                try:
                    ET = WebhookNotificationEventType
                    logging.info(f"took {processing.name} to handle")
                    event, response = await self.__parse_webhook_from_file(processing)

                    if event == ET.PAYMENT_SUCCEEDED:
                        await self.__on_payment_succeeded(processing, response)

                    elif event == ET.PAYMENT_WAITING_FOR_CAPTURE:
                        logging.info(f"{processing.name} is waiting for capture")
                        self.__remove_file(processing)

                    elif event == ET.PAYMENT_CANCELED:
                        await self.__on_payment_canceled(processing, response)

                    elif event == ET.REFUND_SUCCEEDED:
                        await handle_succeeded_refund(self.__session_maker, response)

                    elif event == ET.DEAL_CLOSED:
                        ...

                    elif event == ET.PAYOUT_SUCCEEDED:
                        ...

                    elif event == ET.PAYOUT_CANCELED:
                        ...

                    else:
                        logging.debug(
                            f"skipping uninteresting webhook in file {processing.name}"
                        )
                        self.__remove_file(processing)
                except Exception as e:
                    logging.error(
                        f"error processing file {processing.name}: {e}", exc_info=True
                    )

            await asyncio.sleep(PROCESS_WEBHOOK_PAUSE)

    def __get_pending_webhooks(self):
        return sorted(
            self.__pending_dir.glob("*.json"), key=lambda f: f.stat().st_ctime
        )

    def __mark_webhook_as_processing(self, file: Path) -> Path:
        new_path = self.__processing_dir / file.name
        file.rename(new_path)
        return new_path

    def __remove_file(self, file: Path):
        try:
            file.unlink()
            logging.info(f"{file.name} removed successfully")
        except Exception as e:
            logging.error(f"failed to remove {file.name}: {e}")

    def __remove_on_success(self, success: bool, file: Path):
        if not success:
            logging.info(f"success flag is false, do not remove {file}")
            return

        self.__remove_file(file)

    async def __on_payment_succeeded(self, processing, response):
        try:
            metadata = Metadata.model_validate(response.metadata)
        except pydantic.ValidationError:
            # Возможно здесь нужно удалить .processing webhook файл
            logging.warning(f"invalid metadata in webhook {processing.name}")
            return

        success = await handle_succeeded_payment(
            publisher=self.__publisher,
            tasks_processor=self.__rwms_tasks_processor,
            rwms_client=self.__rwms_client,
            config=self.__config,
            session_maker=self.__session_maker,
            payment=response,
            metadata=metadata,
        )

        self.__remove_on_success(success, processing)

    async def __on_payment_canceled(self, processing, response):
        try:
            metadata = Metadata.model_validate(response.metadata)
        except pydantic.ValidationError:
            # Возможно здесь нужно удалить .processing webhook файл
            logging.warning(f"invalid metadata in webhook {processing.name}")
            return

        success = await handle_canceled_payment(
            publisher=self.__publisher,
            session_maker=self.__session_maker,
            payment=response,
            metadata=metadata,
        )

        self.__remove_on_success(success, processing)

    async def __parse_webhook_from_file(self, file) -> tuple[str, WebhookNotification]:
        text = file.read_text()
        json_data = orjson.loads(text)

        notification = WebhookNotificationFactory().create(json_data)
        event = notification.event
        response = notification.object

        return event, response
