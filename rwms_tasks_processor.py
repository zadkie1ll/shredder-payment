import orjson
import logging
import asyncio
from pathlib import Path
from pydantic import BaseModel
from typing import Literal, Union
from sqlalchemy.ext.asyncio import async_sessionmaker

from config import Config
from save_event_log import save_event_log
from common.rwms_client import RwmsClient
from rwms_helpers import create_user, update_user
from common.models.tariff import Tariff
from common.models.analytics_event import SubscriptionActivated

PROCESS_RWMS_TASK_PAUSE = 10  # seconds


# Поле username используется как идентификатор подписки в remnawave и пользователя в базе данных.
# Поле telegram_id используется для логирования событий в базе данных.
class RwmsAddTimeIntervalTask(BaseModel):
    type: Literal["add-time-interval"]
    username: str
    tariff: Tariff
    telegram_id: int | None = None
    email: str | None = None


class RwmsSubtractTimeIntervalTask(BaseModel):
    type: Literal["subtract-time-interval"]
    username: int
    tariff: Tariff
    telegram_id: int | None = None
    email: str | None = None


RwmsTask = Union[RwmsAddTimeIntervalTask, RwmsSubtractTimeIntervalTask]

RWMS_TASK_CLASSES: dict[str, type[BaseModel]] = {
    "add-time-interval": RwmsAddTimeIntervalTask,
    "subtract-time-interval": RwmsSubtractTimeIntervalTask,
}


class RwmsTasksProcessor:
    def __get_pending_tasks(self):
        return sorted(
            self.__pending_dir.glob("*.json"), key=lambda f: f.stat().st_ctime
        )

    def __mark_task_as_processing(self, file: Path) -> Path:
        new_path = self.__processing_dir / file.name
        file.rename(new_path)
        return new_path

    def __remove_file(self, file: Path):
        try:
            file.unlink()
            logging.info(f"{file} removed successfully")
        except Exception as e:
            logging.error(f"failed to remove {file}: {e}")

    async def __save_subscription_reactivated(self, username: str) -> None:
        async with self.__session_maker() as session:
            async with session.begin():
                try:
                    await save_event_log(session, username, SubscriptionActivated())
                except Exception as e:
                    logging.error(
                        f"saving subscription reactivated event log failed: {e}"
                    )

    def __init__(self, config: Config, session_maker: async_sessionmaker):
        self.__config = config
        self.__session_maker = session_maker
        self.__rwms_client = RwmsClient(addr=config.rwms_address, port=config.rwms_port)

        # Директории для хранения задач для продления подписок в remnawave
        self.__rwms_tasks_dir = Path("rwms-tasks")
        self.__pending_dir = self.__rwms_tasks_dir / "pending"
        self.__processing_dir = self.__rwms_tasks_dir / "processing"

        self.__rwms_tasks_dir.mkdir(parents=True, exist_ok=True)
        self.__pending_dir.mkdir(parents=True, exist_ok=True)
        self.__processing_dir.mkdir(parents=True, exist_ok=True)

    # Сохранение задачи на продление подписки в remnawave на диск.
    # После этого основной цикл будет её обрабатывать.
    def schedule(self, payment_id: str, task: RwmsTask):
        logging.info(f"writing rwms task {payment_id} on disk")

        filename = f"{payment_id}.json"
        task_file = self.__pending_dir / filename

        with open(task_file, "w") as f:
            f.write(task.model_dump_json())

        logging.info(f"rwms task {payment_id} saved on disk at {task_file}")

    async def process(self):
        while True:
            files = self.__get_pending_tasks()

            for file in files:
                processing = self.__mark_task_as_processing(file)

                try:
                    text = processing.read_text()
                    data = orjson.loads(text)
                    type = data.get("type")

                    if type not in RWMS_TASK_CLASSES:
                        raise ValueError(f"unknown rwms task type: {type}")

                    task_class = RWMS_TASK_CLASSES[type]
                    task = task_class.model_validate(data)

                    if task_class == RwmsAddTimeIntervalTask:
                        logging.info(
                            f"processing {task_class} task for subscription {task.username}"
                        )

                        user = await self.__rwms_client.get_user_by_username(
                            task.username
                        )

                        user_response = None

                        if user is None:
                            user_response = await create_user(
                                rwms_client=self.__rwms_client,
                                config=self.__config,
                                tariff=task.tariff,
                                username=task.username,
                                telegram_id=task.telegram_id,
                                email=task.email,
                            )
                        else:
                            logging.info(f"updating expire time for {task.username}")

                            user_response, subscription_activated = await update_user(
                                rwms_client=self.__rwms_client,
                                config=self.__config,
                                user=user,
                                interval=task.tariff.subscription_period,
                            )

                        if user_response is not None:
                            self.__remove_file(processing)
                            logging.info(
                                f"successfully handled {task_class} task for {task.username}, "
                                f"tariff {task.tariff.description}"
                            )

                        if subscription_activated:
                            await self.__save_subscription_reactivated(task.username)

                except Exception as e:
                    logging.error(f"executing rwms task {file.name} failed: {e}")

            await asyncio.sleep(PROCESS_RWMS_TASK_PAUSE)
