import orjson
import logging
from redis.asyncio import Redis
from config import Config
from common.models.messages import MessageUnion


class RedisMessagePublisher:
    def __init__(self, config: Config):
        self.__redis = Redis(
            host=config.redis_host,
            port=config.redis_port,
            password=config.redis_password,
            decode_responses=True,
        )

    async def push_message_to_vpn_bot(self, message: MessageUnion):
        try:
            data = message.model_dump()
            json = orjson.dumps(data).decode("utf-8")
            await self.__redis.rpush("monkey-island-vpn-bot", json)
            logging.info(
                f"pushed message of type {message.type} to VPN bot for {message.telegram_id}"
            )
        except Exception as e:
            logging.error(f"failed to push message to VPN bot: {e}")

    async def push_message_to_vps_bot(self, message: MessageUnion):
        try:
            data = message.model_dump()
            json = orjson.dumps(data).decode("utf-8")
            await self.__redis.rpush("monkey-island-vps-bot", json)
            logging.info(
                f"pushed message of type {message.type} to VPS bot for {message.telegram_id}"
            )
        except Exception as e:
            logging.error(f"failed to push message to VPS bot: {e}")

    async def push_message_to_ym_stat(self, message: MessageUnion):
        json = message.model_dump_json()
        await self.__redis.rpush("monkey-island-ym-stat", json)
        logging.info(
            f"pushed message of type {message.type} to YM stat for {message.client_id}"
        )
