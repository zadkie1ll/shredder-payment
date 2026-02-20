from redis_message_publisher import RedisMessagePublisher
from common.models.messages import SendPurchaseMessage
from common.models.tariff import Tariff


async def send_purchase(
    publisher: RedisMessagePublisher,
    username: str,
    transaction_id: str,
    tariff: Tariff,
):
    message = SendPurchaseMessage(
        service="monkey-island-ym-stat",
        type="send-purchase",
        client_id=username,
        transaction_id=transaction_id,
        tariff=tariff,
    )
    await publisher.push_message_to_ym_stat(message=message)
