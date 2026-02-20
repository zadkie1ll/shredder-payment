from redis_message_publisher import RedisMessagePublisher
from common.models.tariff import Tariff
from common.models.messages import NotificateUserMessage
from common.models.messages import ReferralPurchaseBonusApplied


def create_notification_message(
    notification_type: str, telegram_id: int
) -> NotificateUserMessage:
    return NotificateUserMessage(
        notification_type=notification_type,
        telegram_id=telegram_id,
    )


async def send_referral_purchase_bonus_applied(
    publisher: RedisMessagePublisher,
    telegram_id: int,
    tariff: str,
    bonus_days_count: int,
):
    message = ReferralPurchaseBonusApplied(
        telegram_id=telegram_id,
        referral_tariff=tariff,
        bonus_days_count=bonus_days_count,
    )
    await publisher.push_message_to_vpn_bot(message=message)
    await publisher.push_message_to_vps_bot(message=message)


async def send_succeeded_autopay(publisher: RedisMessagePublisher, telegram_id: int):
    message = create_notification_message("purchase-success-autopay", telegram_id)
    await publisher.push_message_to_vpn_bot(message=message)
    await publisher.push_message_to_vps_bot(message=message)


async def send_succeeded_non_autopay(
    publisher: RedisMessagePublisher, telegram_id: int
):
    message = create_notification_message("purchase-success-non-autopay", telegram_id)
    await publisher.push_message_to_vpn_bot(message=message)
    await publisher.push_message_to_vps_bot(message=message)


async def send_failed_autopay(publisher: RedisMessagePublisher, telegram_id: int):
    message = create_notification_message("purchase-failure-autopay", telegram_id)
    await publisher.push_message_to_vpn_bot(message=message)
    await publisher.push_message_to_vps_bot(message=message)


async def send_failed_non_autopay(publisher: RedisMessagePublisher, telegram_id: int):
    message = create_notification_message("purchase-failure-non-autopay", telegram_id)
    await publisher.push_message_to_vpn_bot(message=message)
    await publisher.push_message_to_vps_bot(message=message)
