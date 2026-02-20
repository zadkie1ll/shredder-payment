import logging

from typing import Optional
from datetime import datetime
from datetime import timedelta
from sqlalchemy import insert
from sqlalchemy import exists
from sqlalchemy import text
from sqlalchemy import select
from sqlalchemy import delete
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from yookassa.domain.notification import PaymentResponse

from common.rwms_client import RwmsClient
from config import Config
from metadata import Metadata
from redis_message_publisher import RedisMessagePublisher
from common.models.db import User
from common.models.db import YkPayment
from common.models.db import ReferralType
from common.models.db import ReferralBonus
from common.models.db import ReferralBonusType
from common.models.db import YkRecurrentPayment
from common.models.tariff import str_to_tariff
from common.models.tariff import OneMonthTariff
from common.models.analytics_event import SubscriptionActivated
from common.models.analytics_event import PaymentTrialManualSuccess
from common.models.analytics_event import PaymentTrialManualFailure
from common.models.analytics_event import PaymentRegularManualSuccess
from common.models.analytics_event import PaymentRegularManualFailure
from common.models.analytics_event import PaymentRegularAutopaySuccess
from common.models.analytics_event import PaymentRegularAutopayFailure
from common.models.analytics_event import PaymentTrialToRegularAutopaySuccess
from common.models.analytics_event import PaymentTrialToRegularAutopayFailure
from rwms_helpers import update_user
from rwms_tasks_processor import RwmsTasksProcessor
from rwms_tasks_processor import RwmsAddTimeIntervalTask
from send_notification import send_succeeded_autopay
from send_notification import send_referral_purchase_bonus_applied
from send_notification import send_succeeded_non_autopay
from send_notification import send_failed_autopay
from send_notification import send_failed_non_autopay
from send_purchase import send_purchase
from save_event_log import save_event_log


async def has_payment(payment_id: str, session: AsyncSession) -> bool:
    result = await session.execute(
        select(YkPayment).where(YkPayment.payment_id == payment_id)
    )

    return result.scalar() is not None


async def get_user_id_by_username(
    username: str, session: AsyncSession
) -> Optional[int]:
    result = await session.execute(select(User.id).where(User.username == username))

    return result.scalars().first()


async def is_user_allow_autopay_disabled(username: str, session: AsyncSession) -> bool:
    result = await session.execute(
        select(User.autopay_allow).where(User.username == username)
    )

    flag = result.scalars().first()

    if flag is not None:
        return flag == False

    # Такого не должно быть, что делать?
    return False


async def save_payment_if_not_exists(
    payment: PaymentResponse, metadata: Metadata, session: AsyncSession
):
    has_payment_flag = await has_payment(payment.id, session)

    if not has_payment_flag:
        user_id = await get_user_id_by_username(metadata.username, session)

        if user_id is None:
            # что делать если нет пользователя для которого прилетел платеж в базе?
            # по идее такой ситуации быть не должно
            logging.critical(
                f"not found user in database with telegram ID "
                f"'{metadata.username}' received by payment ID '{payment.id}'"
            )
            raise RuntimeError(
                f"not found user with telegram ID "
                f"'{metadata.username}' received by payment ID '{payment.id}'"
            )

        captured_at = None

        if payment.captured_at is not None:
            captured_at = datetime.fromisoformat(
                payment.captured_at.replace("Z", "+00:00")
            ).replace(tzinfo=None)

        created_at = datetime.fromisoformat(
            payment.created_at.replace("Z", "+00:00")
        ).replace(tzinfo=None)

        await session.execute(
            insert(YkPayment).values(
                is_trial_promotion=metadata.trial_promotion,
                user_id=user_id,
                amount=payment.amount.value,
                currency=payment.amount.currency,
                status=payment.status,
                captured_at=captured_at,
                created_at=created_at,
                payment_id=payment.id,
                subscription_period=metadata.subscription_period,
            )
        )


async def extend_user_subscription_by_username(
    session: AsyncSession,
    username: str,
    interval: timedelta,
) -> None:
    extend_expire_at_query = text("""
        UPDATE users 
            SET expire_at = 
            CASE 
                WHEN expire_at > (NOW() AT TIME ZONE 'UTC') THEN expire_at + (:interval)::interval
                ELSE (NOW() AT TIME ZONE 'UTC') + (:interval)::interval
            END
            WHERE username = :username
        """)

    await session.execute(
        extend_expire_at_query,
        {
            "username": username,
            "interval": interval,
        },
    )


# Дублирующийся код, который уже есть в rwms_tasks_processor, надо подумать как его вынести в общее место, чтобы не было дублирования
async def save_subscription_reactivated(session: AsyncSession, username: str) -> None:
    try:
        await save_event_log(session, username, SubscriptionActivated())
    except Exception as e:
        logging.error(f"saving subscription reactivated event log failed: {e}")


# Этот метод должен работать по принципу rwms_tasks_processor
async def add_referrer_bonus_if_needed(
    publisher: RedisMessagePublisher,
    session_maker: async_sessionmaker,
    rwms_client: RwmsClient,
    config: Config,
    metadata: Metadata,
    bonus_days_count: int,
):
    if metadata.subscription_period in ["oneday", "threedays"]:
        logging.info(
            f"subscription period {metadata.subscription_period} is too short, skipping referral bonus"
        )
        return

    async with session_maker() as session:
        async with session.begin():
            result = await session.execute(
                select(User.id, User.referred_by_id)
                .where(User.username == metadata.username)
                .where(User.referral_type == ReferralType.STANDARD)
                .where(User.referred_by_id.isnot(None))
                .limit(1)
            )

            row = result.one_or_none()

            # Есть небольшой шанс, что и username в базе нет для которого оплата пришла
            if not row:
                logging.info(
                    f"user {metadata.username} has no referrer, skipping referral bonus"
                )
                return

            referral_id, referrer_id = row

            already_applied_for_referrer = await session.execute(
                select(
                    exists().where(
                        ReferralBonus.referrer_id == referrer_id,
                        ReferralBonus.referral_id == referral_id,
                        ReferralBonus.bonus_type == ReferralBonusType.PURCHASE,
                    )
                )
            )

            if already_applied_for_referrer.scalar():
                logging.info(
                    f"referral bonus for referrer {referrer_id} and referral {referral_id} already applied, skipping"
                )
                return

            referrer_username_result = await session.execute(
                select(User.username, User.telegram_id)
                .where(User.id == referrer_id)
                .limit(1)
            )

            referrer_info = referrer_username_result.one_or_none()

            if not referrer_info:
                logging.warning(
                    f"referrer info not found for referrer_id {referrer_id}"
                )
                return

            referrer_username, referrer_telegram_id = referrer_info
            bonus_days_interval = timedelta(days=bonus_days_count)

            await extend_user_subscription_by_username(
                session, referrer_username, bonus_days_interval
            )
            user = await rwms_client.get_user_by_username(referrer_username)

            user_response, subscription_activated = await update_user(
                rwms_client=rwms_client,
                config=config,
                user=user,
                interval=bonus_days_interval,
            )

            bonus = ReferralBonus(
                referral_id=referral_id,
                referrer_id=referrer_id,
                bonus_type=ReferralBonusType.PURCHASE,
                days_added=bonus_days_count,
            )
            session.add(bonus)

            if user_response is not None:
                logging.info(
                    f"referral bonus for referrer {referrer_username} and referral {metadata.username} applied successfully"
                )
            else:
                logging.error(
                    f"failed to apply referral bonus for referrer {referrer_username} and referral {metadata.username}"
                )

            if subscription_activated:
                await save_subscription_reactivated(session, referrer_username)

            if referrer_telegram_id is not None:
                await send_referral_purchase_bonus_applied(
                    publisher,
                    referrer_telegram_id,
                    metadata.subscription_period,
                    bonus_days_count,
                )


async def handle_succeeded_payment(
    publisher: RedisMessagePublisher,
    tasks_processor: RwmsTasksProcessor,
    rwms_client: RwmsClient,
    config: Config,
    session_maker: async_sessionmaker,
    payment: PaymentResponse,
    metadata: Metadata,
) -> bool:
    try:
        async with session_maker() as session:
            async with session.begin():
                await save_payment_if_not_exists(payment, metadata, session)

                await add_referrer_bonus_if_needed(
                    publisher=publisher,
                    session_maker=session_maker,
                    rwms_client=rwms_client,
                    config=config,
                    metadata=metadata,
                    bonus_days_count=30,
                )

                is_disabled = await is_user_allow_autopay_disabled(
                    metadata.username, session
                )

                # Выглядит странно, так как платеж пришел, но мы не продлеваем подписку?
                # Надо вспомнить почему так было сделано
                # ВЫВОД: Это серьезная ошибка в коде, всегда сперва надо шедулить продление подписки
                # и только потом решать, включать ли автоплатеж пользователю
                #
                # Но вообще при оплате этот флаг должен выставляться всегда в true.
                # Но если webhook будет долго доставляться, то возможно пользователь мог
                # отключить автоплатеж и когда эвент придет сюда, подписка не продлится.
                if is_disabled:
                    return True

                # Аналогично странно
                if not payment.payment_method.saved:
                    return True

                query = text("""
                    WITH user_data AS (
                        SELECT id FROM users WHERE username = :username LIMIT 1
                    )
                    INSERT INTO yk_recurrent_payments (
                        recurrent_payment_id,
                        user_id,
                        amount,
                        currency,
                        captured_at,
                        subscription_period,
                        is_trial_promotion,
                        scheduled_payment
                    )
                    SELECT
                        :recurrent_payment_id,
                        (SELECT id FROM user_data),
                        :amount,
                        :currency,
                        :captured_at,
                        :subscription_period,
                        :is_trial_promotion,
                        :scheduled_payment
                    ON CONFLICT (user_id) DO UPDATE SET
                        recurrent_payment_id = EXCLUDED.recurrent_payment_id,
                        amount = EXCLUDED.amount,
                        currency = EXCLUDED.currency,
                        captured_at = EXCLUDED.captured_at,
                        subscription_period = EXCLUDED.subscription_period,
                        is_trial_promotion = EXCLUDED.is_trial_promotion,
                        scheduled_payment = false
                    """)

                next_autopay_price = payment.amount.value
                next_autopay_period_to_extend = metadata.subscription_period
                tariff = str_to_tariff(metadata.subscription_period)

                if metadata.trial_promotion:
                    one_month_tariff = OneMonthTariff()
                    next_autopay_price = one_month_tariff.price
                    next_autopay_period_to_extend = one_month_tariff.db_tariff_id

                captured_at = datetime.fromisoformat(
                    payment.captured_at.replace("Z", "+00:00")
                ).replace(tzinfo=None)

                await session.execute(
                    query,
                    {
                        "recurrent_payment_id": payment.payment_method.id,
                        "username": metadata.username,
                        "amount": next_autopay_price,
                        "currency": payment.amount.currency,
                        "captured_at": captured_at,
                        "subscription_period": next_autopay_period_to_extend,
                        "is_trial_promotion": metadata.trial_promotion,
                        "scheduled_payment": False,
                    },
                )

                extend_expire_at_query = text("""
                    UPDATE users 
                        SET expire_at = 
                        CASE 
                            WHEN expire_at > (NOW() AT TIME ZONE 'UTC') THEN expire_at + (:interval)::interval
                            ELSE (NOW() AT TIME ZONE 'UTC') + (:interval)::interval
                        END
                        WHERE username = :username
                    """)

                await session.execute(
                    extend_expire_at_query,
                    {
                        "username": metadata.username,
                        "interval": tariff.subscription_period,
                    },
                )

                add_time_interval_task = RwmsAddTimeIntervalTask(
                    type="add-time-interval",
                    username=metadata.username,
                    tariff=tariff,
                    telegram_id=metadata.telegram_id,
                    email=metadata.email,
                )

                tasks_processor.schedule(payment.id, add_time_interval_task)

                if metadata.autopay:
                    await send_succeeded_autopay(publisher, metadata.telegram_id)
                else:
                    await send_succeeded_non_autopay(publisher, metadata.telegram_id)

                await send_purchase(publisher, metadata.username, payment.id, tariff)

                event = None

                if metadata.autopay:
                    if metadata.from_trial:  # автоплатеж-переход с пробного периода
                        event = PaymentTrialToRegularAutopaySuccess()
                    else:  # регулярный автоплатеж по тарифу
                        event = PaymentRegularAutopaySuccess()
                else:
                    if metadata.trial_promotion:  # ручная оплата пробного периода
                        event = PaymentTrialManualSuccess()
                    else:  # ручная оплата обычного тарифа
                        event = PaymentRegularManualSuccess()

                if event is not None:
                    await save_event_log(session, metadata.username, event)

                logging.info(
                    f"succeeded payment {payment.id} for user "
                    f"{metadata.username} successfully processed"
                )

                return True
    except Exception as e:
        logging.error(f"handling succeeded payment error: {e}")
        return False


async def handle_canceled_payment(
    publisher: RedisMessagePublisher,
    session_maker: async_sessionmaker,
    payment: PaymentResponse,
    metadata: Metadata,
) -> bool:
    try:
        async with session_maker() as session:
            async with session.begin():
                await save_payment_if_not_exists(payment, metadata, session)

                logging.info(
                    f"canceled payment {payment.id} for user "
                    f"{metadata.username} successfully processed"
                )

                event = None

                if metadata.autopay:
                    if (
                        metadata.from_trial
                    ):  # неуспешный автоплатеж-переход с пробного периода
                        event = PaymentTrialToRegularAutopayFailure()
                    else:  # неуспешный регулярный автоплатеж по тарифу
                        event = PaymentRegularAutopayFailure()
                else:
                    if metadata.trial_promotion:  # ручная оплата пробного периода
                        event = PaymentTrialManualFailure()
                    else:  # ручная оплата обычного тарифа
                        event = PaymentRegularManualFailure()

                if event is not None:
                    await save_event_log(session, metadata.username, event)

                if payment.status == "expired_on_confirmation":
                    logging.info(
                        f"payment for user {metadata.username} was expired on confirmation, do nothing"
                    )
                    return True

                if payment.status == "general_decline":
                    logging.info(
                        f"payment declined by user {metadata.username}, do nothing"
                    )
                    return True

                if not metadata.autopay:
                    await send_failed_non_autopay(publisher, metadata.telegram_id)
                    return True

                subquery = select(User.id).where(User.username == metadata.username)
                await session.execute(
                    delete(YkRecurrentPayment).where(
                        YkRecurrentPayment.user_id.in_(subquery)
                    )
                )

                await session.execute(
                    update(User)
                    .where(User.username == metadata.username)
                    .values(autopay_allow=False)
                )

                await send_failed_autopay(publisher, metadata.telegram_id)

                return True
    except Exception as e:
        logging.error(f"handling canceled payment error: {e}")
        return False
