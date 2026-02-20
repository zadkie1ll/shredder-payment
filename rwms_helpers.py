from typing import Optional
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from common.rwms_client import RwmsClient
from common.models.tariff import Tariff
import proto.rwmanager_pb2 as proto

from config import Config


async def create_user(
    rwms_client: RwmsClient,
    config: Config,
    tariff: Tariff,
    username: str,
    telegram_id: int | None = None,
    email: str | None = None,
) -> Optional[proto.UserResponse]:
    response = await rwms_client.add_user(
        proto.AddUserRequest(
            username=username,
            telegram_id=telegram_id,
            email=email,
            expire_at=datetime.now(timezone.utc) + tariff.subscription_period,
            activate_all_inbounds=True,
            status=proto.UserStatus.ACTIVE,
            traffic_limit_strategy=proto.TrafficLimitStrategy.NO_RESET,
            active_internal_squads=[config.internal_all_nodes_squad_uuid],
        )
    )

    return response


async def update_user(
    rwms_client: RwmsClient,
    config: Config,
    user: proto.UserResponse,
    interval: timedelta,
) -> Optional[proto.UserResponse]:
    new_expire_at = None
    subscription_activated = False

    if user.HasField("expire_at"):
        new_expire_at = user.expire_at.ToDatetime(tzinfo=timezone.utc)

    if new_expire_at is None or new_expire_at < datetime.now(timezone.utc):
        new_expire_at = datetime.now(timezone.utc) + interval
        subscription_activated = True
    else:
        new_expire_at = new_expire_at + interval

    update_user_response = await rwms_client.update_user(
        proto.UpdateUserRequest(
            uuid=user.uuid,
            expire_at=new_expire_at,
            status=proto.UserStatus.ACTIVE,
            traffic_limit_strategy=proto.TrafficLimitStrategy.NO_RESET,
            active_internal_squads=[config.internal_all_nodes_squad_uuid],
        )
    )

    return update_user_response, subscription_activated
