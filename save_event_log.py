import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from common.models.db import User, EventLog
from common.models.analytics_event import AnalyticsEvent


async def save_event_log(
    session: AsyncSession, username: str, event: AnalyticsEvent
) -> None:
    user_id = await session.scalar(
        select(User.id).where(User.username == username).limit(1)
    )

    if user_id is None:
        logging.error(f"not found user id for username {username}")
        return

    session.add(
        EventLog(
            user_id=user_id,
            event_type=event.event_type,
            event_payload=event.model_dump(),
        )
    )
