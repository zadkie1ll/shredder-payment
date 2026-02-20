import logging
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker
from yookassa.domain.notification import RefundResponse
from common.models.db import YkPayment


async def handle_succeeded_refund(
    session_maker: async_sessionmaker, refund: RefundResponse
) -> bool:
    try:
        async with session_maker() as session:
            await session.execute(
                update(YkPayment)
                .where(YkPayment.payment_id == refund.payment_id)
                .values(status="refunded")
            )

            await session.commit()

        logging.info(f"succeeded refund {refund.id} successfully processed")
        return True
    except Exception as e:
        logging.error(f"handling succeeded refund error: {e}")
        return False
