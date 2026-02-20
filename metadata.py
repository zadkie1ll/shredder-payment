from pydantic import BaseModel
from typing import Literal


class Metadata(BaseModel):
    username: str
    email: str | None = None
    telegram_id: int | None = None
    subscription_period: Literal[
        "oneday", "threedays", "month", "threemonths", "sixmonths", "year"
    ]

    # Если True - значит это автосписание.
    autopay: bool

    # Если True - значит произошла оплата пробного тарифа "Попробовать за 10 рублей"
    trial_promotion: bool

    # Если True - значит оплата происходит в рамках перехода с пробного периода.
    # Это может быть автосписание после пробного периода, а может быть ручная оплата после пробного периода.
    from_trial: bool
