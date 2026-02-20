import os

# yk payment
MI_YKP_HOST = "MI_YKP_HOST"
MI_YKP_PORT = "MI_YKP_PORT"
MI_YKP_LOG_LEVEL = "MI_YKP_LOG_LEVEL"
MI_YKP_TRUST_X_FORWARDED_FOR = "MI_YKP_TRUST_X_FORWARDED_FOR"
MI_YKP_SSL_CERT = "MI_YKP_SSL_CERT"
MI_YKP_SSL_KEY = "MI_YKP_SSL_KEY"
MI_YKP_INTERNAL_ALL_NODES_SQUAD_UUID = "MI_YKP_INTERNAL_ALL_NODES_SQUAD_UUID"

# rwms
MI_YKP_RWMS_ADDR = "MI_YKP_RWMS_ADDR"
MI_YKP_RWMS_PORT = "MI_YKP_RWMS_PORT"

# redis
MI_YKP_REDIS_HOST = "MI_YKP_REDIS_HOST"
MI_YKP_REDIS_PORT = "MI_YKP_REDIS_PORT"
MI_YKP_REDIS_PASSWORD = "MI_YKP_REDIS_PASSWORD"

# postgres
MI_YKP_POSTGRES_HOST = "MI_YKP_POSTGRES_HOST"
MI_YKP_POSTGRES_PORT = "MI_YKP_POSTGRES_PORT"
MI_YKP_POSTGRES_USER = "MI_YKP_POSTGRES_USER"
MI_YKP_POSTGRES_PASSWORD = "MI_YKP_POSTGRES_PASSWORD"
MI_YKP_POSTGRES_DB = "MI_YKP_POSTGRES_DB"


class Config:
    def __init__(self):
        self.server_host: str = self.__read_required_str_env(MI_YKP_HOST)
        self.server_port: int = self.__read_required_int_env(MI_YKP_PORT)
        self.log_level: str = os.getenv(MI_YKP_LOG_LEVEL, "info")
        self.trust_x_forwarded_for: bool = (
            os.getenv(MI_YKP_TRUST_X_FORWARDED_FOR, "false") == "true"
        )

        self.ssl_cert: str | None = os.getenv(MI_YKP_SSL_CERT)
        self.ssl_key: str | None = os.getenv(MI_YKP_SSL_KEY)

        self.internal_all_nodes_squad_uuid: str = self.__read_required_str_env(
            MI_YKP_INTERNAL_ALL_NODES_SQUAD_UUID
        )

        # rwms envs
        self.rwms_address: str = self.__read_required_str_env(MI_YKP_RWMS_ADDR)
        self.rwms_port: int = self.__read_required_int_env(MI_YKP_RWMS_PORT)

        # redis envs
        self.redis_host: str = self.__read_required_str_env(MI_YKP_REDIS_HOST)
        self.redis_port: int = self.__read_required_int_env(MI_YKP_REDIS_PORT)
        self.redis_password: str = self.__read_required_str_env(MI_YKP_REDIS_PASSWORD)

        # postgres envs
        self.pg_host: str = self.__read_required_str_env(MI_YKP_POSTGRES_HOST)
        self.pg_port: int = self.__read_required_int_env(MI_YKP_POSTGRES_PORT)
        self.pg_user: str = self.__read_required_str_env(MI_YKP_POSTGRES_USER)
        self.pg_password: str = self.__read_required_str_env(MI_YKP_POSTGRES_PASSWORD)
        self.pg_db: str = self.__read_required_str_env(MI_YKP_POSTGRES_DB)

    def __read_required_int_env(self, name: str) -> int:
        value = os.getenv(name)

        if value is None:
            raise ValueError(f"{name} environment variable is not set.")

        try:
            return int(value)
        except ValueError:
            raise ValueError(f"{name} must be an integer, got {value!r}")

    def __read_required_str_env(self, name: str) -> str:
        value = os.getenv(name)

        if value is None:
            raise ValueError(f"{name} environment variable is not set.")

        return value
