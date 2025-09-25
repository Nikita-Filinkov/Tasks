from aiochclient import ChClient
from aiohttp import ClientSession
from tasks.task4.loggers import logger
from tasks.task4.config import settings


class ClickHouseClient:
    def __init__(self):
        self._session = None
        self._client = None

    async def __aenter__(self):
        self._session = ClientSession()
        self._client = ChClient(
            self._session,
            url=settings.CLICKHOUSE_HOST,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
        )

        try:
            await self._client.execute("SELECT 1")
            logger.info("Успешное подключение к ClickHouse")
        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {str(e)}")
            await self._session.close()
            raise

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            logger.info("Подключение к ClickHouse закрыто")

