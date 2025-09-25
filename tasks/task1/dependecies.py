import asyncpg

from tasks.task1.config import settings
from tasks.task1.exeptions import BaseConnException, UnexpectedException
from tasks.task1.loggers import logger


async def get_pg_connection() -> asyncpg.Connection:
    """Получение подключения к PostgreSQL"""
    try:
        connection = await asyncpg.connect(settings.database_url)
        logger.info("Успешное подключение к PostgreSQL")
        try:
            yield connection
        finally:
            await connection.close()
            logger.info("Подключение к PostgreSQL закрыто")

    except asyncpg.PostgresError as e:
        extra = {'error': str(e)}
        logger.error(f"Ошибка подключения к PostgreSQL", extra)
        raise BaseConnException

    except Exception as e:
        extra = {'error': str(e)}
        logger.error(f"Неожиданная ошибка при подключении к PostgreSQL", extra)
        raise UnexpectedException




