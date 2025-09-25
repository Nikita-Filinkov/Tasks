from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, FastAPI

from tasks.task1.dependecies import get_pg_connection
from tasks.task1.exeptions import BaseRequestException, UnexpectedException
from tasks.task1.loggers import logger


async def get_db_version(conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]):
    """Получение версии PostgreSQL"""
    try:
        version = await conn.fetchval("SELECT version()")
        return {"version": version}

    except asyncpg.PostgresError as e:
        extra = {'error': str(e)}
        logger.error(f"Ошибка выполнения запроса к PostgreSQL", extra)
        raise BaseRequestException

    except Exception as e:
        extra = {'error': str(e)}
        logger.error(f"Неожиданная ошибка при подключении к PostgreSQL", extra)
        raise UnexpectedException


def register_routes(app: FastAPI):
    """Подключение routes"""
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)