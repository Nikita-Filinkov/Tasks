import asyncio
from typing import List, Iterator
from datetime import date, datetime
from aiochclient import ChClient
from aiohttp import ClientSession, ClientError
import itertools

from tasks.task2.data_objects import Repository
from tasks.task3.loggers import logger
from tasks.task3.config import settings


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

    async def save_repositories(self, repositories: List[Repository]):
        """Сохранение данных о репозиториях в ClickHouse"""
        if not repositories:
            logger.warning("Нет данных для сохранения в ClickHouse")
            return

        today = date.today()
        now = datetime.now()

        repos_generator = self._generate_repos_data(repositories, now)
        positions_generator = self._generate_positions_data(repositories, today)
        authors_generator = self._generate_authors_data(repositories, today)

        try:
            await asyncio.gather(
                self._process_batches("test.repositories", repos_generator,
                                      "(name, owner, stars, watchers, forks, language, updated)"),
                self._process_batches("test.repositories_positions", positions_generator,
                                      "(date, repo, position)"),
                self._process_batches("test.repositories_authors_commits", authors_generator,
                                      "(date, repo, author, commits_num)")
            )

            logger.info(f"Успешно сохранено {len(repositories)} репозиториев в ClickHouse")

        except Exception as e:
            logger.error(f"Ошибка при сохранении данных в ClickHouse: {str(e)}")
            raise

    @staticmethod
    def _generate_repos_data(repositories: List[Repository], updated: datetime) -> Iterator[tuple]:
        """Генератор данных для таблицы repositories"""
        for repo in repositories:
            yield repo.as_repos_tuple(updated)

    @staticmethod
    def _generate_positions_data(repositories: List[Repository], current_date: date) -> Iterator[tuple]:
        """Генератор данных для таблицы repositories_positions"""
        for repo in repositories:
            yield repo.as_positions_tuple(current_date)

    @staticmethod
    def _generate_authors_data(repositories: List[Repository], current_date: date) -> Iterator[tuple]:
        """Генератор данных для таблицы repositories_authors_commits"""
        for repo in repositories:
            yield from repo.generate_authors_tuples(current_date)

    async def _process_batches(self, table: str, data_generator: Iterator, columns: str):
        """Вставка батчами из генератора"""
        processed_records = 0

        while True:
            batch = list(itertools.islice(data_generator, settings.BATCH_SIZE))
            if not batch:
                break

            try:
                await self._client.execute(
                    f"INSERT INTO {table} {columns} VALUES",
                    *batch
                )
                processed_records += len(batch)
                logger.debug(f"Вставлен батч из {len(batch)} записей в таблицу {table}")
            except ClientError as e:
                logger.error(f"Ошибка при вставке батча в таблицу {table}: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Неожиданная ошибка при вставке в таблицу {table}: {str(e)}")
                raise

        logger.info(f"Обработано {processed_records} записей в таблице {table}")