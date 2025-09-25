from pathlib import Path

from tasks.task3.ch_client import ClickHouseClient
from tasks.task3.loggers import logger


class DatabaseInitializer:
    """Класс для инициализации таблиц в базе данных"""

    def __init__(self, sql_file_path="tables.sql"):
        self.sql_file_path = Path(sql_file_path)

    async def initialize(self):
        """Инициализация таблиц из SQL-файла"""
        if not self.sql_file_path.exists():
            logger.error(f"Файл {self.sql_file_path} не найден")
            return False

        async with ClickHouseClient() as client:
            try:
                sql_content = self.sql_file_path.read_text(encoding="utf-8")

                queries = [q.strip() for q in sql_content.split(';') if q.strip()]

                for query in queries:
                    if query:
                        await client._client.execute(query)
                        logger.info(f"Выполнен запрос: {query[:50]}...")

                logger.info("Все таблицы успешно созданы")
                return True

            except Exception as e:
                logger.error(f"Ошибка при создании таблиц: {str(e)}")
                return False
