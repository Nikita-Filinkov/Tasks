from pathlib import Path

from tasks.task4.ch_client import ClickHouseClient
from tasks.task4.loggers import logger


class DatabaseInitializer:
    """Класс для инициализации таблиц в базе данных"""

    def __init__(self, sql_file_path="table.sql"):
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

                if queries[0]:
                    await client._client.execute(queries[0])
                    logger.info("Таблица task4.phrases_views создана")

                if queries[1]:
                    await client._client.execute(queries[1])
                    logger.info(f"Данные загружены в таблицу task4.phrases_views")

                logger.info("Все запросы выполнены успешно")
                return True

            except Exception as e:
                logger.error(f"Ошибка при создании таблиц: {str(e)}")
                return False

    @staticmethod
    async def get_data():
        """Получение данных из запроса из SQL-файла"""
        async with ClickHouseClient() as client:
            try:
                with open('query.sql', 'r', encoding='utf-8') as f:
                    query = f.read()

                result = await client._client.fetch(query)
                print("Результат запроса:")
                rows = [dict(row) for row in result]
                for row in rows:
                    print(row)
                return rows
            except Exception as e:
                logger.error(f"Ошибка при выполнении запроса на получения данных: {str(e)}")
                return False


