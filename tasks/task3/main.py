import asyncio
from datetime import datetime
from contextlib import AsyncExitStack

from tasks.task2.scrapper import GithubReposScrapper
from tasks.task2.config import settings as scraper_settings
from tasks.task3.ch_client import ClickHouseClient
from tasks.task3.database_init import DatabaseInitializer
from tasks.task3.loggers import logger


async def main():
    """Основная функция для сбора данных и сохранения в ClickHouse"""
    logger.info("Запуск сбора данных и сохранения в ClickHouse")

    if not scraper_settings.GITHUB_ACCESS_TOKEN:
        logger.error(
            "GitHub access token не установлен. Пожалуйста, установите переменную GITHUB_ACCESS_TOKEN в .env файле")
        return

    async with AsyncExitStack() as stack:
        try:
            initializer = DatabaseInitializer()
            await initializer.initialize()

            scrapper = await stack.enter_async_context(GithubReposScrapper(
                access_token=scraper_settings.GITHUB_ACCESS_TOKEN,
                max_concurrent_requests=scraper_settings.MAX_CONCURRENT_REQUESTS,
                requests_per_second=scraper_settings.REQUESTS_PER_SECOND
            ))

            start_time = datetime.now()
            repositories = await scrapper.get_repositories()
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Получено {len(repositories)} репозиториев за {duration:.2f} секунд")

            if repositories:
                start_time = datetime.now()
                client = await stack.enter_async_context(ClickHouseClient())
                await client.save_repositories(repositories)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Данные сохранены в ClickHouse за {duration:.2f} секунд")

                total_authors = sum(len(repo.authors_commits_num_today) for repo in repositories)
                logger.info(f"Статистика: {len(repositories)} репозиториев, {total_authors} авторов")
            else:
                logger.warning("Не получено данных для сохранения")

        except Exception as e:
            logger.error(f"Ошибка в основном процессе: {str(e)}")
            raise
        finally:
            logger.info("Процесс завершен")


if __name__ == "__main__":
    asyncio.run(main())
