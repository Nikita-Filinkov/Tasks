import asyncio

from tasks.task2.config import settings
from tasks.task2.loggers import logger
from tasks.task2.scrapper import GithubReposScrapper


async def main():
    """Использование Парсера"""
    scrapper = GithubReposScrapper(
        access_token=settings.GITHUB_ACCESS_TOKEN,
        max_concurrent_requests=settings.MAX_CONCURRENT_REQUESTS,
        requests_per_second=settings.REQUESTS_PER_SECOND
    )

    try:
        repositories = await scrapper.get_repositories()
        logger.info(f"Успешно обработаны {len(repositories)} репозиториев")

        # Вывод информации о первых 5 репозиториях
        for repo in repositories[:5]:
            print(f"{repo.position}. {repo.owner}/{repo.name} - {repo.stars} stars")
            print(f"  Commits today: {len(repo.authors_commits_num_today)} authors")

    finally:
        await scrapper.close()


if __name__ == "__main__":
    asyncio.run(main())
