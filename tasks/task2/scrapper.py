import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from aiohttp import ClientError, ClientSession, ClientTimeout

from tasks.task2.config import settings
from tasks.task2.data_objects import Repository, RepositoryAuthorCommitsNum
from tasks.task2.limiters import SimpleRateLimiter
from tasks.task2.loggers import logger

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


class GithubReposScrapper:
    def __init__(self, access_token: str, max_concurrent_requests: int = 10, requests_per_second: int = 10):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            },
            timeout=ClientTimeout(total=30)
        )
        self._max_concurrent_requests = max_concurrent_requests
        self._requests_per_second = requests_per_second
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._rate_limiter = SimpleRateLimiter(requests_per_second)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def _make_request(self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None) -> Any:
        """Выполнение HTTP-запроса с ограничениями MCR и RPS"""
        await self._rate_limiter.acquire()

        async with self._semaphore:
            try:
                async with self._session.request(
                        method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params
                ) as response:
                    if response.status == 403:
                        raise Exception("Rate limit exceeded")
                    response.raise_for_status()
                    return await response.json()

            except ClientError as e:
                logger.error(f"Ошибка HTTP: {str(e)}",
                             extra={'endpoint': endpoint, 'error': str(e)})
                raise
            except Exception as e:
                logger.error(f"Неожиданная ошибка для {endpoint}: {str(e)}",
                             extra={'endpoint': endpoint, 'error': str(e)})
                raise

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """Получение топ-репозиториев по количеству звезд"""
        try:
            data = await self._make_request(
                endpoint="search/repositories",
                params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
            )
            return data["items"]
        except Exception as e:
            logger.error(f"Ошибка при получении репозиториев: {str(e)}")
            return []

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """Получение коммитов репозитория за последний день"""
        since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        try:
            data = await self._make_request(
                endpoint=f"repos/{owner}/{repo}/commits",
                params={"since": since, "per_page": 100}
            )
            return data
        except Exception as e:
            logger.error(f"Ошибка при извлечении коммитов для {owner}/{repo}: {str(e)}")
            return []

    async def _process_repository(self, repo_data: dict, position: int) -> Repository | None:
        """Обработка данных отдельного репозитория"""
        try:
            owner = repo_data['owner']['login']
            repo_name = repo_data['name']

            commits = await self._get_repository_commits(owner, repo_name)

            author_commits = {}
            for commit in commits:
                author_data = commit.get('author') or commit.get('commit', {}).get('author', {})
                author = author_data.get('login') if isinstance(author_data, dict) else 'Unknown'
                if not author:
                    author = commit.get('commit', {}).get('author', {}).get('name', 'Unknown')
                author_commits[author] = author_commits.get(author, 0) + 1

            authors_commits = [
                RepositoryAuthorCommitsNum(author, count)
                for author, count in author_commits.items()
            ]

            return Repository(
                name=repo_name,
                owner=owner,
                position=position,
                stars=repo_data['stargazers_count'],
                watchers=repo_data['watchers_count'],
                forks=repo_data['forks_count'],
                language=repo_data.get('language', ''),
                authors_commits_num_today=authors_commits
            )

        except Exception as e:
            logger.error(f"Ошибка в формировании репозитория: {repo_data.get('name', 'unknown')}: {str(e)}")
            return None

    async def get_repositories(self) -> list[BaseException]:
        """Основной метод для получения информации о репозиториях"""
        top_repos = await self._get_top_repositories(settings.TOP_REPOSITORIES_LIMIT)
        repositories = []

        tasks = []
        for i, repo_data in enumerate(top_repos):
            task = self._process_repository(repo_data, i + 1)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при фильтрации результатов формирования репозиториев: {str(result)}")
            elif result is not None:
                repositories.append(result)

        return repositories

    async def close(self):
        """Закрытие HTTP-сессии"""
        if not self._session.closed:
            await self._session.close()