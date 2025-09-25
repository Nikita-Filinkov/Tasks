from dataclasses import astuple, dataclass
from datetime import date, datetime
from typing import List


@dataclass
class RepositoryAuthorCommitsNum:
    """Сущность количества коммитов авторов"""
    author: str
    commits_num: int

    def as_tuple(self, repo_name: str, commit_date: date) -> tuple:
        """Преобразование в кортеж для вставки в БД"""
        return (
            commit_date.isoformat(),
            repo_name,
            self.author,
            self.commits_num
        )


@dataclass
class Repository:
    """"Сущность Репозитория"""
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: List[RepositoryAuthorCommitsNum]

    def as_repos_tuple(self, updated: datetime) -> tuple:
        """Преобразование в кортеж для таблицы repositories"""
        return (
            self.name,
            self.owner,
            self.stars,
            self.watchers,
            self.forks,
            self.language or "",
            updated.replace(microsecond=0)
        )

    def as_positions_tuple(self, current_date: date) -> tuple:
        """Преобразование в кортеж для таблицы repositories_positions"""
        return (
            current_date.isoformat(),
            self.name,
            self.position
        )

    def generate_authors_tuples(self, current_date: date):
        """Генератор кортежей для таблицы repositories_authors_commits"""
        for author_commits in self.authors_commits_num_today:
            yield author_commits.as_tuple(self.name, current_date)