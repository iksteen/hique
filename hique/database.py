from __future__ import annotations

import abc
from typing import Any, List, Mapping

from hique.builder import QueryBuilder


class Database(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def query_builder(self) -> QueryBuilder:
        ...

    @abc.abstractmethod
    async def init(self, *args: Any, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    async def connection(self) -> Connection:
        ...


class Connection(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def execute(self, query: str, *args: Any) -> List[Mapping[str, Any]]:
        ...

    @abc.abstractmethod
    async def transaction(self) -> Transaction:
        ...

    @abc.abstractmethod
    async def release(self) -> None:
        ...


class Transaction(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def commit(self) -> None:
        ...

    @abc.abstractmethod
    async def rollback(self) -> None:
        ...
