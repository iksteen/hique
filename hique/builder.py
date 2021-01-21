import abc
from typing import Any, Tuple

from hique.query import Query


class QueryBuilder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __call__(self, query: Query) -> Tuple[str, Tuple[Any, ...]]:
        ...
