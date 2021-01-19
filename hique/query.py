from __future__ import annotations

from functools import reduce
from typing import List, Any, Type

from hique.base import Base
from hique.expr import Expr


class Query:
    pass


class SelectQuery(Query):
    def __init__(self) -> None:
        super().__init__()
        self._from: List[Any] = []
        self._filter: List[Expr] = []

    def from_(self, *sources: Any, replace: bool = False) -> SelectQuery:
        if replace:
            self._from.clear()
        self._from.extend(sources)
        return self

    def filter(self, *args: Expr, **kwargs: Any) -> SelectQuery:
        self._filter.extend(args)
        self._filter.extend(
            self._from[0]._fields[key].get_impl(self._from[0]) == value
            for key, value in kwargs.items()
        )
        return self

    def __repr__(self) -> str:
        filter = reduce(lambda a, i: a & i, self._filter[1:], self._filter[0])
        return f"SELECT * FROM {','.join(f._table_name for f in self._from)} WHERE {filter}"


def select(cls: Type[Base], *fields: Any) -> SelectQuery:
    return SelectQuery(*fields).from_(cls)
