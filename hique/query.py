from __future__ import annotations

from functools import reduce
from typing import Any, List, Type, Union

from hique.base import FieldExpr, Model
from hique.expr import Expr


class Query:
    pass


class SelectQuery(Query):
    def __init__(self, *values: Expr) -> None:
        super().__init__()
        self._values: List[Expr] = list(values)
        self._from: List[Any] = []
        self._filter: List[Expr] = []

    def from_(self, *sources: Type[Model], replace: bool = False) -> SelectQuery:
        if replace:
            self._from.clear()
        self._from.extend(sources)
        return self

    def filter(self, *args: Expr) -> SelectQuery:
        self._filter.extend(args)
        return self

    def __repr__(self) -> str:
        if self._filter:
            filter = f" WHERE {reduce(lambda a, i: a & i, self._filter[1:], self._filter[0])}"
        else:
            filter = ""
        return f"SELECT * FROM {','.join(f.__table_name__ for f in self._from)}{filter}"


def select(*values: Union[Expr, Type[Model]]) -> SelectQuery:
    values_: List[Expr] = []
    from_: List[Type[Model]] = []
    for value in values:
        if isinstance(value, Expr):
            if isinstance(value, FieldExpr) and value.table not in from_:
                from_.append(value.table)
            values_.append(value)
            continue

        if value not in from_:
            from_.append(value)
        for field in value.__fields__.keys():
            values_.append(getattr(value, field))

    return SelectQuery(*values_).from_(*from_)
